"""Taxa validation script for validating CSV uploads without importing.

This script validates taxa CSV files for:
- Required and recommended header columns
- Duplicate GBIF keys within the file
- Duplicate FADA IDs within the file
- Existing GBIF keys in the database
- Existing FADA IDs in the database
- GBIF key validation against GBIF API (rank and taxon name)
- Missing FADA ID per row (on FADA sites)
"""
import csv
import copy
import difflib
import logging
from io import StringIO
from collections import defaultdict

from bims.scripts.species_keys import (
    GBIF_LINK, GBIF_URL, FADA_ID, TAXON, TAXON_RANK, GENUS, SPECIES,
    TAXONOMIC_STATUS, ACCEPTED_TAXON, SYNONYM, AUTHORS, SUBGENUS,
    RANK_HIERARCHY, RANK_TITLE, SPECIES_GROUP, ON_GBIF
)
from bims.scripts.data_upload import FALLBACK_ENCODINGS
from bims.models import Taxonomy, UploadSession
from bims.utils.domain import get_current_domain
from bims.utils.gbif import get_species_by_col_id

logger = logging.getLogger('bims')

VALIDATION_OK = '_validation_ok'
VALIDATION_WARNING = '_validation_warning'
VALIDATION_ERROR = '_validation_error'
NAME_SIMILARITY_THRESHOLD = 1

# Column titles for each rank in the hierarchy, keyed by upper-case rank name.
RANK_COLUMN_TITLES = dict(RANK_TITLE)
RANK_COLUMN_TITLES['SPECIESGROUP'] = SPECIES_GROUP


class TaxaValidator:
    """Validates taxa data without creating records."""

    def __init__(self, upload_session):
        from bims.templatetags.site import is_fada_site
        self.upload_session = upload_session
        self.file_gbif_keys = defaultdict(list)
        self.file_fada_ids = defaultdict(list)
        self.file_taxon_name_rank = defaultdict(list)
        self.file_taxon_name_rank_author = defaultdict(list)
        self.file_taxon_name_rank_status = defaultdict(list)
        self.file_taxon_names_lower = set()
        self.db_accepted_taxon_names_lower = set()
        self.validation_results = []
        self.all_rows = []
        self.headers = []
        self.total_rows = 0
        self.domain = get_current_domain()
        self.is_fada = is_fada_site()

    @staticmethod
    def row_value(row, key):
        """Get cleaned value from row, with case-insensitive key fallback."""
        value = row.get(key, '')
        if not value:
            key_lower = key.lower().strip()
            for k, v in row.items():
                if k.lower().strip() == key_lower:
                    value = v
                    break
        if value:
            value = str(value).strip()
        return value

    def _extract_gbif_key(self, row):
        """Extract GBIF key from GBIF_LINK or GBIF_URL column."""
        gbif_link = self.row_value(row, GBIF_LINK) or self.row_value(row, GBIF_URL)
        if gbif_link:
            last = str(gbif_link).rstrip('/').split('/')[-1]
            return last[:-2] if last.endswith('.0') else last
        return None

    def _check_gbif_link_format(self, row):
        """Validate that a provided GBIF link points at a Catalogue of Life
        taxon (gbif.org/taxon/<col_id>), matching the same rule enforced at
        import time in taxa_upload.py. The legacy gbif.org/species/<gbif_key>
        form, and bare numeric keys (which indicate a legacy GBIF key was
        pasted in by mistake), are rejected.

        Returns a list of error messages.
        """
        messages = []

        gbif_link = self.row_value(row, GBIF_LINK) or self.row_value(row, GBIF_URL)
        if not gbif_link:
            return messages

        link_str = str(gbif_link).rstrip('/')
        last = link_str.split('/')[-1]
        last = last[:-2] if last.endswith('.0') else last

        if '/species/' in link_str.lower():
            messages.append(
                f"ERROR: GBIF key '{last}' is not accepted; provide a Catalogue of "
                f"Life link instead (e.g. gbif.org/taxon/{last})."
            )
            return messages

        if 'gbif.org' in link_str.lower() and '/taxon/' not in link_str.lower():
            messages.append(
                f"ERROR: GBIF link '{gbif_link}' is not recognized; expected a "
                f"Catalogue of Life link in the form gbif.org/taxon/<col_id>"
            )
            return messages

        if last.isdigit():
            messages.append(
                f"ERROR: GBIF col_id '{last}' looks like a legacy GBIF taxon key "
                f"(numeric only); expected a Catalogue of Life taxon key from "
                f"gbif.org/taxon/<col_id>"
            )

        return messages

    def _check_database_duplicates(self, gbif_key, fada_id):
        """Check if record already exists in database. Returns list of warnings."""
        warnings = []
        if gbif_key:
            try:
                gbif_taxa = Taxonomy.objects.filter(gbif_key=int(gbif_key))
                warning_message = ''
                if gbif_taxa.exists():
                    warning_message = f"WARNING: GBIF key {gbif_key} already exists in database:"
                for _taxon in gbif_taxa:
                    warning_message += (
                        f"{_taxon.canonical_name} ({_taxon.rank}) "
                    )
                if warning_message:
                    warnings.append(warning_message)
            except (ValueError, TypeError):
                pass
        if fada_id:
            fada_taxa = Taxonomy.objects.filter(fada_id=fada_id)
            warning_message = ''
            if fada_taxa.exists():
                warning_message = f"WARNING: FADA ID {fada_id} already exists in database:"
            for _taxon in fada_taxa:
                warning_message += (
                    f"{_taxon.canonical_name} ({_taxon.rank}) "
                )
            if warning_message:
                warnings.append(warning_message)
        return warnings

    def _check_required_headers(self):
        """Check that important headers are present in the CSV.

        Returns (errors, warnings) as lists of message strings.
        """
        errors = []
        warnings = []
        headers_lower = {h.lower().strip() for h in self.headers}

        # At least one taxon name column is required
        name_cols = [TAXON, GENUS, SPECIES]
        if not any(col.lower() in headers_lower for col in name_cols):
            errors.append(
                f"Missing taxon name column — expected at least one of: "
                f"{', '.join(name_cols)}"
            )

        if TAXON_RANK.lower() not in headers_lower:
            errors.append(f"Missing required column: '{TAXON_RANK}'")
        if TAXONOMIC_STATUS.lower() not in headers_lower:
            errors.append(f"Missing recommended column: '{TAXONOMIC_STATUS}'")
        if self.is_fada and FADA_ID.lower() not in headers_lower:
            errors.append(f"Missing required column for FADA site: '{FADA_ID}'")

        return errors, warnings

    def _normalize_name(self, name):
        """Normalize taxon name for comparison."""
        if not name:
            return ''
        return ' '.join(name.lower().split())

    def _get_input_taxon_name(self, row):
        """Get the taxon name from the row, composing it if necessary."""
        taxon = self.row_value(row, TAXON)
        if taxon:
            return taxon

        genus = self.row_value(row, GENUS)
        species = self.row_value(row, SPECIES)
        if genus and species:
            return f"{genus} {species}"
        elif genus:
            return genus

        return ''

    def _is_synonym_row(self, row):
        """Return True if the row's taxonomic status marks it as a synonym."""
        taxonomic_status = self.row_value(row, TAXONOMIC_STATUS).upper()
        synonym_flag = self.row_value(row, SYNONYM).lower()

        return (
            'SYNONYM' in taxonomic_status or
            synonym_flag in ('true', 'yes', '1')
        )

    def _status_category(self, row):
        """Classify a row as 'SYNONYM', 'ACCEPTED', or 'OTHER' based on its
        taxonomic status, for homonymy-vs-accepted/synonym disambiguation."""
        if self._is_synonym_row(row):
            return 'SYNONYM'

        taxonomic_status = self.row_value(row, TAXONOMIC_STATUS).upper()
        if 'ACCEPTED' in taxonomic_status:
            return 'ACCEPTED'

        return 'OTHER'

    def _check_synonym_accepted_taxon(self, row):
        """Check that the accepted taxon exists in the system or the CSV for synonym taxa.

        Uses pre-built sets populated in _first_pass_collect_keys to avoid
        per-row database queries (important for CSVs with 1000+ rows).

        Returns a list of error messages.
        """
        messages = []

        if not self._is_synonym_row(row):
            return messages

        accepted_taxon_name = self.row_value(row, ACCEPTED_TAXON)
        if not accepted_taxon_name:
            return messages

        name_lower = accepted_taxon_name.lower()
        in_csv = name_lower in self.file_taxon_names_lower
        in_db = name_lower in self.db_accepted_taxon_names_lower

        if not in_csv and not in_db:
            messages.append(
                f"ERROR: Accepted taxon '{accepted_taxon_name}' is not in the system or the upload file"
            )

        return messages

    def _check_parent_name_conflict(self, row):
        """Check that no rank column in the classification chain (Kingdom,
        Phylum, Class, ... Forma) shares a name with its nearest filled
        ancestor column.

        Returns a list of error messages.
        """
        messages = []
        last_rank = None
        last_name = None

        for rank in RANK_HIERARCHY:
            title = RANK_COLUMN_TITLES.get(rank, rank.capitalize())
            value = self.row_value(row, title)
            if not value:
                continue

            if last_rank and last_name and value.lower() == last_name.lower():
                is_sub_to_nonsub = (
                    rank.startswith('SUB') and rank[3:] == last_rank
                )
                if not is_sub_to_nonsub:
                    messages.append(
                        f"ERROR: Parent '{last_name}' ({last_rank}) "
                        f"cannot have the same name as '{value}' ({rank})"
                    )

            last_rank = rank
            last_name = value

        return messages

    def _check_on_gbif_without_link(self, row, gbif_key):
        """Warn when 'On GBIF' is marked but no GBIF URL/col_id is given,
        since the GBIF lookup will then fall back to matching by taxon name.

        Returns a list of warning messages.
        """
        messages = []

        on_gbif_value = self.row_value(row, ON_GBIF)
        if on_gbif_value and 'yes' in on_gbif_value.lower() and not gbif_key:
            messages.append(
                "WARNING: 'On GBIF' is marked but no GBIF URL was provided; "
                "the GBIF search will fall back to using the taxon name"
            )

        return messages

    def _validate_gbif_key(self, row, gbif_key):
        """Validate GBIF key against GBIF API. Returns list of messages."""
        messages = []

        if not gbif_key:
            return messages

        try:
            gbif_rec = get_species_by_col_id(gbif_key)
        except Exception as e:
            messages.append(f"WARNING: GBIF lookup failed for col_id {gbif_key}: {str(e)}")
            return messages

        if not gbif_rec or not isinstance(gbif_rec, dict) or not gbif_rec.get("usage"):
            messages.append(f"WARNING: GBIF record not found for col_id {gbif_key}")
            return messages

        gbif_usage = gbif_rec.get("usage")

        input_rank = self.row_value(row, TAXON_RANK).upper() if self.row_value(row, TAXON_RANK) else ''
        input_name = self._get_input_taxon_name(row)

        gbif_rank = (gbif_usage.get("rank") or '').upper()
        gbif_name = gbif_usage.get("canonicalName") or gbif_rec.get("name") or ""

        # Check rank mismatch - ERROR
        if input_rank and gbif_rank and input_rank != gbif_rank:
            messages.append(
                f"ERROR: GBIF col_id {gbif_key} refers to a different taxon. "
                f"Expected rank '{input_rank}' but GBIF returns '{gbif_rank}' for '{gbif_name}'"
            )

        # Check name mismatch - WARNING
        if input_name and gbif_name:
            norm_input = self._normalize_name(input_name)
            norm_gbif = self._normalize_name(gbif_name)

            if norm_input != norm_gbif:
                # Calculate similarity
                similarity = difflib.SequenceMatcher(None, norm_input, norm_gbif).ratio()

                if similarity < NAME_SIMILARITY_THRESHOLD:
                    messages.append(
                        f"ERROR: GBIF col_id {gbif_key} may refer to a different taxon. "
                        f"Input name '{input_name}' does not match GBIF name '{gbif_name}' "
                        f"(similarity: {similarity:.0%})"
                    )

        return messages

    def _first_pass_collect_keys(self, rows):
        """First pass: collect all GBIF keys, FADA IDs, and taxon names to detect duplicates.

        Also collects all taxon names present in the CSV and all accepted taxon names
        referenced by synonym rows, then performs a single bulk DB query so that
        _check_synonym_accepted_taxon() can do O(1) set lookups instead of one
        query per row.
        """
        accepted_taxon_names_to_check = set()

        for row_number, row in enumerate(rows, start=2):  # Start at 2 (row 1 is header)
            gbif_key = self._extract_gbif_key(row)
            fada_id = self.row_value(row, FADA_ID)
            taxon_name = self._get_input_taxon_name(row)
            taxon_rank = self.row_value(row, TAXON_RANK)

            if gbif_key:
                self.file_gbif_keys[gbif_key].append(row_number)
            if fada_id:
                self.file_fada_ids[fada_id].append(row_number)
            if taxon_name:
                author = self.row_value(row, AUTHORS)
                subgenus = self.row_value(row, SUBGENUS)
                # (name, rank) for homonymy detection
                name_rank_key = (taxon_name.lower(), (taxon_rank or '').upper())
                self.file_taxon_name_rank[name_rank_key].append(row_number)
                self.file_taxon_name_rank_status[name_rank_key].append(
                    self._status_category(row))
                # (name, rank, subgenus, author) for true-duplicate detection
                name_rank_author_key = (
                    taxon_name.lower(),
                    (taxon_rank or '').upper(),
                    (subgenus or '').upper(),
                    (author or '').lower().strip(),
                )
                self.file_taxon_name_rank_author[name_rank_author_key].append(row_number)
                self.file_taxon_names_lower.add(taxon_name.lower())

            # Collect accepted taxon names referenced by synonym rows
            is_synonym = self._is_synonym_row(row)
            if is_synonym:
                accepted_taxon_name = self.row_value(row, ACCEPTED_TAXON)
                if accepted_taxon_name:
                    accepted_taxon_names_to_check.add(accepted_taxon_name.lower())

        # Single bulk DB query for all accepted taxon names not already in the CSV.
        # Use LOWER() annotation so the lookup is case-insensitive without N queries.
        names_not_in_csv = accepted_taxon_names_to_check - self.file_taxon_names_lower
        if names_not_in_csv:
            from django.db.models.functions import Lower
            existing = (
                Taxonomy.objects
                .annotate(name_lower=Lower('canonical_name'))
                .filter(name_lower__in=names_not_in_csv)
                .values_list('name_lower', flat=True)
            )
            self.db_accepted_taxon_names_lower = set(existing)

    def _validate_row(self, row, row_number):
        """Validate a single row. Returns list of error/warning messages."""
        messages = []

        gbif_key = self._extract_gbif_key(row)
        fada_id = self.row_value(row, FADA_ID)

        # Check for within-file duplicates (GBIF key)
        if gbif_key and len(self.file_gbif_keys.get(gbif_key, [])) > 1:
            other_rows = [r-1 for r in self.file_gbif_keys[gbif_key] if r != row_number]
            messages.append(
                f"ERROR: Duplicate GBIF col_id {gbif_key} (also in row(s) {', '.join(map(str, other_rows))})"
            )

        # Check for within-file duplicates (FADA ID)
        if fada_id and len(self.file_fada_ids.get(fada_id, [])) > 1:
            other_rows = [r-1 for r in self.file_fada_ids[fada_id] if r != row_number]
            messages.append(
                f"ERROR: Duplicate FADA ID {fada_id} (also in row(s) {', '.join(map(str, other_rows))})"
            )

        # On FADA sites, every taxon must have a FADA ID
        if self.is_fada and not fada_id:
            messages.append("ERROR: FADA ID is missing")

        # Check for within-file duplicates and homonyms (same name + rank)
        taxon_name = self._get_input_taxon_name(row)
        taxon_rank = self.row_value(row, TAXON_RANK)
        author = self.row_value(row, AUTHORS)
        subgenus = self.row_value(row, SUBGENUS)

        if taxon_name:
            name_rank_key = (taxon_name.lower(), (taxon_rank or '').upper())
            name_rank_author_key = (
                taxon_name.lower(),
                (taxon_rank or '').upper(),
                (subgenus or '').upper(),
                (author or '').lower().strip(),
            )
            rows_same_name_rank = self.file_taxon_name_rank.get(name_rank_key, [])
            rows_same_name_rank_author = self.file_taxon_name_rank_author.get(name_rank_author_key, [])

            if len(rows_same_name_rank_author) > 1:
                # Same name + rank + author: true duplicate
                other_rows = [r - 1 for r in rows_same_name_rank_author if r != row_number]
                messages.append(
                    f"ERROR: Duplicate taxon name '{taxon_name}' with same rank '{taxon_rank}' and author(s) '{author}' "
                    f"(also in row(s) {', '.join(map(str, other_rows))})"
                )
            elif len(rows_same_name_rank) > 1:
                # Same name + rank but different authors.
                statuses = self.file_taxon_name_rank_status.get(name_rank_key, [])
                is_accepted_synonym_pair = (
                    statuses.count('ACCEPTED') == 1 and
                    statuses.count('SYNONYM') == len(statuses) - 1
                )
                if not is_accepted_synonym_pair:
                    other_rows = [r - 1 for r in rows_same_name_rank if r != row_number]
                    messages.append(
                        f"WARNING: Homonymy detected - '{taxon_name}' ({taxon_rank}) appears with different author(s) "
                        f"(also in row(s) {', '.join(map(str, other_rows))}). "
                        f"Verify that one is the accepted name and the other is a synonym."
                    )

        # Check for conflicting names in the classification chain
        # (e.g. Phylum and Class columns holding the same name)
        parent_name_conflicts = self._check_parent_name_conflict(row)
        messages.extend(parent_name_conflicts)

        # Warn if marked 'On GBIF' but no GBIF URL/col_id is given
        on_gbif_warnings = self._check_on_gbif_without_link(row, gbif_key)
        messages.extend(on_gbif_warnings)

        # Check that the GBIF link, if given, points at a COL taxon
        # (gbif.org/taxon/<col_id>) rather than a legacy numeric GBIF key
        gbif_link_format_errors = self._check_gbif_link_format(row)
        messages.extend(gbif_link_format_errors)

        # Check database duplicates
        db_warnings = self._check_database_duplicates(gbif_key, fada_id)
        messages.extend(db_warnings)

        # Validate GBIF key against GBIF API (rank and name check)
        gbif_validation = self._validate_gbif_key(row, gbif_key)
        messages.extend(gbif_validation)

        # Check that accepted taxon exists in the system for synonym taxa
        synonym_validation = self._check_synonym_accepted_taxon(row)
        messages.extend(synonym_validation)

        return messages

    def validate_file(self):
        """Validate the CSV file from the upload session."""
        try:
            with open(self.upload_session.process_file.path, 'rb') as fh:
                raw = fh.read()
        except Exception as e:
            self.upload_session.error_notes = f"Error reading file: {e}"
            self.upload_session.canceled = True
            self.upload_session.save()
            return

        # Try different encodings
        tried = ["utf-8-sig"] + [e for e in FALLBACK_ENCODINGS if e != "utf-8-sig"]
        text = None
        last_exc = None
        for enc in tried:
            try:
                text = raw.decode(enc)
                break
            except UnicodeDecodeError as exc:
                last_exc = exc
                continue

        if text is None:
            self.upload_session.error_notes = (
                f"Could not decode file with encodings {tried}: {last_exc}"
            )
            self.upload_session.canceled = True
            self.upload_session.save()
            return

        try:
            reader = csv.DictReader(StringIO(text))
            self.headers = reader.fieldnames or []
            self.all_rows = list(reader)
            self.total_rows = len(self.all_rows)
        except Exception as e:
            self.upload_session.error_notes = f"Error parsing CSV: {e}"
            self.upload_session.canceled = True
            self.upload_session.save()
            return

        header_errors, header_warnings = self._check_required_headers()
        if header_errors or header_warnings:
            notes = []
            if header_errors:
                notes.extend(header_errors)
            if header_warnings:
                notes.extend(header_warnings)
            self.upload_session.error_notes = '; '.join(notes)
            self.upload_session.save()
        if header_errors:
            self.upload_session.canceled = True
            self.upload_session.save()
            return

        # Update progress
        self.upload_session.progress = f"0/{self.total_rows}"
        self.upload_session.save()

        # First pass: collect all keys
        self._first_pass_collect_keys(self.all_rows)

        # Second pass: validate each row
        for index, row in enumerate(self.all_rows):
            if UploadSession.objects.get(id=self.upload_session.id).canceled:
                return

            row_number = index + 2
            messages = self._validate_row(row, row_number)

            # Separate errors and warnings
            errors = [m for m in messages if m.startswith('ERROR:')]
            warnings = [m for m in messages if m.startswith('WARNING:')]

            # Set validation columns
            if errors:
                row[VALIDATION_ERROR] = '; '.join(errors)
            else:
                row[VALIDATION_ERROR] = ''

            if warnings:
                row[VALIDATION_WARNING] = '; '.join(warnings)
            else:
                row[VALIDATION_WARNING] = ''

            # Set OK status only if no errors and no warnings
            if not errors and not warnings:
                row[VALIDATION_OK] = 'OK'
            else:
                row[VALIDATION_OK] = ''

            self.validation_results.append(row)

            # Update progress
            self.upload_session.progress = f"{index + 1}/{self.total_rows}"
            self.upload_session.save()

        # Generate validated CSV
        self._generate_validated_csv()

    def _generate_validated_csv(self):
        """Generate the validated CSV with _validation_status column."""
        file_name = self.upload_session.process_file.name.replace('taxa-file/', '')
        file_path = self.upload_session.process_file.path.replace(file_name, '')

        # Add validation status to headers
        output_headers = copy.deepcopy(self.headers)
        if VALIDATION_ERROR not in output_headers:
            output_headers.append(VALIDATION_ERROR)
        if VALIDATION_WARNING not in output_headers:
            output_headers.append(VALIDATION_WARNING)
        if VALIDATION_OK not in output_headers:
            output_headers.append(VALIDATION_OK)

        validated_file_path = f'{file_path}validated_{file_name}'

        with open(validated_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=output_headers,
                quoting=csv.QUOTE_MINIMAL,
                extrasaction='ignore'
            )
            writer.writeheader()
            for row in self.validation_results:
                writer.writerow(row)

        # Save to error_file field (reusing this field for the validated output)
        self.upload_session.error_file.name = f'taxa-file/validated_{file_name}'

        # Count errors and warnings
        error_count = sum(1 for r in self.validation_results if r.get(VALIDATION_ERROR))
        warning_count = sum(1 for r in self.validation_results if r.get(VALIDATION_WARNING))
        ok_count = sum(1 for r in self.validation_results if r.get(VALIDATION_OK) == 'OK')

        self.upload_session.progress = f"Validation complete: {ok_count} OK, {error_count} errors, {warning_count} warnings"
        self.upload_session.processed = True
        self.upload_session.save()
