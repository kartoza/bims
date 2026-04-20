import difflib
import json
import logging
import re

import requests
from django.db import transaction

from bims.api_views.taxon_update import create_taxon_proposal
from bims.models.taxon_group import TaxonGroup
from bims.scripts.collections_upload_source_reference import get_or_create_data_from_model

from bims.scripts.species_keys import *  # noqa
from bims.enums import TaxonomicRank
from bims.models import (
    Taxonomy,
    Endemism,
    IUCNStatus,
    IUCN_CATEGORIES,
    VernacularName,
    ORIGIN_CATEGORIES, TaxonTag, SourceReference,
    SourceReferenceBibliography,
    Invasion, SpeciesGroup, TaxonOrigin,
)
from bims.templatetags import is_fada_site
from bims.utils.fetch_gbif import (
    fetch_all_species_from_gbif, fetch_gbif_vernacular_names, harvest_synonyms_for_accepted_taxonomy
)
from bims.scripts.data_upload import DataCSVUpload
from bims.utils.gbif import get_species
from td_biblio.exceptions import DOILoaderError
from td_biblio.models import Entry
from td_biblio.utils.loaders import DOILoader

from preferences import preferences

logger = logging.getLogger('bims')

NAME_SIM_THRESHOLD = 0.70
_TRUTHY = {'y', 'true', 't', '✓'}
INFRA_MARKER_RE = re.compile(
    r'\b(?:subsp|ssp|subspecies|var|variety|forma|form|f)\.?\b',
    re.IGNORECASE
)

IS_FIELD_RE = re.compile(r'^\s*IS[\s_:-]*(.+?)\s*$', re.IGNORECASE)

def _norm_park_label(s: str) -> str:
    return re.sub(r'\s+', ' ', (s or '').strip()).lower()

def _as_truthy(val) -> bool:
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in _TRUTHY

def _norm_key_label(s: str) -> str:
    # Remove "(y/n)" and collapse whitespace
    return re.sub(r'\s+', ' ', re.sub(r'\(y/n\)', '', str(s), flags=re.IGNORECASE)).strip()

def _safe_upper(s):
    return str(s or '').upper()

def _safe_strip(s):
    return str(s or '').strip()

def _canon(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()

def _norm_taxon_for_similarity(s: str) -> str:
    """
    Canonicalize a taxon string for name matching:
      - lowercase + collapse whitespace (via _canon)
      - drop infraspecific markers like 'subsp.', 'var.', 'f.'
    """
    s = _canon(s)
    s = INFRA_MARKER_RE.sub(' ', s)
    return re.sub(r'\s+', ' ', s).strip()


class TaxaProcessor(object):

    all_keys = {}

    def _get_or_create_vernacular_singleton(self, name: str, language: str | None):
        """
        Robustly fetch a single VernacularName without raising MultipleObjectsReturned.
        If multiples exist, return the earliest one. If none, create one.
        """
        name = re.sub(r'\s+', ' ', (name or '').strip())
        language = (language or '').strip() or None

        qs = VernacularName.objects.filter(
            name=name,
            language=language,
            is_upload=True
        ).order_by('id')

        obj = qs.first()
        if obj:
            return obj

        return VernacularName.objects.create(
            name=name,
            language=language,
            is_upload=True
        )

    def _update_taxon_and_proposal(self, taxonomy, proposal, use_proposal, new_taxon, field, value):
        # Apply on taxonomy for brand-new objects or when proposals are disabled.
        if new_taxon or not use_proposal:
            setattr(taxonomy, field, value)
        # Apply on proposal when proposals are enabled.
        if use_proposal and proposal is not None:
            setattr(proposal, field, value)

    def _compose_species_name(self, row) -> str:
        """
        Return a canonical binomial/trinomial.

        • If the species cell already starts with the genus, normalize capitalization.
        • Otherwise prepend the genus.
        • Always force epithets (everything after the genus) to lower-case.
        """
        genus = _safe_strip(self.get_row_value(row, GENUS))
        species = _safe_strip(self.get_row_value(row, SPECIES))

        if not species:
            species = _safe_strip(self.get_row_value(row, TAXON))

        if not genus or not species:
            return species or genus

        parts = species.split()
        epithets = ' '.join(p.lower() for p in parts[1:]) if len(parts) > 1 else ''
        # If species already includes genus (case-insensitive), normalize capitalization.
        if species.lower().startswith(genus.lower() + ' '):
            genus_cap = genus[:1].upper() + genus[1:].lower()
            rest = species.split(' ', 1)[1] if ' ' in species else ''
            rest_norm = ' '.join(w.lower() for w in rest.split())
            return f'{genus_cap} {rest_norm}'.strip()

        genus_cap = genus[:1].upper() + genus[1:].lower()
        species_lc = parts[0].lower()
        tail = f' {epithets}' if epithets else ''
        return f'{genus_cap} {species_lc}{tail}'.strip()

    def _fix_species_like(self, row, name: str) -> str:
        """
        Ensure Genus prefix is present and normalize casing:
        Genus Capitalized, epithets lower-case.
        """
        name = (name or '').strip()
        if not name:
            return name
        genus = _safe_strip(self.get_row_value(row, GENUS))
        parts = name.split()
        if not parts:
            return name

        if genus:
            genus_cap = genus[:1].upper() + genus[1:].lower()
            if name.lower().startswith(genus.lower() + ' '):
                rest = ' '.join(w.lower() for w in parts[1:])
                return f'{genus_cap} {rest}'.strip()
            rest = ' '.join(w.lower() for w in parts)
            return f'{genus_cap} {rest}'.strip()

        head = parts[0][:1].upper() + parts[0][1:].lower()
        tail = ' '.join(w.lower() for w in parts[1:])
        return f'{head} {tail}'.strip()

    def _choose_taxon_display_name(self, row, rank: str, taxonomic_status: str,
                                   csv_taxon: str, composed_taxon: str) -> str:
        """
        Decide canonical/binomial name:
        - Synonyms: prefer CSV; ensure species-like names have Genus prefix/casing.
        - Species/subspecies (non-synonym): choose the one closer to the composed target.
        - Higher ranks: prefer CSV if present, else composed.
        """
        csv_taxon = _safe_strip(csv_taxon)
        composed_taxon = _safe_strip(composed_taxon)
        rank_l = (rank or '').lower()
        status_l = (taxonomic_status or '').strip().lower()
        is_species_like = 'species' in rank_l
        is_subgenus = 'subgenus' in rank_l

        if is_subgenus:
            subgenus_name = _safe_strip(self.get_row_value(row, SUBGENUS))
            return subgenus_name or composed_taxon

        if _canon(csv_taxon) == _canon(composed_taxon):
            return composed_taxon

        if 'synonym' in status_l:
            return csv_taxon

        if is_species_like:
            target = self._compose_species_name(row)
            r_csv = difflib.SequenceMatcher(
                None, _canon(csv_taxon), _canon(target)).ratio() if csv_taxon else 0.0
            r_cmp = difflib.SequenceMatcher(
                None, _canon(composed_taxon), _canon(target)).ratio() if composed_taxon else 0.0
            name = composed_taxon if r_cmp >= r_csv else csv_taxon
            return self._fix_species_like(row, name) if name else composed_taxon

        return csv_taxon or composed_taxon

    def add_taxon_to_taxon_group(self, taxonomy: Taxonomy, taxon_group: TaxonGroup, validated: bool = True, use_proposal: bool = False):
        """
        Add or update the relationship between a taxonomy and a taxon group,
        ensuring the 'is_validated' field is properly set in the through table.
        Also ensures that if the taxonomy is a synonym, its accepted taxonomy is added to the group.
        """
        taxon_group.taxonomies.add(
            taxonomy,
            through_defaults={'is_validated': validated}
        )
        if not validated:
            taxon_group.taxonomies.through.objects.filter(
                taxongroup=taxon_group,
                taxonomy=taxonomy
            ).update(is_validated=False)

        # Ensure accepted taxonomy is added if this is a synonym
        if taxonomy.accepted_taxonomy:
            from bims.api_views.taxon_update import ensure_accepted_taxonomy_in_group
            ensure_accepted_taxonomy_in_group(taxonomy, taxon_group, use_proposal=use_proposal)

    def add_taxon_to_taxon_group_unvalidated(self, taxonomy, taxon_group):
        """
        Helper for legacy callers. Note: this base implementation uses validated=True.
        TaxaCSVUpload overrides this to respect site preferences.
        """
        self.add_taxon_to_taxon_group(taxonomy, taxon_group, validated=True)

    def handle_error(self, row, message):
        pass

    def finish_processing_row(self, row, taxonomy):
        pass

    def rank_name(self, taxon: Taxonomy):
        return (taxon.rank or '').upper() if taxon and taxon.rank else ''

    def get_row_value(self, row, key):
        """
        Return value with normalization:
        - Treat numeric 0 or string "0" as missing (None).
        """
        val = DataCSVUpload.row_value(row, key, self.all_keys)
        if val is None:
            return None
        if isinstance(val, (int, float)) and val == 0:
            return None
        s = str(val).strip()
        if s == "0":
            return None
        return val

    def _clean_additional_data(self, row) -> dict:
        """
        Build cleaned dict for additional_data
        """
        cleaned = {}
        is_by_park_norm = {}

        for key in row.keys():
            if isinstance(key, str) and key.startswith('_') and isinstance(row, dict):
                meta_val = row.get(key)
                if meta_val is None:
                    continue
                try:
                    json.dumps(meta_val)
                    cleaned[key] = meta_val
                except TypeError:
                    cleaned[key] = str(meta_val)
                continue

            val = self.get_row_value(row, key)
            if val is None:
                continue

            if isinstance(key, str):
                m = IS_FIELD_RE.match(key)
                if m:
                    park_label_raw = re.sub(r'\s+', ' ',
                                            m.group(1).replace('_', ' ').strip())
                    if park_label_raw:
                        is_by_park_norm[_norm_park_label(park_label_raw)] = val

            if isinstance(val, str):
                s = re.sub(r'\s+', ' ', val).strip()
                if not s:
                    continue
                cleaned[key] = s
                continue

            if isinstance(val, (int, float, bool)):
                cleaned[key] = val
                continue

            try:
                json.dumps(val)
                cleaned[key] = val
            except TypeError:
                cleaned[key] = str(val)

        if is_by_park_norm:
            cleaned['invasion_status_by_park'] = is_by_park_norm

        return cleaned

    def endemism(self, row):
        """Processing endemism data (case-insensitive, avoids dupes)."""
        endemism_value = _safe_strip(self.get_row_value(row, ENDEMISM))
        if not endemism_value:
            return None
        try:
            endemism_obj, _ = Endemism.objects.get_or_create(
                name__iexact=endemism_value,
                defaults={'name': endemism_value}
            )
        except Endemism.MultipleObjectsReturned:
            endemism_obj = Endemism.objects.filter(
                name__iexact=endemism_value
            ).order_by('id').first()
        return endemism_obj

    def conservation_status(self, row, global_cons: bool = False):
        """Processing conservation status with graceful fallbacks."""
        national = False
        if global_cons:
            cons_status = self.get_row_value(row, CONSERVATION_STATUS) or self.get_row_value(row, CONSERVATION_STATUS_GLOBAL)
            if not cons_status:
                return None
        else:
            cons_status = self.get_row_value(row, CONSERVATION_STATUS_NATIONAL)
            national = True

        if not cons_status:
            return None

        key = str(cons_status).strip().lower()
        category = IUCN_CATEGORIES.get(key)
        if not category:
            return None

        try:
            iucn_status, _ = IUCNStatus.objects.get_or_create(
                category=category,
                national=national
            )
        except IUCNStatus.MultipleObjectsReturned:
            iucn_status = IUCNStatus.objects.filter(
                category=category,
                national=national
            ).order_by('id').first()
        return iucn_status

    def source_reference(self, row):
        source_reference_value = _safe_strip(self.get_row_value(row, REFERENCES))
        if not source_reference_value:
            return '', None

        source_reference = None

        # DOI?
        doi_pattern = r'^10\.\d{4,9}/[-._;()/:A-Z0-9]+$'
        is_doi = re.match(doi_pattern, source_reference_value, re.IGNORECASE) is not None
        if is_doi:
            entry = get_or_create_data_from_model(
                model=Entry,
                fields={'doi': source_reference_value},
                create=False
            )
            if not entry:
                doi_loader = DOILoader()
                try:
                    doi_loader.load_records(DOIs=[source_reference_value])
                    doi_loader.save_records()
                    entry = get_or_create_data_from_model(
                        Entry,
                        {'doi__iexact': source_reference_value},
                        create=False
                    )
                except (DOILoaderError, requests.exceptions.HTTPError) as e:
                    logger.warning('DOI fetch failed for %s: %s', source_reference_value, e)
                finally:
                    if not entry:
                        return f'Error fetching DOI: {source_reference_value}', None

            if entry and not source_reference:
                SourceReference.create_source_reference(
                    category='bibliography',
                    source_id=entry.id,
                    note=None
                )
                try:
                    source_reference, _ = SourceReferenceBibliography.objects.get_or_create(source=entry)
                except SourceReferenceBibliography.MultipleObjectsReturned:
                    source_reference = SourceReferenceBibliography.objects.filter(source=entry).order_by('id').first()
        else:
            # Unpublished string note
            source_reference = SourceReference.create_source_reference(
                category=None,
                source_id=None,
                note=source_reference_value
            )
        return '', source_reference

    def common_name(self, row):
        """Extract and process common names of a species row; never raises on duplicates."""
        common_name_value = self.get_row_value(row, COMMON_NAME)
        vernacular_lang = None
        if not common_name_value:
            common_name_value = self.get_row_value(row, VERNACULAR_NAME)
            vernacular_lang = self.get_row_value(row, VERNACULAR_NAME_LANG)
        else:
            vernacular_lang = 'eng'

        if not common_name_value:
            return None

        else:
            pass

        vernacular_names = []
        raw_str = str(common_name_value)
        parts = raw_str.split(';') if ';' in raw_str else raw_str.split(',')
        for raw in parts:
            raw = raw.strip()
            if not raw:
                continue
            m = re.match(r'^(.*?)(?:\s*\(([\w-]+)\))?$', raw)
            if not m:
                continue

            name = re.sub(r'\s+', ' ', (m.group(1) or '').strip())
            language = (m.group(2) or vernacular_lang or '').strip() or None
            vn = self._get_or_create_vernacular_singleton(name=name, language=language)
            vernacular_names.append(vn)

        return vernacular_names or None

    def origin(self, row):
        """
        Parse origin + invasion status.
        Returns: (TaxonOrigin instance | None, invasion_instance | None)
        """
        origin_value = _safe_strip(self.get_row_value(row, ORIGIN))
        invasion_category = _safe_strip(self.get_row_value(row, INVASION))
        if not invasion_category:
            invastion_category = _safe_strip(
                self.get_row_value(row, preferences.SiteSetting.invasion_label)
            )
        invasion_instance = None

        if invasion_category:
            invasion_instance, _ = Invasion.objects.get_or_create(category=invasion_category)

        if not origin_value:
            return None, invasion_instance

        if 'invasive' in origin_value.lower() and not invasion_category:
            invasion_instance, _ = Invasion.objects.get_or_create(category=origin_value)
            key = ORIGIN_CATEGORIES.get('non-native', '')
        else:
            key = ORIGIN_CATEGORIES.get(origin_value.lower(), '')

        if not key:
            return None, invasion_instance

        origin_instance = TaxonOrigin.objects.filter(origin_key=key).first()
        return origin_instance, invasion_instance

    def _specific_epithet(self, name: str) -> str:
        """Return specific epithet (2nd token) from a binomial/trinomial string."""
        if not name:
            return ''
        parts = _canon(name).split()
        return parts[1] if len(parts) >= 2 else ''

    def validate_parents(self, taxon, row):
        """
        Validate and (if needed) rebuild the parent chain using CSV hints.
        Works for Taxonomy and (optionally) proposal-like objects that have a 'parent' FK.
        """
        if taxon is None:
            return False, 'Missing taxon'

        if not hasattr(taxon, 'parent'):
            return False, "Object doesn't have a 'parent' field"

        rank_name = self.rank_name(taxon)
        if not rank_name or rank_name not in RANK_INDEX:
            return False, f"Rank '{rank_name or 'UNKNOWN'}' not recognized in the hierarchy."

        max_try = 15
        rank_index = RANK_INDEX[rank_name]

        cursor = taxon

        while max_try > 0 and rank_index > 0:
            max_try -= 1
            parent_rank_name = RANK_HIERARCHY[rank_index - 1]
            csv_parent_name = self.get_row_value(
                row, RANK_TITLE.get(parent_rank_name.upper(), parent_rank_name.capitalize())
            )

            if parent_rank_name == 'SPECIES' and csv_parent_name:
                genus_name = _safe_strip(self.get_row_value(row, 'Genus'))
                if genus_name and genus_name.lower() not in str(csv_parent_name).lower():
                    csv_parent_name = f'{genus_name} {csv_parent_name}'.strip()

            current_parent = getattr(cursor, 'parent', None)

            if current_parent:
                name_mismatch = (
                    current_parent.canonical_name.lower() != str(csv_parent_name or '').lower()
                )
                rank_mismatch = _safe_upper(current_parent.rank) != parent_rank_name
                if name_mismatch or rank_mismatch:
                    correct_parent = self.get_parent(row, parent_rank_name)
                    if correct_parent:
                        if hasattr(cursor, 'pk') and hasattr(correct_parent, 'pk') and cursor.pk and correct_parent.pk == cursor.pk:
                            return False, f"Circular parent reference detected for '{getattr(cursor, 'canonical_name', 'unknown')}'"
                        # Allow sub-ranks to have same name as parent non-sub rank
                        if hasattr(cursor, 'canonical_name') and hasattr(correct_parent, 'canonical_name') and \
                           hasattr(cursor, 'rank') and hasattr(correct_parent, 'rank') and \
                           _safe_strip(cursor.canonical_name).lower() == _safe_strip(correct_parent.canonical_name).lower():
                            cursor_rank = _safe_upper(cursor.rank) if cursor.rank else ''
                            parent_rank = _safe_upper(correct_parent.rank) if correct_parent.rank else ''
                            is_sub_to_nonsub = cursor_rank.startswith('SUB') and cursor_rank[3:] == parent_rank
                            if not is_sub_to_nonsub and cursor_rank != parent_rank:
                                return False, f"Parent '{correct_parent.canonical_name}' ({parent_rank}) cannot have the same name as '{cursor.canonical_name}' ({cursor_rank})"
                        if hasattr(cursor, 'parent'):
                            cursor.parent = correct_parent
                            try:
                                cursor.save(update_fields=['parent'])
                            except Exception:
                                cursor.save()
                        cursor = correct_parent
                    else:
                        logger.warning("Cannot resolve parent '%s' (%s) for '%s'",
                                       csv_parent_name, parent_rank_name,
                                       getattr(cursor, 'canonical_name', 'unknown'))
                        rank_index -= 1
                        continue
                else:
                    cursor = current_parent
            else:
                correct_parent = self.get_parent(row, parent_rank_name)
                if correct_parent:
                    if hasattr(cursor, 'pk') and hasattr(correct_parent, 'pk') and cursor.pk and correct_parent.pk == cursor.pk:
                        return False, f"Circular parent reference detected for '{getattr(cursor, 'canonical_name', 'unknown')}'"
                    # Allow sub-ranks to have same name as parent non-sub rank
                    if hasattr(cursor, 'canonical_name') and hasattr(correct_parent, 'canonical_name') and \
                       hasattr(cursor, 'rank') and hasattr(correct_parent, 'rank') and \
                       _safe_strip(cursor.canonical_name).lower() == _safe_strip(correct_parent.canonical_name).lower():
                        cursor_rank = _safe_upper(cursor.rank) if cursor.rank else ''
                        parent_rank = _safe_upper(correct_parent.rank) if correct_parent.rank else ''
                        is_sub_to_nonsub = cursor_rank.startswith('SUB') and cursor_rank[3:] == parent_rank
                        if not is_sub_to_nonsub and cursor_rank != parent_rank:
                            return False, f"Parent '{correct_parent.canonical_name}' ({parent_rank}) cannot have the same name as '{cursor.canonical_name}' ({cursor_rank})"
                    if hasattr(cursor, 'parent'):
                        cursor.parent = correct_parent
                        try:
                            cursor.save(update_fields=['parent'])
                        except Exception:
                            cursor.save()
                    cursor = correct_parent
                else:
                    logger.warning("Cannot resolve parent '%s' (%s) for '%s'",
                                   csv_parent_name, parent_rank_name,
                                   getattr(cursor, 'canonical_name', 'unknown'))

            rank_index -= 1

        logger.debug("Parent validation finished for '%s'.",
                     getattr(taxon, 'canonical_name', 'unknown'))
        return True, ''

    def get_taxonomy(self, taxon_name, scientific_name, rank, row=None):
        """
        Find or create a taxonomy by canonical name + rank, with GBIF fallback.
        """
        taxon_data = Taxonomy.objects.filter(
            canonical_name__iexact=taxon_name,
            rank=rank
        )
        if not taxon_data.exists():
            classifiers = {}
            if row:
                csv_genus = _safe_strip(self.get_row_value(row, GENUS))
                if csv_genus:
                    classifiers['genus'] = csv_genus

                for classifier_rank in ['kingdom', 'phylum', 'class', 'order', 'family']:
                    csv_value = _safe_strip(self.get_row_value(row, classifier_rank.capitalize()))
                    if csv_value:
                        if classifier_rank == 'class':
                            classifiers['class_name'] = csv_value
                        else:
                            classifiers[classifier_rank] = csv_value

            taxon = fetch_all_species_from_gbif(
                species=taxon_name,
                taxonomic_rank=rank,
                fetch_children=False,
                fetch_vernacular_names=False,
                use_name_lookup=True if classifiers else False,
                preserve_taxonomic_status=is_fada_site(),
                **classifiers
            )
            if taxon:
                if (
                        taxon.rank == rank.upper() and
                        taxon.canonical_name and
                        taxon.canonical_name.lower() == taxon_name.lower()
                ):
                    if taxon_name.lower() not in (taxon.scientific_name or '').lower():
                        taxon.scientific_name = scientific_name
                        taxon.legacy_canonical_name = taxon_name
                        taxon.canonical_name = taxon_name
                        taxon.gbif_key = None
                        taxon.gbif_data = {}
                    taxon.save()
                else:
                    taxon = None
            else:
                taxon, _ = Taxonomy.objects.get_or_create(
                    canonical_name=taxon_name,
                    scientific_name=scientific_name,
                    legacy_canonical_name=taxon_name,
                    rank=rank.upper()
                )
        else:
            taxon = taxon_data.first()
        return taxon

    def get_parent(self, row, current_rank=GENUS):
        if not current_rank:
            return None

        taxon_name = self.get_row_value(row, current_rank)
        if not taxon_name:
            return None
        taxon_name = str(taxon_name).strip()

        # SPECIES: ensure Genus + epithet
        if current_rank == SPECIES:
            genus_name = _safe_strip(self.get_row_value(row, GENUS))
            if genus_name and genus_name.lower() not in taxon_name.lower():
                taxon_name = f'{genus_name} {taxon_name}'

        if current_rank == VARIETY:
            genus_name = _safe_strip(self.get_row_value(row, GENUS))
            species_name = _safe_strip(self.get_row_value(row, SPECIES))
            if species_name and species_name.lower() not in taxon_name.lower():
                taxon_name = f'{species_name} {taxon_name}'
            if genus_name and genus_name.lower() not in taxon_name.lower():
                taxon_name = f'{genus_name} {taxon_name}'

        taxon = self.get_taxonomy(
            taxon_name,
            taxon_name,
            _safe_upper(current_rank),
            row=row
        )

        # If we already have a parent or reached KINGDOM, stop.
        if taxon.parent or _safe_upper(taxon.rank) == 'KINGDOM':
            return taxon

        # Try to climb by using CSV hints
        climb_guard = 0
        rank_cursor = current_rank
        while climb_guard < 10:
            climb_guard += 1
            parent_rank_name = parent_rank(rank_cursor)
            if not parent_rank_name:
                break
            parent = self.get_parent(row, parent_rank_name)
            if parent:
                # Check for literal circular reference
                if taxon.pk and parent.pk == taxon.pk:
                    logger.error(f"Circular parent reference detected for '{taxon.canonical_name}' - cannot be its own parent")
                    break
                # Allow sub-ranks to have same name as parent non-sub rank
                if _safe_strip(taxon.canonical_name).lower() == _safe_strip(parent.canonical_name).lower():
                    taxon_rank = _safe_upper(taxon.rank) if taxon.rank else ''
                    parent_rank_val = _safe_upper(parent.rank) if parent.rank else ''
                    is_sub_to_nonsub = taxon_rank.startswith('SUB') and taxon_rank[3:] == parent_rank_val
                    if not is_sub_to_nonsub and taxon_rank != parent_rank_val:
                        logger.error(f"Parent '{parent.canonical_name}' ({parent_rank_val}) cannot have the same name as '{taxon.canonical_name}' ({taxon_rank})")
                        break
                taxon.parent = parent
                taxon.save()
                break
            rank_cursor = parent_rank_name

        return taxon

    def synonym_key(self, field_key):
        return f'{SYNONYM} {field_key}'

    def process_data(self, row, taxon_group: TaxonGroup, harvest_synonyms: bool = False):
        """Process a single CSV row."""
        if not self.all_keys:
            for key in row.keys():
                self.all_keys[key.upper()] = key

        auto_validate = preferences.SiteSetting.auto_validate_taxa_on_upload
        use_proposal = not auto_validate

        taxonomic_status = _safe_strip(self.get_row_value(row, TAXONOMIC_STATUS))
        is_synonym = 'synonym' in (taxonomic_status or '').lower().strip()
        is_doubtful = 'doubtful' in (taxonomic_status or '').lower().strip()

        taxon_name = _safe_strip(self.get_row_value(row, TAXON))
        csv_taxon = taxon_name

        should_fetch_from_gbif = True

        accepted_taxon = None

        # Rank
        rank = self.get_row_value(row, TAXON_RANK)
        rank = RANK_TITLE.get(_safe_upper(rank), str(rank).capitalize() if rank else '')
        if rank.startswith('Sub'):
            rank = 'Sub' + rank[len('sub'):].capitalize()

        if not rank:
            rank = self.get_row_value(row, 'Taxon rank')
        if not rank:
            self.handle_error(row=row, message='Missing taxon rank')
            return

        subgenus = ''
        is_species = 'species' in rank.lower()
        if is_species:
            subgenus = _safe_strip(self.get_row_value(row, SUBGENUS))

        # Subgenus species are not fetched by name from GBIF (GBIF doesn't
        # distinguish subgenus in name lookups), but synonyms still need GBIF
        # to resolve acceptedKey, so allow fetching for them.
        if is_species and subgenus and not (is_synonym or is_doubtful):
            should_fetch_from_gbif = False

        # on_gbif flag
        raw_on_gbif = self.get_row_value(row, ON_GBIF)
        if raw_on_gbif is None:
            on_gbif = False if is_fada_site() else True
        else:
            on_gbif = 'yes' in raw_on_gbif.lower()

        if not taxon_name:
            if 'species' in str(rank).lower():
                taxon_name = self._compose_species_name(row)
            else:
                taxon_name = _safe_strip(
                    self.get_row_value(
                        row, RANK_TITLE.get(_safe_upper(rank), str(rank).capitalize())))

        if rank == SUBSPECIES:
            sub_species_name = _safe_strip(self.get_row_value(row, SUBSPECIES))
            if sub_species_name and taxon_name not in sub_species_name and not sub_species_name[0].isupper():
                taxon_name = f'{taxon_name} {sub_species_name}'

        if csv_taxon:
            taxon_name = self._choose_taxon_display_name(
                row=row,
                rank=rank,
                taxonomic_status=taxonomic_status,
                csv_taxon=csv_taxon,
                composed_taxon=taxon_name
            )

        if not taxon_name:
            self.handle_error(row=row, message='Missing Taxon value')
            return

        # GBIF key
        gbif_key = None
        if on_gbif:
            gbif_link = self.get_row_value(row, GBIF_LINK) or self.get_row_value(row, GBIF_URL)
            if gbif_link:
                last = str(gbif_link).rstrip('/').split('/')[-1]
                gbif_key = last[:-2] if last.endswith('.0') else last

        # For FADA sites, check if a species with this GBIF key already exists
        # If it has a different rank and taxon name, remove the GBIF key from input
        if gbif_key and is_fada_site():
            try:
                existing_taxonomy = Taxonomy.objects.filter(gbif_key=int(gbif_key)).first()
                if existing_taxonomy:
                    existing_rank = _safe_upper(existing_taxonomy.rank) if existing_taxonomy.rank else ''
                    input_rank = _safe_upper(rank) if rank else ''
                    existing_name = _canon(existing_taxonomy.canonical_name) if existing_taxonomy.canonical_name else ''
                    input_name = _canon(taxon_name) if taxon_name else ''
                    if existing_rank != input_rank and existing_name != input_name:
                        logger.info(
                            "FADA: GBIF key %s already exists with different rank (%s vs %s) "
                            "and name (%s vs %s); removing GBIF key from input",
                            gbif_key, existing_rank, input_rank,
                            existing_taxonomy.canonical_name, taxon_name
                        )
                        gbif_key = None
            except (ValueError, TypeError):
                # gbif_key is not a valid integer, will be handled later
                pass

        accepted_genus_mismatch = False

        if gbif_key:
            try:
                gbif_rec = get_species(gbif_key)

                if gbif_rec and _safe_upper(gbif_rec.get("rank")) == "UNRANKED":
                    parent_key = gbif_rec.get("parentKey")
                    if parent_key:
                        logger.info(
                            "GBIF key %s is UNRANKED; switching to parentKey %s", gbif_key, parent_key)
                        gbif_key = str(parent_key)
                        gbif_rec = get_species(parent_key)

            except Exception as e:
                self.handle_error(
                    row=row,
                    message=f"GBIF lookup failed for key {gbif_key}")
                return

            if not gbif_rec or not isinstance(gbif_rec, dict) or not gbif_rec.get("key"):
                self.handle_error(
                    row=row,
                    message=f"GBIF record not found or invalid for key {gbif_key}; ignoring provided key.")
                return
            else:
                expected_rank = _safe_upper(rank)
                gbif_rank = _safe_upper(gbif_rec.get("rank"))
                # For FADA sites, skip rank mismatch validation
                if expected_rank and gbif_rank and gbif_rank != expected_rank and not is_fada_site():
                    self.handle_error(
                        row=row,
                        message=(
                            f'GBIF key {gbif_key} rank mismatch: expected {expected_rank}, '
                            f'got {gbif_rank}.'
                        )
                    )
                    return
                else:
                    if expected_rank.lower() == 'species' and 'synonym' not in (taxonomic_status or '').lower():
                        expected_name = self._compose_species_name(row)
                    else:
                        expected_name = taxon_name

                    gbif_canonical = gbif_rec.get("canonicalName") or gbif_rec.get("scientificName") or ""
                    canon_expected = _canon(expected_name) if expected_name else ""
                    canon_gbif = _canon(gbif_canonical) if gbif_canonical else ""

                    norm_expected = _norm_taxon_for_similarity(expected_name) if expected_name else ""
                    norm_gbif = _norm_taxon_for_similarity(gbif_canonical) if gbif_canonical else ""

                    ratio = 1.0 if not norm_expected or not norm_gbif else max(
                        difflib.SequenceMatcher(None, canon_expected, canon_gbif).ratio(),
                        difflib.SequenceMatcher(None, norm_expected, norm_gbif).ratio(),
                    )

                    csv_genus = _safe_strip(self.get_row_value(row, GENUS)) or ""
                    gbif_genus = _safe_strip(gbif_rec.get("genus")) or ""

                    # Handle genus mismatch more gracefully if the specific epithet matches
                    # For FADA sites, keep csv_genus even if there's a mismatch
                    genus_mismatch = (
                        rank.lower() != 'genus' and csv_genus and gbif_genus and csv_genus.lower() != gbif_genus.lower()
                    )
                    if genus_mismatch and not is_fada_site():
                        csv_ep = self._specific_epithet(expected_name) or self._specific_epithet(taxon_name) or _canon(_safe_strip(self.get_row_value(row, SPECIES)))
                        gbif_ep = _canon(_safe_strip(gbif_rec.get("specificEpithet"))) or self._specific_epithet(gbif_canonical)
                        if csv_ep and gbif_ep and csv_ep == gbif_ep:
                            accepted_genus_mismatch = True
                            if isinstance(row, dict):
                                row["_gbif_genus_mismatch"] = {
                                    "csv_genus": csv_genus,
                                    "gbif_genus": gbif_genus,
                                    "gbif_key": gbif_key,
                                    "gbif_canonical": gbif_canonical,
                                }
                        else:
                            self.handle_error(
                                row=row,
                                message=(
                                    f"GBIF key {gbif_key}: genus mismatch '{csv_genus}' vs '{gbif_genus}' "
                                    f"and epithet differs; cannot safely reconcile."
                                ),
                            )
                            return

                    # For FADA sites, skip name similarity validation
                    if ratio < NAME_SIM_THRESHOLD and not is_fada_site():
                        self.handle_error(
                            row=row,
                            message=(
                                f"GBIF key {gbif_key}: name mismatch (similarity={ratio:.2f} < {NAME_SIM_THRESHOLD}). "
                                f"Expected '{expected_name}'; GBIF '{gbif_canonical}'."
                            ),
                        )
                        return

        # FADA id (integer part)
        fada_id = self.get_row_value(row, FADA_ID)

        if is_synonym or is_doubtful:
            accepted_taxon_val = self.get_row_value(row, ACCEPTED_TAXON)
            if accepted_taxon_val:
                accepted_taxon = Taxonomy.objects.filter(
                    canonical_name__iexact=accepted_taxon_val
                ).first()
                if not accepted_taxon:
                    self.handle_error(row=row, message=f'Accepted taxon not found: {accepted_taxon_val}')
                    return

        authors = _safe_strip(self.get_row_value(row, AUTHORS))

        species_group = self.get_row_value(row, SPECIES_GROUP)
        if species_group:
            species_group, _ = SpeciesGroup.objects.get_or_create(name=species_group)

        # Scientific name + authorship
        if SCIENTIFIC_NAME in row:
            scientific_name = _safe_strip(self.get_row_value(row, SCIENTIFIC_NAME) or taxon_name)
        else:
            scientific_name = taxon_name

        if authors and authors not in scientific_name:
            scientific_name = f'{scientific_name} {authors}'.strip()

        # Parent check: parent must not have same name as the taxon
        # Exception: sub-ranks can have the same name as their parent non-sub rank
        if not is_synonym and not is_doubtful:
            parent = self.get_parent(row, parent_rank(rank))
            if parent:
                current_rank_upper = _safe_upper(rank)
                parent_rank_upper = _safe_upper(parent.rank) if parent.rank else ''

                # Check if parent and current taxon have the same name and rank
                if _safe_strip(parent.canonical_name) == taxon_name and current_rank_upper == parent_rank_upper:
                    self.handle_error(
                        row=row,
                        message='Taxonomy cannot be its own parent (same name and rank)')
                    return

                # Check if parent has same name but different rank (allowed for sub-ranks)
                if _safe_strip(parent.canonical_name) == taxon_name:
                    is_sub_to_nonsub = (
                        current_rank_upper.startswith('SUB') and
                        current_rank_upper[3:] == parent_rank_upper
                    )
                    if not is_sub_to_nonsub:
                        self.handle_error(
                            row=row,
                            message='Parent cannot have the same name as the taxon')
                        return
        else:
            parent = None

        # Resolve existing taxa (by gbif, fada, or canonical)
        taxa = Taxonomy.objects.none()
        taxa_found_by_id = False

        if gbif_key:
            taxa = Taxonomy.objects.filter(gbif_key=gbif_key)
        if not taxa and fada_id:
            taxa = Taxonomy.objects.filter(fada_id=fada_id)
            taxa_found_by_id = taxa.exists()
        if not taxa:
            taxa = Taxonomy.objects.filter(canonical_name__iexact=taxon_name)
            # Homonymy guard: taxa can share a canonical name but belong to
            # different authors (e.g. Epeorus soldani (Braasch, 1979) vs.
            # Epeorus soldani Nguyen & Bae, 2004).
            # Or have different subgenus
            if taxa.exists() and authors:
                stripped_authors = authors.strip('()').strip()
                taxa_by_author = taxa.filter(author__iexact=authors)
                if not taxa_by_author.exists():
                    taxa_by_author = taxa.filter(author__iexact=stripped_authors)
                if taxa_by_author.exists():
                    # Exact author match, use this record.
                    taxa = taxa_by_author
                    logger.debug(
                        'Homonymy resolved for %r: matched by author %r',
                        taxon_name, authors,
                    )
                else:
                    logger.info(
                        'Homonymy: %r (author %r) does not match any existing '
                        'record — will create a new taxon.',
                        taxon_name, authors,
                    )
                    taxa = Taxonomy.objects.none()

        proposal = None
        new_taxon = False

        try:
            taxonomy = None

            common_name_objs = self.common_name(row)
            should_fetch_vernacular_names = common_name_objs is None

            if taxa.exists():
                if is_species and subgenus and not taxa_found_by_id:
                    taxa_with_subgenus = taxa.filter(
                        subgenus__canonical_name__iexact=subgenus
                    )
                    if taxa_with_subgenus.exists():
                        taxa = taxa_with_subgenus
                    elif is_synonym or is_doubtful:
                        # A synonym may share a canonical name with an accepted taxon
                        # of a different subgenus. Prefer existing records that have
                        # no subgenus set (previously imported synonyms lacking this
                        # data) over wrongly matching a different-subgenus accepted taxon.
                        taxa = taxa.filter(subgenus__isnull=True)
                    else:
                        taxa = taxa_with_subgenus  # empty — new taxon will be created
                taxa_same_rank = taxa.filter(rank=_safe_upper(rank))

                if taxa_same_rank.exists():
                    candidate = taxa_same_rank.first()
                    candidate_author = (candidate.author or '').strip('()').strip().lower()
                    csv_author = authors.strip('()').strip().lower() if authors else ''
                    author_conflict = (
                        csv_author and candidate_author and
                        candidate_author != csv_author
                    )
                    candidate_status = (candidate.taxonomic_status or '').strip().upper()
                    csv_status = (taxonomic_status or '').strip().upper()
                    status_conflict = (
                        csv_status and candidate_status and
                        candidate_status != csv_status
                    )
                    if author_conflict or (csv_author and status_conflict and not candidate_author):
                        logger.info(
                            'Homonymy at assignment: %r candidate has '
                            'author=%r status=%r but CSV has author=%r '
                            'status=%r — will create a new taxon.',
                            taxon_name, candidate.author, candidate_status,
                            authors, csv_status,
                        )
                    else:
                        taxonomy = candidate
                        logger.debug('%s already in the system', taxon_name)

                        if (is_synonym or is_doubtful) and taxonomy.parent:
                            taxonomy.parent = None
                elif taxa.exists():
                    taxonomy = taxa.first()
                    if fada_id and taxonomy.fada_id != fada_id:
                        # New taxon
                        taxonomy = None
                        logger.debug(
                            '%s exists with different rank, treating as new taxon',
                            taxon_name)
                    else:
                        taxonomy.rank = _safe_upper(rank)

            if not taxonomy and gbif_key and should_fetch_from_gbif:
                taxonomy = fetch_all_species_from_gbif(
                    gbif_key=gbif_key,
                    taxonomic_rank=rank,
                    fetch_vernacular_names=should_fetch_vernacular_names,
                    is_synonym=is_synonym,
                    preserve_taxonomic_status=is_fada_site(),
                )
                if taxonomy:
                    new_taxon = True
                    # Ensure synonyms and doubtful species don't have parents
                    if (is_synonym or is_doubtful) and taxonomy.parent:
                        taxonomy.parent = None

            if not taxonomy and on_gbif and should_fetch_from_gbif:
                # Build classifiers from CSV to ensure we match the correct taxon
                classifiers = {}
                csv_genus = _safe_strip(self.get_row_value(row, GENUS))
                if csv_genus:
                    classifiers['genus'] = csv_genus

                # Add other taxonomic classifiers if available
                for classifier_rank in ['kingdom', 'phylum', 'class', 'order', 'family']:
                    csv_value = _safe_strip(self.get_row_value(row, classifier_rank.capitalize()))
                    if csv_value:
                        if classifier_rank == 'class':
                            classifiers['class_name'] = csv_value
                        else:
                            classifiers[classifier_rank] = csv_value

                taxonomy = fetch_all_species_from_gbif(
                    species=taxon_name,
                    taxonomic_rank=rank,
                    fetch_children=False,
                    fetch_vernacular_names=should_fetch_vernacular_names,
                    use_name_lookup=True,
                    is_synonym=is_synonym,
                    preserve_taxonomic_status=is_fada_site(),
                    **classifiers
                )
                if taxonomy:
                    new_taxon = True
                    # Ensure synonyms and doubtful species don't have parents
                    if (is_synonym or is_doubtful) and taxonomy.parent:
                        taxonomy.parent = None

            # Ensure we have a taxonomy; if GBIF didn't return, construct one with a valid parent
            if not taxonomy:
                # Climb until we find a CSV-provided ancestor
                max_try = 10
                current_try = 0
                parent_name = parent_rank(rank)
                while not self.get_row_value(row, parent_name) and parent_name != KINGDOM and current_try < max_try:
                    current_try += 1
                    parent_name = parent_rank(parent_name)

                if not is_synonym and not is_doubtful:
                    parent = self.get_parent(row, parent_name)
                    if not parent:
                        self.handle_error(row=row, message='Data not found from GBIF for this taxon and its parents')
                        return
                else:
                    # Synonyms and doubtful species should not have a parent
                    parent = None
                new_taxon = True
                taxonomy, _ = Taxonomy.objects.get_or_create(
                    scientific_name=scientific_name,
                    canonical_name=taxon_name,
                    rank=TaxonomicRank[_safe_upper(rank)].name,
                    parent=parent,
                    taxonomic_status=taxonomic_status.upper()
                )

            if is_species and subgenus:
                subgenus_taxon = Taxonomy.objects.filter(
                    canonical_name=subgenus,
                    rank=TaxonomicRank.SUBGENUS.name
                )
                if not subgenus_taxon.exists():
                    # Create one
                    genus = _safe_strip(self.get_row_value(row, GENUS))
                    genus_obj = Taxonomy.objects.none()
                    if genus:
                        genus_obj = Taxonomy.objects.filter(
                            canonical_name=genus,
                            rank=TaxonomicRank.GENUS.name,
                        ).first()
                    subgenus_taxon, _ = Taxonomy.objects.get_or_create(
                        scientific_name=subgenus,
                        canonical_name=subgenus,
                        rank=TaxonomicRank.SUBGENUS.name,
                        parent=genus_obj
                    )
                else:
                    subgenus_taxon = subgenus_taxon.first()
                taxonomy.subgenus = subgenus_taxon

            # Backfill parent and author if missing (skip for synonyms and doubtful species)
            if taxonomy and not taxonomy.parent and not is_synonym and not is_doubtful:
                # Ensure taxonomy is not its own parent (check by pk if both are saved)
                if parent and taxonomy.pk and parent.pk == taxonomy.pk:
                    self.handle_error(row=row, message='Taxonomy cannot be its own parent')
                    return
                # Allow sub-ranks to have same name as parent non-sub rank (e.g., subgenus can have same name as genus)
                if parent and _safe_strip(taxonomy.canonical_name).lower() == _safe_strip(parent.canonical_name).lower():
                    taxonomy_rank = _safe_upper(taxonomy.rank) if taxonomy.rank else ''
                    parent_rank_val = _safe_upper(parent.rank) if parent.rank else ''
                    is_sub_to_nonsub = taxonomy_rank.startswith('SUB') and taxonomy_rank[3:] == parent_rank_val
                    if not is_sub_to_nonsub and taxonomy_rank != parent_rank_val:
                        self.handle_error(row=row, message=f"Parent '{parent.canonical_name}' ({parent_rank_val}) "
                                                           f"cannot have the same name as taxonomy '{taxonomy.canonical_name}' "
                                                           f"({taxonomy_rank})")
                        return
                taxonomy.parent = parent

            if on_gbif and taxonomy and not _safe_strip(getattr(taxonomy, 'author', '')):
                refreshed = fetch_all_species_from_gbif(
                    species=taxon_name,
                    taxonomic_rank=rank,
                    fetch_children=False,
                    fetch_vernacular_names=should_fetch_vernacular_names,
                    use_name_lookup=False,
                    is_synonym=is_synonym,
                    preserve_taxonomic_status=is_fada_site(),
                )
                taxonomy = refreshed or taxonomy

                # Ensure synonyms and doubtful species don't have parents after refresh
                if (is_synonym or is_doubtful) and taxonomy.parent:
                    taxonomy.parent = None

                # For FADA sites, ensure CSV taxonomic_status is always preserved after GBIF refresh
                if is_fada_site() and taxonomic_status and taxonomy.taxonomic_status != taxonomic_status.upper():
                    taxonomy.taxonomic_status = taxonomic_status.upper()
                    taxonomy.save()

            # Create proposal only once taxonomy exists and we know we need it
            if use_proposal:
                proposal = create_taxon_proposal(taxonomy, taxon_group)

            # Merge legacy canonical names
            legacy_canonical_name = (taxonomy.legacy_canonical_name or '').replace('\\xa0', '')
            if FORMER_SPECIES_NAME in row:
                former_species_name = _safe_strip(self.get_row_value(row, FORMER_SPECIES_NAME))
                if former_species_name:
                    former_species_name = former_species_name[:500]
                    if former_species_name not in legacy_canonical_name:
                        legacy_canonical_name = f'{legacy_canonical_name};{former_species_name}' if legacy_canonical_name else former_species_name

            self._update_taxon_and_proposal(
                taxonomy, proposal, use_proposal, new_taxon,
                'legacy_canonical_name', legacy_canonical_name[:700]
            )

            # Validate parents for accepted taxon
            if not is_synonym and not is_doubtful:
                ok, message = self.validate_parents(taxon=taxonomy, row=row)
                if not ok:
                    self.handle_error(row=row, message=message)
                    return

            # Only set proposal parent for accepted taxa (not synonyms or doubtful species)
            if use_proposal and proposal and not is_synonym and not is_doubtful:
                # Ensure proposal is not its own parent
                if taxonomy.parent and proposal.pk and taxonomy.parent.pk == proposal.pk:
                    self.handle_error(row=row, message='Proposal cannot be its own parent')
                    return
                # Allow sub-ranks to have same name as parent non-sub rank (e.g., subgenus can have same name as genus)
                if taxonomy.parent and hasattr(proposal, 'canonical_name') and hasattr(taxonomy.parent, 'canonical_name'):
                    if _safe_strip(proposal.canonical_name).lower() == _safe_strip(taxonomy.parent.canonical_name).lower():
                        proposal_rank = _safe_upper(proposal.rank) if hasattr(proposal, 'rank') and proposal.rank else ''
                        parent_rank_val = _safe_upper(taxonomy.parent.rank) if taxonomy.parent.rank else ''
                        is_sub_to_nonsub = proposal_rank.startswith('SUB') and proposal_rank[3:] == parent_rank_val
                        if not is_sub_to_nonsub and proposal_rank != parent_rank_val:
                            self.handle_error(row=row, message=f"Parent '{taxonomy.parent.canonical_name}' ({parent_rank_val}) cannot have the same name as proposal '{proposal.canonical_name}' ({proposal_rank})")
                            return
                proposal.parent = taxonomy.parent

            # Endemism
            endemism_obj = self.endemism(row)
            if endemism_obj:
                self._update_taxon_and_proposal(taxonomy, proposal, use_proposal, new_taxon, 'endemism', endemism_obj)

            # Conservation (global + national)
            iucn_status = self.conservation_status(row, True)
            if iucn_status:
                self._update_taxon_and_proposal(taxonomy, proposal, use_proposal, new_taxon, 'iucn_status', iucn_status)

            national_cons_status = self.conservation_status(row, False)
            if national_cons_status:
                self._update_taxon_and_proposal(taxonomy, proposal, use_proposal, new_taxon, 'national_conservation_status', national_cons_status)

            # References
            msg, reference = self.source_reference(row)
            if msg and not reference:
                self.handle_error(row=row, message=msg)
                return
            if reference:
                self._update_taxon_and_proposal(taxonomy, proposal, use_proposal, new_taxon, 'source_reference', reference)

            # Common names
            if common_name_objs:
                if new_taxon or not use_proposal:
                    taxonomy.vernacular_names.clear()
                    for vn in common_name_objs:
                        taxonomy.vernacular_names.add(vn)
                if use_proposal and proposal:
                    proposal.vernacular_names.clear()
                    for vn in common_name_objs:
                        proposal.vernacular_names.add(vn)
            else:
                if taxonomy and taxonomy.gbif_key:
                    if new_taxon or not use_proposal:
                        fetch_gbif_vernacular_names(taxonomy)
                    if use_proposal and proposal:
                        fetch_gbif_vernacular_names(proposal)

            # Origin / invasion
            origin_data, invasion = self.origin(row)
            if origin_data:
                self._update_taxon_and_proposal(taxonomy, proposal, use_proposal, new_taxon, 'origin', origin_data)
            if invasion:
                self._update_taxon_and_proposal(taxonomy, proposal, use_proposal, new_taxon, 'invasion', invasion)

            # SpeciesGroup
            if species_group:
                self._update_taxon_and_proposal(taxonomy, proposal, use_proposal, new_taxon, 'species_group', species_group)

            if fada_id:
                # fada_id is an identifier and must always be applied directly to
                # the taxonomy, even when proposals are enabled and the taxon is
                # not yet validated.
                taxonomy.fada_id = fada_id
                if use_proposal and proposal is not None:
                    proposal.fada_id = fada_id

            if gbif_key:
                self._update_taxon_and_proposal(taxonomy, proposal, use_proposal, new_taxon, 'gbif_key', gbif_key)

            # Tags + biographic distributions
            if new_taxon or not use_proposal:
                taxonomy.tags.clear()
                taxonomy.biographic_distributions.clear()
            if use_proposal and proposal:
                proposal.tags.clear()
                proposal.biographic_distributions.clear()

            for key in row:
                row_value = self.get_row_value(row, key)
                val_norm = _safe_strip(row_value)
                if val_norm and (val_norm == '?' or _as_truthy(val_norm)):
                    tag_label = _norm_key_label(key)
                    if any(tag_label.lower() == bd.lower() for bd in BIOGRAPHIC_DISTRIBUTIONS):
                        doubtful = (val_norm == '?')
                        try:
                            taxon_tag, _ = TaxonTag.objects.get_or_create(
                                name=tag_label, doubtful=doubtful
                            )
                        except TaxonTag.MultipleObjectsReturned:
                            taxon_tag = TaxonTag.objects.filter(
                                name=tag_label, doubtful=doubtful
                            ).order_by('id').first() or TaxonTag.objects.filter(
                                name=tag_label
                            ).order_by('id').first()
                        if new_taxon or not use_proposal:
                            taxonomy.biographic_distributions.add(taxon_tag)
                        if use_proposal and proposal:
                            proposal.biographic_distributions.add(taxon_tag)
                    elif val_norm != '?':
                        if new_taxon or not use_proposal:
                            taxonomy.tags.add(tag_label)
                        if use_proposal and proposal:
                            proposal.tags.add(tag_label)

            cleaned_additional_data = self._clean_additional_data(row)
            self._update_taxon_and_proposal(
                taxonomy, proposal, use_proposal, new_taxon, 'additional_data', cleaned_additional_data)

            if taxonomy.canonical_name != taxon_name:
                if accepted_genus_mismatch:
                    legacy = (taxonomy.legacy_canonical_name or '').replace('\\xa0', '')
                    if taxon_name and taxon_name not in legacy:
                        legacy = f'{legacy};{taxon_name}' if legacy else taxon_name
                    self._update_taxon_and_proposal(
                        taxonomy, proposal, use_proposal, new_taxon,
                        'legacy_canonical_name', legacy[:700]
                    )
                else:
                    self._update_taxon_and_proposal(
                        taxonomy, proposal, use_proposal, new_taxon, 'canonical_name', taxon_name
                    )

            if taxonomic_status:
                self._update_taxon_and_proposal(taxonomy, proposal, use_proposal, new_taxon, 'taxonomic_status', taxonomic_status.strip().upper())

            # Refresh gbif_data for existing taxa, cached data may be outdated.
            if not new_taxon and taxonomy.gbif_key:
                fresh_data = get_species(taxonomy.gbif_key)
                if fresh_data:
                    taxonomy.gbif_data = fresh_data
                    taxonomy.save(update_fields=['gbif_data'])

            if (is_synonym or is_doubtful) and not accepted_taxon:
                accepted_key = (taxonomy.gbif_data or {}).get('acceptedKey', '')
                if accepted_key:
                    accepted_taxon = Taxonomy.objects.filter(gbif_key=accepted_key).first()
                if accepted_key and not accepted_taxon:
                    accepted_taxon = fetch_all_species_from_gbif(
                        gbif_key=accepted_key,
                        fetch_children=False,
                        is_synonym=False,
                        fetch_vernacular_names=False,
                        use_name_lookup=False,
                        preserve_taxonomic_status=False
                    )

            if accepted_taxon and accepted_taxon.accepted_taxonomy:
                accepted_taxon.accepted_taxonomy = None
                accepted_taxon.save(update_fields=['accepted_taxonomy'])

            if accepted_taxon:
                self._update_taxon_and_proposal(taxonomy, proposal, use_proposal, new_taxon, 'accepted_taxonomy', accepted_taxon)

            # Author(s)
            if authors:
                if new_taxon:
                    taxonomy.author = authors
                    if use_proposal and proposal:
                        proposal.author = authors
                    base_sciname = taxonomy.canonical_name or taxon_name
                    base_sciname = re.sub(r'\s*\([^)]*\)\s*$', '', base_sciname).strip()
                    normalized_sciname = f'{base_sciname} {authors}'.strip()
                    taxonomy.scientific_name = normalized_sciname
                    if use_proposal and proposal:
                        proposal.scientific_name = normalized_sciname
                else:
                    if use_proposal and proposal:
                        proposal.author = authors
                        base_sciname = (taxonomy.scientific_name or taxon_name)
                        base_sciname = re.sub(r'\s*\([^)]*\)\s*$', '', base_sciname).strip()
                        proposal.scientific_name = f'{base_sciname} {authors}'.strip()

            taxonomy.save()
            if proposal:
                proposal.save()

            # Taxon group linking (respects auto-validate in subclass)
            self.add_taxon_to_taxon_group_unvalidated(taxonomy, taxon_group)

            if not is_synonym and harvest_synonyms:
                try:
                    syn_taxa = harvest_synonyms_for_accepted_taxonomy(
                        taxonomy,
                        fetch_vernacular_names=True,
                        accept_language=None,
                    ) or []
                    for syn in syn_taxa:
                        self.add_taxon_to_taxon_group_unvalidated(
                            syn, taxon_group
                        )
                except Exception as syn_exc:
                    logger.exception(
                        f"Error harvesting synonyms for {taxonomy.gbif_key}: {syn_exc}"
                    )

            if is_synonym and taxonomy.accepted_taxonomy and not is_fada_site():
                if use_proposal:
                    create_taxon_proposal(taxonomy.accepted_taxonomy, taxon_group)
                self.add_taxon_to_taxon_group_unvalidated(
                    taxonomy.accepted_taxonomy, taxon_group
                )

            self.finish_processing_row(row, taxonomy)

        except Exception as e:  # noqa
            logger.exception('Error processing row for "%s": %s', taxon_name, e)
            self.handle_error(row, str(e))


class TaxaCSVUpload(DataCSVUpload, TaxaProcessor):
    model_name = 'taxonomy'

    def add_taxon_to_taxon_group_unvalidated(self, taxonomy, taxon_group, proposal=None):
        """
        Respect site preference: auto_validate_taxa_on_upload
        """
        auto_validate = preferences.SiteSetting.auto_validate_taxa_on_upload
        use_proposal = not auto_validate
        self.add_taxon_to_taxon_group(taxonomy, taxon_group, validated=auto_validate, use_proposal=use_proposal)

    def finish_processing_row(self, row, taxonomy):
        # Add to taxon group if missing
        taxon_group = self.upload_session.module_group
        if not taxon_group.taxonomies.filter(id=taxonomy.id).exists():
            taxon_group.taxonomies.add(taxonomy)

        # Import date
        taxonomy.import_date = self.upload_session.uploaded_at.date()
        taxonomy.save()

        self.success_file(row, taxonomy.id)

    def handle_error(self, row, message):
        self.error_file(error_row=row, error_message=message)

    def process_row(self, row):
        taxon_group = self.upload_session.module_group
        harvest_synonyms = self.upload_session.harvest_synonyms
        with transaction.atomic():
            self.process_data(row, taxon_group, harvest_synonyms)
