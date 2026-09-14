# bims/scripts/taxa_upload_worms.py
import json
import logging
import re

from django.db import transaction
from preferences import preferences
from taggit.models import Tag

from bims.scripts.data_upload import DataCSVUpload
from bims.scripts.species_keys import *  # noqa
from bims.models import (
    Taxonomy,
)
from .taxa_upload import TaxaProcessor
from bims.utils.fetch_gbif import harvest_synonyms_for_accepted_taxonomy

logger = logging.getLogger("bims")


def _try_set_gbif_key(taxonomy) -> bool:
    """
    Look up *taxonomy* in the GBIF backbone by canonical name and, if a
    match is found, store the key and raw data on the taxon.
    Returns True when a key was written.
    """
    from bims.utils.gbif import search_exact_match, get_species
    try:
        key = search_exact_match(taxonomy.canonical_name)
        if key:
            detail = get_species(key)
            if detail:
                taxonomy.gbif_key = key
                taxonomy.gbif_data = detail
                taxonomy.save(update_fields=["gbif_key", "gbif_data"])
                return True
    except Exception as exc:
        logger.debug(
            "GBIF lookup failed for %s: %s",
            taxonomy.canonical_name, exc)
    return False


HTML_TAG_RE = re.compile(r"<[^>]+>")
def _strip_html(s: str | None) -> str:
    if not s:
        return ""
    return HTML_TAG_RE.sub("", s).strip()

WORMS_COLUMN_NAMES = {
    "aphia_id": "AphiaID",
    "sci_name": "ScientificName",
    "authority": "Authority",
    "aphia_id_acc": "AphiaID_accepted",
    "sci_name_acc": "ScientificName_accepted",
    "authority_acc": "Authority_accepted",
    "kingdom": "Kingdom",
    "phylum": "Phylum",
    "clazz": "Class",
    "order": "Order",
    "family": "Family",
    "genus": "Genus",
    "subgenus": "Subgenus",
    "species": "Species",
    "subspecies": "Subspecies",
    "rank": "taxonRank",
    "marine": "Marine",
    "brackish": "Brackish",
    "fresh": "Fresh",
    "terrestrial": "Terrestrial",
    "status": "taxonomicStatus",
    "quality": "Qualitystatus",
    "unaccept_reason": "Unacceptreason",
    "date_modified": "DateLastModified",
    "lsid": "LSID",
    "parent_aphia": "Parent AphiaID",
    "path": "Storedpath",
    "citation": "Citation",
}


class WormsTaxaProcessor(TaxaProcessor):
    """Processor for WoRMS-formatted CSV rows."""

    def fetch_accepted_row(self, accepted_aphia_id: int):
        """
        Return the WoRMS CSV-format row dict for *accepted_aphia_id*, or None.
        """
        return None

    @staticmethod
    def row_value(row, key, all_keys=None):
        """
        Safe field accessor compatible with DataCSVUpload.row_value.
        """
        val = row.get(key, '')
        if not isinstance(val, str):
            val = '' if val is None else str(val)
        return val.strip()

    RANK_MAP = {
        "kingdom": "KINGDOM",
        "subkingdom": "SUBKINGDOM",
        "phylum": "PHYLUM",
        "subphylum": "SUBPHYLUM",
        "infraphylum": "INFRAPHYLUM",
        "megaclass": "MEGACLASS",
        "gigaclass": "GIGACLASS",
        "superclass": "SUPERCLASS",
        "class": "CLASS",
        "subclass": "SUBCLASS",
        "infraclass": "INFRACLASS",
        "superorder": "SUPERORDER",
        "order": "ORDER",
        "suborder": "SUBORDER",
        "infraorder": "INFRAORDER",
        "parvorder": "PARVORDER",
        "superfamily": "SUPERFAMILY",
        "family": "FAMILY",
        "subfamily": "SUBFAMILY",
        "tribe": "TRIBE",
        "subtribe": "SUBTRIBE",
        "genus": "GENUS",
        "subgenus": "SUBGENUS",
        "species": "SPECIES",
        "subspecies": "SUBSPECIES",
        "variety": "VARIETY",
        "forma": "FORMA",
    }

    STATUS_MAP = {
        "accepted": "ACCEPTED",
        "nomen novum": "ACCEPTED",
        "nomen protectum": "ACCEPTED",
        "unreplaced junior homonym": "ACCEPTED",

        "junior homonym": "SYNONYM",
        "junior objective synonym": "SYNONYM",
        "junior subjective synonym": "SYNONYM",
        "senior objective synonym": "SYNONYM",
        "senior subjective synonym": "SYNONYM",
        "misspelling - incorrect original spelling": "SYNONYM",
        "misspelling - incorrect subsequent spelling": "SYNONYM",
        "misspellings - incorrect original spelling": "SYNONYM",
        "misspellings - incorrect subsequent spelling": "SYNONYM",
        "nomen nudum": "SYNONYM",
        "nomen oblitum": "SYNONYM",
        "superseded combination": "SYNONYM",
        "superseded rank": "SYNONYM",
        "unjustified emendation": "SYNONYM",
        "incorrect grammatical agreement of specific epithet": "SYNONYM",
        "alternative representation": "SYNONYM",
        "alternative representation: strongly to be avoided": "SYNONYM",
        "misapplication": "SYNONYM",

        "nomen dubium": "DOUBTFUL",
        "taxon inquirendum": "DOUBTFUL",
        "unassessed": "DOUBTFUL",

        "unaccepted": "UNACCEPTED",
        "unavailable name": "UNACCEPTED",
        "interim unpublished": "UNACCEPTED",
        "temporary name": "UNACCEPTED",
        "nomen rejiciendum": "UNACCEPTED",
    }

    SKIP_STATUSES = set()

    HABITAT_TAGS = [
        ("Marine", "marine"),
        ("Brackish", "brackish"),
        ("Fresh", "freshwater"),
        ("Terrestrial", "terrestrial"),
    ]

    def _boolish(self, v):
        """WoRMS habitat flags come as 1/0/''."""
        if v is None:
            return False
        s = str(v).strip().lower()
        return s in {"1", "true", "yes", "y"}

    def _map_rank(self, worms_rank: str | None) -> str | None:
        if not worms_rank:
            return None
        return self.RANK_MAP.get(worms_rank.strip().lower())

    def _compose_worms_scientific(self, row: dict, target_rank: str) -> tuple[str, str]:
        """Return (canonical_name, scientific_name) for the row."""
        sci = (self.row_value(row, WORMS_COLUMN_NAMES["sci_name"]) or "").strip()
        auth = _strip_html(row.get(WORMS_COLUMN_NAMES["authority"]))
        genus = (row.get(WORMS_COLUMN_NAMES["genus"]) or "").strip()
        species = (row.get(WORMS_COLUMN_NAMES["species"]) or "").strip()
        subspecies = (row.get(WORMS_COLUMN_NAMES["subspecies"]) or "").strip()

        # If rank is species/subspecies WoRMS often splits epithet; normalise
        if target_rank in {"SPECIES", "SUBSPECIES"}:
            base = sci or " ".join([p for p in [genus, species] if p])
            if target_rank == "SUBSPECIES" and subspecies:
                if subspecies not in base:
                    base = f"{base} {subspecies}"
            canonical = base
        else:
            canonical = sci or genus or (row.get(WORMS_COLUMN_NAMES["family"]) or "").strip()

        scientific = canonical
        # Append author if not already present
        if auth and auth not in scientific:
            scientific = f"{scientific} {auth}"

        return canonical, scientific

    def _lineage_species_name(self, row: dict) -> str | None:
        """
        Return the species lineage name as a full binomial.
        WoRMS often stores only the epithet in the Species column.
        """
        genus = (row.get(WORMS_COLUMN_NAMES["genus"]) or "").strip()
        species = (row.get(WORMS_COLUMN_NAMES["species"]) or "").strip()
        if not species:
            return None
        if genus and not species.lower().startswith(f"{genus.lower()} "):
            return f"{genus} {species}".strip()
        return species

    def _resolve_taxonomy(self, canonical_name: str, rank: str,
                          aphia_id: int | None = None) -> Taxonomy | None:
        """
        Resolve an existing taxonomy, preferring stable WoRMS identity first.
        """
        if aphia_id:
            taxonomy = Taxonomy.objects.filter(aphia_id=aphia_id).first()
            if taxonomy:
                return taxonomy

        taxonomy = Taxonomy.objects.filter(
            canonical_name__iexact=canonical_name,
            rank=rank,
        ).first()
        if taxonomy:
            return taxonomy

        matches = Taxonomy.objects.filter(canonical_name__iexact=canonical_name)
        if matches.count() == 1:
            return matches.first()
        return None

    def _ensure_parent_chain(self, row: dict, for_rank: str) -> Taxonomy | None:
        """
        Build/find parents up to immediate parent of `for_rank`.
        Returns the immediate parent Taxonomy or None.
        """
        _none = None
        lineage = [
            ("KINGDOM",     row.get(WORMS_COLUMN_NAMES["kingdom"])),
            ("SUBKINGDOM",  _none),
            ("PHYLUM",      row.get(WORMS_COLUMN_NAMES["phylum"])),
            ("SUBPHYLUM",   _none),
            ("INFRAPHYLUM", _none),
            ("MEGACLASS",   _none),
            ("GIGACLASS",   _none),
            ("SUPERCLASS",  _none),
            ("CLASS",       row.get(WORMS_COLUMN_NAMES["clazz"])),
            ("SUBCLASS",    _none),
            ("INFRACLASS",  _none),
            ("SUPERORDER",  _none),
            ("ORDER",       row.get(WORMS_COLUMN_NAMES["order"])),
            ("SUBORDER",    _none),
            ("INFRAORDER",  _none),
            ("PARVORDER",   _none),
            ("SUPERFAMILY", _none),
            ("FAMILY",      row.get(WORMS_COLUMN_NAMES["family"])),
            ("SUBFAMILY",   _none),
            ("TRIBE",       _none),
            ("SUBTRIBE",    _none),
            ("GENUS",       row.get(WORMS_COLUMN_NAMES["genus"])),
            ("SUBGENUS",    row.get(WORMS_COLUMN_NAMES["subgenus"])),
            ("SPECIES",     self._lineage_species_name(row)),
            ("SUBSPECIES",  row.get(WORMS_COLUMN_NAMES["subspecies"])),
            ("VARIETY",     _none),
            ("FORMA",       _none),
        ]
        idx = {r: i for i, (r, _) in enumerate(lineage)}
        if for_rank not in idx:
            return None
        stop_at = idx[for_rank] - 1
        if stop_at < 0:
            return None

        parent = None
        for i in range(stop_at + 1):
            rank, name = lineage[i]
            if not name:
                continue
            name = str(name).strip()
            t = Taxonomy.objects.filter(
                canonical_name__iexact=name,
                rank=rank
            ).first()
            if not t:
                t = Taxonomy.objects.create(
                    canonical_name=name,
                    scientific_name=name,
                    legacy_canonical_name=name,
                    rank=rank,
                    parent=parent
                )
            else:
                if t.parent_id != (parent.id if parent else None):
                    t.parent = parent
                    t.save()
            parent = t

        return parent

    def _attach_habitat_tags(self, taxonomy: Taxonomy, row: dict, is_new: bool):
        """Turn habitat flags into tags.

        Only applied on the first harvest of a taxon. On re-harvest the tags
        are left untouched so experts can edit them freely.
        """
        if not is_new:
            return
        for col, tag_label in self.HABITAT_TAGS:
            val = row.get(col) if col in row else row.get(WORMS_COLUMN_NAMES[col.lower()])
            if self._boolish(val):
                tag, _ = Tag.objects.get_or_create(name=tag_label)
                taxonomy.tags.add(tag)

    def _maybe_add_aquatic_tag(self, taxonomy: Taxonomy, row: dict, is_new: bool):
        """Add 'aquatic' tag only on first harvest of a freshwater taxon.

        On re-harvest the tag is left untouched so experts can edit it freely.
        """
        if not is_new:
            return
        fresh_val = row.get('Fresh') if 'Fresh' in row else row.get(WORMS_COLUMN_NAMES['fresh'])
        if self._boolish(fresh_val):
            tag, _ = Tag.objects.get_or_create(name='aquatic')
            taxonomy.tags.add(tag)

    def _attach_citation(self, taxonomy: Taxonomy, row: dict):
        citation = _strip_html(row.get(WORMS_COLUMN_NAMES["citation"]))
        if not citation:
            return
        additional_data = taxonomy.additional_data or {}
        additional_data["Taxonomic References"] = citation
        taxonomy.additional_data = additional_data

    def process_worms_data(self, row: dict, taxon_group, harvest_synonyms: bool = False,
                           fetch_gbif_key: bool = False):
        """
        Process a single WoRMS row into Taxonomy.

        Parameters
        ----------
        fetch_gbif_key : bool
            When True, attempt a GBIF name-match lookup after saving the
            taxon and store the result in gbif_key / gbif_data if the taxon
            does not already have a GBIF key.
        """
        status_raw = (row.get(WORMS_COLUMN_NAMES["status"]) or "").strip()
        status_key = status_raw.lower().replace("–", "-").replace("—", "-")
        if status_key in self.SKIP_STATUSES:
            logger.debug("Skipping AphiaID=%s: status %r", row.get(WORMS_COLUMN_NAMES["aphia_id"]), status_raw)
            return

        worms_rank = row.get(WORMS_COLUMN_NAMES["rank"])
        rank = self._map_rank(worms_rank)
        if not rank:
            self.handle_error(row, f"Unsupported/empty taxonRank: {worms_rank}")
            return
        taxonomic_status = self.STATUS_MAP.get(status_key, status_raw.upper() or None)

        is_accepted = status_raw.lower() == "accepted"
        accepted_name = (row.get(WORMS_COLUMN_NAMES["sci_name_acc"]) or "").strip()
        accepted_auth = _strip_html(row.get(WORMS_COLUMN_NAMES["authority_acc"]))
        accepted_full = f"{accepted_name} {accepted_auth}".strip() if accepted_name else ""

        canonical_name, scientific_name = self._compose_worms_scientific(row, rank)

        parent = self._ensure_parent_chain(row, rank)
        if parent and parent.canonical_name.lower() == canonical_name.lower():
            self.handle_error(row, "Parent cannot have the same name as the taxon")
            return

        aphia_id_int = None
        aphia_id_val = row.get(WORMS_COLUMN_NAMES["aphia_id"])
        if aphia_id_val is not None:
            try:
                aphia_id_int = int(aphia_id_val)
            except (ValueError, TypeError):
                pass

        existing = None
        if aphia_id_int:
            existing = Taxonomy.objects.filter(aphia_id=aphia_id_int).first()
        if not existing:
            existing = Taxonomy.objects.filter(
                canonical_name__iexact=canonical_name
            ).first()
        is_new = existing is None
        if is_new:
            taxonomy = Taxonomy.objects.create(
                canonical_name=canonical_name,
                scientific_name=scientific_name,
                legacy_canonical_name=canonical_name,
                rank=rank,
                parent=parent
            )
        else:
            taxonomy = existing
            taxonomy.canonical_name = canonical_name
            taxonomy.scientific_name = scientific_name
            taxonomy.legacy_canonical_name = canonical_name
            taxonomy.rank = rank
            if taxonomy.parent_id != (parent.id if parent else None):
                taxonomy.parent = parent

        authority = _strip_html(row.get(WORMS_COLUMN_NAMES["authority"]))
        if authority:
            taxonomy.author = authority

        is_synonym = False

        if taxonomic_status:
            taxonomy.taxonomic_status = taxonomic_status
            if 'synonym' in taxonomic_status.lower():
                is_synonym = True

        if not is_accepted and accepted_name:
            acc = Taxonomy.objects.filter(
                canonical_name__iexact=accepted_name
            ).first()

            accepted_parent = None
            acc_rank = rank
            accepted_aphia_id_val = row.get(WORMS_COLUMN_NAMES["aphia_id_acc"])
            if accepted_aphia_id_val:
                try:
                    accepted_row = self.fetch_accepted_row(int(accepted_aphia_id_val))
                    if accepted_row:
                        acc_rank_raw = accepted_row.get(WORMS_COLUMN_NAMES["rank"], "")
                        acc_rank = self._map_rank(acc_rank_raw) or rank
                        accepted_parent = self._ensure_parent_chain(accepted_row, acc_rank)
                except Exception as exc:
                    logger.debug(
                        "Could not resolve rank/parent for accepted AphiaID=%s: %s",
                        accepted_aphia_id_val, exc,
                    )

            if not acc:
                acc = Taxonomy.objects.create(
                    canonical_name=accepted_name,
                    scientific_name=accepted_full or accepted_name,
                    legacy_canonical_name=accepted_name,
                    rank=acc_rank,
                    parent=accepted_parent,
                )
            else:
                update_fields = []
                if acc_rank and acc.rank != acc_rank:
                    acc.rank = acc_rank
                    update_fields.append("rank")
                if accepted_parent and acc.parent_id != accepted_parent.id:
                    if not (
                        acc.canonical_name.strip().lower()
                        == accepted_parent.canonical_name.strip().lower()
                        and acc.rank == accepted_parent.rank
                    ):
                        acc.parent = accepted_parent
                        update_fields.append("parent")
                if update_fields:
                    acc.save(update_fields=update_fields)

            taxonomy.accepted_taxonomy = acc

        self._attach_habitat_tags(taxonomy, row, is_new)
        self._maybe_add_aquatic_tag(taxonomy, row, is_new)

        if aphia_id_int is not None:
            taxonomy.aphia_id = aphia_id_int

        extras = dict(row)
        taxonomy.additional_data = extras
        self._attach_citation(taxonomy, row)

        taxonomy.save()

        if fetch_gbif_key and not taxonomy.gbif_key:
            _try_set_gbif_key(taxonomy)

        auto_validate = preferences.SiteSetting.auto_validate_taxa_on_upload
        self.add_taxon_to_taxon_group(taxonomy, taxon_group, validated=auto_validate)

        if harvest_synonyms and not is_synonym:
            try:
                syn_taxa = harvest_synonyms_for_accepted_taxonomy(
                    taxonomy,
                    fetch_vernacular_names=True,
                    accept_language=None,
                ) or []
                for syn in syn_taxa:
                    self.add_taxon_to_taxon_group(
                        syn, taxon_group, validated=auto_validate
                    )
            except Exception as syn_exc:
                logger.exception(
                    f"Error harvesting synonyms for {taxonomy.gbif_key}: {syn_exc}"
                )

        self.finish_processing_row(row, taxonomy)


class WormsTaxaCSVUpload(DataCSVUpload, WormsTaxaProcessor):
    """
    CSV uploader that expects WoRMS columns and feeds them through WormsTaxaProcessor.
    Reuses success/error handling from your existing TaxaCSVUpload.
    """
    model_name = "taxonomy"

    def handle_error(self, row, message):
        self.error_file(error_row=row, error_message=message)

    def finish_processing_row(self, row, taxonomy):
        taxon_group = self.upload_session.module_group
        if not taxon_group.taxonomies.filter(id=taxonomy.id).exists():
            taxon_group.taxonomies.add(taxonomy)
        taxonomy.import_date = self.upload_session.uploaded_at.date()
        taxonomy.save()
        self.success_file(row, taxonomy.id)

    def process_row(self, row):
        taxon_group = self.upload_session.module_group
        harvest_synonyms = self.upload_session.harvest_synonyms
        with transaction.atomic():
            self.process_worms_data(row, taxon_group, harvest_synonyms)
