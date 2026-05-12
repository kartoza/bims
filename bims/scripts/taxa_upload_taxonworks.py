# coding=utf-8
from __future__ import annotations

import logging

from django.db import transaction
from preferences import preferences

from bims.models import Taxonomy
from bims.scripts.taxa_upload import TaxaProcessor
from bims.utils.fetch_gbif import harvest_synonyms_for_accepted_taxonomy
from bims.utils.taxonworks import get_taxon_name, taxonworks_record_to_additional_data

logger = logging.getLogger("bims")

GBIF_LINEAGE_RANKS = [
    ("KINGDOM", "kingdom", "kingdomKey"),
    ("PHYLUM", "phylum", "phylumKey"),
    ("CLASS", "class", "classKey"),
    ("ORDER", "order", "orderKey"),
    ("FAMILY", "family", "familyKey"),
    ("GENUS", "genus", "genusKey"),
    ("SPECIES", "species", "speciesKey"),
]
_GBIF_RANK_INDEX = {r: i for i, (r, _, _) in enumerate(GBIF_LINEAGE_RANKS)}


class TaxonWorksTaxaProcessor(TaxaProcessor):

    def __init__(self, base_url: str | None = None,
                 project_token: str | None = None,
                 records_by_id: dict[int, dict] | None = None):
        self.base_url = base_url
        self.project_token = project_token
        self.records_by_id = records_by_id or {}
        self.taxonomies_by_taxonworks_id: dict[int, Taxonomy] = {}

    def handle_error(self, row, message):
        pass

    def finish_processing_row(self, row, taxonomy):
        pass

    def _infer_rank(self, record: dict) -> str | None:
        cached = (record.get("cached") or "").strip()
        if not cached:
            return None
        token_count = len([token for token in cached.split() if token])
        if token_count >= 3:
            return "SUBSPECIES"
        if token_count == 2:
            return "SPECIES"
        if token_count == 1:
            valid_id = record.get("cached_valid_taxon_name_id")
            record_id = record.get("id")
            if valid_id and record_id and int(valid_id) != int(record_id):
                accepted_record = self._fetch_record(int(valid_id))
                accepted_rank = ((accepted_record or {}).get("rank") or "").strip().upper() or None
                if accepted_rank:
                    return accepted_rank
            parent_record = self._fetch_record(record.get("parent_id"))
            parent_rank = ((parent_record or {}).get("rank") or "").strip().upper() or None
            if parent_rank == "FAMILY":
                return "GENUS"
            if parent_rank == "GENUS":
                return "SPECIES"
        return None

    def _fetch_record(self, taxon_name_id: int | None) -> dict | None:
        if not taxon_name_id:
            return None
        taxon_name_id = int(taxon_name_id)
        if taxon_name_id in self.records_by_id:
            return self.records_by_id[taxon_name_id]
        if self.base_url and self.project_token:
            record = get_taxon_name(self.base_url, self.project_token, taxon_name_id)
            if record:
                self.records_by_id[taxon_name_id] = record
            return record
        return None

    def _resolve_taxonomy(self, record: dict, rank: str) -> Taxonomy | None:
        taxon_name_id = record.get("id")
        if taxon_name_id:
            taxonomy = self.taxonomies_by_taxonworks_id.get(int(taxon_name_id))
            if taxonomy:
                return taxonomy

        canonical_name = (record.get("cached") or record.get("name") or "").strip()
        if not canonical_name:
            return None

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

    def _walk_to_kingdom(self, taxonomy: Taxonomy, _depth: int = 0) -> bool:
        """Return True if *taxonomy* has a KINGDOM ancestor."""
        if _depth > 30 or taxonomy is None:
            return False
        if (taxonomy.rank or "").upper() == "KINGDOM":
            return True
        parent = taxonomy.parent
        if parent is None:
            return False
        return self._walk_to_kingdom(parent, _depth + 1)

    def _ensure_gbif_lineage(self, taxonomy: Taxonomy, fill_parents: bool = True) -> None:
        """
        Look up *taxonomy* in GBIF and store its gbif_key.

        When *fill_parents* is True (accepted taxa), also build the full
        parent chain up to Kingdom if it is missing.
        """
        if taxonomy.gbif_key and not fill_parents:
            return

        if fill_parents and self._walk_to_kingdom(taxonomy):
            return

        rank = (taxonomy.rank or "").upper()
        rank_idx = _GBIF_RANK_INDEX.get(rank)
        if rank_idx is None or rank_idx == 0:
            return

        from bims.utils.gbif import search_exact_match, get_species
        canonical_name = taxonomy.canonical_name
        try:
            gbif_key = search_exact_match(canonical_name)
            if not gbif_key:
                logger.debug("GBIF lineage: no match for '%s'", canonical_name)
                return
            gbif_data = get_species(gbif_key)
            if not gbif_data:
                return
        except Exception as exc:
            logger.debug("GBIF lineage lookup failed for '%s': %s", canonical_name, exc)
            return

        if not taxonomy.gbif_key:
            taxonomy.gbif_key = gbif_key
            taxonomy.save(update_fields=["gbif_key"])

        if not fill_parents:
            return

        parent = None
        for i in range(rank_idx):
            r, name_field, key_field = GBIF_LINEAGE_RANKS[i]
            name = (gbif_data.get(name_field) or "").strip()
            if not name:
                continue
            gbif_taxon_key = gbif_data.get(key_field)

            t: Taxonomy | None = None
            if gbif_taxon_key:
                t = Taxonomy.objects.filter(gbif_key=gbif_taxon_key).first()
            if not t:
                t = Taxonomy.objects.filter(
                    canonical_name__iexact=name, rank=r
                ).first()
            if not t:
                t = Taxonomy.objects.create(
                    canonical_name=name,
                    scientific_name=name,
                    legacy_canonical_name=name,
                    rank=r,
                    parent=parent,
                )
                if gbif_taxon_key:
                    t.gbif_key = gbif_taxon_key
                    t.save(update_fields=["gbif_key"])
            else:
                changed = False
                if t.parent_id != (parent.id if parent else None):
                    t.parent = parent
                    changed = True
                if gbif_taxon_key and not t.gbif_key:
                    t.gbif_key = gbif_taxon_key
                    changed = True
                if changed:
                    t.save()
            parent = t

        if parent and taxonomy.parent_id != parent.id:
            taxonomy.parent = parent
            taxonomy.save(update_fields=["parent"])

    _GENUS_TRANSPARENT = frozenset({"SUBGENUS", "SUPERSPECIES"})

    def _find_genus_ancestor(self, start: Taxonomy | None, _depth: int = 0) -> Taxonomy | None:
        """
        Walk up from *start*, passing through SUBGENUS and SUPERSPECIES
        transparently.  Return the first GENUS found, or None.
        """
        cursor = start
        while cursor and _depth < 10:
            rank = (cursor.rank or "").upper()
            if rank == "GENUS":
                return cursor
            if rank not in self._GENUS_TRANSPARENT:
                return None
            cursor = cursor.parent
            _depth += 1
        return None

    def _get_or_create_genus(self, genus_name: str,
                             above: Taxonomy | None = None) -> Taxonomy:
        """
        Return an existing GENUS taxonomy for *genus_name*, or create one.
        """
        genus = Taxonomy.objects.filter(
            canonical_name__iexact=genus_name, rank="GENUS"
        ).first()
        if genus:
            if above and not genus.parent_id:
                genus.parent = above
                genus.save(update_fields=["parent"])
            return genus

        from bims.utils.gbif import search_exact_match, get_species as gbif_get
        try:
            gbif_key = search_exact_match(genus_name)
            if gbif_key:
                gbif_data = gbif_get(gbif_key)
                if gbif_data and (gbif_data.get("rank") or "").upper() == "GENUS":
                    genus = Taxonomy.objects.filter(gbif_key=gbif_key).first()
                    if not genus:
                        genus = Taxonomy.objects.create(
                            canonical_name=(gbif_data.get("canonicalName") or genus_name),
                            scientific_name=(gbif_data.get("scientificName") or genus_name),
                            legacy_canonical_name=(gbif_data.get("canonicalName") or genus_name),
                            rank="GENUS",
                            gbif_key=gbif_key,
                            parent=above,
                        )
                    else:
                        if above and not genus.parent_id:
                            genus.parent = above
                            genus.save(update_fields=["parent"])
                    return genus
        except Exception as exc:
            logger.debug("GBIF genus lookup failed for %r: %s", genus_name, exc)

        return Taxonomy.objects.create(
            canonical_name=genus_name,
            scientific_name=genus_name,
            legacy_canonical_name=genus_name,
            rank="GENUS",
            parent=above,
        )

    def _validate_species_hierarchy(self, taxonomy: Taxonomy) -> None:
        """
        Enforce correct parent structure for SPECIES and SUBSPECIES.
        """
        rank = (taxonomy.rank or "").upper()

        if rank == "SPECIES":
            if self._find_genus_ancestor(taxonomy.parent):
                return

            parts = (taxonomy.canonical_name or "").split()
            if not parts:
                return
            genus_name = parts[0]
            genus = self._get_or_create_genus(genus_name, above=taxonomy.parent)
            if taxonomy.parent_id != genus.id:
                taxonomy.parent = genus
                taxonomy.save(update_fields=["parent"])

        elif rank == "SUBSPECIES":
            parent = taxonomy.parent
            if parent and (parent.rank or "").upper() == "SPECIES":
                self._validate_species_hierarchy(parent)
                return

            parts = (taxonomy.canonical_name or "").split()
            if len(parts) < 2:
                return
            species_name = " ".join(parts[:2])
            genus_name = parts[0]

            species = Taxonomy.objects.filter(
                canonical_name__iexact=species_name, rank="SPECIES"
            ).first()
            if not species:
                genus = self._get_or_create_genus(genus_name, above=taxonomy.parent)
                species = Taxonomy.objects.create(
                    canonical_name=species_name,
                    scientific_name=species_name,
                    legacy_canonical_name=species_name,
                    rank="SPECIES",
                    parent=genus,
                )
            else:
                self._validate_species_hierarchy(species)

            if taxonomy.parent_id != species.id:
                taxonomy.parent = species
                taxonomy.save(update_fields=["parent"])

    def _ensure_taxonomy(self, record: dict, _stack: set[int] | None = None) -> Taxonomy | None:
        record_id = int(record.get("id")) if record.get("id") else None
        _stack = _stack or set()
        if record_id and record_id in _stack:
            self.handle_error(record,
                              f"Cyclic TaxonWorks parent/accepted link for id={record_id}")
            return None
        if record_id:
            _stack = set(_stack)
            _stack.add(record_id)

        rank = (record.get("rank") or "").strip().upper() or self._infer_rank(record)
        if not rank:
            self.handle_error(
                record,
                f"Unsupported/empty rank: {record.get('rank')} (type={record.get('type')})"
            )
            return None

        canonical_name = (record.get("cached") or record.get("name") or "").strip()
        scientific_name = (record.get("name_string") or canonical_name).strip()
        if not canonical_name:
            self.handle_error(record, "Missing canonical name")
            return None

        parent = None
        parent_record = self._fetch_record(record.get("parent_id"))
        if parent_record and (parent_record.get("rank") or "").strip():
            parent = self._ensure_taxonomy(parent_record, _stack=_stack)

        if parent and parent.canonical_name.strip().lower() == canonical_name.lower():
            self.handle_error(record, "Parent cannot have the same name as the taxon")
            return None

        taxonomy = self._resolve_taxonomy(record, rank)
        if not taxonomy:
            taxonomy = Taxonomy.objects.create(
                canonical_name=canonical_name,
                scientific_name=scientific_name,
                legacy_canonical_name=canonical_name,
                rank=rank,
                parent=parent,
            )
        else:
            taxonomy.canonical_name = canonical_name
            taxonomy.scientific_name = scientific_name
            taxonomy.legacy_canonical_name = canonical_name
            taxonomy.rank = rank
            if taxonomy.parent_id != (parent.id if parent else None):
                taxonomy.parent = parent

        author = (record.get("cached_author_year") or record.get("cached_author") or "").strip()
        if author:
            taxonomy.author = author

        taxonomy.taxonomic_status = (
            "ACCEPTED" if record.get("cached_is_valid", True) else "SYNONYM"
        )

        accepted_taxonomy = None
        valid_id = record.get("cached_valid_taxon_name_id")
        if valid_id and int(valid_id) != int(record.get("id")):
            accepted_record = self._fetch_record(int(valid_id))
            if accepted_record:
                accepted_taxonomy = self._ensure_taxonomy(accepted_record, _stack=_stack)
        taxonomy.accepted_taxonomy = accepted_taxonomy

        taxonomy.additional_data = taxonworks_record_to_additional_data(record, self.base_url or "")
        taxonomy.save()

        is_synonym = taxonomy.taxonomic_status == "SYNONYM"
        self._ensure_gbif_lineage(taxonomy, fill_parents=not is_synonym)
        if not is_synonym:
            self._validate_species_hierarchy(taxonomy)

        if record_id:
            self.taxonomies_by_taxonworks_id[record_id] = taxonomy
        self.finish_processing_row(record, taxonomy)
        return taxonomy

    def process_taxonworks_record(self, record: dict, taxon_group,
                                  harvest_synonyms: bool = False) -> Taxonomy | None:
        taxonomy = self._ensure_taxonomy(record=record)
        if not taxonomy:
            return None

        auto_validate = preferences.SiteSetting.auto_validate_taxa_on_upload
        self.add_taxon_to_taxon_group(taxonomy, taxon_group, validated=auto_validate)

        if harvest_synonyms and taxonomy.taxonomic_status == "ACCEPTED":
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
                    "Error harvesting synonyms for %s: %s",
                    taxonomy.canonical_name, syn_exc,
                )
        return taxonomy


class SessionTaxonWorksTaxaProcessor:
    def __init__(self, log_fn, base_url: str, project_token: str,
                 records_by_id: dict[int, dict] | None = None):
        class _Processor(TaxonWorksTaxaProcessor):
            def handle_error(self_, row, message):  # noqa: N805
                log_fn(f"Row error TaxonWorksID={row.get('id')}: {message}")

            def finish_processing_row(self_, row, taxonomy):  # noqa: N805
                pass

        self._processor = _Processor(
            base_url=base_url,
            project_token=project_token,
            records_by_id=records_by_id,
        )

    def process(self, record: dict, taxon_group, harvest_synonyms: bool):
        with transaction.atomic():
            return self._processor.process_taxonworks_record(
                record,
                taxon_group,
                harvest_synonyms=harvest_synonyms,
            )
