# coding=utf-8
"""Celery task: harvest species from a remote BIMS instance."""
from __future__ import annotations

import logging
import re

from celery import shared_task
from django.utils import timezone
from django_tenants.utils import schema_context

logger = logging.getLogger(__name__)

# Matches "(#RRGGBB)" at the end of a tag label, e.g. "aquatic (#51FF3E)"
_TAG_COLOUR_RE = re.compile(r'\s*\(#([0-9A-Fa-f]{3,6})\)\s*$')


def _parse_tag_list(tag_list_str: str) -> list[tuple[str, str | None]]:
    """
    Parse a tag_list string from BIMS into (tag_name, colour_or_None) pairs.

    Examples
    --------
    "test"                                 → [("test", None)]
    "aquatic (#51FF3E), freshwater (#FF5733)"
                                           → [("aquatic", "#51FF3E"),
                                              ("freshwater", "#FF5733")]
    """
    results = []
    if not tag_list_str:
        return results
    for part in tag_list_str.split(','):
        part = part.strip()
        if not part:
            continue
        m = _TAG_COLOUR_RE.search(part)
        if m:
            colour = f'#{m.group(1).upper()}'
            name = part[:m.start()].strip()
        else:
            colour = None
            name = part
        if name:
            results.append((name, colour))
    return results


def _apply_tags(taxonomy, tag_list_str: str) -> None:
    """
    Parse tag_list_str and add tags to taxonomy.
    If a tag carries a colour, find or create a TagGroup with that colour
    and associate the tag with it.
    """
    from taggit.models import Tag
    from bims.models.tag_group import TagGroup

    parsed = _parse_tag_list(tag_list_str)
    for tag_name, colour in parsed:
        tag, _ = Tag.objects.get_or_create(name=tag_name)
        taxonomy.tags.add(tag_name)

        if colour:
            # Find a TagGroup that already owns this tag with the same colour,
            # or create one named after the tag (colour used as key).
            tag_group = TagGroup.objects.filter(
                colour__iexact=colour,
                tags=tag,
            ).first()
            if not tag_group:
                # Try to find any existing group with this colour to reuse it.
                tag_group = TagGroup.objects.filter(colour__iexact=colour).first()
            if not tag_group:
                tag_group = TagGroup.objects.create(name=tag_name, colour=colour)
            if not tag_group.tags.filter(id=tag.id).exists():
                tag_group.tags.add(tag)


def _find_or_create_taxonomy(taxon_data: dict, base_url: str,
                              remote_cache: dict) -> object | None:
    """
    Ensure a local Taxonomy record exists for *taxon_data* fetched from a
    remote BIMS instance.  Parents are resolved recursively.

    remote_cache maps remote_id → local Taxonomy (avoids redundant API calls
    and prevents infinite recursion).

    Matching priority
    -----------------
    1. gbif_key  (if present)
    2. canonical_name + rank

    If no match is found a minimal record is created.
    additional_data from the remote is merged into the local record.
    tag_list is applied via _apply_tags().
    """
    from bims.models.taxonomy import Taxonomy
    from bims.utils.bims_instance import get_taxon_by_id

    remote_id = taxon_data.get('id')
    if remote_id is not None:
        remote_id = int(remote_id)
        cached = remote_cache.get(remote_id)
        if cached is not None:
            return cached

    canonical_name = (taxon_data.get('canonical_name') or '').strip()
    rank = (taxon_data.get('rank') or '').strip().upper()
    scientific_name = (taxon_data.get('scientific_name') or canonical_name).strip()
    author = (taxon_data.get('author') or '').strip()
    taxonomic_status = (taxon_data.get('taxonomic_status') or 'ACCEPTED').strip().upper()
    gbif_key = taxon_data.get('gbif_key')
    if gbif_key:
        try:
            gbif_key = int(gbif_key)
        except (TypeError, ValueError):
            gbif_key = None

    if not canonical_name:
        return None

    # ------------------------------------------------------------------
    # Resolve parent first (depth-first, recursive)
    # ------------------------------------------------------------------
    parent_taxonomy = None
    remote_parent_id = taxon_data.get('parent')
    if remote_parent_id:
        remote_parent_id = int(remote_parent_id)
        if remote_parent_id in remote_cache:
            parent_taxonomy = remote_cache[remote_parent_id]
        else:
            parent_data = get_taxon_by_id(base_url, remote_parent_id)
            if parent_data:
                parent_taxonomy = _find_or_create_taxonomy(
                    parent_data, base_url, remote_cache
                )

    # ------------------------------------------------------------------
    # Find existing local record
    # ------------------------------------------------------------------
    taxonomy = None
    if gbif_key:
        taxonomy = Taxonomy.objects.filter(gbif_key=gbif_key).first()
    if taxonomy is None and rank:
        taxonomy = Taxonomy.objects.filter(
            canonical_name__iexact=canonical_name,
            rank=rank,
        ).first()

    # ------------------------------------------------------------------
    # Create if not found
    # ------------------------------------------------------------------
    if taxonomy is None:
        create_kwargs = {
            'canonical_name': canonical_name,
            'scientific_name': scientific_name or canonical_name,
            'rank': rank,
            'author': author,
            'taxonomic_status': taxonomic_status,
        }
        if gbif_key:
            create_kwargs['gbif_key'] = gbif_key
        if parent_taxonomy:
            create_kwargs['parent'] = parent_taxonomy
        taxonomy = Taxonomy.objects.create(**create_kwargs)
    else:
        # Update parent if it was missing locally
        if parent_taxonomy and not taxonomy.parent:
            taxonomy.parent = parent_taxonomy
            taxonomy.save(update_fields=['parent'])

    # ------------------------------------------------------------------
    # Merge additional_data
    # ------------------------------------------------------------------
    remote_additional = taxon_data.get('additional_data')
    if isinstance(remote_additional, dict) and remote_additional:
        existing = taxonomy.additional_data or {}
        merged = {**remote_additional, **existing}  # local values win on conflict
        if merged != taxonomy.additional_data:
            taxonomy.additional_data = merged
            taxonomy.save(update_fields=['additional_data'])

    # ------------------------------------------------------------------
    # Apply tags
    # ------------------------------------------------------------------
    tag_list_str = taxon_data.get('tag_list') or ''
    if tag_list_str:
        _apply_tags(taxonomy, tag_list_str)

    # Cache and return
    if remote_id is not None:
        remote_cache[remote_id] = taxonomy
    return taxonomy


@shared_task(name='bims.tasks.harvest_bims_species', queue='update', ignore_result=True)
def harvest_bims_species(session_id: int, schema_name: str):
    from bims.models import HarvestSession
    from bims.models.taxon_group import TaxonGroup
    from bims.enums.taxonomic_group_category import TaxonomicGroupCategory
    from bims.signals.utils import connect_bims_signals, disconnect_bims_signals
    from bims.utils.bims_instance import (
        get_all_taxa,
        get_taxon_groups,
        normalize_bims_base_url,
    )

    with schema_context(schema_name):
        try:
            session = HarvestSession.objects.get(id=session_id)
        except HarvestSession.DoesNotExist:
            logger.error("harvest_bims_species: session %s not found", session_id)
            return

        def _log(msg: str):
            ts = timezone.now().isoformat(timespec="seconds")
            line = f"[{ts}] {msg}\n"
            logger.info("BIMS harvest session=%s: %s", session_id, msg)
            if session.log_file:
                try:
                    with open(session.log_file.path, "a") as fh:
                        fh.write(line)
                except Exception:
                    pass

        disconnect_bims_signals()

        if not session.status or session.status == "queued":
            session.status = "Processing"
            session.save(update_fields=["status"])

        additional = session.additional_data or {}
        base_url = normalize_bims_base_url((additional.get("base_url") or "").strip())
        remote_group_id = additional.get("remote_group_id")
        remote_group_name = (additional.get("remote_group_name") or "").strip()
        import_mode = additional.get("import_mode", "existing")

        if not base_url or not remote_group_id:
            _log("Missing base_url or remote_group_id — aborting")
            HarvestSession.objects.filter(id=session_id).update(
                status="Failed: incomplete config",
                finished=True,
            )
            connect_bims_signals()
            return

        remote_group_id = int(remote_group_id)

        # ------------------------------------------------------------------
        # Resolve target taxon group
        # ------------------------------------------------------------------
        if import_mode == "new":
            group_name = remote_group_name or f"Imported from {base_url} (group {remote_group_id})"
            if not remote_group_name:
                for rg in get_taxon_groups(base_url):
                    if int(rg.get('id', 0)) == remote_group_id:
                        group_name = rg.get('name', group_name)
                        break

            from django.contrib.sites.models import Site
            target_group, created = TaxonGroup.objects.get_or_create(
                name=group_name,
                defaults={
                    'category': TaxonomicGroupCategory.SPECIES_MODULE.name,
                    'site': Site.objects.get_current(),
                },
            )
            action_word = "Created" if created else "Using existing"
            _log(f"{action_word} taxon group: '{group_name}' (id={target_group.id})")
            session.module_group = target_group
            session.save(update_fields=["module_group"])
        else:
            target_group = session.module_group
            if not target_group:
                _log("No local taxon group selected — aborting")
                HarvestSession.objects.filter(id=session_id).update(
                    status="Failed: no local taxon group",
                    finished=True,
                )
                connect_bims_signals()
                return

        _log(
            f"Starting BIMS harvest from {base_url} "
            f"(remote group id={remote_group_id}, name='{remote_group_name}') "
            f"→ local group '{target_group.name}'"
        )

        from preferences import preferences
        auto_validate = preferences.SiteSetting.auto_validate_taxa_on_upload

        # remote_id → local Taxonomy cache, shared across all taxa in this run
        remote_cache: dict = {}

        total_processed = 0
        total_skipped = 0
        save_interval = 50
        since_last_save = 0

        for taxon_data in get_all_taxa(base_url, remote_group_id):
            if HarvestSession.objects.filter(id=session_id, canceled=True).exists():
                _log("Harvest canceled by user")
                break

            canonical_name = (taxon_data.get('canonical_name') or '').strip()
            rank = (taxon_data.get('rank') or 'unknown').strip()

            if not canonical_name:
                total_skipped += 1
                continue

            try:
                taxonomy = _find_or_create_taxonomy(taxon_data, base_url, remote_cache)
                if taxonomy is None:
                    total_skipped += 1
                    continue

                # Add to target group
                target_group.taxonomies.add(
                    taxonomy,
                    through_defaults={'is_validated': auto_validate},
                )
                if not auto_validate:
                    target_group.taxonomies.through.objects.filter(
                        taxongroup=target_group,
                        taxonomy=taxonomy,
                    ).update(is_validated=False)

                total_processed += 1
                since_last_save += 1
                _log(f"[{total_processed}] Processed: {canonical_name} ({rank})")

            except Exception as exc:
                _log(f"Error processing '{canonical_name}': {exc}")
                total_skipped += 1
                continue

            if since_last_save >= save_interval:
                session.status = f"Processing ({total_processed} taxa)"
                session.save(update_fields=["status"])
                since_last_save = 0

        finished_at = timezone.now().isoformat(timespec="seconds")
        final_additional_data = {
            **additional,
            "finished_at": finished_at,
            "total_processed": total_processed,
            "total_skipped": total_skipped,
        }

        if not HarvestSession.objects.filter(id=session_id, canceled=True).exists():
            _log(
                f"Harvest complete — {total_processed} taxa processed, "
                f"{total_skipped} skipped"
            )
            HarvestSession.objects.filter(id=session_id).update(
                status=f"Finished ({total_processed} taxa)",
                finished=True,
                additional_data=final_additional_data,
            )
        else:
            HarvestSession.objects.filter(id=session_id).update(
                status=f"Canceled ({total_processed} taxa before cancel)",
                additional_data=final_additional_data,
            )

        connect_bims_signals()
