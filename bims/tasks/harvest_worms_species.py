# coding=utf-8
"""
Celery task: harvest taxonomy from WoRMS (World Register of Marine Species)
via the public REST API and store it in BIMS.

Usage
-----
Start a HarvestSession with category='worms'.  Either:
  - set parent_species to a Taxonomy whose additional_data contains 'AphiaID', or
  - set additional_data={'aphia_id': <int>} directly on the session.

Then enqueue::

    from bims.tasks import harvest_worms_species
    harvest_worms_species.delay(session_id, schema_name=schema_name)

The task runs in the 'update' queue, BFS-traverses the WoRMS subtree rooted at
the given AphiaID, processes every record through WormsTaxaProcessor, and
persists resume state to session.additional_data so an interrupted run can
continue where it left off.
"""
from __future__ import annotations

import json
import logging
from collections import deque

from celery import shared_task
from django.core.files.base import ContentFile
from django.db import transaction
from django.utils import timezone
from django_tenants.utils import schema_context

logger = logging.getLogger(__name__)


class _SessionWormsTaxaProcessor:
    """
    Thin wrapper around WormsTaxaProcessor that swallows per-row errors
    into the harvest log rather than raising.
    """

    def __init__(self, log_fn):
        self._log = log_fn
        from bims.scripts.taxa_upload_worms import WormsTaxaProcessor

        class _Processor(WormsTaxaProcessor):
            def handle_error(self_, row, message):  # noqa: N805
                log_fn(f"Row error AphiaID={row.get('AphiaID')}: {message}")

            def finish_processing_row(self_, row, taxonomy):  # noqa: N805
                pass  # no upload-session bookkeeping needed

            def fetch_accepted_row(self_, accepted_aphia_id: int):  # noqa: N805
                from bims.utils.worms import get_aphia_record, api_record_to_csv_row
                record = get_aphia_record(accepted_aphia_id)
                if record:
                    return api_record_to_csv_row(record)
                return None

        self._processor = _Processor()

    def process(self, row: dict, taxon_group, harvest_synonyms: bool,
                fetch_gbif_key: bool = True):
        with transaction.atomic():
            self._processor.process_worms_data(
                row, taxon_group, harvest_synonyms, fetch_gbif_key=fetch_gbif_key
            )


@shared_task(name='bims.tasks.harvest_worms_species', queue='update')
def harvest_worms_species(session_id: int, schema_name: str):
    """
    BFS-traverse the WoRMS subtree rooted at the session's AphiaID and
    import every taxon into BIMS.

    Resumption: already-processed AphiaIDs are persisted in
    session.additional_data['processed_aphia_ids'] so an interrupted run
    skips records that were already stored.
    """
    from bims.signals.utils import disconnect_bims_signals, connect_bims_signals
    from bims.models import HarvestSession
    from bims.utils.worms import (
        get_aphia_record,
        get_aphia_children,
        api_record_to_csv_row,
    )

    with schema_context(schema_name):
        try:
            session = HarvestSession.objects.get(id=session_id)
        except HarvestSession.DoesNotExist:
            logger.error("harvest_worms_species: session %s not found", session_id)
            return

        def _log(msg: str):
            ts = timezone.now().isoformat(timespec="seconds")
            line = f"[{ts}] {msg}\n"
            logger.info("WoRMS harvest session=%s: %s", session_id, msg)
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
        aphia_id: int | None = None

        if "aphia_id" in additional:
            aphia_id = int(additional["aphia_id"])
        elif session.parent_species_id:
            ps_data = session.parent_species.additional_data
            if isinstance(ps_data, str):
                try:
                    ps_data = json.loads(ps_data)
                except (ValueError, TypeError):
                    ps_data = {}
            if isinstance(ps_data, dict):
                aphia_id = ps_data.get("AphiaID") or ps_data.get("_worms_aphia_id")
            if aphia_id:
                aphia_id = int(aphia_id)

        if not aphia_id:
            _log("No AphiaID found on session or parent_species — aborting")
            HarvestSession.objects.filter(id=session_id).update(
                status="Failed: no AphiaID",
                finished=True,
            )
            connect_bims_signals()
            return

        _log(f"Starting WoRMS harvest from AphiaID={aphia_id}")

        processed_ids: set[int] = set(
            int(x) for x in additional.get("processed_aphia_ids", [])
        )
        queue: deque[int] = deque()
        queue.append(aphia_id)

        taxon_group = session.module_group
        harvest_synonyms = session.harvest_synonyms
        processor = _SessionWormsTaxaProcessor(_log)
        total_processed = len(processed_ids)

        if aphia_id not in processed_ids:
            root = get_aphia_record(aphia_id)
            if root:
                row = api_record_to_csv_row(root)
                try:
                    processor.process(row, taxon_group, harvest_synonyms, fetch_gbif_key=True)
                    processed_ids.add(aphia_id)
                    total_processed += 1
                    _log(f"Processed root AphiaID={aphia_id} ({root.get('scientificname', '')})")
                except Exception as exc:
                    _log(f"Error processing root AphiaID={aphia_id}: {exc}")
            else:
                _log(f"Root AphiaID={aphia_id} not found in WoRMS")

        SAVE_INTERVAL = 50
        since_last_save = 0

        while queue:
            if HarvestSession.objects.filter(id=session_id, canceled=True).exists():
                _log("Harvest canceled by user")
                break

            current_id = queue.popleft()
            children = get_aphia_children(current_id)
            _log(f"AphiaID={current_id}: {len(children)} child(ren)")

            for child in children:
                child_id = child.get("AphiaID")
                if not child_id:
                    continue
                child_id = int(child_id)
                if child_id in processed_ids:
                    continue

                row = api_record_to_csv_row(child)
                try:
                    processor.process(row, taxon_group, harvest_synonyms, fetch_gbif_key=True)
                    processed_ids.add(child_id)
                    total_processed += 1
                    since_last_save += 1
                except Exception as exc:
                    _log(f"Error processing AphiaID={child_id}: {exc}")
                    continue

                if (child.get("status") or "").lower() == "accepted":
                    queue.append(child_id)

            if since_last_save >= SAVE_INTERVAL:
                session.additional_data = {
                    **additional,
                    "aphia_id": aphia_id,
                    "processed_aphia_ids": list(processed_ids),
                }
                session.status = f"Processing ({total_processed} taxa)"
                session.save(update_fields=["additional_data", "status"])
                since_last_save = 0

        final_additional_data = {
            **additional,
            "aphia_id": aphia_id,
            "processed_aphia_ids": list(processed_ids),
        }

        if not HarvestSession.objects.filter(id=session_id, canceled=True).exists():
            _log(f"Harvest complete — {total_processed} taxa processed")
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
