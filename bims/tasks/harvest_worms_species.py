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
import time
from collections import deque

from celery import shared_task
from django.db import transaction
from django.utils import timezone
from django_tenants.utils import schema_context

logger = logging.getLogger(__name__)

FETCH_ATTEMPTS = 4
FETCH_BACKOFF_SECONDS = 30


def _fetch_with_retry(log_fn, description: str, fetch):
    """
    Call *fetch()* with exponential backoff on WormsAPIError; re-raises once
    all attempts have failed.
    """
    from bims.utils.worms import WormsAPIError

    for attempt in range(1, FETCH_ATTEMPTS + 1):
        try:
            return fetch()
        except WormsAPIError as exc:
            if attempt >= FETCH_ATTEMPTS:
                raise
            wait = FETCH_BACKOFF_SECONDS * (2 ** (attempt - 1))
            log_fn(
                f"Failed to fetch {description} "
                f"(attempt {attempt}/{FETCH_ATTEMPTS}): {exc} - "
                f"retrying in {wait}s"
            )
            time.sleep(wait)


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
                fetch_col_id: bool = True):
        with transaction.atomic():
            self._processor.process_worms_data(
                row, taxon_group, harvest_synonyms, fetch_col_id=fetch_col_id
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
        WormsAPIError,
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
        # Only freshwater taxa are harvested unless explicitly disabled
        freshwater_only = additional.get("freshwater_only", True) is not False
        processor = _SessionWormsTaxaProcessor(_log)
        total_processed = len(processed_ids)

        include_unaccepted = additional.get("include_unaccepted", False) is True

        def _is_freshwater(row: dict) -> bool:
            return bool(row.get("Fresh"))

        def _is_unaccepted(record: dict) -> bool:
            return (record.get("status") or "").strip().lower() == "unaccepted"

        if freshwater_only:
            _log("Freshwater-only filter enabled - non-freshwater taxa will be skipped")
        if not include_unaccepted:
            _log("Unaccepted names will be skipped")

        fetch_error = None
        failed_aphia_id = None
        failed_records = {
            int(k): v for k, v in (additional.get("failed_aphia_ids") or {}).items()
        }
        failed_rows = {}

        def _record_failure(record_id: int, row: dict, exc: Exception):
            failed_records[record_id] = str(exc)
            failed_rows[record_id] = row

        def _record_success(record_id: int):
            processed_ids.add(record_id)
            failed_records.pop(record_id, None)
            failed_rows.pop(record_id, None)

        def _progress_data(**extra):
            return {
                **additional,
                "aphia_id": aphia_id,
                "processed_aphia_ids": list(processed_ids),
                "failed_aphia_ids": {str(k): v for k, v in failed_records.items()},
                **extra,
            }

        root = None
        if aphia_id not in processed_ids:
            try:
                root = _fetch_with_retry(
                    _log, f"root AphiaID={aphia_id}",
                    lambda: get_aphia_record(aphia_id, raise_on_error=True),
                )
            except WormsAPIError as exc:
                fetch_error = exc
                failed_aphia_id = aphia_id
                queue.clear()
                _log(
                    f"Giving up on root AphiaID={aphia_id} after "
                    f"{FETCH_ATTEMPTS} attempts: {exc}"
                )
        if aphia_id not in processed_ids and fetch_error is None:
            if root:
                row = api_record_to_csv_row(root)
                if not include_unaccepted and _is_unaccepted(root):
                    _log(f"Skipped root AphiaID={aphia_id} (unaccepted)")
                    processed_ids.add(aphia_id)
                elif freshwater_only and not _is_freshwater(row):
                    _log(f"Skipped root AphiaID={aphia_id} (not flagged as freshwater)")
                    processed_ids.add(aphia_id)
                else:
                    try:
                        processor.process(row, taxon_group, harvest_synonyms, fetch_col_id=True)
                        _record_success(aphia_id)
                        total_processed += 1
                        _log(f"Processed root AphiaID={aphia_id} ({root.get('scientificname', '')})")
                    except Exception as exc:
                        _record_failure(aphia_id, row, exc)
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
            try:
                children = _fetch_with_retry(
                    _log, f"children of AphiaID={current_id}",
                    lambda: get_aphia_children(current_id),
                )
            except WormsAPIError as exc:
                fetch_error = exc
                failed_aphia_id = current_id
                _log(
                    f"Giving up on children of AphiaID={current_id} after "
                    f"{FETCH_ATTEMPTS} attempts: {exc}"
                )
                break
            _log(f"AphiaID={current_id}: {len(children)} child(ren)")

            for child in children:
                child_id = child.get("AphiaID")
                if not child_id:
                    continue
                child_id = int(child_id)
                already_processed = child_id in processed_ids

                if not already_processed:
                    row = api_record_to_csv_row(child)
                    if not include_unaccepted and _is_unaccepted(child):
                        _log(
                            f"Skipped AphiaID={child_id} "
                            f"({child.get('scientificname', '')}, unaccepted)"
                        )
                        processed_ids.add(child_id)
                        since_last_save += 1
                    elif freshwater_only and not _is_freshwater(row):
                        _log(f"Skipped AphiaID={child_id} (not flagged as freshwater)")
                        processed_ids.add(child_id)
                        since_last_save += 1
                    else:
                        try:
                            processor.process(row, taxon_group, harvest_synonyms, fetch_col_id=True)
                            _record_success(child_id)
                            total_processed += 1
                            since_last_save += 1
                            _log(
                                f"[{total_processed}] Processed AphiaID={child_id} "
                                f"({child.get('scientificname', '')})"
                            )
                        except Exception as exc:
                            _record_failure(child_id, row, exc)
                            _log(f"Error processing AphiaID={child_id}: {exc}")

                if (child.get("status") or "").lower() == "accepted":
                    queue.append(child_id)

            if since_last_save >= SAVE_INTERVAL:
                session.additional_data = _progress_data()
                session.status = f"Processing ({total_processed} taxa)"
                session.save(update_fields=["additional_data", "status"])
                since_last_save = 0

        canceled = HarvestSession.objects.filter(
            id=session_id, canceled=True
        ).exists()

        if failed_records and fetch_error is None and not canceled:
            _log(f"Retrying {len(failed_records)} record(s) that failed to process")
            for record_id in list(failed_records):
                if HarvestSession.objects.filter(id=session_id, canceled=True).exists():
                    _log("Harvest canceled by user")
                    canceled = True
                    break
                row = failed_rows.get(record_id)
                if row is None:
                    try:
                        record = _fetch_with_retry(
                            _log, f"AphiaID={record_id}",
                            lambda: get_aphia_record(record_id, raise_on_error=True),
                        )
                    except WormsAPIError as exc:
                        failed_records[record_id] = str(exc)
                        _log(f"Retry failed for AphiaID={record_id}: {exc}")
                        continue
                    if not record:
                        failed_records.pop(record_id, None)
                        _log(f"AphiaID={record_id} no longer exists in WoRMS, dropping it")
                        continue
                    row = api_record_to_csv_row(record)
                try:
                    processor.process(row, taxon_group, harvest_synonyms, fetch_col_id=True)
                    _record_success(record_id)
                    total_processed += 1
                    _log(f"[{total_processed}] Retry succeeded for AphiaID={record_id}")
                except Exception as exc:
                    failed_records[record_id] = str(exc)
                    _log(f"Retry failed for AphiaID={record_id}: {exc}")

        error_suffix = (
            f", {len(failed_records)} record(s) failed to process - see log"
            if failed_records else ""
        )

        if fetch_error is not None and not canceled:
            HarvestSession.objects.filter(id=session_id).update(
                status=(
                    f"Failed ({total_processed} taxa processed{error_suffix}): "
                    f"could not fetch AphiaID={failed_aphia_id} from WoRMS"
                ),
                finished=True,
                additional_data=_progress_data(
                    failed_at=timezone.now().isoformat(timespec="seconds"),
                    failed_aphia_id=failed_aphia_id,
                    last_error=str(fetch_error),
                ),
            )
        elif not canceled:
            _log(f"Harvest complete — {total_processed} taxa processed{error_suffix}")
            HarvestSession.objects.filter(id=session_id).update(
                status=f"Finished ({total_processed} taxa{error_suffix})",
                finished=True,
                additional_data=_progress_data(),
            )
        else:
            HarvestSession.objects.filter(id=session_id).update(
                status=f"Canceled ({total_processed} taxa before cancel)",
                additional_data=_progress_data(),
            )

        connect_bims_signals()


@shared_task(
    name='bims.tasks.auto_resume_worms_harvest',
    queue='update',
    ignore_result=True,
)
def auto_resume_worms_harvest(schema_name: str):
    """Re-dispatch WoRMS harvest sessions that stopped mid-run."""
    from bims.utils.harvest_resume import auto_resume_stalled_sessions

    with schema_context(schema_name):
        auto_resume_stalled_sessions(
            schema_name,
            category='worms',
            dispatch=lambda sid, schema: harvest_worms_species.delay(
                sid, schema_name=schema
            ),
            progress_marker=lambda data: len(data.get('processed_aphia_ids') or []),
            label='WoRMS harvest',
            describe_position=lambda data: (
                f"after {len(data.get('processed_aphia_ids') or [])} records"
            ),
        )


@shared_task(
    name='bims.tasks.auto_resume_worms_harvest_all_schemas',
    queue='update',
    ignore_result=True,
)
def auto_resume_worms_harvest_all_schemas():
    from bims.utils.harvest_resume import for_each_tenant_schema

    for_each_tenant_schema(auto_resume_worms_harvest, 'auto_resume_worms_harvest')
