# coding=utf-8
from __future__ import annotations

import logging
from collections import deque

from celery import shared_task
from django.utils import timezone
from django_tenants.utils import schema_context

logger = logging.getLogger(__name__)


class _SessionTaxonWorksTaxaProcessor:
    def __init__(self, log_fn, base_url: str, project_token: str,
                 records_by_id: dict[int, dict] | None = None):
        from bims.scripts.taxa_upload_taxonworks import SessionTaxonWorksTaxaProcessor
        self._processor = SessionTaxonWorksTaxaProcessor(
            log_fn,
            base_url=base_url,
            project_token=project_token,
            records_by_id=records_by_id,
        )

    def process(self, record: dict, taxon_group, harvest_synonyms: bool):
        return self._processor.process(
            record,
            taxon_group,
            harvest_synonyms=harvest_synonyms,
        )


@shared_task(name='bims.tasks.harvest_taxonworks_species', queue='update')
def harvest_taxonworks_species(session_id: int, schema_name: str):
    from bims.models import HarvestSession
    from bims.signals.utils import connect_bims_signals, disconnect_bims_signals
    from bims.utils.taxonworks import (
        get_all_taxon_names,
        get_taxon_name,
        taxonworks_record_is_extinct,
    )

    with schema_context(schema_name):
        try:
            session = HarvestSession.objects.get(id=session_id)
        except HarvestSession.DoesNotExist:
            logger.error("harvest_taxonworks_species: session %s not found", session_id)
            return

        def _log(msg: str):
            ts = timezone.now().isoformat(timespec="seconds")
            line = f"[{ts}] {msg}\n"
            logger.info("TaxonWorks harvest session=%s: %s", session_id, msg)
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
        base_url = (additional.get("base_url") or "").strip()
        project_token = (additional.get("project_token") or "").strip()
        root_taxon_name_id = additional.get("taxon_name_id")
        exclude_extinct = additional.get("exclude_extinct", True)

        if not base_url or not project_token or not root_taxon_name_id:
            _log("Missing TaxonWorks base_url, project_token, or taxon_name_id — aborting")
            HarvestSession.objects.filter(id=session_id).update(
                status="Failed: incomplete TaxonWorks config",
                finished=True,
            )
            connect_bims_signals()
            return

        root_taxon_name_id = int(root_taxon_name_id)
        root_record = get_taxon_name(base_url, project_token, root_taxon_name_id)
        if not root_record:
            _log(f"Root taxon_name_id={root_taxon_name_id} not found in TaxonWorks")
            HarvestSession.objects.filter(id=session_id).update(
                status="Failed: root taxon not found",
                finished=True,
            )
            connect_bims_signals()
            return

        _log(
            f"Starting TaxonWorks harvest from taxon_name_id={root_taxon_name_id} "
            f"at {base_url}"
        )

        all_records = get_all_taxon_names(base_url, project_token)
        records_by_id = {int(r["id"]): r for r in all_records if r.get("id")}
        records_by_id[root_taxon_name_id] = root_record

        children_by_parent_id: dict[int, list[dict]] = {}
        for record in records_by_id.values():
            parent_id = record.get("parent_id")
            if parent_id is None:
                continue
            children_by_parent_id.setdefault(int(parent_id), []).append(record)

        processed_ids: set[int] = set(
            int(x) for x in additional.get("processed_taxonworks_ids", [])
        )
        queue: deque[int] = deque([root_taxon_name_id])
        processor = _SessionTaxonWorksTaxaProcessor(
            _log,
            base_url=base_url,
            project_token=project_token,
            records_by_id=records_by_id,
        )
        total_processed = len(processed_ids)
        latest_updated_at = additional.get("source_version_latest_updated_at")
        since_last_save = 0
        save_interval = 50

        while queue:
            if HarvestSession.objects.filter(id=session_id, canceled=True).exists():
                _log("Harvest canceled by user")
                break

            current_id = queue.popleft()
            record = records_by_id.get(current_id)
            if not record:
                _log(f"Missing cached TaxonWorks record id={current_id}")
                continue

            if current_id not in processed_ids:
                if exclude_extinct and taxonworks_record_is_extinct(record):
                    _log(f"Skipping extinct taxon id={current_id} ({record.get('cached', '')})")
                    processed_ids.add(current_id)
                else:
                    try:
                        processor.process(
                            record,
                            session.module_group,
                            harvest_synonyms=session.harvest_synonyms,
                        )
                        processed_ids.add(current_id)
                        total_processed += 1
                        since_last_save += 1
                        _log(
                            f"[{total_processed}] Processed: "
                            f"{record.get('cached') or record.get('name', '')} "
                            f"({(record.get('rank') or 'unknown rank').capitalize()}, "
                            f"id={current_id})"
                        )
                        updated_at = record.get("updated_at")
                        if updated_at and (not latest_updated_at or updated_at > latest_updated_at):
                            latest_updated_at = updated_at
                    except Exception as exc:
                        _log(f"Error processing TaxonWorks id={current_id}: {exc}")
                        continue

            for child in children_by_parent_id.get(current_id, []):
                child_id = child.get("id")
                if child_id:
                    child_id = int(child_id)
                    if child_id not in processed_ids:
                        queue.append(child_id)

            if since_last_save >= save_interval:
                session.additional_data = {
                    **additional,
                    "base_url": base_url,
                    "project_token": project_token,
                    "taxon_name_id": root_taxon_name_id,
                    "exclude_extinct": exclude_extinct,
                    "processed_taxonworks_ids": list(processed_ids),
                    "source_version_latest_updated_at": latest_updated_at,
                }
                session.status = f"Processing ({total_processed} taxa)"
                session.save(update_fields=["additional_data", "status"])
                since_last_save = 0

        final_additional_data = {
            **additional,
            "base_url": base_url,
            "project_token": project_token,
            "taxon_name_id": root_taxon_name_id,
            "exclude_extinct": exclude_extinct,
            "processed_taxonworks_ids": list(processed_ids),
            "source_version_latest_updated_at": latest_updated_at,
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
