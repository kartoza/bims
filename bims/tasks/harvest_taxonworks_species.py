# coding=utf-8
from __future__ import annotations

import logging
import os
import time

from celery import shared_task
from django.utils import timezone
from django_tenants.utils import schema_context

logger = logging.getLogger(__name__)

PER_PAGE = 50

# A failed page fetch is retried this many times (on top of the HTTP-level
# retries in bims.utils.taxonworks) before the harvest is stopped.
PAGE_FETCH_ATTEMPTS = 4
PAGE_FETCH_BACKOFF_SECONDS = 30

# An unfinished session whose log hasn't been written for this long is
# considered dead (e.g. the worker was restarted by a deployment). The task
# writes a log line for every record and every retry, so a live harvest never
# stays silent this long.
STALL_SECONDS = 30 * 60
# Stop auto-resuming a session that keeps dying on the same page.
MAX_AUTO_RESUMES_PER_PAGE = 5


def taxonworks_session_seconds_since_activity(session) -> float | None:
    """Seconds since the session's log file was last written, or None."""
    try:
        if session.log_file:
            return time.time() - os.path.getmtime(session.log_file.path)
    except (OSError, ValueError):
        pass
    return None


def taxonworks_session_is_stalled(session) -> bool:
    idle = taxonworks_session_seconds_since_activity(session)
    return idle is None or idle >= STALL_SECONDS


def _append_session_log(session, msg: str):
    ts = timezone.now().isoformat(timespec="seconds")
    try:
        with open(session.log_file.path, "a") as fh:
            fh.write(f"[{ts}] {msg}\n")
    except Exception:
        pass


class _SessionTaxonWorksTaxaProcessor:
    def __init__(self, log_fn, base_url: str, project_token: str,
                 records_by_id: dict[int, dict],
                 otus_by_taxon_name_id: dict[int, int]):
        from bims.scripts.taxa_upload_taxonworks import SessionTaxonWorksTaxaProcessor
        self._processor = SessionTaxonWorksTaxaProcessor(
            log_fn,
            base_url=base_url,
            project_token=project_token,
            records_by_id=records_by_id,
            otus_by_taxon_name_id=otus_by_taxon_name_id,
        )

    def process(self, record: dict, taxon_group, harvest_synonyms: bool):
        return self._processor.process(
            record,
            taxon_group,
            harvest_synonyms=harvest_synonyms,
        )


def _fetch_page_with_retry(log_fn, base_url, project_token, page,
                           taxon_name_id=None):
    """
    Fetch a taxon names page, retrying with exponential backoff.
    Returns ``(records, total_pages)``; raises TaxonWorksAPIError once all
    attempts have failed.
    """
    from bims.utils.taxonworks import (
        TaxonWorksAPIError,
        get_taxon_names_page_with_meta,
    )

    for attempt in range(1, PAGE_FETCH_ATTEMPTS + 1):
        try:
            return get_taxon_names_page_with_meta(
                base_url, project_token, page=page, per_page=PER_PAGE,
                taxon_name_id=taxon_name_id,
            )
        except TaxonWorksAPIError as exc:
            if attempt >= PAGE_FETCH_ATTEMPTS:
                raise
            wait = PAGE_FETCH_BACKOFF_SECONDS * (2 ** (attempt - 1))
            log_fn(
                f"Failed to fetch page {page} "
                f"(attempt {attempt}/{PAGE_FETCH_ATTEMPTS}): {exc} - "
                f"retrying in {wait}s"
            )
            time.sleep(wait)


@shared_task(name='bims.tasks.harvest_taxonworks_species', queue='update')
def harvest_taxonworks_species(session_id: int, schema_name: str):
    from bims.models import HarvestSession
    from bims.signals.utils import connect_bims_signals, disconnect_bims_signals
    from bims.utils.taxonworks import (
        TaxonWorksAPIError,
        fetch_otus_for_ids,
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
        exclude_extinct = additional.get("exclude_extinct", True)
        root_taxon_name_id = additional.get("taxon_name_id")
        try:
            root_taxon_name_id = int(root_taxon_name_id) if root_taxon_name_id else None
        except (TypeError, ValueError):
            root_taxon_name_id = None

        if not base_url or not project_token:
            _log("Missing TaxonWorks base_url or project_token - aborting")
            HarvestSession.objects.filter(id=session_id).update(
                status="Failed: incomplete TaxonWorks config",
                finished=True,
            )
            connect_bims_signals()
            return

        if root_taxon_name_id:
            _log(
                f"Starting TaxonWorks harvest at {base_url} "
                f"(taxon name id {root_taxon_name_id} and descendants)"
            )
        else:
            _log(f"Starting TaxonWorks harvest at {base_url} (whole project)")

        processed_ids: set[int] = set(
            int(x) for x in additional.get("processed_taxonworks_ids", [])
        )
        page = additional.get("current_page", 1)
        total_processed = len(processed_ids)
        latest_updated_at = additional.get("source_version_latest_updated_at")
        total_pages = additional.get("total_pages")

        # Tracks the underlying Taxonomy rows already reported in this run
        seen_taxonomy_ids: set[int] = set()
        counters = {"not_added": 0, "duplicate": 0}

        # Mutable caches shared with the processor across pages.
        # records_cache grows as pages are fetched; it also serves as the
        # parent-lookup cache when _ensure_taxonomy recurses up the tree.
        records_cache: dict[int, dict] = {}
        otus_cache: dict[int, int] = {}

        processor = _SessionTaxonWorksTaxaProcessor(
            _log,
            base_url=base_url,
            project_token=project_token,
            records_by_id=records_cache,
            otus_by_taxon_name_id=otus_cache,
        )

        def _process_record(record: dict):
            nonlocal total_processed, latest_updated_at

            record_id = record.get("id")
            if not record_id:
                return
            record_id = int(record_id)

            if record_id in processed_ids:
                return

            if exclude_extinct and taxonworks_record_is_extinct(record):
                _log(
                    f"Skipping extinct taxon id={record_id} "
                    f"({record.get('cached', '')})"
                )
                processed_ids.add(record_id)
                return

            name = record.get("cached") or record.get("name", "")
            try:
                taxonomy = processor.process(
                    record,
                    session.module_group,
                    harvest_synonyms=session.harvest_synonyms,
                )
                processed_ids.add(record_id)

                if taxonomy is None:
                    counters["not_added"] += 1
                    _log(
                        f"NOT added: {name} (id={record_id}) - "
                        f"rank is missing/unrecognised or invalid, see "
                        f"error above"
                    )
                    return

                updated_at = record.get("updated_at")
                if updated_at and (
                    not latest_updated_at or updated_at > latest_updated_at
                ):
                    latest_updated_at = updated_at

                if taxonomy.id in seen_taxonomy_ids:
                    counters["duplicate"] += 1
                    _log(
                        f"Already processed: {name} (id={record_id}) is "
                        f"the same taxon as TaxonomyID={taxonomy.id} "
                        f"({taxonomy.canonical_name}), not counted again"
                    )
                    return

                seen_taxonomy_ids.add(taxonomy.id)
                total_processed += 1
                _log(
                    f"[{total_processed}] Processed: "
                    f"{name} "
                    f"({(record.get('rank') or 'unknown rank').capitalize()}, "
                    f"id={record_id})"
                )
            except Exception as exc:
                _log(f"Error processing TaxonWorks id={record_id}: {exc}")

        def _progress_data(**extra):
            return {
                **additional,
                "base_url": base_url,
                "project_token": project_token,
                "exclude_extinct": exclude_extinct,
                "current_page": page,
                "total_pages": total_pages,
                "processed_taxonworks_ids": list(processed_ids),
                "source_version_latest_updated_at": latest_updated_at,
                **extra,
            }

        canceled = False
        fetch_error = None

        # The descendants query does not include the root name itself, so
        # harvest it explicitly first.
        if root_taxon_name_id and root_taxon_name_id not in processed_ids:
            root_record = get_taxon_name(base_url, project_token, root_taxon_name_id)
            if root_record:
                records_cache[root_taxon_name_id] = root_record
                otus_cache.update(
                    fetch_otus_for_ids(base_url, project_token, [root_taxon_name_id])
                )
                _process_record(root_record)
            else:
                _log(
                    f"Could not fetch root taxon name id={root_taxon_name_id}; "
                    f"continuing with its descendants"
                )

        while True:
            if HarvestSession.objects.filter(id=session_id, canceled=True).exists():
                _log("Harvest canceled by user")
                canceled = True
                break

            _log(
                f"Fetching taxon names page {page}"
                f"{f' of {total_pages}' if total_pages else ''} "
                f"(per={PER_PAGE})..."
            )
            try:
                page_records, page_total = _fetch_page_with_retry(
                    _log, base_url, project_token, page,
                    taxon_name_id=root_taxon_name_id,
                )
            except TaxonWorksAPIError as exc:
                fetch_error = exc
                _log(
                    f"Giving up on page {page} after {PAGE_FETCH_ATTEMPTS} "
                    f"attempts: {exc}"
                )
                break

            if page_total:
                total_pages = page_total

            if not page_records:
                if total_pages and page < total_pages:
                    _log(
                        f"Page {page} of {total_pages} is empty, "
                        f"continuing with next page"
                    )
                    page += 1
                    continue
                _log("No more records")
                break

            # Extend the parent-lookup cache with this page's records.
            for r in page_records:
                if r.get("id"):
                    records_cache[int(r["id"])] = r

            # Fetch OTUs only for the taxon_name_ids on this page.
            page_ids = [int(r["id"]) for r in page_records if r.get("id")]
            page_otus = fetch_otus_for_ids(base_url, project_token, page_ids)
            otus_cache.update(page_otus)

            _log(
                f"Page {page}: {len(page_records)} taxon names, "
                f"{len(page_otus)} OTUs"
            )

            for record in page_records:
                if HarvestSession.objects.filter(id=session_id, canceled=True).exists():
                    _log("Harvest canceled by user")
                    canceled = True
                    break
                _process_record(record)

            if canceled:
                break

            # Persist progress after every page so a resume starts from here.
            session.additional_data = _progress_data()
            session.status = (
                f"Processing (page {page}"
                f"{f'/{total_pages}' if total_pages else ''}, "
                f"{total_processed} taxa)"
            )
            session.save(update_fields=["additional_data", "status"])

            if total_pages:
                if page >= total_pages:
                    break
            elif len(page_records) < PER_PAGE:
                break
            page += 1

        not_added_count = counters["not_added"]
        duplicate_count = counters["duplicate"]
        summary_suffix = (
            f", {not_added_count} not added (unrecognised rank), "
            f"{duplicate_count} duplicate record(s) skipped"
            if (not_added_count or duplicate_count) else ""
        )

        if fetch_error is not None:
            # Keep current_page pointing at the failed page so re-running the
            # task for this session resumes from there.
            HarvestSession.objects.filter(id=session_id).update(
                status=(
                    f"Failed at page {page}"
                    f"{f'/{total_pages}' if total_pages else ''} "
                    f"({total_processed} taxa processed{summary_suffix}): "
                    f"could not fetch data from TaxonWorks"
                ),
                finished=True,
                additional_data=_progress_data(
                    failed_at=timezone.now().isoformat(timespec="seconds"),
                    last_error=str(fetch_error),
                ),
            )
        elif not canceled:
            _log(f"Harvest complete - {total_processed} taxa processed{summary_suffix}")
            HarvestSession.objects.filter(id=session_id).update(
                status=f"Finished ({total_processed} taxa{summary_suffix})",
                finished=True,
                additional_data=_progress_data(
                    finished_at=timezone.now().isoformat(timespec="seconds"),
                ),
            )
        else:
            HarvestSession.objects.filter(id=session_id).update(
                status=f"Canceled ({total_processed} taxa before cancel{summary_suffix})",
                additional_data=_progress_data(
                    finished_at=timezone.now().isoformat(timespec="seconds"),
                ),
            )

        connect_bims_signals()


@shared_task(
    name='bims.tasks.auto_resume_taxonworks_harvest',
    queue='update',
    ignore_result=True,
)
def auto_resume_taxonworks_harvest(schema_name: str):
    """Re-dispatch TaxonWorks harvest sessions that stopped mid-run."""
    from bims.models import HarvestSession

    with schema_context(schema_name):
        sessions = HarvestSession.objects.filter(
            category='taxonworks',
            finished=False,
            canceled=False,
        ).exclude(log_file='').exclude(log_file__isnull=True)

        for session in sessions:
            if not taxonworks_session_is_stalled(session):
                continue

            additional = dict(session.additional_data or {})
            current_page = additional.get('current_page', 1)
            if additional.get('auto_resume_page') != current_page:
                additional['auto_resume_page'] = current_page
                additional['auto_resume_count'] = 0
            count = additional.get('auto_resume_count', 0)

            if count >= MAX_AUTO_RESUMES_PER_PAGE:
                msg = (
                    f"Auto-resume gave up: harvest stopped "
                    f"{count} times at page {current_page}"
                )
                _append_session_log(session, msg)
                HarvestSession.objects.filter(id=session.id).update(
                    status=f"Failed at page {current_page}: {msg}",
                    finished=True,
                    additional_data=additional,
                )
                logger.warning(
                    "TaxonWorks harvest session=%s: %s", session.id, msg
                )
                continue

            additional['auto_resume_count'] = count + 1
            HarvestSession.objects.filter(id=session.id).update(
                additional_data=additional,
            )
            # Writing to the log also refreshes its mtime, so the next
            # check doesn't dispatch the session a second time.
            _append_session_log(
                session,
                f"Harvest appears to have stopped - auto-resuming from page "
                f"{current_page} (attempt {count + 1}/"
                f"{MAX_AUTO_RESUMES_PER_PAGE})",
            )
            logger.info(
                "Auto-resuming TaxonWorks harvest session=%s schema=%s",
                session.id, schema_name,
            )
            harvest_taxonworks_species.delay(session.id, schema_name=schema_name)


@shared_task(
    name='bims.tasks.auto_resume_taxonworks_harvest_all_schemas',
    queue='update',
    ignore_result=True,
)
def auto_resume_taxonworks_harvest_all_schemas():
    from django_tenants.utils import get_public_schema_name, get_tenant_model

    public = get_public_schema_name()
    for tenant in get_tenant_model().objects.exclude(schema_name=public):
        try:
            auto_resume_taxonworks_harvest(tenant.schema_name)
        except Exception as exc:
            logger.error(
                "auto_resume_taxonworks_harvest failed for %s: %s",
                tenant.schema_name, exc,
            )
