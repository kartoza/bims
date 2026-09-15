# coding=utf-8
"""
Report (and optionally delete) GBIF-sourced occurrences that no longer exist
upstream in GBIF.

A GET /occurrence/{gbifID} that returns 404 means GBIF no longer indexes the
occurrence (effectively deleted). In dry_run mode nothing is deleted; the task
only reports how many records would be removed. Either way a summary is emailed
to superusers, mirroring the "Clear taxa not in group" cleanup.

Records are checked oldest-modified first (see modified_date) so that, combined
with a limit, each run spends its budget on the stalest records.
"""

import csv
import io
import logging
from datetime import timedelta

import requests
from requests.adapters import HTTPAdapter, Retry
from celery import shared_task
from django.db.models import F, Q
from django.utils import timezone

from bims.utils.mail import mail_superusers, get_domain_name

logger = logging.getLogger("bims")

GBIF_API = 'https://api.gbif.org/v1'


def _build_session():
    session = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(['GET']),
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.mount('http://', HTTPAdapter(max_retries=retries))
    return session


def _occurrence_deleted(session, gbif_id, timeout):
    """True if GBIF no longer indexes this occurrence (404)."""
    try:
        response = session.get(
            GBIF_API + '/occurrence/%s' % gbif_id, timeout=timeout)
        return response.status_code == 404
    except requests.RequestException as e:
        logger.warning("occurrence check failed for %s: %s", gbif_id, e)
        return False


CSV_FIELDNAMES = [
    'local_id', 'name', 'gbif_id', 'dataset_key', 'modified_date',
]

# Records are checked one-by-one against GBIF, but writes (mark-checked and
# delete) are flushed in batches so nothing large accumulates in memory.
BATCH_SIZE = 5000


@shared_task(name="bims.tasks.clear_gbif_deleted_occurrences", queue="update")
def clear_gbif_deleted_occurrences(
    dry_run: bool = True,
    stale_days: int = 30,
    limit: int = 0,
    timeout: int = 30,
) -> dict:
    """
    Check GBIF-sourced occurrences against GBIF and report/delete the ones that
    have been removed upstream.

    Records that still exist upstream have their modified_date bumped to now,
    so they rotate out of the stale window and are not re-checked on the next
    run (regardless of dry_run). Records deleted upstream are left as-is so, on
    a dry run, they keep being reported until they are actually removed.

    :param dry_run: If True, do not delete anything; only report. A CSV of the
        affected records is emailed to superusers.
    :param stale_days: Only check occurrences whose modified_date is older than
        this many days (0 = no age filter). Never-checked records (NULL
        modified_date) always qualify.
    :param limit: Max number of records to check (0 = no limit). Records are
        checked oldest-modified first.
    :param timeout: Per-request timeout in seconds.
    """
    from bims.models.biological_collection_record import (
        BiologicalCollectionRecord,
    )

    session = _build_session()
    domain_name = get_domain_name()

    qs = BiologicalCollectionRecord.objects.filter(
        source_collection__icontains='gbif',
        upstream_id__isnull=False,
    ).exclude(upstream_id='')

    if stale_days:
        cutoff = timezone.now() - timedelta(days=stale_days)
        qs = qs.filter(
            Q(modified_date__lt=cutoff) | Q(modified_date__isnull=True))

    qs = qs.order_by(F('modified_date').asc(nulls_first=True), 'id')
    if limit:
        qs = qs[:limit]

    # Running totals and a capped preview, so nothing large is held in memory.
    stats = {'checked': 0, 'to_delete': 0, 'deleted': 0}
    sample = []

    # CSV of affected records, written incrementally as batches are flushed.
    csv_buffer = io.StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=CSV_FIELDNAMES)
    csv_writer.writeheader()

    def _mark_checked(rows):
        """
        Bump modified_date on records that still exist upstream so they rotate
        out of the stale window and are not re-checked every run. Uses update()
        to bypass save() (no side effects, no auto-restamp).
        """
        if rows:
            BiologicalCollectionRecord.objects.filter(
                id__in=[r['local_id'] for r in rows]).update(
                    modified_date=timezone.now())

    def _flush_deleted(rows):
        """Write a batch of upstream-deleted records to the CSV report and,
        unless dry_run, delete them locally."""
        if not rows:
            return
        csv_writer.writerows(rows)
        stats['to_delete'] += len(rows)
        if not dry_run:
            _, detail_map = BiologicalCollectionRecord.objects.filter(
                id__in=[r['local_id'] for r in rows]).delete()
            stats['deleted'] += detail_map.get(
                'bims.BiologicalCollectionRecord', 0)

    alive_batch = []
    deleted_batch = []
    for record in qs.iterator():
        stats['checked'] += 1
        name = (
            record.taxonomy.scientific_name
            if record.taxonomy else record.original_species_name
        )
        row = {
            'local_id': record.id,
            'name': name or '',
            'gbif_id': record.upstream_id,
            'dataset_key': record.dataset_key or '',
            'modified_date': record.modified_date or '',
        }
        if not _occurrence_deleted(session, record.upstream_id, timeout):
            # Still exists upstream: record it as freshly checked.
            alive_batch.append(row)
            if len(alive_batch) >= BATCH_SIZE:
                _mark_checked(alive_batch)
                alive_batch = []
            continue
        if len(sample) < 25:
            sample.append(
                f"{record.id}: {name or '-'} (gbifID={record.upstream_id})")
        deleted_batch.append(row)
        if len(deleted_batch) >= BATCH_SIZE:
            _flush_deleted(deleted_batch)
            deleted_batch = []

    _mark_checked(alive_batch)
    _flush_deleted(deleted_batch)

    to_delete = stats['to_delete']
    deleted = stats['deleted']
    total_checked = stats['checked']

    result = {
        "ok": True,
        "dry_run": dry_run,
        "domain": domain_name,
        "stale_days": stale_days,
        "checked": total_checked,
        "to_delete": to_delete,
        "deleted": deleted,
        "sample_to_delete": sample,
    }

    subject = (
        f"[{domain_name}] GBIF deleted-occurrence cleanup"
        f"{' (DRY RUN)' if dry_run else ''}"
    )
    message = (
        f"GBIF deleted-occurrence cleanup completed"
        f"{' (DRY RUN – no changes made)' if dry_run else ''}.\n\n"
        f"• Stale threshold (days) : {stale_days or 'none'}\n"
        f"• Occurrences checked : {total_checked}\n"
        f"• Deleted upstream in GBIF : {to_delete}\n"
        f"• Removed locally : {deleted}\n"
        f"• Sample (up to 25) : {', '.join(sample) or '-'}\n"
    )

    # On a dry run, attach the full list of affected records as a CSV so
    # superusers can review before running the real deletion.
    attachment = None
    if dry_run and to_delete:
        attachment = (
            'gbif_deleted_occurrences.csv',
            csv_buffer.getvalue(),
            'text/csv',
        )
    mail_superusers(subject=subject, body=message, attachment=attachment)
    logger.info(message.replace("\n", " "))

    return result


def _dataset_deleted(session, dataset_key, timeout):
    """
    True if GBIF no longer has this dataset: either the dataset endpoint
    404s, or it returns 200 with a non-null 'deleted' timestamp (GBIF
    soft-deletes datasets rather than removing the record outright).
    """
    try:
        response = session.get(
            GBIF_API + '/dataset/%s' % dataset_key, timeout=timeout)
        if response.status_code == 404:
            return True
        if response.status_code == 200:
            try:
                return bool(response.json().get('deleted'))
            except ValueError:
                return False
        return False
    except requests.RequestException as e:
        logger.warning("dataset check failed for %s: %s", dataset_key, e)
        return False


@shared_task(name="bims.tasks.clear_gbif_dataset_occurrences", queue="update")
def clear_gbif_dataset_occurrences(
    dry_run: bool = True,
    timeout: int = 30,
) -> dict:
    """
    Check every dataset in the local Dataset table against GBIF. For any
    dataset that no longer exists upstream (404 from GBIF's dataset API, or a
    non-null 'deleted' timestamp), remove all local GBIF-sourced occurrences
    belonging to it and the local Dataset record itself (unless dry_run).

    :param dry_run: If True, do not delete anything; only report.
    :param timeout: Per-request timeout in seconds.
    """
    from bims.models.biological_collection_record import (
        BiologicalCollectionRecord,
    )
    from bims.models.dataset import Dataset

    session = _build_session()
    domain_name = get_domain_name()

    datasets = Dataset.objects.all().order_by('id')

    datasets_checked = 0
    deleted_dataset_keys = []
    to_delete = 0
    deleted = 0
    datasets_removed = 0
    sample = []

    for dataset in datasets.iterator():
        datasets_checked += 1
        dataset_key = str(dataset.uuid)
        if not _dataset_deleted(session, dataset_key, timeout):
            continue

        deleted_dataset_keys.append(dataset_key)
        qs = BiologicalCollectionRecord.objects.filter(
            source_collection__icontains='gbif',
            dataset_key=dataset_key,
        )
        count = qs.count()
        to_delete += count

        for record in qs[:5]:
            if len(sample) >= 25:
                break
            name = (
                record.taxonomy.scientific_name
                if record.taxonomy else record.original_species_name
            )
            sample.append(
                f"{record.id}: {name or '-'} (dataset={dataset_key})")

        if not dry_run:
            _, detail_map = qs.delete()
            deleted += detail_map.get('bims.BiologicalCollectionRecord', 0)

            # Once every occurrence for this dataset is gone locally, remove
            # the local Dataset record too, so it doesn't linger orphaned.
            if not BiologicalCollectionRecord.objects.filter(
                dataset_key=dataset_key
            ).exists():
                _, dataset_detail_map = Dataset.objects.filter(
                    id=dataset.id).delete()
                datasets_removed += dataset_detail_map.get('bims.Dataset', 0)

    result = {
        "ok": True,
        "dry_run": dry_run,
        "domain": domain_name,
        "datasets_checked": datasets_checked,
        "datasets_deleted": deleted_dataset_keys,
        "to_delete": to_delete,
        "deleted": deleted,
        "datasets_removed": datasets_removed,
        "sample_to_delete": sample,
    }

    subject = (
        f"[{domain_name}] GBIF dataset occurrence cleanup"
        f"{' (DRY RUN)' if dry_run else ''}"
    )
    message = (
        f"GBIF dataset occurrence cleanup completed"
        f"{' (DRY RUN – no changes made)' if dry_run else ''}.\n\n"
        f"• Datasets checked : {datasets_checked}\n"
        f"• Datasets no longer on GBIF : {len(deleted_dataset_keys)}\n"
        f"• Occurrences matched : {to_delete}\n"
        f"• Occurrences removed locally : {deleted}\n"
        f"• Local Dataset records removed : {datasets_removed}\n"
        f"• Affected dataset keys : {', '.join(deleted_dataset_keys) or '-'}\n"
        f"• Sample (up to 25) : {', '.join(sample) or '-'}\n"
    )
    mail_superusers(subject=subject, body=message)
    logger.info(message.replace("\n", " "))

    return result
