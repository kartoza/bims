# coding=utf-8
"""Celery task: export a published ChecklistVersion as a ColDP ZIP."""
import csv
import io
import logging
import zipfile
from datetime import date

from celery import shared_task
from django.core.files.base import ContentFile

logger = logging.getLogger(__name__)


@shared_task(name='bims.tasks.export_coldp_zip', queue='update', ignore_result=True)
def export_coldp_zip(download_request_id, checklist_version_id):
    from django.utils import timezone

    from bims.models.checklist_version import ChecklistSnapshot, ChecklistVersion
    from bims.models.download_request import DownloadRequest
    from bims.tasks.email_csv import send_csv_via_email

    try:
        dr = DownloadRequest.objects.get(id=download_request_id)
    except DownloadRequest.DoesNotExist:
        logger.error('export_coldp_zip: DownloadRequest %s not found', download_request_id)
        return

    try:
        version = (
            ChecklistVersion.objects
            .select_related('taxon_group', 'license')
            .get(pk=checklist_version_id)
        )
    except ChecklistVersion.DoesNotExist:
        logger.error('export_coldp_zip: ChecklistVersion %s not found', checklist_version_id)
        dr.processing = False
        dr.save(update_fields=['processing'])
        return

    qs = (
        ChecklistSnapshot.objects
        .filter(checklist_version=version)
        .order_by('scientific_name', 'checklist_id')
    )
    total = qs.count()

    NAME_USAGE_COLS = [
        'taxonID', 'parentID', 'basionymID', 'rank', 'scientificName',
        'authorship', 'status', 'nameStatus', 'kingdom', 'phylum',
        'class', 'order', 'family', 'genus', 'remarks', 'referenceID',
    ]
    VERNACULAR_COLS = ['taxonID', 'name', 'language']
    DISTRIBUTION_COLS = ['taxonID', 'area', 'status']

    name_usage_rows = []
    vernacular_rows = []
    distribution_rows = []
    processed = 0

    for row in qs.iterator(chunk_size=500):
        name_usage_rows.append({
            'taxonID':        row.checklist_id,
            'parentID':       row.parent_checklist_id,
            'basionymID':     row.basionym_checklist_id,
            'rank':           row.rank,
            'scientificName': row.scientific_name,
            'authorship':     row.authorship,
            'status':         row.taxonomic_status,
            'nameStatus':     row.name_status,
            'kingdom':        row.kingdom,
            'phylum':         row.phylum,
            'class':          row.klass,
            'order':          row.order,
            'family':         row.family,
            'genus':          row.genus,
            'remarks':        row.remarks,
            'referenceID':    row.reference_id,
        })
        for vn in (row.vernacular_names or []):
            vernacular_rows.append({
                'taxonID':  row.checklist_id,
                'name':     vn.get('name', ''),
                'language': vn.get('language', ''),
            })
        for dist in (row.distributions or []):
            distribution_rows.append({
                'taxonID': row.checklist_id,
                'area':    dist.get('area', ''),
                'status':  dist.get('status', ''),
            })

        processed += 1
        if processed % 200 == 0:
            dr.progress = f'{processed}/{total}'
            dr.progress_updated_at = timezone.now()
            dr.save(update_fields=['progress', 'progress_updated_at'])

    def _write_tsv(cols, rows):
        buf = io.StringIO()
        writer = csv.DictWriter(
            buf, fieldnames=cols, delimiter='\t', extrasaction='ignore'
        )
        writer.writeheader()
        writer.writerows(rows)
        return buf.getvalue().encode('utf-8')

    # Build metadata.yaml
    issued = (
        version.published_at.date().isoformat()
        if version.published_at
        else date.today().isoformat()
    )
    license_str = version.license.identifier if version.license_id else ''
    module_name = version.taxon_group.name if version.taxon_group_id else ''
    metadata_lines = [
        f'title: "{module_name} Checklist {version.version}"',
        f'version: "{version.version}"',
        f'issued: "{issued}"',
        f'license: "{license_str}"',
    ]
    if version.doi:
        metadata_lines.append(f'identifier: "{version.doi}"')
    if version.notes:
        # Indent multi-line notes as YAML block scalar
        indented = '\n  '.join(version.notes.splitlines())
        metadata_lines.append(f'description: |\n  {indented}')
    metadata_yaml = '\n'.join(metadata_lines) + '\n'

    # Assemble ZIP in memory
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('NameUsage.tsv',     _write_tsv(NAME_USAGE_COLS,   name_usage_rows))
        zf.writestr('VernacularName.tsv', _write_tsv(VERNACULAR_COLS,   vernacular_rows))
        zf.writestr('Distribution.tsv',  _write_tsv(DISTRIBUTION_COLS, distribution_rows))
        zf.writestr('metadata.yaml',     metadata_yaml.encode('utf-8'))

    # Persist to disk
    safe_module = module_name.replace(' ', '_')
    safe_version = version.version.replace(' ', '_').replace('/', '-')
    zip_filename = f'coldp_{safe_module}_{safe_version}.zip'
    dr.request_file.save(
        zip_filename,
        ContentFile(zip_buf.getvalue()),
        save=False,
    )
    dr.request_category = f'{module_name} {version.version}'
    dr.progress = f'{total}/{total}'
    dr.progress_updated_at = timezone.now()
    dr.processing = False
    dr.save(update_fields=[
        'request_file',
        'request_category',
        'progress',
        'progress_updated_at',
        'processing',
    ])
    if dr.requester_id and dr.request_file:
        send_csv_via_email.delay(
            user_id=dr.requester_id,
            csv_file=dr.request_file.path,
            file_name=dr.request_category or zip_filename,
            approved=dr.approved,
            download_request_id=dr.id,
        )
    logger.info('export_coldp_zip: wrote %s (%d rows)', zip_filename, total)
