import logging
import csv
import os
import time
import gc
import tempfile
import shutil

from django.utils import timezone

from bims.models.download_request import DownloadRequest
from bims.scripts.collection_csv_keys import PARK_OR_MPA_NAME, END_EMBARGO_DATE

logger = logging.getLogger(__name__)


def queryset_iterator(qs, batch_size=500, gc_collect=True):
    iterator = (
        qs.values_list('pk', flat=True).order_by('pk').distinct().iterator()
    )
    eof = False
    while not eof:
        primary_key_buffer = []
        try:
            while len(primary_key_buffer) < batch_size:
                primary_key_buffer.append(next(iterator))
        except StopIteration:
            eof = True
        for obj in qs.filter(
                pk__in=primary_key_buffer
        ).order_by('pk').iterator(chunk_size=batch_size):
            yield obj
        if gc_collect:
            gc.collect()


HEADER_TITLES = {
    'class_name': 'Class',
    'sub_species': 'SubSpecies',
    'cites_listing': 'CITES listing',
    'park_or_mpa_name': PARK_OR_MPA_NAME,
    'authors': 'Author(s)',
    'end_embargo_date': END_EMBARGO_DATE,
    'gbif_coordinate_uncertainty_m': 'GBIF coordinate uncertainty (m)',
    'gbif_coordinate_precision': 'GBIF coordinate precision',
}


def format_header(header: str) -> str:
    if header in HEADER_TITLES:
        return HEADER_TITLES[header]
    if header.lower() == 'uuid':
        return header.upper()
    header = header.replace('_or_', '/')
    if not header[0].isupper():
        header = header.replace('_', ' ').capitalize()
    return header


def write_to_csv(headers: list,
                 rows: list,
                 path_file: str,
                 current_csv_row: int = 0):
    fmt_map = {h: format_header(h) for h in headers}
    incoming_fmt_headers = [fmt_map[h] for h in headers]

    file_exists = os.path.exists(path_file) and os.path.getsize(path_file) > 0

    if file_exists:
        with open(path_file, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            try:
                existing_header = next(reader)
            except StopIteration:
                existing_header = []
    else:
        existing_header = []

    existing_set = set(existing_header)
    new_cols = [h for h in incoming_fmt_headers if h not in existing_set]
    union_header = existing_header + new_cols if file_exists else incoming_fmt_headers

    def row_to_union(row_dict):
        out = {fmt_map[k]: v for k, v in row_dict.items() if k in fmt_map}
        return [out.get(col, '') for col in union_header]

    if not file_exists or new_cols:
        tmp_path = f"{path_file}.tmp"
        try:
            with open(tmp_path, 'w', newline='', encoding='utf-8') as out_f:
                w = csv.writer(out_f)
                w.writerow(union_header)

                if file_exists:
                    with open(path_file, newline='', encoding='utf-8') as in_f:
                        r = csv.DictReader(in_f)
                        for old in r:
                            w.writerow([old.get(col, '') for col in union_header])

                for row in rows:
                    current_csv_row += 1
                    w.writerow(row_to_union(row))

            os.replace(tmp_path, path_file)
        except Exception as e:
            logger.error(f"Rewrite failed: {e}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise
    else:
        with open(path_file, 'a', newline='', encoding='utf-8', buffering=1) as f:
            w = csv.writer(f)
            for row in rows:
                try:
                    current_csv_row += 1
                    w.writerow(row_to_union(row))
                except Exception as e:
                    logger.error(f"Error writing row {current_csv_row}: {e}")
                    continue
            f.flush()

    return current_csv_row


def count_csv_data_rows(path_file):
    """Return the number of data rows (excluding header) in an existing CSV."""
    if not os.path.exists(path_file) or os.path.getsize(path_file) == 0:
        return 0
    count = 0
    try:
        with open(path_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            for _ in reader:
                count += 1
    except Exception:
        return 0
    return count


def download_collection_records(
        path_file,
        request,
        send_email=False,
        user_id=None,
        process_id=None
):
    from django.contrib.auth import get_user_model
    from bims.serializers.bio_collection_serializer import (
        BioCollectionOneRowSerializer
    )
    from bims.api_views.search import CollectionSearch
    from bims.models import BiologicalCollectionRecord
    from bims.tasks.email_csv import send_csv_via_email
    from preferences import preferences

    project_name = preferences.SiteSetting.project_name

    exclude_fields = []
    headers = []
    added_headers = set()

    if project_name.lower() == 'sanparks':
        exclude_fields = [
            'user_river_name',
            'river_name',
            'user_wetland_name',
            'wetland_name',
            'user_geomorphological_zone',
            'hydroperiod',
            'wetland_indicator_status',
            'broad_biotope',
            'specific_biotope',
            'substratum',
            'analyst',
            'analyst_institute',
            'sampling_effort_measure',
            'sampling_effort_value',
            'abundance_value',
            'abundance_measure'
        ]
    else:
        exclude_fields = [
            'gbif_coordinate_uncertainty_m',
            'gbif_coordinate_precision',
        ]

    def get_download_request(request_id):
        try:
            return DownloadRequest.objects.get(
                id=request_id
            )
        except DownloadRequest.DoesNotExist:
            return None

    start = time.time()

    filters = request
    download_request_id = filters.get('downloadRequestId', '')
    download_request = get_download_request(download_request_id)

    site_results = None
    search = CollectionSearch(filters, user_id if user_id else None)
    collection_results = search.process_search()
    total_records = collection_results.count()

    if not collection_results and site_results:
        site_ids = site_results.values_list('id', flat=True)
        collection_results = BiologicalCollectionRecord.objects.filter(
            site__id__in=site_ids
        ).distinct()

    # Support resuming a partially completed download
    rows_already_written = count_csv_data_rows(path_file)
    current_csv_row = rows_already_written
    record_number = min(total_records, 500)
    collection_data = []

    if download_request and download_request.rejected:
        return

    # When resuming, skip records already written to the file
    if rows_already_written > 0 and rows_already_written < total_records:
        logger.debug('Resuming download from row %d / %d', rows_already_written, total_records)
        try:
            resume_pk = collection_results.order_by('pk').values_list(
                'pk', flat=True
            )[rows_already_written - 1]
            collection_results = collection_results.filter(pk__gt=resume_pk)
        except (IndexError, Exception) as e:
            logger.warning('Could not determine resume position, restarting: %s', e)
            rows_already_written = 0
            current_csv_row = 0
    elif rows_already_written >= total_records:
        logger.debug('Download already complete (%d rows), sending email', rows_already_written)
        if download_request:
            download_request.progress = f'{total_records}/{total_records}'
            download_request.progress_updated_at = timezone.now()
            download_request.save(update_fields=['progress', 'progress_updated_at'])
        if send_email and user_id:
            from django.contrib.auth import get_user_model
            from bims.tasks.email_csv import send_csv_via_email
            UserModel = get_user_model()
            try:
                user = UserModel.objects.get(id=user_id)
                send_csv_via_email(
                    user_id=user.id,
                    file_name='Occurrence Data',
                    csv_file=path_file,
                    download_request_id=download_request_id
                )
            except UserModel.DoesNotExist:
                pass
        return

    from bims.models.taxon_group import TaxonGroup
    upload_template_headers = []

    def _extend_headers(headers):
        if not headers:
            return
        seen = set(upload_template_headers)
        for h in headers:
            if h and h not in seen:
                upload_template_headers.append(h)
                seen.add(h)

    # Results can span several taxon groups (e.g. the summary dashboard),
    # so collect template headers from every group present.
    taxon_group_ids = collection_results.order_by().values_list(
        'module_group_id', flat=True
    ).distinct()
    taxon_groups = TaxonGroup.objects.filter(
        id__in=taxon_group_ids
    ).prefetch_related('occurrence_upload_templates').order_by('id')

    for taxon_group in taxon_groups:
        legacy_field = getattr(taxon_group, 'occurrence_upload_template', None)
        if legacy_field:
            try:
                with open(legacy_field.path, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    _extend_headers(reader.fieldnames)
            except (FileNotFoundError, UnicodeDecodeError, AttributeError, ValueError):
                pass

        for tpl in taxon_group.occurrence_upload_templates.all():
            try:
                with open(tpl.file.path, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    _extend_headers(reader.fieldnames)
            except (FileNotFoundError, UnicodeDecodeError, AttributeError, ValueError):
                continue

    def write_batch_to_csv(header, rows, _path_file, _start_index):
        bio_serializer = BioCollectionOneRowSerializer(
            rows, many=True,
            context={
                'header': header,
                'exclude_fields': exclude_fields,
                'upload_template_headers': upload_template_headers,
                'added_headers': added_headers,
            }
        )
        bio_data = bio_serializer.data

        header = bio_serializer.context['header']

        present_cols = set()
        for r in bio_data:
            present_cols.update(r.keys())

        filtered_header = [h for h in header if h in present_cols]

        if PARK_OR_MPA_NAME in filtered_header:
            i = filtered_header.index(PARK_OR_MPA_NAME)
            if i != 1:
                filtered_header.insert(1, filtered_header.pop(i))

        csv_row = write_to_csv(
            filtered_header,
            bio_data,
            _path_file,
            _start_index
        )
        del bio_serializer
        return csv_row, filtered_header

    for obj in queryset_iterator(collection_results, batch_size=record_number):
        collection_data.append(obj)
        if len(collection_data) >= record_number:
            start_index = current_csv_row
            current_csv_row, headers = write_batch_to_csv(
                headers,
                collection_data,
                path_file,
                current_csv_row
            )

            logger.debug('Serialize time {0}:{1}: {2}'.format(
                start_index,
                current_csv_row,
                round(time.time() - start, 2))
            )

            del collection_data
            collection_data = []

            gc.collect()

            download_request = get_download_request(download_request_id)

            if download_request.rejected:
                logger.debug('Download request is rejected, closing.')
                try:
                    os.remove(path_file)
                except Exception: # noqa
                    pass
                return
            else:
                download_request.progress = f'{current_csv_row}/{total_records}'
                download_request.progress_updated_at = timezone.now()
                download_request.save()

    if collection_data:
        start_index = current_csv_row
        current_csv_row, headers = write_batch_to_csv(
            headers,
            collection_data,
            path_file,
            current_csv_row
        )
        logger.debug('Serialize time {0}:{1}: {2}'.format(
            start_index,
            current_csv_row,
            round(time.time() - start, 2))
        )

    logger.debug('Serialize time : {}'.format(
        round(time.time() - start, 2))
    )

    if download_request:
        download_request = get_download_request(download_request_id)
        download_request.progress = f'{current_csv_row}/{total_records}'
        download_request.progress_updated_at = timezone.now()
        download_request.save()

    logger.debug(
        'Write csv time : {}'.format(
            round(time.time() - start, 2)
        )
    )

    if send_email and user_id:
        UserModel = get_user_model()
        try:
            user = UserModel.objects.get(id=user_id)
            send_csv_via_email(
                user_id=user.id,
                file_name='Occurrence Data',
                csv_file=path_file,
                download_request_id=download_request_id
            )
        except UserModel.DoesNotExist:
            pass
    return
