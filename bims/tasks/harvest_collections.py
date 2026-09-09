# coding=utf-8
import os

from celery import shared_task
import re
import logging

from django.db import connection
from django.db.models import Max, Subquery, OuterRef
from django_tenants.utils import schema_context

from bims.cache import get_cache, set_cache, delete_cache

logger = logging.getLogger(__name__)


def find_last_index(pattern_str, filepath) -> int:
    # Pattern to match the URL and extract the offset
    pattern = re.compile(pattern_str)

    # Open the file
    with open(filepath, 'rb') as f:
        # Go to the end of the file
        f.seek(0, 2)
        # Get the current position, which is the size of the file
        filesize = f.tell()

        # Starting from the end, read backwards
        linesize = 1024
        for i in range(filesize // linesize + 1):
            # Calculate the start of the next block to read
            start = max(0, filesize - (i + 1) * linesize)
            # Calculate the size of the block to read
            size = min(linesize, filesize - start)

            # Go to the start of the next block
            f.seek(start)
            # Read the block
            block = f.read(size).decode('utf-8', errors='ignore')

            # Since we're reading backwards, reverse the block and search for the pattern
            for line in reversed(block.splitlines()):
                match = pattern.search(line)
                if match:
                    return int(match.group(1))

    return 0


def find_last_batch_info(log_file_path):
    """
    Find information about the last batch that was being processed.
    Returns a dict with 'batch_no', 'first_idx', 'last_idx', 'total' or None.
    """
    import re
    import os

    if not log_file_path or not os.path.exists(log_file_path):
        return None

    # Pattern to match: "Areas batch X: polygons Y-Z of TOTAL (batch size=N)"
    pattern = re.compile(r'Areas batch (\d+): polygons (\d+)-(\d+) of (\d+)')
    last_batch_info = None

    with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            match = pattern.search(line)
            if match:
                last_batch_info = {
                    'batch_no': int(match.group(1)),
                    'first_idx': int(match.group(2)),
                    'last_idx': int(match.group(3)),
                    'total': int(match.group(4))
                }

    return last_batch_info


def find_last_downloaded_zip(log_file_path):
    """
    Find the last downloaded zip file from the log that may not have been fully processed.
    Returns the path to the zip file or None.
    """
    import re
    import os
    from pathlib import Path

    if not log_file_path or not os.path.exists(log_file_path):
        return None

    # Pattern to match: "Saved to /path/to/file.zip"
    pattern = re.compile(r'Saved to (.+\.zip)')
    last_zip = None
    last_download_time = None

    with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            match = pattern.search(line)
            if match:
                zip_path = match.group(1)
                # Check if file exists
                if os.path.exists(zip_path):
                    last_zip = zip_path
                    # Extract timestamp from the line if possible
                    time_match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                    if time_match:
                        last_download_time = time_match.group(1)

    if not last_zip:
        return None

    # Check if there are any "processed X accepted occurrences from archive" messages after this download
    # If not, the file might not have been fully processed
    if last_download_time:
        found_processing = False
        found_new_download = False
        with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            capture = False
            for line in f:
                if last_download_time in line and 'Saved to' in line:
                    capture = True
                    continue
                if capture:
                    if 'processed' in line and 'accepted occurrences from archive' in line:
                        found_processing = True
                        break
                    # If we see another download starting, mark it and stop
                    if 'POST https://api.gbif.org' in line or 'Downloading https://' in line:
                        found_new_download = True
                        break

        # Only return the zip if it wasn't processed AND no new download started
        if not found_processing and not found_new_download:
            return last_zip

    return None


# tasks.py

@shared_task(name='bims.tasks.harvest_collections', queue='update')
def harvest_collections(session_id, resume=False, chunk_size=250, schema_name='public'):
    import re
    import json
    from itertools import islice
    from bims.signals.utils import disconnect_bims_signals, connect_bims_signals
    from bims.utils.logger import log
    from bims.models import HarvestSession
    from django.db.models import Q
    from bims.scripts.import_gbif_occurrences import import_gbif_occurrences, process_gbif_response, log_to_file_or_logger

    with schema_context(schema_name):
        try:
            harvest_session = HarvestSession.objects.get(id=session_id)
        except HarvestSession.DoesNotExist:
            log('Session does not exist')
            return

        disconnect_bims_signals()

        taxonomies_qs = harvest_session.module_group.taxonomies.filter(
            Q(rank='GENUS') | Q(rank='SPECIES') | Q(rank='SUBSPECIES')
        ).order_by('id')

        # Initialise progress variables
        start_taxon_idx = 1
        offset = 0
        area_index = 1

        # Try to load resume state from additional_data
        resume_state = {}
        if resume and harvest_session.additional_data:
            try:
                resume_state = json.loads(
                    harvest_session.additional_data
                ) if isinstance(harvest_session.additional_data, str) else harvest_session.additional_data
            except:
                resume_state = {}

        if resume and harvest_session.status:
            # Resume index in human-friendly (1-based) terms
            match = re.search(r'\((\d+)', harvest_session.status)
            start_taxon_idx = int(match.group(1)) if match else 1

            # Try to get area_index from resume state first
            if 'area_index' in resume_state:
                area_index = resume_state['area_index']
            else:
                # Fallback to parsing log file
                area_index = find_last_index(r'polygons (\d+)-(\d+)', harvest_session.log_file.path) or 1

            # Check for incomplete zip file processing
            last_zip_file = resume_state.get('last_zip_file')

            # If no zip file in resume state, try to find it from the log
            if not last_zip_file and harvest_session.log_file:
                last_zip_file = find_last_downloaded_zip(harvest_session.log_file.path)
                if last_zip_file:
                    log_to_file_or_logger(harvest_session.log_file.path, f'Found incomplete zip file from log: {last_zip_file}')

            if last_zip_file and os.path.exists(last_zip_file):
                log_to_file = lambda m: log_to_file_or_logger(harvest_session.log_file.path, m)
                log_to_file(f'Resuming processing of incomplete zip file: {last_zip_file}')

                # Re-process the last zip file
                from pathlib import Path
                error, data_count = process_gbif_response(
                    Path(last_zip_file),
                    session_id,
                    harvest_session.module_group,
                    None,  # habitat
                    None,  # origin
                    harvest_session.log_file.path
                )

                if error:
                    log_to_file_or_logger(harvest_session.log_file.path, message=f'Error reprocessing zip: {error}\n', is_error=True)
                else:
                    log_to_file(f'Successfully reprocessed {data_count} records from {last_zip_file}')

                    # Calculate the next area_index based on the batch that was completed
                    # Parse the log to find which batch was processed
                    batch_info = find_last_batch_info(harvest_session.log_file.path)
                    if batch_info:
                        last_idx = batch_info['last_idx']
                        # Next area should start from last_idx + 1
                        area_index = last_idx + 1
                        log_to_file(f'Moving to next area: area_index={area_index} (after completing polygons up to {last_idx})')

                        # Update resume state with new area_index
                        resume_state['area_index'] = area_index

                    # Clear the last_zip_file from resume state
                    resume_state['last_zip_file'] = None
                    harvest_session.additional_data = json.dumps(resume_state)
                    harvest_session.save(update_fields=['additional_data'])

        if resume and harvest_session.canceled:
            harvest_session.canceled = False
            harvest_session.save()

        # Skip already-processed taxonomies
        taxonomies_iter = islice(taxonomies_qs, start_taxon_idx - 1, None)
        total_taxa = taxonomies_qs.count()
        processed = start_taxon_idx

        while True:
            chunk = list(islice(taxonomies_iter, chunk_size))
            if not chunk:
                break

            # Halt if user canceled
            if HarvestSession.objects.get(id=session_id).canceled:
                connect_bims_signals()
                return

            # Status message (show first & last canonical names in the chunk)
            chunk_first = chunk[0].canonical_name
            chunk_last = chunk[-1].canonical_name
            harvest_session.status = (
                f'Fetching GBIF data for {chunk_first} … {chunk_last} '
                f'({processed}-{processed + len(chunk) - 1}/{total_taxa})'
            )

            # Save area_index to resume state
            resume_state['area_index'] = area_index
            resume_state['start_taxon_idx'] = processed
            resume_state['last_zip_file'] = None
            harvest_session.additional_data = json.dumps(resume_state)
            harvest_session.save()

            import_gbif_occurrences(
                taxonomy_ids=[t.col_id for t in chunk if t.col_id],
                log_file_path=harvest_session.log_file.path,
                session_id=session_id,
                taxon_group=harvest_session.module_group,
                area_index=area_index
            )

            # Check if user canceled during import
            if HarvestSession.objects.get(id=session_id).canceled:
                connect_bims_signals()
                return

            # Reset per-chunk controls
            area_index = 1
            processed += len(chunk)

        harvest_session.status = 'Finished'
        harvest_session.finished = True
        harvest_session.additional_data = json.dumps({})  # Clear resume state
        harvest_session.save()

        connect_bims_signals()


@shared_task(
    name='bims.tasks.auto_resume_harvest',
    queue='update',
    ignore_result=True)
def auto_resume_harvest():
    from bims.models.harvest_session import HarvestSession
    try:
        # Subquery to get the latest harvest session id for each harvester
        latest_sessions_subquery = HarvestSession.objects.filter(
            harvester=OuterRef('harvester'),
            finished=False,
            canceled=False,
            is_fetching_species=False
        ).order_by('-id').values('id')[:1]

        # Retrieve the latest harvest session for each harvester that meets the conditions
        harvest_sessions = HarvestSession.objects.filter(
            id__in=Subquery(latest_sessions_subquery)
        )

        harvester_keys_label = 'harvester_keys'
        new_harvester_keys = []
        schema_name = connection.schema_name

        for session in harvest_sessions:
            cache_key = f'harvester_{session.id}_last_log'
            last_log_line = read_last_line(session.log_file)

            # Retrieve the last known log line from the cache
            cached_log_line = get_cache(cache_key)
            new_harvester_keys.append(session.id)

            # Compare log lines to determine if the session needs to be resumed
            if cached_log_line and last_log_line == cached_log_line:
                logger.info(f"Resuming harvest session for harvester {session.harvester_id}.")
                harvest_collections.delay(session.id, True, schema_name=schema_name)

            # Update the cache with the current last log line
            set_cache(cache_key, last_log_line)

        # Remove old caches for harvesters that are no longer active
        old_harvester_keys = get_cache(harvester_keys_label, [])
        for old_key in old_harvester_keys:
            if old_key not in new_harvester_keys:
                delete_cache(f'harvester_{old_key}_last_log')

        # Update the cache with the current active harvester keys
        set_cache(harvester_keys_label, new_harvester_keys)

    except Exception as e:
        logger.error(f"Error in auto_resume_harvest: {str(e)}")


import os
import logging

logger = logging.getLogger(__name__)


def read_last_line(log_file):
    """Read the last non-empty line of the log file."""
    try:
        if log_file and log_file.path:
            with open(log_file.path, 'rb') as f:
                f.seek(-1, os.SEEK_END)

                # Handle case where file is empty or has only newlines
                if f.read(1) == b'\n':
                    f.seek(-2, os.SEEK_END)

                # Read backwards until we find a non-empty line
                while f.tell() > 0:
                    f.seek(-2, os.SEEK_CUR)
                    while f.read(1) != b'\n' and f.tell() > 1:
                        f.seek(-2, os.SEEK_CUR)

                    last_line = f.readline().decode().strip()

                    if last_line:  # Return if the line is not empty
                        return last_line

                    f.seek(-2, os.SEEK_CUR)  # Move back before the current line

                # If reached the beginning of the file
                f.seek(0)
                first_line = f.readline().decode().strip()
                return first_line if first_line else ''
        return ''
    except Exception as e:
        logger.error(f"Error reading last line of log file: {str(e)}")
        return ''

@shared_task(
    name='bims.tasks.auto_resume_harvest_all_schemas',
    queue='update',
    ignore_result=True
)
def auto_resume_harvest_all_schemas():
    from django_tenants.utils import get_tenant_model, tenant_context

    for tenant in get_tenant_model().objects.all():
        with tenant_context(tenant):
            auto_resume_harvest()
