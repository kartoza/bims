"""
Tests for harvest_collections resume functionality.

Tests cover:
1. area_index tracking and restoration
2. Incomplete zip file detection and reprocessing
3. Batch info parsing from logs
4. Preventing duplicate downloads after resume
"""
import os
import json
import tempfile
import zipfile
from pathlib import Path
from unittest import mock
from django.test import TestCase
from django.core.files.base import ContentFile

from bims.tasks.harvest_collections import (
    find_last_index,
    find_last_batch_info,
    find_last_downloaded_zip,
    harvest_collections
)
from bims.tests.model_factories import (
    HarvestSessionF,
    TaxonomyF,
    TaxonGroupF,
    UserF
)


class TestHarvestResumeHelpers(TestCase):
    """Test helper functions for resume functionality."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.log_file = os.path.join(self.temp_dir, 'test_harvest.log')

    def tearDown(self):
        if os.path.exists(self.log_file):
            os.remove(self.log_file)
        # Clean up any remaining files in temp_dir
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_find_last_index_polygons(self):
        """Test finding the last polygon index from log."""
        log_content = """
2025-11-12 08:38:26,312 - INFO - Areas batch 1: polygons 1-10 of 18 (batch size=10)
2025-11-12 09:06:48,477 - INFO - Areas batch 2: polygons 11-18 of 18 (batch size=8)
"""
        with open(self.log_file, 'w') as f:
            f.write(log_content)

        result = find_last_index(r'polygons (\d+)-(\d+)', self.log_file)
        self.assertEqual(result, 11)  # First index of last batch

    def test_find_last_index_no_match(self):
        """Test find_last_index returns 0 when no match found."""
        log_content = "No polygon data here\n"
        with open(self.log_file, 'w') as f:
            f.write(log_content)

        result = find_last_index(r'polygons (\d+)-(\d+)', self.log_file)
        self.assertEqual(result, 0)

    def test_find_last_batch_info(self):
        """Test parsing batch information from log."""
        log_content = """
2025-11-12 08:38:26,295 - INFO - Fetching GBIF data for Anas sparsa
2025-11-12 08:38:26,312 - INFO - Areas batch 1: polygons 1-10 of 18 (batch size=10)
2025-11-12 08:38:26,312 - INFO - POST https://api.gbif.org/v1/occurrence/download/request
2025-11-12 09:06:48,453 - INFO - Saved to /home/web/media/gbif_downloads/0028406-251025141854904.zip
"""
        with open(self.log_file, 'w') as f:
            f.write(log_content)

        result = find_last_batch_info(self.log_file)
        self.assertIsNotNone(result)
        self.assertEqual(result['batch_no'], 1)
        self.assertEqual(result['first_idx'], 1)
        self.assertEqual(result['last_idx'], 10)
        self.assertEqual(result['total'], 18)

    def test_find_last_batch_info_multiple_batches(self):
        """Test that find_last_batch_info returns the LAST batch."""
        log_content = """
2025-11-12 08:38:26,312 - INFO - Areas batch 1: polygons 1-10 of 18 (batch size=10)
2025-11-12 09:06:48,477 - INFO - Areas batch 2: polygons 11-18 of 18 (batch size=8)
"""
        with open(self.log_file, 'w') as f:
            f.write(log_content)

        result = find_last_batch_info(self.log_file)
        self.assertIsNotNone(result)
        self.assertEqual(result['batch_no'], 2)
        self.assertEqual(result['first_idx'], 11)
        self.assertEqual(result['last_idx'], 18)

    def test_find_last_batch_info_no_match(self):
        """Test find_last_batch_info returns None when no batch info found."""
        log_content = "No batch information here\n"
        with open(self.log_file, 'w') as f:
            f.write(log_content)

        result = find_last_batch_info(self.log_file)
        self.assertIsNone(result)

    def test_find_last_downloaded_zip_incomplete(self):
        """Test detection of incomplete zip file (downloaded but not processed)."""
        zip_path = os.path.join(self.temp_dir, '0028406-251025141854904.zip')

        # Create a dummy zip file
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('occurrence.txt', 'gbifID\ttaxonKey\n1\t123\n')

        log_content = f"""
2025-11-12 09:06:48,453 - INFO - Saved to {zip_path}
2025-11-12 09:06:48,477 - INFO - Skipping occurrence 2248813300
2025-11-17 12:50:06,473 - ERROR - Could not read {zip_path}: cursor already closed
"""
        with open(self.log_file, 'w') as f:
            f.write(log_content)

        result = find_last_downloaded_zip(self.log_file)
        self.assertEqual(result, zip_path)

        # Cleanup
        os.remove(zip_path)

    def test_find_last_downloaded_zip_complete(self):
        """Test that completed zip files are not returned."""
        zip_path = os.path.join(self.temp_dir, '0028406-251025141854904.zip')

        # Create a dummy zip file
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('occurrence.txt', 'gbifID\ttaxonKey\n1\t123\n')

        log_content = f"""
2025-11-12 09:06:48,453 - INFO - Saved to {zip_path}
2025-11-12 09:06:49,238 - INFO - -- processed 100 accepted occurrences from archive
"""
        with open(self.log_file, 'w') as f:
            f.write(log_content)

        result = find_last_downloaded_zip(self.log_file)
        self.assertIsNone(result)

        # Cleanup
        os.remove(zip_path)

    def test_find_last_downloaded_zip_file_not_exists(self):
        """Test that non-existent zip files are not returned."""
        log_content = """
2025-11-12 09:06:48,453 - INFO - Saved to /nonexistent/path/file.zip
"""
        with open(self.log_file, 'w') as f:
            f.write(log_content)

        result = find_last_downloaded_zip(self.log_file)
        self.assertIsNone(result)

    def test_find_last_downloaded_zip_new_download_started(self):
        """Test that if a new download started, previous incomplete is not returned."""
        zip_path = os.path.join(self.temp_dir, '0028406-251025141854904.zip')

        # Create a dummy zip file
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('occurrence.txt', 'gbifID\ttaxonKey\n1\t123\n')

        # The log shows: download saved, then a NEW download request was made
        # This means we moved on from the first zip, so it shouldn't be returned as incomplete
        log_content = f"""
2025-11-12 09:06:48,453 - INFO - Saved to {zip_path}
2025-11-12 09:06:49,238 - INFO - Skipping some occurrences
2025-11-12 09:07:00,000 - INFO - POST https://api.gbif.org/v1/occurrence/download/request
2025-11-12 09:07:01,000 - INFO - Downloading https://api.gbif.org/v1/occurrence/download/request/0032665-251025141854904.zip
"""
        with open(self.log_file, 'w') as f:
            f.write(log_content)

        result = find_last_downloaded_zip(self.log_file)
        # Should be None because a new download was started, meaning we moved past the first one
        self.assertIsNone(result)


class TestHarvestCollectionsResume(TestCase):
    """Test harvest_collections resume functionality."""

    def setUp(self):
        self.user = UserF.create()
        self.taxon_group = TaxonGroupF.create(name='Test Group')
        self.taxonomy1 = TaxonomyF.create(
            col_id='1',
            rank='SPECIES',
            canonical_name='Species One'
        )
        self.taxonomy2 = TaxonomyF.create(
            col_id='2',
            rank='SPECIES',
            canonical_name='Species Two'
        )
        self.taxon_group.taxonomies.add(self.taxonomy1, self.taxonomy2)

        # Create temporary log file
        self.temp_dir = tempfile.mkdtemp()
        self.log_file_path = os.path.join(self.temp_dir, 'harvest.log')

    def tearDown(self):
        # Clean up any remaining files in temp_dir
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_resume_with_area_index_in_additional_data(self):
        """Test that area_index is restored from additional_data."""
        # Create log file
        with open(self.log_file_path, 'w') as f:
            f.write('Initial log\n')

        # Create harvest session with resume state
        resume_state = {
            'area_index': 11,
            'start_taxon_idx': 1
        }

        harvest_session = HarvestSessionF.create(
            module_group=self.taxon_group,
            harvester=self.user,
            status='Fetching GBIF data (1-250/500)',
            finished=False,
            canceled=False,
            log_file=ContentFile(b'Initial log\n', name='harvest.log'),
            additional_data=json.dumps(resume_state)
        )

        # Patch where the function is used, not where it's defined
        with mock.patch('bims.scripts.import_gbif_occurrences.import_gbif_occurrences') as mock_import:
            with mock.patch('bims.signals.utils.disconnect_bims_signals'):
                with mock.patch('bims.signals.utils.connect_bims_signals'):
                    mock_import.return_value = True

                    harvest_collections(
                        session_id=harvest_session.id,
                        resume=True,
                        chunk_size=250,
                        schema_name='public'
                    )

                    # Verify that import_gbif_occurrences was called with area_index=11
                    if mock_import.called:
                        call_kwargs = mock_import.call_args[1]
                        self.assertEqual(call_kwargs.get('area_index'), 11)

    def test_resume_with_incomplete_zip_file(self):
        """Test that incomplete zip files are reprocessed on resume."""
        # Create a dummy zip file
        zip_path = os.path.join(self.temp_dir, '0028406-251025141854904.zip')
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('occurrence.txt',
                       'gbifID\tdecimalLongitude\tdecimalLatitude\t'
                       'coordinateUncertaintyInMeters\teventDate\trecordedBy\t'
                       'institutionCode\treferences\tverbatimLocality\tlocality\t'
                       'species\tdatasetKey\tmodified\tbasisOfRecord\tprojectId\ttaxonKey\n'
                       '1\t30\t-25\t\t2021-01-01\tCollector\tINST\thttp://example.org/1\t'
                       'Site A\tSite A\tSpecies A\tabc123\t2021-01-02\tOBSERVATION\t\t1\n')

        # Create log file with incomplete processing
        log_content = f"""
2025-11-12 08:38:26,312 - INFO - Areas batch 1: polygons 1-10 of 18 (batch size=10)
2025-11-12 09:06:48,453 - INFO - Saved to {zip_path}
2025-11-17 12:50:06,473 - ERROR - Could not read {zip_path}: cursor already closed
"""
        with open(self.log_file_path, 'w') as f:
            f.write(log_content)

        resume_state = {
            'last_zip_file': zip_path,
            'area_index': 1
        }

        harvest_session = HarvestSessionF.create(
            module_group=self.taxon_group,
            harvester=self.user,
            status='Fetching GBIF data (1-250/500)',
            finished=False,
            canceled=False,
            log_file=ContentFile(log_content.encode(), name='harvest.log'),
            additional_data=json.dumps(resume_state)
        )

        # Patch where functions are used
        with mock.patch('bims.scripts.import_gbif_occurrences.process_gbif_response') as mock_process:
            with mock.patch('bims.scripts.import_gbif_occurrences.import_gbif_occurrences') as mock_import:
                with mock.patch('bims.signals.utils.disconnect_bims_signals'):
                    with mock.patch('bims.signals.utils.connect_bims_signals'):
                        # Mock successful reprocessing
                        mock_process.return_value = (None, 50)  # No error, 50 records
                        mock_import.return_value = True

                        harvest_collections(
                            session_id=harvest_session.id,
                            resume=True,
                            chunk_size=250,
                            schema_name='public'
                        )

                        # Verify that process_gbif_response was called to reprocess the zip
                        mock_process.assert_called_once()
                        call_args = mock_process.call_args[0]
                        self.assertEqual(str(call_args[0]), zip_path)

        # Cleanup
        os.remove(zip_path)

    def test_area_index_updated_after_successful_zip_reprocessing(self):
        """Test that area_index is updated to next batch after reprocessing."""
        # Create a taxonomy so the main loop runs at least once
        taxonomy = TaxonomyF.create(
            scientific_name='Test Species',
            canonical_name='Test Species',
            col_id='12345'
        )
        self.taxon_group.taxonomies.add(taxonomy)

        # Create a dummy zip file
        zip_path = os.path.join(self.temp_dir, '0028406-251025141854904.zip')
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('occurrence.txt',
                       'gbifID\tdecimalLongitude\tdecimalLatitude\t'
                       'coordinateUncertaintyInMeters\teventDate\trecordedBy\t'
                       'institutionCode\treferences\tverbatimLocality\tlocality\t'
                       'species\tdatasetKey\tmodified\tbasisOfRecord\tprojectId\ttaxonKey\n'
                       '1\t30\t-25\t\t2021-01-01\tCollector\tINST\thttp://example.org/1\t'
                       'Site A\tSite A\tSpecies A\tabc123\t2021-01-02\tOBSERVATION\t\t1\n')

        # Create log file showing batch 1 (polygons 1-10) was downloaded
        log_content = f"""
2025-11-12 08:38:26,312 - INFO - Areas batch 1: polygons 1-10 of 18 (batch size=10)
2025-11-12 09:06:48,453 - INFO - Saved to {zip_path}
2025-11-17 12:50:06,473 - ERROR - Could not read {zip_path}: cursor already closed
"""
        with open(self.log_file_path, 'w') as f:
            f.write(log_content)

        resume_state = {
            'last_zip_file': zip_path,
            'area_index': 1
        }

        harvest_session = HarvestSessionF.create(
            module_group=self.taxon_group,
            harvester=self.user,
            status='Fetching GBIF data (1-250/500)',
            finished=False,
            canceled=False,
            log_file=ContentFile(log_content.encode(), name='harvest.log'),
            additional_data=json.dumps(resume_state)
        )

        # Write log content to the actual log file path so find_last_batch_info can read it
        with open(harvest_session.log_file.path, 'w') as f:
            f.write(log_content)

        def cancel_after_first_call(*args, **kwargs):
            """Cancel the harvest after first call to prevent completion."""
            harvest_session.refresh_from_db()
            harvest_session.canceled = True
            harvest_session.save()
            return True

        with mock.patch('bims.scripts.import_gbif_occurrences.process_gbif_response') as mock_process:
            with mock.patch('bims.scripts.import_gbif_occurrences.import_gbif_occurrences', side_effect=cancel_after_first_call) as mock_import:
                with mock.patch('bims.signals.utils.disconnect_bims_signals'):
                    with mock.patch('bims.signals.utils.connect_bims_signals'):
                        # Mock successful reprocessing
                        mock_process.return_value = (None, 50)

                        harvest_collections(
                            session_id=harvest_session.id,
                            resume=True,
                            chunk_size=250,
                            schema_name='public'
                        )

                        # Refresh session and check area_index was updated
                        harvest_session.refresh_from_db()
                        updated_state = json.loads(harvest_session.additional_data)

                        # After processing batch 1 (polygons 1-10),
                        # area_index should be 11 (next unprocessed polygon)
                        self.assertEqual(updated_state['area_index'], 11)

        # Cleanup
        os.remove(zip_path)

    def test_no_duplicate_download_after_resume(self):
        """Test that the same batch is not downloaded twice after resume."""
        # This is an integration-style test that verifies the fix for the
        # duplicate download issue described in the user's log

        # Create a taxonomy so the main loop runs at least once
        taxonomy = TaxonomyF.create(
            scientific_name='Test Species',
            canonical_name='Test Species',
            col_id='12345'
        )
        self.taxon_group.taxonomies.add(taxonomy)

        zip_path = os.path.join(self.temp_dir, '0028406-251025141854904.zip')
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('occurrence.txt',
                       'gbifID\tdecimalLongitude\tdecimalLatitude\t'
                       'coordinateUncertaintyInMeters\teventDate\trecordedBy\t'
                       'institutionCode\treferences\tverbatimLocality\tlocality\t'
                       'species\tdatasetKey\tmodified\tbasisOfRecord\tprojectId\ttaxonKey\n')

        log_content = f"""
2025-11-12 08:38:26,312 - INFO - Areas batch 1: polygons 1-10 of 18 (batch size=10)
2025-11-12 09:06:48,453 - INFO - Saved to {zip_path}
"""
        with open(self.log_file_path, 'w') as f:
            f.write(log_content)

        # Simulate the state from the log
        harvest_session = HarvestSessionF.create(
            module_group=self.taxon_group,
            harvester=self.user,
            status='Fetching GBIF data (1-250/500)',
            finished=False,
            canceled=False,
            log_file=ContentFile(log_content.encode(), name='harvest.log'),
            additional_data=json.dumps({'area_index': 1})
        )

        # Write log content to the actual log file path so find_last_batch_info can read it
        with open(harvest_session.log_file.path, 'w') as f:
            f.write(log_content)

        def cancel_after_first_call(*args, **kwargs):
            """Cancel the harvest after first call to prevent completion."""
            harvest_session.refresh_from_db()
            harvest_session.canceled = True
            harvest_session.save()
            return True

        with mock.patch('bims.scripts.import_gbif_occurrences.process_gbif_response') as mock_process:
            with mock.patch('bims.scripts.import_gbif_occurrences.import_gbif_occurrences', side_effect=cancel_after_first_call) as mock_import:
                with mock.patch('bims.signals.utils.disconnect_bims_signals'):
                    with mock.patch('bims.signals.utils.connect_bims_signals'):
                        mock_process.return_value = (None, 50)

                        # First resume - should reprocess the zip and update area_index
                        harvest_collections(
                            session_id=harvest_session.id,
                            resume=True,
                            chunk_size=250,
                            schema_name='public'
                        )

                        harvest_session.refresh_from_db()
                        state = json.loads(harvest_session.additional_data)

                        # Verify area_index moved to 11 (next batch starts at polygon 11)
                        self.assertEqual(state['area_index'], 11)

                        # Verify import was called with area_index=11 (not 1)
                        if mock_import.called:
                            call_kwargs = mock_import.call_args[1]
                            self.assertEqual(call_kwargs.get('area_index'), 11)

        # Cleanup
        os.remove(zip_path)

    def test_resume_with_different_batch_size(self):
        """Test that resume continues from correct polygon index when batch size changes.

        Scenario: Original harvest used boundary_batch_size=10 (polygons 1-10).
        On resume with boundary_batch_size=5, it should:
        1. Continue from polygon 11 (not restart or use old batch size)
        2. Use the new batch size of 5 for subsequent batches (11-15, 16-20, etc.)
        """
        # Create a taxonomy so the main loop runs at least once
        taxonomy = TaxonomyF.create(
            scientific_name='Test Species',
            canonical_name='Test Species',
            col_id='12345'
        )
        self.taxon_group.taxonomies.add(taxonomy)

        # Create a dummy zip file representing batch 1 with old batch size of 10
        zip_path = os.path.join(self.temp_dir, '0028406-251025141854904.zip')
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('occurrence.txt',
                       'gbifID\tdecimalLongitude\tdecimalLatitude\t'
                       'coordinateUncertaintyInMeters\teventDate\trecordedBy\t'
                       'institutionCode\treferences\tverbatimLocality\tlocality\t'
                       'species\tdatasetKey\tmodified\tbasisOfRecord\tprojectId\ttaxonKey\n'
                       '1\t30\t-25\t\t2021-01-01\tCollector\tINST\thttp://example.org/1\t'
                       'Site A\tSite A\tSpecies A\tabc123\t2021-01-02\tOBSERVATION\t\t1\n')

        # Log shows batch was processed with batch_size=10 (polygons 1-10 of 20)
        log_content = f"""
2025-11-12 08:38:26,312 - INFO - Areas batch 1: polygons 1-10 of 20 (batch size=10)
2025-11-12 09:06:48,453 - INFO - Saved to {zip_path}
2025-11-17 12:50:06,473 - ERROR - Could not read {zip_path}: cursor already closed
"""

        # Resume state shows the incomplete zip from batch with size 10
        resume_state = {
            'last_zip_file': zip_path,
            'area_index': 1  # Was at polygon 1 when it failed
        }

        harvest_session = HarvestSessionF.create(
            module_group=self.taxon_group,
            harvester=self.user,
            status='Fetching GBIF data (1-250/500)',
            finished=False,
            canceled=False,
            log_file=ContentFile(log_content.encode(), name='harvest.log'),
            additional_data=json.dumps(resume_state)
        )

        # Write log content to the actual log file path
        with open(harvest_session.log_file.path, 'w') as f:
            f.write(log_content)

        def cancel_after_first_call(*args, **kwargs):
            """Cancel the harvest after first call to prevent completion."""
            harvest_session.refresh_from_db()
            harvest_session.canceled = True
            harvest_session.save()
            return True

        with mock.patch('bims.scripts.import_gbif_occurrences.process_gbif_response') as mock_process:
            with mock.patch('bims.scripts.import_gbif_occurrences.import_gbif_occurrences', side_effect=cancel_after_first_call) as mock_import:
                with mock.patch('bims.signals.utils.disconnect_bims_signals'):
                    with mock.patch('bims.signals.utils.connect_bims_signals'):
                        # Mock successful reprocessing
                        mock_process.return_value = (None, 50)

                        # Resume with NEW boundary_batch_size=5 (changed from 10)
                        harvest_collections(
                            session_id=harvest_session.id,
                            resume=True,
                            chunk_size=250,
                            schema_name='public'
                        )

                        # Refresh session and check area_index
                        harvest_session.refresh_from_db()
                        updated_state = json.loads(harvest_session.additional_data)

                        # After reprocessing batch 1 (polygons 1-10 with old size),
                        # area_index should be 11 (next unprocessed polygon)
                        # NOT 6 (which would be 1 + new batch_size of 5)
                        self.assertEqual(updated_state['area_index'], 11,
                                       "area_index should continue from 11 (after old batch of 10), "
                                       "not 6 (which would incorrectly use new batch_size)")

                        # Verify import was called with area_index=11
                        # This means the NEXT batch will start at polygon 11
                        # and will use the new batch_size=5 (polygons 11-15)
                        if mock_import.called:
                            call_kwargs = mock_import.call_args[1]
                            self.assertEqual(call_kwargs.get('area_index'), 11,
                                           "Next batch should start at polygon 11, "
                                           "then use new batch_size for subsequent batches")

        # Cleanup
        os.remove(zip_path)
