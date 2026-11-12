# coding=utf-8
"""
Tests for bims.tasks.harvest_worms_species.
"""
from unittest import mock

from django.core.files.base import ContentFile
from django.db import connection
from django_tenants.test.cases import FastTenantTestCase

from bims.models.harvest_session import HarvestSession
from bims.models import Taxonomy
from bims.tests.model_factories import TaxonGroupF, UserF
from bims.tasks.harvest_worms_species import harvest_worms_species


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _api_record(aphia_id, sci_name, rank, status,
                genus="", family="Accipitridae",
                valid_aphia_id=None, valid_name=None,
                is_freshwater=1):
    """Minimal WoRMS REST-API-style record dict."""
    return {
        "AphiaID": aphia_id,
        "scientificname": sci_name,
        "authority": "",
        "rank": rank,
        "status": status,
        "valid_AphiaID": valid_aphia_id or aphia_id,
        "valid_name": valid_name or sci_name,
        "valid_authority": "",
        "qualitystatus": "checked",
        "unacceptreason": None,
        "kingdom": "Animalia",
        "phylum": "Chordata",
        "class": "Aves",
        "order": "Accipitriformes",
        "family": family,
        "genus": genus,
        "subgenus": None,
        "isMarine": 0,
        "isBrackish": 0,
        "isFreshwater": is_freshwater,
        "isTerrestrial": 1,
        "lsid": f"urn:lsid:marinespecies.org:taxname:{aphia_id}",
        "citation": "",
        "modified": "2020-01-01T00:00:00.000+0000",
        "parentNameUsageID": None,
        "url": f"https://www.marinespecies.org/aphia.php?p=taxdetails&id={aphia_id}",
    }


# Patch targets used repeatedly across tests
_PATCH_DISCONNECT = 'bims.signals.utils.disconnect_bims_signals'
_PATCH_CONNECT    = 'bims.signals.utils.connect_bims_signals'
_PATCH_RECORD     = 'bims.utils.worms.get_aphia_record'
_PATCH_CHILDREN   = 'bims.utils.worms.get_aphia_children'
_PATCH_COL_ID     = 'bims.scripts.taxa_upload_worms._try_set_col_id'
_PATCH_PREFS      = 'bims.scripts.taxa_upload_worms.preferences'
_PATCH_SLEEP = 'bims.tasks.harvest_worms_species.time.sleep'


class TestHarvestWormsSpeciesTask(FastTenantTestCase):

    def setUp(self):
        self.taxon_group = TaxonGroupF.create()
        self.user = UserF.create()
        self.schema_name = connection.schema_name

    # ------------------------------------------------------------------
    # Utility: create a session with a log file
    # ------------------------------------------------------------------

    def _make_session(self, aphia_id=1836, additional=None):
        data = additional if additional is not None else {'aphia_id': aphia_id}
        session = HarvestSession.objects.create(
            harvester=self.user,
            module_group=self.taxon_group,
            category='worms',
            additional_data=data,
        )
        session.log_file.save(
            f'worms-test-{session.id}.log',
            ContentFile(b''),
        )
        return session

    # ------------------------------------------------------------------
    # 1. Happy path: session is marked finished
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_COL_ID, return_value=False)
    @mock.patch(_PATCH_CHILDREN, return_value=[])
    @mock.patch(_PATCH_RECORD)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_session_marked_finished(
        self, mock_dis, mock_con, mock_record, mock_children, mock_gbif, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        mock_record.return_value = _api_record(1836, 'Aves', 'class', 'accepted')

        session = self._make_session(1836)
        harvest_worms_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertIn('Finished', session.status)

    # ------------------------------------------------------------------
    # 2. BFS traverses children and taxa land in DB
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_COL_ID, return_value=False)
    @mock.patch(_PATCH_CHILDREN)
    @mock.patch(_PATCH_RECORD)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_children_are_processed(
        self, mock_dis, mock_con, mock_record, mock_children, mock_gbif, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True

        root = _api_record(1836, 'Aves', 'class', 'accepted')
        child_a = _api_record(2001, 'Vultur', 'genus', 'accepted',
                              genus='Vultur', family='Cathartidae')
        child_b = _api_record(2002, 'Neophron', 'genus', 'accepted',
                              genus='Neophron')

        mock_record.return_value = root

        def _children(aphia_id, **kw):
            if aphia_id == 1836:
                return [child_a, child_b]
            return []

        mock_children.side_effect = _children

        session = self._make_session(1836)
        harvest_worms_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertTrue(session.finished)
        # Root + 2 accepted children = 3
        self.assertIn('3', session.status)
        self.assertTrue(Taxonomy.objects.filter(canonical_name='Vultur').exists())
        self.assertTrue(Taxonomy.objects.filter(canonical_name='Neophron').exists())

    # ------------------------------------------------------------------
    # 3. Missing AphiaID → session finishes with failure status
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_missing_aphia_id_marks_failed(self, mock_dis, mock_con):
        session = self._make_session(additional={})  # no aphia_id key
        harvest_worms_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertIn('no AphiaID', session.status)

    # ------------------------------------------------------------------
    # 4. Synonyms are not recursed into
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_COL_ID, return_value=False)
    @mock.patch(_PATCH_CHILDREN)
    @mock.patch(_PATCH_RECORD)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_synonyms_not_added_to_bfs_queue(
        self, mock_dis, mock_con, mock_record, mock_children, mock_gbif, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True

        root = _api_record(1836, 'Aves', 'class', 'accepted')
        synonym = _api_record(
            3001, 'Vultur ginginianus', 'species', 'unaccepted',
            genus='Vultur',
            valid_aphia_id=3002,
            valid_name='Neophron percnopterus ginginianus',
        )

        mock_record.return_value = root
        recursed_ids = []

        def _children(aphia_id, **kw):
            recursed_ids.append(aphia_id)
            if aphia_id == 1836:
                return [synonym]
            return []

        mock_children.side_effect = _children

        session = self._make_session(1836)
        harvest_worms_species(session.id, schema_name=self.schema_name)

        # Only the root should have been recursed; not the synonym child
        self.assertNotIn(3001, recursed_ids)

    # ------------------------------------------------------------------
    # 5. Cancellation: session not marked finished
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_COL_ID, return_value=False)
    @mock.patch(_PATCH_CHILDREN)
    @mock.patch(_PATCH_RECORD)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_canceled_session_not_finished(
        self, mock_dis, mock_con, mock_record, mock_children, mock_gbif, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True

        root = _api_record(1836, 'Aves', 'class', 'accepted')
        mock_record.return_value = root

        # Make children mark session as canceled mid-traversal
        session = self._make_session(1836)

        def _children(aphia_id, **kw):
            HarvestSession.objects.filter(id=session.id).update(canceled=True)
            return []

        mock_children.side_effect = _children

        harvest_worms_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertFalse(session.finished)
        self.assertIn('Canceled', session.status)

    # ------------------------------------------------------------------
    # 6. Freshwater-only filter: non-freshwater children are skipped
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_COL_ID, return_value=False)
    @mock.patch(_PATCH_CHILDREN)
    @mock.patch(_PATCH_RECORD)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_freshwater_only_skips_non_freshwater(
        self, mock_dis, mock_con, mock_record, mock_children, mock_gbif, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True

        root = _api_record(1836, 'Aves', 'class', 'accepted', is_freshwater=1)
        freshwater_child = _api_record(
            2001, 'Freshius', 'genus', 'accepted', genus='Freshius', is_freshwater=1
        )
        marine_child = _api_record(
            2002, 'Marinus', 'genus', 'accepted', genus='Marinus', is_freshwater=0
        )

        mock_record.return_value = root

        def _children(aphia_id, **kw):
            if aphia_id == 1836:
                return [freshwater_child, marine_child]
            return []

        mock_children.side_effect = _children

        session = self._make_session(
            additional={'aphia_id': 1836, 'freshwater_only': True}
        )
        harvest_worms_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertTrue(Taxonomy.objects.filter(canonical_name='Freshius').exists())
        self.assertFalse(Taxonomy.objects.filter(canonical_name='Marinus').exists())

    # ------------------------------------------------------------------
    # 7. Resume: already-processed IDs are skipped
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_COL_ID, return_value=False)
    @mock.patch(_PATCH_CHILDREN)
    @mock.patch(_PATCH_RECORD)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_already_processed_ids_skipped(
        self, mock_dis, mock_con, mock_record, mock_children, mock_gbif, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True

        child = _api_record(2001, 'Vultur', 'genus', 'accepted', genus='Vultur')
        root  = _api_record(1836, 'Aves', 'class', 'accepted')

        mock_record.return_value = root

        def _children(aphia_id, **kw):
            if aphia_id == 1836:
                return [child]
            return []

        mock_children.side_effect = _children

        # Pre-seed processed list to include the child
        session = self._make_session(
            additional={'aphia_id': 1836, 'processed_aphia_ids': [1836, 2001]}
        )

        with mock.patch(
            'bims.tasks.harvest_worms_species._SessionWormsTaxaProcessor.process'
        ) as mock_process:
            harvest_worms_species(session.id, schema_name=self.schema_name)
            # Neither root nor child should be processed again
            mock_process.assert_not_called()

        session.refresh_from_db()
        self.assertTrue(session.finished)

    # ------------------------------------------------------------------
    # Unaccepted names are skipped unless include_unaccepted is set
    # ------------------------------------------------------------------

    def _run_with_unaccepted_child(self, mock_record, mock_children, additional):
        root = _api_record(1836, 'Aves', 'class', 'accepted')
        accepted = _api_record(
            4001, 'Neophron percnopterus', 'species', 'accepted', genus='Neophron',
        )
        unaccepted = _api_record(
            4002, 'Vultur percnopterus', 'species', 'unaccepted',
            genus='Vultur', valid_aphia_id=4001,
            valid_name='Neophron percnopterus',
        )
        mock_record.side_effect = lambda aphia_id, **kw: {
            1836: root, 4001: accepted,
        }.get(aphia_id)
        mock_children.side_effect = (
            lambda aphia_id, **kw: [accepted, unaccepted] if aphia_id == 1836 else []
        )
        session = self._make_session(additional=additional)
        harvest_worms_species(session.id, schema_name=self.schema_name)
        session.refresh_from_db()
        return session

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_COL_ID, return_value=False)
    @mock.patch(_PATCH_CHILDREN)
    @mock.patch(_PATCH_RECORD)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_unaccepted_skipped_by_default(
        self, mock_dis, mock_con, mock_record, mock_children, mock_col, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        session = self._run_with_unaccepted_child(
            mock_record, mock_children, {'aphia_id': 1836},
        )

        self.assertIn('Finished', session.status)
        self.assertTrue(
            Taxonomy.objects.filter(canonical_name='Neophron percnopterus').exists()
        )
        self.assertFalse(
            Taxonomy.objects.filter(canonical_name='Vultur percnopterus').exists()
        )
        # Marked processed so a resume doesn't revisit it
        self.assertIn(4002, session.additional_data['processed_aphia_ids'])
        with open(session.log_file.path) as fh:
            self.assertIn('AphiaID=4002 (Vultur percnopterus, unaccepted)', fh.read())

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_COL_ID, return_value=False)
    @mock.patch(_PATCH_CHILDREN)
    @mock.patch(_PATCH_RECORD)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_unaccepted_harvested_when_enabled(
        self, mock_dis, mock_con, mock_record, mock_children, mock_col, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        self._run_with_unaccepted_child(
            mock_record, mock_children,
            {'aphia_id': 1836, 'include_unaccepted': True},
        )

        self.assertTrue(
            Taxonomy.objects.filter(canonical_name='Vultur percnopterus').exists()
        )

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_COL_ID, return_value=False)
    @mock.patch(_PATCH_CHILDREN, return_value=[])
    @mock.patch(_PATCH_RECORD)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_unaccepted_root_skipped_by_default(
        self, mock_dis, mock_con, mock_record, mock_children, mock_col, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        mock_record.return_value = _api_record(
            5001, 'Oldname', 'genus', 'unaccepted',
            valid_aphia_id=5002, valid_name='Newname',
        )
        session = self._make_session(5001)
        harvest_worms_species(session.id, schema_name=self.schema_name)

        self.assertFalse(Taxonomy.objects.filter(canonical_name='Oldname').exists())


    # ------------------------------------------------------------------
    # WoRMS request failures must not silently drop part of the tree
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_COL_ID, return_value=False)
    @mock.patch(_PATCH_CHILDREN)
    @mock.patch(_PATCH_RECORD)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_children_fetch_failure_marks_failed_and_resume_completes(
        self, mock_dis, mock_con, mock_record, mock_children, mock_col,
        mock_prefs, mock_sleep,
    ):
        from bims.tasks.harvest_worms_species import FETCH_ATTEMPTS
        from bims.utils.worms import WormsAPIError

        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        root = _api_record(1836, 'Aves', 'class', 'accepted')
        genus = _api_record(6001, 'Neophron', 'genus', 'accepted', genus='Neophron')
        species = _api_record(
            6002, 'Neophron percnopterus', 'species', 'accepted', genus='Neophron',
        )
        mock_record.return_value = root
        tree = {1836: [genus], 6001: [species]}
        calls = {6001: 0}

        def _failing_children(aphia_id, **kw):
            if aphia_id == 6001:
                calls[6001] += 1
                raise WormsAPIError('read timed out')
            return tree.get(aphia_id, [])

        mock_children.side_effect = _failing_children
        session = self._make_session(1836)
        harvest_worms_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertEqual(calls[6001], FETCH_ATTEMPTS)
        self.assertTrue(session.finished)
        self.assertTrue(session.status.startswith('Failed'))
        self.assertIn('AphiaID=6001', session.status)
        self.assertEqual(session.additional_data['failed_aphia_id'], 6001)
        self.assertIn('last_error', session.additional_data)
        self.assertIn(6001, session.additional_data['processed_aphia_ids'])
        self.assertFalse(
            Taxonomy.objects.filter(canonical_name='Neophron percnopterus').exists()
        )

        # Resuming (as the admin action does) re-walks the tree and picks up
        # the subtree that failed.
        mock_children.side_effect = lambda aphia_id, **kw: tree.get(aphia_id, [])
        HarvestSession.objects.filter(id=session.id).update(finished=False)
        harvest_worms_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertIn('Finished', session.status)
        self.assertTrue(
            Taxonomy.objects.filter(canonical_name='Neophron percnopterus').exists()
        )

    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_COL_ID, return_value=False)
    @mock.patch(_PATCH_CHILDREN)
    @mock.patch(_PATCH_RECORD)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_transient_children_failure_is_retried(
        self, mock_dis, mock_con, mock_record, mock_children, mock_col,
        mock_prefs, mock_sleep,
    ):
        from bims.utils.worms import WormsAPIError

        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        mock_record.return_value = _api_record(1836, 'Aves', 'class', 'accepted')
        child = _api_record(6101, 'Gyps', 'genus', 'accepted', genus='Gyps')
        failures = {'left': 2}

        def _children(aphia_id, **kw):
            if aphia_id == 1836:
                if failures['left']:
                    failures['left'] -= 1
                    raise WormsAPIError('503')
                return [child]
            return []

        mock_children.side_effect = _children
        session = self._make_session(1836)
        harvest_worms_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertIn('Finished', session.status)
        self.assertTrue(Taxonomy.objects.filter(canonical_name='Gyps').exists())

    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_COL_ID, return_value=False)
    @mock.patch(_PATCH_CHILDREN)
    @mock.patch(_PATCH_RECORD)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_root_fetch_failure_marks_failed(
        self, mock_dis, mock_con, mock_record, mock_children, mock_col,
        mock_prefs, mock_sleep,
    ):
        from bims.utils.worms import WormsAPIError

        mock_record.side_effect = WormsAPIError('connection reset')
        session = self._make_session(1836)
        harvest_worms_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertTrue(session.status.startswith('Failed'))
        self.assertNotIn(1836, session.additional_data['processed_aphia_ids'])
        mock_children.assert_not_called()

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_COL_ID, return_value=False)
    @mock.patch(_PATCH_CHILDREN)
    @mock.patch(_PATCH_RECORD)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_processing_error_still_walks_subtree(
        self, mock_dis, mock_con, mock_record, mock_children, mock_col, mock_prefs
    ):
        from bims.tasks.harvest_worms_species import _SessionWormsTaxaProcessor

        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        mock_record.return_value = _api_record(1836, 'Aves', 'class', 'accepted')
        bad_genus = _api_record(6201, 'Badgenus', 'genus', 'accepted', genus='Badgenus')
        species = _api_record(
            6202, 'Badgenus goodspecies', 'species', 'accepted', genus='Badgenus',
        )
        tree = {1836: [bad_genus], 6201: [species]}
        mock_children.side_effect = lambda aphia_id, **kw: tree.get(aphia_id, [])

        original = _SessionWormsTaxaProcessor.process

        def _process(self_, row, *args, **kwargs):
            if row.get('AphiaID') == 6201 or str(row.get('AphiaID')) == '6201':
                raise RuntimeError('boom')
            return original(self_, row, *args, **kwargs)

        session = self._make_session(1836)
        with mock.patch.object(_SessionWormsTaxaProcessor, 'process', _process):
            harvest_worms_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertIn(6201, [c.args[0] for c in mock_children.call_args_list])
        self.assertIn('1 record(s) failed to process', session.status)
        self.assertNotIn(6201, session.additional_data['processed_aphia_ids'])
        self.assertIn('boom', session.additional_data['failed_aphia_ids']['6201'])
        with open(session.log_file.path) as fh:
            self.assertIn('Retry failed for AphiaID=6201: boom', fh.read())

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_COL_ID, return_value=False)
    @mock.patch(_PATCH_CHILDREN)
    @mock.patch(_PATCH_RECORD)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_transient_processing_failure_succeeds_on_retry(
        self, mock_dis, mock_con, mock_record, mock_children, mock_col, mock_prefs
    ):
        from bims.tasks.harvest_worms_species import _SessionWormsTaxaProcessor

        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        mock_record.return_value = _api_record(1836, 'Aves', 'class', 'accepted')
        genus = _api_record(6301, 'Flakygenus', 'genus', 'accepted', genus='Flakygenus')
        mock_children.side_effect = (
            lambda aphia_id, **kw: [genus] if aphia_id == 1836 else []
        )

        original = _SessionWormsTaxaProcessor.process
        failures = {'left': 1}

        def _process(self_, row, *args, **kwargs):
            if str(row.get('AphiaID')) == '6301' and failures['left']:
                failures['left'] -= 1
                raise RuntimeError('deadlock detected')
            return original(self_, row, *args, **kwargs)

        session = self._make_session(1836)
        with mock.patch.object(_SessionWormsTaxaProcessor, 'process', _process):
            harvest_worms_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertIn('Finished', session.status)
        self.assertNotIn('failed to process', session.status)
        self.assertEqual(session.additional_data['failed_aphia_ids'], {})
        self.assertIn(6301, session.additional_data['processed_aphia_ids'])
        self.assertTrue(
            Taxonomy.objects.filter(canonical_name='Flakygenus', rank='GENUS').exists()
        )

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_COL_ID, return_value=False)
    @mock.patch(_PATCH_CHILDREN, return_value=[])
    @mock.patch(_PATCH_RECORD)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_failures_from_earlier_run_are_refetched_and_retried(
        self, mock_dis, mock_con, mock_record, mock_children, mock_col, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        root = _api_record(1836, 'Aves', 'class', 'accepted')
        earlier = _api_record(6401, 'Retriedgenus', 'genus', 'accepted', genus='Retriedgenus')
        mock_record.side_effect = lambda aphia_id, **kw: {
            1836: root, 6401: earlier,
        }.get(aphia_id)

        session = self._make_session(additional={
            'aphia_id': 1836,
            'processed_aphia_ids': [1836],
            'failed_aphia_ids': {'6401': 'boom', '6402': 'boom'},
        })
        harvest_worms_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertTrue(
            Taxonomy.objects.filter(canonical_name='Retriedgenus', rank='GENUS').exists()
        )
        # 6402 no longer exists in WoRMS (no record), so it is dropped
        self.assertEqual(session.additional_data['failed_aphia_ids'], {})
        self.assertIn('Finished', session.status)


# ---------------------------------------------------------------------------
# View: include_unaccepted option
# ---------------------------------------------------------------------------

class TestHarvestWormsSpeciesViewOptions(FastTenantTestCase):

    def setUp(self):
        from django.contrib.auth.models import Permission
        from django_tenants.test.client import TenantClient

        self.taxon_group = TaxonGroupF.create()
        self.user = UserF.create(is_superuser=True, is_staff=True)
        self.user.user_permissions.add(
            *Permission.objects.filter(codename='can_harvest_species')
        )
        self.client = TenantClient(self.tenant)
        self.client.force_login(self.user)

    def _post(self, extra):
        from django.urls import reverse
        with mock.patch(
            'bims.views.harvest_worms_species.harvest_worms_species.delay'
        ):
            self.client.post(reverse('harvest-worms-species'), {
                'taxon_group': self.taxon_group.id,
                'aphia_id': '1836',
                **extra,
            })
        return HarvestSession.objects.filter(category='worms').latest('id')

    def test_include_unaccepted_defaults_to_false(self):
        session = self._post({})
        self.assertIs(session.additional_data['include_unaccepted'], False)

    def test_include_unaccepted_when_checked(self):
        session = self._post({'include_unaccepted': '1'})
        self.assertIs(session.additional_data['include_unaccepted'], True)


# ---------------------------------------------------------------------------
# Auto-resume of stalled WoRMS sessions
# ---------------------------------------------------------------------------

_PATCH_WORMS_DELAY = 'bims.tasks.harvest_worms_species.harvest_worms_species.delay'


class TestAutoResumeWormsHarvest(FastTenantTestCase):

    def setUp(self):
        self.taxon_group = TaxonGroupF.create()
        self.user = UserF.create()
        self.schema_name = connection.schema_name

    def _make_session(self, idle_seconds, additional=None, **kwargs):
        import os
        import time
        session = HarvestSession.objects.create(
            harvester=self.user,
            module_group=self.taxon_group,
            category=kwargs.pop('category', 'worms'),
            additional_data=additional if additional is not None else {
                'aphia_id': 1836, 'processed_aphia_ids': [1, 2, 3],
            },
            **kwargs,
        )
        session.log_file.save(
            f'worms-autoresume-{session.id}.log', ContentFile(b'x\n'),
        )
        mtime = time.time() - idle_seconds
        os.utime(session.log_file.path, (mtime, mtime))
        return session

    @mock.patch(_PATCH_WORMS_DELAY)
    def test_stalled_session_is_resumed_once(self, mock_delay):
        from bims.tasks.harvest_worms_species import auto_resume_worms_harvest
        from bims.utils.harvest_resume import STALL_SECONDS

        session = self._make_session(STALL_SECONDS + 60)
        auto_resume_worms_harvest(self.schema_name)

        mock_delay.assert_called_once_with(session.id, schema_name=self.schema_name)
        session.refresh_from_db()
        self.assertEqual(session.additional_data['auto_resume_count'], 1)
        self.assertEqual(session.additional_data['auto_resume_progress'], 3)

        # The resume log line refreshes the heartbeat: no second dispatch.
        auto_resume_worms_harvest(self.schema_name)
        mock_delay.assert_called_once()

    @mock.patch(_PATCH_WORMS_DELAY)
    def test_running_finished_and_other_categories_ignored(self, mock_delay):
        from bims.tasks.harvest_worms_species import auto_resume_worms_harvest
        from bims.utils.harvest_resume import STALL_SECONDS

        self._make_session(60)
        self._make_session(STALL_SECONDS + 60, finished=True)
        self._make_session(STALL_SECONDS + 60, canceled=True)
        self._make_session(STALL_SECONDS + 60, category='taxonworks')
        auto_resume_worms_harvest(self.schema_name)

        mock_delay.assert_not_called()

    @mock.patch(_PATCH_WORMS_DELAY)
    def test_gives_up_when_no_progress(self, mock_delay):
        from bims.tasks.harvest_worms_species import auto_resume_worms_harvest
        from bims.utils.harvest_resume import (
            MAX_AUTO_RESUMES_WITHOUT_PROGRESS, STALL_SECONDS,
        )

        session = self._make_session(STALL_SECONDS + 60, additional={
            'aphia_id': 1836,
            'processed_aphia_ids': [1, 2, 3],
            'auto_resume_progress': 3,
            'auto_resume_count': MAX_AUTO_RESUMES_WITHOUT_PROGRESS,
        })
        auto_resume_worms_harvest(self.schema_name)

        mock_delay.assert_not_called()
        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertTrue(session.status.startswith('Failed after 3 records'))

    @mock.patch(_PATCH_WORMS_DELAY)
    def test_counter_resets_after_progress(self, mock_delay):
        from bims.tasks.harvest_worms_species import auto_resume_worms_harvest
        from bims.utils.harvest_resume import (
            MAX_AUTO_RESUMES_WITHOUT_PROGRESS, STALL_SECONDS,
        )

        session = self._make_session(STALL_SECONDS + 60, additional={
            'aphia_id': 1836,
            'processed_aphia_ids': [1, 2, 3, 4, 5],
            'auto_resume_progress': 3,
            'auto_resume_count': MAX_AUTO_RESUMES_WITHOUT_PROGRESS,
        })
        auto_resume_worms_harvest(self.schema_name)

        mock_delay.assert_called_once()
        session.refresh_from_db()
        self.assertEqual(session.additional_data['auto_resume_count'], 1)


# ---------------------------------------------------------------------------
# WoRMS API client: failures raise instead of returning partial data
# ---------------------------------------------------------------------------

class TestWormsClientErrors(FastTenantTestCase):

    class _Resp:
        def __init__(self, payload, status_code=200):
            self._payload = payload
            self.status_code = status_code

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f'HTTP {self.status_code}')

    @mock.patch('bims.utils.worms.time.sleep', return_value=None)
    @mock.patch('bims.utils.worms._http.get')
    def test_children_failure_mid_paging_raises(self, mock_get, mock_sleep):
        from bims.utils.worms import PAGE_SIZE, WormsAPIError, get_aphia_children

        first_page = [{'AphiaID': i} for i in range(PAGE_SIZE)]
        mock_get.side_effect = [self._Resp(first_page), self._Resp({}, 503)]

        with self.assertRaises(WormsAPIError):
            get_aphia_children(1836)

    @mock.patch('bims.utils.worms.time.sleep', return_value=None)
    @mock.patch('bims.utils.worms._http.get')
    def test_children_no_content_returns_empty(self, mock_get, mock_sleep):
        from bims.utils.worms import get_aphia_children

        mock_get.return_value = self._Resp(None, 204)
        self.assertEqual(get_aphia_children(1836), [])

    @mock.patch('bims.utils.worms._http.get')
    def test_record_failure_raises_only_when_asked(self, mock_get):
        from bims.utils.worms import WormsAPIError, get_aphia_record

        mock_get.return_value = self._Resp({}, 500)
        self.assertIsNone(get_aphia_record(1836))
        with self.assertRaises(WormsAPIError):
            get_aphia_record(1836, raise_on_error=True)
