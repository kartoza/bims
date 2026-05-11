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
                valid_aphia_id=None, valid_name=None):
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
        "isFreshwater": 0,
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
_PATCH_GBIF       = 'bims.scripts.taxa_upload_worms._try_set_gbif_key'
_PATCH_PREFS      = 'bims.scripts.taxa_upload_worms.preferences'


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
    @mock.patch(_PATCH_GBIF, return_value=False)
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
    @mock.patch(_PATCH_GBIF, return_value=False)
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
    @mock.patch(_PATCH_GBIF, return_value=False)
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
    @mock.patch(_PATCH_GBIF, return_value=False)
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
    # 6. Resume: already-processed IDs are skipped
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_GBIF, return_value=False)
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
