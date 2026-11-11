# coding=utf-8
from unittest import mock

from django.core.files.base import ContentFile
from django.db import connection
from django_tenants.test.cases import FastTenantTestCase

from bims.models import Taxonomy
from bims.models.harvest_session import HarvestSession
from bims.models.taxon_group_taxonomy import TaxonGroupTaxonomy
from bims.models.taxonomy_update_proposal import TaxonomyUpdateProposal
from bims.scripts.taxa_upload_taxonworks import TaxonWorksTaxaProcessor
from bims.tasks.harvest_taxonworks_species import harvest_taxonworks_species
from bims.tests.model_factories import TaxonGroupF, UserF

_PATCH_DISCONNECT = 'bims.signals.utils.disconnect_bims_signals'
_PATCH_CONNECT = 'bims.signals.utils.connect_bims_signals'
_PATCH_PREFS = 'bims.scripts.taxa_upload_taxonworks.preferences'
_PATCH_HTTP_GET = 'bims.utils.taxonworks._http.get'
_PATCH_SLEEP = 'bims.utils.taxonworks.time.sleep'


def _record(taxon_id, name, rank, parent_id=None, valid=True,
            valid_id=None, author='', updated_at='2024-01-01T00:00:00.000Z',
            extinct=False):
    return {
        "id": taxon_id,
        "name": name.split()[-1] if rank in {'species', 'subspecies'} else name,
        "parent_id": parent_id,
        "cached": name,
        "cached_html": f"† <i>{name}</i>" if extinct else name,
        "rank": rank,
        "rank_string": rank,
        "type": "Protonym",
        "project_id": 55,
        "cached_valid_taxon_name_id": valid_id or taxon_id,
        "cached_author": author,
        "cached_author_year": author,
        "cached_is_valid": valid,
        "created_at": "2023-01-01T00:00:00.000Z",
        "updated_at": updated_at,
        "name_string": f"{name} {author}".strip(),
        "original_combination": None,
    }


class TestHarvestTaxonWorksSpeciesTask(FastTenantTestCase):
    """
    Integration tests for harvest_taxonworks_species.

    The task now works page-by-page:
      1. GET /taxon_names?page=N&per=50   - one page of taxon name records
      2. GET /otus?taxon_name_id[]=...    - OTUs for that page (batched, max 10/call)
      3. Process each record
      4. Repeat until the page comes back short or empty
    """

    # Reusable sample page (< PER_PAGE records so the loop stops after page 1)
    SAMPLE_PAGE_1 = [
        {
            "id": 909313, "name": "Root", "parent_id": None,
            "cached": "Root", "cached_html": "Root",
            "nomenclatural_code": None,
            "rank": "nomenclatural rank",
            "rank_string": "NomenclaturalRank", "type": "Protonym",
            "project_id": 55, "cached_valid_taxon_name_id": 909313,
            "cached_author": None, "cached_author_year": None,
            "cached_is_valid": True,
            "created_at": "2023-08-26T00:14:29.157Z",
            "updated_at": "2023-08-26T00:14:29.157Z",
            "name_string": "Root", "original_combination": None,
        },
        {
            "id": 909335, "name": "Animalia", "parent_id": 909313,
            "cached": "Animalia", "cached_html": "Animalia",
            "nomenclatural_code": "iczn",
            "rank": "kingdom",
            "rank_string": "NomenclaturalRank::Iczn::HigherClassificationGroup::Kingdom",
            "type": "Protonym", "project_id": 55,
            "cached_valid_taxon_name_id": 909335,
            "cached_author": "", "cached_author_year": None,
            "cached_is_valid": True,
            "created_at": "2023-08-26T05:26:00.656Z",
            "updated_at": "2023-08-26T05:26:00.656Z",
            "name_string": "Animalia", "original_combination": None,
        },
        {
            "id": 998313, "name": "Osmylites", "parent_id": 909335,
            "cached": "Osmylites", "cached_html": "<i>Osmylites</i>",
            "nomenclatural_code": "iczn",
            "rank": "genus",
            "rank_string": "NomenclaturalRank::Iczn::GenusGroup::Genus",
            "type": "Protonym", "project_id": 55,
            "cached_valid_taxon_name_id": 998313,
            "cached_author": "", "cached_author_year": None,
            "cached_is_valid": True,
            "created_at": "2013-03-12T16:32:00.000Z",
            "updated_at": "2014-03-07T10:53:00.000Z",
            "name_string": "Osmylites", "original_combination": None,
        },
        {
            "id": 998314, "name": "Sinoephemera", "parent_id": 909335,
            "cached": "Sinoephemera", "cached_html": "<i>Sinoephemera</i>",
            "nomenclatural_code": "iczn",
            "rank": "genus",
            "rank_string": "NomenclaturalRank::Iczn::GenusGroup::Genus",
            "type": "Protonym", "project_id": 55,
            "cached_valid_taxon_name_id": 998314,
            "cached_author": "", "cached_author_year": None,
            "cached_is_valid": True,
            "created_at": "2013-03-12T16:32:00.000Z",
            "updated_at": "2014-03-07T10:53:00.000Z",
            "name_string": "Sinoephemera", "original_combination": None,
        },
    ]

    def setUp(self):
        self.taxon_group = TaxonGroupF.create()
        self.user = UserF.create()
        self.schema_name = connection.schema_name

    def _make_session(self, additional=None):
        data = additional if additional is not None else {
            'base_url': 'https://test.taxonworks.org',
            'project_token': 'token',
            'exclude_extinct': True,
        }
        session = HarvestSession.objects.create(
            harvester=self.user,
            module_group=self.taxon_group,
            category='taxonworks',
            additional_data=data,
        )
        session.log_file.save(
            f'taxonworks-test-{session.id}.log',
            ContentFile(b''),
        )
        return session

    class _MockResponse:
        def __init__(self, payload, status_code=200, headers=None):
            self._payload = payload
            self.status_code = status_code
            self.headers = headers or {}

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f'HTTP {self.status_code}')

    def _mock_http_get(self, pages, otus=None, extra_records=None):
        """
        Build a side_effect for ``_http.get`` covering the three URL patterns:

        - ``/taxon_names``       - paginated listing; *pages* is a list of
                                   page payloads (one list per page).
        - ``/taxon_names/<id>``  - individual record lookup used by
                                   _fetch_record when resolving parents.
        - ``/otus``              - OTU lookup; returns *otus* (default []).

        *extra_records* is a ``{taxon_id: record}`` dict for parent records
        that do not appear in any page (e.g. grandparent stubs).
        """
        otus = otus or []
        extra_records = extra_records or {}

        # Build a flat id -> record lookup from all pages + extras
        record_lookup: dict[int, dict] = {}
        for page in pages:
            for r in page:
                if r.get("id"):
                    record_lookup[int(r["id"])] = r
        record_lookup.update({int(k): v for k, v in extra_records.items()})

        def _get_page_param(params):
            """Extract 'page' from dict or list-of-tuples params."""
            if isinstance(params, dict):
                return int(params.get('page', 1))
            for k, v in (params or []):
                if k == 'page':
                    return int(v)
            return 1

        def _side_effect(url, params=None, timeout=None):
            # Individual taxon_name lookup: /taxon_names/<int>
            if '/taxon_names/' in url:
                taxon_id = int(url.rstrip('/').split('/')[-1])
                record = record_lookup.get(taxon_id)
                if record is None:
                    return self._MockResponse({}, status_code=404)
                return self._MockResponse(record)

            # OTU lookup: /otus
            if url.endswith('/otus'):
                return self._MockResponse(otus)

            # Paginated taxon_names listing: /taxon_names
            page_num = _get_page_param(params)
            if 1 <= page_num <= len(pages):
                return self._MockResponse(pages[page_num - 1])
            return self._MockResponse([])

        return _side_effect

    # ------------------------------------------------------------------
    # Basic: session completes, taxa are created
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_HTTP_GET)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_session_marked_finished(
        self, mock_dis, mock_con, mock_http_get, mock_sleep, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        mock_http_get.side_effect = self._mock_http_get([self.SAMPLE_PAGE_1])

        session = self._make_session()
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertIn('Finished', session.status)
        taxon = Taxonomy.objects.get(canonical_name='Animalia', rank='KINGDOM')
        self.assertEqual(taxon.additional_data['_taxonworks_taxon_name_id'], 909335)
        self.assertTrue(Taxonomy.objects.filter(canonical_name='Osmylites', rank='GENUS').exists())

    # ------------------------------------------------------------------
    # Not auto-validated: harvested taxa are unvalidated and get a
    # pending TaxonomyUpdateProposal for review, instead of being
    # silently added as validated.
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_HTTP_GET)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_not_auto_validated_creates_proposal(
        self, mock_dis, mock_con, mock_http_get, mock_sleep, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = False
        mock_http_get.side_effect = self._mock_http_get([self.SAMPLE_PAGE_1])

        session = self._make_session()
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        taxon = Taxonomy.objects.get(canonical_name='Osmylites', rank='GENUS')

        tgt = TaxonGroupTaxonomy.objects.get(
            taxonomy=taxon, taxongroup=self.taxon_group
        )
        self.assertFalse(tgt.is_validated)

        proposal = TaxonomyUpdateProposal.objects.get(
            original_taxonomy=taxon,
            taxon_group=self.taxon_group,
            status='pending',
        )
        self.assertEqual(proposal.canonical_name, 'Osmylites')

    # ------------------------------------------------------------------
    # Extinct taxa are skipped; valid taxa are created
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_HTTP_GET)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_extinct_taxa_skipped(
        self, mock_dis, mock_con, mock_http_get, mock_sleep, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        root = _record(100, 'Plecoptera', 'order', parent_id=10)
        family = _record(101, 'Perlidae', 'family', parent_id=100)
        extinct_genus = _record(102, 'Thaumatophora', 'genus', parent_id=100, extinct=True)
        species = _record(103, 'Perla marginata', 'species', parent_id=101)
        mock_http_get.side_effect = self._mock_http_get(
            [[root, family, extinct_genus, species]]
        )

        session = self._make_session()
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        self.assertTrue(Taxonomy.objects.filter(canonical_name='Perlidae', rank='FAMILY').exists())
        self.assertTrue(Taxonomy.objects.filter(canonical_name='Perla marginata', rank='SPECIES').exists())
        self.assertFalse(Taxonomy.objects.filter(canonical_name='Thaumatophora').exists())

    # ------------------------------------------------------------------
    # Synonym links to accepted taxonomy
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_HTTP_GET)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_synonym_links_to_accepted_taxonomy(
        self, mock_dis, mock_con, mock_http_get, mock_sleep, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        root = _record(100, 'Plecoptera', 'order', parent_id=10)
        genus = _record(110, 'Neophron', 'genus', parent_id=100)
        species_parent = _record(112, 'Neophron percnopterus', 'species', parent_id=110)
        accepted = _record(111, 'Neophron percnopterus ginginianus', 'subspecies', parent_id=112)
        synonym = _record(113, 'Vultur ginginianus', None, parent_id=100,
                          valid=False, valid_id=111)
        synonym['type'] = 'Combination'
        mock_http_get.side_effect = self._mock_http_get(
            [[root, genus, species_parent, accepted, synonym]]
        )

        session = self._make_session()
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        synonym_taxon = Taxonomy.objects.get(canonical_name='Vultur ginginianus', rank='SPECIES')
        self.assertEqual(synonym_taxon.taxonomic_status, 'SYNONYM')
        self.assertIsNotNone(synonym_taxon.accepted_taxonomy)
        self.assertEqual(
            synonym_taxon.accepted_taxonomy.canonical_name,
            'Neophron percnopterus ginginianus',
        )
        self.assertEqual(synonym_taxon.accepted_taxonomy.rank, 'SUBSPECIES')

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_HTTP_GET)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_rankless_combination_linked_to_accepted_becomes_synonym(
        self, mock_dis, mock_con, mock_http_get, mock_sleep, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        root = _record(100, 'Plecoptera', 'order', parent_id=10)
        genus = _record(110, 'Neophron', 'genus', parent_id=100)
        accepted = _record(112, 'Neophron percnopterus', 'species', parent_id=110)
        combination = _record(113, 'Vultur percnopterus', None, parent_id=110,
                              valid=False, valid_id=112)
        combination['name'] = None
        combination['type'] = 'Combination'
        orphan = _record(114, 'Gyps unknownus', None, parent_id=110, valid=False)
        orphan['name'] = None
        orphan['type'] = 'Combination'
        mock_http_get.side_effect = self._mock_http_get(
            [[root, genus, accepted, combination, orphan]]
        )

        session = self._make_session()
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        syn = Taxonomy.objects.get(canonical_name='Vultur percnopterus')
        self.assertEqual(syn.rank, 'SPECIES')
        self.assertEqual(syn.taxonomic_status, 'SYNONYM')
        self.assertEqual(syn.accepted_taxonomy.canonical_name, 'Neophron percnopterus')
        # Rankless and not connected to an accepted name: still skipped
        self.assertFalse(Taxonomy.objects.filter(canonical_name='Gyps unknownus').exists())

    # ------------------------------------------------------------------
    # OTU ID is stored on Taxonomy
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_HTTP_GET)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_otu_id_stored_on_taxonomy(
        self, mock_dis, mock_con, mock_http_get, mock_sleep, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        genus = _record(998313, 'Osmylites', 'genus')
        otus = [
            {"id": 890805, "taxon_name_id": 998313, "project_id": 55,
             "name": None, "global_id": "gid://taxon-works/Otu/890805"},
        ]
        mock_http_get.side_effect = self._mock_http_get([[genus]], otus=otus)

        session = self._make_session()
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        taxon = Taxonomy.objects.get(canonical_name='Osmylites', rank='GENUS')
        self.assertEqual(taxon.taxonworks_otu_id, 890805)

    # ------------------------------------------------------------------
    # Multi-page: records on page 2 are also processed
    # ------------------------------------------------------------------

    @mock.patch('bims.utils.gbif.get_species', return_value=None)
    @mock.patch('bims.utils.gbif.search_exact_match', return_value=None)
    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_HTTP_GET)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_multi_page_all_records_processed(
        self, mock_dis, mock_con, mock_http_get, mock_sleep, mock_prefs,
        mock_gbif_search, mock_gbif_get
    ):
        """Records spread across two pages are all imported."""
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True

        # Build 50 stub records for page 1 (fills a full page) + 2 real ones on page 2
        from bims.tasks.harvest_taxonworks_species import PER_PAGE
        page1 = [
            _record(1000 + i, f'Genus{i}', 'genus') for i in range(PER_PAGE)
        ]
        page2 = [
            _record(2001, 'Perlidae', 'family'),
            _record(2002, 'Chloroperlidae', 'family'),
        ]
        mock_http_get.side_effect = self._mock_http_get([page1, page2])

        session = self._make_session()
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertIn('Finished', session.status)
        self.assertTrue(Taxonomy.objects.filter(canonical_name='Perlidae', rank='FAMILY').exists())
        self.assertTrue(Taxonomy.objects.filter(canonical_name='Chloroperlidae', rank='FAMILY').exists())
        # All page-1 genera should be present too
        self.assertEqual(
            Taxonomy.objects.filter(canonical_name__startswith='Genus', rank='GENUS').count(),
            PER_PAGE,
        )

    # ------------------------------------------------------------------
    # Progress is saved after each page (current_page in additional_data)
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_HTTP_GET)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_progress_saved_per_page(
        self, mock_dis, mock_con, mock_http_get, mock_sleep, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        mock_http_get.side_effect = self._mock_http_get([self.SAMPLE_PAGE_1])

        session = self._make_session()
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertIn('current_page', session.additional_data)
        self.assertIn('processed_taxonworks_ids', session.additional_data)

    # ------------------------------------------------------------------
    # Cancellation stops processing
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_HTTP_GET)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_cancel_stops_harvest(
        self, mock_dis, mock_con, mock_http_get, mock_sleep, mock_prefs
    ):
        """
        A session already marked canceled is detected on the first loop
        iteration (before any page is fetched) and the task exits cleanly
        without importing any taxa.
        """
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True

        page = [_record(4001, 'ShouldNotExist', 'family')]
        mock_http_get.side_effect = self._mock_http_get([page])

        session = self._make_session()
        # Mark canceled before the task runs so the first cancel-check fires.
        HarvestSession.objects.filter(id=session.id).update(canceled=True)

        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertIn('Canceled', session.status)
        self.assertFalse(Taxonomy.objects.filter(canonical_name='ShouldNotExist').exists())

    # ------------------------------------------------------------------
    # A failed page fetch must not be treated as end-of-data
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_HTTP_GET)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_failed_page_marks_session_failed_and_resumable(
        self, mock_dis, mock_con, mock_http_get, mock_sleep, mock_prefs
    ):
        from bims.tasks.harvest_taxonworks_species import (
            PER_PAGE, PAGE_FETCH_ATTEMPTS,
        )
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True

        page_1 = [
            _record(5000 + i, f'Fam{i}', 'family') for i in range(PER_PAGE)
        ]
        page_2 = [_record(6001, 'NeverReached', 'family')]
        base_side_effect = self._mock_http_get([page_1, page_2])
        page_2_calls = {'n': 0}

        def _side_effect(url, params=None, timeout=None):
            if url.endswith('/taxon_names') and ('page', 2) in (params or []):
                page_2_calls['n'] += 1
                return self._MockResponse({}, status_code=503)
            return base_side_effect(url, params=params, timeout=timeout)

        mock_http_get.side_effect = _side_effect

        session = self._make_session()
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertEqual(page_2_calls['n'], PAGE_FETCH_ATTEMPTS)
        self.assertTrue(session.status.startswith('Failed at page 2'))
        self.assertNotIn('Finished', session.status)
        self.assertEqual(session.additional_data['current_page'], 2)
        self.assertIn('last_error', session.additional_data)
        self.assertEqual(
            len(session.additional_data['processed_taxonworks_ids']), PER_PAGE
        )
        self.assertFalse(
            Taxonomy.objects.filter(canonical_name='NeverReached').exists()
        )

        # Re-running the task for the same session resumes from page 2.
        mock_http_get.side_effect = base_side_effect
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)
        session.refresh_from_db()
        self.assertIn('Finished', session.status)
        self.assertTrue(
            Taxonomy.objects.filter(canonical_name='NeverReached').exists()
        )

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_HTTP_GET)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_transient_page_failure_is_retried(
        self, mock_dis, mock_con, mock_http_get, mock_sleep, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        base_side_effect = self._mock_http_get([self.SAMPLE_PAGE_1])
        failures = {'left': 2}

        def _side_effect(url, params=None, timeout=None):
            if url.endswith('/taxon_names') and failures['left'] > 0:
                failures['left'] -= 1
                raise ConnectionError('read timed out')
            return base_side_effect(url, params=params, timeout=timeout)

        mock_http_get.side_effect = _side_effect

        session = self._make_session()
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertIn('Finished', session.status)
        self.assertTrue(
            Taxonomy.objects.filter(canonical_name='Osmylites', rank='GENUS').exists()
        )

    # ------------------------------------------------------------------
    # pagination-total-pages header drives the loop
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_HTTP_GET)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_continues_until_total_pages(
        self, mock_dis, mock_con, mock_http_get, mock_sleep, mock_prefs
    ):
        """A short or empty page before the last page doesn't stop the harvest."""
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        pages = [
            [_record(7001, 'FamA', 'family')],
            [],
            [_record(7003, 'FamC', 'family')],
        ]
        base_side_effect = self._mock_http_get(pages)

        def _side_effect(url, params=None, timeout=None):
            resp = base_side_effect(url, params=params, timeout=timeout)
            if url.endswith('/taxon_names'):
                resp.headers = {'pagination-total-pages': str(len(pages))}
            return resp

        mock_http_get.side_effect = _side_effect

        session = self._make_session()
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertIn('Finished', session.status)
        self.assertEqual(session.additional_data['total_pages'], 3)
        self.assertTrue(Taxonomy.objects.filter(canonical_name='FamA').exists())
        self.assertTrue(Taxonomy.objects.filter(canonical_name='FamC').exists())

    # ------------------------------------------------------------------
    # taxon_name_id limits the harvest to that branch
    # ------------------------------------------------------------------

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_HTTP_GET)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_taxon_name_id_harvests_root_and_descendants(
        self, mock_dis, mock_con, mock_http_get, mock_sleep, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        root = _record(8000, 'Plecoptera', 'order')
        descendants = [
            _record(8001, 'Perlidae', 'family', parent_id=8000),
            _record(8002, 'Acroneuria', 'genus', parent_id=8001),
        ]
        mock_http_get.side_effect = self._mock_http_get(
            [descendants], extra_records={8000: root}
        )

        session = self._make_session({
            'base_url': 'https://test.taxonworks.org',
            'project_token': 'token',
            'exclude_extinct': True,
            'taxon_name_id': 8000,
        })
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        listing_params = [
            call.kwargs.get('params')
            for call in mock_http_get.call_args_list
            if call.args[0].endswith('/taxon_names')
        ]
        self.assertTrue(listing_params)
        for params in listing_params:
            self.assertIn(('taxon_name_id[]', 8000), params)
            self.assertIn(('descendants', 'true'), params)

        session.refresh_from_db()
        self.assertIn('Finished', session.status)
        self.assertIn(8000, session.additional_data['processed_taxonworks_ids'])
        for name, rank in (('Plecoptera', 'ORDER'), ('Perlidae', 'FAMILY'),
                           ('Acroneuria', 'GENUS')):
            self.assertTrue(
                Taxonomy.objects.filter(canonical_name=name, rank=rank).exists(),
                name,
            )


# ---------------------------------------------------------------------------
# TaxonWorksTaxaProcessor — GBIF lineage fallback
# ---------------------------------------------------------------------------

_GBIF_SEARCH = 'bims.scripts.taxa_upload_taxonworks.search_exact_match'
_GBIF_GET = 'bims.scripts.taxa_upload_taxonworks.get_species'
_PATCH_PREFS2 = 'bims.scripts.taxa_upload_taxonworks.preferences'

_GBIF_FAMILY_DATA = {
    'key': 9999,
    'rank': 'FAMILY',
    'canonicalName': 'Perlidae',
    'scientificName': 'Perlidae',
    'kingdom': 'Animalia',
    'phylum': 'Arthropoda',
    'class': 'Insecta',
    'order': 'Plecoptera',
    'family': 'Perlidae',
    'kingdomKey': 1,
    'phylumKey': 2,
    'classKey': 3,
    'orderKey': 4,
    'familyKey': 9999,
}


class TestTaxonWorksGbifLineage(FastTenantTestCase):
    """TaxonWorksTaxaProcessor._ensure_gbif_lineage fills missing ancestors."""

    def setUp(self):
        self.taxon_group = TaxonGroupF.create()

    def _make_processor(self):
        return TaxonWorksTaxaProcessor(
            base_url='https://test.taxonworks.org',
            project_token='tok',
        )

    # -- _walk_to_kingdom ---------------------------------------------------

    def test_walk_to_kingdom_returns_true_for_kingdom(self):
        kingdom = Taxonomy.objects.create(
            canonical_name='Animalia', scientific_name='Animalia',
            legacy_canonical_name='Animalia', rank='KINGDOM',
        )
        proc = self._make_processor()
        self.assertTrue(proc._walk_to_kingdom(kingdom))

    def test_walk_to_kingdom_returns_true_via_parent(self):
        kingdom = Taxonomy.objects.create(
            canonical_name='Animalia2', scientific_name='Animalia2',
            legacy_canonical_name='Animalia2', rank='KINGDOM',
        )
        family = Taxonomy.objects.create(
            canonical_name='TestFam', scientific_name='TestFam',
            legacy_canonical_name='TestFam', rank='FAMILY',
            parent=kingdom,
        )
        proc = self._make_processor()
        self.assertTrue(proc._walk_to_kingdom(family))

    def test_walk_to_kingdom_returns_false_when_no_kingdom(self):
        family = Taxonomy.objects.create(
            canonical_name='OrphanFam', scientific_name='OrphanFam',
            legacy_canonical_name='OrphanFam', rank='FAMILY',
        )
        proc = self._make_processor()
        self.assertFalse(proc._walk_to_kingdom(family))

    # -- _ensure_gbif_lineage -----------------------------------------------

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_gbif_lineage_fills_missing_ancestors(
        self, mock_search, mock_get
    ):
        """A FAMILY with no parent gets Kingdom→Phylum→Class→Order chain."""
        mock_search.return_value = 9999
        mock_get.return_value = _GBIF_FAMILY_DATA

        family = Taxonomy.objects.create(
            canonical_name='Perlidae', scientific_name='Perlidae',
            legacy_canonical_name='Perlidae', rank='FAMILY',
        )
        proc = self._make_processor()
        proc._ensure_gbif_lineage(family)
        family.refresh_from_db()

        self.assertIsNotNone(family.parent)
        self.assertEqual(family.parent.rank, 'ORDER')
        self.assertEqual(family.parent.canonical_name, 'Plecoptera')
        self.assertEqual(family.parent.parent.rank, 'CLASS')
        self.assertEqual(family.parent.parent.canonical_name, 'Insecta')
        self.assertEqual(family.parent.parent.parent.rank, 'PHYLUM')
        self.assertEqual(family.parent.parent.parent.parent.rank, 'KINGDOM')
        self.assertEqual(family.parent.parent.parent.parent.canonical_name, 'Animalia')

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_gbif_lineage_skipped_when_kingdom_already_exists(
        self, mock_search, mock_get
    ):
        """No GBIF call when the taxonomy already has a Kingdom ancestor."""
        kingdom = Taxonomy.objects.create(
            canonical_name='ExistingKingdom', scientific_name='ExistingKingdom',
            legacy_canonical_name='ExistingKingdom', rank='KINGDOM',
        )
        family = Taxonomy.objects.create(
            canonical_name='AlreadyLinkedFam', scientific_name='AlreadyLinkedFam',
            legacy_canonical_name='AlreadyLinkedFam', rank='FAMILY',
            parent=kingdom,
        )
        proc = self._make_processor()
        proc._ensure_gbif_lineage(family)

        mock_search.assert_not_called()
        mock_get.assert_not_called()

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_gbif_lineage_graceful_when_no_gbif_match(
        self, mock_search, mock_get
    ):
        """If GBIF returns no match, parent stays None without raising."""
        mock_search.return_value = None

        family = Taxonomy.objects.create(
            canonical_name='UnknownFam', scientific_name='UnknownFam',
            legacy_canonical_name='UnknownFam', rank='FAMILY',
        )
        proc = self._make_processor()
        proc._ensure_gbif_lineage(family)  # should not raise

        family.refresh_from_db()
        self.assertIsNone(family.parent)
        mock_get.assert_not_called()

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_gbif_lineage_reuses_existing_taxonomy(
        self, mock_search, mock_get
    ):
        """If a Kingdom/Phylum node already exists in DB it is reused, not duplicated."""
        mock_search.return_value = 9999
        mock_get.return_value = _GBIF_FAMILY_DATA

        # Pre-create Animalia with the same gbif_key that the mock returns
        existing_kingdom = Taxonomy.objects.create(
            canonical_name='Animalia', scientific_name='Animalia',
            legacy_canonical_name='Animalia', rank='KINGDOM',
            gbif_key=1,
        )

        family = Taxonomy.objects.create(
            canonical_name='Perlidae2', scientific_name='Perlidae2',
            legacy_canonical_name='Perlidae2', rank='FAMILY',
        )
        proc = self._make_processor()
        proc._ensure_gbif_lineage(family)
        family.refresh_from_db()

        # Walk to kingdom
        cursor = family
        while cursor.parent:
            cursor = cursor.parent
        self.assertEqual(cursor.id, existing_kingdom.id)
        # No extra Animalia rows should have been created
        self.assertEqual(
            Taxonomy.objects.filter(canonical_name='Animalia', rank='KINGDOM').count(), 1
        )

    @mock.patch(_PATCH_PREFS2)
    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_process_taxonworks_record_calls_gbif_lineage(
        self, mock_search, mock_get, mock_prefs
    ):
        """
        Full process_taxonworks_record: a family-level record with no
        TaxonWorks parents above it gets ancestors from GBIF.
        """
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        mock_search.return_value = 9999
        mock_get.return_value = _GBIF_FAMILY_DATA

        family_record = {
            'id': 200,
            'name': 'Perlidae',
            'cached': 'Perlidae',
            'parent_id': None,
            'rank': 'family',
            'rank_string': 'family',
            'type': 'Protonym',
            'project_id': 1,
            'cached_valid_taxon_name_id': 200,
            'cached_is_valid': True,
            'cached_author': '',
            'cached_author_year': '',
            'name_string': 'Perlidae',
            'updated_at': '2024-01-01T00:00:00.000Z',
            'created_at': '2023-01-01T00:00:00.000Z',
        }

        proc = self._make_processor()
        taxonomy = proc.process_taxonworks_record(family_record, self.taxon_group)

        self.assertIsNotNone(taxonomy)
        taxonomy.refresh_from_db()
        self.assertIsNotNone(taxonomy.parent)
        self.assertEqual(taxonomy.parent.rank, 'ORDER')


# ---------------------------------------------------------------------------
# TaxonWorksTaxaProcessor — species / subspecies hierarchy validation
# ---------------------------------------------------------------------------

_NO_GBIF_SEARCH = 'bims.scripts.taxa_upload_taxonworks.search_exact_match'
_NO_GBIF_GET = 'bims.scripts.taxa_upload_taxonworks.get_species'


class TestSpeciesHierarchyValidation(FastTenantTestCase):

    def setUp(self):
        self.taxon_group = TaxonGroupF.create()

    def _proc(self):
        return TaxonWorksTaxaProcessor(
            base_url='https://test.taxonworks.org',
            project_token='tok',
        )

    def _species(self, name, parent=None):
        return Taxonomy.objects.create(
            canonical_name=name, scientific_name=name,
            legacy_canonical_name=name, rank='SPECIES', parent=parent,
        )

    def _genus(self, name, parent=None):
        return Taxonomy.objects.create(
            canonical_name=name, scientific_name=name,
            legacy_canonical_name=name, rank='GENUS', parent=parent,
        )

    # -- _find_genus_ancestor -----------------------------------------------

    def test_find_genus_direct_parent(self):
        genus = self._genus('Homo')
        species = self._species('Homo sapiens', parent=genus)
        proc = self._proc()
        self.assertEqual(proc._find_genus_ancestor(species.parent), genus)

    def test_find_genus_through_subgenus(self):
        genus = self._genus('Homo')
        subgenus = Taxonomy.objects.create(
            canonical_name='Homo (Homo)', scientific_name='Homo (Homo)',
            legacy_canonical_name='Homo (Homo)', rank='SUBGENUS', parent=genus,
        )
        species = self._species('Homo sapiens', parent=subgenus)
        proc = self._proc()
        self.assertEqual(proc._find_genus_ancestor(species.parent), genus)

    def test_find_genus_through_superspecies(self):
        genus = self._genus('Canis')
        supersp = Taxonomy.objects.create(
            canonical_name='Canis lupus group', scientific_name='Canis lupus group',
            legacy_canonical_name='Canis lupus group', rank='SUPERSPECIES', parent=genus,
        )
        species = self._species('Canis lupus', parent=supersp)
        proc = self._proc()
        self.assertEqual(proc._find_genus_ancestor(species.parent), genus)

    def test_find_genus_returns_none_for_family_parent(self):
        family = Taxonomy.objects.create(
            canonical_name='Hominidae', scientific_name='Hominidae',
            legacy_canonical_name='Hominidae', rank='FAMILY',
        )
        species = self._species('Homo sapiens', parent=family)
        proc = self._proc()
        self.assertIsNone(proc._find_genus_ancestor(species.parent))

    # -- _get_or_create_genus -----------------------------------------------

    def test_get_or_create_genus_reuses_existing(self):
        existing = self._genus('Felis')
        proc = self._proc()
        result = proc._get_or_create_genus('Felis')
        self.assertEqual(result.id, existing.id)
        self.assertEqual(Taxonomy.objects.filter(canonical_name='Felis', rank='GENUS').count(), 1)

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_get_or_create_genus_uses_gbif(self, mock_search, mock_get):
        mock_search.return_value = 42
        mock_get.return_value = {
            'key': 42, 'rank': 'GENUS',
            'canonicalName': 'Panthera', 'scientificName': 'Panthera Oken, 1816',
        }
        proc = self._proc()
        genus = proc._get_or_create_genus('Panthera')
        self.assertEqual(genus.rank, 'GENUS')
        self.assertEqual(genus.canonical_name, 'Panthera')
        self.assertEqual(genus.gbif_key, 42)

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_get_or_create_genus_stub_when_no_gbif(self, mock_search, mock_get):
        mock_search.return_value = None
        proc = self._proc()
        genus = proc._get_or_create_genus('UnknownGenus')
        self.assertEqual(genus.rank, 'GENUS')
        self.assertEqual(genus.canonical_name, 'UnknownGenus')
        mock_get.assert_not_called()

    # -- _validate_species_hierarchy: SPECIES --------------------------------

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_species_with_genus_parent_unchanged(self, mock_search, mock_get):
        genus = self._genus('Aquila')
        species = self._species('Aquila chrysaetos', parent=genus)
        proc = self._proc()
        proc._validate_species_hierarchy(species)
        species.refresh_from_db()
        self.assertEqual(species.parent_id, genus.id)
        mock_search.assert_not_called()

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_species_under_family_gets_genus_inserted(self, mock_search, mock_get):
        """Species directly under Family → a Genus must be inserted."""
        mock_search.return_value = None  # force stub creation
        family = Taxonomy.objects.create(
            canonical_name='Accipitridae', scientific_name='Accipitridae',
            legacy_canonical_name='Accipitridae', rank='FAMILY',
        )
        species = self._species('Aquila chrysaetos', parent=family)
        proc = self._proc()
        proc._validate_species_hierarchy(species)
        species.refresh_from_db()
        self.assertEqual(species.parent.rank, 'GENUS')
        self.assertEqual(species.parent.canonical_name, 'Aquila')
        # genus should be placed above family
        self.assertEqual(species.parent.parent_id, family.id)

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_species_with_no_parent_creates_genus(self, mock_search, mock_get):
        mock_search.return_value = None
        species = self._species('Canis lupus')
        proc = self._proc()
        proc._validate_species_hierarchy(species)
        species.refresh_from_db()
        self.assertEqual(species.parent.rank, 'GENUS')
        self.assertEqual(species.parent.canonical_name, 'Canis')

    # -- _validate_species_hierarchy: SUBSPECIES -----------------------------

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_subspecies_with_correct_chain_unchanged(self, mock_search, mock_get):
        genus = self._genus('Canis')
        species = self._species('Canis lupus', parent=genus)
        subsp = Taxonomy.objects.create(
            canonical_name='Canis lupus familiaris',
            scientific_name='Canis lupus familiaris',
            legacy_canonical_name='Canis lupus familiaris',
            rank='SUBSPECIES', parent=species,
        )
        proc = self._proc()
        proc._validate_species_hierarchy(subsp)
        subsp.refresh_from_db()
        self.assertEqual(subsp.parent_id, species.id)
        mock_search.assert_not_called()

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_subspecies_under_genus_creates_species(self, mock_search, mock_get):
        """Subspecies directly under Genus → a Species must be inserted."""
        mock_search.return_value = None
        genus = self._genus('Canis')
        subsp = Taxonomy.objects.create(
            canonical_name='Canis lupus familiaris',
            scientific_name='Canis lupus familiaris',
            legacy_canonical_name='Canis lupus familiaris',
            rank='SUBSPECIES', parent=genus,
        )
        proc = self._proc()
        proc._validate_species_hierarchy(subsp)
        subsp.refresh_from_db()
        self.assertEqual(subsp.parent.rank, 'SPECIES')
        self.assertEqual(subsp.parent.canonical_name, 'Canis lupus')
        self.assertEqual(subsp.parent.parent_id, genus.id)

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_subspecies_with_no_parent_creates_species_and_genus(self, mock_search, mock_get):
        mock_search.return_value = None
        subsp = Taxonomy.objects.create(
            canonical_name='Homo sapiens neanderthalensis',
            scientific_name='Homo sapiens neanderthalensis',
            legacy_canonical_name='Homo sapiens neanderthalensis',
            rank='SUBSPECIES',
        )
        proc = self._proc()
        proc._validate_species_hierarchy(subsp)
        subsp.refresh_from_db()
        self.assertEqual(subsp.parent.rank, 'SPECIES')
        self.assertEqual(subsp.parent.canonical_name, 'Homo sapiens')
        self.assertEqual(subsp.parent.parent.rank, 'GENUS')
        self.assertEqual(subsp.parent.parent.canonical_name, 'Homo')


# ---------------------------------------------------------------------------
# Auto-resume of stalled TaxonWorks sessions
# ---------------------------------------------------------------------------

_PATCH_DELAY = 'bims.tasks.harvest_taxonworks_species.harvest_taxonworks_species.delay'


class TestAutoResumeTaxonWorksHarvest(FastTenantTestCase):

    def setUp(self):
        self.taxon_group = TaxonGroupF.create()
        self.user = UserF.create()
        self.schema_name = connection.schema_name

    def _make_session(self, idle_seconds, **kwargs):
        import os
        import time
        session = HarvestSession.objects.create(
            harvester=self.user,
            module_group=self.taxon_group,
            category=kwargs.pop('category', 'taxonworks'),
            additional_data=kwargs.pop('additional_data', {'current_page': 88}),
            **kwargs,
        )
        session.log_file.save(
            f'taxonworks-autoresume-{session.id}.log', ContentFile(b'x\n'),
        )
        mtime = time.time() - idle_seconds
        os.utime(session.log_file.path, (mtime, mtime))
        return session

    @mock.patch(_PATCH_DELAY)
    def test_stalled_session_is_resumed(self, mock_delay):
        from bims.tasks.harvest_taxonworks_species import (
            STALL_SECONDS, auto_resume_taxonworks_harvest,
            taxonworks_session_is_stalled,
        )
        session = self._make_session(STALL_SECONDS + 60)
        auto_resume_taxonworks_harvest(self.schema_name)

        mock_delay.assert_called_once_with(session.id, schema_name=self.schema_name)
        session.refresh_from_db()
        self.assertEqual(session.additional_data['auto_resume_count'], 1)
        self.assertEqual(session.additional_data['auto_resume_page'], 88)
        # The log line written on resume refreshes the heartbeat, so the
        # next check doesn't dispatch it again.
        self.assertFalse(taxonworks_session_is_stalled(session))
        auto_resume_taxonworks_harvest(self.schema_name)
        mock_delay.assert_called_once()

    @mock.patch(_PATCH_DELAY)
    def test_active_finished_and_other_sessions_ignored(self, mock_delay):
        from bims.tasks.harvest_taxonworks_species import (
            STALL_SECONDS, auto_resume_taxonworks_harvest,
        )
        self._make_session(60)  # still running
        self._make_session(STALL_SECONDS + 60, finished=True)
        self._make_session(STALL_SECONDS + 60, canceled=True)
        self._make_session(STALL_SECONDS + 60, category='gbif')
        auto_resume_taxonworks_harvest(self.schema_name)
        mock_delay.assert_not_called()

    @mock.patch(_PATCH_DELAY)
    def test_gives_up_after_repeated_stalls_on_same_page(self, mock_delay):
        from bims.tasks.harvest_taxonworks_species import (
            MAX_AUTO_RESUMES_PER_PAGE, STALL_SECONDS,
            auto_resume_taxonworks_harvest,
        )
        session = self._make_session(
            STALL_SECONDS + 60,
            additional_data={
                'current_page': 88,
                'auto_resume_page': 88,
                'auto_resume_count': MAX_AUTO_RESUMES_PER_PAGE,
            },
        )
        auto_resume_taxonworks_harvest(self.schema_name)

        mock_delay.assert_not_called()
        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertTrue(session.status.startswith('Failed at page 88'))

    @mock.patch(_PATCH_DELAY)
    def test_counter_resets_when_page_advanced(self, mock_delay):
        from bims.tasks.harvest_taxonworks_species import (
            MAX_AUTO_RESUMES_PER_PAGE, STALL_SECONDS,
            auto_resume_taxonworks_harvest,
        )
        session = self._make_session(
            STALL_SECONDS + 60,
            additional_data={
                'current_page': 120,
                'auto_resume_page': 88,
                'auto_resume_count': MAX_AUTO_RESUMES_PER_PAGE,
            },
        )
        auto_resume_taxonworks_harvest(self.schema_name)

        mock_delay.assert_called_once()
        session.refresh_from_db()
        self.assertEqual(session.additional_data['auto_resume_count'], 1)
        self.assertEqual(session.additional_data['auto_resume_page'], 120)
