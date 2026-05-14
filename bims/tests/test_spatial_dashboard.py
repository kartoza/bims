from unittest.mock import patch

from django.test import TestCase
from django.urls import reverse
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient

from bims.models.search_process import (
    SearchProcess,
    SPATIAL_DASHBOARD_CONS_STATUS,
    SPATIAL_DASHBOARD_RLI,
    SPATIAL_DASHBOARD_MAP,
    SPATIAL_DASHBOARD_SUMMARY,
)
from bims.models.iucn_assessment import IUCNAssessment
from bims.models.taxon_origin import TaxonOrigin
from bims.tests.model_factories import (
    BiologicalCollectionRecordF,
    LocationSiteF,
    TaxonomyF,
    TaxonGroupF,
    IUCNStatusF,
    EndemismF,
    UserF,
)


def _native_origin():
    """Return (or create) the TaxonOrigin record for native species."""
    obj, _ = TaxonOrigin.objects.get_or_create(
        origin_key='indigenous',
        defaults={'category': 'Native', 'order': 1},
    )
    return obj


def _alien_origin():
    """Return (or create) the TaxonOrigin record for non-native species."""
    obj, _ = TaxonOrigin.objects.get_or_create(
        origin_key='alien',
        defaults={'category': 'alien', 'order': 2},
    )
    return obj


class TestSpatialDashboardView(FastTenantTestCase):
    """Tests for the spatial dashboard template view."""

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.user = UserF.create()
        self.url = reverse('spatial-dashboard')

    def test_requires_login(self):
        response = self.client.get(self.url)
        self.assertNotEqual(response.status_code, 200)

    def test_accessible_when_logged_in(self):
        self.client.login(
            username=self.user.username,
            password='password',
        )
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)

    def test_template_used(self):
        self.client.login(
            username=self.user.username,
            password='password',
        )
        response = self.client.get(self.url)
        self.assertTemplateUsed(response, 'spatial_dashboard.html')

    def test_context_has_basemap_layers(self):
        self.client.login(
            username=self.user.username,
            password='password',
        )
        response = self.client.get(self.url)
        self.assertIn('basemap_layers', response.context)


class TestSpatialDashboardApis(FastTenantTestCase):
    """Tests for the spatial dashboard API endpoints."""

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.user = UserF.create()

        self.module = TaxonGroupF.create(
            name='Fish',
            category='SPECIES_MODULE',
            display_order=0,
        )

        endemism = EndemismF.create(name='Endemic')
        endemism_2 = EndemismF.create(name='Non-endemic')

        iucn_lc = IUCNStatusF.create(category='LC', national=False)
        iucn_vu = IUCNStatusF.create(category='VU', national=False)

        self.taxa_1 = TaxonomyF.create(
            scientific_name='Species A',
            rank='SPECIES',
            taxonomic_status='ACCEPTED',
            origin=_native_origin(),
            iucn_status=iucn_lc,
            endemism=endemism,
        )
        self.taxa_2 = TaxonomyF.create(
            scientific_name='Species B',
            rank='SPECIES',
            taxonomic_status='ACCEPTED',
            origin=_alien_origin(),
            iucn_status=iucn_vu,
            endemism=endemism_2,
        )

        site = LocationSiteF.create()
        BiologicalCollectionRecordF.create(
            taxonomy=self.taxa_1,
            collection_date='2020-06-15',
            site=site,
            module_group=self.module,
            validated=True,
        )
        BiologicalCollectionRecordF.create(
            taxonomy=self.taxa_2,
            collection_date='2021-03-10',
            site=site,
            module_group=self.module,
            validated=True,
        )

    def test_apis_require_login(self):
        urls = [
            reverse('spatial-dashboard-cons-status'),
            reverse('spatial-dashboard-rli'),
            reverse('spatial-dashboard-map'),
            reverse('spatial-dashboard-summary'),
        ]
        for url in urls:
            response = self.client.get(url)
            self.assertNotEqual(
                response.status_code, 200,
                f'{url} should require login',
            )

    @patch('bims.api_views.spatial_dashboard.spatial_dashboard_cons_status.delay')
    def test_cons_status_api_triggers_task(self, mock_delay):
        mock_delay.return_value.id = 'fake-task-id'
        self.client.login(
            username=self.user.username,
            password='password',
        )
        url = reverse('spatial-dashboard-cons-status')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        mock_delay.assert_called_once()

    @patch('bims.api_views.spatial_dashboard.spatial_dashboard_rli.delay')
    def test_rli_api_triggers_task(self, mock_delay):
        mock_delay.return_value.id = 'fake-task-id'
        self.client.login(
            username=self.user.username,
            password='password',
        )
        url = reverse('spatial-dashboard-rli')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        mock_delay.assert_called_once()

    @patch('bims.api_views.spatial_dashboard.spatial_dashboard_map.delay')
    def test_map_api_triggers_task(self, mock_delay):
        mock_delay.return_value.id = 'fake-task-id'
        self.client.login(
            username=self.user.username,
            password='password',
        )
        url = reverse('spatial-dashboard-map')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        mock_delay.assert_called_once()

    @patch('bims.api_views.spatial_dashboard.spatial_dashboard_summary.delay')
    def test_summary_api_triggers_task(self, mock_delay):
        mock_delay.return_value.id = 'fake-task-id'
        self.client.login(
            username=self.user.username,
            password='password',
        )
        url = reverse('spatial-dashboard-summary')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        mock_delay.assert_called_once()

    @patch('bims.api_views.spatial_dashboard.spatial_dashboard_cons_status.delay')
    def test_cons_status_returns_processing(self, mock_delay):
        mock_delay.return_value.id = 'fake-task-id'
        self.client.login(
            username=self.user.username,
            password='password',
        )
        url = reverse('spatial-dashboard-cons-status')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertIn('status', response.data)
        self.assertEqual(response.data['status'], 'processing')


class TestSpatialDashboardTasks(FastTenantTestCase):
    """Tests for the spatial dashboard Celery tasks."""

    def setUp(self):
        self.user = UserF.create()
        self.module = TaxonGroupF.create(
            name='Fish',
            category='SPECIES_MODULE',
            display_order=0,
        )
        endemism = EndemismF.create(name='Endemic')
        iucn_lc = IUCNStatusF.create(category='LC', national=False)
        iucn_vu = IUCNStatusF.create(category='VU', national=False)

        # Both taxa: accepted, species rank, native — included in all filters.
        self.taxa_1 = TaxonomyF.create(
            scientific_name='Species A',
            canonical_name='Species A',
            rank='SPECIES',
            taxonomic_status='ACCEPTED',
            origin=_native_origin(),
            iucn_status=iucn_lc,
            endemism=endemism,
        )
        self.taxa_2 = TaxonomyF.create(
            scientific_name='Species B',
            canonical_name='Species B',
            rank='SPECIES',
            taxonomic_status='ACCEPTED',
            origin=_native_origin(),
            iucn_status=iucn_vu,
            endemism=endemism,
        )

        site = LocationSiteF.create()
        BiologicalCollectionRecordF.create(
            taxonomy=self.taxa_1,
            original_species_name='test_species_1',
            collection_date='2020-06-15',
            site=site,
            module_group=self.module,
            validated=True,
        )
        BiologicalCollectionRecordF.create(
            taxonomy=self.taxa_2,
            original_species_name='test_species_2',
            collection_date='2021-03-10',
            site=site,
            module_group=self.module,
            validated=True,
        )

        self.search_params = {
            'search': 'Species',
            'requester': str(self.user.id),
        }

    def _create_search_process(self, category):
        sp = SearchProcess.objects.create(
            category=category,
            requester=self.user,
        )
        sp.set_process_id('test-process-id')
        return sp

    @patch('bims.utils.celery.memcache_lock')
    def test_cons_status_task(self, mock_lock):
        from bims.tasks.spatial_dashboard import spatial_dashboard_cons_status

        mock_lock.return_value.__enter__ = lambda s: True
        mock_lock.return_value.__exit__ = lambda s, *a: None

        search_process = self._create_search_process(
            SPATIAL_DASHBOARD_CONS_STATUS
        )
        spatial_dashboard_cons_status(
            search_parameters=self.search_params,
            search_process_id=search_process.id,
        )
        search_process.refresh_from_db()
        self.assertTrue(search_process.finished)
        results = search_process.get_file_if_exits()
        self.assertIsNotNone(results)
        self.assertIn('modules', results)

    # ------------------------------------------------------------------
    # cons_status graph filters
    # ------------------------------------------------------------------

    @patch('bims.utils.celery.memcache_lock')
    def test_cons_status_only_native_accepted_species(self, mock_lock):
        """Conservation-status graph excludes non-native, non-accepted, and
        non-species-rank taxa."""
        from bims.tasks.spatial_dashboard import spatial_dashboard_cons_status

        mock_lock.return_value.__enter__ = lambda s: True
        mock_lock.return_value.__exit__ = lambda s, *a: None

        iucn_en = IUCNStatusF.create(category='EN', national=False)
        site = LocationSiteF.create()

        # Non-native taxon (should be excluded)
        alien_taxon = TaxonomyF.create(
            scientific_name='Alien Species',
            rank='SPECIES',
            taxonomic_status='ACCEPTED',
            origin=_alien_origin(),
            iucn_status=iucn_en,
        )
        BiologicalCollectionRecordF.create(
            taxonomy=alien_taxon,
            collection_date='2020-01-01',
            site=site,
            module_group=self.module,
            validated=True,
        )

        # Synonym taxon (should be excluded)
        synonym_taxon = TaxonomyF.create(
            scientific_name='Synonym Species',
            rank='SPECIES',
            taxonomic_status='SYNONYM',
            origin=_native_origin(),
            iucn_status=iucn_en,
        )
        BiologicalCollectionRecordF.create(
            taxonomy=synonym_taxon,
            collection_date='2020-01-01',
            site=site,
            module_group=self.module,
            validated=True,
        )

        # Genus-rank taxon (should be excluded)
        genus_taxon = TaxonomyF.create(
            scientific_name='Some Genus',
            rank='GENUS',
            taxonomic_status='ACCEPTED',
            origin=_native_origin(),
            iucn_status=iucn_en,
        )
        BiologicalCollectionRecordF.create(
            taxonomy=genus_taxon,
            collection_date='2020-01-01',
            site=site,
            module_group=self.module,
            validated=True,
        )

        search_process = self._create_search_process(SPATIAL_DASHBOARD_CONS_STATUS)
        spatial_dashboard_cons_status(
            search_parameters=self.search_params,
            search_process_id=search_process.id,
        )
        search_process.refresh_from_db()
        results = search_process.get_file_if_exits()

        # Only taxa_1 (LC) and taxa_2 (VU) pass all filters — no EN entries.
        all_categories = []
        for module in results.get('modules', []):
            for entry in module.get('cons_status', []):
                all_categories.append(entry['category'])

        self.assertNotIn('EN', all_categories,
                         'EN from alien/synonym/genus taxa must be excluded')
        self.assertIn('LC', all_categories)
        self.assertIn('VU', all_categories)

    # ------------------------------------------------------------------
    # RLI — smoke test
    # ------------------------------------------------------------------

    @patch('bims.utils.celery.memcache_lock')
    def test_rli_task(self, mock_lock):
        from bims.tasks.spatial_dashboard import spatial_dashboard_rli

        mock_lock.return_value.__enter__ = lambda s: True
        mock_lock.return_value.__exit__ = lambda s, *a: None

        search_process = self._create_search_process(SPATIAL_DASHBOARD_RLI)
        spatial_dashboard_rli(
            search_parameters=self.search_params,
            search_process_id=search_process.id,
        )
        search_process.refresh_from_db()
        self.assertTrue(search_process.finished)
        results = search_process.get_file_if_exits()
        self.assertIsNotNone(results)
        self.assertIn('series', results)
        self.assertIn('aggregate', results)
        self.assertIn('metadata', results)

    # ------------------------------------------------------------------
    # RLI — assessment history
    # ------------------------------------------------------------------

    @patch('bims.utils.celery.memcache_lock')
    def test_rli_task_with_assessments(self, mock_lock):
        """RLI is computed from IUCNAssessment history; species pool is fixed
        at the first assessment year's count."""
        from bims.tasks.spatial_dashboard import spatial_dashboard_rli

        mock_lock.return_value.__enter__ = lambda s: True
        mock_lock.return_value.__exit__ = lambda s, *a: None

        # Both native species assessed in 2018 and 2022:
        #   Species A: LC(2018) -> VU(2022)
        #   Species B: VU(2018) -> EN(2022)
        IUCNAssessment.objects.create(
            taxonomy=self.taxa_1, assessment_id=1001,
            year_published=2018, red_list_category_code='LC',
        )
        IUCNAssessment.objects.create(
            taxonomy=self.taxa_1, assessment_id=1002,
            year_published=2022, red_list_category_code='VU',
        )
        IUCNAssessment.objects.create(
            taxonomy=self.taxa_2, assessment_id=2001,
            year_published=2018, red_list_category_code='VU',
        )
        IUCNAssessment.objects.create(
            taxonomy=self.taxa_2, assessment_id=2002,
            year_published=2022, red_list_category_code='EN',
        )

        search_process = self._create_search_process(SPATIAL_DASHBOARD_RLI)
        spatial_dashboard_rli(
            search_parameters=self.search_params,
            search_process_id=search_process.id,
        )
        search_process.refresh_from_db()
        results = search_process.get_file_if_exits()
        aggregate = results['aggregate']
        years = [p['year'] for p in aggregate]
        self.assertIn(2018, years)
        self.assertIn(2022, years)

        # 2018: A=LC(0), B=VU(2), N_fixed=2
        # RLI = 1 - (0+2)/(5×2) = 0.8
        year_2018 = next(p for p in aggregate if p['year'] == 2018)
        self.assertEqual(year_2018['value'], 0.8)
        self.assertEqual(year_2018['num_assessed'], 2)

        # 2022: A=VU(2), B=EN(3), N_fixed=2
        # RLI = 1 - (2+3)/(5×2) = 0.5
        year_2022 = next(p for p in aggregate if p['year'] == 2022)
        self.assertEqual(year_2022['value'], 0.5)
        self.assertEqual(year_2022['num_assessed'], 2)

    # ------------------------------------------------------------------
    # RLI — fixed species pool (worked example from requirements)
    # ------------------------------------------------------------------

    @patch('bims.utils.celery.memcache_lock')
    def test_rli_fixed_pool(self, mock_lock):
        """The denominator N is fixed to the species count from the first
        assessment year and does not grow when additional species are assessed
        in later years.

        Example from requirements:
            Species  1994  2000  2010
            A        EN    EN    CR
            B        VU    EN    EN
            C        CR    CR    CR
            D        -     -     LC
            E        -     -     NT

        1994: N=3, RLI = 1 - (3+2+4)/(5×3) = 0.40
        2000: N=3, RLI = 1 - (3+3+4)/(5×3) = 0.33
        2010: N=3, RLI = 1 - (4+3+4+0+1)/(5×3) = 0.20
        """
        from bims.tasks.spatial_dashboard import spatial_dashboard_rli

        mock_lock.return_value.__enter__ = lambda s: True
        mock_lock.return_value.__exit__ = lambda s, *a: None

        iucn_lc = IUCNStatusF.create(category='LC', national=False)
        site = LocationSiteF.create()

        # Create species C, D, E in addition to setUp's taxa_1 (A) and taxa_2 (B)
        taxa_c = TaxonomyF.create(
            scientific_name='Species C',
            canonical_name='Species C',
            rank='SPECIES',
            taxonomic_status='ACCEPTED',
            origin=_native_origin(),
            iucn_status=iucn_lc,
        )
        taxa_d = TaxonomyF.create(
            scientific_name='Species D',
            canonical_name='Species D',
            rank='SPECIES',
            taxonomic_status='ACCEPTED',
            origin=_native_origin(),
            iucn_status=iucn_lc,
        )
        taxa_e = TaxonomyF.create(
            scientific_name='Species E',
            canonical_name='Species E',
            rank='SPECIES',
            taxonomic_status='ACCEPTED',
            origin=_native_origin(),
            iucn_status=iucn_lc,
        )
        for taxon in (taxa_c, taxa_d, taxa_e):
            BiologicalCollectionRecordF.create(
                taxonomy=taxon,
                collection_date='2010-01-01',
                site=site,
                module_group=self.module,
                validated=True,
            )

        # Assessment history (taxa_1=A, taxa_2=B)
        # A: EN(1994), EN(2000), CR(2010)
        IUCNAssessment.objects.create(
            taxonomy=self.taxa_1, assessment_id=101,
            year_published=1994, red_list_category_code='EN',
        )
        IUCNAssessment.objects.create(
            taxonomy=self.taxa_1, assessment_id=102,
            year_published=2000, red_list_category_code='EN',
        )
        IUCNAssessment.objects.create(
            taxonomy=self.taxa_1, assessment_id=103,
            year_published=2010, red_list_category_code='CR',
        )
        # B: VU(1994), EN(2000), EN(2010)
        IUCNAssessment.objects.create(
            taxonomy=self.taxa_2, assessment_id=201,
            year_published=1994, red_list_category_code='VU',
        )
        IUCNAssessment.objects.create(
            taxonomy=self.taxa_2, assessment_id=202,
            year_published=2000, red_list_category_code='EN',
        )
        IUCNAssessment.objects.create(
            taxonomy=self.taxa_2, assessment_id=203,
            year_published=2010, red_list_category_code='EN',
        )
        # C: CR(1994), CR(2000), CR(2010)
        IUCNAssessment.objects.create(
            taxonomy=taxa_c, assessment_id=301,
            year_published=1994, red_list_category_code='CR',
        )
        IUCNAssessment.objects.create(
            taxonomy=taxa_c, assessment_id=302,
            year_published=2000, red_list_category_code='CR',
        )
        IUCNAssessment.objects.create(
            taxonomy=taxa_c, assessment_id=303,
            year_published=2010, red_list_category_code='CR',
        )
        # D and E: first assessed in 2010 only
        IUCNAssessment.objects.create(
            taxonomy=taxa_d, assessment_id=401,
            year_published=2010, red_list_category_code='LC',
        )
        IUCNAssessment.objects.create(
            taxonomy=taxa_e, assessment_id=501,
            year_published=2010, red_list_category_code='NT',
        )

        search_process = self._create_search_process(SPATIAL_DASHBOARD_RLI)
        spatial_dashboard_rli(
            search_parameters=self.search_params,
            search_process_id=search_process.id,
        )
        search_process.refresh_from_db()
        results = search_process.get_file_if_exits()
        aggregate = results['aggregate']

        year_map = {p['year']: p for p in aggregate}
        self.assertIn(1994, year_map)
        self.assertIn(2000, year_map)
        self.assertIn(2010, year_map)

        # 1994: A=EN(3), B=VU(2), C=CR(4). N_fixed=3.
        # RLI = 1 - (3+2+4)/(5×3) = 1 - 9/15 = 0.4
        self.assertAlmostEqual(year_map[1994]['value'], 0.4, places=4)
        self.assertEqual(year_map[1994]['num_assessed'], 3)

        # 2000: A=EN(3), B=EN(3), C=CR(4). N_fixed=3.
        # RLI = 1 - (3+3+4)/(5×3) = 1 - 10/15 ≈ 0.3333
        self.assertAlmostEqual(year_map[2000]['value'], round(1 - 10 / 15, 4), places=4)
        self.assertEqual(year_map[2000]['num_assessed'], 3)

        # 2010: A=CR(4), B=EN(3), C=CR(4), D=LC(0), E=NT(1). N_fixed=3.
        # RLI = 1 - (4+3+4+0+1)/(5×3) = 1 - 12/15 = 0.2
        self.assertAlmostEqual(year_map[2010]['value'], 0.2, places=4)
        self.assertEqual(year_map[2010]['num_assessed'], 5)

    # ------------------------------------------------------------------
    # RLI — no back-casting
    # ------------------------------------------------------------------

    @patch('bims.utils.celery.memcache_lock')
    def test_rli_no_backcast(self, mock_lock):
        """Species not assessed in year T must not contribute to that year's
        RLI, even if they were assessed in an earlier year.

        Setup:
            A: EN(2000), not reassessed in 2010
            B: VU(2000), EN(2010)

        With back-casting A would appear in 2010 (using 2000 status EN).
        Without back-casting, only B appears in 2010.

        2000: A=EN(3), B=VU(2). N_fixed=2. RLI=1-(3+2)/(5×2)=0.5
        2010: B=EN(3) only. N_fixed=2. RLI=1-3/(5×2)=0.7
        """
        from bims.tasks.spatial_dashboard import spatial_dashboard_rli

        mock_lock.return_value.__enter__ = lambda s: True
        mock_lock.return_value.__exit__ = lambda s, *a: None

        IUCNAssessment.objects.create(
            taxonomy=self.taxa_1, assessment_id=1001,
            year_published=2000, red_list_category_code='EN',
        )
        # taxa_1 (A) has NO 2010 assessment — no back-casting must apply
        IUCNAssessment.objects.create(
            taxonomy=self.taxa_2, assessment_id=2001,
            year_published=2000, red_list_category_code='VU',
        )
        IUCNAssessment.objects.create(
            taxonomy=self.taxa_2, assessment_id=2002,
            year_published=2010, red_list_category_code='EN',
        )

        search_process = self._create_search_process(SPATIAL_DASHBOARD_RLI)
        spatial_dashboard_rli(
            search_parameters=self.search_params,
            search_process_id=search_process.id,
        )
        search_process.refresh_from_db()
        results = search_process.get_file_if_exits()
        aggregate = results['aggregate']

        year_map = {p['year']: p for p in aggregate}
        self.assertIn(2000, year_map)
        self.assertIn(2010, year_map)

        # 2000: A=EN(3), B=VU(2). N_fixed=2. RLI=1-5/10=0.5
        self.assertAlmostEqual(year_map[2000]['value'], 0.5, places=4)
        self.assertEqual(year_map[2000]['num_assessed'], 2)

        # 2010: only B=EN(3). N_fixed=2. RLI=1-3/10=0.7
        # (A must NOT be back-cast from 2000)
        self.assertAlmostEqual(year_map[2010]['value'], 0.7, places=4)
        self.assertEqual(year_map[2010]['num_assessed'], 1)

    # ------------------------------------------------------------------
    # RLI — DD / NE exclusion
    # ------------------------------------------------------------------

    @patch('bims.utils.celery.memcache_lock')
    def test_rli_excludes_dd_and_ne(self, mock_lock):
        """DD species are excluded from the RLI value but counted for
        confidence-interval metadata."""
        from bims.tasks.spatial_dashboard import spatial_dashboard_rli

        mock_lock.return_value.__enter__ = lambda s: True
        mock_lock.return_value.__exit__ = lambda s, *a: None

        iucn_dd = IUCNStatusF.create(category='DD', national=False)
        # DD taxon is native/accepted/species so it enters the pool
        taxa_dd = TaxonomyF.create(
            scientific_name='Species DD',
            canonical_name='Species DD',
            rank='SPECIES',
            taxonomic_status='ACCEPTED',
            origin=_native_origin(),
            iucn_status=iucn_dd,
        )
        site = LocationSiteF.create()
        BiologicalCollectionRecordF.create(
            taxonomy=taxa_dd,
            original_species_name='test_species_dd',
            collection_date='2020-01-01',
            site=site,
            module_group=self.module,
            validated=True,
        )

        # In 2020: taxa_1=LC (valid), taxa_dd=DD (excluded from RLI, counted as DD)
        IUCNAssessment.objects.create(
            taxonomy=self.taxa_1, assessment_id=3001,
            year_published=2020, red_list_category_code='LC',
        )
        IUCNAssessment.objects.create(
            taxonomy=taxa_dd, assessment_id=3002,
            year_published=2020, red_list_category_code='DD',
        )

        search_process = self._create_search_process(SPATIAL_DASHBOARD_RLI)
        spatial_dashboard_rli(
            search_parameters=self.search_params,
            search_process_id=search_process.id,
        )
        search_process.refresh_from_db()
        results = search_process.get_file_if_exits()

        aggregate = results['aggregate']
        self.assertTrue(len(aggregate) > 0)
        year_2020 = next(p for p in aggregate if p['year'] == 2020)
        # Only taxa_1 (LC) is assessed; DD excluded from count
        self.assertEqual(year_2020['num_assessed'], 1)
        self.assertEqual(year_2020['num_dd'], 1)
        # LC only → N_fixed=1, RLI = 1 - 0/(5×1) = 1.0
        self.assertEqual(year_2020['value'], 1.0)

        # Metadata should report DD count
        self.assertGreaterEqual(results['metadata']['total_dd'], 1)

    # ------------------------------------------------------------------
    # RLI — filter: non-native taxa excluded
    # ------------------------------------------------------------------

    @patch('bims.utils.celery.memcache_lock')
    def test_rli_excludes_non_native(self, mock_lock):
        """Non-native (alien) taxa must not contribute to the RLI."""
        from bims.tasks.spatial_dashboard import spatial_dashboard_rli

        mock_lock.return_value.__enter__ = lambda s: True
        mock_lock.return_value.__exit__ = lambda s, *a: None

        iucn_cr = IUCNStatusF.create(category='CR', national=False)
        site = LocationSiteF.create()
        alien_taxon = TaxonomyF.create(
            scientific_name='Alien CR Species',
            canonical_name='Alien CR Species',
            rank='SPECIES',
            taxonomic_status='ACCEPTED',
            origin=_alien_origin(),
            iucn_status=iucn_cr,
        )
        BiologicalCollectionRecordF.create(
            taxonomy=alien_taxon,
            collection_date='2020-01-01',
            site=site,
            module_group=self.module,
            validated=True,
        )

        # Native taxa_1=LC assessed in 2020, alien_taxon=CR also in 2020
        IUCNAssessment.objects.create(
            taxonomy=self.taxa_1, assessment_id=4001,
            year_published=2020, red_list_category_code='LC',
        )
        IUCNAssessment.objects.create(
            taxonomy=alien_taxon, assessment_id=4002,
            year_published=2020, red_list_category_code='CR',
        )

        search_process = self._create_search_process(SPATIAL_DASHBOARD_RLI)
        spatial_dashboard_rli(
            search_parameters=self.search_params,
            search_process_id=search_process.id,
        )
        search_process.refresh_from_db()
        results = search_process.get_file_if_exits()
        aggregate = results['aggregate']

        year_2020 = next(p for p in aggregate if p['year'] == 2020)
        # alien_taxon must be excluded; only taxa_1(LC) present
        self.assertEqual(year_2020['num_assessed'], 1)
        # RLI = 1 - 0/(5×1) = 1.0 (alien CR taxon excluded)
        self.assertEqual(year_2020['value'], 1.0)
        self.assertNotIn('CR', year_2020['categories'])

    # ------------------------------------------------------------------
    # RLI — filter: non-accepted and non-species-rank taxa excluded
    # ------------------------------------------------------------------

    @patch('bims.utils.celery.memcache_lock')
    def test_rli_excludes_non_accepted_and_non_species_rank(self, mock_lock):
        """Synonyms and genus-rank taxa must not contribute to the RLI."""
        from bims.tasks.spatial_dashboard import spatial_dashboard_rli

        mock_lock.return_value.__enter__ = lambda s: True
        mock_lock.return_value.__exit__ = lambda s, *a: None

        iucn_en = IUCNStatusF.create(category='EN', national=False)
        site = LocationSiteF.create()

        synonym_taxon = TaxonomyF.create(
            scientific_name='Synonym EN',
            canonical_name='Synonym EN',
            rank='SPECIES',
            taxonomic_status='SYNONYM',
            origin=_native_origin(),
            iucn_status=iucn_en,
        )
        genus_taxon = TaxonomyF.create(
            scientific_name='Native Genus',
            canonical_name='Native Genus',
            rank='GENUS',
            taxonomic_status='ACCEPTED',
            origin=_native_origin(),
            iucn_status=iucn_en,
        )
        for taxon in (synonym_taxon, genus_taxon):
            BiologicalCollectionRecordF.create(
                taxonomy=taxon,
                collection_date='2020-01-01',
                site=site,
                module_group=self.module,
                validated=True,
            )

        IUCNAssessment.objects.create(
            taxonomy=self.taxa_1, assessment_id=5001,
            year_published=2020, red_list_category_code='LC',
        )
        for aid, taxon in enumerate([synonym_taxon, genus_taxon], start=5002):
            IUCNAssessment.objects.create(
                taxonomy=taxon, assessment_id=aid,
                year_published=2020, red_list_category_code='EN',
            )

        search_process = self._create_search_process(SPATIAL_DASHBOARD_RLI)
        spatial_dashboard_rli(
            search_parameters=self.search_params,
            search_process_id=search_process.id,
        )
        search_process.refresh_from_db()
        results = search_process.get_file_if_exits()
        aggregate = results['aggregate']

        year_2020 = next(p for p in aggregate if p['year'] == 2020)
        # Only taxa_1 (LC, accepted, species) contributes
        self.assertEqual(year_2020['num_assessed'], 1)
        self.assertNotIn('EN', year_2020['categories'])

    # ------------------------------------------------------------------
    # RLI — fallback to current iucn_status
    # ------------------------------------------------------------------

    @patch('bims.utils.celery.memcache_lock')
    def test_rli_fallback_current_status(self, mock_lock):
        """Without IUCNAssessment records, falls back to current iucn_status."""
        from datetime import date
        from bims.tasks.spatial_dashboard import spatial_dashboard_rli

        mock_lock.return_value.__enter__ = lambda s: True
        mock_lock.return_value.__exit__ = lambda s, *a: None

        search_process = self._create_search_process(SPATIAL_DASHBOARD_RLI)
        spatial_dashboard_rli(
            search_parameters=self.search_params,
            search_process_id=search_process.id,
        )
        search_process.refresh_from_db()
        results = search_process.get_file_if_exits()
        aggregate = results['aggregate']
        # Should produce a single point for the current year
        self.assertTrue(len(aggregate) > 0)
        current_year = date.today().year
        years = [p['year'] for p in aggregate]
        self.assertIn(current_year, years)
        # taxa_1=LC(0), taxa_2=VU(2), N=2
        # RLI = 1 - (0+2)/(5×2) = 0.8
        point = next(p for p in aggregate if p['year'] == current_year)
        self.assertEqual(point['value'], 0.8)

    # ------------------------------------------------------------------
    # Other tasks
    # ------------------------------------------------------------------

    @patch('bims.utils.celery.memcache_lock')
    def test_summary_task(self, mock_lock):
        from bims.tasks.spatial_dashboard import spatial_dashboard_summary

        mock_lock.return_value.__enter__ = lambda s: True
        mock_lock.return_value.__exit__ = lambda s, *a: None

        search_process = self._create_search_process(SPATIAL_DASHBOARD_SUMMARY)
        spatial_dashboard_summary(
            search_parameters=self.search_params,
            search_process_id=search_process.id,
        )
        search_process.refresh_from_db()
        self.assertTrue(search_process.finished)
        results = search_process.get_file_if_exits()
        self.assertIsNotNone(results)
        self.assertIn('modules', results)
        self.assertIn('overview', results)
        self.assertIn('origin', results)
        self.assertIn('endemism', results)
        self.assertIn('cons_status_global', results)
        self.assertIn('cons_status_national', results)

    @patch('bims.utils.celery.memcache_lock')
    def test_summary_task_overview_data(self, mock_lock):
        from bims.tasks.spatial_dashboard import spatial_dashboard_summary

        mock_lock.return_value.__enter__ = lambda s: True
        mock_lock.return_value.__exit__ = lambda s, *a: None

        search_process = self._create_search_process(SPATIAL_DASHBOARD_SUMMARY)
        spatial_dashboard_summary(
            search_parameters=self.search_params,
            search_process_id=search_process.id,
        )
        search_process.refresh_from_db()
        results = search_process.get_file_if_exits()
        overview = results['overview']
        self.assertIn('Number of Taxa', overview)
        taxa_counts = overview['Number of Taxa']
        self.assertIn('Fish', taxa_counts)
        self.assertEqual(taxa_counts['Fish'], 2)

    @patch('bims.utils.celery.memcache_lock')
    def test_map_task(self, mock_lock):
        from bims.tasks.spatial_dashboard import spatial_dashboard_map

        mock_lock.return_value.__enter__ = lambda s: True
        mock_lock.return_value.__exit__ = lambda s, *a: None

        search_process = self._create_search_process(SPATIAL_DASHBOARD_MAP)
        spatial_dashboard_map(
            search_parameters=self.search_params,
            search_process_id=search_process.id,
        )
        search_process.refresh_from_db()
        self.assertTrue(search_process.finished)
        results = search_process.get_file_if_exits()
        self.assertIsNotNone(results)
        self.assertIn('extent', results)
        self.assertIn('sites_raw_query', results)

    def test_task_with_nonexistent_search_process(self):
        from bims.tasks.spatial_dashboard import spatial_dashboard_cons_status

        # Should not raise an exception
        result = spatial_dashboard_cons_status(
            search_parameters={},
            search_process_id=99999,
        )
        self.assertIsNone(result)
