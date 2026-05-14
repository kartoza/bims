import json
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient

from bims.models.search_process import SearchProcess, SEARCH_RESULTS
from bims.models.upload_session import UploadSession
from bims.tests.model_factories import (
    LocationSiteF,
    LocationContextF,
    LocationContextGroupF,
)
from climate.models import Climate
from climate.scripts.climate_upload import ClimateCSVUpload
from climate.views import (
    _build_daily_records,
    _build_monthly_records,
    _build_annual_records,
    _build_historical_monthly_rainfall,
    _build_chart_payload,
)


class ClimateHelperFunctionsTests(FastTenantTestCase):
    """Tests for climate view helper functions."""

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.location_site = LocationSiteF.create(site_code='HELPER001')

        # Create climate data spanning multiple years and months
        test_data = [
            # Year 2023
            (date(2023, 1, 10), 18.0, 8.0, 28.0, 50.0, 4.0, 15.0),
            (date(2023, 1, 20), 19.0, 9.0, 29.0, 52.0, 4.5, 10.0),
            (date(2023, 2, 15), 21.0, 11.0, 31.0, 48.0, 5.0, 20.0),
            (date(2023, 6, 10), 15.0, 5.0, 25.0, 60.0, 3.0, 30.0),
            # Year 2024
            (date(2024, 1, 15), 20.0, 10.0, 30.0, 45.0, 5.0, 12.0),
            (date(2024, 1, 25), 22.0, 12.0, 32.0, 47.0, 5.5, 8.0),
            (date(2024, 2, 10), 23.0, 13.0, 33.0, 44.0, 6.0, 25.0),
            (date(2024, 6, 15), 14.0, 4.0, 24.0, 62.0, 2.5, 35.0),
        ]

        for d, avg_t, min_t, max_t, hum, wind, rain in test_data:
            Climate.objects.create(
                location_site=self.location_site,
                station_name='Test Station',
                date=d,
                year=d.year,
                month=d.month,
                avg_temperature=avg_t,
                min_temperature=min_t,
                max_temperature=max_t,
                avg_humidity=hum,
                min_humidity=hum - 5,
                max_humidity=hum + 5,
                avg_windspeed=wind,
                daily_rainfall=rain,
            )

    def test_build_daily_records(self):
        """Test that _build_daily_records returns correct daily data."""
        queryset = Climate.objects.filter(
            location_site=self.location_site
        ).order_by('date')

        daily_records = _build_daily_records(queryset)

        self.assertEqual(len(daily_records), 8)
        # Check first record
        first = daily_records[0]
        self.assertEqual(first['period'], date(2023, 1, 10))
        self.assertEqual(first['temperature']['avg'], 18.0)
        self.assertEqual(first['temperature']['min'], 8.0)
        self.assertEqual(first['temperature']['max'], 28.0)
        self.assertEqual(first['rainfall']['total'], 15.0)

    def test_build_monthly_records(self):
        """Test that _build_monthly_records aggregates data correctly."""
        queryset = Climate.objects.filter(
            location_site=self.location_site
        ).order_by('date')

        monthly_records = _build_monthly_records(queryset)

        # Should have 6 months (Jan 2023, Feb 2023, Jun 2023, Jan 2024, Feb 2024, Jun 2024)
        self.assertEqual(len(monthly_records), 6)

        # Check January 2023 (2 records: avg temps 18 and 19)
        jan_2023 = monthly_records[0]
        self.assertEqual(jan_2023['period'], date(2023, 1, 1))
        self.assertEqual(jan_2023['temperature']['avg'], 18.5)  # (18+19)/2
        self.assertEqual(jan_2023['temperature']['min'], 8.0)  # min of 8, 9
        self.assertEqual(jan_2023['temperature']['max'], 29.0)  # max of 28, 29
        self.assertEqual(jan_2023['rainfall']['total'], 25.0)  # 15 + 10

    def test_build_annual_records(self):
        """Test that _build_annual_records aggregates data by year."""
        queryset = Climate.objects.filter(
            location_site=self.location_site
        ).order_by('date')

        annual_records = _build_annual_records(queryset)

        self.assertEqual(len(annual_records), 2)  # 2023 and 2024

        # Check 2023 (4 records)
        year_2023 = annual_records[0]
        self.assertEqual(year_2023['period'], date(2023, 1, 1))
        # Rainfall total for 2023: 15 + 10 + 20 + 30 = 75
        self.assertEqual(year_2023['rainfall']['total'], 75.0)

        # Check 2024 (4 records)
        year_2024 = annual_records[1]
        self.assertEqual(year_2024['period'], date(2024, 1, 1))
        # Rainfall total for 2024: 12 + 8 + 25 + 35 = 80
        self.assertEqual(year_2024['rainfall']['total'], 80.0)

    def test_build_historical_monthly_rainfall(self):
        """Test that _build_historical_monthly_rainfall calculates averages correctly."""
        queryset = Climate.objects.filter(location_site=self.location_site)

        historical = _build_historical_monthly_rainfall(queryset)

        # January: 2023 total = 25mm, 2024 total = 20mm, average = 22.5mm
        self.assertEqual(historical[1], 22.5)

        # February: 2023 total = 20mm, 2024 total = 25mm, average = 22.5mm
        self.assertEqual(historical[2], 22.5)

        # June: 2023 total = 30mm, 2024 total = 35mm, average = 32.5mm
        self.assertEqual(historical[6], 32.5)

    def test_build_chart_payload_daily(self):
        """Test chart payload for daily granularity."""
        queryset = Climate.objects.filter(
            location_site=self.location_site
        ).order_by('date')
        daily_records = _build_daily_records(queryset)

        payload = _build_chart_payload(daily_records, 'daily')

        self.assertEqual(len(payload['labels']), 8)
        self.assertEqual(payload['labels'][0], '2023-01-10')
        self.assertEqual(payload['temperature']['avg'][0], 18.0)
        self.assertNotIn('historical', payload['rainfall'])

    def test_build_chart_payload_monthly(self):
        """Test chart payload for monthly granularity."""
        queryset = Climate.objects.filter(
            location_site=self.location_site
        ).order_by('date')
        monthly_records = _build_monthly_records(queryset)

        payload = _build_chart_payload(monthly_records, 'monthly')

        self.assertEqual(len(payload['labels']), 6)
        self.assertEqual(payload['labels'][0], 'Jan 2023')
        self.assertNotIn('historical', payload['rainfall'])

    def test_build_chart_payload_monthly_with_historical(self):
        """Test chart payload includes historical rainfall when provided."""
        queryset = Climate.objects.filter(
            location_site=self.location_site
        ).order_by('date')
        monthly_records = _build_monthly_records(queryset)
        historical = _build_historical_monthly_rainfall(queryset)

        payload = _build_chart_payload(monthly_records, 'monthly', historical)

        self.assertIn('historical', payload['rainfall'])
        # First month is January, historical average should be 22.5
        self.assertEqual(payload['rainfall']['historical'][0], 22.5)

    def test_build_chart_payload_annual(self):
        """Test chart payload for annual granularity."""
        queryset = Climate.objects.filter(
            location_site=self.location_site
        ).order_by('date')
        annual_records = _build_annual_records(queryset)

        payload = _build_chart_payload(annual_records, 'annual')

        self.assertEqual(len(payload['labels']), 2)
        self.assertEqual(payload['labels'][0], '2023')
        self.assertEqual(payload['labels'][1], '2024')


class ClimateSiteViewTests(FastTenantTestCase):
    """Tests for the climate site view."""

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.location_site = LocationSiteF.create(site_code='CLIM001')

        # Create test data
        Climate.objects.create(
            location_site=self.location_site,
            station_name='Station A',
            date=date(2024, 1, 15),
            year=2024,
            month=1,
            avg_temperature=20.0,
            min_temperature=10.0,
            max_temperature=30.0,
            avg_humidity=40.0,
            min_humidity=35.0,
            max_humidity=50.0,
            avg_windspeed=5.0,
            daily_rainfall=5.0,
        )
        Climate.objects.create(
            location_site=self.location_site,
            station_name='Station A',
            date=date(2024, 2, 15),
            year=2024,
            month=2,
            avg_temperature=22.0,
            min_temperature=12.0,
            max_temperature=32.0,
            avg_humidity=45.0,
            min_humidity=38.0,
            max_humidity=55.0,
            avg_windspeed=6.0,
            daily_rainfall=10.0,
        )

    def test_site_view_builds_monthly_payload(self):
        """Test that the site view builds the correct monthly payload."""
        url = reverse('climate:climate-site', kwargs={'site_id': self.location_site.id})
        response = self.client.get(url, {'siteId': self.location_site.id})

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.context['chart_payload'])

        # Check monthly data
        self.assertEqual(payload['monthly']['labels'], ['Jan 2024', 'Feb 2024'])
        self.assertEqual(payload['monthly']['temperature']['avg'][0], 20.0)
        self.assertEqual(payload['monthly']['temperature']['max'][1], 32.0)
        self.assertEqual(payload['monthly']['rainfall']['total'][1], 10.0)

        self.assertTrue(response.context['availability']['temperature'])
        self.assertTrue(response.context['availability']['rainfall'])

    def test_site_view_includes_all_granularities(self):
        """Test that the site view includes daily, monthly, and annual data."""
        url = reverse('climate:climate-site', kwargs={'site_id': self.location_site.id})
        response = self.client.get(url, {'siteId': self.location_site.id})

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.context['chart_payload'])

        self.assertIn('daily', payload)
        self.assertIn('monthly', payload)
        self.assertIn('annual', payload)

    def test_site_view_includes_historical_rainfall(self):
        """Test that monthly payload includes historical rainfall data."""
        url = reverse('climate:climate-site', kwargs={'site_id': self.location_site.id})
        response = self.client.get(url, {'siteId': self.location_site.id})

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.context['chart_payload'])

        self.assertIn('historical', payload['monthly']['rainfall'])

    def test_site_view_monthly_climate_records(self):
        """Test that climate_records contains monthly averaged data."""
        url = reverse('climate:climate-site', kwargs={'site_id': self.location_site.id})
        response = self.client.get(url, {'siteId': self.location_site.id})

        self.assertEqual(response.status_code, 200)
        records = response.context['climate_records']

        self.assertEqual(len(records), 2)  # 2 months
        self.assertEqual(records[0]['period_formatted'], 'Jan 2024')
        self.assertEqual(records[0]['avg_temperature'], 20.0)
        self.assertEqual(records[1]['period_formatted'], 'Feb 2024')
        self.assertEqual(records[1]['total_rainfall'], 10.0)

    def test_site_view_date_range_filter(self):
        """Test that the site view filters by date range."""
        url = reverse('climate:climate-site', kwargs={'site_id': self.location_site.id})
        response = self.client.get(url, {
            'siteId': self.location_site.id,
            'startDate': '2024-02-01',
            'endDate': '2024-02-28'
        })

        self.assertEqual(response.status_code, 200)
        records = response.context['climate_records']

        # Should only have February
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['period_formatted'], 'Feb 2024')


class ClimatePrivateUserTests(FastTenantTestCase):
    """Tests for private user access to daily data."""

    def setUp(self):
        self.client = TenantClient(self.tenant)
        User = get_user_model()

        # Create private group
        self.private_group, _ = Group.objects.get_or_create(name='PrivateDataGroup')

        # Create regular user
        self.regular_user = User.objects.create_user(
            username='regular',
            email='regular@example.com',
            password='test-pass'
        )

        # Create private user
        self.private_user = User.objects.create_user(
            username='private',
            email='private@example.com',
            password='test-pass'
        )
        self.private_user.groups.add(self.private_group)

        self.location_site = LocationSiteF.create(site_code='PRIV001')
        Climate.objects.create(
            location_site=self.location_site,
            station_name='Station A',
            date=date(2024, 1, 15),
            year=2024,
            month=1,
            avg_temperature=20.0,
            min_temperature=10.0,
            max_temperature=30.0,
            avg_humidity=40.0,
            min_humidity=35.0,
            max_humidity=50.0,
            avg_windspeed=5.0,
            daily_rainfall=5.0,
        )

    def test_regular_user_no_daily_csv(self):
        """Test that regular users don't get daily_records_json."""
        self.client.login(username='regular', password='test-pass')
        url = reverse('climate:climate-site', kwargs={'site_id': self.location_site.id})
        response = self.client.get(url, {'siteId': self.location_site.id})

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.context['is_private_user'])
        self.assertNotIn('daily_records_json', response.context)

    def test_private_user_gets_daily_csv(self):
        """Test that private users get daily_records_json."""
        self.client.login(username='private', password='test-pass')
        url = reverse('climate:climate-site', kwargs={'site_id': self.location_site.id})
        response = self.client.get(url, {'siteId': self.location_site.id})

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.context['is_private_user'])
        self.assertIn('daily_records_json', response.context)

        # Verify daily records content
        daily_records = json.loads(response.context['daily_records_json'])
        self.assertEqual(len(daily_records), 1)
        self.assertEqual(daily_records[0]['date'], '2024-01-15')
        self.assertEqual(daily_records[0]['avg_temperature'], 20.0)

    def test_anonymous_user_no_daily_csv(self):
        """Test that anonymous users don't get daily_records_json."""
        url = reverse('climate:climate-site', kwargs={'site_id': self.location_site.id})
        response = self.client.get(url, {'siteId': self.location_site.id})

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.context['is_private_user'])
        self.assertNotIn('daily_records_json', response.context)


class ClimateUploadViewTests(FastTenantTestCase):
    """Tests for climate upload view."""

    def setUp(self):
        self.client = TenantClient(self.tenant)
        User = get_user_model()
        self.user = User.objects.create_user(
            username='uploader',
            email='uploader@example.com',
            password='test-pass'
        )
        self.user.is_superuser = True
        self.user.is_staff = True
        self.user.save()
        self.session = UploadSession.objects.create(
            uploader=self.user,
            category='climate',
            progress='1/2',
            process_file=SimpleUploadedFile('climate.csv', b'site,data')
        )
        self.url = reverse('climate:climate-upload')

    def test_upload_view_requires_auth(self):
        """Test that upload view requires authentication."""
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.startswith('/accounts/login/'))

    def test_upload_view_lists_sessions(self):
        """Test that upload view lists upload sessions."""
        self.client.login(username='uploader', password='test-pass')
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        upload_sessions = list(response.context['upload_sessions'])
        self.assertEqual(upload_sessions, [self.session])
        finished_sessions = response.context['finished_sessions']
        self.assertEqual(finished_sessions.count(), 0)


class ClimateModelTests(FastTenantTestCase):
    """Tests for the Climate model."""

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.location_site = LocationSiteF.create(site_code='MODEL001')

    def test_climate_model_creation(self):
        """Test creating a Climate record."""
        climate = Climate.objects.create(
            location_site=self.location_site,
            station_name='Test Station',
            date=date(2024, 3, 15),
            year=2024,
            month=3,
            avg_temperature=25.5,
            min_temperature=15.0,
            max_temperature=35.0,
            avg_humidity=55.0,
            avg_windspeed=7.5,
            daily_rainfall=12.5,
        )

        self.assertEqual(climate.station_name, 'Test Station')
        self.assertEqual(climate.date, date(2024, 3, 15))
        self.assertEqual(climate.avg_temperature, 25.5)
        self.assertEqual(climate.daily_rainfall, 12.5)

    def test_climate_model_nullable_fields(self):
        """Test that optional fields can be null."""
        climate = Climate.objects.create(
            location_site=self.location_site,
            station_name='Minimal Station',
            date=date(2024, 4, 1),
            year=2024,
            month=4,
        )

        self.assertIsNone(climate.avg_temperature)
        self.assertIsNone(climate.daily_rainfall)
        self.assertIsNone(climate.avg_humidity)


class ClimateEdgeCasesTests(FastTenantTestCase):
    """Tests for edge cases in climate views."""

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.location_site = LocationSiteF.create(site_code='EDGE001')

    def test_site_view_no_data(self):
        """Test site view when no climate data exists."""
        url = reverse('climate:climate-site', kwargs={'site_id': self.location_site.id})
        response = self.client.get(url, {'siteId': self.location_site.id})

        # Should return 404 when no climate data
        self.assertEqual(response.status_code, 404)

    def test_site_view_missing_site_id_param(self):
        """Test site view without siteId parameter."""
        url = reverse('climate:climate-site', kwargs={'site_id': self.location_site.id})
        response = self.client.get(url)

        self.assertEqual(response.status_code, 404)

    def test_site_view_invalid_site_id(self):
        """Test site view with invalid site ID."""
        url = reverse('climate:climate-site', kwargs={'site_id': 99999})
        response = self.client.get(url, {'siteId': 99999})

        self.assertEqual(response.status_code, 404)

    def test_single_month_data(self):
        """Test that a single month of data displays correctly."""
        Climate.objects.create(
            location_site=self.location_site,
            station_name='Single Month Station',
            date=date(2024, 5, 15),
            year=2024,
            month=5,
            avg_temperature=28.0,
            min_temperature=18.0,
            max_temperature=38.0,
            avg_humidity=35.0,
            avg_windspeed=4.0,
            daily_rainfall=0.0,
        )

        url = reverse('climate:climate-site', kwargs={'site_id': self.location_site.id})
        response = self.client.get(url, {'siteId': self.location_site.id})

        self.assertEqual(response.status_code, 200)
        records = response.context['climate_records']
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['period_formatted'], 'May 2024')

    def test_null_values_in_records(self):
        """Test handling of null values in climate records."""
        Climate.objects.create(
            location_site=self.location_site,
            station_name='Partial Data Station',
            date=date(2024, 6, 15),
            year=2024,
            month=6,
            avg_temperature=None,
            min_temperature=None,
            max_temperature=None,
            avg_humidity=None,
            avg_windspeed=None,
            daily_rainfall=None,
        )

        url = reverse('climate:climate-site', kwargs={'site_id': self.location_site.id})
        response = self.client.get(url, {'siteId': self.location_site.id})

        self.assertEqual(response.status_code, 200)
        records = response.context['climate_records']
        self.assertEqual(len(records), 1)
        self.assertIsNone(records[0]['avg_temperature'])
        self.assertIsNone(records[0]['total_rainfall'])


class ClimateDashboardMultipleSitesViewTests(FastTenantTestCase):
    """Tests for the multi-site climate dashboard template view."""

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.url = '/climate/dashboard-multi-sites/'

    def test_view_returns_200(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)

    def test_view_uses_correct_template(self):
        response = self.client.get(self.url)
        self.assertTemplateUsed(response, 'climate/multi_site.html')


class ClimateDashboardMultipleSitesApiViewTests(FastTenantTestCase):
    """Tests for the multi-site climate dashboard API view."""

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.api_url = '/climate/dashboard-multi-sites-api/'

        self.site_a = LocationSiteF.create(site_code='MULTI001')
        self.site_b = LocationSiteF.create(site_code='MULTI002')
        self.site_no_climate = LocationSiteF.create(site_code='NOCLIM')

        for d, site in [
            (date(2023, 1, 10), self.site_a),
            (date(2023, 3, 15), self.site_a),
            (date(2023, 6, 20), self.site_b),
        ]:
            Climate.objects.create(
                location_site=site,
                station_name='Station',
                date=d,
                year=d.year,
                month=d.month,
                avg_temperature=20.0,
                min_temperature=10.0,
                max_temperature=30.0,
                avg_humidity=50.0,
                min_humidity=45.0,
                max_humidity=55.0,
                avg_windspeed=5.0,
                daily_rainfall=10.0,
            )

    def _get_json(self, params=None):
        response = self.client.get(self.api_url, params or {})
        self.assertEqual(response.status_code, 200)
        return response.json()

    def test_returns_only_sites_with_climate_data(self):
        data = self._get_json()
        site_ids = data['climate_summary_data']['site_id']
        self.assertIn(self.site_a.id, site_ids)
        self.assertIn(self.site_b.id, site_ids)
        self.assertNotIn(self.site_no_climate.id, site_ids)

    def test_response_structure(self):
        data = self._get_json()
        summary = data['climate_summary_data']
        for key in ('site_id', 'site_code', 'avg_temp', 'min_temp', 'max_temp',
                    'avg_humidity', 'avg_windspeed', 'total_rainfall',
                    'max_rainfall', 'record_count'):
            self.assertIn(key, summary)
        self.assertIn('coordinates', data)
        self.assertIn('bing_map_key', data)

    def test_site_code_values(self):
        data = self._get_json()
        site_codes = data['climate_summary_data']['site_code']
        self.assertIn('MULTI001', site_codes)
        self.assertIn('MULTI002', site_codes)

    def test_aggregate_stats_for_site_a(self):
        """site_a has 2 records, each with rainfall=10 → total=20."""
        data = self._get_json()
        summary = data['climate_summary_data']
        idx = summary['site_id'].index(self.site_a.id)
        self.assertEqual(summary['record_count'][idx], 2)
        self.assertEqual(summary['total_rainfall'][idx], 20.0)
        self.assertEqual(summary['avg_temp'][idx], 20.0)
        self.assertEqual(summary['min_temp'][idx], 10.0)
        self.assertEqual(summary['max_temp'][idx], 30.0)

    def test_aggregate_stats_for_site_b(self):
        """site_b has 1 record."""
        data = self._get_json()
        summary = data['climate_summary_data']
        idx = summary['site_id'].index(self.site_b.id)
        self.assertEqual(summary['record_count'][idx], 1)
        self.assertEqual(summary['total_rainfall'][idx], 10.0)

    def test_filter_by_site_id(self):
        data = self._get_json({'siteId': self.site_a.id})
        summary = data['climate_summary_data']
        self.assertEqual(summary['site_id'], [self.site_a.id])

    def test_filter_by_search_term_matches_site_code(self):
        data = self._get_json({'search': 'MULTI001'})
        summary = data['climate_summary_data']
        self.assertEqual(summary['site_id'], [self.site_a.id])

    def test_filter_by_search_term_no_match(self):
        data = self._get_json({'search': 'ZZZNOMATCH'})
        summary = data['climate_summary_data']
        self.assertEqual(summary['site_id'], [])

    def test_coordinates_match_returned_sites(self):
        data = self._get_json()
        self.assertEqual(
            len(data['coordinates']),
            len(data['climate_summary_data']['site_id'])
        )

    def test_empty_result_when_no_climate_data(self):
        Climate.objects.all().delete()
        data = self._get_json()
        self.assertEqual(data['climate_summary_data']['site_id'], [])
        self.assertEqual(data['coordinates'], [])

    def test_sites_ordered_by_site_code(self):
        data = self._get_json()
        codes = data['climate_summary_data']['site_code']
        self.assertEqual(codes, sorted(codes))


class ClimateDashboardMultipleSitesSanparksTests(FastTenantTestCase):
    """Tests for park name in multi-site summary when site_code_generator is sanparks."""

    def setUp(self):
        from bims.models.site_setting import SiteSetting
        self.client = TenantClient(self.tenant)
        self.api_url = '/climate/dashboard-multi-sites-api/'
        self.template_url = '/climate/dashboard-multi-sites/'

        self._site_setting = SiteSetting.singleton.get()
        self._original_generator = self._site_setting.site_code_generator
        self._site_setting.site_code_generator = 'sanparks'
        self._site_setting.save()

        self.park_group = LocationContextGroupF.create(
            name='sanparks and mpas',
        )

        self.site = LocationSiteF.create(site_code='KNP00001')
        self.site_no_park = LocationSiteF.create(site_code='KNP00002')

        LocationContextF.create(
            site=self.site,
            group=self.park_group,
            value='Kruger National Park',
        )

        for site in [self.site, self.site_no_park]:
            Climate.objects.create(
                location_site=site,
                station_name='Station',
                date=date(2024, 1, 10),
                year=2024,
                month=1,
                avg_temperature=25.0,
                min_temperature=15.0,
                max_temperature=35.0,
                avg_humidity=50.0,
                avg_windspeed=5.0,
                daily_rainfall=10.0,
            )

    def tearDown(self):
        self._site_setting.site_code_generator = self._original_generator
        self._site_setting.save()

    def _get_json(self):
        response = self.client.get(self.api_url)
        self.assertEqual(response.status_code, 200)
        return response.json()

    def test_park_name_field_present_in_summary(self):
        data = self._get_json()
        self.assertIn('park_name', data['climate_summary_data'])
        self.assertIsNotNone(data['climate_summary_data']['park_name'])

    def test_park_name_populated_from_location_context(self):
        data = self._get_json()
        summary = data['climate_summary_data']
        idx = summary['site_id'].index(self.site.id)
        self.assertEqual(summary['park_name'][idx], 'Kruger National Park')

    def test_site_code_unchanged(self):
        """site_code should remain the raw site code, not prefixed with park name."""
        data = self._get_json()
        summary = data['climate_summary_data']
        idx = summary['site_id'].index(self.site.id)
        self.assertEqual(summary['site_code'][idx], 'KNP00001')

    def test_park_name_empty_when_no_context(self):
        """Sites without a park context entry get an empty park name."""
        data = self._get_json()
        summary = data['climate_summary_data']
        idx = summary['site_id'].index(self.site_no_park.id)
        self.assertEqual(summary['park_name'][idx], '')

    def test_park_name_field_absent_for_non_sanparks(self):
        self._site_setting.site_code_generator = 'bims'
        self._site_setting.save()
        data = self._get_json()
        self.assertIsNone(data['climate_summary_data']['park_name'])

    def test_template_view_is_sanparks_context(self):
        response = self.client.get(self.template_url)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.context['is_sanparks'])

    def test_template_shows_park_name_column(self):
        response = self.client.get(self.template_url)
        self.assertContains(response, 'Park Name')


class ClimateUploadClearSearchProcessTests(FastTenantTestCase):
    """Tests for _clear_climate_search_results in ClimateCSVUpload."""

    def _make_uploader(self, canceled=False):
        session = MagicMock()
        session.id = 1
        session.canceled = canceled
        uploader = ClimateCSVUpload.__new__(ClimateCSVUpload)
        uploader.upload_session = session
        uploader.success_list = []
        uploader.error_list = []
        return uploader

    def _make_search_process(self, raw_query, locked=False):
        return SearchProcess.objects.create(
            category=SEARCH_RESULTS,
            process_id=f'test-{SearchProcess.objects.count()}',
            search_raw_query=raw_query,
            finished=True,
            locked=locked,
        )

    def test_clears_climate_search_process_records(self):
        """process_ended deletes unlocked SearchProcess records referencing climate_climate."""
        sp = self._make_search_process(
            'SELECT ... FROM "climate_climate" WHERE ...'
        )
        uploader = self._make_uploader()
        uploader._clear_climate_search_results()
        self.assertFalse(SearchProcess.objects.filter(pk=sp.pk).exists())

    def test_does_not_delete_locked_records(self):
        """Locked SearchProcess records are preserved even when they reference climate_climate."""
        sp = self._make_search_process(
            'SELECT ... FROM "climate_climate" WHERE ...',
            locked=True,
        )
        uploader = self._make_uploader()
        uploader._clear_climate_search_results()
        self.assertTrue(SearchProcess.objects.filter(pk=sp.pk).exists())

    def test_does_not_delete_unrelated_search_process(self):
        """SearchProcess records without climate_climate in their query are left untouched."""
        sp = self._make_search_process(
            'SELECT ... FROM "bims_biologicalcollectionrecord" WHERE ...'
        )
        uploader = self._make_uploader()
        uploader._clear_climate_search_results()
        self.assertTrue(SearchProcess.objects.filter(pk=sp.pk).exists())

    def test_process_ended_skips_clear_when_canceled(self):
        """process_ended does not clear search results when the session was canceled."""
        sp = self._make_search_process(
            'SELECT ... FROM "climate_climate" WHERE ...'
        )
        uploader = self._make_uploader(canceled=True)
        uploader.process_ended()
        self.assertTrue(SearchProcess.objects.filter(pk=sp.pk).exists())

    def test_process_ended_clears_on_success(self):
        """process_ended clears climate search results when the upload succeeds."""
        sp = self._make_search_process(
            'SELECT ... FROM "climate_climate" WHERE ...'
        )
        uploader = self._make_uploader(canceled=False)
        uploader.process_ended()
        self.assertFalse(SearchProcess.objects.filter(pk=sp.pk).exists())


class ClimateMissingValueTests(FastTenantTestCase):
    """-999 sentinel preservation: upload, calculations, and daily CSV download."""

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.location_site = LocationSiteF.create(site_code='MISS001')

        # One normal record and one record whose measurements are all -999.
        Climate.objects.create(
            location_site=self.location_site,
            station_name='Station',
            date=date(2024, 3, 1),
            year=2024, month=3,
            avg_temperature=20.0,
            min_temperature=10.0,
            max_temperature=30.0,
            avg_humidity=50.0,
            min_humidity=45.0,
            max_humidity=55.0,
            avg_windspeed=5.0,
            daily_rainfall=10.0,
        )
        Climate.objects.create(
            location_site=self.location_site,
            station_name='Station',
            date=date(2024, 3, 2),
            year=2024, month=3,
            avg_temperature=-999.0,
            min_temperature=-999.0,
            max_temperature=-999.0,
            avg_humidity=-999.0,
            min_humidity=-999.0,
            max_humidity=-999.0,
            avg_windspeed=-999.0,
            daily_rainfall=-999.0,
        )

    # ------------------------------------------------------------------
    # Upload helper
    # ------------------------------------------------------------------

    def test_get_float_value_returns_sentinel(self):
        """-999 in a CSV cell must be stored as -999.0, not as None."""
        from climate.scripts.climate_upload import ClimateCSVUpload
        uploader = ClimateCSVUpload.__new__(ClimateCSVUpload)
        self.assertEqual(uploader.get_float_value({'x': '-999'}, 'x'), -999.0)

    def test_get_float_value_returns_none_for_empty(self):
        """Empty cell still produces None."""
        from climate.scripts.climate_upload import ClimateCSVUpload
        uploader = ClimateCSVUpload.__new__(ClimateCSVUpload)
        self.assertIsNone(uploader.get_float_value({'x': ''}, 'x'))
        self.assertIsNone(uploader.get_float_value({}, 'x'))

    def test_get_float_value_preserves_zero(self):
        """Zero is a real measurement and must not be confused with -999."""
        from climate.scripts.climate_upload import ClimateCSVUpload
        uploader = ClimateCSVUpload.__new__(ClimateCSVUpload)
        self.assertEqual(uploader.get_float_value({'x': '0'}, 'x'), 0.0)
        self.assertEqual(uploader.get_float_value({'x': '0.0'}, 'x'), 0.0)

    # ------------------------------------------------------------------
    # _round_value helper
    # ------------------------------------------------------------------

    def test_round_value_treats_sentinel_as_none(self):
        from climate.views import _round_value, MISSING
        self.assertIsNone(_round_value(MISSING))
        self.assertIsNone(_round_value(-999.0))
        self.assertIsNone(_round_value(None))
        self.assertEqual(_round_value(0.0), 0.0)
        self.assertEqual(_round_value(20.5), 20.5)

    # ------------------------------------------------------------------
    # _build_daily_records  (used for charts — -999 should become None)
    # ------------------------------------------------------------------

    def test_build_daily_records_converts_sentinel_to_none(self):
        """-999 values must appear as None in chart data (not plotted)."""
        qs = Climate.objects.filter(
            location_site=self.location_site
        ).order_by('date')
        records = _build_daily_records(qs)

        normal = records[0]
        missing = records[1]

        self.assertEqual(normal['temperature']['avg'], 20.0)
        self.assertEqual(normal['rainfall']['total'], 10.0)

        # All -999 fields should be None in chart data
        self.assertIsNone(missing['temperature']['avg'])
        self.assertIsNone(missing['temperature']['min'])
        self.assertIsNone(missing['temperature']['max'])
        self.assertIsNone(missing['rainfall']['total'])

    # ------------------------------------------------------------------
    # _build_monthly_records  (summary — -999 must not skew averages)
    # ------------------------------------------------------------------

    def test_build_monthly_records_excludes_sentinel(self):
        """Monthly summary for March 2024 must only use the real record."""
        qs = Climate.objects.filter(
            location_site=self.location_site
        ).order_by('date')
        monthly = _build_monthly_records(qs)

        # March 2024 — only 1 valid record (the -999 record contributes nothing)
        self.assertEqual(len(monthly), 1)
        march = monthly[0]
        self.assertEqual(march['temperature']['avg'], 20.0)
        self.assertEqual(march['temperature']['min'], 10.0)
        self.assertEqual(march['temperature']['max'], 30.0)
        self.assertEqual(march['rainfall']['total'], 10.0)

    def test_build_monthly_records_sentinel_does_not_create_cold_day(self):
        """-999 for min_temperature must not be counted as a cold day (<0°C)."""
        qs = Climate.objects.filter(
            location_site=self.location_site
        ).order_by('date')
        monthly = _build_monthly_records(qs)
        march = monthly[0]
        self.assertEqual(march['extremes']['cold_days'], 0)

    # ------------------------------------------------------------------
    # _build_historical_monthly_rainfall  (must skip -999)
    # ------------------------------------------------------------------

    def test_historical_rainfall_excludes_sentinel(self):
        """Historical average must be based only on real measurements."""
        qs = Climate.objects.filter(location_site=self.location_site)
        historical = _build_historical_monthly_rainfall(qs)
        # Only the real record (10 mm) counts — the -999 row is excluded.
        self.assertEqual(historical.get(3), 10.0)

    # ------------------------------------------------------------------
    # Daily CSV download — -999 must be written verbatim
    # ------------------------------------------------------------------

    def test_daily_csv_preserves_sentinel(self):
        """-999 in the database must appear as -999 in the daily CSV download,
        not as a blank."""
        from django.contrib.auth import get_user_model
        from django.contrib.auth.models import Group

        User = get_user_model()
        private_group, _ = Group.objects.get_or_create(name='PrivateDataGroup')
        user = User.objects.create_user(
            username='private_miss', password='pass', email='pm@example.com'
        )
        user.groups.add(private_group)

        self.client.login(username='private_miss', password='pass')
        url = reverse('climate:climate-site', kwargs={'site_id': self.location_site.id})
        response = self.client.get(url, {'siteId': self.location_site.id})

        self.assertEqual(response.status_code, 200)
        self.assertIn('daily_records_json', response.context)

        records = json.loads(response.context['daily_records_json'])
        self.assertEqual(len(records), 2)

        # Sort by date so the -999 record is always second
        records_by_date = {r['date']: r for r in records}
        missing_record = records_by_date['2024-03-02']

        self.assertEqual(missing_record['avg_temperature'], -999.0)
        self.assertEqual(missing_record['min_temperature'], -999.0)
        self.assertEqual(missing_record['max_temperature'], -999.0)
        self.assertEqual(missing_record['daily_rainfall'], -999.0)

    def test_daily_csv_preserves_zero(self):
        """Zero measurements (real data) must not be confused with None/blank."""
        Climate.objects.create(
            location_site=self.location_site,
            station_name='Station',
            date=date(2024, 3, 3),
            year=2024, month=3,
            avg_temperature=0.0,
            min_temperature=0.0,
            max_temperature=0.0,
            avg_humidity=0.0,
            avg_windspeed=0.0,
            daily_rainfall=0.0,
        )

        from django.contrib.auth import get_user_model
        from django.contrib.auth.models import Group

        User = get_user_model()
        private_group, _ = Group.objects.get_or_create(name='PrivateDataGroup')
        user = User.objects.create_user(
            username='private_zero', password='pass', email='pz@example.com'
        )
        user.groups.add(private_group)

        self.client.login(username='private_zero', password='pass')
        url = reverse('climate:climate-site', kwargs={'site_id': self.location_site.id})
        response = self.client.get(url, {'siteId': self.location_site.id})

        records = json.loads(response.context['daily_records_json'])
        records_by_date = {r['date']: r for r in records}
        zero_record = records_by_date['2024-03-03']

        self.assertEqual(zero_record['avg_temperature'], 0.0)
        self.assertEqual(zero_record['daily_rainfall'], 0.0)
