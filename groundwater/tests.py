from datetime import datetime, timedelta, timezone as dt_timezone

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.urls import reverse
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient

from bims.tests.model_factories import LocationSiteF
from groundwater.models import GroundwaterReading
from groundwater.utils import (
    build_chart_payload, summary_stats, HOURLY_WINDOW_DAYS
)


class GroundwaterTestBase(FastTenantTestCase):

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.site = LocationSiteF.create(site_code='GW001')
        # 48 hourly readings, level rises 0.1 m per hour from 10.0
        start = datetime(2024, 3, 1, 0, 0, tzinfo=dt_timezone.utc)
        for hour in range(48):
            GroundwaterReading.objects.create(
                location_site=self.site,
                timestamp=start + timedelta(hours=hour),
                water_level=10.0 + hour * 0.1,
                water_temperature=20.0,
            )


class GroundwaterAggregationTests(GroundwaterTestBase):

    def test_summary_stats(self):
        stats = summary_stats(
            GroundwaterReading.objects.filter(location_site=self.site))
        self.assertEqual(stats['count'], 48)
        self.assertEqual(stats['level_min'], 10.0)
        self.assertEqual(stats['level_max'], 14.7)
        self.assertEqual(stats['level_range'], 4.7)
        self.assertEqual(stats['temperature_avg'], 20.0)
        self.assertEqual(stats['completeness'], 100.0)
        self.assertIsNone(stats['conductivity_avg'])

    def test_completeness_with_gaps(self):
        GroundwaterReading.objects.filter(
            location_site=self.site,
            timestamp__hour__in=[5, 6]
        ).delete()
        stats = summary_stats(
            GroundwaterReading.objects.filter(location_site=self.site))
        self.assertEqual(stats['count'], 44)
        self.assertEqual(stats['completeness'], round(44 / 48 * 100, 1))

    def test_chart_payload_granularities(self):
        payload = build_chart_payload(
            GroundwaterReading.objects.filter(location_site=self.site))
        self.assertEqual(len(payload['hourly']['timestamps']), 48)
        self.assertEqual(len(payload['daily']['timestamps']), 2)
        self.assertEqual(len(payload['monthly']['timestamps']), 1)
        self.assertEqual(len(payload['annual']['timestamps']), 1)

    def test_hourly_window_is_limited(self):
        start = datetime(2023, 1, 1, tzinfo=dt_timezone.utc)
        readings = [
            GroundwaterReading(
                location_site=self.site,
                timestamp=start + timedelta(days=d),
                water_level=1.0
            ) for d in range(90)
        ]
        GroundwaterReading.objects.bulk_create(readings)
        payload = build_chart_payload(
            GroundwaterReading.objects.filter(location_site=self.site))
        # Hourly only covers the last days of the data
        last = max(payload['hourly']['timestamps'])
        first = min(payload['hourly']['timestamps'])
        self.assertLessEqual(
            last - first, HOURLY_WINDOW_DAYS * 24 * 3600 * 1000)


class GroundwaterViewTests(GroundwaterTestBase):

    def test_dashboard(self):
        url = reverse(
            'groundwater:groundwater-site', kwargs={'site_id': self.site.id})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Groundwater data for GW001')
        self.assertEqual(response.context['stats']['count'], 48)
        self.assertTrue(response.context['availability']['level'])
        self.assertFalse(response.context['availability']['conductivity'])

    def test_date_range_filter(self):
        url = reverse(
            'groundwater:groundwater-site', kwargs={'site_id': self.site.id})
        response = self.client.get(
            url, {'startDate': '2024-03-02', 'endDate': '2024-03-02'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context['stats']['count'], 24)
        self.assertEqual(
            response.context['stats_overall']['count'], 48)

    def test_site_without_data_returns_404(self):
        other = LocationSiteF.create(site_code='GW002')
        url = reverse(
            'groundwater:groundwater-site', kwargs={'site_id': other.id})
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_download_requires_private_user(self):
        url = reverse(
            'groundwater:groundwater-download',
            kwargs={'site_id': self.site.id})
        self.assertEqual(self.client.get(url).status_code, 403)

        user = get_user_model().objects.create_user(
            username='gw_private', password='pw')
        group, _ = Group.objects.get_or_create(name='PrivateDataGroup')
        user.groups.add(group)
        self.client.force_login(user)
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/csv')
        # header + 48 readings
        self.assertEqual(
            len(response.content.decode().strip().splitlines()), 49)

    def test_multi_site_dashboard(self):
        response = self.client.get(
            reverse('groundwater:groundwater-dashboard-multi-sites'))
        self.assertEqual(response.status_code, 200)
        stations = response.context['stations']
        self.assertEqual(len(stations), 1)
        self.assertEqual(stations[0]['readings'], 48)
