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


class GroundwaterUploadTests(FastTenantTestCase):
    """Upload of hourly groundwater readings from a CSV file."""

    HEADER = (
        'Station,Latitude,Longitude,Timestamp,Water level,'
        'Water temperature,Electrical conductivity,Flag\n'
    )

    def setUp(self):
        from bims.models.upload_session import UploadSession
        self.UploadSession = UploadSession
        self.user = get_user_model().objects.create_user(
            username='gw_uploader', password='pw')

    def upload(self, content, name='groundwater.csv'):
        from django.core.files.uploadedfile import SimpleUploadedFile
        from groundwater.upload import GroundwaterCSVUpload
        session = self.UploadSession.objects.create(
            uploader=self.user,
            category='groundwater',
            process_file=SimpleUploadedFile(name, content.encode('utf-8'))
        )
        uploader = GroundwaterCSVUpload(session)
        uploader.start(encoding='utf-8-sig')
        session.refresh_from_db()
        return session, uploader

    def test_upload_creates_station_and_readings(self):
        session, uploader = self.upload(
            self.HEADER +
            'Borehole A,-24.99,31.58,2025-01-01 00:00,12.5,24.1,650,\n'
            'Borehole A,-24.99,31.58,2025-01-01 01:00,12.6,-999,651,'
            'Sensor fault\n'
        )
        self.assertTrue(session.processed)
        self.assertFalse(session.canceled)
        self.assertEqual(len(uploader.success_list), 2)
        self.assertEqual(len(uploader.error_list), 0)
        readings = GroundwaterReading.objects.filter(
            location_site__name='Borehole A').order_by('timestamp')
        self.assertEqual(readings.count(), 2)
        self.assertTrue(uploader.success_list[0]['link'].endswith(
            f'/groundwaterreading/{readings[0].pk}/change/'))
        self.assertEqual(readings[0].water_level, 12.5)
        self.assertEqual(
            readings[0].timestamp,
            datetime(2025, 1, 1, 0, 0, tzinfo=dt_timezone.utc))
        # -999 is stored as missing
        self.assertIsNone(readings[1].water_temperature)
        self.assertEqual(readings[1].flag, 'Sensor fault')
        site = readings[0].location_site
        self.assertEqual(site.location_type.name, 'Groundwater Station')
        self.assertTrue(site.site_code)

    def test_upload_updates_existing_reading(self):
        row = 'Borehole B,-24.99,31.58,2025-01-01 00:00,{level},,,\n'
        self.upload(self.HEADER + row.format(level='10.0'))
        self.upload(self.HEADER + row.format(level='11.0'))
        readings = GroundwaterReading.objects.filter(
            location_site__name='Borehole B')
        self.assertEqual(readings.count(), 1)
        self.assertEqual(readings.get().water_level, 11.0)

    def test_existing_site_is_not_changed(self):
        site = LocationSiteF.create(
            name='Existing Borehole', site_code='KEEP001')
        location_type = site.location_type
        self.upload(
            self.HEADER +
            'existing borehole,-20.0,30.0,2025-01-01 00:00,5.0,,,\n')
        site.refresh_from_db()
        self.assertEqual(site.site_code, 'KEEP001')
        self.assertEqual(site.location_type, location_type)
        self.assertEqual(
            GroundwaterReading.objects.filter(location_site=site).count(), 1)

    def test_invalid_rows_go_to_error_file(self):
        session, uploader = self.upload(
            self.HEADER +
            'Borehole C,-24.99,31.58,2025-01-01 00:00,12.5,,,\n'
            'Borehole C,-24.99,31.58,not a date,12.5,,,\n'
            'Borehole C,-24.99,31.58,2025-01-01 02:00,,,,\n'
            'Borehole C,-24.99,31.58,2025-01-01 03:00,deep,,,\n'
            'Borehole C,-124.99,31.58,2025-01-01 04:00,12.5,,,\n'
        )
        self.assertTrue(session.processed)
        self.assertEqual(len(uploader.success_list), 1)
        messages = [row['error_message'] for row in uploader.error_list]
        self.assertEqual(len(messages), 4)
        self.assertIn('Invalid timestamp', messages[0])
        self.assertEqual(messages[1], 'No measurement values')
        self.assertIn('is not a number', messages[2])
        self.assertIn('out of range', messages[3])
        self.assertTrue(session.error_file)

    def test_duplicate_rows_keep_the_last(self):
        session, uploader = self.upload(
            self.HEADER +
            'Borehole D,-24.99,31.58,2025-01-01 00:00,1.0,,,\n'
            'Borehole D,-24.99,31.58,2025-01-01 00:00,2.0,,,\n'
        )
        reading = GroundwaterReading.objects.get(
            location_site__name='Borehole D')
        self.assertEqual(reading.water_level, 2.0)
        self.assertEqual(len(uploader.error_list), 1)

    def test_missing_headers_cancel_the_upload(self):
        session, _ = self.upload(
            'Station,Latitude,Longitude,Water level\n'
            'Borehole E,-24.99,31.58,12.5\n')
        self.assertTrue(session.canceled)
        self.assertIn('Timestamp', session.progress)
        self.assertFalse(GroundwaterReading.objects.filter(
            location_site__name='Borehole E').exists())

    def test_missing_measurement_columns_cancel_the_upload(self):
        session, _ = self.upload(
            'Station,Latitude,Longitude,Timestamp\n'
            'Borehole F,-24.99,31.58,2025-01-01 00:00\n')
        self.assertTrue(session.canceled)
        self.assertIn('No measurement columns', session.progress)

    def test_download_headers_and_bom_are_accepted(self):
        session, uploader = self.upload(
            '﻿Station,Latitude,Longitude, Timestamp ,'
            'Water level (m below ground)\n'
            'Borehole G,-24.99,31.58,2025-01-01T05:00:00+02:00,7.5\n')
        self.assertEqual(len(uploader.success_list), 1)
        reading = GroundwaterReading.objects.get(
            location_site__name='Borehole G')
        self.assertEqual(reading.water_level, 7.5)
        self.assertEqual(
            reading.timestamp,
            datetime(2025, 1, 1, 3, 0, tzinfo=dt_timezone.utc))


class GroundwaterUploadViewTests(FastTenantTestCase):

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.url = reverse('groundwater:groundwater-upload')

    def test_requires_login(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 302)

    def test_superuser_can_open_upload_page(self):
        user = get_user_model().objects.create_superuser(
            username='gw_admin', password='pw', email='gw@example.com')
        self.client.force_login(user)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Upload Groundwater Data')
        self.assertContains(response, 'groundwater_upload_template.csv')


class GroundwaterSiteOverviewTests(GroundwaterTestBase):
    """The multiple sites overview with the groundwater module selected
    only counts the biodiversity records of groundwater stations."""

    def setUp(self):
        super().setUp()
        from bims.enums import TaxonomicGroupCategory
        from bims.tests.model_factories import (
            BiologicalCollectionRecordF, TaxonGroupF
        )
        self.group = TaxonGroupF.create(
            name='Fish',
            category=TaxonomicGroupCategory.SPECIES_MODULE.name
        )
        BiologicalCollectionRecordF.create(
            site=self.site, module_group=self.group,
            source_collection='test')
        BiologicalCollectionRecordF.create(
            site=LocationSiteF.create(site_code='OTHER'),
            module_group=self.group, source_collection='test')

    def overview(self, module):
        from bims.api_views.location_site_overview import (
            LocationSiteOverviewData
        )
        overview = LocationSiteOverviewData()
        # A search without any filter returns no records
        overview.search_filters = {
            'module': module, 'sourceCollection': '["test"]'}
        return overview.biodiversity_data()['Fish']

    def test_occurrence_module_counts_all_sites(self):
        fish = self.overview('occurrence')
        self.assertEqual(fish['occurrences'], 2)
        self.assertEqual(fish['sites'], 2)

    def test_groundwater_module_counts_groundwater_sites(self):
        fish = self.overview('groundwater')
        self.assertEqual(fish['occurrences'], 1)
        self.assertEqual(fish['sites'], 1)

    def test_climate_module_without_climate_sites(self):
        fish = self.overview('climate')
        self.assertEqual(fish['occurrences'], 0)
        self.assertEqual(fish['sites'], 0)
