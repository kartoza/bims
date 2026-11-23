# coding=utf-8
"""Tests for the daily usage of OGC layers and the admin usage page."""
from datetime import timedelta

from django.core.cache import cache
from django.urls import reverse
from django.utils import timezone
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient

from bims.models import OgcLayer, OgcLayerDailyUsage
from bims.tests.model_factories import (
    BiologicalCollectionRecordF,
    LocationSiteF,
    TaxonomyF,
    UserF,
)
from bims.utils.ogc_usage import (
    layer_usage_report,
    owners_near_limit,
    usage_period,
    usage_report,
)

LOCMEM = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'ogc-usage-tests',
    }
}


class UsageTestCase(FastTenantTestCase):

    def setUp(self):
        overrides = self.settings(
            CACHES=LOCMEM,
            OGC_RATE_LIMIT_IP=0,
            OGC_RATE_LIMIT_LAYER=0,
            OGC_RATE_LIMIT_OWNER=0,
            OGC_RATE_LIMIT_BAD_TOKEN=0,
            OGC_RATE_LIMIT_EVALUATIONS=0,
            OGC_LAYER_MAX_FAILURES=3,
            OGC_MAX_LAYERS_PER_OWNER=5,
            OGC_LAYERS_ENABLED=True,
            OGC_SLOW_REQUEST_SECONDS=3,
        )
        overrides.enable()
        self.addCleanup(overrides.disable)
        cache.clear()
        self.client = TenantClient(self.tenant)
        self.owner = UserF.create(username='usage_owner')
        self.taxon = TaxonomyF.create()
        BiologicalCollectionRecordF.create(
            taxonomy=self.taxon, site=LocationSiteF.create())
        self.layer = self.create_layer(name='Fish layer')
        self.today = timezone.localdate()

    def create_layer(self, owner=None, **kwargs):
        kwargs.setdefault('query', {'taxon': str(self.taxon.id)})
        kwargs.setdefault('name', 'Layer')
        return OgcLayer.objects.create(owner=owner or self.owner, **kwargs)

    def items(self, layer=None, **params):
        layer = layer or self.layer
        url = reverse('ogc-items', kwargs={
            'token': layer.raw_token, 'collection_id': 'records'})
        return self.client.get(url, params)

    def usage(self, layer=None, day=None):
        return OgcLayerDailyUsage.objects.get(
            layer=layer or self.layer, date=day or self.today)


class TestDailyUsageAdd(UsageTestCase):

    def test_first_add_creates_the_day(self):
        OgcLayerDailyUsage.add(self.layer.pk, requests=4)
        usage = self.usage()
        self.assertEqual(usage.requests, 4)
        self.assertEqual(usage.slow_requests, 0)

    def test_later_adds_are_summed(self):
        OgcLayerDailyUsage.add(self.layer.pk, requests=4)
        OgcLayerDailyUsage.add(self.layer.pk, requests=2, timeouts=1)
        OgcLayerDailyUsage.add(self.layer.pk, slow_requests=1)
        usage = self.usage()
        self.assertEqual(usage.requests, 6)
        self.assertEqual(usage.timeouts, 1)
        self.assertEqual(usage.slow_requests, 1)
        self.assertEqual(OgcLayerDailyUsage.objects.count(), 1)

    def test_slowest_keeps_the_largest_duration(self):
        OgcLayerDailyUsage.add(self.layer.pk, slow_requests=1,
                               slowest_ms=5000)
        OgcLayerDailyUsage.add(self.layer.pk, slow_requests=1,
                               slowest_ms=3000)
        self.assertEqual(self.usage().slowest_ms, 5000)
        OgcLayerDailyUsage.add(self.layer.pk, slow_requests=1,
                               slowest_ms=9000)
        self.assertEqual(self.usage().slowest_ms, 9000)

    def test_days_are_kept_apart(self):
        yesterday = self.today - timedelta(days=1)
        OgcLayerDailyUsage.add(self.layer.pk, requests=1, day=yesterday)
        OgcLayerDailyUsage.add(self.layer.pk, requests=2)
        self.assertEqual(self.usage(day=yesterday).requests, 1)
        self.assertEqual(self.usage().requests, 2)

    def test_no_layer_is_ignored(self):
        OgcLayerDailyUsage.add(None, requests=1)
        self.assertFalse(OgcLayerDailyUsage.objects.exists())

    def test_usage_is_deleted_with_the_layer(self):
        OgcLayerDailyUsage.add(self.layer.pk, requests=1)
        self.layer.delete()
        self.assertFalse(OgcLayerDailyUsage.objects.exists())


class TestUsageIsRecorded(UsageTestCase):

    def test_written_requests_are_added_to_today(self):
        self.layer.add_requests(7)
        self.assertEqual(self.usage().requests, 7)

    def test_served_request_is_counted(self):
        self.assertEqual(self.items().status_code, 200)
        self.assertEqual(self.usage().requests, 1)

    def test_timeout_is_counted(self):
        self.layer.record_failure()
        self.layer.record_failure()
        self.assertEqual(self.usage().timeouts, 2)

    def test_slow_request_is_counted(self):
        with self.settings(OGC_SLOW_REQUEST_SECONDS=0.000001):
            self.items()
        usage = self.usage()
        self.assertEqual(usage.slow_requests, 1)

    def test_fast_request_is_not_slow(self):
        self.items()
        self.assertEqual(self.usage().slow_requests, 0)

    def test_request_with_unknown_token_records_nothing(self):
        url = reverse('ogc-items', kwargs={
            'token': 'unknown', 'collection_id': 'records'})
        with self.settings(OGC_SLOW_REQUEST_SECONDS=0.000001):
            self.client.get(url)
        self.assertFalse(OgcLayerDailyUsage.objects.exists())


class TestUsageReport(UsageTestCase):

    def test_period_parameter(self):
        self.assertEqual(usage_period('7'), 7)
        self.assertEqual(usage_period('90'), 90)
        self.assertEqual(usage_period('12'), 30)
        self.assertEqual(usage_period(None), 30)
        self.assertEqual(usage_period('abc'), 30)

    def test_requests_per_day_are_zero_filled(self):
        OgcLayerDailyUsage.add(
            self.layer.pk, requests=10, day=self.today - timedelta(days=2))
        OgcLayerDailyUsage.add(self.layer.pk, requests=5)
        report = usage_report(7, today=self.today)
        self.assertEqual(len(report['daily']), 7)
        self.assertEqual(report['daily'][0]['date'],
                         self.today - timedelta(days=6))
        self.assertEqual(report['daily'][-1]['date'], self.today)
        self.assertEqual(
            [day['requests'] for day in report['daily']],
            [0, 0, 0, 0, 10, 0, 5])
        self.assertEqual(report['daily'][4]['bar'], 100)
        self.assertEqual(report['daily'][6]['bar'], 50)
        self.assertEqual(report['totals']['requests'], 15)

    def test_usage_before_the_period_is_left_out(self):
        OgcLayerDailyUsage.add(
            self.layer.pk, requests=10, day=self.today - timedelta(days=8))
        report = usage_report(7, today=self.today)
        self.assertEqual(report['totals']['requests'], 0)

    def test_layers_are_sorted_by_requests(self):
        busy = self.create_layer(name='Busy layer')
        OgcLayerDailyUsage.add(busy.pk, requests=50)
        OgcLayerDailyUsage.add(self.layer.pk, requests=5)
        report = usage_report(7, today=self.today)
        names = [row['layer'].name for row in report['layers']]
        self.assertEqual(names, ['Busy layer', 'Fish layer'])
        self.assertEqual(report['layers'][0]['bar'], 100)
        self.assertEqual(report['layers'][1]['bar'], 10)

    def test_unused_active_layer_is_listed_with_zeros(self):
        report = usage_report(7, today=self.today)
        self.assertEqual(len(report['layers']), 1)
        self.assertEqual(report['layers'][0]['requests'], 0)

    def test_revoked_layer_without_usage_is_left_out(self):
        self.layer.is_active = False
        self.layer.save()
        self.assertEqual(usage_report(7, today=self.today)['layers'], [])

    def test_slow_layers_worst_first(self):
        slow = self.create_layer(name='Slow layer')
        failing = self.create_layer(name='Failing layer')
        OgcLayerDailyUsage.add(slow.pk, slow_requests=4, slowest_ms=6000)
        OgcLayerDailyUsage.add(failing.pk, timeouts=1)
        OgcLayerDailyUsage.add(self.layer.pk, requests=100)
        report = usage_report(7, today=self.today)
        names = [row['layer'].name for row in report['slow_layers']]
        self.assertEqual(names, ['Failing layer', 'Slow layer'])
        self.assertEqual(report['totals']['slowest_ms'], 6000)

    def test_auto_disabled_layers(self):
        for _ in range(3):
            self.layer.record_failure()
        revoked = self.create_layer(name='Revoked')
        revoked.is_active = False
        revoked.save()
        report = usage_report(7, today=self.today)
        self.assertEqual(report['auto_disabled'], [self.layer])

    def test_owners_near_the_limit(self):
        # Limit 5, owners are listed from 4 active layers
        for _ in range(3):
            self.create_layer()
        full = UserF.create(username='full_owner')
        for _ in range(5):
            self.create_layer(owner=full)
        few = UserF.create(username='few_owner')
        self.create_layer(owner=few)
        owners, threshold = owners_near_limit()
        self.assertEqual(threshold, 4)
        self.assertEqual(
            [(owner['owner__username'], owner['active_layers'],
              owner['at_limit']) for owner in owners],
            [('full_owner', 5, True), ('usage_owner', 4, False)])

    def test_revoked_layers_do_not_count_towards_the_limit(self):
        for _ in range(4):
            self.create_layer(is_active=False)
        owners, _ = owners_near_limit()
        self.assertEqual(owners, [])

    def test_no_limit_lists_no_owner(self):
        for _ in range(4):
            self.create_layer()
        self.assertEqual(owners_near_limit(limit=0), ([], 0))

    def test_layer_report(self):
        OgcLayerDailyUsage.add(self.layer.pk, requests=3,
                               day=self.today - timedelta(days=1))
        OgcLayerDailyUsage.add(self.layer.pk, requests=2, slow_requests=1,
                               slowest_ms=4000)
        other = self.create_layer(name='Other')
        OgcLayerDailyUsage.add(other.pk, requests=100)
        report = layer_usage_report(self.layer, 7, today=self.today)
        self.assertEqual(report['totals']['requests'], 5)
        self.assertEqual(report['totals']['slowest_ms'], 4000)
        self.assertEqual(report['daily_newest_first'][0]['date'], self.today)
        self.assertEqual(report['daily_newest_first'][0]['requests'], 2)


class TestUsageAdminViews(UsageTestCase):

    def setUp(self):
        super().setUp()
        self.admin = UserF.create(
            username='usage_admin', is_staff=True, is_superuser=True)

    def test_usage_page(self):
        OgcLayerDailyUsage.add(self.layer.pk, requests=12, timeouts=1)
        self.client.force_login(self.admin)
        response = self.client.get(
            reverse('admin:bims_ogclayer_usage'), {'days': 7})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Requests per day')
        self.assertContains(response, 'Fish layer')
        self.assertContains(response, 'Owners near the layer limit')
        self.assertEqual(response.context['days'], 7)

    def test_layer_usage_page(self):
        OgcLayerDailyUsage.add(self.layer.pk, requests=12)
        self.client.force_login(self.admin)
        response = self.client.get(reverse(
            'admin:bims_ogclayer_layer_usage', args=[self.layer.pk]))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Usage of Fish layer')
        self.assertEqual(response.context['totals']['requests'], 12)

    def test_unknown_layer_is_not_found(self):
        self.client.force_login(self.admin)
        response = self.client.get(reverse(
            'admin:bims_ogclayer_layer_usage', args=[999999]))
        self.assertEqual(response.status_code, 404)

    def test_changelist_links_to_usage(self):
        self.client.force_login(self.admin)
        response = self.client.get(reverse('admin:bims_ogclayer_changelist'))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, reverse('admin:bims_ogclayer_usage'))

    def test_non_staff_user_is_sent_to_login(self):
        self.client.force_login(self.owner)
        response = self.client.get(reverse('admin:bims_ogclayer_usage'))
        self.assertEqual(response.status_code, 302)
