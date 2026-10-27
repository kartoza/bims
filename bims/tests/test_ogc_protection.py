# coding=utf-8
"""Tests for the protection of the shared database by OGC layers."""
from unittest import mock

from django.conf import settings
from django.core.cache import cache
from django.db import connection
from django.test import RequestFactory, SimpleTestCase, override_settings
from django.urls import reverse
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient

from bims.models import OgcLayer
from bims.tests.model_factories import (
    BiologicalCollectionRecordF,
    LocationSiteF,
    TaxonomyF,
    UserF,
)
from bims.utils import ogc_layer as ogc_layer_utils
from bims.utils.ogc_protection import (
    OgcLayerBusyError,
    OgcLayerThrottledError,
    OgcLayerTimeoutError,
    _schema_key,
    check_rate,
    client_ip,
    evaluation_slot,
    single_flight,
    statement_timeout,
)

LOCMEM = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'ogc-protection-tests',
    }
}


@override_settings(CACHES=LOCMEM)
class ProtectionTestCase(FastTenantTestCase):

    def setUp(self):
        cache.clear()


class TestStatementTimeout(ProtectionTestCase):

    def test_slow_query_is_cancelled(self):
        with self.assertRaises(OgcLayerTimeoutError) as raised:
            with statement_timeout(0.2):
                with connection.cursor() as cursor:
                    cursor.execute('SELECT pg_sleep(3)')
        self.assertGreater(raised.exception.retry_after, 0)

    def test_fast_query_is_not_affected(self):
        with statement_timeout(5):
            with connection.cursor() as cursor:
                cursor.execute('SELECT 1')
                self.assertEqual(cursor.fetchone()[0], 1)

    def test_disabled_with_zero(self):
        with statement_timeout(0):
            with connection.cursor() as cursor:
                cursor.execute('SELECT pg_sleep(0.3)')

    def test_other_database_errors_are_not_hidden(self):
        from django.db import ProgrammingError
        with self.assertRaises(ProgrammingError):
            with statement_timeout(5):
                with connection.cursor() as cursor:
                    cursor.execute('SELECT * FROM table_that_is_missing')

    def test_index_scans_can_be_switched_off_for_a_block(self):
        def setting():
            with connection.cursor() as cursor:
                cursor.execute('SHOW enable_indexscan')
                return cursor.fetchone()[0]

        self.assertEqual(setting(), 'on')
        with statement_timeout(5, avoid_index_scan=True):
            self.assertEqual(setting(), 'off')
        self.assertEqual(setting(), 'on')

    def test_index_scans_can_be_switched_off_without_a_timeout(self):
        with statement_timeout(0, avoid_index_scan=True):
            with connection.cursor() as cursor:
                cursor.execute('SHOW enable_indexscan')
                self.assertEqual(cursor.fetchone()[0], 'off')

    def test_timeout_does_not_outlive_the_block(self):
        with connection.cursor() as cursor:
            cursor.execute('SHOW statement_timeout')
            before = cursor.fetchone()[0]
        with statement_timeout(5):
            pass
        with connection.cursor() as cursor:
            cursor.execute('SHOW statement_timeout')
            self.assertEqual(cursor.fetchone()[0], before)


class TestCheckRate(ProtectionTestCase):

    def test_allows_up_to_the_limit_then_refuses(self):
        for _ in range(3):
            check_rate('ip', '1.2.3.4', 3)
        with self.assertRaises(OgcLayerThrottledError) as raised:
            check_rate('ip', '1.2.3.4', 3)
        self.assertTrue(1 <= raised.exception.retry_after <= 60)

    def test_identifiers_are_counted_separately(self):
        check_rate('ip', 'a', 1)
        check_rate('ip', 'b', 1)
        with self.assertRaises(OgcLayerThrottledError):
            check_rate('ip', 'a', 1)

    def test_scopes_are_counted_separately(self):
        check_rate('ip', 'a', 1)
        check_rate('layer', 'a', 1)

    def test_zero_limit_disables_the_check(self):
        for _ in range(10):
            check_rate('ip', 'a', 0)

    def test_new_window_resets_the_count(self):
        with mock.patch('time.time', return_value=1000):
            check_rate('ip', 'a', 1)
        with mock.patch('time.time', return_value=1061):
            check_rate('ip', 'a', 1)

    def test_fails_open_when_the_cache_is_down(self):
        with mock.patch.object(cache, 'incr', side_effect=Exception('down')):
            for _ in range(5):
                check_rate('ip', 'a', 1)


class TestClientIp(SimpleTestCase):

    def request(self, **meta):
        return RequestFactory().get('/', **meta)

    @override_settings(OGC_TRUSTED_PROXIES=1)
    def test_uses_the_address_added_by_the_proxy(self):
        request = self.request(
            HTTP_X_FORWARDED_FOR='6.6.6.6, 9.9.9.9', REMOTE_ADDR='10.0.0.1')
        self.assertEqual(client_ip(request), '9.9.9.9')

    @override_settings(OGC_TRUSTED_PROXIES=2)
    def test_skips_one_entry_per_extra_proxy(self):
        request = self.request(
            HTTP_X_FORWARDED_FOR='6.6.6.6, 9.9.9.9, 10.1.1.1')
        self.assertEqual(client_ip(request), '9.9.9.9')

    @override_settings(OGC_TRUSTED_PROXIES=0)
    def test_ignores_forwarded_header_without_proxy(self):
        request = self.request(
            HTTP_X_FORWARDED_FOR='6.6.6.6', REMOTE_ADDR='7.7.7.7')
        self.assertEqual(client_ip(request), '7.7.7.7')

    @override_settings(OGC_TRUSTED_PROXIES=1)
    def test_falls_back_to_remote_addr(self):
        self.assertEqual(
            client_ip(self.request(REMOTE_ADDR='7.7.7.7')), '7.7.7.7')


class TestSingleFlight(ProtectionTestCase):

    def setUp(self):
        super().setUp()
        overrides = self.settings(
            OGC_LAYER_LOCK_WAIT=0,
            OGC_LAYER_LOCK_TTL=30,
            OGC_LAYER_MAX_CONCURRENT_EVALUATIONS=2,
        )
        overrides.enable()
        self.addCleanup(overrides.disable)

    def test_builds_when_the_value_is_missing(self):
        self.assertEqual(
            single_flight('a', lambda: None, lambda: 'built'), 'built')

    def test_returns_the_cached_value_without_building(self):
        build = mock.Mock()
        self.assertEqual(single_flight('a', lambda: 'hit', build), 'hit')
        build.assert_not_called()

    def test_skips_build_when_value_appears_after_taking_the_lock(self):
        values = iter([None, 'filled'])
        build = mock.Mock()
        # Not found before the lock, found by the double check
        result = single_flight(
            'a', lambda: next(values, 'filled'), build)
        self.assertEqual(result, 'filled')
        build.assert_not_called()

    def test_lock_is_released_after_build(self):
        single_flight('a', lambda: None, lambda: 1)
        self.assertEqual(single_flight('a', lambda: None, lambda: 2), 2)

    def test_lock_is_released_when_build_fails(self):
        with self.assertRaises(ValueError):
            single_flight('a', lambda: None, mock.Mock(side_effect=ValueError))
        self.assertEqual(single_flight('a', lambda: None, lambda: 2), 2)

    def test_second_request_gets_busy_while_lock_is_held(self):
        cache.set(_schema_key('lock', 'a'), 1, 30)
        build = mock.Mock()
        with self.assertRaises(OgcLayerBusyError):
            single_flight('a', lambda: None, build)
        build.assert_not_called()

    def test_waiter_gets_the_value_built_by_the_lock_holder(self):
        cache.set(_schema_key('lock', 'a'), 1, 30)
        values = iter([None, 'from-holder'])
        with override_settings(OGC_LAYER_LOCK_WAIT=2):
            with mock.patch('time.sleep'):
                result = single_flight(
                    'a', lambda: next(values), mock.Mock())
        self.assertEqual(result, 'from-holder')

    def test_other_names_are_not_blocked(self):
        cache.set(_schema_key('lock', 'a'), 1, 30)
        self.assertEqual(single_flight('b', lambda: None, lambda: 'ok'), 'ok')

    def test_concurrent_evaluations_are_capped(self):
        self.assertEqual(settings.OGC_LAYER_MAX_CONCURRENT_EVALUATIONS, 2)
        with evaluation_slot():
            with evaluation_slot():
                with self.assertRaises(OgcLayerBusyError):
                    with evaluation_slot():
                        pass
        # Slots are given back
        with evaluation_slot():
            with evaluation_slot():
                pass

    def test_waits_for_a_slot_that_is_given_back(self):
        sleeps = []
        with evaluation_slot():
            with evaluation_slot():
                def give_back(_):
                    sleeps.append(1)
                    # Another request finishes while this one waits
                    cache.decr(_schema_key('slots'))

                with self.settings(OGC_LAYER_LOCK_WAIT=5):
                    with mock.patch('time.sleep', side_effect=give_back):
                        with evaluation_slot():
                            pass
        self.assertEqual(sleeps, [1])

    def test_gives_up_after_waiting_for_a_slot(self):
        with evaluation_slot():
            with evaluation_slot():
                with self.settings(OGC_LAYER_LOCK_WAIT=1):
                    with mock.patch(
                        'time.monotonic', side_effect=[0, 0.5, 2, 2, 2]
                    ):
                        with mock.patch('time.sleep'):
                            with self.assertRaises(OgcLayerBusyError):
                                with evaluation_slot():
                                    pass

    def test_slot_is_given_back_when_the_block_fails(self):
        for _ in range(5):
            with self.assertRaises(ValueError):
                with evaluation_slot():
                    raise ValueError()


class TestProtectedEndpoints(FastTenantTestCase):

    def setUp(self):
        overrides = self.settings(
            CACHES=LOCMEM,
            OGC_LAYER_LOCK_WAIT=0,
            OGC_RATE_LIMIT_IP=0,
            OGC_RATE_LIMIT_LAYER=0,
            OGC_RATE_LIMIT_OWNER=0,
            OGC_RATE_LIMIT_BAD_TOKEN=0,
            OGC_RATE_LIMIT_EVALUATIONS=0,
        )
        overrides.enable()
        self.addCleanup(overrides.disable)
        cache.clear()
        self.client = TenantClient(self.tenant)
        self.owner = UserF.create(username='protect_owner')
        self.taxon = TaxonomyF.create()
        self.site = LocationSiteF.create()
        BiologicalCollectionRecordF.create(
            taxonomy=self.taxon, site=self.site)
        self.layer = OgcLayer.objects.create(
            name='Layer', owner=self.owner,
            query={'taxon': str(self.taxon.id)})
        self.token = self.layer.raw_token

    def items_url(self, token=None):
        return reverse('ogc-items', kwargs={
            'token': token or self.token, 'collection_id': 'records'})

    def landing_url(self, token=None):
        return reverse(
            'ogc-landing-page', kwargs={'token': token or self.token})

    def test_ip_limit_returns_429_with_retry_after(self):
        with override_settings(OGC_RATE_LIMIT_IP=2):
            self.assertEqual(self.client.get(self.landing_url()).status_code, 200)
            self.assertEqual(self.client.get(self.landing_url()).status_code, 200)
            response = self.client.get(self.landing_url())
        self.assertEqual(response.status_code, 429)
        self.assertGreaterEqual(int(response['Retry-After']), 1)
        self.assertEqual(response.json()['code'], 'TooManyRequests')

    def test_layer_limit_returns_429(self):
        with override_settings(OGC_RATE_LIMIT_LAYER=1):
            self.client.get(self.landing_url())
            response = self.client.get(self.landing_url())
        self.assertEqual(response.status_code, 429)

    def test_owner_limit_covers_all_the_owners_layers(self):
        other = OgcLayer.objects.create(
            name='Other', owner=self.owner,
            query={'taxon': str(self.taxon.id)})
        with override_settings(OGC_RATE_LIMIT_OWNER=1):
            self.client.get(self.landing_url())
            response = self.client.get(self.landing_url(other.raw_token))
        self.assertEqual(response.status_code, 429)

    def test_limits_are_per_client_address(self):
        with override_settings(OGC_RATE_LIMIT_IP=1, OGC_TRUSTED_PROXIES=1):
            first = self.client.get(
                self.landing_url(), HTTP_X_FORWARDED_FOR='1.1.1.1')
            second = self.client.get(
                self.landing_url(), HTTP_X_FORWARDED_FOR='2.2.2.2')
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)

    def test_unknown_tokens_hit_the_bad_token_limit(self):
        with override_settings(OGC_RATE_LIMIT_BAD_TOKEN=2):
            codes = [
                self.client.get(self.landing_url('nope%d' % i)).status_code
                for i in range(4)
            ]
        self.assertEqual(codes, [404, 404, 429, 429])

    def test_valid_requests_do_not_count_as_bad_tokens(self):
        with override_settings(OGC_RATE_LIMIT_BAD_TOKEN=1):
            for _ in range(3):
                self.assertEqual(
                    self.client.get(self.landing_url()).status_code, 200)

    def test_busy_layer_returns_503_with_retry_after(self):
        key = ogc_layer_utils._cache_key(self.layer)
        cache.set(_schema_key('lock', key), 1, 30)
        response = self.client.get(self.items_url())
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()['code'], 'ServiceUnavailable')
        self.assertGreaterEqual(int(response['Retry-After']), 1)

    def test_timeout_returns_503(self):
        with mock.patch.object(
            ogc_layer_utils, 'get_matched_queryset',
            side_effect=OgcLayerTimeoutError('too slow', retry_after=7)
        ):
            response = self.client.get(self.items_url())
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response['Retry-After'], '7')

    def test_repeated_fresh_evaluations_are_limited(self):
        # Freeze the clock: the limit is counted per minute, and a minute
        # change between the requests would reset the count
        with override_settings(OGC_RATE_LIMIT_EVALUATIONS=2), \
                mock.patch('bims.utils.ogc_protection.time.time',
                           return_value=1_700_000_010):
            codes = []
            for _ in range(3):
                cache.delete(ogc_layer_utils._cache_key(self.layer))
                codes.append(self.client.get(self.items_url()).status_code)
        self.assertEqual(codes, [200, 200, 429])

    def test_bbox_evaluations_have_their_own_limit(self):
        # Panning a map asks for a new bbox each time
        bboxes = ['0,0,1,1', '2,2,3,3', '4,4,5,5']
        with override_settings(OGC_RATE_LIMIT_EVALUATIONS=1,
                               OGC_RATE_LIMIT_BBOX_EVALUATIONS=2), \
                mock.patch('bims.utils.ogc_protection.time.time',
                           return_value=1_700_000_010):
            codes = [
                self.client.get(
                    self.items_url() + '?bbox=' + bbox).status_code
                for bbox in bboxes]
        self.assertEqual(codes, [200, 200, 429])

    def test_cached_requests_do_not_use_the_evaluation_budget(self):
        with override_settings(OGC_RATE_LIMIT_EVALUATIONS=1):
            codes = [self.client.get(self.items_url()).status_code
                     for _ in range(4)]
        self.assertEqual(codes, [200] * 4)

    def test_cached_ids_are_served_while_another_request_evaluates(self):
        self.client.get(self.items_url())
        key = ogc_layer_utils._cache_key(self.layer)
        cache.set(_schema_key('lock', key), 1, 30)
        self.assertEqual(self.client.get(self.items_url()).status_code, 200)

    def test_validate_layer_ignores_evaluation_limit(self):
        with override_settings(OGC_RATE_LIMIT_EVALUATIONS=1):
            for _ in range(3):
                ogc_layer_utils.validate_layer(self.layer)
