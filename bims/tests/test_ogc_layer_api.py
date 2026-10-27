# coding=utf-8
"""Tests for the API to create, list and revoke OGC layers."""
from django.core.cache import cache
from django.test import override_settings
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


class OgcLayerApiTestCase(FastTenantTestCase):

    def setUp(self):
        cache.clear()
        self.client = TenantClient(self.tenant)
        self.owner = UserF.create(username='api_owner')
        self.other = UserF.create(username='api_other')
        self.staff = UserF.create(username='api_staff', is_staff=True)
        self.creator = UserF.create(username='api_creator', is_staff=True)
        self.taxon = TaxonomyF.create()
        self.site = LocationSiteF.create()
        for _ in range(3):
            BiologicalCollectionRecordF.create(
                taxonomy=self.taxon, site=self.site)
        self.list_url = reverse('ogc-layer-list')

    def detail_url(self, layer, action=None):
        name = 'ogc-layer-detail' if action is None else (
            'ogc-layer-%s' % action)
        return reverse(name, kwargs={'pk': layer.pk})

    def create_layer(self, owner=None, **kwargs):
        return OgcLayer.objects.create(
            name=kwargs.pop('name', 'Layer'),
            owner=owner or self.owner,
            query=kwargs.pop('query', {'taxon': str(self.taxon.id)}),
            **kwargs)

    def post(self, data):
        return self.client.post(
            self.list_url, data, content_type='application/json')

    # Permissions

    def test_requires_authentication(self):
        self.assertIn(
            self.client.get(self.list_url).status_code, (401, 403))
        self.assertIn(self.post({'name': 'x'}).status_code, (401, 403))

    # Create

    def test_create_from_query_string(self):
        self.client.force_login(self.creator)
        response = self.post({
            'name': 'My layer',
            'query_string': '?taxon=%s&requester=99' % self.taxon.id,
        })
        self.assertEqual(response.status_code, 201, response.content)
        data = response.json()
        layer = OgcLayer.objects.get(pk=data['id'])
        self.assertEqual(layer.owner, self.creator)
        self.assertEqual(layer.query, {'taxon': str(self.taxon.id)})
        self.assertEqual(data['record_count'], 3)
        self.assertTrue(data['url'].startswith('http'))
        self.assertTrue(data['url'].endswith('/'))
        self.assertIn('/ogc/layers/', data['url'])
        token = data['url'].rstrip('/').rsplit('/', 1)[1]
        self.assertEqual(OgcLayer.get_by_token(token), layer)

    def test_created_url_serves_the_layer(self):
        self.client.force_login(self.creator)
        url = self.post({
            'name': 'L', 'query': {'taxon': str(self.taxon.id)}
        }).json()['url']
        self.client.logout()
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)

    def test_create_from_query_object(self):
        self.client.force_login(self.creator)
        response = self.post(
            {'name': 'L', 'query': {'taxon': str(self.taxon.id)}})
        self.assertEqual(response.status_code, 201, response.content)

    def test_create_rejects_empty_filter(self):
        self.client.force_login(self.creator)
        for payload in (
            {'name': 'L', 'query': {}},
            {'name': 'L', 'query_string': ''},
            {'name': 'L', 'query_string': 'yearFrom=2000'},
            {'name': 'L'},
        ):
            response = self.post(payload)
            self.assertEqual(response.status_code, 400, payload)
        self.assertFalse(OgcLayer.objects.exists())

    def test_create_rejects_blank_name(self):
        self.client.force_login(self.creator)
        response = self.post({'name': '  ', 'query': {'taxon': '1'}})
        self.assertEqual(response.status_code, 400)

    @override_settings(OGC_LAYER_MAX_RECORDS=2)
    def test_create_rejects_over_cap(self):
        self.client.force_login(self.creator)
        response = self.post(
            {'name': 'L', 'query': {'taxon': str(self.taxon.id)}})
        self.assertEqual(response.status_code, 400)
        self.assertIn('more than 2 records', response.json()['query'][0])
        self.assertFalse(OgcLayer.objects.exists())

    def test_create_rejects_query_and_query_string(self):
        self.client.force_login(self.creator)
        response = self.post({
            'name': 'L', 'query': {'taxon': '1'}, 'query_string': 'taxon=1'})
        self.assertEqual(response.status_code, 400)

    def test_create_rejects_sensitive_without_group(self):
        self.client.force_login(self.creator)
        response = self.post({
            'name': 'L', 'query': {'taxon': str(self.taxon.id)},
            'include_sensitive': True})
        self.assertEqual(response.status_code, 400)
        self.assertIn('include_sensitive', response.json())

    @override_settings(OGC_MAX_LAYERS_PER_OWNER=1)
    def test_create_respects_layer_limit(self):
        self.create_layer(owner=self.creator)
        self.client.force_login(self.creator)
        response = self.post(
            {'name': 'L', 'query': {'taxon': str(self.taxon.id)}})
        self.assertEqual(response.status_code, 400)

    @override_settings(OGC_LAYERS_ENABLED=False)
    def test_create_when_disabled(self):
        self.client.force_login(self.creator)
        response = self.post(
            {'name': 'L', 'query': {'taxon': str(self.taxon.id)}})
        self.assertEqual(response.status_code, 503)

    def test_create_and_preview_need_staff(self):
        self.client.force_login(self.owner)
        self.assertEqual(self.post(
            {'name': 'L', 'query': {'taxon': str(self.taxon.id)}}
        ).status_code, 403)
        response = self.client.post(
            self.list_url + 'preview/',
            {'query_string': 'taxon=%s' % self.taxon.id},
            content_type='application/json')
        self.assertEqual(response.status_code, 403)
        self.assertEqual(OgcLayer.objects.count(), 0)

    # List and retrieve

    def test_list_only_own_layers(self):
        mine = self.create_layer()
        self.create_layer(owner=self.other, name='Theirs')
        self.client.force_login(self.owner)
        response = self.client.get(self.list_url)
        self.assertEqual(response.status_code, 200)
        results = response.json()
        results = results.get('results', results)
        self.assertEqual([r['id'] for r in results], [mine.id])
        self.assertIsNone(results[0]['url'])

    def test_staff_lists_all_layers(self):
        self.create_layer()
        self.create_layer(owner=self.other)
        self.client.force_login(self.staff)
        results = self.client.get(self.list_url).json()
        results = results.get('results', results)
        self.assertEqual(len(results), 2)

    def test_retrieve_has_current_count_and_masked_url(self):
        layer = self.create_layer()
        self.client.force_login(self.owner)
        data = self.client.get(self.detail_url(layer)).json()
        self.assertEqual(data['record_count'], 3)
        self.assertIsNone(data['url'])
        self.assertIn('...' + layer.token_hint, data['masked_url'])
        BiologicalCollectionRecordF.create(
            taxonomy=self.taxon, site=self.site)
        cache.clear()
        data = self.client.get(self.detail_url(layer)).json()
        self.assertEqual(data['record_count'], 4)

    def test_retrieve_other_users_layer_is_404(self):
        layer = self.create_layer(owner=self.other)
        self.client.force_login(self.owner)
        self.assertEqual(
            self.client.get(self.detail_url(layer)).status_code, 404)

    @override_settings(OGC_LAYER_MAX_RECORDS=2)
    def test_retrieve_over_cap_still_works(self):
        layer = self.create_layer()
        self.client.force_login(self.owner)
        response = self.client.get(self.detail_url(layer))
        self.assertEqual(response.status_code, 200)

    def test_staff_retrieves_any_layer(self):
        layer = self.create_layer(owner=self.other)
        self.client.force_login(self.staff)
        self.assertEqual(
            self.client.get(self.detail_url(layer)).status_code, 200)

    # Update

    def test_update_name(self):
        layer = self.create_layer()
        old_hash = layer.token_hash
        self.client.force_login(self.owner)
        response = self.client.patch(
            self.detail_url(layer), {'name': 'Renamed'},
            content_type='application/json')
        self.assertEqual(response.status_code, 200, response.content)
        layer.refresh_from_db()
        self.assertEqual(layer.name, 'Renamed')
        self.assertEqual(layer.token_hash, old_hash)

    def test_update_cannot_change_filter(self):
        layer = self.create_layer()
        self.client.force_login(self.owner)
        response = self.client.patch(
            self.detail_url(layer), {'query': {'taxon': '999'}},
            content_type='application/json')
        self.assertEqual(response.status_code, 400)
        layer.refresh_from_db()
        self.assertEqual(layer.query, {'taxon': str(self.taxon.id)})

    def test_cannot_update_other_users_layer(self):
        layer = self.create_layer(owner=self.other)
        self.client.force_login(self.owner)
        response = self.client.patch(
            self.detail_url(layer), {'name': 'Mine now'},
            content_type='application/json')
        self.assertEqual(response.status_code, 404)

    def test_staff_cannot_rename_or_rotate_other_users_layer(self):
        layer = self.create_layer(owner=self.other)
        old_hash = layer.token_hash
        self.client.force_login(self.staff)
        response = self.client.patch(
            self.detail_url(layer), {'name': 'x'},
            content_type='application/json')
        self.assertEqual(response.status_code, 403)
        response = self.client.post(self.detail_url(layer, 'rotate-token'))
        self.assertEqual(response.status_code, 403)
        layer.refresh_from_db()
        self.assertEqual(layer.token_hash, old_hash)

    # Rotate

    def test_rotate_token_invalidates_old_url(self):
        layer = self.create_layer()
        old_token = layer.raw_token
        self.client.force_login(self.owner)
        response = self.client.post(self.detail_url(layer, 'rotate-token'))
        self.assertEqual(response.status_code, 200, response.content)
        new_url = response.json()['url']
        new_token = new_url.rstrip('/').rsplit('/', 1)[1]
        self.assertNotEqual(new_token, old_token)
        self.client.logout()
        old_url = reverse('ogc-landing-page', kwargs={'token': old_token})
        self.assertEqual(self.client.get(old_url).status_code, 404)
        self.assertEqual(self.client.get(new_url).status_code, 200)

    def test_cannot_rotate_other_users_layer(self):
        layer = self.create_layer(owner=self.other)
        self.client.force_login(self.owner)
        response = self.client.post(self.detail_url(layer, 'rotate-token'))
        self.assertEqual(response.status_code, 404)

    def test_cannot_rotate_revoked_layer(self):
        layer = self.create_layer(is_active=False)
        self.client.force_login(self.owner)
        response = self.client.post(self.detail_url(layer, 'rotate-token'))
        self.assertEqual(response.status_code, 400)

    # Revoke and delete

    def test_revoke_stops_the_url(self):
        layer = self.create_layer()
        token = layer.raw_token
        self.client.force_login(self.owner)
        response = self.client.post(self.detail_url(layer, 'revoke'))
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()['is_active'])
        self.assertTrue(OgcLayer.objects.filter(pk=layer.pk).exists())
        self.client.logout()
        url = reverse('ogc-landing-page', kwargs={'token': token})
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_delete(self):
        layer = self.create_layer()
        self.client.force_login(self.owner)
        response = self.client.delete(self.detail_url(layer))
        self.assertEqual(response.status_code, 204)
        self.assertFalse(OgcLayer.objects.filter(pk=layer.pk).exists())

    def test_cannot_delete_other_users_layer(self):
        layer = self.create_layer(owner=self.other)
        self.client.force_login(self.owner)
        response = self.client.delete(self.detail_url(layer))
        self.assertEqual(response.status_code, 404)
        self.assertTrue(OgcLayer.objects.filter(pk=layer.pk).exists())

    def test_staff_can_revoke_and_delete_any_layer(self):
        layer = self.create_layer(owner=self.other)
        self.client.force_login(self.staff)
        self.assertEqual(
            self.client.post(self.detail_url(layer, 'revoke')).status_code,
            200)
        self.assertEqual(
            self.client.delete(self.detail_url(layer)).status_code, 204)
