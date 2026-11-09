# coding=utf-8
"""Tests for the OGC API - Features endpoints under /ogc/layers/<token>/."""
from django.core.cache import cache
from django.test import override_settings
from django.urls import reverse
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient
from rest_framework import status

from bims.models import OgcLayer
from bims.tests.model_factories import (
    BiologicalCollectionRecordF,
    LocationSiteF,
    TaxonomyF,
    UserF,
)


class OgcFeaturesApiTestCase(FastTenantTestCase):

    def setUp(self):
        cache.clear()
        self.client = TenantClient(self.tenant)
        self.owner = UserF.create(username='layer_owner')
        self.taxon = TaxonomyF.create()
        self.other_taxon = TaxonomyF.create()
        self.site = LocationSiteF.create()

    def create_record(self, **kwargs):
        kwargs.setdefault('taxonomy', self.taxon)
        kwargs.setdefault('site', self.site)
        return BiologicalCollectionRecordF.create(**kwargs)

    def create_layer(self, query=None, **kwargs):
        if query is None:
            query = {'taxon': str(self.taxon.id)}
        return OgcLayer.objects.create(
            name='Layer', owner=self.owner, query=query, **kwargs)

    def landing_url(self, token):
        return reverse('ogc-landing-page', kwargs={'token': token})

    def conformance_url(self, token):
        return reverse('ogc-conformance', kwargs={'token': token})

    def collections_url(self, token):
        return reverse('ogc-collections', kwargs={'token': token})

    def collection_url(self, token, collection_id='records'):
        return reverse(
            'ogc-collection',
            kwargs={'token': token, 'collection_id': collection_id})

    def items_url(self, token, collection_id='records'):
        return reverse(
            'ogc-items',
            kwargs={'token': token, 'collection_id': collection_id})

    def item_url(self, token, item_id, collection_id='records'):
        return reverse(
            'ogc-item',
            kwargs={
                'token': token,
                'collection_id': collection_id,
                'item_id': item_id,
            })


class TestTokenHandling(OgcFeaturesApiTestCase):

    def test_unknown_token_is_404(self):
        response = self.client.get(self.landing_url('not-a-real-token'))
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        body = response.json()
        self.assertEqual(body['code'], 'NotFound')

    def test_revoked_token_is_404(self):
        layer = self.create_layer()
        layer.is_active = False
        layer.save()
        response = self.client.get(self.landing_url(layer.raw_token))
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_expired_token_is_404(self):
        from datetime import timedelta
        from django.utils import timezone

        layer = self.create_layer(
            expires_at=timezone.now() - timedelta(days=1))
        response = self.client.get(self.landing_url(layer.raw_token))
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)


class TestLandingConformanceCollections(OgcFeaturesApiTestCase):

    def test_landing_page(self):
        layer = self.create_layer()
        response = self.client.get(self.landing_url(layer.raw_token))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        body = response.json()
        self.assertEqual(body['title'], layer.name)
        rels = {link['rel'] for link in body['links']}
        self.assertEqual(
            {'self', 'service-desc', 'conformance', 'data'}, rels)

    def test_landing_page_links_to_api_definition(self):
        layer = self.create_layer()
        body = self.client.get(self.landing_url(layer.raw_token)).json()
        link = next(l for l in body['links'] if l['rel'] == 'service-desc')
        self.assertEqual(
            link['type'], 'application/vnd.oai.openapi+json;version=3.0')
        self.assertTrue(link['href'].endswith('/api'))

    def test_api_definition(self):
        layer = self.create_layer()
        response = self.client.get('/ogc/layers/%s/api' % layer.raw_token)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response['Content-Type'],
            'application/vnd.oai.openapi+json;version=3.0')
        body = response.json()
        self.assertTrue(body['openapi'].startswith('3.0'))
        self.assertIn('/collections/{collectionId}/items', body['paths'])
        self.assertEqual(
            body['components']['parameters']['limit']['schema']['maximum'],
            5000)

    def test_api_definition_needs_a_valid_token(self):
        response = self.client.get('/ogc/layers/unknown/api')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_conformance(self):
        layer = self.create_layer()
        response = self.client.get(self.conformance_url(layer.raw_token))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        conforms_to = response.json()['conformsTo']
        self.assertIn(
            'http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core',
            conforms_to)
        self.assertIn(
            'http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson',
            conforms_to)

    def test_collections_list(self):
        layer = self.create_layer()
        response = self.client.get(self.collections_url(layer.raw_token))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        collections = response.json()['collections']
        self.assertEqual(len(collections), 1)
        self.assertEqual(collections[0]['id'], 'records')

    def test_single_collection(self):
        layer = self.create_layer()
        response = self.client.get(self.collection_url(layer.raw_token))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.json()['id'], 'records')

    def test_urls_work_without_trailing_slash(self):
        record = self.create_record()
        layer = self.create_layer()
        base = '/ogc/layers/%s' % layer.raw_token
        for path in (
            '', '/conformance', '/collections', '/collections/records',
            '/collections/records/items',
            '/collections/records/items/%s' % record.uuid,
        ):
            response = self.client.get(base + path)
            self.assertEqual(
                response.status_code, status.HTTP_200_OK, path)

    def test_urls_with_trailing_slash_still_work(self):
        layer = self.create_layer()
        base = '/ogc/layers/%s/' % layer.raw_token
        for path in ('', 'conformance/', 'collections/',
                     'collections/records/items/'):
            response = self.client.get(base + path)
            self.assertEqual(
                response.status_code, status.HTTP_200_OK, path)

    def test_generated_links_have_no_trailing_slash(self):
        layer = self.create_layer()
        self.create_record()
        urls = [
            self.landing_url(layer.raw_token),
            self.collections_url(layer.raw_token),
            self.collection_url(layer.raw_token),
            self.items_url(layer.raw_token),
        ]
        for url in urls:
            body = self.client.get(url).json()
            for link in body.get('links', []):
                path = link['href'].split('?')[0]
                self.assertFalse(path.endswith('/'), link['href'])
            for collection in body.get('collections', []):
                for link in collection['links']:
                    self.assertFalse(link['href'].endswith('/'), link)

    def test_generated_links_can_be_followed(self):
        layer = self.create_layer()
        body = self.client.get(self.landing_url(layer.raw_token)).json()
        for link in body['links']:
            path = link['href'].split('://', 1)[1].split('/', 1)[1]
            response = self.client.get('/' + path)
            self.assertEqual(
                response.status_code, status.HTTP_200_OK, link['href'])

    def test_collection_extent_covers_the_records(self):
        from django.contrib.gis.geos import Point
        self.create_record(site=LocationSiteF.create(
            geometry_point=Point(10.0, 20.0)))
        self.create_record(site=LocationSiteF.create(
            geometry_point=Point(12.0, 18.0)))
        layer = self.create_layer()
        for url in (self.collection_url(layer.raw_token),):
            spatial = self.client.get(url).json()['extent']['spatial']
            self.assertEqual(spatial['bbox'], [[10.0, 18.0, 12.0, 20.0]])
        listed = self.client.get(
            self.collections_url(layer.raw_token)).json()['collections'][0]
        self.assertEqual(
            listed['extent']['spatial']['bbox'], [[10.0, 18.0, 12.0, 20.0]])

    def test_collection_extent_left_out_without_records(self):
        layer = self.create_layer()
        body = self.client.get(self.collection_url(layer.raw_token)).json()
        self.assertNotIn('extent', body)

    @override_settings(OGC_LAYER_MAX_RECORDS=1)
    def test_collection_over_cap_is_422(self):
        self.create_record()
        self.create_record()
        layer = self.create_layer()
        response = self.client.get(self.collection_url(layer.raw_token))
        self.assertEqual(response.status_code, 422)

    def test_unknown_collection_is_404(self):
        layer = self.create_layer()
        response = self.client.get(
            self.collection_url(layer.raw_token, collection_id='other'))
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)


class TestItems(OgcFeaturesApiTestCase):

    def test_items_content_type_and_shape(self):
        record = self.create_record()
        layer = self.create_layer()
        response = self.client.get(self.items_url(layer.raw_token))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response['Content-Type'], 'application/geo+json')
        body = response.json()
        self.assertEqual(body['type'], 'FeatureCollection')
        self.assertEqual(body['numberMatched'], 1)
        self.assertEqual(body['numberReturned'], 1)
        feature = body['features'][0]
        self.assertEqual(feature['id'], str(record.uuid))
        self.assertEqual(feature['properties']['UUID'], str(record.uuid))
        self.assertEqual(
            feature['properties']['Taxon'],
            self.taxon.canonical_name)

    def test_cors_header_present(self):
        self.create_record()
        layer = self.create_layer()
        response = self.client.get(self.items_url(layer.raw_token))
        self.assertEqual(response['Access-Control-Allow-Origin'], '*')

    def test_pagination_pages_through_all_records_exactly_once(self):
        records = [self.create_record() for _ in range(5)]
        layer = self.create_layer()
        seen_ids = []
        url = self.items_url(layer.raw_token) + '?limit=2'
        while url:
            response = self.client.get(url)
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            body = response.json()
            seen_ids.extend(
                feature['properties']['UUID']
                for feature in body['features'])
            next_links = [
                link['href'] for link in body['links']
                if link['rel'] == 'next']
            if next_links:
                from urllib.parse import urlsplit
                split = urlsplit(next_links[0])
                url = '%s?%s' % (split.path, split.query)
            else:
                url = None
        self.assertEqual(sorted(seen_ids), sorted(str(r.uuid) for r in records))
        self.assertEqual(len(seen_ids), len(set(seen_ids)))

    def test_bad_limit_is_400(self):
        layer = self.create_layer()
        response = self.client.get(
            self.items_url(layer.raw_token), {'limit': 'abc'})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.json()['code'], 'InvalidParameter')

    def test_bad_bbox_is_400(self):
        layer = self.create_layer()
        response = self.client.get(
            self.items_url(layer.raw_token), {'bbox': '1,2,3'})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_bbox_filters_records(self):
        from django.contrib.gis.geos import Point

        inside_site = LocationSiteF.create(geometry_point=Point(10.0, 10.0))
        outside_site = LocationSiteF.create(
            geometry_point=Point(-80.0, -80.0))
        inside = self.create_record(site=inside_site)
        self.create_record(site=outside_site)
        layer = self.create_layer()
        response = self.client.get(
            self.items_url(layer.raw_token),
            {'bbox': '0,0,20,20'})
        body = response.json()
        ids = [f['properties']['UUID'] for f in body['features']]
        self.assertEqual(ids, [str(inside.uuid)])

    def test_paging_returns_every_record_once(self):
        created_records = [self.create_record() for _ in range(5)]
        created_id = [(created.id, created.uuid) for created in created_records]

        layer = self.create_layer()
        seen = []
        for offset in (0, 2, 4):
            response = self.client.get(
                self.items_url(layer.raw_token),
                {'limit': 2, 'offset': offset})
            body = response.json()
            self.assertEqual(body['numberMatched'], 5)
            seen += [f['properties']['UUID'] for f in body['features']]
        self.assertEqual(seen, [str(tup[1]) for tup in sorted(created_id, key=lambda x: x[0])])

    def test_cached_ids_do_not_serve_a_record_embargoed_since(self):
        from datetime import timedelta
        from django.utils import timezone
        record = self.create_record()
        layer = self.create_layer()
        url = self.items_url(layer.raw_token)
        self.assertEqual(self.client.get(url).json()['numberReturned'], 1)
        type(record).objects.filter(pk=record.pk).update(
            end_embargo_date=timezone.now().date() + timedelta(days=10))
        self.assertEqual(self.client.get(url).json()['numberReturned'], 0)

    @override_settings(OGC_LAYER_MAX_RECORDS=1)
    def test_over_cap_is_422(self):
        self.create_record()
        self.create_record()
        layer = self.create_layer()
        response = self.client.get(self.items_url(layer.raw_token))
        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()['code'], 'TooManyMatches')

    def test_unvalidated_records_are_excluded(self):
        self.create_record(validated=False)
        visible = self.create_record()
        layer = self.create_layer()
        response = self.client.get(self.items_url(layer.raw_token))
        body = response.json()
        ids = [f['properties']['UUID'] for f in body['features']]
        self.assertEqual(ids, [str(visible.uuid)])


class TestSingleItem(OgcFeaturesApiTestCase):

    def test_get_item_by_uuid(self):
        record = self.create_record()
        layer = self.create_layer()
        response = self.client.get(
            self.item_url(layer.raw_token, record.uuid))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response['Content-Type'], 'application/geo+json')
        body = response.json()
        self.assertEqual(body['id'], str(record.uuid))
        self.assertEqual(body['properties']['UUID'], str(record.uuid))

    def test_unknown_item_is_404(self):
        import uuid as uuid_module

        layer = self.create_layer()
        response = self.client.get(
            self.item_url(layer.raw_token, uuid_module.uuid4()))
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_item_outside_filter_is_404(self):
        other_record = self.create_record(taxonomy=self.other_taxon)
        layer = self.create_layer()
        response = self.client.get(
            self.item_url(layer.raw_token, other_record.uuid))
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
