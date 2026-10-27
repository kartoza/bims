# coding=utf-8
"""OGC API - Features (Part 1: Core) read-only endpoints for OgcLayer.

Serves the saved filter of an OgcLayer as a single feature collection,
named "records", under /ogc/layers/<token>/...
"""
import hashlib
import json
import logging
import math
import time
from bisect import bisect_left
from datetime import datetime, timezone as dt_timezone

from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder
from django.http import HttpResponse, JsonResponse
from django.urls import reverse
from django.utils.cache import get_conditional_response
from django.utils import timezone
from django.views import View

from bims.models.biological_collection_record import (
    BiologicalCollectionRecord
)
from bims.models.ogc_layer import OgcLayer
from bims.serializers.ogc_serializer import (
    feature_from_record,
    features_from_records,
)
from bims.utils.ogc_layer import (
    OgcLayerError,
    OgcLayerFilterError,
    OgcLayerOverCapError,
    get_layer_extent,
    get_layer_ids,
    get_layer_version,
    visible_records,
)
from bims.utils.ogc_protection import (
    OgcLayerProtectionError,
    OgcLayerThrottledError,
    check_rate,
    client_ip,
    count_request,
    statement_timeout,
    take_request_count,
)

logger = logging.getLogger(__name__)

COLLECTION_ID = 'records'

OPENAPI_MEDIA_TYPE = 'application/vnd.oai.openapi+json;version=3.0'

CONFORMANCE_CLASSES = [
    'http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core',
    'http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30',
    'http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson',
]

SELECT_RELATED = (
    'site',
    'site__location_type',
    'taxonomy',
    'taxonomy__origin',
    'taxonomy__iucn_status',
    'taxonomy__national_conservation_status',
    'taxonomy__accepted_taxonomy__iucn_status',
    'owner',
    'collector_user',
)

class OgcApiException(Exception):
    """An error to be reported in the OGC API - Features exception format."""

    def __init__(self, status, code, description):
        self.status = status
        self.code = code
        self.description = description
        super().__init__(description)


def ogc_exception_response(exc):
    return JsonResponse(
        {'code': exc.code, 'description': exc.description},
        status=exc.status,
        content_type='application/json',
    )


def get_active_layer(token, ip=None):
    layer = OgcLayer.get_by_token(token)
    if layer is None or not layer.is_available:
        check_rate(
            'bad_token', ip, settings.OGC_RATE_LIMIT_BAD_TOKEN,
            message='Too many requests with an unknown layer token.')
        raise OgcApiException(
            404, 'NotFound', 'Unknown or revoked layer token.')
    return layer


def protection_exception(exc):
    """Map a back-off error to a 429 or 503 response description."""
    if isinstance(exc, OgcLayerThrottledError):
        return OgcApiException(429, 'TooManyRequests', str(exc))
    return OgcApiException(503, 'ServiceUnavailable', str(exc))


def is_in_sorted(values, value):
    index = bisect_left(values, value)
    return index < len(values) and values[index] == value


def abs_url(request, name, **kwargs):
    """Absolute URL of an endpoint, without the trailing slash.

    Both forms are served. Clients such as QGIS append "/<name>" to the
    links they are given, so the links must not end with a slash.
    """
    return request.build_absolute_uri(
        reverse(name, kwargs=kwargs)).rstrip('/')


def parse_int_param(request, name, default, minimum=None, maximum=None):
    raw = request.GET.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise OgcApiException(
            400, 'InvalidParameter', '"%s" must be an integer.' % name)
    if minimum is not None and value < minimum:
        raise OgcApiException(
            400, 'InvalidParameter',
            '"%s" must be at least %d.' % (name, minimum))
    if maximum is not None and value > maximum:
        value = maximum
    return value


def parse_bbox_param(request):
    raw = request.GET.get('bbox')
    if not raw:
        return None
    parts = raw.split(',')
    if len(parts) != 4:
        raise OgcApiException(
            400, 'InvalidParameter',
            '"bbox" must have 4 comma-separated numbers: '
            'minx,miny,maxx,maxy.')
    try:
        minx, miny, maxx, maxy = (float(part) for part in parts)
    except ValueError:
        raise OgcApiException(
            400, 'InvalidParameter', '"bbox" values must be numbers.')
    if not all(math.isfinite(value) for value in (minx, miny, maxx, maxy)):
        raise OgcApiException(
            400, 'InvalidParameter', '"bbox" values must be finite numbers.')
    if minx > maxx or miny > maxy:
        raise OgcApiException(
            400, 'InvalidParameter', '"bbox" min must not exceed max.')
    return minx, miny, maxx, maxy


def items_url(request, token, **overrides):
    base = abs_url(
        request, 'ogc-items', token=token, collection_id=COLLECTION_ID)
    params = request.GET.copy()
    for key, value in overrides.items():
        params[key] = value
    query = params.urlencode()
    return base + ('?' + query if query else '')


def make_etag(*parts):
    digest = hashlib.sha256(
        '|'.join(str(part) for part in parts).encode('utf-8')).hexdigest()
    return 'W/"%s"' % digest[:32]


class OgcFeaturesView(View):
    """Resolves the layer token and maps evaluation errors to responses.

    Views that can be revalidated define ``cache_parts()``. It returns what
    the response depends on, so that a request with a matching
    If-None-Match is answered with 304 before any data is read.
    """

    # Whether the response changes when the layer data is rebuilt
    depends_on_data = False

    def cache_parts(self, request, **kwargs):
        """Values the response depends on, or None to skip validators."""
        return [self.__class__.__name__, sorted(kwargs.items())]

    def validators(self, request, **kwargs):
        """Return (etag, last_modified) for the request, or (None, None)."""
        parts = self.cache_parts(request, **kwargs)
        if parts is None:
            return None, None
        version = None
        if self.depends_on_data:
            version = get_layer_version(self.layer, self.bbox_for_cache)
            if version is None:
                # Data is not cached, so there is nothing to validate against
                return None, None
        etag = make_etag(
            self.layer.pk, self.layer.name, request.scheme,
            request.get_host(), parts, version)
        last_modified = (
            int(version) if version is not None else None)
        return etag, last_modified

    bbox_for_cache = None

    def apply_validators(self, response, etag, last_modified):
        max_age = settings.OGC_HTTP_CACHE_MAX_AGE
        if response.status_code not in (200, 304) or not etag:
            return
        response['ETag'] = etag
        if last_modified is not None:
            response['Last-Modified'] = datetime.fromtimestamp(
                last_modified, dt_timezone.utc).strftime(
                '%a, %d %b %Y %H:%M:%S GMT')
        response['Cache-Control'] = (
            'private, max-age=%d' % max_age if max_age else
            'private, no-cache')

    def dispatch(self, request, token, *args, **kwargs):
        if not settings.OGC_LAYERS_ENABLED:
            response = ogc_exception_response(OgcApiException(
                503, 'ServiceUnavailable',
                'Live layers are switched off on this site.'))
            response['Retry-After'] = str(settings.OGC_LAYER_RETRY_AFTER)
            response['Access-Control-Allow-Origin'] = '*'
            return response
        started = time.monotonic()
        self.layer = None
        response = self.handle(request, token, *args, **kwargs)
        self.log_if_slow(request, response, started)
        return response

    def log_if_slow(self, request, response, started):
        """Log slow requests. The URL holds the token, so it is not logged."""
        duration = time.monotonic() - started
        limit = settings.OGC_SLOW_REQUEST_SECONDS
        if not limit or duration < limit:
            return
        params = {
            name: request.GET[name]
            for name in ('limit', 'offset', 'bbox', 'f')
            if name in request.GET
        }
        logger.warning(
            'Slow OGC request: layer=%s view=%s status=%s duration=%.2fs '
            'params=%s',
            self.layer.pk if self.layer else None,
            self.__class__.__name__, response.status_code, duration, params)

    def handle(self, request, token, *args, **kwargs):
        retry_after = None
        try:
            ip = client_ip(request)
            check_rate('ip', ip, settings.OGC_RATE_LIMIT_IP)
            self.layer = get_active_layer(token, ip)
            check_rate(
                'layer', self.layer.pk, settings.OGC_RATE_LIMIT_LAYER)
            check_rate(
                'owner', self.layer.owner_id,
                settings.OGC_RATE_LIMIT_OWNER)
            count_request(self.layer.pk)
            if self.layer.touch():
                # Written now and then, not on every request
                self.layer.add_requests(
                    max(take_request_count(self.layer.pk), 1))
            etag = last_modified = None
            response = None
            if request.method in ('GET', 'HEAD'):
                etag, last_modified = self.validators(request, **kwargs)
                response = get_conditional_response(
                    request, etag=etag, last_modified=last_modified)
            if response is None:
                response = super().dispatch(request, token, *args, **kwargs)
                if etag is None and request.method in ('GET', 'HEAD'):
                    # The data was not cached before, it is now
                    etag, last_modified = self.validators(request, **kwargs)
            self.apply_validators(response, etag, last_modified)
        except OgcLayerProtectionError as exc:
            retry_after = exc.retry_after
            response = ogc_exception_response(protection_exception(exc))
        except OgcApiException as exc:
            response = ogc_exception_response(exc)
        except OgcLayerOverCapError as exc:
            response = ogc_exception_response(
                OgcApiException(422, 'TooManyMatches', str(exc)))
        except OgcLayerFilterError as exc:
            response = ogc_exception_response(
                OgcApiException(400, 'InvalidLayer', str(exc)))
        except OgcLayerError as exc:
            response = ogc_exception_response(
                OgcApiException(400, 'InvalidLayer', str(exc)))
        if retry_after is not None:
            response['Retry-After'] = str(retry_after)
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Expose-Headers'] = 'ETag, Retry-After'
        return response

    def require_collection(self, collection_id):
        if collection_id != COLLECTION_ID:
            raise OgcApiException(404, 'NotFound', 'Unknown collection.')


class LandingPageView(OgcFeaturesView):

    def get(self, request, token):
        return JsonResponse({
            'title': self.layer.name,
            'description': (
                'OGC API - Features endpoint for the BIMS layer "%s".'
                % self.layer.name),
            'links': [
                {
                    'href': abs_url(request, 'ogc-landing-page', token=token),
                    'rel': 'self',
                    'type': 'application/json',
                    'title': 'This document',
                },
                {
                    'href': abs_url(request, 'ogc-api', token=token),
                    'rel': 'service-desc',
                    'type': OPENAPI_MEDIA_TYPE,
                    'title': 'API definition',
                },
                {
                    'href': abs_url(request, 'ogc-conformance', token=token),
                    'rel': 'conformance',
                    'type': 'application/json',
                    'title': 'Conformance classes implemented by this API',
                },
                {
                    'href': abs_url(request, 'ogc-collections', token=token),
                    'rel': 'data',
                    'type': 'application/json',
                    'title': 'Feature collections',
                },
            ],
        })


def openapi_definition(request, token, layer):
    """Minimal OpenAPI 3.0 description of the layer endpoints."""
    json_content = {'application/json': {'schema': {'type': 'object'}}}
    geojson_content = {
        'application/geo+json': {'schema': {'type': 'object'}}}
    collection_id = {
        'name': 'collectionId', 'in': 'path', 'required': True,
        'schema': {'type': 'string', 'enum': [COLLECTION_ID]},
    }
    parameters = {
        'limit': {
            'name': 'limit', 'in': 'query', 'required': False,
            'description': 'Maximum number of features to return.',
            'schema': {
                'type': 'integer',
                'minimum': 1,
                'maximum': settings.OGC_LAYER_MAX_PAGE_SIZE,
                'default': settings.OGC_LAYER_DEFAULT_PAGE_SIZE,
            },
            'style': 'form', 'explode': False,
        },
        'offset': {
            'name': 'offset', 'in': 'query', 'required': False,
            'description': 'Number of features to skip.',
            'schema': {'type': 'integer', 'minimum': 0, 'default': 0},
            'style': 'form', 'explode': False,
        },
        'bbox': {
            'name': 'bbox', 'in': 'query', 'required': False,
            'description': 'Only features that intersect the bounding box '
                           '(minx,miny,maxx,maxy) in CRS84.',
            'schema': {
                'type': 'array', 'minItems': 4, 'maxItems': 4,
                'items': {'type': 'number'},
            },
            'style': 'form', 'explode': False,
        },
    }
    ok = {'200': {'description': 'Successful response'}}

    def operation(summary, operation_id, content, extra=None):
        return {
            'get': {
                'summary': summary,
                'operationId': operation_id,
                'parameters': extra or [],
                'responses': {
                    '200': dict(ok['200'], content=content),
                    'default': {'description': 'Error'},
                },
            }
        }

    return {
        'openapi': '3.0.2',
        'info': {
            'title': layer.name,
            'description': (
                'OGC API - Features endpoint for the BIMS layer "%s".'
                % layer.name),
            'version': '1.0.0',
        },
        'servers': [{
            'url': abs_url(
                request, 'ogc-landing-page', token=token).rstrip('/'),
        }],
        'paths': {
            '/': operation('Landing page', 'getLandingPage', json_content),
            '/api': operation(
                'API definition', 'getApiDefinition', json_content),
            '/conformance': operation(
                'Conformance classes', 'getConformanceClasses',
                json_content),
            '/collections': operation(
                'Feature collections', 'getCollections', json_content),
            '/collections/{collectionId}': operation(
                'Feature collection', 'describeCollection', json_content,
                [collection_id]),
            '/collections/{collectionId}/items': operation(
                'Features', 'getFeatures', geojson_content,
                [collection_id,
                 {'$ref': '#/components/parameters/limit'},
                 {'$ref': '#/components/parameters/offset'},
                 {'$ref': '#/components/parameters/bbox'}]),
            '/collections/{collectionId}/items/{featureId}': operation(
                'Feature', 'getFeature', geojson_content,
                [collection_id,
                 {'name': 'featureId', 'in': 'path', 'required': True,
                  'schema': {'type': 'string', 'format': 'uuid'}}]),
        },
        'components': {'parameters': parameters},
    }


class ApiView(OgcFeaturesView):

    def get(self, request, token):
        return JsonResponse(
            openapi_definition(request, token, self.layer),
            content_type=OPENAPI_MEDIA_TYPE)


class ConformanceView(OgcFeaturesView):

    def get(self, request, token):
        return JsonResponse({'conformsTo': CONFORMANCE_CLASSES})


def collection_metadata(request, token, layer):
    metadata = {
        'id': COLLECTION_ID,
        'title': layer.name,
        'description': 'Records matching the "%s" layer filter.' % (
            layer.name),
        'itemType': 'feature',
        'crs': ['http://www.opengis.net/def/crs/OGC/1.3/CRS84'],
        'links': [
            {
                'href': abs_url(
                    request, 'ogc-collection', token=token,
                    collection_id=COLLECTION_ID),
                'rel': 'self',
                'type': 'application/json',
            },
            {
                'href': abs_url(
                    request, 'ogc-items', token=token,
                    collection_id=COLLECTION_ID),
                'rel': 'items',
                'type': 'application/geo+json',
            },
        ],
    }
    extent = get_layer_extent(layer)
    if extent:
        # The extent is optional, leave it out when there is no geometry
        metadata['extent'] = {
            'spatial': {
                'bbox': [extent],
                'crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84',
            },
        }
    return metadata


class CollectionsView(OgcFeaturesView):
    depends_on_data = True

    def get(self, request, token):
        return JsonResponse({
            'collections': [
                collection_metadata(request, token, self.layer)],
            'links': [
                {
                    'href': abs_url(request, 'ogc-collections', token=token),
                    'rel': 'self',
                    'type': 'application/json',
                },
            ],
        })


class CollectionView(OgcFeaturesView):
    depends_on_data = True

    def get(self, request, token, collection_id):
        self.require_collection(collection_id)
        return JsonResponse(collection_metadata(request, token, self.layer))


class ItemsView(OgcFeaturesView):
    depends_on_data = True

    def cache_parts(self, request, **kwargs):
        self.bbox_for_cache = parse_bbox_param(request)
        return [
            self.__class__.__name__, sorted(kwargs.items()),
            sorted(request.GET.items()),
        ]

    def get(self, request, token, collection_id):
        self.require_collection(collection_id)

        f = request.GET.get('f', 'json').lower()
        if f not in ('json', 'geojson'):
            raise OgcApiException(
                400, 'InvalidParameter',
                'Only f=json or f=geojson are supported.')

        limit = parse_int_param(
            request, 'limit', settings.OGC_LAYER_DEFAULT_PAGE_SIZE,
            minimum=1, maximum=settings.OGC_LAYER_MAX_PAGE_SIZE)
        offset = parse_int_param(request, 'offset', 0, minimum=0)
        bbox = parse_bbox_param(request)

        # The record cap is enforced on the whole layer, even when a narrow
        # bbox is requested.
        ids = get_layer_ids(self.layer)
        if bbox is not None:
            ids = get_layer_ids(self.layer, bbox=bbox)

        number_matched = len(ids)
        with statement_timeout():
            page = list(visible_records(
                BiologicalCollectionRecord.objects.filter(
                    pk__in=ids[offset:offset + limit]),
                self.layer
            ).select_related(*SELECT_RELATED).order_by('pk'))
        features, size = [], 0
        for feature in features_from_records(page):
            chunk = json.dumps(feature, cls=DjangoJSONEncoder)
            size += len(chunk)
            # A page stops growing once it is too big, but never ends up
            # empty, so a client can always make progress
            if features and size > settings.OGC_RESPONSE_MAX_BYTES:
                break
            features.append(chunk)
        number_returned = len(features)
        step = limit
        if number_returned < len(page):
            # The page was cut short: the next one starts at the first
            # record left out, which is found by its place among the ids
            step = bisect_left(
                ids[offset:offset + limit], page[number_returned].pk)

        links = [
            {
                'href': items_url(request, token, limit=limit, offset=offset),
                'rel': 'self',
                'type': 'application/geo+json',
            },
        ]
        if offset + step < number_matched:
            links.append({
                'href': items_url(
                    request, token, limit=limit, offset=offset + step),
                'rel': 'next',
                'type': 'application/geo+json',
            })
        if offset > 0:
            links.append({
                'href': items_url(
                    request, token, limit=limit,
                    offset=max(offset - limit, 0)),
                'rel': 'prev',
                'type': 'application/geo+json',
            })
        links.append({
            'href': abs_url(
                request, 'ogc-collection', token=token,
                collection_id=COLLECTION_ID),
            'rel': 'collection',
            'type': 'application/json',
        })

        header = json.dumps({
            'type': 'FeatureCollection',
            'numberMatched': number_matched,
            'numberReturned': number_returned,
            'timeStamp': timezone.now().isoformat(),
            'links': links,
        }, cls=DjangoJSONEncoder)
        return HttpResponse(
            '%s, "features": [%s]}' % (header[:-1], ','.join(features)),
            content_type='application/geo+json',
        )


class ItemView(OgcFeaturesView):
    depends_on_data = True

    def get(self, request, token, collection_id, item_id):
        self.require_collection(collection_id)

        with statement_timeout():
            record = visible_records(
                BiologicalCollectionRecord.objects.filter(uuid=item_id),
                self.layer
            ).select_related(*SELECT_RELATED).first()
        ids = get_layer_ids(self.layer)
        if record is None or not is_in_sorted(ids, record.pk):
            raise OgcApiException(404, 'NotFound', 'Unknown feature.')

        feature = feature_from_record(record)
        feature['links'] = [
            {
                'href': abs_url(
                    request, 'ogc-item', token=token,
                    collection_id=COLLECTION_ID, item_id=item_id),
                'rel': 'self',
                'type': 'application/geo+json',
            },
            {
                'href': abs_url(
                    request, 'ogc-collection', token=token,
                    collection_id=COLLECTION_ID),
                'rel': 'collection',
                'type': 'application/json',
            },
        ]
        return JsonResponse(feature, content_type='application/geo+json')
