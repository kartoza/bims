# coding=utf-8
"""Evaluate the saved filter of an OgcLayer into a record queryset.

The filter is run through the same CollectionSearch used by the BIMS
search page, then restricted so that a layer only ever exposes records
that are safe to serve behind a shared URL.
"""
import datetime
import hashlib
import json
import logging
import math
import time
import zlib
from array import array

from django.conf import settings
from django.contrib.gis.geos import Polygon as GEOSPolygon
from django.core.cache import cache
from django.db import connection
from django.contrib.gis.db.models import Extent
from django.db.models import Q
from django.utils import timezone

from bims.models.biological_collection_record import (
    BiologicalCollectionRecord
)
from bims.utils.ogc_protection import (
    OgcLayerTimeoutError,
    check_rate,
    single_flight,
    statement_timeout,
)

# Search parameters that narrow the result set. A layer needs at least one,
# so that an unfiltered layer can never expose the whole dataset.
NARROWING_PARAMS = (
    'search',
    'taxon',
    'siteId',
    'months',
    'invasions',
    'category',
    'collector',
    'collectors',
    'reference',
    'referenceCategory',
    'conservationStatus',
    'dst',
    'boundary',
    'userBoundary',
    'endemic',
    'sourceCollection',
    'tags',
    'datasetKeys',
    'polygon',
    'modules',
    'mg',
    'ecosystemType',
    'spatialFilter',
    'asf',
)

# Search modules that do not return biological collection records
UNSUPPORTED_MODULES = ('water_temperature', 'physico_chemistry', 'climate')

# Ids checked for visibility in one query
VISIBILITY_CHUNK = 10000

SENSITIVE_GROUP = 'SensitiveDataGroup'
PRIVATE_GROUP = 'PrivateDataGroup'

GEOMETRY_FIELDS = (
    'geometry_point',
    'geometry_polygon',
    'geometry_multipolygon',
    'geometry_line',
)

# Cached marker for a layer that matches more records than the cap
OVER_CAP = 'over_cap'
# Cached marker for a layer without any geometry
NO_EXTENT = 'no_extent'

logger = logging.getLogger('bims')


class OgcLayerError(Exception):
    """Base class for layer evaluation errors."""


class OgcLayerFilterError(OgcLayerError):
    """The saved filter is empty, too broad or not supported."""


class OgcLayerOverCapError(OgcLayerError):
    """The filter matches more records than OGC_LAYER_MAX_RECORDS."""

    def __init__(self, cap):
        self.cap = cap
        super().__init__(
            'The filter matches more than %d records. '
            'Narrow the filter to publish it as a layer.' % cap
        )


def _is_empty(value):
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() in ('', '[]', '{}', 'null', 'false')
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) == 0
    return False


def has_narrowing_filter(query):
    """Whether the query has at least one filter that narrows the results."""
    if not isinstance(query, dict):
        return False
    for param in NARROWING_PARAMS:
        if not _is_empty(query.get(param)):
            return True
    # The year range only narrows the results when both ends are given
    return (
        not _is_empty(query.get('yearFrom')) and
        not _is_empty(query.get('yearTo'))
    )


def validate_query(query):
    """Check that a saved filter can be served as a layer.

    :raises OgcLayerFilterError: if the filter is not allowed.
    """
    if not isinstance(query, dict):
        raise OgcLayerFilterError('The filter must be a JSON object.')
    if not has_narrowing_filter(query):
        raise OgcLayerFilterError(
            'Apply at least one filter, such as a taxon, site, year range, '
            'reference or area, before publishing a layer.'
        )
    if (
        query.get('module') in UNSUPPORTED_MODULES or
        not _is_empty(query.get('thermalModule'))
    ):
        raise OgcLayerFilterError(
            'Only occurrence records can be published as a layer.'
        )


def _search_parameters(layer):
    parameters = {
        key: value for key, value in layer.query.items()
        if key != 'requester'
    }
    return parameters


def _allowed_data_types(layer):
    """Data types the layer may expose.

    Sensitive and private records need both the layer opting in and the
    owner belonging to the matching group.
    """
    group_names = set(layer.owner.groups.values_list('name', flat=True))
    data_types = ['public']
    if layer.include_sensitive and SENSITIVE_GROUP in group_names:
        data_types.append('sensitive')
    if layer.include_private and PRIVATE_GROUP in group_names:
        data_types.append('private')
    return data_types


def visible_records(queryset, layer):
    """Restrict records to what a layer may serve.

    Only validated records that are not under embargo (even the owner's
    own) and only the data types allowed for the layer are kept. This is
    applied again when a page is read, so a record that becomes embargoed
    or unvalidated stops being served even if its id is still cached.
    """
    today = datetime.date.today()
    return queryset.filter(
        validated=True
    ).filter(
        Q(end_embargo_date__isnull=True) |
        Q(end_embargo_date__lte=today) |
        Q(start_embargo_date__gt=today)
    ).filter(
        Q(data_type='') |
        Q(data_type__in=_allowed_data_types(layer))
    )


def get_matched_queryset(layer):
    """Return the records that match the saved filter of a layer.

    The filter is evaluated as the layer owner. The result is not yet
    restricted to what the layer may serve: visible_records() has to be
    applied, see visible_ids().
    """
    from bims.api_views.search import CollectionSearch

    validate_query(layer.query)
    if not layer.owner.is_active:
        return BiologicalCollectionRecord.objects.none()

    search = CollectionSearch(
        _search_parameters(layer),
        requester_id=layer.owner_id
    )
    if search.is_sass_records_only():
        raise OgcLayerFilterError(
            'SASS filters are not supported for layers.'
        )
    try:
        return search.process_search()
    except (ValueError, TypeError, SyntaxError) as exc:
        # Parameters in a format the search does not understand
        logger.warning(
            'OGC layer %s has a filter that cannot be evaluated',
            layer.pk, exc_info=True)
        raise OgcLayerFilterError(
            'The layer filter could not be evaluated: %s' % exc) from exc


def visible_ids(layer, ids):
    """Return the sorted ids, of those given, that the layer may serve.

    The visibility rules are checked by primary key in chunks, which reads
    only the records that were found. Applying them to the search query
    instead makes Postgres scan the whole records table.
    """
    visible = []
    for start in range(0, len(ids), VISIBILITY_CHUNK):
        chunk = ids[start:start + VISIBILITY_CHUNK]
        visible.extend(visible_records(
            BiologicalCollectionRecord.objects.filter(pk__in=chunk),
            layer
        ).order_by().values_list('pk', flat=True))
    return sorted(visible)


def bbox_filter(bbox):
    """Q object for records whose site overlaps (minx, miny, maxx, maxy)."""
    bbox_polygon = GEOSPolygon.from_bbox(bbox)
    query = Q()
    for field in GEOMETRY_FIELDS:
        query |= Q(**{'site__%s__bboverlaps' % field: bbox_polygon})
    return query


def snap_bbox(bbox):
    """Widen a bbox to a grid, so nearby requests share one cache entry.

    The step is about an eighth of the larger side, rounded down to
    1, 2 or 5 times a power of ten, and never smaller than
    OGC_BBOX_MIN_STEP degrees. The result always contains the original
    bbox and stays within the world (lon/lat).

    :return: (minx, miny, maxx, maxy), or None if no bbox was given.
    """
    if not bbox:
        return None
    minx, miny, maxx, maxy = bbox
    size = max(maxx - minx, maxy - miny)
    step = settings.OGC_BBOX_MIN_STEP
    target = size / 8
    if target > step:
        power = 10 ** math.floor(math.log10(target))
        step = max(
            (m * power for m in (1, 2, 5) if m * power <= target),
            default=power)
    # Rounding keeps float noise out of the cache key
    snapped = (
        round(max(math.floor(minx / step) * step, -180.0), 6),
        round(max(math.floor(miny / step) * step, -90.0), 6),
        round(min(math.ceil(maxx / step) * step, 180.0), 6),
        round(min(math.ceil(maxy / step) * step, 90.0), 6),
    )
    return snapped


def _cache_ttl(bbox):
    if bbox:
        return settings.OGC_BBOX_CACHE_TTL
    return settings.OGC_LAYER_COUNT_CACHE_TTL


def _cache_key(layer, bbox=None):
    digest = hashlib.sha256(json.dumps({
        'query': layer.query,
        'owner': layer.owner_id,
        'sensitive': layer.include_sensitive,
        'private': layer.include_private,
        'bbox': list(bbox) if bbox else None,
    }, sort_keys=True, default=str).encode('utf-8')).hexdigest()
    return 'ogc_layer_ids:%s:%s:%s' % (
        connection.schema_name, layer.pk, digest)


def _encode_ids(ids):
    """Pack sorted ids compactly, to stay under the cache item size limit."""
    return zlib.compress(array('Q', ids).tobytes())


def _decode_ids(value):
    ids = array('Q')
    ids.frombytes(zlib.decompress(value))
    return ids.tolist()


def _read_cached_ids(cache_key):
    """Cached ids, or None. Raises OgcLayerOverCapError for a cached overflow."""
    try:
        cached = cache.get(cache_key)
    except Exception:  # The cache is an optimisation, not required
        logger.warning('Could not read OGC layer ids', exc_info=True)
        return None
    if cached == OVER_CAP:
        raise OgcLayerOverCapError(settings.OGC_LAYER_MAX_RECORDS)
    if cached is None:
        return None
    return _decode_ids(cached)


def _version_key(cache_key):
    return cache_key.replace('ogc_layer_ids:', 'ogc_layer_version:', 1)


def get_layer_version(layer, bbox=None):
    """Return when the cached ids of a layer were built, as a timestamp.

    :return: the timestamp, or None if the ids are not cached.
    """
    try:
        return cache.get(_version_key(_cache_key(layer, snap_bbox(bbox))))
    except Exception:
        logger.warning('Could not read OGC layer version', exc_info=True)
        return None


def _matching_ids(matched, cap):
    """Return up to cap + 1 distinct ids of a queryset of records.

    DISTINCT is left to Python: with ``DISTINCT ... LIMIT`` Postgres reads
    the records in id order, which scans the whole table when few match.
    """
    ids = matched.order_by().values_list('pk', flat=True)

    def read(avoid_index_scan, distinct=False):
        # Short first attempt: when Postgres picks the slow plan, a
        # second attempt without plain index scans is much faster than
        # waiting for the first
        timeout = (
            None if avoid_index_scan
            else settings.OGC_LAYER_FIRST_ATTEMPT_TIMEOUT)
        with statement_timeout(timeout, avoid_index_scan=avoid_index_scan):
            return list((ids.distinct() if distinct else ids)[:cap + 1])

    try:
        avoid = False
        rows = read(avoid)
    except OgcLayerTimeoutError:
        avoid = True
        rows = read(avoid)
    found = set(rows)
    if len(rows) > cap and len(found) <= cap:
        # Joins repeated some ids, so the limit may have cut the result
        # short. Let the database remove the repeats to know for sure.
        found = set(read(avoid, distinct=True))
    return found if len(found) > cap else sorted(found)


def _evaluate_ids(layer, bbox, cache_key, use_cache):
    """Run _find_ids() and keep the cost figures of the layer up to date."""
    started = time.monotonic()
    try:
        ids = _find_ids(layer, bbox, cache_key, use_cache)
    except OgcLayerOverCapError:
        # The search worked, the layer just matches too much
        _record_evaluation(layer, bbox, started)
        raise
    except OgcLayerTimeoutError:
        layer.record_failure()
        raise
    _record_evaluation(layer, bbox, started)
    return ids


def _record_evaluation(layer, bbox, started):
    if bbox:
        # Only whole-layer evaluations say how costly the layer is
        return
    layer.record_evaluation(int((time.monotonic() - started) * 1000))


def _find_ids(layer, bbox, cache_key, use_cache):
    cap = settings.OGC_LAYER_MAX_RECORDS
    if use_cache:
        # GIS clients ask for a new bbox on every pan or zoom, and each one
        # is a cache miss, so those get a higher limit than whole layers.
        # They are cheaper too, the search is cut down by the bbox.
        if bbox:
            check_rate(
                'evaluate-bbox', layer.pk,
                settings.OGC_RATE_LIMIT_BBOX_EVALUATIONS,
                message='This layer is being re-evaluated too often.')
        else:
            check_rate(
                'evaluate', layer.pk, settings.OGC_RATE_LIMIT_EVALUATIONS,
                message='This layer is being re-evaluated too often.')
    with statement_timeout():
        matched = get_matched_queryset(layer)
        if bbox:
            matched = matched.filter(bbox_filter(bbox))
    # Reading one id past the cap is enough to know it is exceeded. The cap
    # counts matches before the visibility rules, so a filter that matches
    # more than the cap is refused even if some of them are hidden.
    found = _matching_ids(matched, cap)
    # The visibility rules are checked by primary key, the index is fine
    with statement_timeout():
        ids = found if len(found) > cap else visible_ids(layer, found)
    over_cap = len(ids) > cap
    if use_cache:
        try:
            cache.set(
                cache_key,
                OVER_CAP if over_cap else _encode_ids(ids),
                _cache_ttl(bbox))
            if not over_cap:
                cache.set(
                    _version_key(cache_key), int(time.time()),
                    _cache_ttl(bbox))
        except Exception:  # The cache is an optimisation, not required
            logger.warning('Could not cache OGC layer ids', exc_info=True)
    if over_cap:
        raise OgcLayerOverCapError(cap)
    if not bbox and layer.pk:
        now = timezone.now()
        type(layer).objects.filter(pk=layer.pk).update(
            record_count_cached=len(ids),
            record_count_updated=now
        )
        layer.record_count_cached = len(ids)
        layer.record_count_updated = now
    return ids


def get_layer_ids(layer, bbox=None, use_cache=True):
    """Return the sorted ids of the records a layer serves.

    Only one request at a time evaluates a layer; the query runs under
    OGC_LAYER_STATEMENT_TIMEOUT (see bims/utils/ogc_protection.py).

    :param bbox: optional (minx, miny, maxx, maxy) to restrict the ids to.
        It is widened with snap_bbox(), so records slightly outside it can
        be included, and cached for OGC_BBOX_CACHE_TTL seconds.
    :raises OgcLayerOverCapError: if the layer, or the bbox part of it,
        has more records than OGC_LAYER_MAX_RECORDS.
    :raises OgcLayerProtectionError: if the layer cannot be evaluated now.
    """
    bbox = snap_bbox(bbox)
    cache_key = _cache_key(layer, bbox)
    if not use_cache:
        return _evaluate_ids(layer, bbox, cache_key, use_cache=False)

    def read():
        return _read_cached_ids(cache_key)

    return single_flight(
        cache_key, read,
        lambda: _evaluate_ids(layer, bbox, cache_key, use_cache=True))


def _compute_extent(ids):
    from bims.models.location_site import LocationSite

    boxes = []
    with statement_timeout():
        for start in range(0, len(ids), VISIBILITY_CHUNK):
            sites = LocationSite.objects.filter(
                id__in=BiologicalCollectionRecord.objects.filter(
                    pk__in=ids[start:start + VISIBILITY_CHUNK]
                ).order_by().values('site_id'))
            extents = sites.aggregate(**{
                field: Extent(field) for field in GEOMETRY_FIELDS
            })
            boxes.extend(box for box in extents.values() if box)
    if not boxes:
        return None
    return [
        min(box[0] for box in boxes),
        min(box[1] for box in boxes),
        max(box[2] for box in boxes),
        max(box[3] for box in boxes),
    ]


def get_layer_extent(layer, use_cache=True):
    """Return the bounding box of a layer as [minx, miny, maxx, maxy].

    The extent covers the sites of the layer records, in lon/lat (EPSG:4326).
    It is cached like the ids.

    :raises OgcLayerOverCapError: if the layer is over the cap.
    :raises OgcLayerProtectionError: if the layer cannot be evaluated now.
    :return: the bounding box, or None if no record has a geometry.
    """
    # Enforces the cap, and avoids the search for a layer that is too big
    ids = get_layer_ids(layer, use_cache=use_cache)

    cache_key = _cache_key(layer).replace(
        'ogc_layer_ids:', 'ogc_layer_extent:', 1)

    def read():
        try:
            return cache.get(cache_key) if use_cache else None
        except Exception:
            logger.warning('Could not read OGC layer extent', exc_info=True)
            return None

    def build():
        try:
            extent = _compute_extent(ids)
        except OgcLayerTimeoutError:
            layer.record_failure()
            raise
        if use_cache:
            try:
                cache.set(
                    cache_key,
                    NO_EXTENT if extent is None else extent,
                    settings.OGC_LAYER_COUNT_CACHE_TTL)
            except Exception:
                logger.warning(
                    'Could not cache OGC layer extent', exc_info=True)
        return NO_EXTENT if extent is None else extent

    if use_cache:
        cached = single_flight(cache_key, read, build)
    else:
        cached = build()
    return None if cached == NO_EXTENT else cached


def count_layer_records(layer, use_cache=True):
    """Count the records of a layer, never counting beyond the cap.

    :raises OgcLayerOverCapError: if the layer has more records than
        OGC_LAYER_MAX_RECORDS.
    :return: number of records, at most OGC_LAYER_MAX_RECORDS.
    """
    return len(get_layer_ids(layer, use_cache=use_cache))


def validate_layer(layer):
    """Check a layer can be published and return its record count.

    :raises OgcLayerError: if the filter is not allowed or matches more
        than OGC_LAYER_MAX_RECORDS records.
    """
    validate_query(layer.query)
    return count_layer_records(layer, use_cache=False)
