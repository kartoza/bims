# coding=utf-8
"""Settings for OGC API - Features layers, imported by project.py."""
import os

# Live layers for GIS clients
OGC_LAYER_MAX_RECORDS = int(os.environ.get('OGC_LAYER_MAX_RECORDS', 100000))
OGC_LAYER_DEFAULT_PAGE_SIZE = int(
    os.environ.get('OGC_LAYER_DEFAULT_PAGE_SIZE', 1000))
OGC_LAYER_MAX_PAGE_SIZE = int(
    os.environ.get('OGC_LAYER_MAX_PAGE_SIZE', 5000))
OGC_LAYER_COUNT_CACHE_TTL = int(
    os.environ.get('OGC_LAYER_COUNT_CACHE_TTL', 300))

# Protection of the shared database, see bims/utils/ogc_protection.py.
# Seconds a layer query may run before Postgres cancels it (0 to disable)
OGC_LAYER_STATEMENT_TIMEOUT = int(
    os.environ.get('OGC_LAYER_STATEMENT_TIMEOUT', 20))
# Layers that may be evaluated at the same time, per tenant
OGC_LAYER_MAX_CONCURRENT_EVALUATIONS = int(
    os.environ.get('OGC_LAYER_MAX_CONCURRENT_EVALUATIONS', 4))
# Seconds a request waits for another request already evaluating the layer
OGC_LAYER_LOCK_WAIT = int(os.environ.get('OGC_LAYER_LOCK_WAIT', 3))
# A lock or slot held by a crashed worker is released after this long
OGC_LAYER_LOCK_TTL = int(os.environ.get('OGC_LAYER_LOCK_TTL', 90))
# Retry-After sent with 503 responses
OGC_LAYER_RETRY_AFTER = int(os.environ.get('OGC_LAYER_RETRY_AFTER', 10))
# Number of reverse proxies in front of BIMS that append to X-Forwarded-For
OGC_TRUSTED_PROXIES = int(os.environ.get('OGC_TRUSTED_PROXIES', 1))
# Requests allowed per minute (0 disables a limit)
OGC_RATE_LIMIT_IP = int(os.environ.get('OGC_RATE_LIMIT_IP', 600))
OGC_RATE_LIMIT_LAYER = int(os.environ.get('OGC_RATE_LIMIT_LAYER', 1200))
OGC_RATE_LIMIT_OWNER = int(os.environ.get('OGC_RATE_LIMIT_OWNER', 2400))
# Requests with an unknown or revoked token, per IP
OGC_RATE_LIMIT_BAD_TOKEN = int(
    os.environ.get('OGC_RATE_LIMIT_BAD_TOKEN', 20))
# Fresh evaluations of the same layer, which bypass the cache
OGC_RATE_LIMIT_EVALUATIONS = int(
    os.environ.get('OGC_RATE_LIMIT_EVALUATIONS', 6))
# Fresh evaluations of the same layer for a bbox (panning and zooming)
OGC_RATE_LIMIT_BBOX_EVALUATIONS = int(
    os.environ.get('OGC_RATE_LIMIT_BBOX_EVALUATIONS', 60))

# Seconds a GIS client may reuse a response without asking again
OGC_HTTP_CACHE_MAX_AGE = int(os.environ.get('OGC_HTTP_CACHE_MAX_AGE', 60))

# Requests with a bbox are widened to a grid and cached for a short time
OGC_BBOX_CACHE_TTL = int(os.environ.get('OGC_BBOX_CACHE_TTL', 60))
# Smallest grid step, in degrees (0.001 is about 100 m)
OGC_BBOX_MIN_STEP = float(os.environ.get('OGC_BBOX_MIN_STEP', 0.001))
# A page of features stops growing once its JSON passes this size
OGC_RESPONSE_MAX_BYTES = int(
    os.environ.get('OGC_RESPONSE_MAX_BYTES', 10 * 1024 * 1024))

# Seconds the first attempt to read the matching ids may take. If it takes
# longer, Postgres probably chose a slow plan and the query is retried
# without plain index scans (see statement_timeout in ogc_protection.py).
OGC_LAYER_FIRST_ATTEMPT_TIMEOUT = int(
    os.environ.get('OGC_LAYER_FIRST_ATTEMPT_TIMEOUT', 3))

# Switch the whole feature off (every layer URL answers 503)
OGC_LAYERS_ENABLED = os.environ.get(
    'OGC_LAYERS_ENABLED', 'true').lower() not in ('0', 'false', 'no', 'off')
# Evaluations that time out in a row before a layer is switched off (0: never)
OGC_LAYER_MAX_FAILURES = int(os.environ.get('OGC_LAYER_MAX_FAILURES', 5))
# Active layers one owner may have (0: no limit)
OGC_MAX_LAYERS_PER_OWNER = int(os.environ.get('OGC_MAX_LAYERS_PER_OWNER', 20))
# Requests slower than this are logged, with the layer id and no token
OGC_SLOW_REQUEST_SECONDS = float(
    os.environ.get('OGC_SLOW_REQUEST_SECONDS', 3))
