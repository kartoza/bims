# -*- coding: utf-8 -*-
"""Settings for running Django on the host while db, rabbitmq, cache,
geoserver and the celery worker run in docker-compose.dev.yml.

Use with: DJANGO_SETTINGS_MODULE=core.settings.dev_host
"""
from .dev_docker import *  # noqa

DEPLOYMENT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))),
    'deployment'
)

DATABASES['default'].update({
    'HOST': os.getenv('DATABASE_HOST', 'localhost'),
    'PORT': int(os.getenv('DATABASE_PORT', 6543)),
})

CACHES['default']['LOCATION'] = os.getenv(
    'CACHE_LOCATION', 'localhost:11211')

CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', 'amqp://localhost:5672')

# Same directories the worker container mounts as /home/web/media and
# /home/web/static, so both sides see the same files.
MEDIA_ROOT = os.getenv(
    'MEDIA_ROOT', os.path.join(DEPLOYMENT_DIR, 'media'))
TEMP_FOLDER = MEDIA_ROOT + '/temp'
STATIC_ROOT = os.getenv(
    'STATIC_ROOT', os.path.join(DEPLOYMENT_DIR, 'static'))

GEOSERVER_LOCATION = os.getenv(
    'GEOSERVER_LOCATION', 'http://localhost:63305/geoserver/')
GEOSERVER_PUBLIC_LOCATION = os.getenv(
    'GEOSERVER_PUBLIC_LOCATION', 'http://localhost:63305/geoserver/')
