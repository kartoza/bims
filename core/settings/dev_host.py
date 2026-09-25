# -*- coding: utf-8 -*-
"""Settings for running Django on the host while db, rabbitmq, cache,
geoserver and the celery worker run in docker-compose.dev.yml.

Use with: DJANGO_SETTINGS_MODULE=core.settings.dev_host
"""
from django.core.exceptions import ImproperlyConfigured

from .dev_docker import *  # noqa

DATABASES['default'].update({
    'HOST': 'localhost',
    'PORT': 6543,
})

CACHES['default']['LOCATION'] = 'localhost:11211'

CELERY_BROKER_URL = 'amqp://localhost:5672'

for _path in (MEDIA_ROOT, STATIC_ROOT):
    if not os.path.isdir(_path):
        raise ImproperlyConfigured(
            f'{_path} is missing. Link it to the deployment directory the '
            f'containers mount, e.g.:\n'
            f'  sudo mkdir -p /home/web\n'
            f'  sudo ln -s <repo>/deployment/{os.path.basename(_path)} {_path}'
        )

GEOSERVER_LOCATION = 'http://localhost:63305/geoserver/'
GEOSERVER_PUBLIC_LOCATION = 'http://localhost:63305/geoserver/'

# celery.log in the repo root is created by the (root) containers.
LOGGING['handlers']['celery']['filename'] = 'celery-host.log'
