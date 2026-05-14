# coding=utf-8
"""
core.settings.contrib
"""
from .base import *  # noqa
# Override base settings from geonode
from .legacy_geonode_settings import *
import os
try:
    from .secret import IUCN_API_KEY  # noqa
except ImportError:
    IUCN_API_KEY = ''


STOP_WORDS = (
    'a', 'an', 'and', 'if', 'is', 'the', 'in', 'i', 'you', 'other',
    'this', 'that', 'to',
)

STATICFILES_STORAGE = 'bims.storage.NoSourceMapsStorage'
STATICFILES_FINDERS += (
    'pipeline.finders.PipelineFinder',
)

# Django-allauth related settings
AUTHENTICATION_BACKENDS = (
    # Needed to login by username in Django admin, regardless of `allauth`
    'django.contrib.auth.backends.ModelBackend',
    'guardian.backends.ObjectPermissionBackend',
    # `allauth` specific authentication methods, such as login by e-mail
    'allauth.account.auth_backends.AuthenticationBackend',
)

# Grapelli settings
GRAPPELLI_ADMIN_TITLE = 'Bims Admin Page'

SHARED_APPS = (
    'grappelli',
    'django_tenants',
    'tenants',

    'colorfield',
    'polymorphic',
    'webpack_loader',
    'ckeditor_uploader',
    'django_admin_inline_paginator',

    # Apps bundled with Django
    'modeltranslation',
    'dal',
    'dal_select2',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.sitemaps',
    'django.contrib.staticfiles',
    'django.contrib.messages',
    'django.contrib.humanize',
    'django.contrib.gis',
    'django.contrib.flatpages',

    # Utility
    'drf_yasg',
    'dj_pagination',
    'taggit',
    'mptt',

    'corsheaders',

    'core.config_hook',
    'rolepermissions',
    'rest_framework',
    'rest_framework_gis',
    'celery',
    'pipeline',
    'modelsdoc',
    'contactus',

    'crispy_forms',
    'rangefilter',
    'preferences',
    'sorl.thumbnail',
    'ckeditor',
    'django_json_widget',
    'django_forms_bootstrap',

    'allauth',
    'allauth.account',
    'django.contrib.auth',
    'django.contrib.admin',
    # tenant-specific apps
    'geonode.people',
    'geonode.base',
    'geonode.groups',
    'geonode.documents',

    'invitations',
    'guardian',
    'rest_framework.authtoken',
    'bims',
    'bims.signals',
    'sass',
    'td_biblio',
    'scripts',
    'bims_theme',
    'mobile',
    'pesticide',
    'cloud_native_gis',
    'climate',
    'django_celery_beat',
)

TENANT_APPS = (
    'django_celery_results',
    'rest_framework',
    'rest_framework_gis',
    'taggit',
    'allauth',
    'allauth.account',
    'django.contrib.auth',
    'django.contrib.admin',
    'django.contrib.flatpages',
    # tenant-specific apps
    'geonode.people',
    'geonode.base',
    'geonode.groups',
    'geonode.documents',

    'invitations',
    'guardian',
    'rest_framework.authtoken',
    'bims',
    'bims.signals',
    'sass',
    'td_biblio',
    'scripts',
    'bims_theme',
    'mobile',
    'pesticide',
    'cloud_native_gis',
    'climate',
)

MIDDLEWARE = (
    'django_tenants.middleware.main.TenantMainMiddleware',
    'allauth.account.middleware.AccountMiddleware',
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.contrib.sites.middleware.CurrentSiteMiddleware',
    'dj_pagination.middleware.PaginationMiddleware',
    'django.middleware.locale.LocaleMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'django.middleware.security.SecurityMiddleware',
)


TESTING = sys.argv[1:2] == ['test']
if not TESTING and not on_travis:
    SHARED_APPS += (
        'easyaudit',
    )
    TENANT_APPS += (
        'easyaudit',
    )
    MIDDLEWARE += (
        'easyaudit.middleware.easyaudit.EasyAuditMiddleware',
    )

INSTALLED_APPS = list(SHARED_APPS) + [app for app in TENANT_APPS if app not in SHARED_APPS]

TENANT_MODEL = "tenants.Client"
TENANT_DOMAIN_MODEL = "tenants.Domain"


if os.environ.get('SENTRY_KEY'):
    import sentry_sdk
    from sentry_sdk.integrations.django import DjangoIntegration
    from bims.utils.sentry import before_send, before_breadcrumb
    sentry_sdk.init(
        dsn=os.environ.get('SENTRY_KEY'),
        integrations=[DjangoIntegration()],
        traces_sample_rate=0.2,
        before_send=before_send,
        before_breadcrumb=before_breadcrumb,
        environment='production'
    )

# workaround to get flatpages picked up in installed apps.

# Set templates
try:
    TEMPLATES[0]['DIRS'] = [
        absolute_path('core', 'base_templates'),
        absolute_path('bims', 'templates'),
        absolute_path('sass', 'templates'),
        absolute_path('td_biblio', 'templates'),
        absolute_path('pesticide', 'templates'),
    ] + TEMPLATES[0]['DIRS']

    TEMPLATES[0]['OPTIONS']['context_processors'] += [
        'bims.context_processor.add_recaptcha_key',
        'bims.context_processor.custom_navbar_url',
        'bims.context_processor.google_analytic_key',
        'bims.context_processor.bing_api_key',
        'bims.context_processor.bims_preferences',
        'bims.context_processor.application_name',
        'bims.context_processor.site_ready',
        'bims.context_processor.download_request_message',
        'bims.context_processor.download_request_purpose',
        'bims_theme.context_processor.bims_custom_theme',
        'preferences.context_processors.preferences_cp',
    ]
except KeyError:
    TEMPLATES = [
        {
            'BACKEND': 'django.template.backends.django.DjangoTemplates',
            'DIRS': [
                # project level templates
                absolute_path('core', 'base_templates'),
                absolute_path('bims', 'templates'),
                absolute_path('sass', 'templates'),
                absolute_path('td_biblio', 'templates'),
            ],
            'APP_DIRS': True,
            'OPTIONS': {
                'context_processors': [
                    'django.template.context_processors.debug',
                    'django.template.context_processors.request',
                    'django.contrib.auth.context_processors.auth',
                    'django.contrib.messages.context_processors.messages',

                    # `allauth` needs this from django
                    'django.template.context_processors.request',
                    'bims.context_processor.add_recaptcha_key',
                    'bims.context_processor.custom_navbar_url',
                    'bims.context_processor.google_analytic_key',
                    'bims.context_processor.application_name',
                    'bims.context_processor.download_request_message',
                    'bims_theme.context_processor.bims_custom_theme',
                ],
            },
        },
    ]

# Absolute path to the directory static files should be collected to.
# Don't put anything in this directory yourself; store your static files
# in apps' "static/" subdirectories and in STATICFILES_DIRS.
# Example: "/var/www/example.com/static/"
STATIC_ROOT = '/home/web/static'

# Additional locations of static files
STATICFILES_DIRS = [
    # Put strings here, like "/home/html/static" or "C:/www/django/static".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    absolute_path('core', 'base_static'),
    absolute_path('bims', 'static'),
    absolute_path('sass', 'static'),
    absolute_path('scripts', 'static'),
    absolute_path('td_biblio', 'static'),
    absolute_path('pesticide', 'static'),
]

# for middleware in MIDDLEWARE_CLASSES:
#     if middleware not in MIDDLEWARE:
#         MIDDLEWARE += (middleware,)

# Defines whether to log model related events,
# such as when an object is created, updated, or deleted
DJANGO_EASY_AUDIT_WATCH_MODEL_EVENTS = True

# Defines whether to log user authentication events,
# such as logins, logouts and failed logins.
DJANGO_EASY_AUDIT_WATCH_AUTH_EVENTS = True

# Defines whether to log URL requests made to the project
DJANGO_EASY_AUDIT_WATCH_REQUEST_EVENTS = False

DJANGO_EASY_AUDIT_CRUD_DIFFERENCE_CALLBACKS = [
    'bims.utils.easy_audit_callback',
]

LOGIN_REDIRECT_URL = "/"


IUCN_API_URL = 'http://apiv3.iucnredlist.org/api/v3'

# django modelsdoc settings
MODELSDOC_APPS = ('bims', 'td_biblio',)

MODELSDOC_OUTPUT_FORMAT = 'rst'
MODELSDOC_MODEL_WRAPPER = 'modelsdoc.wrappers.ModelWrapper'
MODELSDOC_FIELD_WRAPPER = 'modelsdoc.wrappers.FieldWrapper'
MODELSDOC_INCLUDE_AUTO_CREATED = True

# contact us email
SERVER_EMAIL = os.environ.get('ADMIN_EMAILS', 'admin@kartoza.com')
CONTACT_US_EMAIL = os.environ.get('CONTACT_US_EMAIL', '')
DEFAULT_FROM_EMAIL = os.environ.get('DEFAULT_FROM_EMAIL', CONTACT_US_EMAIL)

# site tracking stats settings
TRACK_PAGEVIEWS = True
TRACK_AJAX_REQUESTS = False
TRACK_REFERER = True
TRACK_IGNORE_STATUS_CODES = [301, 303, 403, 404, 405, 410]

DJANGO_EASY_AUDIT_UNREGISTERED_CLASSES_EXTRA = [
    # 'layers.Layer',
    'people.Profile',
    'bims.Pageview',
    'bims.Visitor',
    'bims.LocationContext',
    'bims.LocationContextGroup',
    'bims.SearchProcess',
    'flatpages.FlatPage',
    'td_biblio.author',
    'django_celery_results.TaskResult',
    'bims.DownloadRequest',
    'bims.Survey',
    'bims.TaxonomyUpdateProposal',
    'bims.ImportTask',
    'bims.IngestedData',
    'bims.CustomTaggedUpdateTaxonomy',
    'bims.TaxonGroupTaxonomy',
    'bims.HarvestSession',
    'django_celery_beat.PeriodicTask',
]

DJANGO_EASY_AUDIT_CRUD_EVENT_NO_CHANGED_FIELDS_SKIP = True
DJANGO_EASY_AUDIT_CHECK_IF_REQUEST_USER_EXISTS = False


LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d '
                      '%(thread)d %(message)s'
        },
        'simple': {
            'format': '%(message)s',
        },
    },
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'
        }
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        },
        'celery': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'celery.log',
            'formatter': 'simple',
            'maxBytes': 1024 * 1024 * 10,  # 10 mb
        },
        'mail_admins': {
            'level': 'ERROR',
            'filters': ['require_debug_false'],
            'class': 'django.utils.log.AdminEmailHandler',
        }
    },
    "loggers": {
        "django": {
            "handlers": ["console"], "level": "ERROR", },
        "bims": {
            "handlers": ["console"], "level": "DEBUG", },
        "geonode": {
            "handlers": ["console"], "level": "INFO", },
        "geonode.qgis_server": {
            "handlers": ["console"],
            "level": "ERROR",
            "propagate": False,
        },
        "gsconfig.catalog": {
            "handlers": ["console"], "level": "ERROR", },
        "owslib": {
            "handlers": ["console"], "level": "ERROR", },
        "pycsw": {
            "handlers": ["console"], "level": "ERROR", },
        "celery": {
            'handlers': ['celery', 'console'], 'level': 'DEBUG', },
    },
}

ASYNC_SIGNALS_GEONODE = ast.literal_eval(os.environ.get(
        'ASYNC_SIGNALS_GEONODE', 'False'))

# Set institutionID default value
INSTITUTION_ID_DEFAULT = os.environ.get('INSTITUTION_ID_DEFAULT', 'bims')


CELERY_TASK_PROTOCOL = 1

BIMS_API_SCHEMA_INCLUDE = [
    '/api/taxa-list/',
    '/api/module-list/',
    '/api/taxon/',
    '/api/coldp/',
    '/api/checklist-version/',
]

REST_FRAMEWORK = {
    'DEFAULT_PAGINATION_CLASS':
        'rest_framework.pagination.PageNumberPagination',
    'PAGE_SIZE': 100,
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'rest_framework.authentication.BasicAuthentication',
        'rest_framework.authentication.SessionAuthentication',
        'rest_framework.authentication.TokenAuthentication',
    ]
}

SELENIUM_DRIVER = os.environ.get(
    'SELENIUM_DRIVER',
    'http://hub:4444/wd/hub')

# Enable or disable SASS
try:
    SASS_ENABLED = ast.literal_eval(os.environ.get('SASS_ENABLED', 'False'))
except ValueError:
    SASS_ENABLED = False

# Bims site preferences
BIMS_PREFERENCES = {
    'enable_module_filter': ast.literal_eval(
        os.environ.get('ENABLE_MODULE_FILTER', 'True')
    ),
    'enable_catchment_filter': ast.literal_eval(
        os.environ.get('ENABLE_CATCHMENT_FILTER', 'False')
    ),
    'enable_ecoregion_filter': ast.literal_eval(
        os.environ.get('ENABLE_ECOREGION_FILTER', 'False')
    ),
    'enable_user_boundary_filter': ast.literal_eval(
        os.environ.get('ENABLE_USER_BOUNDARY_FILTER', 'False')
    ),
    'enable_download_data_from_map': ast.literal_eval(
        os.environ.get('ENABLE_DOWNLOAD_DATA_FROM_MAP', 'False')
    ),
    'geoserver_location_site_layer': os.environ.get(
        'GEOSERVER_LOCATION_SITE_LAYER',
        ''
    ),
    'empty_location_site_cluster': os.environ.get(
        'EMPTY_LOCATION_SITE_CLUSTER',
        'empty_location_site_cluster'
    ),
    'recaptcha_key': os.environ.get(
        'RECAPTCHA_KEY',
        ''
    ),
    'enable_upload_data': ast.literal_eval(
        os.environ.get('ENABLE_UPLOAD_DATA', 'True')
    )
}

# Remove geonode session middleware
if (
    'geonode.security.middleware.SessionControlMiddleware' in
        MIDDLEWARE
):
    MIDDLEWARE_CLASSES_LIST = list(MIDDLEWARE)
    MIDDLEWARE_CLASSES_LIST.remove(
        'geonode.security.middleware.SessionControlMiddleware'
    )
    MIDDLEWARE = tuple(MIDDLEWARE_CLASSES_LIST)


# CKEDITOR configurations
CKEDITOR_CONFIGS = {
    'default': {
        'toolbar': 'full',
        'height': 300,
        'width': '100%',
        'contentsCss': [
            '/static/js/libs/bootstrap-4.0.0/css/bootstrap.min.css',
            '/static/css/base.css'
        ],
        'removePlugins': 'stylesheetparser',
        'allowedContent': True,
        'extraAllowedContent': 'span(*) btn(*)',
        'font_names': '',
    },
    "landing": {
        "toolbar": "Custom",
        "extraPlugins": ",".join(["font", "colorbutton", "justify"]),
        "allowedContent": True,
        "contentsCss": [],
        "font_names": "",
    }
}

# SORL THUMBNAIL SETTINGS
THUMBNAIL_COLORSPACE = None
THUMBNAIL_PRESERVE_FORMAT = True

# add bims url
ROOT_URLCONF = 'core.urls'


CELERY_TASK_QUEUES += (
    Queue('search',
          GEONODE_EXCHANGE, routing_key='search', priority=0),
    Queue('geocontext',
          GEONODE_EXCHANGE, routing_key='geocontext', priority=0),
)
CELERY_TASK_TRACK_STARTED = True
CELERY_TRACK_STARTED = True
TASK_TRACK_STARTED = True
CELERY_IGNORE_RESULT = False

from core.settings.celery_settings import *  # noqa

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.memcached.PyMemcacheCache',
        'LOCATION': 'cache:11211',
        'KEY_FUNCTION': 'django_tenants.cache.make_key',
        'REVERSE_KEY_FUNCTION': 'django_tenants.cache.reverse_key',
    }
}

FILE_UPLOAD_PERMISSIONS = 0o644
LOGIN_URL = '/accounts/login/'
LOGOUT_URL = '/accounts/logout/'

# CKEDITOR CONFIGURATIONS
CKEDITOR_UPLOAD_PATH = 'ckeditor/'

ACCOUNT_APPROVAL_REQUIRED = True
ACCOUNT_ADAPTER = 'bims.adapters.account_adapter.AccountAdapter'
ACCOUNT_EMAIL_VERIFICATION = 'none'
ACCOUNT_FORMS = {
    'signup': 'bims.forms.CustomSignupForm',
}
ACCOUNT_UNIQUE_EMAIL = True
# For django-allauth 65.x: Updated to new settings format
# No username field in signup form - it's auto-generated from first_name and last_name
ACCOUNT_SIGNUP_FIELDS = ['email*', 'password1*', 'password2*']
# Replaced ACCOUNT_AUTHENTICATION_METHOD = 'username_email' with:
ACCOUNT_LOGIN_METHODS = {'email', 'username'}
ACCOUNT_AUTHENTICATED_LOGIN_REDIRECTS = False
ACCOUNT_LOGOUT_REDIRECT_URL = '/'

CELERY_RESULT_BACKEND = 'django-db'

customColorPalette = [
        {
            'color': 'hsl(4, 90%, 58%)',
            'label': 'Red'
        },
        {
            'color': 'hsl(340, 82%, 52%)',
            'label': 'Pink'
        },
        {
            'color': 'hsl(291, 64%, 42%)',
            'label': 'Purple'
        },
        {
            'color': 'hsl(262, 52%, 47%)',
            'label': 'Deep Purple'
        },
        {
            'color': 'hsl(231, 48%, 48%)',
            'label': 'Indigo'
        },
        {
            'color': 'hsl(207, 90%, 54%)',
            'label': 'Blue'
        },
    ]
