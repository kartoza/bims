# coding=utf-8
"""
BIMS API Schema configuration using drf_yasg.

All endpoints are excluded by default.
To expose an endpoint, add its URL pattern name or path prefix
to the BIMS_API_SCHEMA_INCLUDE list in settings, or use the
@swagger_auto_schema decorator with the 'include' tag on the view.

Usage example — include specific paths in settings.py:
    BIMS_API_SCHEMA_INCLUDE = [
        '/api/taxon/',
        '/api/location-site/',
    ]
"""

from drf_yasg import openapi
from drf_yasg.generators import OpenAPISchemaGenerator
from drf_yasg.views import get_schema_view
from rest_framework import permissions


class BIMSSchemaGenerator(OpenAPISchemaGenerator):
    """
    Custom schema generator that only exposes paths listed in
    settings.BIMS_API_SCHEMA_INCLUDE.  When that list is empty (the
    default), no endpoints are shown.
    """

    def get_schema(self, request=None, public=False):
        schema = super().get_schema(request=request, public=public)
        return schema

    def get_endpoints(self, request):
        from django.conf import settings
        include_prefixes = getattr(settings, 'BIMS_API_SCHEMA_INCLUDE', [])

        endpoints = super().get_endpoints(request)

        if not include_prefixes:
            # Nothing whitelisted — return an empty endpoint map.
            return {}

        filtered = {}
        for path, (view_cls, methods) in endpoints.items():
            if any(path.startswith(prefix) for prefix in include_prefixes):
                filtered[path] = (view_cls, methods)

        return filtered


schema_view = get_schema_view(
    openapi.Info(
        title='BIMS API',
        default_version='v1',
        description=(
            'Biodiversity Information Management System REST API.\n'
        ),
        contact=openapi.Contact(email='dimas@kartoza.com'),
        license=openapi.License(name='BSD License'),
    ),
    public=True,
    permission_classes=[permissions.AllowAny],
    generator_class=BIMSSchemaGenerator,
)
