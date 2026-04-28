from datetime import date

from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.response import Response
from rest_framework.views import APIView

from bims.models.data_source import DataSource
from bims.models.taxonomy_checklist import TaxonomyChecklist
from bims.utils.domain import get_current_domain


def _user_to_dict(user, base_url: str = '') -> dict:
    """Serialize a User instance to a ColDP person dict."""
    full_name = user.get_full_name() or user.username
    return {
        'name': full_name,
        'email': user.email or '',
        'url': base_url,
    }


def _get_checklist(version: str = None) -> TaxonomyChecklist | None:
    """
    Return the requested TaxonomyChecklist.

    If *version* is given, look up that exact version string (published or not).
    Otherwise return the latest published checklist (ordered by -released_at, -id).
    """
    qs = TaxonomyChecklist.objects.select_related('contact').prefetch_related('creators')
    if version:
        return qs.filter(version=version).first()
    return qs.filter(is_published=True).first()


class ColDPMetadataView(APIView):
    """
    Returns dataset-level metadata in ChecklistBank / ColDP metadata.yaml format.

    By default serves the latest published TaxonomyChecklist.
    Pass ``?version=<version>`` to retrieve a specific checklist version.
    """

    @swagger_auto_schema(
        operation_id='coldp_metadata',
        operation_summary='ColDP dataset metadata',
        operation_description=(
            'Returns dataset-level metadata compatible with the Catalogue of Life '
            'Data Package (ColDP) ``metadata.yaml`` format, drawn from the '
            'TaxonomyChecklist records.\n\n'
            'By default returns the latest **published** checklist. '
            'Pass ``?version=<version>`` to request a specific version.'
        ),
        manual_parameters=[
            openapi.Parameter(
                'version',
                openapi.IN_QUERY,
                description='Checklist version string (e.g. "1.0", "2025-04"). '
                            'Returns the latest published checklist when omitted.',
                type=openapi.TYPE_STRING,
                required=False,
            ),
        ],
        responses={
            200: openapi.Response(
                description='ColDP metadata object',
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'title': openapi.Schema(type=openapi.TYPE_STRING),
                        'description': openapi.Schema(type=openapi.TYPE_STRING),
                        'version': openapi.Schema(type=openapi.TYPE_STRING),
                        'issued': openapi.Schema(
                            type=openapi.TYPE_STRING,
                            format='date',
                            description='Release date from the checklist record (YYYY-MM-DD).',
                        ),
                        'license': openapi.Schema(type=openapi.TYPE_STRING),
                        'citation': openapi.Schema(type=openapi.TYPE_STRING),
                        'identifier': openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description='DOI or persistent URL for this checklist version.',
                        ),
                        'contact': openapi.Schema(
                            type=openapi.TYPE_OBJECT,
                            description='Primary contact (from User record).',
                            properties={
                                'name': openapi.Schema(type=openapi.TYPE_STRING),
                                'email': openapi.Schema(type=openapi.TYPE_STRING),
                                'url': openapi.Schema(type=openapi.TYPE_STRING),
                            },
                        ),
                        'creator': openapi.Schema(
                            type=openapi.TYPE_ARRAY,
                            description='Dataset creators (from User records).',
                            items=openapi.Schema(
                                type=openapi.TYPE_OBJECT,
                                properties={
                                    'name': openapi.Schema(type=openapi.TYPE_STRING),
                                    'email': openapi.Schema(type=openapi.TYPE_STRING),
                                    'url': openapi.Schema(type=openapi.TYPE_STRING),
                                },
                            ),
                        ),
                        'source': openapi.Schema(
                            type=openapi.TYPE_ARRAY,
                            description='Upstream source datasets (from DataSource records).',
                            items=openapi.Schema(
                                type=openapi.TYPE_OBJECT,
                                properties={
                                    'id': openapi.Schema(type=openapi.TYPE_STRING),
                                    'title': openapi.Schema(type=openapi.TYPE_STRING),
                                    'description': openapi.Schema(type=openapi.TYPE_STRING),
                                },
                            ),
                        ),
                    },
                ),
            ),
            404: openapi.Response(description='Checklist version not found or none published.'),
        },
        tags=['ColDP'],
    )
    def get(self, request, *args, **kwargs):
        version_param = request.query_params.get('version', '').strip()
        checklist = _get_checklist(version_param or None)

        if checklist is None:
            return Response(
                {'detail': 'No published checklist found.'
                 if not version_param
                 else f'Checklist version "{version_param}" not found.'},
                status=404,
            )

        domain = get_current_domain()
        base_url = f'https://{domain}' if domain else ''

        issued = (
            checklist.released_at.isoformat()
            if checklist.released_at
            else date.today().isoformat()
        )

        contact = (
            _user_to_dict(checklist.contact, base_url)
            if checklist.contact_id
            else {'name': '', 'email': '', 'url': base_url}
        )

        creator = [
            _user_to_dict(u, base_url)
            for u in checklist.creators.all()
        ]

        if checklist.citation:
            citation = checklist.citation
        else:
            org = (
                creator[0]['name'] if creator
                else contact['name']
            )
            citation = (
                f'{org}. {checklist.title}. {base_url}. Accessed {issued}.'
                if org
                else f'{checklist.title}. {base_url}. Accessed {issued}.'
            )

        all_data_sources = DataSource.objects.all().values('name', 'category', 'description')
        source = [
            {
                'id': ds['name'].lower().replace(' ', '_'),
                'title': (
                    f"{ds['name']} - {ds['category']}"
                    if ds['category'] else ds['name']
                ),
                'description': ds['description'] or '',
            }
            for ds in all_data_sources
            if ds['name']
        ]

        payload = {
            'title': checklist.title,
            'description': checklist.description,
            'version': checklist.version,
            'issued': issued,
            'license': checklist.license,
            'citation': citation,
            'identifier': checklist.doi or base_url,
            'contact': contact,
            'creator': creator,
            'source': source,
        }

        return Response(payload)
