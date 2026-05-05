from datetime import date

from django.db import models
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.views import APIView

from preferences import preferences

from bims.models.checklist_version import ChecklistSnapshot, ChecklistVersion
from bims.models.data_source import DataSource
from bims.models.taxonomy import Taxonomy
from bims.models.taxonomy_checklist import TaxonomyChecklist
from bims.serializers.coldp_serializer import ColDPTaxonSerializer
from bims.utils.domain import get_current_domain


class ColDPTaxonPagination(PageNumberPagination):
    page_size = 100
    page_size_query_param = 'page_size'
    max_page_size = 1000


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


class ColDPTaxonView(APIView):
    """
    Paginated ColDP NameUsage endpoint.

    Returns Taxonomy records serialised in the ColDP NameUsage flat format.
    Accepts the following query parameters:

    * ``rank``   – filter by taxonomic rank name (e.g. ``SPECIES``, ``GENUS``)
    * ``parent`` – filter to direct children of the given Taxonomy ``id``
    * ``status`` – filter by BIMS taxonomic status (default: all statuses)
    * ``q``      – search by taxon name (case-insensitive substring on canonical_name / scientific_name)
    * ``page`` / ``page_size`` – pagination controls (default page size: 100)
    """

    pagination_class = ColDPTaxonPagination

    @property
    def paginator(self):
        if not hasattr(self, '_paginator'):
            self._paginator = self.pagination_class()
        return self._paginator

    @swagger_auto_schema(
        operation_id='coldp_taxon',
        operation_summary='ColDP taxon list (NameUsage)',
        operation_description=(
            'Returns a paginated list of taxa in the ColDP **NameUsage** flat '
            'format. Each record includes the taxon ID, parent ID, status, '
            'scientific name, authorship, rank, and full classification from '
            'kingdom down to species.\n\n'
            'By default all taxa (accepted + synonyms) are returned. Use the '
            '``status`` parameter to restrict to accepted names only.'
        ),
        manual_parameters=[
            openapi.Parameter(
                'rank', openapi.IN_QUERY,
                description='Filter by rank (e.g. SPECIES, GENUS, FAMILY).',
                type=openapi.TYPE_STRING, required=False,
            ),
            openapi.Parameter(
                'parent', openapi.IN_QUERY,
                description='Return only direct children of this Taxonomy ID.',
                type=openapi.TYPE_INTEGER, required=False,
            ),
            openapi.Parameter(
                'status', openapi.IN_QUERY,
                description=(
                    'Filter by taxonomic status. '
                    'Accepted values: ACCEPTED, SYNONYM, HETEROTYPIC_SYNONYM, '
                    'HOMOTYPIC_SYNONYM, PROPARTE_SYNONYM, MISAPPLIED, DOUBTFUL.'
                ),
                type=openapi.TYPE_STRING, required=False,
            ),
            openapi.Parameter(
                'q', openapi.IN_QUERY,
                description=(
                    'Search by taxon name. Case-insensitive substring match '
                    'against canonical_name and scientific_name.'
                ),
                type=openapi.TYPE_STRING, required=False,
            ),
            openapi.Parameter(
                'page_size', openapi.IN_QUERY,
                description='Number of results per page (default 100, max 1000).',
                type=openapi.TYPE_INTEGER, required=False,
            ),
        ],
        responses={
            200: openapi.Response(
                description='Paginated ColDP NameUsage records',
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'count': openapi.Schema(type=openapi.TYPE_INTEGER),
                        'next': openapi.Schema(
                            type=openapi.TYPE_STRING, format='uri', x_nullable=True),
                        'previous': openapi.Schema(
                            type=openapi.TYPE_STRING, format='uri', x_nullable=True),
                        'results': openapi.Schema(
                            type=openapi.TYPE_ARRAY,
                            items=openapi.Schema(
                                type=openapi.TYPE_OBJECT,
                                properties={
                                    'taxonID': openapi.Schema(type=openapi.TYPE_STRING),
                                    'parentID': openapi.Schema(type=openapi.TYPE_STRING),
                                    'status': openapi.Schema(type=openapi.TYPE_STRING),
                                    'scientificName': openapi.Schema(type=openapi.TYPE_STRING),
                                    'authorship': openapi.Schema(type=openapi.TYPE_STRING),
                                    'rank': openapi.Schema(type=openapi.TYPE_STRING),
                                    'kingdom': openapi.Schema(type=openapi.TYPE_STRING),
                                    'phylum': openapi.Schema(type=openapi.TYPE_STRING),
                                    'class': openapi.Schema(type=openapi.TYPE_STRING),
                                    'subclass': openapi.Schema(type=openapi.TYPE_STRING),
                                    'order': openapi.Schema(type=openapi.TYPE_STRING),
                                    'suborder': openapi.Schema(type=openapi.TYPE_STRING),
                                    'superfamily': openapi.Schema(type=openapi.TYPE_STRING),
                                    'family': openapi.Schema(type=openapi.TYPE_STRING),
                                    'tribe': openapi.Schema(type=openapi.TYPE_STRING),
                                    'subtribe': openapi.Schema(type=openapi.TYPE_STRING),
                                    'genus': openapi.Schema(type=openapi.TYPE_STRING),
                                    'subgenus': openapi.Schema(type=openapi.TYPE_STRING),
                                    'environment': openapi.Schema(
                                        type=openapi.TYPE_STRING,
                                        description=(
                                            'Habitat environment: brackish, freshwater, '
                                            'marine, terrestrial, or empty.'
                                        ),
                                    ),
                                    'species': openapi.Schema(type=openapi.TYPE_STRING),
                                },
                            ),
                        ),
                    },
                ),
            ),
        },
        tags=['ColDP'],
    )
    def get(self, request, *args, **kwargs):
        qs = Taxonomy.objects.select_related(
            'parent', 'accepted_taxonomy'
        ).prefetch_related('tags').order_by('canonical_name', 'id')

        rank = request.query_params.get('rank', '').strip().upper()
        if rank:
            qs = qs.filter(rank=rank)

        parent_id = request.query_params.get('parent', '').strip()
        if parent_id:
            qs = qs.filter(parent_id=parent_id)

        status = request.query_params.get('status', '').strip().upper()
        if status:
            qs = qs.filter(taxonomic_status=status)

        q = request.query_params.get('q', '').strip()
        if q:
            qs = qs.filter(
                models.Q(canonical_name__icontains=q) |
                models.Q(scientific_name__icontains=q)
            )

        site_prefix = (
            getattr(preferences.SiteSetting, 'default_data_source', '') or ''
        ).upper()

        page = self.paginator.paginate_queryset(qs, request, view=self)
        serializer = ColDPTaxonSerializer(
            page, many=True, context={'site_prefix': site_prefix}
        )
        return self.paginator.get_paginated_response(serializer.data)


class ColDPSnapshotView(APIView):
    """
    Paginated ColDP NameUsage list from a published ChecklistSnapshot.

    Serves pre-rendered snapshot rows for a specific ChecklistVersion UUID.
    Supports filtering by ``rank``, ``change_type``, and free-text ``q`` search.
    """

    pagination_class = ColDPTaxonPagination

    @property
    def paginator(self):
        if not hasattr(self, '_paginator'):
            self._paginator = self.pagination_class()
        return self._paginator

    @swagger_auto_schema(
        operation_id='coldp_snapshot_taxon',
        operation_summary='ColDP NameUsage snapshot for a checklist version',
        operation_description=(
            'Returns a paginated list of pre-rendered ColDP NameUsage rows '
            'from the materialized **ChecklistSnapshot** for the given '
            'ChecklistVersion UUID.\n\n'
            'Only published versions are accessible. Snapshot rows are written '
            'once at publish time and never modified, so this endpoint is '
            'suitable for stable, reproducible exports.'
        ),
        manual_parameters=[
            openapi.Parameter(
                'rank', openapi.IN_QUERY,
                description='Filter by taxonomic rank (e.g. SPECIES, GENUS).',
                type=openapi.TYPE_STRING, required=False,
            ),
            openapi.Parameter(
                'change_type', openapi.IN_QUERY,
                description=(
                    'Filter by change type relative to the previous version: '
                    '``added``, ``updated``, or ``unchanged``.'
                ),
                type=openapi.TYPE_STRING,
                enum=['added', 'updated', 'unchanged'],
                required=False,
            ),
            openapi.Parameter(
                'q', openapi.IN_QUERY,
                description='Case-insensitive substring search on scientific_name.',
                type=openapi.TYPE_STRING, required=False,
            ),
            openapi.Parameter(
                'page_size', openapi.IN_QUERY,
                description='Results per page (default 100, max 1000).',
                type=openapi.TYPE_INTEGER, required=False,
            ),
        ],
        responses={
            200: openapi.Response(
                description='Paginated ColDP NameUsage snapshot rows.',
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'count': openapi.Schema(type=openapi.TYPE_INTEGER),
                        'next': openapi.Schema(
                            type=openapi.TYPE_STRING, format='uri', x_nullable=True),
                        'previous': openapi.Schema(
                            type=openapi.TYPE_STRING, format='uri', x_nullable=True),
                        'version': openapi.Schema(
                            type=openapi.TYPE_STRING,
                            description='Human-readable version string of the checklist.',
                        ),
                        'checklist_version_id': openapi.Schema(
                            type=openapi.TYPE_STRING, format='uuid',
                        ),
                        'results': openapi.Schema(
                            type=openapi.TYPE_ARRAY,
                            items=openapi.Schema(
                                type=openapi.TYPE_OBJECT,
                                properties={
                                    'taxonID':         openapi.Schema(type=openapi.TYPE_STRING),
                                    'parentID':        openapi.Schema(type=openapi.TYPE_STRING),
                                    'basionymID':      openapi.Schema(type=openapi.TYPE_STRING),
                                    'rank':            openapi.Schema(type=openapi.TYPE_STRING),
                                    'scientificName':  openapi.Schema(type=openapi.TYPE_STRING),
                                    'authorship':      openapi.Schema(type=openapi.TYPE_STRING),
                                    'status':          openapi.Schema(type=openapi.TYPE_STRING),
                                    'nameStatus':      openapi.Schema(type=openapi.TYPE_STRING),
                                    'kingdom':         openapi.Schema(type=openapi.TYPE_STRING),
                                    'phylum':          openapi.Schema(type=openapi.TYPE_STRING),
                                    'class':           openapi.Schema(type=openapi.TYPE_STRING),
                                    'order':           openapi.Schema(type=openapi.TYPE_STRING),
                                    'family':          openapi.Schema(type=openapi.TYPE_STRING),
                                    'genus':           openapi.Schema(type=openapi.TYPE_STRING),
                                    'vernacularNames': openapi.Schema(type=openapi.TYPE_ARRAY,
                                        items=openapi.Schema(type=openapi.TYPE_OBJECT)),
                                    'distributions':   openapi.Schema(type=openapi.TYPE_ARRAY,
                                        items=openapi.Schema(type=openapi.TYPE_OBJECT)),
                                    'referenceID':     openapi.Schema(type=openapi.TYPE_STRING),
                                    'remarks':         openapi.Schema(type=openapi.TYPE_STRING),
                                    'changeType':      openapi.Schema(type=openapi.TYPE_STRING),
                                },
                            ),
                        ),
                    },
                ),
            ),
            404: openapi.Response(description='ChecklistVersion not found or not published.'),
        },
        tags=['ColDP'],
    )
    def get(self, request, checklist_uuid):
        try:
            version = ChecklistVersion.objects.get(
                pk=checklist_uuid,
                status=ChecklistVersion.STATUS_PUBLISHED,
            )
        except ChecklistVersion.DoesNotExist:
            return Response(
                {'detail': 'Checklist version not found or not published.'},
                status=404,
            )

        qs = (
            ChecklistSnapshot.objects
            .filter(checklist_version=version)
            .order_by('scientific_name', 'checklist_id')
        )

        rank = request.query_params.get('rank', '').strip().upper()
        if rank:
            qs = qs.filter(rank=rank)

        change_type = request.query_params.get('change_type', '').strip().lower()
        if change_type in (
            ChecklistSnapshot.CHANGE_ADDED,
            ChecklistSnapshot.CHANGE_UPDATED,
            ChecklistSnapshot.CHANGE_UNCHANGED,
        ):
            qs = qs.filter(change_type=change_type)

        q = request.query_params.get('q', '').strip()
        if q:
            qs = qs.filter(scientific_name__icontains=q)

        page = self.paginator.paginate_queryset(qs, request, view=self)
        results = [_snapshot_to_coldp(row) for row in page]
        response = self.paginator.get_paginated_response(results)
        response.data['version'] = version.version
        response.data['checklist_version_id'] = str(version.pk)
        return response


def _snapshot_to_coldp(row: ChecklistSnapshot) -> dict:
    """Serialize a ChecklistSnapshot row to the ColDP NameUsage dict."""
    return {
        'taxonID':         row.checklist_id,
        'parentID':        row.parent_checklist_id,
        'basionymID':      row.basionym_checklist_id,
        'rank':            row.rank,
        'scientificName':  row.scientific_name,
        'authorship':      row.authorship,
        'status':          row.taxonomic_status,
        'nameStatus':      row.name_status,
        'kingdom':         row.kingdom,
        'phylum':          row.phylum,
        'class':           row.klass,
        'order':           row.order,
        'family':          row.family,
        'genus':           row.genus,
        'vernacularNames': row.vernacular_names,
        'distributions':   row.distributions,
        'referenceID':     row.reference_id,
        'remarks':         row.remarks,
        'changeType':      row.change_type,
    }
