# coding=utf-8
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import serializers
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.views import APIView

from bims.models.checklist_version import ChecklistVersion


class ChecklistVersionSerializer(serializers.ModelSerializer):
    taxon_group_name = serializers.CharField(
        source='taxon_group.name', read_only=True
    )
    published_by_name = serializers.SerializerMethodField()

    class Meta:
        model = ChecklistVersion
        fields = [
            'id',
            'version',
            'status',
            'taxon_group',
            'taxon_group_name',
            'doi',
            'dataset_key',
            'notes',
            'taxa_count',
            'additions_count',
            'updates_count',
            'created_at',
            'published_at',
            'published_by_name',
            'previous_version',
        ]

    def get_published_by_name(self, obj):
        if obj.published_by:
            return obj.published_by.get_full_name() or obj.published_by.username
        return None


class ChecklistVersionPagination(PageNumberPagination):
    page_size = 20
    page_size_query_param = 'page_size'
    max_page_size = 100


class ChecklistVersionListView(APIView):
    """
    List published checklist versions, optionally filtered by module.
    """

    @swagger_auto_schema(
        operation_id='checklist_version_list',
        operation_summary='List checklist versions',
        operation_description=(
            'Returns a paginated list of published ChecklistVersion records. '
            'Filter by taxon group using `?taxon_group=<id>`. '
            'Use `?status=draft` to include draft versions (superusers only).'
        ),
        manual_parameters=[
            openapi.Parameter(
                'taxon_group',
                openapi.IN_QUERY,
                description='Filter by TaxonGroup ID.',
                type=openapi.TYPE_INTEGER,
                required=False,
            ),
            openapi.Parameter(
                'status',
                openapi.IN_QUERY,
                description='Filter by status: `published` (default) or `draft`.',
                type=openapi.TYPE_STRING,
                enum=['published', 'draft'],
                required=False,
            ),
            openapi.Parameter(
                'page',
                openapi.IN_QUERY,
                description='Page number.',
                type=openapi.TYPE_INTEGER,
                required=False,
            ),
            openapi.Parameter(
                'page_size',
                openapi.IN_QUERY,
                description='Results per page (max 100).',
                type=openapi.TYPE_INTEGER,
                required=False,
            ),
        ],
        responses={
            200: openapi.Response(
                description='Paginated list of checklist versions.',
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'count': openapi.Schema(type=openapi.TYPE_INTEGER),
                        'next': openapi.Schema(type=openapi.TYPE_STRING, nullable=True),
                        'previous': openapi.Schema(type=openapi.TYPE_STRING, nullable=True),
                        'results': openapi.Schema(
                            type=openapi.TYPE_ARRAY,
                            items=openapi.Schema(type=openapi.TYPE_OBJECT),
                        ),
                    },
                ),
            ),
        },
        tags=['Checklist'],
    )
    def get(self, request):
        status_param = request.query_params.get('status', ChecklistVersion.STATUS_PUBLISHED)

        # Only superusers may view drafts
        if status_param == ChecklistVersion.STATUS_DRAFT and not request.user.is_superuser:
            status_param = ChecklistVersion.STATUS_PUBLISHED

        qs = (
            ChecklistVersion.objects
            .filter(status=status_param)
            .select_related('taxon_group', 'published_by')
            .order_by('-published_at', '-created_at')
        )

        taxon_group_id = request.query_params.get('taxon_group')
        if taxon_group_id:
            qs = qs.filter(taxon_group_id=taxon_group_id)

        paginator = ChecklistVersionPagination()
        page = paginator.paginate_queryset(qs, request)
        serializer = ChecklistVersionSerializer(page, many=True, context={'request': request})
        return paginator.get_paginated_response(serializer.data)


class ChecklistVersionDetailView(APIView):
    """
    Retrieve a single checklist version by its UUID.
    """

    @swagger_auto_schema(
        operation_id='checklist_version_detail',
        operation_summary='Retrieve a checklist version',
        operation_description='Returns full detail for a single ChecklistVersion identified by its UUID.',
        responses={
            200: ChecklistVersionSerializer(),
            404: openapi.Response(description='Not found.'),
        },
        tags=['Checklist'],
    )
    def get(self, request, pk):
        try:
            obj = (
                ChecklistVersion.objects
                .select_related('taxon_group', 'published_by', 'previous_version')
                .get(pk=pk)
            )
        except ChecklistVersion.DoesNotExist:
            return Response({'detail': 'Not found.'}, status=404)

        serializer = ChecklistVersionSerializer(obj, context={'request': request})
        return Response(serializer.data)
