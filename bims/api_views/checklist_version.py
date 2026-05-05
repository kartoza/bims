# coding=utf-8
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import serializers
from rest_framework.pagination import PageNumberPagination
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from bims.models.checklist_version import ChecklistVersion


class ChecklistVersionSerializer(serializers.ModelSerializer):
    taxon_group_name = serializers.CharField(
        source='taxon_group.name', read_only=True
    )
    published_by_name = serializers.SerializerMethodField()
    created_by_name = serializers.SerializerMethodField()

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
            'created_by_name',
            'previous_version',
            'license',
        ]

    def get_published_by_name(self, obj):
        if obj.published_by:
            return obj.published_by.get_full_name() or obj.published_by.username
        return None

    def get_created_by_name(self, obj):
        if obj.created_by:
            return obj.created_by.get_full_name() or obj.created_by.username
        return None


class ChecklistVersionCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = ChecklistVersion
        fields = [
            'taxon_group',
            'version',
            'doi',
            'notes',
            'previous_version',
            'license',
        ]

    def validate(self, attrs):
        taxon_group = attrs.get('taxon_group')
        version = attrs.get('version', '').strip()
        if not version:
            raise serializers.ValidationError({'version': 'Version string is required.'})
        if ChecklistVersion.objects.filter(
            taxon_group=taxon_group, version=version
        ).exists():
            raise serializers.ValidationError(
                {'version': f'Version "{version}" already exists for this module.'}
            )
        return attrs

    def create(self, validated_data):
        validated_data['created_by'] = self.context['request'].user
        return ChecklistVersion.objects.create(**validated_data)


class ChecklistVersionPagination(PageNumberPagination):
    page_size = 20
    page_size_query_param = 'page_size'
    max_page_size = 100


class ChecklistVersionListView(APIView):
    """
    GET  — list published/draft checklist versions
    POST — create a new draft version (superusers only)
    """

    @swagger_auto_schema(
        operation_id='checklist_version_list',
        operation_summary='List checklist versions',
        operation_description=(
            'Returns a paginated list of ChecklistVersion records. '
            'Filter by taxon group using `?taxon_group=<id>`. '
            'Use `?status=draft` to include draft versions (superusers only).'
        ),
        manual_parameters=[
            openapi.Parameter(
                'taxon_group', openapi.IN_QUERY,
                description='Filter by TaxonGroup ID.',
                type=openapi.TYPE_INTEGER, required=False,
            ),
            openapi.Parameter(
                'status', openapi.IN_QUERY,
                description=(
                    '`published` (default), `draft`, or omit for all statuses '
                    '(superusers only — non-superusers always receive published only).'
                ),
                type=openapi.TYPE_STRING,
                enum=['published', 'draft'],
                required=False,
            ),
            openapi.Parameter(
                'page_size', openapi.IN_QUERY,
                description='Results per page (max 100).',
                type=openapi.TYPE_INTEGER, required=False,
            ),
        ],
        responses={200: ChecklistVersionSerializer(many=True)},
        tags=['Checklist'],
    )
    def get(self, request):
        status_param = request.query_params.get('status', '')

        qs = (
            ChecklistVersion.objects
            .select_related('taxon_group', 'published_by', 'created_by')
            .order_by('-created_at')
        )

        # Superusers see all statuses by default; others only see published
        if request.user.is_superuser:
            if status_param in (ChecklistVersion.STATUS_DRAFT, ChecklistVersion.STATUS_PUBLISHED):
                qs = qs.filter(status=status_param)
            # else: no filter → all statuses
        else:
            qs = qs.filter(status=ChecklistVersion.STATUS_PUBLISHED)

        taxon_group_id = request.query_params.get('taxon_group')
        if taxon_group_id:
            qs = qs.filter(taxon_group_id=taxon_group_id)

        paginator = ChecklistVersionPagination()
        page = paginator.paginate_queryset(qs, request)
        serializer = ChecklistVersionSerializer(page, many=True, context={'request': request})
        return paginator.get_paginated_response(serializer.data)

    @swagger_auto_schema(
        operation_id='checklist_version_create',
        operation_summary='Create a draft checklist version',
        operation_description='Superusers only. Creates a new draft ChecklistVersion.',
        request_body=ChecklistVersionCreateSerializer,
        responses={
            201: ChecklistVersionSerializer(),
            400: openapi.Response(description='Validation error.'),
            403: openapi.Response(description='Superuser required.'),
        },
        tags=['Checklist'],
    )
    def post(self, request):
        if not request.user.is_superuser:
            return Response({'detail': 'Superuser access required.'}, status=403)

        serializer = ChecklistVersionCreateSerializer(
            data=request.data, context={'request': request}
        )
        if serializer.is_valid():
            obj = serializer.save()
            return Response(
                ChecklistVersionSerializer(obj, context={'request': request}).data,
                status=201,
            )
        return Response(serializer.errors, status=400)


class ChecklistVersionDetailView(APIView):
    """
    GET — retrieve a single checklist version by UUID.
    """

    @swagger_auto_schema(
        operation_id='checklist_version_detail',
        operation_summary='Retrieve a checklist version',
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


class ChecklistVersionPublishView(APIView):
    """
    POST /api/checklist-version/<uuid>/publish/
    Publish a draft ChecklistVersion (superusers only).
    """

    permission_classes = [IsAuthenticated]

    @swagger_auto_schema(
        operation_id='checklist_version_publish',
        operation_summary='Publish a draft checklist version',
        operation_description=(
            'Superusers only. Transitions a draft ChecklistVersion to published status, '
            'creates all ChecklistSnapshot rows, and stamps Taxonomy version UUIDs.'
        ),
        responses={
            200: ChecklistVersionSerializer(),
            400: openapi.Response(description='Already published.'),
            403: openapi.Response(description='Superuser required.'),
            404: openapi.Response(description='Not found.'),
        },
        tags=['Checklist'],
    )
    def post(self, request, pk):
        if not request.user.is_superuser:
            return Response({'detail': 'Superuser access required.'}, status=403)

        try:
            obj = ChecklistVersion.objects.select_related('taxon_group').get(pk=pk)
        except ChecklistVersion.DoesNotExist:
            return Response({'detail': 'Not found.'}, status=404)

        if obj.status == ChecklistVersion.STATUS_PUBLISHED:
            return Response({'detail': 'Already published.'}, status=400)

        obj.publish(published_by=request.user)
        obj.refresh_from_db()
        return Response(
            ChecklistVersionSerializer(obj, context={'request': request}).data
        )
