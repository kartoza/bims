# coding=utf-8
import os

from django.conf import settings
from django.core.exceptions import SuspiciousFileOperation
from django.db import connection
from django.http import FileResponse
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import serializers
from rest_framework.pagination import PageNumberPagination
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from bims.models.checklist_version import ChecklistVersion
from bims.models.download_request import DownloadRequest
from bims.models.taxon_group import TaxonGroup
from bims.utils.filepath import ensure_within_dir, sanitize_path_component


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
            'deletions_count',
            'is_publishing',
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
        taxon_group_id = request.query_params.get('taxon_group')
        can_manage_group = False
        if taxon_group_id and request.user.is_authenticated:
            can_manage_group = (
                request.user.is_superuser or
                TaxonGroup.objects.filter(
                    id=taxon_group_id,
                    experts=request.user
                ).exists()
            )

        qs = (
            ChecklistVersion.objects
            .select_related('taxon_group', 'published_by', 'created_by')
            .order_by('-created_at')
        )

        if request.user.is_superuser or can_manage_group:
            if status_param in (ChecklistVersion.STATUS_DRAFT, ChecklistVersion.STATUS_PUBLISHED):
                qs = qs.filter(status=status_param)
        else:
            qs = qs.filter(status=ChecklistVersion.STATUS_PUBLISHED)

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
        try:
            obj = ChecklistVersion.objects.select_related('taxon_group').get(pk=pk)
        except ChecklistVersion.DoesNotExist:
            return Response({'detail': 'Not found.'}, status=404)

        can_publish = (
            request.user.is_superuser or
            obj.taxon_group.experts.filter(id=request.user.id).exists()
        )
        if not can_publish:
            return Response(
                {'detail': 'Admin or taxon group expert access required.'},
                status=403
            )

        if obj.status == ChecklistVersion.STATUS_PUBLISHED:
            return Response({'detail': 'Already published.'}, status=400)

        if obj.is_publishing:
            return Response({'detail': 'Already publishing.'}, status=400)

        # Mark as publishing immediately so the UI reflects it right away,
        # then hand off to Celery so the endpoint returns without blocking.
        obj.is_publishing = True
        obj.save(update_fields=['is_publishing'])

        from django.db import connection
        from bims.tasks.checklist import publish_versions_task
        publish_versions_task.delay(
            schema_name=connection.schema_name,
            version_ids=[str(obj.pk)],
            published_by_id=request.user.pk,
        )

        obj.refresh_from_db()
        return Response(
            ChecklistVersionSerializer(obj, context={'request': request}).data,
            status=202,
        )


class ChecklistVersionDeleteView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, pk):
        try:
            obj = ChecklistVersion.objects.select_related('taxon_group').get(pk=pk)
        except ChecklistVersion.DoesNotExist:
            return Response({'detail': 'Not found.'}, status=404)

        can_delete = (
            request.user.is_superuser or
            obj.taxon_group.experts.filter(id=request.user.id).exists()
        )
        if not can_delete:
            return Response(
                {'detail': 'Admin or taxon group expert access required.'},
                status=403
            )

        if obj.status != ChecklistVersion.STATUS_PUBLISHED:
            return Response(
                {'detail': 'Only published checklist versions can be removed.'},
                status=400
            )

        from bims.tasks.checklist import delete_published_checklist_version_task
        delete_published_checklist_version_task.delay(
            str(connection.schema_name),
            str(obj.pk),
        )
        return Response({
            'message': 'Checklist removal queued.'
        }, status=202)


class ChecklistVersionExportView(APIView):
    """
    POST /api/checklist-version/<uuid>/export/
        Enqueue a ColDP ZIP export for a published ChecklistVersion.
        Returns {download_request_id, status_url}.

    GET /api/checklist-version/<uuid>/export/?download_request_id=<id>
        Stream the completed ZIP file.
    """

    permission_classes = [IsAuthenticated]

    def post(self, request, pk):
        try:
            version = ChecklistVersion.objects.get(pk=pk)
        except ChecklistVersion.DoesNotExist:
            return Response({'detail': 'Not found.'}, status=404)

        if version.status != ChecklistVersion.STATUS_PUBLISHED:
            return Response({'detail': 'Only published versions can be exported.'}, status=400)

        download_request_id = request.data.get('download_request_id')
        if download_request_id:
            try:
                dr = DownloadRequest.objects.get(
                    id=download_request_id,
                    requester=request.user,
                )
            except DownloadRequest.DoesNotExist:
                return Response({'detail': 'Download request not found.'}, status=404)

            if dr.request_file:
                return Response({
                    'download_request_id': dr.id,
                    'status_url': f'/api/download-request/{dr.id}/progress/',
                    'download_url': f'/api/download-request/{dr.id}/file/',
                })

            if dr.processing and dr.progress:
                return Response({
                    'download_request_id': dr.id,
                    'status_url': f'/api/download-request/{dr.id}/progress/',
                    'download_url': f'/api/download-request/{dr.id}/file/',
                }, status=202)

            dr.processing = True
            dr.resource_type = DownloadRequest.ZIP
            dr.resource_name = f'Checklist ColDP ZIP {version.pk}'
            dr.request_category = f'{version.taxon_group.name} {version.version}'
            dr.approved = True
            dr.save(update_fields=[
                'processing',
                'resource_type',
                'resource_name',
                'request_category',
                'approved',
            ])
        else:
            dr = DownloadRequest.objects.create(
                requester=request.user,
                resource_type=DownloadRequest.ZIP,
                resource_name=f'Checklist ColDP ZIP {version.pk}',
                request_category=f'{version.taxon_group.name} {version.version}',
                approved=True,
                processing=True,
            )

        from bims.tasks.coldp_export import export_coldp_zip
        export_coldp_zip.delay(dr.id, str(version.pk))

        return Response({
            'download_request_id': dr.id,
            'status_url': f'/api/download-request/{dr.id}/progress/',
            'download_url': f'/api/download-request/{dr.id}/file/',
        }, status=202)

    def get(self, request, pk):
        """Stream the ZIP once export is complete."""
        dr_id = request.query_params.get('download_request_id')
        if not dr_id:
            return Response({'detail': 'download_request_id is required.'}, status=400)

        try:
            dr = DownloadRequest.objects.get(id=dr_id, requester=request.user)
        except DownloadRequest.DoesNotExist:
            return Response({'detail': 'Download request not found.'}, status=404)

        if dr.processing:
            return Response({'detail': 'Export still in progress.'}, status=202)

        if dr.request_file:
            return FileResponse(
                dr.request_file.open('rb'),
                content_type='application/zip',
                as_attachment=True,
                filename=dr.request_category or os.path.basename(dr.request_file.name),
            )

        file_path = dr.download_path
        if not file_path or not os.path.exists(file_path):
            return Response({'detail': 'Export file not found.'}, status=404)

        try:
            safe_file_path = ensure_within_dir(file_path, settings.MEDIA_ROOT)
        except SuspiciousFileOperation:
            return Response({'detail': 'Export file not found.'}, status=404)

        filename = sanitize_path_component(
            dr.request_category or os.path.basename(safe_file_path),
            'checklist',
        )
        response = FileResponse(
            open(safe_file_path, 'rb'),
            content_type='application/zip',
        )
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response
