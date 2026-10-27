# coding=utf-8
"""API to create, list, rename, rotate and revoke OGC layers.

A layer is a saved BIMS filter that is served as an OGC API - Features
collection (see bims/api_views/ogc_features.py). The URL contains a secret
token of which only a hash is stored, so the full URL is returned when the
layer is created or its token is rotated, and cannot be shown again.
"""
from django.conf import settings
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.exceptions import APIException
from rest_framework.permissions import BasePermission, IsAuthenticated
from rest_framework.response import Response

from bims.models.ogc_layer import OgcLayer
from bims.serializers.ogc_serializer import OgcLayerSerializer
from bims.utils.ogc_layer import (
    OgcLayerError,
    count_layer_records,
)
from bims.utils.ogc_protection import OgcLayerProtectionError


class OgcLayersDisabled(APIException):
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    default_detail = 'Live layers are switched off on this server.'
    default_code = 'ogc_layers_disabled'


class IsOwnerOrStaffRevoke(BasePermission):
    """Owners manage their layers; staff can see and revoke any layer."""

    staff_actions = ('retrieve', 'revoke', 'destroy')

    def has_permission(self, request, view):
        return bool(request.user and request.user.is_authenticated)

    def has_object_permission(self, request, view, obj):
        if obj.owner_id == request.user.id:
            return True
        return request.user.is_staff and view.action in self.staff_actions


class OgcLayerViewSet(viewsets.ModelViewSet):
    """Manage the OGC layers of the current user (all layers for staff)."""

    serializer_class = OgcLayerSerializer
    permission_classes = [IsAuthenticated, IsOwnerOrStaffRevoke]
    http_method_names = ['get', 'post', 'patch', 'put', 'delete', 'head',
                         'options']

    def get_queryset(self):
        queryset = OgcLayer.objects.select_related('owner')
        user = self.request.user
        if user.is_staff:
            if self.request.query_params.get('mine') in ('1', 'true'):
                return queryset.filter(owner=user)
            return queryset
        return queryset.filter(owner=user)

    def initial(self, request, *args, **kwargs):
        super().initial(request, *args, **kwargs)
        if request.method == 'POST' and self.action == 'create' and \
                not settings.OGC_LAYERS_ENABLED:
            raise OgcLayersDisabled()

    def handle_exception(self, exc):
        response = super().handle_exception(exc)
        retry_after = getattr(exc, 'retry_after', None)
        if retry_after:
            response['Retry-After'] = str(retry_after)
        return response

    def _with_count(self, layer, serializer_class=None):
        """Serialize a layer with its current number of records."""
        context = self.get_serializer_context()
        context['record_counts'] = {}
        try:
            context['record_counts'][layer.pk] = count_layer_records(layer)
        except (OgcLayerError, OgcLayerProtectionError):
            # Over the cap or busy: the cached figure is shown instead
            pass
        return (serializer_class or self.get_serializer_class())(
            layer, context=context).data

    def retrieve(self, request, *args, **kwargs):
        layer = self.get_object()
        return Response(self._with_count(layer))

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        layer = serializer.save()
        serializer.context['record_counts'] = {
            layer.pk: serializer.context['validated_count']}
        layer.record_count_cached = serializer.context['validated_count']
        layer.save(update_fields=['record_count_cached'])
        return Response(
            self.get_serializer(layer, context=serializer.context).data,
            status=status.HTTP_201_CREATED)

    def update(self, request, *args, **kwargs):
        layer = self.get_object()
        serializer = self.get_serializer(
            layer, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        # Save the name only: fields such as the token must stay as stored
        layer.name = serializer.validated_data.get('name', layer.name)
        layer.save(update_fields=['name'])
        return Response(self.get_serializer(layer).data)

    @action(detail=True, methods=['post'], url_path='rotate-token')
    def rotate_token(self, request, pk=None):
        """Replace the token. The old URL stops working at once."""
        layer = self.get_object()
        if not layer.is_active:
            return Response(
                {'detail': 'A revoked layer cannot get a new URL.'},
                status=status.HTTP_400_BAD_REQUEST)
        layer.rotate_token()
        return Response(self.get_serializer(layer).data)

    @action(detail=True, methods=['post'])
    def revoke(self, request, pk=None):
        """Switch the layer off. The URL returns 404; the layer is kept."""
        layer = self.get_object()
        layer.is_active = False
        layer.save(update_fields=['is_active'])
        return Response(self.get_serializer(layer).data)
