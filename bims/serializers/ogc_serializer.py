# coding=utf-8
"""Serializers for OGC layers.

Builds the GeoJSON features served by the OGC API - Features endpoints,
a flat, GIS-client friendly property list for a BiologicalCollectionRecord,
and serializes OgcLayer for the layer management API.
"""
import json
from urllib.parse import parse_qsl

from django.core.exceptions import ValidationError as DjangoValidationError
from django.urls import reverse
from preferences import preferences
from rest_framework import serializers

from bims.exceptions import OgcLayerBackOff
from bims.models.ogc_layer import OgcLayer
from bims.utils.ogc_layer import (
    OgcLayerError,
    OgcLayerOverCapError,
    validate_layer,
)
from bims.utils.ogc_protection import OgcLayerProtectionError
from bims.serializers.bio_collection_serializer import (
    BioCollectionOneRowSerializer,
    prefetch_batch,
)


def feature_geometry(record):
    site = record.site
    if not site:
        return None
    geometry = site.get_geometry()
    if geometry is None:
        return None
    return json.loads(geometry.geojson)


def _serializer_context(project_name=None) -> dict:
    """
    Get serializer context for biological collection record based on the project.
    :param project_name: the project name, if already known
    :return: dict of serializer context
    """
    if project_name is None:
        project_name = preferences.SiteSetting.project_name
    if project_name.lower() == 'sanparks':
        exclude_fields = [
            'user_river_name',
            'river_name',
            'user_wetland_name',
            'wetland_name',
            'user_geomorphological_zone',
            'hydroperiod',
            'wetland_indicator_status',
            'broad_biotope',
            'specific_biotope',
            'substratum',
            'analyst',
            'analyst_institute',
            'sampling_effort_measure',
            'sampling_effort_value',
            'abundance_value',
            'abundance_measure'
        ]
    else:
        exclude_fields = [
            'gbif_coordinate_uncertainty_m',
            'gbif_coordinate_precision',
        ]

    return {
        'header': [],
        'exclude_fields': exclude_fields,
        'added_headers': set(),
    }


def format_properties_key(properties, project_name=None):
    from bims.download.collection_record import format_header, PARK_OR_MPA_NAME
    if project_name is None:
        project_name = preferences.SiteSetting.project_name
    updated_properties = {}
    for prop_key, prop_val in properties.items():
        if project_name.lower() == 'sanparks' and prop_key == 'site_description':
            updated_prop_key = PARK_OR_MPA_NAME
        else:
            updated_prop_key = format_header(prop_key)
        updated_properties[updated_prop_key] = prop_val
    return updated_properties


def feature_from_record(record, properties=None, project_name=None):
    """Return a GeoJSON Feature dict for a BiologicalCollectionRecord.

    The feature id is the record uuid.

    :param properties: the serialized properties, if already known. See
        features_from_records().
    :param project_name: the project name, if already known.
    """
    if properties is None:
        properties = BioCollectionOneRowSerializer(
            record,
            context=_serializer_context(project_name)).data
    return {
        'type': 'Feature',
        'id': str(record.uuid) if record.uuid else record.pk,
        'geometry': feature_geometry(record),
        'properties': format_properties_key(properties, project_name),
    }


def features_from_records(records):
    """Return a GeoJSON Feature dict for each record, lazily.

    Serializing the properties of all records together builds the
    serializer fields once instead of once per record, and the lookups
    are bulk-loaded for the page (prefetch_batch), so the number of
    queries does not grow with the number of records.
    """
    records = list(records)
    project_name = preferences.SiteSetting.project_name
    context = _serializer_context(project_name)
    prefetch_batch(records, context)
    properties = BioCollectionOneRowSerializer(
        records, context=context, many=True).data
    for record, props in zip(records, properties):
        yield feature_from_record(record, props, project_name)


def layer_url(request, token):
    """The OGC API landing page URL to paste into a GIS client."""
    return request.build_absolute_uri(
        reverse('ogc-landing-page', kwargs={'token': token}))


def parse_query(value):
    """Turn a filter query string, or a URL with one, into a dict.

    The same parameters the BIMS filter panel puts in the search URL.
    """
    text = value.strip()
    if '?' in text:
        text = text.split('?', 1)[1]
    query = dict(parse_qsl(text))
    query.pop('requester', None)
    return query


class OgcLayerSerializer(serializers.ModelSerializer):
    owner = serializers.CharField(source='owner.username', read_only=True)
    query = serializers.JSONField(required=False)
    query_string = serializers.CharField(
        write_only=True, required=False, allow_blank=True,
        help_text='Filter as a query string, as in the BIMS search URL. '
                  'Used instead of ``query``.')
    record_count = serializers.SerializerMethodField()
    masked_url = serializers.SerializerMethodField()
    url = serializers.SerializerMethodField()

    class Meta:
        model = OgcLayer
        fields = (
            'id', 'name', 'owner', 'query', 'query_string',
            'include_sensitive', 'include_private', 'is_active',
            'disabled_reason', 'expires_at', 'created', 'last_accessed',
            'token_hint', 'url', 'masked_url',
            'record_count', 'record_count_updated',
        )
        read_only_fields = (
            'id', 'is_active', 'disabled_reason', 'created',
            'last_accessed', 'token_hint', 'record_count_updated',
        )

    def get_record_count(self, layer):
        # Counts are evaluated in the view (retrieve/create) and passed in
        # the context, so listing does not run a search per layer.
        counts = self.context.get('record_counts', {})
        return counts.get(layer.pk, layer.record_count_cached)

    def get_url(self, layer):
        """Full URL, only while the plain token is known."""
        if not layer.raw_token:
            return None
        return layer_url(self.context['request'], layer.raw_token)

    def get_masked_url(self, layer):
        return layer_url(
            self.context['request'], '...%s' % layer.token_hint)

    def validate_name(self, value):
        value = value.strip()
        if not value:
            raise serializers.ValidationError('The name cannot be empty.')
        return value

    def validate(self, attrs):
        if self.instance is not None:
            # Only the name can change. A different filter is a new layer.
            immutable = set(self.initial_data) & {
                'query', 'query_string', 'include_sensitive',
                'include_private'}
            if immutable:
                raise serializers.ValidationError(
                    'Only the name can be changed. Create a new layer to '
                    'use a different filter.')
            return attrs
        query_string = attrs.pop('query_string', None)
        if query_string is not None and 'query' in attrs:
            raise serializers.ValidationError(
                'Send either query or query_string, not both.')
        if query_string is not None:
            attrs['query'] = parse_query(query_string)
        query = attrs.get('query')
        if not isinstance(query, dict):
            raise serializers.ValidationError(
                {'query': 'A filter is required, as an object or '
                          'query_string.'})
        query.pop('requester', None)
        layer = OgcLayer(owner=self.context['request'].user, **attrs)
        try:
            layer.clean()
        except DjangoValidationError as exc:
            raise serializers.ValidationError(exc.message_dict)
        try:
            count = validate_layer(layer)
        except OgcLayerOverCapError as exc:
            raise serializers.ValidationError(
                {'query': str(exc)})
        except OgcLayerError as exc:
            raise serializers.ValidationError({'query': str(exc)})
        except OgcLayerProtectionError as exc:
            raise OgcLayerBackOff(exc)
        self.context['validated_count'] = count
        return attrs

    def create(self, validated_data):
        return OgcLayer.objects.create(
            owner=self.context['request'].user, **validated_data)
