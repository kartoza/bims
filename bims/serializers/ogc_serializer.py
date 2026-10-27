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
from rest_framework import serializers

from bims.enums.taxonomic_rank import TaxonomicRank
from bims.exceptions import OgcLayerBackOff
from bims.models.ogc_layer import OgcLayer
from bims.utils.ogc_layer import (
    OgcLayerError,
    OgcLayerOverCapError,
    validate_layer,
)
from bims.utils.ogc_protection import OgcLayerProtectionError


class OgcFeaturePropertiesSerializer(serializers.Serializer):
    """Flat property list for a BiologicalCollectionRecord OGC feature."""

    record_id = serializers.IntegerField(source='pk')
    uuid = serializers.SerializerMethodField()
    species_name = serializers.SerializerMethodField()
    taxon_rank = serializers.SerializerMethodField()
    site_code = serializers.SerializerMethodField()
    date = serializers.SerializerMethodField()
    collector = serializers.SerializerMethodField()
    owner = serializers.SerializerMethodField()
    institution = serializers.SerializerMethodField()
    validation_status = serializers.SerializerMethodField()
    origin = serializers.SerializerMethodField()
    conservation_status = serializers.SerializerMethodField()

    @staticmethod
    def _person_name(user):
        if not user:
            return None
        full_name = ('%s %s' % (user.first_name, user.last_name)).strip()
        return full_name or user.username

    def get_uuid(self, obj):
        return str(obj.uuid) if obj.uuid else None

    def get_species_name(self, obj):
        taxonomy = obj.taxonomy
        if taxonomy and taxonomy.canonical_name:
            return taxonomy.canonical_name
        return obj.original_species_name

    def get_taxon_rank(self, obj):
        taxonomy = obj.taxonomy
        if not taxonomy or not taxonomy.rank:
            return None
        try:
            return TaxonomicRank[taxonomy.rank].value
        except KeyError:
            return taxonomy.rank

    def get_site_code(self, obj):
        site = obj.site
        if not site:
            return None
        return site.site_code or site.legacy_site_code

    def get_date(self, obj):
        if not obj.collection_date:
            return None
        return obj.collection_date.isoformat()

    def get_collector(self, obj):
        return obj.collector or self._person_name(obj.collector_user)

    def get_owner(self, obj):
        return self._person_name(obj.owner)

    def get_institution(self, obj):
        return obj.institution_id or None

    def get_validation_status(self, obj):
        if obj.ready_for_validation:
            return 'in_review'
        if obj.is_validated:
            return 'validated'
        if obj.is_rejected:
            return 'rejected'
        return 'unvalidated'

    def get_origin(self, obj):
        taxonomy = obj.taxonomy
        if taxonomy and taxonomy.origin:
            return taxonomy.origin.category
        return None

    def get_conservation_status(self, obj):
        taxonomy = obj.taxonomy
        if not taxonomy:
            return None
        status = taxonomy.iucn_status or taxonomy.national_conservation_status
        if not status and taxonomy.accepted_taxonomy:
            status = taxonomy.accepted_taxonomy.iucn_status
        if not status:
            return None
        return status.get_status()


def feature_geometry(record):
    site = record.site
    if not site:
        return None
    geometry = site.get_geometry()
    if geometry is None:
        return None
    return json.loads(geometry.geojson)


def feature_from_record(record, properties=None):
    """Return a GeoJSON Feature dict for a BiologicalCollectionRecord.

    The feature id is the record uuid.

    :param properties: the serialized properties, if already known. See
        features_from_records().
    """
    if properties is None:
        properties = OgcFeaturePropertiesSerializer(record).data
    return {
        'type': 'Feature',
        'id': str(record.uuid) if record.uuid else record.pk,
        'geometry': feature_geometry(record),
        'properties': properties,
    }


def features_from_records(records):
    """Return a GeoJSON Feature dict for each record, lazily.

    Serializing the properties of all records together builds the
    serializer fields once instead of once per record, which is much
    faster for a page of features.
    """
    records = list(records)
    properties = OgcFeaturePropertiesSerializer(records, many=True).data
    for record, props in zip(records, properties):
        yield feature_from_record(record, props)


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
