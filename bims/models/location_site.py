# coding=utf-8
"""Site model definition.

"""

import logging

import requests
import json
from preferences import preferences

from bims.tasks.location_site import update_location_context
from bims.models.validation import AbstractValidation
from django.core.exceptions import ValidationError
from django.contrib.gis.db import models
from django.dispatch import receiver
from django.db.models import JSONField
from django.conf import settings
from django.utils import timezone

from bims.models.location_type import LocationType
from bims.utils.get_key import get_key
from bims.enums.geomorphological_zone import GeomorphologicalZoneCategory
from bims.models.location_context import LocationContext
from bims.models.location_context_group import LocationContextGroup
from bims.utils.decorator import prevent_recursion
from bims.enums.ecosystem_type import (
    ECOSYSTEM_TYPE_CHOICES, HYDROGEOMORPHIC_NONE, HYDROGEOMORPHIC_CHOICES
)
from bims.utils.uuid import is_uuid
from cloud_native_gis.utils.geometry import query_features

LOGGER = logging.getLogger(__name__)

ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
BASE = len(ALPHABET)


class LocationSite(AbstractValidation):
    """Location Site model."""

    __original_centroid = None
    geocontext_url_format = '{geocontext_url}/api/v2/query?registry=collection&key={geocontext_collection_key}&' \
                            'x={longitude}&y={latitude}&outformat=json'

    name = models.CharField(
        max_length=300,
        blank=False,
    )
    site_description = models.CharField(
        max_length=500,
        blank=True,
        default=''
    )
    site_code = models.CharField(
        max_length=100,
        blank=True,
        default=''
    )
    legacy_site_code = models.CharField(
        max_length=100,
        blank=True,
        default=''
    )
    location_type = models.ForeignKey(
        LocationType,
        models.CASCADE,
        null=False
    )
    refined_geomorphological = models.CharField(
        blank=True,
        null=True,
        max_length=200,
        verbose_name='User geomorphological',
        choices=[(g.value, g.value) for g in GeomorphologicalZoneCategory],
        help_text='Would be used in preference to the one discovered '
                  'in geocontext',
    )
    geometry_point = models.PointField(
        null=True,
        blank=True,
    )
    geometry_line = models.LineStringField(
        null=True,
        blank=True,
    )
    geometry_polygon = models.PolygonField(
        null=True,
        blank=True,
    )
    geometry_multipolygon = models.MultiPolygonField(
        null=True,
        blank=True,
    )
    map_reference = models.CharField(
        null=True,
        blank=True,
        max_length=200
    )
    land_owner_detail = models.TextField(
        null=True,
        blank=True,
    )
    river = models.ForeignKey(
        'sass.River',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )
    legacy_river_name = models.CharField(
        max_length=100,
        blank=True
    )

    location_context_document = JSONField(
        verbose_name='Document for location context as JSON.',
        help_text='This document is generated from GeoContext by using '
                  'management command or changing the geometry.',
        null=True,
        blank=True
    )

    location_context = JSONField(
        verbose_name='Formatted location_context_document',
        help_text='This is intended for filtering',
        null=True,
        blank=True
    )

    additional_data = JSONField(
        verbose_name='Additional json data',
        null=True,
        blank=True
    )

    latitude = models.FloatField(
        blank=True,
        help_text='Latitude of location site',
        null=True
    )

    longitude = models.FloatField(
        blank=True,
        help_text='Longitude of location site',
        null=True
    )
    original_geomorphological = models.CharField(
        blank=True,
        null=True,
        max_length=200,
        help_text='Original geomorphological zone from spatial layer'
    )

    creator = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        models.SET_NULL,
        related_name='site_creator',
        help_text='The creator of the site',
        blank=True,
        null=True,
    )

    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        models.SET_NULL,
        related_name='site_owner',
        help_text='The owner of the site',
        blank=True,
        null=True,
    )

    date_created = models.DateTimeField(
        null=True,
        blank=True,
        default=timezone.now
    )

    wetland_id = models.CharField(
        default='',
        blank=True,
        max_length=256
    )

    ecosystem_type = models.CharField(
        max_length=128,
        blank=True,
        choices=ECOSYSTEM_TYPE_CHOICES,
        default='',
        null=True
    )
    wetland_name = models.CharField(
        max_length=256,
        blank=True,
        default='',
        null=True
    )
    user_wetland_name = models.CharField(
        max_length=256,
        blank=True,
        default='',
        null=True
    )
    hydrogeomorphic_type = models.CharField(
        max_length=128,
        blank=True,
        default='',
        null=True
    )
    user_hydrogeomorphic_type = models.CharField(
        max_length=128,
        blank=True,
        null=True,
        default=HYDROGEOMORPHIC_NONE,
        choices=HYDROGEOMORPHIC_CHOICES
    )

    coordinate_precision = models.DecimalField(
        max_digits=10,
        decimal_places=6,
        blank=True,
        null=True,
        help_text='A decimal representation of the precision of the coordinates. '
                  'Examples: 0.00001 (GPS limit), 0.000278 (nearest second), '
                  '0.01667 (nearest minute), 1.0 (nearest degree)'
    )

    coordinate_uncertainty_in_meters = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        blank=True,
        null=True,
        help_text='The horizontal distance (in meters) from the given coordinates '
                  'describing the smallest circle containing the whole of the Location. '
                  'Examples: 30 (GPS after 2000-05-01), 100 (GPS before 2000-05-01)'
    )

    harvested_from_gbif = models.BooleanField(
        default=False,
        help_text='Indicates whether this location site was created from GBIF harvesting'
    )

    @property
    def location_site_identifier(self):
        """
        Returns location site code if exist, if not return site name
        """
        if self.site_code:
            return self.site_code
        if self.legacy_site_code:
            return self.legacy_site_code
        return self.name

    @property
    def data_name(self):
        return self.location_site_identifier

    def get_centroid(self):
        """ Getting centroid of location site """

        if (
            self.geometry_point
        ):
            return self.geometry_point
        else:
            if self.get_geometry():
                return self.get_geometry().centroid
            else:
                return None

    def get_geometry(self):
        """Function to get geometry."""
        geometry = None
        validation_error = ValidationError('Only one geometry allowed.')

        if not self.location_type:
            return self.geometry_point

        if (
            self.geometry_point and
            self.location_type.allowed_geometry == 'POINT'
        ):
            return self.geometry_point

        if self.geometry_polygon:
            if geometry:
                raise validation_error
            geometry = self.geometry_polygon

        if self.geometry_multipolygon:
            if geometry:
                raise validation_error
            geometry = self.geometry_multipolygon

        if self.geometry_line:
            if geometry:
                raise validation_error
            geometry = self.geometry_line

        return geometry

    def get_geocontext_group_data(self, group_key):
        LOGGER.debug('get_location_group_data for ' + group_key)
        geocontext_url = get_key('GEOCONTEXT_URL')
        if not geocontext_url:
            message = (
                'Can not update location context document because geocontext '
                'url is None. Please set it.')
            return False, message
        if not self.longitude and not self.latitude:
            message = (
                'Can not update location context document because centroid is '
                'None. Please set it.')
            return False, message
        longitude = self.longitude
        latitude = self.latitude

        geocontext_group_url_format = (
            '{geocontext_url}/api/v2/query?registry=group&key={geocontext_group_key}&'
            'x={longitude}&y={latitude}&outformat=json')
        # build url
        url = geocontext_group_url_format.format(
            geocontext_url=geocontext_url,
            geocontext_group_key=group_key,
            longitude=longitude,
            latitude=latitude,
        )
        LOGGER.info('Request url : {}'.format(url))

        try:
            r = requests.get(url, timeout=10)
            if r.status_code != 200:
                message = (
                        'Request to url %s got %s [%s], can not update location '
                        'context document.' % (url, r.status_code, r.reason))
                LOGGER.info(message)
                return None
        except requests.exceptions.Timeout:
            message = 'Request to url %s timed out.' % url
            LOGGER.info(message)
            return None
        except requests.exceptions.SSLError:
            return None

        return json.dumps(r.json())

    def add_context_group(self, group_key):
        from bims.models import LocationContext
        from cloud_native_gis.models import Layer

        context_key = ''
        if ':' in group_key:
            group_key, context_key = group_key.split(':')

        if is_uuid(group_key) and context_key:
            layer = Layer.objects.filter(unique_id=group_key).first()
            if layer:
                feature_data = query_features(
                    table_name=layer.query_table_name,
                    field_names=[context_key],
                    coordinates=[(self.longitude, self.latitude)],
                    tolerance=preferences.GeocontextSetting.tolerance
                )
                # Find by name first
                context_groups = LocationContextGroup.objects.filter(
                    key=group_key,
                    layer_identifier=context_key
                )
                if context_groups.exists():
                    context_groups.update(
                        geocontext_group_key=group_key,
                        layer_identifier=context_key,
                        key=group_key)
                    context_group = context_groups.first()
                else:
                    # Check if name already exists
                    layer_name = layer.name
                    if LocationContextGroup.objects.filter(
                        name=layer_name
                    ).exists():
                        layer_name = f'{layer.name} ({context_key})'

                    context_group, _ = LocationContextGroup.objects.get_or_create(
                        geocontext_group_key=group_key,
                        key=group_key,
                        layer_identifier=context_key,
                        defaults = {
                            'name': layer_name,
                        }
                    )

                for result in feature_data['result']:
                    if context_key in result['feature']:
                        try:
                            LocationContext.objects.update_or_create(
                                site=self,
                                group=context_group,
                                defaults={
                                    'fetch_time': timezone.now(),
                                    'value': str(result['feature'][context_key])
                                }
                            )
                        except LocationContext.MultipleObjectsReturned:
                            LocationContext.objects.filter(
                                site=self,
                                group=context_group,
                            ).delete()
                            LocationContext.objects.create(
                                site=self,
                                group=context_group,
                                fetch_time=timezone.now(),
                                value=str(result['feature'][context_key])
                            )
            return True, 'Added'

        try:
            geocontext_data_string = self.get_geocontext_group_data(group_key)
            geocontext_data = json.loads(geocontext_data_string)
            for geocontext_value in geocontext_data['services']:
                if 'value' not in geocontext_value:
                    continue
                try:
                    location_context_group, group_created = (
                        LocationContextGroup.objects.get_or_create(
                            key=geocontext_value['key'],
                            geocontext_group_key=group_key
                        )
                    )
                except LocationContextGroup.MultipleObjectsReturned:
                    _groups = LocationContextGroup.objects.filter(
                        key=geocontext_value['key'],
                        geocontext_group_key=group_key
                    ).order_by('id')
                    _first_group = _groups.first()
                    _remaining_groups = _groups[1:]
                    LocationContext.objects.filter(group__in=_groups).update(
                        group=_first_group
                    )
                    for _group in _remaining_groups:
                        _group.delete()
                    location_context_group = _first_group
                if LocationContext.objects.filter(
                        site=self,
                        group=location_context_group).exists():
                    LocationContext.objects.filter(
                        site=self,
                        group=location_context_group
                    ).delete()

                if geocontext_value.get('value') is not None:
                    LocationContext.objects.create(
                        site=self,
                        group=location_context_group,
                        fetch_time=timezone.now(),
                        value=str(geocontext_value['value'])
                    )
                if (
                        not location_context_group.name or
                        location_context_group.name == '-'
                ):
                    location_context_group.name = geocontext_value['name']
                location_context_group.key = geocontext_value['key']
                location_context_group.geocontext_group_key = (
                    geocontext_data['key']
                )
                location_context_group.save()

        except (ValueError, KeyError, TypeError):
            return False, 'Could not format the geocontext data'
        return True, 'Added'

    def clear_location_context_document(self):
        self.location_context_document = ""
        return True, "Document cleared"

    def update_location_context_document(self, group_keys=None):
        """Update location context document."""
        from bims.utils.location_context import get_location_context_data
        LOGGER.debug('location_site - update_location_context_document')

        # Update location context in background
        get_location_context_data(
            site_id=str(self.id),
            only_empty=False,
            group_keys=group_keys
        )
        return True, 'Successfully update location context document.'

    # noinspection PyClassicStyleClass
    class Meta:
        """Meta class for project."""
        app_label = 'bims'

    def __unicode__(self):
        return u'%s' % self.name

    def __str__(self):
        if self.site_code:
            return self.site_code
        return '%s' % self.name

    def validate_json_field(self, field):
        max_allowed = 10
        attempt = 0
        is_dictionary = False

        while not is_dictionary and attempt < max_allowed:
            if not field:
                break
            if isinstance(field, dict):
                is_dictionary = True
            else:
                try:
                    field = json.loads(
                        field
                    )
                except json.decoder.JSONDecodeError:
                    break
                attempt += 1
        return field

    def save(self, *args, **kwargs):
        lon_lat_changed = False

        if self.additional_data:
            self.additional_data = self.validate_json_field(
                self.additional_data
            )
        self.location_context_document = self.validate_json_field(
            self.location_context_document
        )
        self.location_context = self.validate_json_field(
            self.location_context
        )

        """Check if one of geometry is not null."""
        if self.geometry_point or self.geometry_line or \
                self.geometry_polygon or self.geometry_multipolygon:
            # Check if geometry is allowed
            if isinstance(self.get_geometry(),
                          self.location_type.get_allowed_geometry_class()):
                if (
                        self.get_centroid().x != self.__original_longitude or
                        not self.longitude
                ):
                    self.longitude = self.get_centroid().x
                    lon_lat_changed = True
                if (
                        self.get_centroid().y != self.__original_latitude or
                        not self.latitude
                ):
                    self.latitude = self.get_centroid().y
                    lon_lat_changed = True

                # Check if latitude or longitude changed manually
                if (
                        self.latitude != self.__original_latitude or
                        self.longitude != self.__original_longitude
                ):
                    if not lon_lat_changed:
                        self.geometry_point.x = self.longitude
                        self.geometry_point.y = self.latitude
                if self.location_type.allowed_geometry != 'POINT':
                    self.geometry_point = self.get_centroid()

                super(LocationSite, self).save(*args, **kwargs)
                self.__original_centroid = self.get_centroid()
            else:
                raise ValidationError('Geometry is not allowed.')
        else:
            raise ValidationError('At least one geometry need to be filled.')

    def __init__(self, *args, **kwargs):
        super(LocationSite, self).__init__(*args, **kwargs)
        try:
            self.__original_centroid = self.get_centroid()
            self.__original_refined_geomorphological = (
                self.refined_geomorphological
            )
            self.__original_latitude = self.latitude
            self.__original_longitude = self.longitude
        except LocationType.DoesNotExist:
            return


def update_location_site_context(location_site_id):
    async_result = update_location_context.delay(location_site_id)
    return async_result


@receiver(models.signals.post_save, sender=LocationSite)
@prevent_recursion
def location_site_post_save_handler(sender, instance, **kwargs):
    """
    Post save location site
    """
    if (
            not isinstance(sender, LocationSite) and
            not sender.__name__ == 'LocationSite'
    ):
        return

    # Update refined geomorphological if exist
    if instance.refined_geomorphological:
        try:
            groups = LocationContextGroup.objects.filter(
                key='geo_class_recoded'
            )
            if not groups.exists():
                return
            site_contexts = LocationContext.objects.filter(
                site=instance,
                group__in=groups
            )
            if site_contexts.exists():
                geo_context = site_contexts[0]
                if instance.refined_geomorphological != geo_context.value:
                    if not instance.original_geomorphological:
                        instance.original_geomorphological = geo_context.value
                    geo_context.value = instance.refined_geomorphological
                    geo_context.save()
            else:
                if groups.exclude(layer_name='').exists():
                    group = groups.exclude(layer_name='')[0]
                else:
                    group = groups[0]
                geo_context, _ = LocationContext.objects.get_or_create(
                    site=instance,
                    group=group,
                    value=instance.refined_geomorphological
                )
            if not instance.original_geomorphological:
                instance.original_geomorphological = geo_context.value
        except LocationContext.MultipleObjectsReturned:
            pass

    async_result = update_location_site_context(location_site_id=instance.id)

def int_to_base36(n: int, width: int = 6) -> str:
    if n < 0:
        raise ValueError("n must be non-negative")
    # special-case zero
    if n == 0:
        return "0".zfill(width)
    out = []
    while n:
        n, rem = divmod(n, BASE)
        out.append(ALPHABET[rem])
    return "".join(reversed(out)).zfill(width)

def next_site_code(project_name: str, site_prefix: str, width: int = 6) -> str:
    """
    Generate the next unique site code for the given prefix.
    Uses decimal for every prefix except 'fbis_africa', which is base-36.
    """
    import re as _re
    from django.db.models import Max, IntegerField
    from django.db.models.functions import Cast, Substr

    if project_name == "fbis_africa":
        serial = (
            LocationSite.objects
            .filter(site_code__startswith=site_prefix)
            .count()
        ) + 1
    else:
        result = (
            LocationSite.objects
            .filter(site_code__regex=rf'^{_re.escape(site_prefix)}\d+$')
            .annotate(
                serial_num=Cast(
                    Substr('site_code', len(site_prefix) + 1),
                    output_field=IntegerField()
                )
            )
            .aggregate(max_serial=Max('serial_num'))
        )
        serial = (result['max_serial'] or 0) + 1

    while True:
        if project_name == "fbis_africa":
            counter = int_to_base36(serial, width)
        else:
            counter = str(serial).zfill(width)

        candidate = f"{site_prefix}{counter}"

        if not LocationSite.objects.filter(site_code=candidate).exists():
            return candidate

        serial += 1

def generate_site_code(
        location_site=None,
        lat=None,
        lon=None,
        river_name='',
        ecosystem_type='',
        wetland_name='',
        **kwargs):
    """Generate site code"""
    from bims.utils.site_code import (
        fbis_catchment_generator,
        rbis_catchment_generator,
        wetland_catchment,
        open_waterbody_catchment,
        generate_sanparks_site_code,
        generate_fbis_africa_site_code,
        kafue_site_code,
        fips_generator,
    )
    from preferences import preferences
    site_code = ''
    catchments_data = {}
    site_code_generator = preferences.SiteSetting.site_code_generator
    site_count_width = 5

    site_name = kwargs.get('site_name', '')
    if not site_name or site_name == 'undefined':
        site_name = kwargs.get('site_desc', '')
    if site_name == 'undefined':
        site_name = ''

    if site_code_generator == 'fbis':
        if ecosystem_type.lower() == 'wetland':
            wetland_data = location_site.additional_data if (
                location_site and
                location_site.additional_data and
                'wetlid' in location_site.additional_data
            ) else {}
            new_wetland_data, site_code = wetland_catchment(lat, lon, wetland_data, wetland_name)
            catchments_data = new_wetland_data
        elif ecosystem_type.lower() == 'open waterbody':
            site_code = open_waterbody_catchment(
                lat, lon, river_name
            )
        else:
            site_code, catchments_data = fbis_catchment_generator(
                location_site=location_site,
                lat=lat,
                lon=lon,
                river_name=river_name
            )
    elif site_code_generator == 'rbis':
        site_code, catchments_data = rbis_catchment_generator(
            location_site=location_site,
            lat=lat,
            lon=lon
        )
    elif site_code_generator == 'sanparks':
        site_code = generate_sanparks_site_code(
            lat=float(lat),
            lon=float(lon),
            site_name=site_name
        )
    elif site_code_generator == 'fbis_africa':
        site_count_width = 6
        site_code = generate_fbis_africa_site_code(
            latitude=float(lat),
            longitude=float(lon),
            site_name=site_name
        )
    elif site_code_generator == 'kafue':
        site_code = kafue_site_code(
            lat=float(lat),
            lon=float(lon),
            site_name=site_name
        )
    elif site_code_generator == 'fips':
        site_code = fips_generator(
            lat=float(lat),
            lon=float(lon),
        )
    else:
        site_name = kwargs.get('site_name', '')
        site_description = kwargs.get('site_desc', '')
        site_name_length = 2
        if site_code_generator in ['bims'] and (site_name or site_description):
            site_code = site_name[:site_name_length].upper()
            if site_code_generator == 'bims':
                site_code += site_description[:2].upper()
        elif location_site:
            site_code += location_site.name[:site_name_length].upper()
            site_code += location_site.site_description[:4].upper()

    # Add hyphen
    site_code += '-'

    site_code_full = next_site_code(
        project_name=site_code_generator, site_prefix=site_code, width=site_count_width)

    return site_code_full, catchments_data
