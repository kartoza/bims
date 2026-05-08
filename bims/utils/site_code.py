import hashlib
import json
import logging
import random
import string
from typing import Dict

import requests
import re

logger = logging.getLogger(__name__)

from bims.models import LocationContextGroup, LocationContext
from bims.models.location_site import LocationSite
from requests.exceptions import HTTPError
from bims.utils.get_key import get_key
from cloud_native_gis.models import Layer
from cloud_native_gis.utils.geometry import query_features
from sass.models import River

FBIS_CATCHMENT_KEY = 'river_catchment_areas_group'
SANPARK_PARK_KEY = 'sanparks and mpas'
QUATERNARY_CATCHMENT_LAYER = 'quaternary catchment'


def _fetch_catchments_data(catchment_key, lon, lat):
    """
    Fetch catchments data from geocontext
    :param catchment_key: catchment key in geocontext
    :param lon: longitude
    :param lon: latitude
    :return: list of catchment data, catchments object
    """
    catchments = {}
    catchment_data = {}
    catchment_url = (
        '{base_url}/api/v2/query?registry=group&key='
        '{catchment_key}&x={lon}&y={lat}'
    ).format(
        base_url=get_key('GEOCONTEXT_URL'),
        lon=lon,
        lat=lat,
        catchment_key=catchment_key
    )
    try:
        response = requests.get(catchment_url)
        if response.status_code == 200:
            catchment_data = json.loads(response.content)
            for _catchment in catchment_data['properties']['services']:
                catchments[_catchment['key']] = _catchment['value']
    except (HTTPError, ValueError, KeyError):
        pass
    return catchments, catchment_data


def _get_catchments_data(
        location_site=None, lat=None, lon=None, catchment_key='catchment'):
    """
    Get catchments data
    :return: list of catchment data
    """
    from bims.models.location_context import LocationContext

    catchment_key = catchment_key
    catchments = {}
    catchments_data = {}
    if not location_site:
        catchments, catchments_data = _fetch_catchments_data(
            catchment_key=catchment_key,
            lon=lon,
            lat=lat
        )
    if location_site:
        location_contexts = LocationContext.objects.filter(
            site=location_site
        )
        catchments = location_contexts.values_from_group(catchment_key)
        if not catchments:
            location_site.update_location_context_document(catchment_key)
            catchments = location_contexts.values_from_group(catchment_key)
    return catchments, catchments_data


def get_feature_data(lon, lat, context_key, layer_name, tolerance=0, location_site = None) -> str:
    layer = Layer.objects.filter(
        name__istartswith=layer_name
    ).first()

    if not layer:
        layer = Layer.objects.filter(
            name__icontains=layer_name
        ).first()

    if not layer:
        return ''

    feature_data = ''

    features = query_features(
        table_name=layer.query_table_name,
        field_names=[context_key],
        coordinates=[(lon, lat)],
        tolerance=tolerance
    )
    results = features.get('result', [])
    if results and 'feature' in results[0] and context_key in results[0]['feature']:
        feature_data = results[0]['feature'][context_key]

    if feature_data and location_site:
        location_contex_group = LocationContextGroup.objects.filter(
            geocontext_group_key=layer.unique_id
        ).first()
        if location_contex_group:
            location_context, _ = LocationContext.objects.get_or_create(
                site=location_site,
                group=location_contex_group,
                value=feature_data,
            )

    return feature_data


def generate_fbis_africa_site_code(latitude: float, longitude: float, site_name: str):
    countries = Layer.objects.filter(
        name__iexact='Political boundaries').first()
    context_key = 'ISO'
    iso_name = ''

    if countries:
        feature_data = query_features(
            table_name=countries.query_table_name,
            field_names=[context_key],
            coordinates=[(longitude, latitude)],
            tolerance=0
        )
        results = feature_data.get('result', [])
        if results and 'feature' in results[0] and context_key in results[0]['feature']:
            iso_name = results[0]['feature'][context_key]

    if not iso_name:
        cleaned_name = re.sub('[^A-Za-z]', '', site_name)
        iso_name = cleaned_name[:3]

    if not iso_name:
        return 'UNKNOWN'

    return iso_name.upper()


def kafue_site_code(lat: float, lon: float, site_name: str = ''):
    district_name = get_feature_data(
        lon=lon,
        lat=lat,
        context_key='NAME1',
        layer_name='district',
        tolerance=0
    )
    if not district_name:
        district_name = site_name

    if not district_name:
        district_name = 'UNKNOWN'

    return district_name[:5].upper()


def generate_sanparks_site_code(lat: float, lon: float, site_name: str = ''):
    """
    Generates a SANParks site code for a given location site.

    1. Tries to find whether the site's coordinates lie within a SANParks/MPA boundary.
       If so, returns that boundary's name.
    2. If not found, falls back to the first three alphabetical characters of the
       location site's name (ignoring digits, spaces, and special characters).

    :return: SANParks site code
    """
    park_name = ''
    park_group = LocationContextGroup.objects.filter(
        name__iexact=SANPARK_PARK_KEY).first()

    if park_group:
        layer = Layer.objects.filter(unique_id=park_group.key).first()
        if layer:
            context_key = park_group.layer_identifier
            feature_data = query_features(
                table_name=layer.query_table_name,
                field_names=[context_key],
                coordinates=[(lon, lat)],
                tolerance=0
            )
            results = feature_data.get('result', [])
            if results and 'feature' in results[0] and context_key in results[0]['feature']:
                park_name = results[0]['feature'][context_key]
                logger.debug(f"Found park name from GIS query: {park_name}")

    if not park_name:
        park_name = site_name
        logger.warning(f"No park boundary found. Fallback: {park_name if park_name else 'N/A'}")

    cleaned_name = re.sub('[^A-Za-z]', '', park_name)
    code = cleaned_name[:3]

    if len(code) < 3:
        lat_lon_str = f"{lat:.6f},{lon:.6f}"
        hash_bytes = hashlib.md5(lat_lon_str.encode('utf-8')).digest()
        needed = 3 - len(code)
        additional_letters = ''.join(
            chr((hash_bytes[i] % 26) + ord('A'))
            for i in range(needed)
        )
        code += additional_letters

    return code.upper()


def fbis_catchment_generator(
        location_site: LocationSite = None, lat=None, lon=None, river_name=''):
    """
    Generate catchment string for FBIS Site Code
    :param river_name: The name of the river
    :param location_site: Location site object
    :param lat: Latitude of site
    :param lon: Longitude of site
    :return: catchment string :
        2 characters from secondary catchment area
        4 characters from river name
        , catchments data in dictionary
    """
    catchments_data = {}
    secondary_catchment_name = get_feature_data(
        lon=lon,
        lat=lat,
        context_key='name',
        layer_name='secondary catchment',
        tolerance=1
    )

    catchment_code = secondary_catchment_name[:2].upper()

    if location_site and not river_name:
        if location_site.legacy_river_name:
            river_name = location_site.legacy_river_name
        elif location_site.river:
            river_name = location_site.river.name
        else:
            river_name = get_feature_data(
                lon=location_site.geometry_point[0],
                lat=location_site.geometry_point[1],
                context_key='river_name',
                layer_name='river',
                tolerance=10
            )

    # Search river name by coordinates
    if not river_name and lat and lon:
        river_name = get_feature_data(
            lon=lon,
            lat=lat,
            context_key='river_name',
            layer_name='river',
            tolerance=10
        )

    if not river_name:
        river_name = 'UNKNOWN'

    if river_name:
        river_name = river_name.replace(' ', '')
        catchment_code += river_name[:4].upper()

    if location_site and river_name:
        if not location_site.river:
            try:
                river, _ = River.objects.get_or_create(
                    name=river_name
                )
            except River.MultipleObjectsReturned:
                river = River.objects.filter(
                    name=river_name
                ).first()
            location_site.river = river
            location_site.save()

    return catchment_code, catchments_data


def rbis_catchment_generator(location_site=None, lat=None, lon=None):
    """
    Generate catchment string for RBIS Site Code
    :param location_site: Location site object
    :param lat: Latitude of site
    :param lon: Longitude of site
    :return: catchment string :
        1 character from catchment level_0 +
        2 characters from catchment level_1 +
        1 characters from catchment level_2
        , catchments data
    """
    catchment_key = 'rwanda_catchments'
    level_2 = ''
    district_id = ''
    catchments, catchments_data = _get_catchments_data(
        location_site=location_site,
        lat=lat,
        lon=lon,
        catchment_key=catchment_key
    )
    for key, value in catchments.items():
        if value:
            if 'level_2' in key and not level_2:
                level_2 = value[:6].upper()
                level_2 = level_2.replace('_', '')

    # Get district id
    geocontext_key = 'rwanda_district_id'
    district_data, district_geocontext_data = _get_catchments_data(
        location_site=location_site,
        lat=lat,
        lon=lon,
        catchment_key=geocontext_key
    )
    if district_data and 'districts_id' in district_data:
        district_id = district_data['districts_id']

    return level_2 + district_id, catchments_data


def wetland_catchment(lat, lon, wetland_data: Dict, user_wetland_name: str) -> str:
    """
    Generates a catchment code for a given wetland based on location and data.

    :param user_wetland_name: Wetland name from user
    :param lat: Latitude of the location site
    :param lon: Longitude of the location site
    :param wetland_data: A dictionary containing data about the wetland layer.
    :return: The generated catchment code, e.g. L112-NAME
    """
    wetland_site_code = ''
    quaternary_catchment_area_key = 'quaternary_catchment_area'
    quaternary_geocontext_key = 'quaternary_catchment_group'
    context_key = 'name'
    quaternary_catchment = ''

    layer = Layer.objects.filter(
        name__icontains=QUATERNARY_CATCHMENT_LAYER
    ).first()

    if layer:
        features = query_features(
            table_name=layer.query_table_name,
            field_names=[context_key],
            coordinates=[(lon, lat)],
            tolerance=0.01
        )
        results = features.get('result', [])
        if results and 'feature' in results[0] and context_key in results[0]['feature']:
            quaternary_catchment = results[0]['feature'][context_key]

    if not quaternary_catchment and not layer:
        catchments, catchments_data = _get_catchments_data(
            lat=lat,
            lon=lon,
            catchment_key=quaternary_geocontext_key
        )
        if quaternary_catchment_area_key in catchments and catchments[quaternary_catchment_area_key]:
            quaternary_catchment = catchments[quaternary_catchment_area_key]

    wetland_site_code += quaternary_catchment + '-'

    if wetland_data and 'name' in wetland_data and wetland_data['name']:
        wetland_site_code += wetland_data['name'].replace(' ', '')[:4]
    elif user_wetland_name:
        wetland_site_code += user_wetland_name.replace(' ', '')[:4]

    return wetland_data, wetland_site_code


def fips_generator(lat: float, lon: float) -> str:
    """
    Generate site code prefix for FIPS using up to four spatial layers:

      [2-char GBIF continent code]
      [up to 6-char basin short name]
      [up to 4-char sub-basin short name]
      [HydroBASIN ID]
    """
    from bims import conf as bims_conf

    cont_code = get_feature_data(
        lon=lon,
        lat=lat,
        context_key=bims_conf.FIPS_GBIF_CONTINENT_FIELD,
        layer_name=bims_conf.FIPS_GBIF_CONTINENT_LAYER,
        tolerance=0.01,
    )
    cont_code = re.sub('[^A-Za-z]', '', cont_code or '')[:2].upper() or 'XX'

    basin_code = ''
    if bims_conf.FIPS_BASIN_LAYER:
        basin_name = get_feature_data(
            lon=lon,
            lat=lat,
            context_key=bims_conf.FIPS_BASIN_FIELD,
            layer_name=bims_conf.FIPS_BASIN_LAYER,
            tolerance=0.01,
        )
        if basin_name:
            basin_code = re.sub('[^A-Za-z0-9]', '', basin_name)[:3].upper()

    subbasin_code = ''
    if bims_conf.FIPS_SUBBASIN_LAYER:
        subbasin_name = get_feature_data(
            lon=lon,
            lat=lat,
            context_key=bims_conf.FIPS_SUBBASIN_FIELD,
            layer_name=bims_conf.FIPS_SUBBASIN_LAYER,
            tolerance=0.01,
        )
        if subbasin_name:
            subbasin_code = re.sub('[^A-Za-z0-9]', '', subbasin_name)[:3].upper()

    hydrobasin_code = ''
    if bims_conf.FIPS_HYDROBASIN_LAYER:
        hydrobasin_id = get_feature_data(
            lon=lon,
            lat=lat,
            context_key=bims_conf.FIPS_HYDROBASIN_FIELD,
            layer_name=bims_conf.FIPS_HYDROBASIN_LAYER,
            tolerance=0.01,
        )
        if hydrobasin_id:
            hydrobasin_code = re.sub('[^A-Za-z0-9]', '', str(hydrobasin_id)).upper()

    return cont_code + basin_code + subbasin_code + hydrobasin_code


def open_waterbody_catchment(lat, lon, user_open_waterbody_name: str) -> str:
    """
    Generates a catchment code for a given open waterbody based on location and data.

    :param user_open_waterbody_name: Open waterbody name from user
    :param lat: Latitude of the location site
    :param lon: Longitude of the location site
    :return: The generated catchment code, e.g. L112-NAME
    """
    site_code = ''
    quaternary_catchment_area_key = 'quaternary_catchment_area'
    quaternary_geocontext_key = 'quaternary_catchment_group'

    catchments, catchments_data = _get_catchments_data(
        lat=lat,
        lon=lon,
        catchment_key=quaternary_geocontext_key
    )

    try:
        if quaternary_catchment_area_key in catchments:
            site_code += catchments[quaternary_catchment_area_key]
        site_code += '-'
    except TypeError:
        pass

    if user_open_waterbody_name:
        site_code += user_open_waterbody_name.replace(' ', '')[:4]

    return site_code
