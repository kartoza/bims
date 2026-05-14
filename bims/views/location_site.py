import json
from django.contrib.gis.geos import GEOSGeometry
from datetime import datetime
import pytz
from django.contrib.sites.models import Site
from django.db import IntegrityError
from django_tenants.test.client import TenantClient
from django_tenants.utils import get_tenant
from preferences import preferences

from bims.models.taxon_group import TaxonGroup
from django.db.models import Q

from bims.models.biological_collection_record import BiologicalCollectionRecord
from rest_framework.test import APIClient

from bims.permissions.api_permission import user_has_permission_to_validate
from django.views.generic import TemplateView, View, ListView
from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from django.contrib.auth import get_user_model
from django.http import Http404, HttpResponseRedirect
from django.contrib import messages
from django.shortcuts import reverse
from django.contrib.gis.geos import Point
from django.contrib.auth.mixins import UserPassesTestMixin, LoginRequiredMixin
from django.shortcuts import get_object_or_404

from bims.utils.get_key import get_key
from bims.enums.geomorphological_zone import (
    GEOMORPHOLOGICAL_ZONE_CATEGORY_ORDER
)
from bims.models import (
    LocationSite, LocationType, LocationContext, LocationContextGroup,
    SiteImage, Survey, ChemicalRecord, BaseMapLayer, SITE_KEY, NonBiodiversityLayer
)
from sass.models import River
from bims.utils.jsonify import json_loads_byteified
from bims.serializers.survey_serializer import SurveySerializer
from bims.enums.ecosystem_type import HYDROGEOMORPHIC_CHOICES


def handle_location_site_post_data(
        post_data: dict,
        collector: get_user_model(),
        location_site: LocationSite = None) -> LocationSite or None:
    """
    Handle post request to create or update a location site
    :param post_data: data from POST request
    :param collector: user object
    :param location_site: If none then create a new one
    :return: new or updated location site
    """
    owner = post_data.get('owner', '').strip()
    if not owner:
        owner = None
    latitude = post_data.get('latitude', None)
    longitude = post_data.get('longitude', None)
    legacy_site_code = post_data.get('user_site_code', '')
    if not legacy_site_code:
        legacy_site_code = post_data.get('legacy_site_code', '')
    additional_data = post_data.get('additional_data', None)
    date = post_data.get('date', datetime.now())
    site_geometry = post_data.get('site-geometry', None)
    wetland_id = post_data.get('wetland_id', None)
    ecosystem_type = post_data.get('ecosystem_type', '').capitalize()

    wetland_name = post_data.get('wetland_name', '')
    user_wetland_name = post_data.get('user_wetland_name', '')
    hydrogeomorphic_type = post_data.get('hydrogeomorphic_type', '')
    user_hydrogeomorphic_type = post_data.get('user_hydrogeomorphic_type', '')

    if site_geometry and 'geometry' in site_geometry:
        try:
            site_geometry_data = json.loads(site_geometry)
            site_geometry = GEOSGeometry(json.dumps(site_geometry_data['geometry']))
        except json.decoder.JSONDecodeError:
            site_geometry = None
    else:
        site_geometry = None

    if date and not isinstance(date, datetime):
        if isinstance(date, str):
            timestamp = int(date)
        else:
            timestamp = date
        date = datetime.fromtimestamp(
            timestamp,
            pytz.UTC
        )

    refined_geomorphological_zone = post_data.get(
        'refined_geomorphological_zone',
        None
    )
    river_name = post_data.get('river_name', None)
    user_river_name = post_data.get('user_river_name', '')
    if not river_name and ecosystem_type.lower() == 'river':
        river_name = user_river_name

    site_code = post_data.get('site_code', None)
    site_description = post_data.get('site_description', '')
    site_name = post_data.get('site_name', '')
    catchment_geocontext = post_data.get('catchment_geocontext', None)
    geomorphological_group_geocontext = post_data.get(
        'geomorphological_group_geocontext',
        None
    )

    if not latitude or not longitude or not site_code:
        raise Http404()

    latitude = float(latitude)
    longitude = float(longitude)

    if owner:
        try:
            owner = get_user_model().objects.get(
                id=owner
            )
        except (get_user_model().DoesNotExist, ValueError):
            raise Http404('User does not exist')
    else:
        owner = collector

    geometry_point = Point(longitude, latitude)
    if not site_geometry:
        location_type, status = LocationType.objects.get_or_create(
            name='PointObservation',
            allowed_geometry='POINT'
        )
    else:
        location_type, status = LocationType.objects.get_or_create(
            name='MultipolygonObservation',
            allowed_geometry='MULTIPOLYGON'
        )
    post_dict = {
        'name': site_code if not site_name else site_name,
        'owner': owner,
        'latitude': latitude,
        'longitude': longitude,
        'site_description': site_description if site_description else '',
        'geometry_point': geometry_point,
        'location_type': location_type,
        'site_code': site_code,
        'legacy_river_name': user_river_name,
        'legacy_site_code': legacy_site_code,
        'date_created': date,
        'ecosystem_type': ecosystem_type,
        'wetland_name': wetland_name,
        'user_wetland_name': user_wetland_name,
        'hydrogeomorphic_type': hydrogeomorphic_type,
        'user_hydrogeomorphic_type': user_hydrogeomorphic_type
    }
    if site_geometry:
        post_dict['geometry_multipolygon'] = site_geometry
    if wetland_id:
        post_dict['wetland_id'] = wetland_id

    if river_name:
        river, river_created = River.objects.get_or_create(
            name=river_name,
            owner=owner
        )
        post_dict['river_id'] = river.id

    if not location_site:
        try:
            location_site = LocationSite.objects.create(**post_dict)
        except IntegrityError:
            sites = LocationSite.objects.filter(**post_dict)
            if sites.exists():
                location_site = sites.first()
            else:
                return None
    else:
        for key in post_dict:
            setattr(location_site, key, post_dict[key])

    # Flag to indicate new geomorphological data has been
    # fetched from geocontext
    geomorphological_fetched = False
    if geomorphological_group_geocontext:
        geomorphological_data = json_loads_byteified(
            geomorphological_group_geocontext
        )
        if 'properties' in geomorphological_data:
            geomorphological_data = geomorphological_data['properties']
        for registry in geomorphological_data['services']:
            if 'key' in registry and 'name' in registry:
                if registry['value']:
                    group, group_created = (
                        LocationContextGroup.objects.get_or_create(
                            key=registry['key'],
                            name=registry['name'],
                            geocontext_group_key=
                            geomorphological_data['key']
                        )
                    )
                    LocationContext.objects.get_or_create(
                        site=location_site,
                        value=registry['value'],
                        group=group
                    )
                    geomorphological_fetched = True
            else:
                LocationContext.objects.filter(
                    site=location_site,
                    group__geocontext_group_key=geomorphological_data[
                        'key']
                ).delete()

    try:
        if catchment_geocontext:
            if isinstance(catchment_geocontext, dict):
                catchment_geocontext = json.dumps(catchment_geocontext)
            catchment_data = json_loads_byteified(
                catchment_geocontext
            )
            if 'properties' in catchment_data:
                catchment_data = catchment_data['properties']
            if 'services' in catchment_data:
                for registry in catchment_data['services']:
                    if not registry['value']:
                        continue
                    group, group_created = (
                        LocationContextGroup.objects.get_or_create(
                            key=registry['key'],
                            name=registry['name'],
                            geocontext_group_key=catchment_data['key']
                        )
                    )
                    LocationContext.objects.get_or_create(
                        site=location_site,
                        value=registry['value'],
                        group=group
                    )
    except TypeError:
        pass

    if refined_geomorphological_zone:
        location_site.refined_geomorphological = (
            refined_geomorphological_zone
        )
    else:
        if location_site.refined_geomorphological:
            location_site.refined_geomorphological = ''

    geo_class = LocationContext.objects.filter(
        site=location_site,
        group__key='geo_class_recoded'
    )
    if not location_site.creator:
        location_site.creator = collector
    # Set original_geomorphological
    if geo_class.exists():
        if geomorphological_fetched:
            location_site.original_geomorphological = (
                geo_class[0].value
            )
    else:
        if not location_site.original_geomorphological:
            location_site.original_geomorphological = (
                refined_geomorphological_zone
            )

    if additional_data:
        try:
            additional_data = json.loads(additional_data)
            if not location_site.additional_data:
                location_site.additional_data = additional_data
            else:
                for key in additional_data:
                    location_site.additional_data[key] = additional_data[key]
        except json.decoder.JSONDecodeError:
            pass

    location_site.save()

    return location_site


class LocationSiteFormView(TemplateView):
    template_name = 'location_site_form_view.html'
    success_message = 'New site has been successfully added'
    location_site = None
    ecosystem_type = None

    def additional_context_data(self):
        return {
            'username': self.request.user.username,
            'user_id': self.request.user.id
        }

    def check_site_images(self, location_site):
        site_image_file = self.request.FILES.get('site-image', None)
        if location_site:
            if site_image_file:
                SiteImage.objects.get_or_create(
                    site=location_site,
                    image=site_image_file,
                    form_uploader=SITE_KEY
                )
        site_image_to_delete = self.request.POST.get(
            'id_site_image_delete', None)
        if site_image_to_delete:
            try:
                SiteImage.objects.get(id=site_image_to_delete).delete()
            except SiteImage.DoesNotExist:
                pass

    def get_context_data(self, **kwargs):
        context = super(LocationSiteFormView, self).get_context_data(**kwargs)
        context['geoserver_public_location'] = get_key(
            'GEOSERVER_PUBLIC_LOCATION')
        context['geomorphological_zone_category'] = [
            (g.name, g.value) for g in GEOMORPHOLOGICAL_ZONE_CATEGORY_ORDER
        ]
        context['hydrogeomorphic_type_category'] = HYDROGEOMORPHIC_CHOICES
        if not self.ecosystem_type:
            self.ecosystem_type = self.request.GET.get('type', '')
        context['ecosystem_type'] = self.ecosystem_type
        try:
            context['bing_key'] = (
                BaseMapLayer.objects.get(source_type='bing').key
            )
        except BaseMapLayer.DoesNotExist:
            context['bing_key'] = ''

        if self.request.user.get_full_name():
            context['fullname'] = self.request.user.get_full_name()
        else:
            context['fullname'] = self.request.user.username
        context['taxon_group'] = TaxonGroup.objects.filter(
            category='SPECIES_MODULE'
        ).distinct()
        context['user_id'] = self.request.user.id

        if preferences.SiteSetting.park_layer:
            park_layer = preferences.SiteSetting.park_layer
            context['park_layer_tiles'] = (
                park_layer.absolute_pmtiles_url(self.request)
            ).replace('pmtiles://', '')
            non_biodiversity_layer = NonBiodiversityLayer.objects.filter(
                native_layer=park_layer
            )
            if non_biodiversity_layer.exists():
                context['park_layer_style'] = (
                    json.dumps(non_biodiversity_layer.first().native_layer_style.style)
                )
            else:
                context['park_layer_style'] = (
                    json.dumps(park_layer.default_style.style)
                )

        context.update(self.additional_context_data())
        return context

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
        return super(LocationSiteFormView, self).get(request, *args, **kwargs)

    @method_decorator(login_required)
    def post(self, request, *args, **kwargs):
        location_site = handle_location_site_post_data(
            request.POST.dict(),
            self.request.user,
            self.location_site
        )

        if not location_site:
            raise Http404('Error creating location site')

        self.check_site_images(location_site)

        messages.success(
            self.request,
            self.success_message,
            extra_tags='location_site_form'
        )

        client = TenantClient(get_tenant(self.request))
        http_host = self.request.META.get("HTTP_HOST")

        api_url = '/api/send-email-validation/'
        res = client.get(
            api_url,
            {'pk': location_site.pk, 'model': 'Site'},
            HTTP_HOST=http_host
        )

        return HttpResponseRedirect(
            '{url}?id={id}'.format(
                url=reverse('location-site-update-form'),
                id=location_site.id
            )
        )


class LocationSiteFormUpdateView(LocationSiteFormView):
    location_site = None
    success_message = 'Site has been successfully updated'

    def additional_context_data(self):
        context_data = dict()
        context_data['location_site_lat'] = self.location_site.latitude
        context_data['user_wetland_name'] = self.location_site.user_wetland_name
        context_data['wetland_name'] = self.location_site.wetland_name
        context_data['hydrogeomorphic_type'] = self.location_site.hydrogeomorphic_type
        context_data['user_hydrogeomorphic_type'] = self.location_site.user_hydrogeomorphic_type
        context_data['location_site_long'] = self.location_site.longitude
        context_data['site_code'] = self.location_site.site_code
        context_data['site_description'] = self.location_site.site_description
        context_data['site_name'] = self.location_site.name
        context_data['ecosystem_type'] = self.location_site.ecosystem_type
        context_data['additional_data'] = json.dumps(self.location_site.additional_data);
        context_data['refined_geo_zone'] = (
            self.location_site.refined_geomorphological
        )
        context_data['original_geo_zone'] = (
            self.location_site.original_geomorphological
        )
        context_data['site_identifier'] = (
            self.location_site.location_site_identifier
        )
        context_data['update'] = True
        context_data['allow_to_edit'] = self.allow_to_edit()
        context_data['site_id'] = self.location_site.id
        context_data['is_station'] = bool(
            self.location_site.location_type and
            self.location_site.location_type.name == 'Weather Station'
        )
        context_data['legacy_site_code'] = self.location_site.legacy_site_code
        context_data['legacy_river_name'] = (
            self.location_site.legacy_river_name
        )
        context_data['site_image'] = SiteImage.objects.filter(
            site=self.location_site
        )

        if (
                ChemicalRecord.objects.filter(
                    survey__site=self.location_site
                ).exists()):
            context_data['surveys'] = SurveySerializer(
                Survey.objects.filter(
                    site_id=self.location_site,
                    chemical_collection_record__isnull=False
                ).distinct().order_by('date'), many=True
            ).data

        if self.location_site.owner:
            context_data['fullname'] = self.location_site.owner.get_full_name()
            context_data['user_id'] = self.location_site.owner.id
        elif self.location_site.creator:
            context_data['fullname'] = (
                self.location_site.creator.get_full_name()
            )
            context_data['user_id'] = self.location_site.creator.id
        else:
            context_data['fullname'] = ''
            context_data['user_id'] = ''
        if self.location_site.river:
            context_data['river_name'] = self.location_site.river.name
        return context_data

    def allow_to_edit(self):
        """Check if user is allowed to update the data"""
        if self.request.user.is_superuser:
            return True
        if self.location_site.owner:
            if self.request.user.id == self.location_site.owner.id:
                return True
        if self.location_site.creator:
            if self.request.user.id == self.location_site.creator.id:
                return True
        return False

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
        location_site_id = self.request.GET.get('id', None)
        if not location_site_id:
            raise Http404('Need location site id')
        try:
            self.location_site = LocationSite.objects.get(id=location_site_id)
        except LocationSite.DoesNotExist:
            raise Http404('Location site does not exist')
        if self.location_site.ecosystem_type == 'Wetland':
            self.template_name = 'wetland_site_form.html'

        return super(LocationSiteFormUpdateView, self).get(
            request, *args, **kwargs)

    @method_decorator(login_required)
    def post(self, request, *args, **kwargs):
        # Check if user is the creator of the site or superuser
        location_site_id = self.request.POST.get('id', None)
        if not location_site_id:
            raise Http404('Need location site id')
        try:
            self.location_site = LocationSite.objects.get(id=location_site_id)
        except LocationSite.DoesNotExist:
            raise Http404('Location site does not exist')
        if not self.allow_to_edit():
            raise Http404()
        return super(LocationSiteFormUpdateView, self).post(
            request, *args, **kwargs
        )


class LocationSiteFormDeleteView(UserPassesTestMixin, View):
    location_site = None

    def test_func(self):
        if self.request.user.is_anonymous:
            return False
        if self.request.user.is_superuser:
            return True
        location_site_id = self.kwargs.get('site_id', None)
        if not location_site_id:
            return False
        try:
            self.location_site = LocationSite.objects.get(
                id=location_site_id
            )
        except LocationSite.DoesNotExist:
            return False
        if self.location_site.creator:
            if self.request.user.id == self.location_site.creator.id:
                return True
        if self.location_site.owner:
            if self.request.user.id == self.location_site.owner.id:
                return True
        return False

    @method_decorator(login_required)
    def post(self, request, *args, **kwargs):
        location_site = get_object_or_404(
            LocationSite,
            id=self.kwargs.get('site_id', None)
        )
        is_wetland = (
            location_site.ecosystem_type and
            location_site.ecosystem_type.lower() == 'wetland'
        )
        location_site.delete()
        messages.success(
            self.request,
            'Location site successfully deleted',
            extra_tags='location_site_form'
        )
        if is_wetland:
            redirect_url = reverse('add-wetland-site')
        else:
            redirect_url = reverse('location-site-form')

        return HttpResponseRedirect(
            redirect_url
        )


class NonValidatedSiteView(
    UserPassesTestMixin,
    LoginRequiredMixin,
    ListView):
    model = LocationSite
    context_object_name = 'location_sites'
    template_name = 'non_validated_location_site.html'
    paginate_by = 10

    def test_func(self):
        return user_has_permission_to_validate(self.request.user)

    def handle_no_permission(self):
        messages.error(self.request, 'You don\'t have permission '
                                     'to validate location site')
        return super(NonValidatedSiteView, self).handle_no_permission()

    def get_context_data(self, **kwargs):
        filter_owner = self.request.GET.get('owner', None)
        filter_site_code = self.request.GET.get('site_code', None)
        filter_river_name = self.request.GET.get('river_name', None)
        context = super(
            NonValidatedSiteView, self).get_context_data(**kwargs)
        context['total'] = self.get_queryset().count()
        context['custom_url'] = ''
        if filter_site_code:
            context['custom_url'] = '&site_code={}'.format(filter_site_code)
        elif filter_owner:
            context['custom_url'] = '&owner={}'.format(filter_owner)
        if filter_river_name:
            '&river_name={}'.format(filter_river_name)
        return context

    def get_queryset(self):
        filter_owner = self.request.GET.get('owner', None)
        filter_site_code = self.request.GET.get('site_code', None)
        filter_river_name = self.request.GET.get('river_name', None)
        filter_pk = self.request.GET.get('pk', None)
        if self.queryset is None:
            gbif_site = BiologicalCollectionRecord.objects.exclude(
                source_collection=preferences.SiteSetting.default_data_source
            ).filter(
                owner_id__isnull=True
            ).values('site_id')
            queryset = LocationSite.objects.filter(
                validated=False,
                owner_id__isnull=False,
            ).exclude(
                Q(pk__in=gbif_site) |
                Q(owner__username__icontains='_vm')
            ).order_by('site_code')
            if filter_pk is not None:
                queryset = queryset.filter(pk=filter_pk)
            if filter_site_code is not None:
                queryset = queryset.filter(
                    site_code=filter_site_code)
            if filter_owner is not None:
                queryset = queryset.filter(
                    Q(owner__first_name__icontains=filter_owner.split(' ')[0]) |
                    Q(owner__last_name__icontains=filter_owner.split(' ')[-1])
                )
            if filter_river_name is not None:
                queryset = queryset.filter(
                    river__name__icontains=filter_river_name)
            return queryset
        return self.queryset


class SiteLocationDetailView(NonValidatedSiteView):
    location_site = None
    template_name = 'location_site_detail.html'
    model = LocationSite

    @method_decorator(login_required)
    def get(self, request, locationsiteid, *args, **kwargs):
        location_site_id = locationsiteid
        if not location_site_id:
            raise Http404('Need location site id')
        try:
            self.location_site = LocationSite.objects.get(id=location_site_id)
        except LocationSite.DoesNotExist:
            raise Http404('Location site does not exist')
        return super(NonValidatedSiteView, self).get(
            request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super(SiteLocationDetailView, self).get_context_data(**kwargs)
        context['location_site'] = self.location_site
        context['geoserver_public_location'] = get_key(
            'GEOSERVER_PUBLIC_LOCATION')
        context['site_image'] = SiteImage.objects.filter(
            site=self.location_site
        )
        if self.location_site.owner:
            context['fullname'] = self.location_site.owner.get_full_name()
        if (
                ChemicalRecord.objects.filter(
                    survey__site=self.location_site
                ).exists()):
            context['surveys'] = SurveySerializer(
                Survey.objects.filter(
                    site_id=self.location_site,
                    chemical_collection_record__isnull=False
                ).distinct().order_by('date'), many=True
            ).data

        return context
