# coding=utf-8
import csv
import logging
import re
from datetime import timedelta
from datetime import date
import json

from django.contrib.sites.models import Site
from django.http import HttpResponse, HttpResponseRedirect
from django.conf import settings
from django.utils.timezone import localtime
from django.db import connection
from django_tenants.utils import schema_context, get_public_schema_name
from rangefilter.filters import DateRangeFilterBuilder
from preferences.admin import PreferencesAdmin
from preferences import preferences

from django.contrib.admin import SimpleListFilter
from django import forms
from django.utils.safestring import mark_safe
from django.contrib.gis import admin
from django.contrib import admin as django_admin
from django.contrib import messages
from django.core.mail import send_mail
from django.utils.translation import gettext_lazy as _
from django.contrib.auth.models import Permission
from django.contrib.flatpages.admin import FlatPageAdmin
from django.contrib.flatpages.models import FlatPage
from django.db import models
from django.db.models import Q, Count, F, Case, When, IntegerField
from django.utils.html import format_html
from django.contrib.auth import get_user_model
from django.urls import reverse, path
from django.contrib.auth.forms import UserCreationForm

from django_json_widget.widgets import JSONEditorWidget
from taggit.admin import TagAdmin
from taggit.models import Tag

from bims.admins.custom_ckeditor_admin import DynamicCKEditorUploadingWidget, CustomCKEditorAdmin
from bims.admins.site_setting import SiteSettingAdmin
import bims.admin_site
import bims.admins.gbif_admin
import bims.admins.licence
from bims.api_views.taxon_update import create_taxon_proposal
from bims.enums import TaxonomicGroupCategory, TaxonomicStatus, TaxonomicRank
from bims.models.harvest_schedule import HarvestPeriod
from bims.models.record_type import merge_record_types
from bims.tasks import fetch_vernacular_names
from bims.tasks.taxa import fetch_iucn_status as fetch_iucn_status_task
from bims.utils.endemism import merge_endemism
from bims.utils.sampling_method import merge_sampling_method
from bims.tasks.cites_info import fetch_and_save_cites_listing
from bims.helpers.list import chunk_list
from bims.utils.fetch_gbif import harvest_synonyms_for_accepted_taxonomy
from geonode.documents.admin import DocumentAdmin
from geonode.documents.models import Document
from geonode.people.admin import ProfileAdmin
from geonode.people.models import Profile
from ordered_model.admin import OrderedModelAdmin

from django_admin_inline_paginator.admin import TabularInlinePaginated
from bims.models import (
    LocationType,
    LocationSite,
    IUCNAssessment,
    IUCNStatus,
    Survey,
    SurveyData,
    SurveyDataOption,
    SurveyDataValue,
    Boundary,
    BoundaryType,
    Cluster,
    BiologicalCollectionRecord,
    LinkCategory,
    Link,
    Visitor,
    Pageview,
    ShapefileUploadSession,
    Shapefile,
    NonBiodiversityLayer,
    UserBoundary,
    SearchProcess,
    ReferenceLink,
    Endemism,
    Taxonomy,
    TaxonGroup,
    VernacularName,
    RiverCatchment,
    FbisUUID,
    Biotope,
    DataSource,
    SpatialScale,
    SpatialScaleGroup,
    SamplingMethod,
    SiteImage,
    SiteSetting,
    GeocontextSetting,
    BimsDocument,
    ChemicalRecord,
    LocationContext,
    Chem,
    LocationContextGroup,
    LocationContextFilterGroupOrder,
    LocationContextFilter,
    AlgaeData,
    UploadSession,
    DashboardConfiguration,
    HarvestSession,
    generate_site_code,
    location_site_post_save_handler,
    DownloadRequest,
    DownloadRequestPurpose,
    BaseMapLayer,
    RequestLog,
    IngestedData,
    TaxonImage,
    WaterTemperature,
    TaxonExtraAttribute,
    DecisionSupportTool,
    Unit,
    DecisionSupportToolName,
    WaterTemperatureThreshold,
    Notification,
    RecordType,
    Hydroperiod,
    WetlandIndicatorStatus,
    AbundanceType,
    SamplingEffortMeasure,
    TaxonGroupTaxonomy,
    TaxonomyUpdateProposal,
    TaxonomyUpdateReviewer,
    CITESListingInfo,
    ImportTask,
    Invasion,
    TaxonOrigin,
    FlatPageExtension,
    TagGroup,
    Dataset, LayerGroup,
    SpeciesGroup,
    TaxonGroupCitation,
    HarvestSchedule,
    OccurrenceUploadTemplate,
    UploadRequest, UploadType, CertaintyHierarchy,
    FilterPanelInfo,
    TaxonTagDescription
)
from bims.models.taxonomy import TaxonTag
from bims.utils.fetch_gbif import merge_taxa_data
from bims.conf import TRACK_PAGEVIEWS
from bims.models.profile import Profile as BimsProfile, Role
from bims.utils.gbif import suggest_search
from bims.utils.location_context import merge_context_group
from bims.utils.user import merge_users
from bims.tasks.location_site import (
    update_location_context as update_location_context_task,
    update_site_code as update_site_code_task
)
from bims.tasks import run_scheduled_gbif_harvest
from bims.forms.harvest_schedule import HarvestScheduleAdminForm


logger = logging.getLogger('bims')


class ExportCsvMixin:
    def export_as_csv(self, request, queryset):

        meta = self.model._meta
        field_names = [field.name for field in meta.fields]

        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename={}.csv'.format(meta)
        writer = csv.writer(response)

        writer.writerow(field_names)
        for obj in queryset:
            row = writer.writerow([getattr(obj, field) for field in field_names])

        return response

    export_as_csv.short_description = "Export Selected"


class LocationSiteForm(forms.ModelForm):
    class Media:
        css = {}
        js = (
            '/static/libs/jquery/jquery-3.3.1.min.js',
            '/static/js/forms/location-site-admin-form.js')


class HasLocationContextDocument(django_admin.SimpleListFilter):
    """Filter based on Location Context Document existence."""
    title = 'has_location_context'
    parameter_name = 'has_location_context'

    def lookups(self, request, model_admin):
        return (
            ('Yes', 'Yes'),
            ('No', 'No'),
        )

    def queryset(self, request, queryset):
        value = self.value()
        if value == 'Yes':
            return queryset.filter(locationcontext__isnull=False)
        elif value == 'No':
            return queryset.filter(locationcontext__isnull=True)
        return queryset


class LocationContextInline(admin.TabularInline):
    model = LocationContext
    raw_id_fields = ('group',)


class LocationContextAdmin(admin.ModelAdmin):
    list_display = ('key', 'name', 'value')


class InsideSiteBoundaryFilter(SimpleListFilter):
    """Filter LocationSites by whether they are inside the configured site boundary."""
    title = 'Inside site boundary'
    parameter_name = 'inside_boundary'

    def lookups(self, request, model_admin):
        """Defines the filter choices."""
        return (
            ('yes', 'Yes'),
            ('no', 'No'),
        )

    def queryset(self, request, queryset):
        """Returns the filtered queryset based on user's choice."""
        boundary = preferences.SiteSetting.site_boundary

        if not boundary or not boundary.geometry:
            return queryset

        if self.value() == 'yes':
            return queryset.filter(
                geometry_point__within=boundary.geometry)
        elif self.value() == 'no':
            return queryset.exclude(
                geometry_point__within=boundary.geometry)
        return queryset


class LocationSiteAdmin(admin.GISModelAdmin):
    change_list_template = 'admin/location_site_changelist.html'
    form = LocationSiteForm
    default_zoom = 5
    default_lat = -30
    default_lon = 25

    readonly_fields = (
        'original_geomorphological',)

    list_display = (
        'name',
        'site_code',
        'location_type',
        'get_centroid',
        'geocontext_data_percentage',
        'indicator_thermal')
    search_fields = ('name', 'site_code', 'legacy_site_code')
    list_filter = (
        HasLocationContextDocument,
        InsideSiteBoundaryFilter,
    )
    raw_id_fields = ('river',)
    list_display_links = ['name', 'site_code']

    actions = [
        'update_site_code',
        'update_location_context',
        'delete_location_context',
        'update_location_context_in_background']
    inlines = [LocationContextInline, ]

    def get_readonly_fields(self, request, obj=None):
        return ['original_geomorphological']

    def indicator_thermal(self, obj):
        if obj.watertemperature_set.all().exists():
            return format_html(
                '''
                <a href="/thermal-dashboard/?site-id={0}">Open Thermal Indicator Dashboard</a>
                ''',
                obj.id
            )
        else:
            return '-'

    def geocontext_data_percentage(self, obj):
        raw_keys = preferences.GeocontextSetting.geocontext_keys.split(',')
        key_pairs = []
        for key in raw_keys:
            if ':' in key:
                group_key, layer_id = key.split(':', 1)
                key_pairs.append((group_key, layer_id))
            else:
                key_pairs.append((key, None))

        filters = Q()
        for group_key, layer_id in key_pairs:
            if layer_id:
                filters |= Q(
                    site=obj,
                    group__geocontext_group_key=group_key,
                    group__layer_identifier=layer_id
                )
            else:
                filters |= Q(
                    site=obj,
                    group__geocontext_group_key=group_key
                )

        groups = LocationContext.objects.filter(filters).values(
            'group__geocontext_group_key', 'group__layer_identifier'
        ).distinct()

        matched_count = groups.count()
        total_expected = len(key_pairs)
        percentage = round(matched_count / total_expected * 100) if total_expected > 0 else 0

        return format_html(
            '''
            <progress value="{0}" max="100"></progress>
            <span style="font-weight:bold">{0}%</span>
            ''',
            percentage
        )

    def update_site_code(self, request, queryset):
        """Action to update site code"""
        models.signals.post_save.disconnect(
            location_site_post_save_handler,
            sender=LocationSite
        )

        if queryset.count() > 10:
            """ Update site code in background """
            update_site_code_task.delay(
                [site.id for site in queryset]
            )
            full_message = (
                'Updating site code for {} sites in background'.format(
                    queryset.count()
                )
            )
            self.message_user(request, full_message)

            return

        for location_site in queryset:
            wetland_name = (
                location_site.wetland_name if location_site.wetland_name else location_site.user_wetland_name
            )
            if not wetland_name:
                wetland_name = 'UNKNOWN'
            location_site.site_code, catchments_data = generate_site_code(
                location_site=location_site,
                lat=location_site.latitude,
                lon=location_site.longitude,
                ecosystem_type=location_site.ecosystem_type,
                wetland_name=wetland_name,
            )
            if (
                    location_site.ecosystem_type and
                    location_site.ecosystem_type.lower() == 'wetland' and
                    catchments_data
            ):
                if 'hgm_type' in catchments_data and catchments_data['hgm_type']:
                    location_site.hydrogeomorphic_type = catchments_data['hgm_type']
                if 'name' in catchments_data and catchments_data['name']:
                    location_site.wetland_name = catchments_data['name']
            location_site.save()

        models.signals.post_save.connect(
            location_site_post_save_handler,
            sender=LocationSite
        )

        self.message_user(
            request,
            'Site codes has been updated for {} sites'.format(
                queryset.count()))

    def update_location_context_in_background(self, request, queryset):
        """Action method to update location context in background"""
        generate_spatial_scale_filter = queryset.count() <= 1
        for location_site in queryset:
            update_location_context_task.delay(
                location_site.id,
                False,
                generate_spatial_scale_filter)
        full_message = (
            'Updating location context for {} sites in background'.format(
                queryset.count()
            )
        )
        self.message_user(request, full_message)

    def update_location_context(self, request, queryset):
        """Action method to update selected location contexts."""
        if len(queryset) > 5:
            message = 'You can not update for more than 5 location sites.'
            self.message_user(request, message)
            return
        rows_updated = 0
        rows_failed = 0
        error_message = ''
        site_names = []
        for location_site in queryset:
            success, message = location_site.update_location_context_document()
            if success:
                rows_updated += 1
                site_names.append(location_site.location_site_identifier)
            else:
                rows_failed += 1
                error_message += (
                                     'Failed to update site [%s] because [%s]\n') % (
                                     location_site.location_site_identifier, message)

        full_message = "%s successfully updated." % ','.join(site_names)

        if rows_failed > 0:
            error_message_bit = 'There are %s not updated site.' % rows_failed
            error_message_bit += '\n' + error_message
            full_message += '\n' + error_message_bit

        self.message_user(request, full_message)

    def delete_location_context(self, request, queryset):
        """Action method to delete selected location contexts."""
        LocationContext.objects.filter(
            site__in=queryset
        ).delete()
        if queryset.count() == 1:
            message_bit = "1 location context"
        else:
            message_bit = "%s location contexts" % queryset.count()
        self.message_user(request, "%s successfully deleted." % message_bit)

    update_location_context.short_description = (
        'Update location context data for selected sites.')

    update_location_context_in_background.short_description = (
        'Update location context data for selected sites in background.')

    delete_location_context.short_description = (
        'Delete location context data for selected sites.')

    def save_model(self, request, obj, form, change):
        if not obj.creator:
            obj.creator = request.user
        if obj.owner and not request.user.is_superuser:
            pass
        obj.save()


class TaxonOriginAdmin(OrderedModelAdmin):
    ordering = ('order',)
    list_display = ('id', 'move_up_down_links', 'category', 'origin_key')


class InvasionAdmin(OrderedModelAdmin):
    ordering = ('order',)
    list_display = ('id', 'move_up_down_links', 'category')


class IUCNStatusAdmin(OrderedModelAdmin):
    list_display = ('id', 'get_category_display', 'move_up_down_links',
                    'order', 'sensitive',
                    'iucn_colour', 'national', 'total_species')
    ordering = ('order',)

    list_filter = (
        'national',
    )

    def iucn_colour(self, obj):
        return format_html(
            '<div style="background:{}; width: 50px; height: 15px;"></div>',
            obj.colour)

    def total_species(self, obj):
        total_taxa = Taxonomy.objects.filter(
            iucn_status=obj
        ).count()
        return format_html(
            '<a href="/admin/bims/taxonomy/?iucn_status__id__exact={}">'
            '{}</a>',
            obj.id, total_taxa
        )

    iucn_colour.allow_tags = True


class IUCNAssessmentAdmin(admin.ModelAdmin):
    class GlobalScopeFilter(SimpleListFilter):
        title = 'Scope'
        parameter_name = 'scope_label'

        def value(self):
            return self.used_parameters.get(self.parameter_name, 'global')

        def lookups(self, request, model_admin):
            return [
                ('global', 'Global'),
                ('all', 'All'),
            ]

        def queryset(self, request, queryset):
            value = self.value()
            if value == 'all':
                return queryset
            return queryset.filter(scope_label__iexact='Global')

    list_display = (
        'id',
        'taxonomy',
        'year_published',
        'red_list_category_code',
        'latest',
        'sis_taxon_id',
        'assessment_id',
    )
    list_filter = (
        GlobalScopeFilter,
        'latest',
        'year_published',
        'red_list_category_code',
        'scope_code',
    )
    search_fields = (
        'taxonomy__scientific_name',
        'taxonomy__canonical_name',
        'assessment_id',
        'sis_taxon_id',
        'url',
    )
    raw_id_fields = ('taxonomy', 'normalized_status')
    ordering = ('-year_published', '-id')


class BoundaryAdmin(admin.ModelAdmin):
    list_display = ('code_name', 'name', 'type', 'top_level_boundary')
    list_filter = ('type',)
    ordering = ('type', 'name')


class ClusterAdmin(admin.ModelAdmin):
    list_display = ('boundary', 'module')
    list_filter = ('boundary', 'module')


class PermissionContenTypeFilter(SimpleListFilter):
    title = 'App Label'
    parameter_name = 'content_type_app_label'

    def lookups(self, request, model_admin):
        content_types = []
        app_labels = []
        all_object = model_admin.model.objects.all()
        for permission_object in all_object:
            if permission_object.content_type.app_label not in app_labels:
                app_labels.append(permission_object.content_type.app_label)
                content_types.append(permission_object.content_type)
        return [(c.app_label, c.app_label) for c in content_types]

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(content_type__app_label=self.value())
        else:
            return queryset.all()


class PermissionAdmin(admin.ModelAdmin):
    list_display = ('content_type', 'name', 'codename')
    list_filter = ('content_type', 'name', PermissionContenTypeFilter)


class BiologicalCollectionAdmin(admin.ModelAdmin, ExportCsvMixin):
    show_full_result_count = False
    list_per_page = 25
    list_select_related = (
        'taxonomy',
        'taxonomy__origin',
        'site',
        'biotope',
        'record_type',
        'sampling_method',
        'owner',
    )

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        if request.user.is_superuser:
            return qs
        if request.user.is_staff:
            if not hasattr(
                request.user,
                'bims_profile'
            ):
                return qs.none()
            return qs
        return qs

    class Media:
        css = {
            'all': ('admin/custom-admin.css',)
        }

    # exclude = ['source_reference',]
    list_display = (
        'uuid',
        'taxonomy',
        'get_origin',
        'collection_date',
        'collector',
        'owner',
        'site',
        'abundance_number',
        'biotope',
        'record_type',
        'sampling_method'
    )
    raw_id_fields = (
        'site',
        'owner',
        'taxonomy',
        'source_reference',
        'collector_user',
        'survey'
    )
    list_filter = (
        ('collection_date', DateRangeFilterBuilder()),
        'taxonomy__origin',
        'record_type__name',
        'sampling_method',
        'ecosystem_type',
        'source_collection',
        'data_type'
    )
    search_fields = (
        'taxonomy__scientific_name',
        'taxonomy__canonical_name',
        'original_species_name',
        'uuid',
        'site__site_code',
        'upstream_id',
        'source_collection'
    )
    actions = ['export_as_csv']

    def get_search_results(self, request, queryset, search_term):
        import re
        uuid_pattern = re.compile(
            r'^[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}$',
            re.IGNORECASE
        )
        if uuid_pattern.match(search_term.strip()):
            normalised = search_term.strip().replace('-', '')
            uuid_queryset = queryset.filter(
                Q(uuid__iexact=search_term.strip()) |
                Q(uuid__icontains=normalised),
            )
            if uuid_queryset.exists():
                return uuid_queryset, False
        queryset, use_distinct = super(
            BiologicalCollectionAdmin, self
        ).get_search_results(request, queryset, search_term)
        return queryset, use_distinct

    def get_origin(self, obj):
        if obj.taxonomy and obj.taxonomy.origin:
            return obj.taxonomy.origin.category
        return 'Unknown'

    get_origin.short_description = 'Origin'
    get_origin.admin_order_field = 'taxonomy__origin'


class ShapefileInline(admin.TabularInline):

    def shapefile_name(self, obj):
        if obj.shapefile:
            return mark_safe("""<a href="%s" />%s</a>""" % (
                obj.shapefile.fileurl, obj.shapefile.filename))

    model = ShapefileUploadSession.shapefiles.through
    fields = ('shapefile_name', 'shapefile')
    readonly_fields = ('shapefile_name',)


class ShapefileUploadSessionAdmin(admin.ModelAdmin):
    exclude = ('shapefiles', 'token')
    list_display = (
        'uploader',
        'uploaded_at',
        'processed',
    )

    inlines = (ShapefileInline,)


class ShapefileAdmin(admin.ModelAdmin):
    exclude = ('token',)
    list_display = (
        'id',
        'shapefile',
    )


class UserBoundaryAdmin(admin.ModelAdmin):
    list_display = (
        'name',
        'user'
    )


class LinkAdmin(admin.ModelAdmin):
    list_display = ('name', 'category')
    list_filter = ('category',)


admin.site.register(LinkCategory)
admin.site.register(Link, LinkAdmin)


class ProfileInline(admin.StackedInline):
    classes = ('grp-collapse grp-open',)
    inline_classes = ('grp-collapse grp-open',)
    model = BimsProfile


class TaxonGroupExpertInline(admin.TabularInline):
    """Inline to assign user as expert for taxon groups."""
    model = TaxonGroup.experts.through
    extra = 1
    verbose_name = 'Expert for Taxon Group'
    verbose_name_plural = 'Expert for Taxon Groups'
    autocomplete_fields = ['taxongroup']


class BimsProfileAdmin(admin.ModelAdmin):
    model = BimsProfile
    list_display = [
        'user',
        'first_name',
        'last_name',
        'qualifications',
        'sass_accredited_date_from',
        'sass_accredited_date_to']
    search_fields = (
        'user__first_name',
        'user__username'
    )


class ProfileCreationForm(UserCreationForm):
    class Meta:
        model = get_user_model()
        fields = ("username",)

    def clean_username(self):
        # Since User.username is unique, this check is redundant,
        # but it sets a nicer error message than the ORM. See #13147.
        username = self.cleaned_data["username"]
        try:
            get_user_model().objects.get(username=username)
        except get_user_model().DoesNotExist:
            return username
        raise forms.ValidationError(
            self.error_messages['duplicate_username'],
            code='duplicate_username',
        )


# Inherits from GeoNode ProfileCreationForm
class UserCreateForm(ProfileCreationForm):
    class Meta:
        fields = ('username', 'first_name', 'last_name', 'email')

    def __init__(self, *args, **kwargs):
        super(UserCreateForm, self).__init__(*args, **kwargs)
        self.fields['email'].required = True


class SassAccreditedStatusFilter(SimpleListFilter):
    title = 'Sass accredited status'
    parameter_name = 'sass_accredited'

    def lookups(self, request, model_admin):
        return [
            (True, 'Accredited'),
            (False, 'Not accredited')
        ]

    def queryset(self, request, queryset):
        if self.value() == 'True':
            return queryset.filter(
                bims_profile__sass_accredited_date_to__gte=date.today()
            )
        elif self.value() == 'False':
            return queryset.filter(
                bims_profile__sass_accredited_date_to__lte=date.today()
            )
        return queryset


class UserHasEmailFilter(SimpleListFilter):
    title = 'User has email address'
    parameter_name = 'user_has_email'

    def lookups(self, request, model_admin):
        return [
            (True, 'Yes'),
            (False, 'No')
        ]

    def queryset(self, request, queryset):
        if self.value() == 'False':
            return queryset.filter(
                models.Q(email__isnull=True) |
                models.Q(email='')
            )
        elif self.value() == 'True':
            return queryset.filter(
                email__isnull=False
            ).exclude(email='')
        return queryset


class RoleFilter(SimpleListFilter):
    title = 'Role'
    parameter_name = 'role'

    def lookups(self, request, model_admin):
        return BimsProfile.ROLE_CHOICES

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(bims_profile__role__iexact=self.value())
        return queryset


class SignedUpFilter(SimpleListFilter):
    title = 'Signed up'
    parameter_name = 'signed_up'

    def lookups(self, request, model_admin):
        return [
            (True, 'Yes'),
            (False, 'No')
        ]

    def queryset(self, request, queryset):
        if self.value() == 'False':
            return queryset.filter(
                models.Q(email__isnull=True) |
                models.Q(email='')
            )
        elif self.value() == 'True':
            return queryset.filter(
                models.Q(last_login__isnull=False) |
                models.Q(email__isnull=False)
            ).exclude(email='')
        return queryset


# Inherits from GeoNode's ProfileAdmin page
class CustomUserAdmin(ProfileAdmin):
    add_form = UserCreateForm
    change_form_template = 'admin/user_changeform.html'
    inlines = [ProfileInline, TaxonGroupExpertInline]
    add_fieldsets = (
        (None, {
            'classes': ('wide',),
            'fields': ('username', 'password1', 'password2', 'first_name',
                       'last_name', 'email', 'is_staff', 'is_superuser',
                       'groups',),
        }),
    )
    list_display = (
        'username',
        'email',
        'first_name',
        'last_name',
        'organization',
        'role',
        'is_staff',
        'is_active',
        'signed_up',
        'sass_accredited_status',
        'expert_taxon_groups',
        'date_joined',
        'last_login'
    )
    list_filter = (
        'is_staff',
        'is_superuser',
        'is_active',
        'groups',
        SassAccreditedStatusFilter,
        UserHasEmailFilter,
        'organization',
        SignedUpFilter,
        RoleFilter
    )
    readonly_fields = (
        'password',
    )

    actions = ['merge_users', 'download_csv']

    def download_csv(self, request, queryset):
        import csv
        from django.http import HttpResponse
        try:
            from StringIO import StringIO  # for Python 2
        except ImportError:
            from io import StringIO  # for Python 3

        f = StringIO()
        writer = csv.writer(f)
        writer.writerow([
            'Username',
            'Email',
            'First Name',
            'Last Name',
            'Organization Name',
            'Role',
            'Staff status',
            'Active',
            'Signed up',
            'SASS Accredited Status',
            'Date joined',
            'Last login'])

        for s in queryset:
            sass_accredited_status = (
                self.sass_accredited_status(s, text_only='True')
            )
            writer.writerow([
                s.username, s.email,
                s.first_name, s.last_name,
                s.organization,
                self.role(s),
                s.is_staff,
                s.is_active,
                self.signed_up(s, text_only='True'),
                sass_accredited_status,
                s.date_joined,
                s.last_login])

        f.seek(0)
        response = HttpResponse(f, content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename=users.csv'
        return response

    download_csv.short_description = (
        "Download CSV file for selected users"
    )

    def merge_users(self, request, queryset):
        active_user = queryset.filter(is_active=True)
        if queryset.count() <= 1:
            self.message_user(
                request, 'Need more than 1 user', messages.ERROR
            )
            return
        if not active_user.exists():
            self.message_user(
                request, 'Missing active user', messages.ERROR)
            return
        if active_user.count() > 1:
            self.message_user(
                request, 'There are more than 1 active user',
                messages.ERROR)
            return
        merge_users(
            primary_user=active_user[0],
            user_list=queryset.values_list('id', flat=True)
        )
        self.message_user(request, 'Users has been merged')

    merge_users.short_description = 'Merge users'

    def sass_accredited_status(self, obj):
        false_response = mark_safe(
            '<img src="/static/admin/img/icon-no.svg" alt="False">')
        true_response = mark_safe(
            '<img src="/static/admin/img/icon-yes.svg" alt="True">')

    def role(self, obj):
        try:
            profile = BimsProfile.objects.get(user=obj)
            role = [(v) for k, v in BimsProfile.ROLE_CHOICES if k == profile.role]
            return role[0] if len(role) > 0 else '-'
        except BimsProfile.DoesNotExist:
            return '-'

    role.admin_order_field = 'bims_profile__role'

    def sass_accredited_status(self, obj, **kwargs):
        text_only = kwargs.get('text_only', 'False')
        if text_only == 'True':
            false_response = 'False'
            true_response = 'True'
        else:
            false_response = mark_safe(
                '<img src="/static/admin/img/icon-no.svg" alt="False">')
            true_response = mark_safe(
                '<img src="/static/admin/img/icon-yes.svg" alt="True">')
        try:
            profile = BimsProfile.objects.get(user=obj)
            valid_to = profile.sass_accredited_date_to
            if not valid_to:
                return '-'
            # Check if it is still valid
            if date.today() > valid_to:
                return false_response
            else:
                return true_response
        except BimsProfile.DoesNotExist:
            return '-'

    def signed_up(self, obj, **kwargs):
        text_only = kwargs.get('text_only', 'False')
        if text_only == 'True':
            false_response = 'False'
            true_response = 'True'
        else:
            false_response = mark_safe(
                '<img src="/static/admin/img/icon-no.svg" alt="False">')
            true_response = mark_safe(
                '<img src="/static/admin/img/icon-yes.svg" alt="True">')
        if not obj.email:
            return false_response
        if obj.last_login:
            return true_response
        try:
            profile_data = json.loads(
                obj.bims_profile.data
            )
            if 'DateModified' in profile_data:
                return false_response
        except (ValueError, TypeError, BimsProfile.DoesNotExist):
            pass
        return true_response

    def expert_taxon_groups(self, obj):
        """Return the taxon groups where this user is an expert."""
        taxon_groups = TaxonGroup.objects.filter(experts=obj)
        if not taxon_groups.exists():
            return '-'
        return ', '.join([str(tg.name) for tg in taxon_groups])

    expert_taxon_groups.short_description = 'Expert for Taxon Groups'

    def save_model(self, request, obj, form, change):
        if obj.pk is None:
            obj.is_active = False
        super(CustomUserAdmin, self).save_model(request, obj, form, change)

    def response_add(self, request, obj, post_url_continue=None):
        scheme = request.scheme
        site = request.get_host()
        data = {
            'firstname': obj.first_name,
            'lastname': obj.last_name,
            'scheme': scheme,
            'site': site,
            'username': obj.username
        }
        send_mail(
            '[BIMS] Activate your new account',
            'Dear {firstname} {lastname},\n\nPlease follow this link '
            'to change your password and to activate your account:\n'
            '{scheme}://{site}/activate-user/{username}/\n\n'
            'Here is your information:\n'
            'Username: {username}\n\n'
            'Kind regards,\nBIMS Team.'.format(
                **data
            ),
            settings.DEFAULT_FROM_EMAIL,
            [obj.email],
            fail_silently=False
        )
        return super(CustomUserAdmin, self).response_add(
            request, obj, post_url_continue)

    def changelist_view(self, request, extra_context=None):
        referer = request.META.get('HTTP_REFERER', '')
        if referer:
            test = referer.split(request.META['PATH_INFO'])
            if test[-1] and not test[-1].startswith('?'):
                if 'signed_up' not in request.GET:
                    q = request.GET.copy()
                    q['signed_up'] = 'True'
                    request.GET = q
                    request.META['QUERY_STRING'] = request.GET.urlencode()
        return super().changelist_view(request, extra_context=extra_context)

class BaseMapLayerAdmin(OrderedModelAdmin):
    list_display = (
        'title',
        'url',
        'default_basemap',
        'move_up_down_links')
    list_filter = ('title',)
    ordering = ('order',)


class NonBiodiversityLayerAdmin(OrderedModelAdmin):
    list_display = (
        'order',
        'name',
        'wms_url',
        'wms_layer_name',
        'move_up_down_links',)
    list_filter = ('wms_url',)
    ordering = ('order',)


class VisitorAdmin(admin.ModelAdmin):
    date_hierarchy = 'start_time'

    list_display = (
        'session_key',
        'user',
        'start_time',
        'session_over',
        'pretty_time_on_site',
        'ip_address',
        'user_agent')

    list_filter = ('user', 'ip_address')

    def session_over(self, obj):
        return obj.session_ended() or obj.session_expired()

    session_over.boolean = True

    def pretty_time_on_site(self, obj):
        if obj.time_on_site is not None:
            return timedelta(seconds=obj.time_on_site)

    pretty_time_on_site.short_description = 'Time on site'


class SearchProcessAdmin(admin.ModelAdmin):
    list_display = (
        'file_path',
        'category',
        'finished')

    def delete_model(self, request, obj):
        if obj.locked:
            self.message_user(
                request,
                "This record is locked and cannot be deleted.",
                level='warning')
        else:
            super().delete_model(request, obj)

    def delete_queryset(self, request, queryset):
        for obj in queryset:
            if obj.locked:
                self.message_user(
                    request,
                    f"Record with ID {obj.id} is locked and cannot be deleted.",
                    level='warning')
        queryset = queryset.filter(locked=False)
        if queryset:
            super().delete_queryset(request, queryset)


class PageviewAdmin(admin.ModelAdmin):
    date_hierarchy = 'view_time'

    list_display = ('url', 'view_time')


class ReferenceLinkAdmin(admin.ModelAdmin):
    list_display = (
        'collection_record',
        'reference'
    )


class EndemismAdmin(admin.ModelAdmin):
    list_display = (
        'name',
        'description',
        'display_order',
        'verified'
    )

    actions = ['merge_endemisms']

    def merge_endemisms(self, request, queryset):
        verified = queryset.filter(verified=True)
        if queryset.count() <= 1:
            self.message_user(
                request, 'Need more than 1 endemism', messages.ERROR
            )
            return
        if not verified.exists():
            self.message_user(
                request, 'Missing verified endemism', messages.ERROR)
            return
        if verified.count() > 1:
            self.message_user(
                request, 'There are more than 1 verified endemism',
                messages.ERROR)
            return
        excluded = verified[0]
        endemisms = queryset.exclude(id=verified[0].id)
        merge_endemism(excluded, endemisms)
        self.message_user(request, 'Endemism has been merged')

    merge_endemisms.short_description = 'Merge endemism'


class TaxonImagesInline(admin.TabularInline):
    model = TaxonImage
    raw_id_fields = (
        'survey',
        'owner',
        'uploader'
    )


class TaxonomyAdminForm(forms.ModelForm):
    class Meta:
        model = Taxonomy
        widgets = {
            'gbif_data': JSONEditorWidget,
            'additional_data': JSONEditorWidget,
        }
        exclude = (
            'reliability_of_sources',
            'certainty_of_identification',
            'accuracy_of_coordinates',
        )

    def clean(self):
        """
        Enforce duplicate rules for taxa:

        - Manual taxa (no gbif_key):
          If another taxon with the same canonical_name exists, do not allow
          creating a duplicate.

        - GBIF taxa:
          If an ACCEPTED taxon with this canonical_name already exists:
            * Disallow adding a taxon with non-ACCEPTED status (synonym, doubtful, etc)
              using the same canonical_name.
            * Disallow adding another ACCEPTED taxon with a different author.
        """
        cleaned_data = super().clean()
        canonical_name = (cleaned_data.get('canonical_name') or '').strip()
        author = (cleaned_data.get('author') or '').strip()
        taxonomic_status = cleaned_data.get('taxonomic_status')
        gbif_key = cleaned_data.get('gbif_key')

        if not canonical_name:
            return cleaned_data

        duplicate_fields = (
            'canonical_name', 'author', 'taxonomic_status', 'gbif_key'
        )
        if self.instance.pk and not any(f in self.changed_data for f in duplicate_fields):
            return cleaned_data

        qs = Taxonomy.objects.filter(
            canonical_name__iexact=canonical_name
        )
        if self.instance.pk:
            qs = qs.exclude(pk=self.instance.pk)

        if not qs.exists():
            return cleaned_data

        existing = qs.first()

        if not gbif_key:
            raise forms.ValidationError(
                _(
                    "A taxon with canonical name '%(name)s' already exists "
                    "(ID %(id)s). Please use or update the existing taxon "
                    "instead of creating a duplicate."
                ) % {"name": canonical_name, "id": existing.pk}
            )

        accepted = qs.filter(
            taxonomic_status=TaxonomicStatus.ACCEPTED.name
        ).first()

        if accepted:
            if taxonomic_status and taxonomic_status != TaxonomicStatus.ACCEPTED.name:
                raise forms.ValidationError(
                    _(
                        "This canonical name already exists in BIMS as an "
                        "ACCEPTED taxon. You cannot add a synonym/doubtful "
                        "entry with the same canonical name."
                    )
                )

            if author and accepted.author and author.strip() != accepted.author.strip():
                raise forms.ValidationError(
                    _(
                        "There is already an accepted taxon with this "
                        "canonical name but a different author "
                        "('%(author)s'). Please review the existing record "
                        "before adding another taxon."
                    ) % {"author": accepted.author}
                )

        return cleaned_data


class TaxonGroupTaxonomyInline(TabularInlinePaginated):
    model = TaxonGroupTaxonomy
    extra = 1
    raw_id_fields = ('taxonomy',)
    per_page = 20


class OccurrenceUploadTemplateInline(admin.TabularInline):
    model = OccurrenceUploadTemplate
    extra = 1


class TaxonGroupAdmin(admin.ModelAdmin):
    inlines = [
        TaxonGroupTaxonomyInline,
        OccurrenceUploadTemplateInline
    ]
    list_per_page = 20
    list_display = (
        'name',
        'singular_name',
        'category'
    )
    search_fields = (
        'name',
        'singular_name',
    )
    list_filter = (
        'category',
    )
    filter_horizontal = (
        'experts',
    )
    raw_id_fields = (
        'gbif_parent_species',
    )


class TaxonGroupListFilter(django_admin.SimpleListFilter):
    title = 'Taxon Group'

    parameter_name = 'taxon_group'

    def lookups(self, request, model_admin):
        taxon_groups = TaxonGroup.objects.filter(
            category=TaxonomicGroupCategory.SPECIES_MODULE.name)
        return [
            (taxon_group.id, taxon_group.name) for taxon_group in taxon_groups
        ]

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(
                taxongrouptaxonomy__taxongroup=self.value()
            )
        else:
            return queryset


class DuplicateTaxonomyFilter(django_admin.SimpleListFilter):
    """
    Filter Taxonomy rows that have duplicate canonical_name values.
    This is used to help admins find potential duplicate taxa.
    """
    title = 'Possible duplicates'
    parameter_name = 'has_duplicates'

    def lookups(self, request, model_admin):
        return (
            ('yes', 'Has duplicates (by canonical name)'),
            ('no', 'No duplicates'),
        )

    def queryset(self, request, queryset):
        value = self.value()
        if not value:
            return queryset
        duplicated_names = (
            Taxonomy.objects
            .values('canonical_name')
            .annotate(cnt=Count('id'))
            .filter(cnt__gt=1)
            .values_list('canonical_name', flat=True)
        )

        if value == 'yes':
            return queryset.filter(canonical_name__in=duplicated_names)
        if value == 'no':
            return queryset.exclude(canonical_name__in=duplicated_names)
        return queryset


class InvalidParentRankFilter(django_admin.SimpleListFilter):
    title = _('Invalid parent rank')
    parameter_name = 'invalid_parent_rank'

    def lookups(self, request, model_admin):
        return (
            ('invalid', _('Parent is self or not higher rank')),
        )

    def queryset(self, request, queryset):
        if self.value() != 'invalid':
            return queryset

        # Build rank order mapping as database-level Case/When statements
        hierarchy = [rank.name for rank in TaxonomicRank.hierarchy()]
        rank_whens = [When(rank=rank_name, then=idx) for idx, rank_name in enumerate(hierarchy)]
        parent_rank_whens = [When(parent__rank=rank_name, then=idx) for idx, rank_name in enumerate(hierarchy)]

        queryset_annotated = queryset.filter(
            parent__isnull=False,
        ).annotate(
            rank_order=Case(
                *rank_whens,
                default=None,
                output_field=IntegerField()
            ),
            parent_rank_order=Case(
                *parent_rank_whens,
                default=None,
                output_field=IntegerField()
            )
        )

        # Filter using database-level comparisons
        return queryset_annotated.filter(
            Q(parent_id=F('id')) |  # Self-referencing parent
            Q(
                rank_order__isnull=False,
                parent_rank_order__isnull=False,
                parent_rank_order__gte=F('rank_order')  # Parent rank >= child rank (invalid)
            )
        )


class QualityCheckFilter(django_admin.SimpleListFilter):
    title = 'Quality check'
    parameter_name = 'quality_check'

    # Rank values that are exempt from the "must have parent" rule.
    # Both enum names and display values are included to guard against
    # data that was imported using the display value.
    TOP_LEVEL_RANKS = ['DOMAIN', 'KINGDOM', 'Domain', 'Kingdom']

    def _accepted_no_parent_q(self):
        return Q(
            taxonomic_status='ACCEPTED',
            parent__isnull=True,
        ) & ~Q(rank__in=self.TOP_LEVEL_RANKS)

    def _synonym_no_accepted_q(self):
        return Q(
            Q(taxonomic_status='DOUBTFUL') | Q(taxonomic_status__icontains='SYNONYM'),
            accepted_taxonomy__isnull=True,
        )

    def _no_upstream_id_q(self):
        # Missing upstream ID = no GBIF key AND no FADA ID (null or empty)
        return Q(gbif_key__isnull=True) & (Q(fada_id__isnull=True) | Q(fada_id=''))

    def lookups(self, request, model_admin):
        return (
            ('all_passed', 'All passed (100%)'),
            ('has_issues', 'Has issues'),
            ('accepted_no_parent', 'Accepted missing parent'),
            ('synonym_no_accepted', 'Synonym/Doubtful missing accepted taxon'),
            ('no_upstream_id', 'Missing upstream ID'),
        )

    def queryset(self, request, queryset):
        value = self.value()
        if not value:
            return queryset
        no_parent_q = self._accepted_no_parent_q()
        no_accepted_q = self._synonym_no_accepted_q()
        no_upstream_q = self._no_upstream_id_q()
        if value == 'has_issues':
            return queryset.filter(no_parent_q | no_accepted_q | no_upstream_q)
        if value == 'all_passed':
            return queryset.exclude(no_parent_q | no_accepted_q | no_upstream_q).filter(
                Q(taxonomic_status='ACCEPTED') |
                Q(taxonomic_status='DOUBTFUL') |
                Q(taxonomic_status__icontains='SYNONYM')
            )
        if value == 'accepted_no_parent':
            return queryset.filter(no_parent_q)
        if value == 'synonym_no_accepted':
            return queryset.filter(no_accepted_q)
        if value == 'no_upstream_id':
            return queryset.filter(no_upstream_q)
        return queryset


class TaxonomyAdmin(admin.ModelAdmin):
    form = TaxonomyAdminForm
    change_form_template = 'admin/taxonomy_changeform.html'
    readonly_fields = ('parent_hierarchy',)

    autocomplete_fields = (
        'vernacular_names',
    )

    ordering = ('canonical_name', )

    list_display = (
        'canonical_name',
        'link_to_gbif',
        'scientific_name',
        'author',
        'rank',
        'parent',
        'import_date',
        'taxonomic_status',
        'accepted_taxonomy',
        'iucn_status',
        'verified',
        'tag_list',
        'quality_check',
    )

    list_filter = (
        'rank',
        'verified',
        'import_date',
        'taxonomic_status',
        'iucn_status',
        TaxonGroupListFilter,
        DuplicateTaxonomyFilter,
        InvalidParentRankFilter,
        QualityCheckFilter,
    )

    search_fields = (
        'scientific_name',
        'canonical_name',
        'legacy_canonical_name',
        'gbif_key',
        'accepted_taxonomy__scientific_name'
    )

    raw_id_fields = (
        'parent',
        'accepted_taxonomy',
        'source_reference'
    )

    actions = [
        'merge_taxa', 'update_taxa', 'fetch_common_names',
        'fetch_cites_listing', 'extract_author',
        'fetch_iucn_assessments',
        'harvest_synonyms_for_accepted',
        'export_taxa_list',
    ]
    fieldsets = (
        (_('Taxon Details'), {
            'fields': (
                ('scientific_name', 'canonical_name'),
                ('legacy_canonical_name', 'author'),
                'rank',
                ('taxonomic_status', 'accepted_taxonomy'),
                'parent',
                'parent_hierarchy',
                'species_group',
                'gbif_key',
                'fada_id',
                'last_modified_by',
                'subgenus',
                'verified',
            )
        }),
        (_('Names & Tags'), {
            'fields': (
                'vernacular_names',
                'tags',
                'biographic_distributions',
            )
        }),
        (_('Conservation & Origin & Invasion'), {
            'fields': (
                ('origin', 'endemism', 'invasion'),
                ('iucn_status', 'national_conservation_status'),
                ('iucn_redlist_id', 'iucn_data'),
            )
        }),
        (_('References & Data'), {
            'fields': (
                ('source_reference', 'import_date'),
                'hierarchical_data',
                'gbif_data',
                'additional_data',
            )
        }),
        (_('Ownership'), {
            'classes': ('collapse',),
            'fields': (
                ('owner', 'collector_user', 'analyst'),
            )
        }),
        (_('Validation'), {
            'classes': ('collapse',),
            'fields': (
                ('validated', 'ready_for_validation', 'rejected'),
                'validation_message',
                'end_embargo_date',
            )
        }),
    )

    def extract_author(self, request, queryset):
        author_year_pattern = re.compile(r'^(.*?)(\b\d{4}\b)')

        count_updated = 0
        for taxon in queryset:
            sci = taxon.scientific_name or ''
            canon = taxon.canonical_name or ''

            if canon and sci.startswith(canon):
                remainder = sci[len(canon):].strip()
                match = author_year_pattern.match(remainder)
                if match:
                    author_year = match.group(0).strip(", ")
                    taxon.author = author_year
                    taxon.save()
                    count_updated += 1

        self.message_user(
            request,
            f"Updated {count_updated} taxa with extracted author and year.")

    extract_author.short_description = "Extract author"

    inlines = [TaxonImagesInline]

    class Media:
        css = {
            'all': ('admin/custom-admin.css',)
        }
        js = ('js/admin/quality-tooltip.js',)

    def fetch_cites_listing(self, request, queryset):
        taxa_ids = list(queryset.values_list('id', flat=True))
        chunk_size = 1000
        for chunk in chunk_list(taxa_ids, chunk_size):
            fetch_and_save_cites_listing.delay(chunk)
        self.message_user(request, "CITES data fetching initiated for selected taxa.")

    def fetch_common_names(self, request, queryset):
        taxa_ids = list(queryset.values_list('id', flat=True))
        fetch_vernacular_names.delay([str(taxa_id) for taxa_id in taxa_ids])

    def fetch_iucn_assessments(self, request, queryset):
        taxa_ids = list(queryset.values_list('id', flat=True))
        chunk_size = 1000
        for chunk in chunk_list(taxa_ids, chunk_size):
            fetch_iucn_status_task.delay(chunk)
        self.message_user(
            request,
            "IUCN status and assessment fetching initiated for selected taxa."
        )

    fetch_iucn_assessments.short_description = (
        "Fetch IUCN assessment history"
    )

    def update_taxa(self, request, queryset):
        for taxa in queryset:
            if taxa.additional_data:
                taxa.additional_data['fetch_gbif'] = True
            else:
                taxa.additional_data = {
                    'fetch_gbif': True
                }
            taxa.save()

    def merge_taxa(self, request, queryset):
        verified = queryset.filter(verified=True)
        if queryset.count() <= 1:
            self.message_user(
                request, 'Need more than 1 taxon', messages.ERROR
            )
            return
        if not verified.exists():
            self.message_user(
                request, 'Missing verified taxon', messages.ERROR)
            return
        if verified.count() > 1:
            self.message_user(
                request, 'There are more than 1 verified taxon',
                messages.ERROR)
            return
        merge_taxa_data(
            excluded_taxon=verified[0],
            taxa_list=queryset.exclude(id=verified[0].id)
        )
        self.message_user(request, 'Taxa has been merged')

    merge_taxa.short_description = 'Merge taxa'

    def link_to_gbif(self, obj):
        if obj.gbif_key:
            link = 'https://gbif.org/species/{}'.format(obj.gbif_key)
            label = obj.gbif_key
            return format_html(
                '<a href="{}" target="_blank">{}</a>', link, label)
        else:
            return '-'

    def change_view(self, request, object_id, form_url='', extra_context=None):
        extra_context = extra_context or {}
        scientific_name = Taxonomy.objects.get(pk=object_id).scientific_name

        if scientific_name is None:
            extra_context['results'] = None
            return super().change_view(
                request, object_id, form_url, extra_context=extra_context,
            )
        name = Taxonomy.objects.get(pk=object_id).scientific_name
        parameter = {'limit': 20, 'q': name}
        results = suggest_search(parameter)
        extra_context['results'] = json.dumps(results)
        return super().change_view(
            request, object_id, form_url, extra_context=extra_context,
        )

    def get_queryset(self, request):
        return super().get_queryset(request).prefetch_related('tags')

    def tag_list(self, obj):
        return u", ".join(o.name for o in obj.tags.all())

    # Ranks that are top-level and are not expected to have a parent
    _top_level_ranks = {'DOMAIN', 'KINGDOM', 'Domain', 'Kingdom'}

    def quality_check(self, obj):
        status = (obj.taxonomic_status or '').upper()
        is_accepted = status == 'ACCEPTED'
        is_synonym_or_doubtful = status == 'DOUBTFUL' or 'SYNONYM' in status

        checks = []
        issues = []

        if is_accepted and (obj.rank or '') not in self._top_level_ranks:
            if obj.parent_id:
                checks.append(True)
            else:
                checks.append(False)
                issues.append('Missing parent taxon')

        if is_synonym_or_doubtful:
            if obj.accepted_taxonomy_id:
                checks.append(True)
            else:
                checks.append(False)
                issues.append('Missing accepted taxon')

        # Upstream ID check applies to all statuses
        if obj.gbif_key or obj.fada_id:
            checks.append(True)
        else:
            checks.append(False)
            issues.append('Missing upstream ID')

        if not checks:
            return '-'

        percentage = max(0, 100 - len(issues) * 25)

        if percentage == 100:
            color = '#28a745'
        elif percentage >= 50:
            color = '#ffc107'
        else:
            color = '#dc3545'

        tooltip = '; '.join(issues) if issues else 'All checks passed'

        return format_html(
            '<span class="quality-badge" data-tooltip="{}" style="background:{};">{}</span>',
            tooltip,
            color,
            '{}%'.format(percentage),
        )

    quality_check.short_description = 'Quality'
    quality_check.allow_tags = True

    @admin.display(description=_('Parent hierarchy'))
    def parent_hierarchy(self, obj):
        """
        Display the ancestry chain of the selected parent to help admins
        understand the hierarchy being linked to this taxon.
        """
        if not obj or not obj.parent:
            return _('No parent selected')

        hierarchy = []
        current = obj.parent
        seen = set()
        depth = 0
        max_depth = 20

        while current and depth < max_depth:
            label = current.canonical_name or current.scientific_name or str(current.pk)
            rank = (current.rank or '').title()
            rank_label = f"{rank}: " if rank else ''
            hierarchy.append(f"{rank_label}{label}")

            if current.pk in seen:
                hierarchy.append(_('…cycle detected…'))
                break

            seen.add(current.pk)
            current = current.parent
            depth += 1

        if current:
            hierarchy.append(_('…truncated…'))

        return ' → '.join(str(h) for h in hierarchy)

    def harvest_synonyms_for_accepted(self, request, queryset):
        """
        For each selected ACCEPTED taxon (with a GBIF key), fetch synonyms
        """
        accepted_qs = queryset.filter(
            taxonomic_status=TaxonomicStatus.ACCEPTED.name
        ).exclude(gbif_key__isnull=True)

        if not accepted_qs.exists():
            self.message_user(
                request,
                "No accepted taxa with a GBIF key in the selected rows.",
                level=messages.WARNING,
            )
            return

        total_taxa = 0
        total_synonyms_linked = 0
        errors = 0

        for taxon in accepted_qs.iterator(chunk_size=100):
            try:
                processed = harvest_synonyms_for_accepted_taxonomy(
                    taxon,
                    fetch_vernacular_names=False,
                    log_file_path=None,
                    accept_language=None,
                )
                total_taxa += 1
                total_synonyms_linked += len(processed or [])
            except Exception as e:
                logger.exception(
                    "Failed harvesting synonyms for taxon id=%s key=%s: %s",
                    taxon.id, taxon.gbif_key, e
                )
                errors += 1

        self.message_user(
            request,
            f"Synonym harvest completed. Accepted taxa processed: {total_taxa}; "
            f"synonyms linked: {total_synonyms_linked}; errors: {errors}.",
            level=messages.INFO if errors == 0 else messages.WARNING,
        )

    harvest_synonyms_for_accepted.short_description = (
        "Harvest synonyms for accepted taxa (GBIF)"
    )

    def export_taxa_list(self, request, queryset):
        import os
        from bims.models.download_request import DownloadRequest
        from bims.tasks.download_taxa_list import download_taxa_list

        taxa_ids = list(queryset.values_list('id', flat=True))
        filename = 'taxa_list_export'
        folder = settings.PROCESSED_CSV_PATH
        path_folder = os.path.join(settings.MEDIA_ROOT, folder)
        os.makedirs(path_folder, exist_ok=True)
        csv_file_path = os.path.join(path_folder, '{}.csv'.format(filename))

        download_request = DownloadRequest.objects.create(
            resource_name='Taxa List',
            resource_type=DownloadRequest.CSV,
            approved=True,
            dashboard_url=request.get_full_path(),
            requester=request.user,
        )

        download_taxa_list.delay(
            {},
            csv_file_path,
            filename,
            request.user.id,
            download_request_id=download_request.id,
            taxa_ids=taxa_ids,
            order_by='canonical_name',
        )

        self.message_user(
            request,
            'Export is being processed. You will receive an email when it\'s ready.',
            level=messages.SUCCESS,
        )

    export_taxa_list.short_description = 'Export taxa list'

    def get_actions(self, request):
        actions = super().get_actions(request)
        if 'has_duplicates' not in request.GET:
            actions.pop('export_taxa_list', None)
        return actions

    def get_fieldsets(self, request, obj:Taxonomy=None):
        fieldsets = super().get_fieldsets(request, obj)

        hierarchy = TaxonomicRank.hierarchy()
        subgenus_index = next(
            i for i, r in enumerate(hierarchy) if r == TaxonomicRank.SUBGENUS
        )

        show_subgenus = False
        if obj and obj.rank:
            current_rank  = next(
                (r for r in hierarchy if r.name == obj.rank), None
            )
            if current_rank:
                show_subgenus = hierarchy.index(current_rank) > subgenus_index

        if not show_subgenus:
            result = []
            for name, options in fieldsets:
                new_fields = tuple(f for f in options['fields'] if f != 'subgenus')
                result.append((name, {**options, 'fields': new_fields}))
            return result

        return fieldsets


class VernacularNameAdmin(admin.ModelAdmin):
    search_fields = (
        'name',
    )

    list_display = (
        'name',
        'language',
        'source'
    )


class RiverCatchmentAdmin(admin.ModelAdmin):
    list_display = (
        'key',
        'value',
        'parent',
    )

    list_filter = (
        'key',
    )


class FbisUUIDAdmin(admin.ModelAdmin):
    list_display = ('uuid', 'content_type', 'content_object')
    list_filter = ('content_type',)
    ordering = ('content_type', 'uuid')
    search_fields = ('uuid',)


class SassBiotopeAdmin(admin.ModelAdmin):

    def taxon_group_list(self, obj: Biotope):
        return [taxon_group.name for taxon_group in obj.taxon_group.all()]

    taxon_group_list.short_description = 'Taxon groups'

    def used_in_SASS(self, obj: Biotope):
        if obj.sassbiotopefraction_set.all().exists():
            return mark_safe(
                '<img src="/static/admin/img/icon-yes.svg" alt="True">')
        return mark_safe(
            '<img src="/static/admin/img/icon-no.svg" alt="False">')

    list_display = (
        'name',
        'display_order',
        'biotope_form',
        'biotope_type',
        'taxon_group_list',
        'used_in_SASS',
        'verified'
    )
    list_filter = (
        'name',
    )
    ordering = (
        'name',
        'display_order',
        'biotope_form'
    )

    actions = ['merge_biotopes']

    def merge_biotopes(self, request, queryset):
        from bims.models import merge_biotope
        verified = queryset.filter(verified=True)
        if queryset.count() <= 1:
            self.message_user(
                request, 'Need more than 1 biotope', messages.ERROR
            )
            return
        if not verified.exists():
            self.message_user(
                request, 'Missing verified biotope', messages.ERROR)
            return
        if verified.count() > 1:
            self.message_user(
                request, 'There are more than 1 verified biotope',
                messages.ERROR)
            return
        excluded = verified[0]
        biotopes = queryset.exclude(id=verified[0].id)
        merge_biotope(excluded, biotopes)
        self.message_user(request, 'Biotope has been merged')


class DataSourceAdmin(admin.ModelAdmin):
    list_display = (
        'name',
        'category',
        'description'
    )
    search_fields = (
        'name',
        'category',
        'description'
    )
    list_filter = (
        'category',
    )
    readonly_fields = (
        'name',
    )
    fields = (
        'name',
        'category',
        'description'
    )


class SpatialScaleAdmin(admin.ModelAdmin):
    list_display = (
        'query',
        'name',
        'type',
        'group'
    )
    list_filter = (
        'group',
    )
    search_fields = (
        'query',
    )


class SpatialScaleGroupAdmin(admin.ModelAdmin):
    list_display = (
        'name',
        'parent'
    )


class SamplingMethodAdmin(admin.ModelAdmin):

    def taxon_group_list(self, obj: SamplingMethod):
        return [taxon_group.name for taxon_group in obj.taxon_group.all()]

    taxon_group_list.short_description = 'Taxon groups'

    list_display = (
        'sampling_method',
        'effort_measure',
        'verified',
        'taxon_group_list',
    )

    list_filter = (
        'sampling_method',
        'effort_measure',
        'verified'
    )

    actions = ['merge_sampling_methods']

    def merge_sampling_methods(self, request, queryset):
        verified = queryset.filter(verified=True)
        if queryset.count() <= 1:
            self.message_user(
                request, 'Need more than 1 sampling method', messages.ERROR
            )
            return
        if not verified.exists():
            self.message_user(
                request, 'Missing verified sampling method', messages.ERROR)
            return
        if verified.count() > 1:
            self.message_user(
                request, 'There are more than 1 verified sampling method',
                messages.ERROR)
            return
        excluded_sampling_method = verified[0]
        sampling_methods = queryset.exclude(id=verified[0].id)
        merge_sampling_method(
            excluded_sampling_method=excluded_sampling_method,
            sampling_methods=sampling_methods
        )
        self.message_user(request, 'Sampling method has been merged')

    merge_sampling_methods.short_description = 'Merge sampling methods'


class SiteImageAdmin(admin.ModelAdmin):
    list_display = (
        'get_site_code',
        'image',
        'date'
    )
    raw_id_fields = (
        'site',
        'site_visit',
        'survey'
    )
    search_fields = (
        'site__site_code',
        'site__legacy_site_code'
    )

    def get_site_code(self, obj):
        return obj.site.location_site_identifier

    get_site_code.short_description = 'Site'
    get_site_code.admin_order_field = 'site__site_code'


class BimsDocumentInline(admin.StackedInline):
    model = BimsDocument
    classes = ('collapse open',)
    inline_classes = ('collapse open',)


class BimsDocumentAdmin(DocumentAdmin):
    inlines = [BimsDocumentInline]


class ChemicalRecordAdmin(admin.ModelAdmin):
    date_hierarchy = 'date'
    list_display = (
        'value',
        'chem',
        'date',
        'get_site_code'
    )
    raw_id_fields = (
        'location_site',
        'survey',
        'source_reference'
    )
    list_filter = (
        ('date', DateRangeFilterBuilder()),
        'chem',
    )
    search_fields = (
        'location_site__site_code',
        'survey__site__site_code'
    )

    def get_site_code(self, obj):
        if obj.location_site:
            return obj.location_site.location_site_identifier
        elif obj.survey:
            return obj.survey.site.location_site_identifier
        return '-'

    get_site_code.short_description = 'Site Code'
    get_site_code.admin_order_field = 'site__site_code'


class ChemAdmin(admin.ModelAdmin):
    list_display = (
        'chem_code',
        'chem_description',
        'chem_unit',
        'show_in_abiotic_list',
        'minimum',
        'maximum'
    )
    list_filter = (
        'show_in_abiotic_list',
        'chem_unit'
    )
    search_fields = (
        'chem_code',
        'chem_description'
    )


class LocationContextFilterGroupOrderAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'link_to_filter',
        'link_to_group',
        'filter_display_order',
        'group_display_order',
        'show_in_dashboard',
        'show_in_side_panel'
    )

    sortable_by = (
        'filter_display_order',
        'group_display_order',
    )

    def link_to_group(self, obj):
        link = reverse('admin:bims_locationcontextgroup_change',
                       args=[obj.group.id])
        return format_html(
            "<a href='#' onclick='showPopup(event, \"{}\" )'>{}</a>",
            link,
            obj.group.name,
        )

    def link_to_filter(self, obj):
        link = reverse('admin:bims_locationcontextfilter_change',
                       args=[obj.filter.id])
        return format_html(
            "<a href='#' onclick='showPopup(event, \"{}\" )'>{}</a>",
            link,
            obj.filter.title,
        )

    class Media:
        js = (
            'js/show_popup.js',
        )


class LocationContextFilterAdmin(admin.ModelAdmin):
    list_display = (
        'title',
        'display_order'
    )
    ordering = ('display_order',)


class SurveyAdmin(admin.ModelAdmin):

    date_hierarchy = 'date'
    raw_id_fields = (
        'site',
    )
    search_fields = (
        'site__site_code',
        'uuid'
    )
    list_filter = (
        ('date', DateRangeFilterBuilder()),
    )
    list_display = (
        'id',
        'site',
        'date',
        'validated',
        'owner',
        'link_to_dashboard'
    )


    def link_to_dashboard(self, obj):
        link = reverse('site-visit-detail',
                       args=[obj.id])
        return format_html(
            "<a href='{}'>Dashboard ⎘</a>",
            link,
        )


class SurveyDataValueAdmin(admin.ModelAdmin):
    raw_id_fields = (
        'survey',
    )
    list_display = (
        'survey',
        'survey_data',
        'survey_data_option'
    )


class AlgaeDataAdmin(admin.ModelAdmin):
    raw_id_fields = (
        'survey',
    )
    list_display = (
        'survey',
        'curation_process',
        'indicator_chl_a',
        'indicator_afdm',
        'ai'
    )
    search_fields = (
        'survey__site__site_code',
    )
    list_filter = (
        'indicator_chl_a',
        'indicator_afdm',
    )


class DownloadRequestStatusFilter(django_admin.SimpleListFilter):
    title = _('Download Request Status')

    # Parameter for the filter that will be used in the URL query.
    parameter_name = 'status'

    def lookups(self, request, model_admin):
        """
        Returns a list of tuples. The first element in each
        tuple is the coded value for the option that will
        appear in the URL query. The second element is the
        human-readable name for the option that will appear
        in the right sidebar.
        """
        return (
            ('pending', _('Pending')),
            ('approved', _('Approved')),
            ('rejected', _('Rejected')),
            ('processing', _('Processing')),
        )

    def queryset(self, request, queryset):
        """
        Returns the filtered queryset based on the value
        provided in the query string and retrievable via
        `self.value()`.
        """
        if self.value() == 'processing':
            return queryset.filter(
                processing=True
            ).exclude(
                Q(approved=True) | Q(rejected=True)
            )
        if self.value() == 'approved':
            return queryset.filter(
                approved=True
            )
        if self.value() == 'rejected':
            return queryset.filter(
                rejected=True
            )
        if self.value() == 'pending':
            return queryset.filter(
                Q(progress='') | Q(progress__isnull=True)
            )
        return queryset


class DownloadRequestAdmin(admin.ModelAdmin):
    raw_id_fields = (
        'requester',
        'taxon',
        'survey',
        'location_site'
    )
    list_display = (
        'request_date',
        'requester',
        'resource_type',
        'resource_name',
        'purpose',
        'status'
    )
    list_filter = (
        DownloadRequestStatusFilter,
    )
    actions = ['restart_download']

    def status(self, obj: DownloadRequest):
        if obj.approved:
            return 'Approved'
        if obj.rejected:
            return 'Rejected'
        if obj.processing:
            return 'Processing'
        return 'Pending'

    def restart_download(self, request, queryset):
        import os
        from bims.tasks.collection_record import download_collection_record_task

        restarted = 0
        skipped = 0
        for dr in queryset:
            if (
                dr.resource_name != 'Occurrence Data' or
                dr.resource_type not in ('CSV', 'XLS')
            ):
                skipped += 1
                continue

            if not dr.download_path or not dr.download_params:
                skipped += 1
                continue

            try:
                if os.path.exists(dr.download_path):
                    os.remove(dr.download_path)
            except OSError:
                pass

            dr.progress = None
            dr.progress_updated_at = None
            dr.processing = True
            dr.approved = True
            dr.rejected = False
            dr.rejection_message = ''
            dr.save(update_fields=[
                'progress', 'progress_updated_at', 'processing',
                'approved', 'rejected', 'rejection_message'
            ])

            download_collection_record_task.delay(
                dr.download_path,
                dr.download_params,
                send_email=True,
                user_id=dr.requester_id,
            )
            restarted += 1

        if restarted:
            self.message_user(request, f'{restarted} download(s) restarted.')
        if skipped:
            self.message_user(
                request,
                f'{skipped} download(s) skipped '
                f'(must be Occurrence Data with CSV or XLS type, '
                f'and have a download path and params).',
                level='warning'
            )

    restart_download.short_description = 'Restart download'


class DownloadRequestPurposeAdmin(admin.ModelAdmin):
    list_display = (
        'order',
        'name',
    )


class RequestLogAdmin(admin.ModelAdmin):
    list_display = (
        'user',
        'remote_address',
        'request_path',
        'response_status',
        'run_time'
    )


class IngestedDataAdmin(admin.ModelAdmin):
    list_display = (
        'datetime',
        'is_valid',
        'content_object',
        'get_content_object_id',
        'category',
        'data_key'
    )
    list_filter = (
        'is_valid',
        'category',
    )
    sortable_by = (
        'datetime',
        'is_valid',
        'category',
        'data_key'
    )
    search_fields = (
        'data_key',
    )

    def get_content_object_id(self, obj):
        return obj.content_object.id if obj.content_object else None

    get_content_object_id.short_description = 'Content Object ID'


class UploadSessionAdmin(admin.ModelAdmin):
    list_display = (
        'uploader',
        'module_group',
        'token',
        'uploaded_at',
        'category',
        'processed',
        'canceled'
    )


class LocationContextGroupAdmin(admin.ModelAdmin):
    list_display = (
        'name',
        'key',
        'geocontext_group_key',
        'layer_name',
        'verified'
    )
    search_fields = (
        'name',
        'key',
        'geocontext_group_key'
    )
    actions = ['merge_group']

    def merge_group(self, request, queryset):
        verified = queryset.filter(verified=True)
        if queryset.count() <= 1:
            self.message_user(
                request, 'Need more than 1 group', messages.ERROR
            )
            return
        if not verified.exists():
            self.message_user(
                request, 'Missing verified group', messages.ERROR)
            return
        if verified.count() > 1:
            self.message_user(
                request, 'There are more than 1 verified group',
                messages.ERROR)
            return
        excluded_group = verified[0]
        group_list = queryset.exclude(id=verified[0].id)
        merge_context_group(
            excluded_group=excluded_group,
            group_list=group_list
        )
        self.message_user(request, 'Groups has been merged')

    merge_group.short_description = 'Merge groups'


class WaterTemperatureAdmin(admin.ModelAdmin):
    list_display = (
        'value', 'date_time', 'is_daily', 'maximum', 'minimum'
    )
    raw_id_fields = (
        'location_site', 'uploader', 'owner', 'source_reference'
    )
    search_fields = (
        'location_site__site_code',
    )
    list_filter = (
        'date_time',
    )


class WaterTemperatureThresholdAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'location_site', 'creator'
    )
    raw_id_fields = (
        'location_site', 'creator'
    )


class TaxonExtraAttributeAdmin(admin.ModelAdmin):
    list_display = (
        'name', 'taxon_group'
    )


class DecisionSupportToolNameAdmin(admin.ModelAdmin):
    list_display = (
        'name',
    )


class DecisionSupportToolAdmin(admin.ModelAdmin):
    change_list_template = 'admin/dst_changelist.html'
    list_display = (
        'dst_name',
        'get_bio_uuid'
    )
    raw_id_fields = (
        'biological_collection_record',
    )
    list_filter = (
        'name',
    )
    search_fields = (
        'dst_name__name',
        'biological_collection_record__uuid'
    )

    def get_bio_uuid(self, obj):
        if obj.biological_collection_record:
            return obj.biological_collection_record.uuid
        return '-'

    get_bio_uuid.short_description = 'Biological Collection Record UUID'
    get_bio_uuid.admin_order_field = 'biological_collection_record__uuid'


class UnitAdmin(admin.ModelAdmin):
    list_display = ('unit_name', 'unit',)
    list_filter = ('unit_name', 'unit',)
    search_fields = ('unit_name', 'unit',)


class NotificationAdmin(admin.ModelAdmin):
    list_display = ('name', 'description', 'get_users', 'site')

    def get_users(self, obj):
        return ", ".join([user.username for user in obj.users.all()])
    get_users.short_description = 'Users'

    filter_horizontal = ('users',)


class TaxonImageAdmin(admin.ModelAdmin):
    list_display = ('taxonomy', 'source', 'date')
    raw_id_fields = ('taxonomy', 'uploader', 'owner', 'survey')


class RecordTypeAdmin(OrderedModelAdmin):
    list_display = (
        'name',
        'verified',
        'total_records',
        'is_hidden_in_form',
        'move_up_down_links',
        'order',)
    ordering = ('order',)

    def total_records(self, obj):
        return BiologicalCollectionRecord.objects.filter(
            record_type=obj
        ).count()

    total_records.short_description = 'Total Occurrences'

    actions = ['merge_record_types']

    def merge_record_types(self, request, queryset):
        verified = queryset.filter(verified=True)
        if queryset.count() <= 1:
            self.message_user(
                request, 'Need more than 1 record type', messages.ERROR
            )
            return
        if not verified.exists():
            self.message_user(
                request, 'Missing verified record type', messages.ERROR)
            return
        merge_record_types(
            primary_record_type=verified.first(),
            record_type_list=queryset
        )


    merge_record_types.short_description = 'Merge record types'


class WetlandIndicatorStatusAdmin(OrderedModelAdmin):
    list_display = ('name', 'move_up_down_links', 'order')
    ordering = ('order',)


class HydroperiodAdmin(OrderedModelAdmin):
    list_display = ('name', 'move_up_down_links', 'order')
    ordering = ('order',)


class AbundanceTypeAdmin(OrderedModelAdmin):
    list_display = ('name', 'move_up_down_links', 'order')
    ordering = ('order',)


class SamplingEffortMeasureAdmin(OrderedModelAdmin):
    list_display = ('name', 'move_up_down_links', 'order')
    ordering = ('order',)


class HarvestSessionAdmin(admin.ModelAdmin):
    list_display = (
        'start_time', 'status', 'harvester',
        'module_group',
        'finished', 'canceled', 'is_fetching_species')

    actions = [
        'resume_harvest',
    ]

    autocomplete_fields = [
        'parent_species',
    ]

    def resume_harvest(self, request, queryset):
        from bims.tasks.harvest_collections import (
            harvest_collections
        )
        from bims.tasks.harvest_gbif_species import (
            harvest_gbif_species
        )
        if len(queryset) > 1:
            message = 'You can only resume one session'
            self.message_user(request, message, level=messages.ERROR)
            return

        harvest_session = queryset.first()

        harvest_session.canceled = False
        harvest_session.save()

        schema_name = connection.schema_name

        full_message = 'Resumed'
        if harvest_session.is_fetching_species:
            harvest_gbif_species.delay(
                harvest_session.id,
                schema_name
            )
        else:
            harvest_collections.delay(
                harvest_session.id,
                True,
                schema_name=schema_name
            )

        self.message_user(request, full_message)


class TaxonGroupTaxonomyAdmin(admin.ModelAdmin):
    raw_id_fields = (
        'taxonomy',
    )
    list_display = (
        'taxongroup',
        'taxonomy',
        'is_validated'
    )
    search_fields = ('taxonomy__scientific_name', 'taxonomy__canonical_name')
    actions = ('create_proposal', )

    def create_proposal(self, request, queryset):
        """Action to create proposal"""
        unvalidated = queryset.filter(is_validated=False, is_rejected=False)
        for data in unvalidated:
            create_taxon_proposal(data.taxonomy, data.taxongroup)

        self.message_user(
            request,
            'Proposal has been created for {} data'.format(
                unvalidated.count()))


class TaxonomyUpdateProposalAdmin(admin.ModelAdmin):
    raw_id_fields = (
        'original_taxonomy',
        'accepted_taxonomy',
        'parent',
        'owner',
        'collector_user',
        'analyst'
    )
    list_display = (
        'original_taxonomy',
        'scientific_name',
        'canonical_name',
        'status',
        'created_at'
    )
    autocomplete_fields = (
        'vernacular_names',
    )
    search_fields = (
        'scientific_name',
        'canonical_name'
    )


class TaxonomyUpdateReviewerAdmin(admin.ModelAdmin):
    list_display = (
        'taxonomy_update_proposal',
        'reviewer',
        'status'
    )


@admin.register(CITESListingInfo)
class CITESListingInfoAdmin(admin.ModelAdmin):
    list_display = ('appendix', 'effective_at', 'taxonomy')
    list_filter = ('appendix', 'effective_at')
    search_fields = ('annotation', 'taxonomy__name')
    raw_id_fields = ('taxonomy',)


class RoleAdminForm(forms.ModelForm):
    class Meta:
        model = Role
        fields = ['display_name']


@admin.register(Role)
class RoleAdmin(OrderedModelAdmin):
    list_display = ('display_name', 'move_up_down_links')
    ordering = ('order',)
    form = RoleAdminForm


@admin.register(ImportTask)
class ImportTaskAdmin(admin.ModelAdmin):
    list_display = ('celery_task_id', 'in_progress', 'created_at', 'updated_at')
    list_filter = ('in_progress', 'created_at', 'updated_at')
    search_fields = ('celery_task_id',)


class FlatPageExtensionInline(admin.StackedInline):
    model = FlatPageExtension
    can_delete = False
    verbose_name_plural = 'Custom Options'
    fk_name = 'flatpage'


class ExtendedFlatPageAdmin(CustomCKEditorAdmin, FlatPageAdmin):
    inlines = (FlatPageExtensionInline,)

    fieldsets = (
        (None, {
            "fields": ("url", "title", "content"),
        }),
        ("Advanced options", {
            "classes": ("collapse",),
            "fields": ("registration_required", "template_name"),
        }),
    )

    formfield_overrides = {
        models.TextField: {
            'widget': DynamicCKEditorUploadingWidget}
    }

    def save_related(self, request, form, formsets, change):
        super().save_related(request, form, formsets, change)
        form.instance.sites.set(Site.objects.all())

    def show_in_navbar(self, instance):
        return instance.extension.show_in_navbar
    show_in_navbar.short_description = "Show in Navbar"
    show_in_navbar.boolean = True

    def display_order(self, instance):
        return instance.extension.display_order
    display_order.short_description = "Display Order"

    list_display = ('title', 'url', 'show_in_navbar', 'display_order')


class DatasetAdmin(admin.ModelAdmin):
    list_display = ('uuid', 'name', 'abbreviation')
    search_fields = ('uuid', 'name')
    change_list_template = "admin/dataset_changelist.html"

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('fetch-datasets/', self.fetch_datasets, name='fetch_datasets'),
        ]
        return custom_urls + urls

    def fetch_datasets(self, request):
        from bims.tasks.dataset import retrieve_datasets_from_gbif
        retrieve_datasets_from_gbif.delay()
        self.message_user(request, 'Fetching datasets in background')
        return HttpResponseRedirect(reverse('admin:bims_dataset_changelist'))

class TagGroupAdmin(OrderedModelAdmin):
    ordering = ('order',)
    list_display = ('id', 'move_up_down_links', 'name', 'colour')


class LayerGroupAdminForm(forms.ModelForm):
    order_manual = forms.IntegerField(label="Order", required=False)

    class Meta:
        model = LayerGroup
        fields = ('name', 'slug', 'description', 'layers')   # no “order” here

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance.pk:
            self.fields['order_manual'].initial = self.instance.order

    def save(self, commit=True):
        obj = super().save(commit=False)
        obj.order = self.cleaned_data.get('order_manual') or obj.order
        if commit:
            obj.save()
        return obj


@admin.register(LayerGroup)
class LayerGroupAdmin(OrderedModelAdmin):
    form = LayerGroupAdminForm
    list_display = ('name', 'order', 'move_up_down_links')
    filter_horizontal = ('layers',)
    ordering = ('order',)


@admin.register(SpeciesGroup)
class SpeciesGroupAdmin(admin.ModelAdmin):
    list_display = ("name",)
    search_fields = ("name",)


# Re-register GeoNode's Profile page
admin.site.unregister(Profile)
admin.site.register(Profile, CustomUserAdmin)

admin.site.register(LocationSite, LocationSiteAdmin)
admin.site.register(LocationType)
admin.site.register(IUCNStatus, IUCNStatusAdmin)
admin.site.register(IUCNAssessment, IUCNAssessmentAdmin)
admin.site.register(Endemism, EndemismAdmin)
admin.site.register(Survey, SurveyAdmin)
admin.site.register(SurveyData)
admin.site.register(SurveyDataOption)
admin.site.register(SurveyDataValue, SurveyDataValueAdmin)
admin.site.register(NonBiodiversityLayer, NonBiodiversityLayerAdmin)
admin.site.register(Taxonomy, TaxonomyAdmin)
admin.site.register(TaxonGroup, TaxonGroupAdmin)

admin.site.register(Boundary, BoundaryAdmin)
admin.site.register(BoundaryType, admin.ModelAdmin)
admin.site.register(Cluster, ClusterAdmin)
admin.site.register(BiologicalCollectionRecord, BiologicalCollectionAdmin)

admin.site.register(ShapefileUploadSession, ShapefileUploadSessionAdmin)
admin.site.register(Shapefile, ShapefileAdmin)

admin.site.register(Visitor, VisitorAdmin)
admin.site.register(Permission, PermissionAdmin)

admin.site.register(UserBoundary, UserBoundaryAdmin)
admin.site.register(SearchProcess, SearchProcessAdmin)
admin.site.register(DataSource, DataSourceAdmin)

admin.site.register(ReferenceLink, ReferenceLinkAdmin)
admin.site.register(VernacularName, VernacularNameAdmin)
admin.site.register(RiverCatchment, RiverCatchmentAdmin)
admin.site.register(FbisUUID, FbisUUIDAdmin)
admin.site.register(Biotope, SassBiotopeAdmin)
admin.site.register(SpatialScale, SpatialScaleAdmin)
admin.site.register(SpatialScaleGroup, SpatialScaleGroupAdmin)
admin.site.register(SamplingMethod, SamplingMethodAdmin)
admin.site.register(SiteImage, SiteImageAdmin)
admin.site.register(SiteSetting, SiteSettingAdmin)
admin.site.register(GeocontextSetting, PreferencesAdmin)
admin.site.register(ChemicalRecord, ChemicalRecordAdmin)
admin.site.register(Chem, ChemAdmin)
admin.site.register(UploadSession, UploadSessionAdmin)
admin.site.register(HarvestSession, HarvestSessionAdmin)
admin.site.register(DashboardConfiguration)
admin.site.register(DownloadRequest, DownloadRequestAdmin)
admin.site.register(DownloadRequestPurpose, DownloadRequestPurposeAdmin)
admin.site.register(BaseMapLayer, BaseMapLayerAdmin)
admin.site.register(RequestLog, RequestLogAdmin)
admin.site.register(IngestedData, IngestedDataAdmin)

admin.site.register(TaxonImage, TaxonImageAdmin)

admin.site.register(LocationContextGroup, LocationContextGroupAdmin)
admin.site.register(
    LocationContextFilterGroupOrder, LocationContextFilterGroupOrderAdmin)
admin.site.register(LocationContextFilter, LocationContextFilterAdmin)

# Hide upload files from geonode in admin
# admin.site.unregister(Upload)
# admin.site.unregister(UploadFile)

# Rerender geonode document admin
admin.site.unregister(Document)
admin.site.register(Document, BimsDocumentAdmin)

admin.site.register(AlgaeData, AlgaeDataAdmin)

if TRACK_PAGEVIEWS:
    admin.site.register(Pageview, PageviewAdmin)

from bims.custom_admin import *  # noqa

admin.site.register(WaterTemperature, WaterTemperatureAdmin)
admin.site.register(TaxonExtraAttribute, TaxonExtraAttributeAdmin)
admin.site.register(DecisionSupportTool, DecisionSupportToolAdmin)
admin.site.register(Unit, UnitAdmin)
admin.site.register(BimsProfile, BimsProfileAdmin)
admin.site.register(
    DecisionSupportToolName, DecisionSupportToolNameAdmin)
admin.site.register(
    WaterTemperatureThreshold, WaterTemperatureThresholdAdmin
)
admin.site.register(
    Notification,
    NotificationAdmin
)
admin.site.register(RecordType, RecordTypeAdmin)
admin.site.register(
    WetlandIndicatorStatus, WetlandIndicatorStatusAdmin)
admin.site.register(
    Hydroperiod,
    HydroperiodAdmin
)
admin.site.register(
    AbundanceType,
    AbundanceTypeAdmin,
)
admin.site.register(
    SamplingEffortMeasure,
    SamplingEffortMeasureAdmin
)
admin.site.register(
    TaxonGroupTaxonomy,
    TaxonGroupTaxonomyAdmin
)
admin.site.register(
    TaxonomyUpdateProposal,
    TaxonomyUpdateProposalAdmin
)
admin.site.register(
    TaxonomyUpdateReviewer,
    TaxonomyUpdateReviewerAdmin
)
admin.site.register(
    Invasion,
    InvasionAdmin
)
admin.site.register(
    TaxonOrigin,
    TaxonOriginAdmin
)

admin.site.unregister(FlatPage)
admin.site.register(FlatPage, ExtendedFlatPageAdmin)
admin.site.register(TagGroup, TagGroupAdmin)
admin.site.register(Dataset, DatasetAdmin)

admin.site.unregister(Tag)
@admin.register(Tag)
class TagAdmin(TagAdmin):
    inlines = []

@admin.register(TaxonGroupCitation)
class TaxonGroupCitationAdmin(admin.ModelAdmin):
    list_display = ("taxon_group", "authors", "year", "access_date", "formatted_citation")
    autocomplete_fields = ("taxon_group",)
    search_fields = ("taxon_group__name", "authors", "citation_text")
    exclude = ("created_at", "updated_at")

    def formatted_citation(self, obj):
        return obj.formatted_citation

    formatted_citation.short_description = "Citation"


@admin.register(HarvestSchedule)
class HarvestScheduleAdmin(admin.ModelAdmin):
    form = HarvestScheduleAdminForm

    list_display = (
        "module_group",
        "parent_species_display",
        "harvest_type_display",
        "enabled",
        "period",
        "schedule_human",
        "timezone",
        "last_harvest_until_local",
        "updated_at",
    )
    list_filter = ("enabled", "is_fetching_species", "period", "timezone")
    search_fields = ("module_group__name",)
    autocomplete_fields = ("module_group", "parent_species")
    readonly_fields = ("last_harvest_until", "updated_at", "schedule_preview")
    actions = ["action_run_now", "action_enable", "action_disable"]

    fieldsets = (
        ("Target", {
            "fields": ("module_group", "parent_species", "enabled"),
            "description": (
                "Select a module group and optionally override the parent species. "
                "If parent species is not set, the module group's default GBIF parent will be used."
            ),
        }),
        ("When to run", {
            "fields": (
                "period",
                "run_at",
                "day_of_week",
                "day_of_month",
                "cron_expression",
                "timezone",
                "schedule_preview",
            ),
            "description": (
                "Daily/Weekly/Monthly use run_at (+ day fields). "
                "Custom uses standard 5-field cron: 'm h dom mon dow' (e.g. '15 */6 * * *'). "
                "Day of week accepts 'mon,tue' or '0-6' (0=Sunday)."
            ),
        }),
        ("Defaults for the job", {
            "fields": ("harvest_synonyms_default", "boundary_default", "is_fetching_species"),
        }),
        ("Watermark & audit", {
            "fields": ("last_harvest_until", "updated_at"),
        }),
    )

    def harvest_type_display(self, obj: HarvestSchedule):
        return "Species" if obj.is_fetching_species else "Occurrences"

    harvest_type_display.short_description = "Type"

    def parent_species_display(self, obj: HarvestSchedule):
        if obj.parent_species:
            return obj.parent_species.canonical_name
        if obj.module_group and obj.module_group.gbif_parent_species:
            return f"({obj.module_group.gbif_parent_species.canonical_name})"
        return "—"

    parent_species_display.short_description = "Parent Species"

    def schedule_human(self, obj: HarvestSchedule):
        if obj.period == HarvestPeriod.CUSTOM and obj.cron_expression:
            return obj.cron_expression
        if obj.period == HarvestPeriod.DAILY:
            return f"Daily at {obj.run_at}"
        if obj.period == HarvestPeriod.WEEKLY:
            return f"Weekly {obj.day_of_week or 'mon'} at {obj.run_at}"
        if obj.period == HarvestPeriod.MONTHLY:
            return f"Monthly day {obj.day_of_month or 1} at {obj.run_at}"
        return "-"

    schedule_human.short_description = "Schedule"

    def last_harvest_until_local(self, obj):
        return localtime(
            obj.last_harvest_until
        ).strftime("%Y-%m-%d %H:%M") if obj.last_harvest_until else "—"
    last_harvest_until_local.short_description = "Last watermark"

    def schedule_preview(self, obj):
        text = self.schedule_human(obj)
        return format_html("<code>{}</code>", text) if text else "—"
    schedule_preview.short_description = "Schedule preview"

    @admin.action(description="Run now (enqueue Celery task)")
    def action_run_now(self, request, queryset):
        count = 0
        for sched in queryset:
            schema_name = str(connection.schema_name)
            run_scheduled_gbif_harvest.delay(schema_name, sched.id)
            count += 1
        self.message_user(
            request, f"Queued {count} run(s).", messages.SUCCESS)

    @admin.action(description="Enable schedule")
    def action_enable(self, request, queryset):
        updated = queryset.update(enabled=True)
        self.message_user(
            request, f"Enabled {updated} schedule(s).", messages.SUCCESS)

    @admin.action(description="Disable schedule")
    def action_disable(self, request, queryset):
        updated = queryset.update(enabled=False)
        self.message_user(
            request, f"Disabled {updated} schedule(s).", messages.SUCCESS)


@admin.register(UploadType)
class UploadTypeAdmin(admin.ModelAdmin):
    list_display = ('order', 'name', 'code')
    search_fields = ('name', 'code', 'description')
    ordering = ('order',)


@admin.register(UploadRequest)
class UploadRequestAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'created_at', 'name', 'email', 'upload_type', 'status', 'github_issue_number',
        'gbif_consent'
    )
    list_filter = ('upload_type', 'status', 'created_at', 'gbif_consent')
    search_fields = ('name', 'email', 'notes', 'github_issue_url')
    readonly_fields = ('created_at', 'updated_at', 'client_ip', 'github_issue_number', 'github_issue_url')


@admin.register(CertaintyHierarchy)
class CertaintyHierarchyAdmin(OrderedModelAdmin):
    ordering = ('order',)
    list_display = ('id', 'move_up_down_links', 'name')
@admin.register(FilterPanelInfo)
class FilterPanelInfoAdmin(admin.ModelAdmin):
    list_display = ('title', 'is_active', 'updated_at')
    list_filter = ('is_active',)
    search_fields = ('title', 'description')


@admin.register(TaxonTag)
class TaxonTagAdmin(admin.ModelAdmin):
    list_display = ('name', 'doubtful')
    search_fields = ('name',)
    list_filter = ('doubtful',)


@admin.register(TaxonTagDescription)
class TaxonTagDescriptionAdmin(admin.ModelAdmin):
    list_display = ('tag', 'description_preview')
    search_fields = ('tag__name', 'description')
    autocomplete_fields = ('tag',)

    def description_preview(self, obj):
        if obj.description:
            return obj.description[:100] + '...' if len(obj.description) > 100 else obj.description
        return '-'
    description_preview.short_description = 'Description'
