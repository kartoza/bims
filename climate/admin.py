from django.contrib import admin, messages
from django.contrib.admin.actions import delete_selected as django_delete_selected
from django.contrib.admin.utils import NestedObjects, quote
from django.core.exceptions import PermissionDenied
from django.db import router
from django.db.models import Count, OuterRef, Subquery
from django.db.models.functions import Coalesce
from django.http import HttpResponseRedirect
from django.template.response import TemplateResponse
from django.urls import NoReverseMatch, path, reverse
from django.utils.html import format_html
from django.utils.text import capfirst

from bims.models.location_site import LocationSite
from climate.models import Climate, ClimateStation
from climate.tasks.climate_station_delete import delete_climate_stations
from climate.utils import merge_climate_stations


class ClimateCountCollector(NestedObjects):
    """
    Admin delete collector that counts climate data instead of loading
    every record, which can be thousands per station.
    """

    def can_fast_delete(self, objs, from_field=None):
        # Called with the related model class or a queryset
        if objs is Climate or getattr(objs, 'model', None) is Climate:
            return True
        return super().can_fast_delete(objs, from_field=from_field)


@admin.register(Climate)
class ClimateAdmin(admin.ModelAdmin):
    """Admin interface for Climate model."""

    list_display = [
        'location_site',
        'station_name',
        'date',
        'avg_temperature',
        'daily_rainfall',
        'avg_humidity',
        'avg_windspeed',
        'flag'
    ]

    list_filter = [
        'year',
        'month',
        'station_name'
    ]

    search_fields = [
        'location_site__name',
        'location_site__site_code',
        'station_name'
    ]

    date_hierarchy = 'date'

    readonly_fields = ['created_at', 'updated_at', 'year', 'month', 'day']

    fieldsets = (
        ('Location Information', {
            'fields': ('location_site', 'station_name')
        }),
        ('Date Information', {
            'fields': ('date', 'year', 'month', 'day')
        }),
        ('Temperature Data (°C)', {
            'fields': ('avg_temperature', 'max_temperature', 'min_temperature')
        }),
        ('Humidity Data (%)', {
            'fields': ('avg_humidity', 'max_humidity', 'min_humidity')
        }),
        ('Other Measurements', {
            'fields': ('avg_windspeed', 'daily_rainfall', 'flag')
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )

    ordering = ['-date']

    def get_queryset(self, request):
        """Optimize queryset with select_related."""
        qs = super().get_queryset(request)
        return qs.select_related('location_site')


@admin.register(ClimateStation)
class ClimateStationAdmin(admin.ModelAdmin):
    """Admin interface for merging climate stations (LocationSite proxy)."""

    list_display = ['name', 'site_code', 'latitude', 'longitude', 'climate_record_count']
    search_fields = ['name', 'site_code']
    actions = ['merge_stations_action', 'delete_selected']
    readonly_fields = ['site_code', 'latitude', 'longitude']

    def get_queryset(self, request):
        # Collect the station ids first: filtering location sites on
        # "weather station OR has climate data" scans every location site
        station_ids = set(
            Climate.objects.order_by().values_list(
                'location_site_id', flat=True).distinct()
        )
        station_ids.update(
            LocationSite.objects.filter(
                location_type__name__iexact='Weather Station'
            ).values_list('id', flat=True)
        )
        return super().get_queryset(request).filter(
            id__in=station_ids
        ).annotate(
            climate_record_total=Coalesce(
                Subquery(
                    Climate.objects.filter(
                        location_site=OuterRef('pk')
                    ).order_by().values('location_site').annotate(
                        # Counting the indexed column avoids heap reads
                        total=Count('location_site')
                    ).values('total')
                ),
                0
            )
        )

    @admin.display(description='Records', ordering='climate_record_total')
    def climate_record_count(self, obj):
        return obj.climate_record_total

    def has_add_permission(self, request):
        return False

    def get_deleted_objects(self, objs, request):
        """
        Same as the default admin summary, except climate data is
        counted instead of listed one by one.
        """
        try:
            obj = objs[0]
        except IndexError:
            return [], {}, set(), []
        collector = ClimateCountCollector(
            using=router.db_for_write(obj._meta.model), origin=objs)
        collector.collect(objs)
        perms_needed = set()

        def format_callback(obj):
            opts = obj._meta
            no_edit_link = f'{capfirst(opts.verbose_name)}: {obj}'
            if not self.admin_site.is_registered(obj.__class__):
                return no_edit_link
            if not self.admin_site.get_model_admin(
                    obj.__class__).has_delete_permission(request, obj):
                perms_needed.add(opts.verbose_name)
            try:
                admin_url = reverse(
                    f'{self.admin_site.name}:'
                    f'{opts.app_label}_{opts.model_name}_change',
                    None,
                    (quote(obj.pk),),
                )
            except NoReverseMatch:
                return no_edit_link
            return format_html(
                '{}: <a href="{}">{}</a>',
                capfirst(opts.verbose_name), admin_url, obj)

        to_delete = collector.nested(format_callback)
        protected = [format_callback(obj) for obj in collector.protected]
        model_count = {
            model._meta.verbose_name_plural: len(model_objs)
            for model, model_objs in collector.model_objs.items()
        }

        total_climate_data = sum(
            qs.count() for qs in collector.fast_deletes
            if getattr(qs, 'model', None) is Climate
        )
        if total_climate_data:
            if not self.admin_site.get_model_admin(
                    Climate).has_delete_permission(request):
                perms_needed.add(Climate._meta.verbose_name)
            model_count[Climate._meta.verbose_name_plural] = (
                total_climate_data)
            to_delete.append(
                f'{capfirst(Climate._meta.verbose_name_plural)}: '
                f'{total_climate_data} record(s), '
                f'deleted in the background')
        return to_delete, model_count, perms_needed, protected

    def delete_stations_in_background(self, request, station_ids):
        delete_climate_stations.delay(list(station_ids))
        self.message_user(
            request,
            f'Deleting {len(station_ids)} climate station(s) and their '
            f'climate data in the background. They will be removed from '
            f'the list once finished.',
            messages.SUCCESS
        )

    def delete_model(self, request, obj):
        self.delete_stations_in_background(request, [obj.pk])

    def delete_queryset(self, request, queryset):
        self.delete_stations_in_background(
            request, list(queryset.values_list('pk', flat=True)))

    def change_view(self, request, object_id, form_url='', extra_context=None):
        """Add what will be deleted, so the delete button can ask for
        confirmation in a dialog instead of the confirmation page."""
        extra_context = extra_context or {}
        obj = self.get_object(request, object_id)
        if obj and self.has_delete_permission(request, obj):
            _, model_count, perms_needed, protected = (
                self.get_deleted_objects([obj], request))
            # Keep the confirmation page when the deletion is not allowed
            if not perms_needed and not protected:
                extra_context['delete_dialog'] = {
                    'station': str(obj),
                    'url': reverse(
                        'admin:climate_climatestation_delete',
                        args=[quote(obj.pk)]),
                    'related': [
                        f'{count} {name}'
                        for name, count in model_count.items()
                        if name != self.model._meta.verbose_name_plural
                    ],
                }
        return super().change_view(
            request, object_id, form_url, extra_context=extra_context)

    def response_delete(self, request, obj_display, obj_id):
        # delete_model already added the message
        return HttpResponseRedirect(
            reverse('admin:climate_climatestation_changelist'))

    @admin.action(
        permissions=['delete'],
        description=django_delete_selected.short_description
    )
    def delete_selected(self, request, queryset):
        """Default delete action, without its "Successfully deleted"
        message, since stations are deleted in the background."""
        if request.POST.get('post'):
            _, _, perms_needed, protected = self.get_deleted_objects(
                queryset, request)
            if perms_needed:
                raise PermissionDenied
            if not protected:
                self.log_deletions(request, queryset)
                self.delete_queryset(request, queryset)
                return None
        return django_delete_selected(self, request, queryset)

    def merge_stations_action(self, request, queryset):
        if queryset.count() < 2:
            self.message_user(
                request,
                'Select at least 2 stations to merge.',
                messages.ERROR
            )
            return
        selected = ','.join(str(pk) for pk in queryset.values_list('pk', flat=True))
        return HttpResponseRedirect(f'merge/?ids={selected}')
    merge_stations_action.short_description = 'Merge selected climate stations'

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                'merge/',
                self.admin_site.admin_view(self.merge_view),
                name='climate_climatestation_merge',
            ),
        ]
        return custom_urls + urls

    def merge_view(self, request):
        ids_param = request.GET.get('ids', '') or request.POST.get('ids', '')
        id_list = [
            int(i) for i in ids_param.split(',') if i.strip().isdigit()
        ]
        stations = ClimateStation.objects.filter(pk__in=id_list)

        if stations.count() < 2:
            self.message_user(
                request,
                'Select at least 2 stations to merge.',
                messages.ERROR
            )
            return HttpResponseRedirect('../')

        if request.method == 'POST':
            primary_id = request.POST.get('primary_station')
            if not primary_id:
                self.message_user(
                    request,
                    'Please select a primary station.',
                    messages.ERROR
                )
            else:
                try:
                    primary = ClimateStation.objects.get(pk=primary_id)
                except ClimateStation.DoesNotExist:
                    self.message_user(request, 'Primary station not found.', messages.ERROR)
                    return HttpResponseRedirect('../')

                secondaries = list(stations.exclude(pk=primary_id))
                secondary_count = len(secondaries)

                new_lat = request.POST.get('latitude', '').strip()
                new_lon = request.POST.get('longitude', '').strip()
                if new_lat and new_lon:
                    try:
                        from django.contrib.gis.geos import Point
                        lat = float(new_lat)
                        lon = float(new_lon)
                        primary.latitude = lat
                        primary.longitude = lon
                        primary.geometry_point = Point(lon, lat)
                        primary.save()
                    except (ValueError, Exception):
                        self.message_user(
                            request,
                            'Invalid coordinates - skipping coordinate update.',
                            messages.WARNING
                        )

                merge_climate_stations(primary, secondaries)
                self.message_user(
                    request,
                    f'Successfully merged {secondary_count} station(s) into "{primary.name}".',
                )
                return HttpResponseRedirect('../')

        context = {
            **self.admin_site.each_context(request),
            'title': 'Merge Climate Stations',
            'stations': stations,
            'ids': ids_param,
            'opts': self.model._meta,
        }
        return TemplateResponse(
            request,
            'admin/climate/climatestation/merge_stations.html',
            context,
        )
