from django.contrib import admin
from django.db.models import Count, OuterRef, Subquery
from django.db.models.functions import Coalesce

from groundwater.models import GroundwaterReading, GroundwaterStation


@admin.register(GroundwaterReading)
class GroundwaterReadingAdmin(admin.ModelAdmin):
    list_display = [
        'location_site', 'timestamp', 'water_level',
        'water_temperature', 'electrical_conductivity', 'flag'
    ]
    search_fields = ['location_site__name', 'location_site__site_code']
    raw_id_fields = ['location_site']
    date_hierarchy = 'timestamp'


@admin.register(GroundwaterStation)
class GroundwaterStationAdmin(admin.ModelAdmin):
    list_display = ['name', 'site_code', 'latitude', 'longitude',
                    'reading_count']
    search_fields = ['name', 'site_code']
    readonly_fields = ['site_code', 'latitude', 'longitude']

    def get_queryset(self, request):
        station_ids = set(
            GroundwaterReading.objects.order_by().values_list(
                'location_site_id', flat=True).distinct()
        )
        return super().get_queryset(request).filter(
            id__in=station_ids
        ).annotate(
            reading_total=Coalesce(
                Subquery(
                    GroundwaterReading.objects.filter(
                        location_site=OuterRef('pk')
                    ).order_by().values('location_site').annotate(
                        total=Count('location_site')
                    ).values('total')
                ),
                0
            )
        )

    @admin.display(description='Readings', ordering='reading_total')
    def reading_count(self, obj):
        return obj.reading_total

    def has_add_permission(self, request):
        return False
