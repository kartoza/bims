from django.contrib import admin
from climate.models import Climate


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
