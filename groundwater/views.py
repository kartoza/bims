import csv
import json
import time
from datetime import datetime, time as dt_time

from django.core.serializers.json import DjangoJSONEncoder
from django.db.models import Avg, Count, Max, Min
from django.http import Http404, HttpResponse
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.views.generic import TemplateView, View

from bims.models.location_site import LocationSite
from bims.utils.location_site import overview_site_detail
from climate.views import can_edit_station
from groundwater.models import GroundwaterReading
from groundwater.utils import (
    availability, build_chart_payload, monthly_summary, summary_stats,
    HOURLY_WINDOW_DAYS,
)


def _is_private_user(user):
    return user.is_authenticated and user.groups.filter(
        name='PrivateDataGroup').exists()


def _parse_date(value):
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except (TypeError, ValueError):
        return None


def _filter_by_dates(queryset, start_date, end_date):
    """Limit readings to the inclusive date range."""
    tz = timezone.get_current_timezone()
    if start_date:
        queryset = queryset.filter(
            timestamp__gte=timezone.make_aware(
                datetime.combine(start_date, dt_time.min), tz))
    if end_date:
        queryset = queryset.filter(
            timestamp__lte=timezone.make_aware(
                datetime.combine(end_date, dt_time.max), tz))
    return queryset


class GroundwaterSiteView(TemplateView):
    """Dashboard of the hourly groundwater data of one station."""
    template_name = 'groundwater/single_site.html'

    def get_context_data(self, **kwargs):
        start_time = time.time()
        context = super().get_context_data(**kwargs)
        site = get_object_or_404(LocationSite, pk=kwargs['site_id'])

        all_readings = GroundwaterReading.objects.filter(location_site=site)
        if not all_readings.exists():
            raise Http404('No groundwater data available for this station')

        start_date = _parse_date(self.request.GET.get('startDate'))
        end_date = _parse_date(self.request.GET.get('endDate'))
        readings = _filter_by_dates(all_readings, start_date, end_date)

        stats = summary_stats(readings)
        stats_overall = summary_stats(all_readings)

        site_details = overview_site_detail(site.id)
        site_overview = site_details.get('Site details', {})
        site_overview.pop('User Site Code', None)

        context.update({
            'location_site': site,
            'site_id': site.id,
            'site_code': site.site_code or site.name,
            'coord': [site.get_centroid().x, site.get_centroid().y],
            'site_overview': site_overview,
            'site_details': json.dumps(site_details),
            'stats': stats,
            'stats_overall': stats_overall,
            'availability': availability(stats),
            'chart_payload': json.dumps(
                build_chart_payload(readings), cls=DjangoJSONEncoder),
            'monthly_records': monthly_summary(readings),
            'start_date': start_date,
            'end_date': end_date,
            'data_start_date': stats_overall['first'],
            'data_end_date': stats_overall['last'],
            'hourly_window_days': HOURLY_WINDOW_DAYS,
            'can_edit_station': can_edit_station(self.request.user, site),
            'is_private_user': _is_private_user(self.request.user),
            'execution_time': time.time() - start_time,
        })
        return context


class GroundwaterDownloadView(View):
    """Download the hourly readings of a station as CSV."""

    def get(self, request, site_id):
        if not _is_private_user(request.user):
            return HttpResponse(status=403)
        site = get_object_or_404(LocationSite, pk=site_id)
        readings = _filter_by_dates(
            GroundwaterReading.objects.filter(location_site=site),
            _parse_date(request.GET.get('startDate')),
            _parse_date(request.GET.get('endDate')),
        ).order_by('timestamp')

        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = (
            f'attachment; filename="groundwater_{site.site_code or site.id}'
            f'.csv"'
        )
        writer = csv.writer(response)
        writer.writerow([
            'Timestamp', 'Water level (m below ground)',
            'Water temperature (°C)', 'Electrical conductivity (µS/cm)',
            'Flag'
        ])
        for reading in readings.iterator():
            writer.writerow([
                timezone.localtime(reading.timestamp).isoformat(),
                reading.water_level, reading.water_temperature,
                reading.electrical_conductivity, reading.flag
            ])
        return response


class GroundwaterDashboardMultipleSitesView(TemplateView):
    """Summary of all the stations that have groundwater data."""
    template_name = 'groundwater/multi_site.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        rows = GroundwaterReading.objects.order_by().values(
            'location_site_id'
        ).annotate(
            readings=Count('id'),
            first=Min('timestamp'),
            last=Max('timestamp'),
            level_min=Min('water_level'),
            level_avg=Avg('water_level'),
            level_max=Max('water_level'),
            temperature_avg=Avg('water_temperature'),
            conductivity_avg=Avg('electrical_conductivity'),
        )
        by_site = {row['location_site_id']: row for row in rows}
        sites = LocationSite.objects.filter(
            id__in=by_site.keys()).order_by('site_code', 'name')
        stations = []
        for site in sites:
            row = by_site[site.id]
            stations.append({
                'id': site.id,
                'name': site.name,
                'site_code': site.site_code or site.name,
                'readings': row['readings'],
                'first': row['first'],
                'last': row['last'],
                'level_min': _r(row['level_min']),
                'level_avg': _r(row['level_avg']),
                'level_max': _r(row['level_max']),
                'temperature_avg': _r(row['temperature_avg']),
                'conductivity_avg': _r(row['conductivity_avg']),
            })
        context['stations'] = stations
        return context


def _r(value):
    return round(value, 2) if value is not None else None
