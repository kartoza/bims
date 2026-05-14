import csv
import hashlib
import json
import time
from collections import OrderedDict
from datetime import date, datetime

from django.core.serializers.json import DjangoJSONEncoder
from django.http import HttpResponse, Http404
from django.shortcuts import render, get_object_or_404
from django.views.generic import TemplateView
from django.db.models import Avg, Min, Max, Sum, Count, Value
from django.db.models.functions import NullIf
from rest_framework.views import APIView
from rest_framework.response import Response

from .models import Climate
from bims.models.location_site import LocationSite
from bims.models.location_context import LocationContext
from bims.models.search_process import (
    SEARCH_RESULTS,
    SEARCH_PROCESSING,
)
from bims.utils.location_site import overview_site_detail
from bims.utils.search_process import get_or_create_search_process
from bims.views.data_upload import DataUploadView
from climate.tasks.climate_upload import climate_upload
from climate.tasks.climate_multi_site import climate_multi_site_summary

# Sentinel value used in uploaded CSVs to indicate truly missing data.
MISSING = -999.0


def _is_valid(value):
    """Return True when *value* is a real measurement."""
    return value is not None and value != MISSING


EXTREME_HEADERS = [
    ('hot_days', 'Days above 35°C'),
    ('cold_days', 'Days below 0°C'),
    ('heavy_rain_days', 'Rain days >20mm'),
    ('zero_rain_days', '0 Rainfall days'),
    ('consecutive_dry_days', 'Consecutive dry days'),
]


def _init_month_bucket():
    return {
        'temperature': {'min': None, 'max': None, 'avg_sum': 0.0, 'avg_count': 0},
        'humidity': {'min': None, 'max': None, 'avg_sum': 0.0, 'avg_count': 0},
        'rainfall': {
            'min': None,
            'max': None,
            'avg_sum': 0.0,
            'avg_count': 0,
            'total': 0.0,
        },
        'wind': {'max': None, 'avg_sum': 0.0, 'avg_count': 0},
        'extremes': {
            'hot_days': 0,
            'cold_days': 0,
            'heavy_rain_days': 0,
            'zero_rain_days': 0,
            'consecutive_dry_days': 0,
            'current_dry_streak': 0,
        }
    }


def _accumulate_month_bucket(bucket, record):
    temperature = bucket['temperature']
    if _is_valid(record.min_temperature):
        temperature['min'] = (
            record.min_temperature
            if temperature['min'] is None
            else min(temperature['min'], record.min_temperature)
        )
    if _is_valid(record.max_temperature):
        temperature['max'] = (
            record.max_temperature
            if temperature['max'] is None
            else max(temperature['max'], record.max_temperature)
        )
    if _is_valid(record.avg_temperature):
        temperature['avg_sum'] += record.avg_temperature
        temperature['avg_count'] += 1

    humidity = bucket['humidity']
    if _is_valid(record.min_humidity):
        humidity['min'] = (
            record.min_humidity
            if humidity['min'] is None
            else min(humidity['min'], record.min_humidity)
        )
    if _is_valid(record.max_humidity):
        humidity['max'] = (
            record.max_humidity
            if humidity['max'] is None
            else max(humidity['max'], record.max_humidity)
        )
    if _is_valid(record.avg_humidity):
        humidity['avg_sum'] += record.avg_humidity
        humidity['avg_count'] += 1

    rainfall = bucket['rainfall']
    rainfall_value = record.daily_rainfall
    if _is_valid(rainfall_value):
        rainfall['min'] = (
            rainfall_value
            if rainfall['min'] is None
            else min(rainfall['min'], rainfall_value)
        )
        rainfall['max'] = (
            rainfall_value
            if rainfall['max'] is None
            else max(rainfall['max'], rainfall_value)
        )
        rainfall['avg_sum'] += rainfall_value
        rainfall['avg_count'] += 1
        rainfall['total'] += rainfall_value

    wind = bucket['wind']
    if _is_valid(record.avg_windspeed):
        wind['avg_sum'] += record.avg_windspeed
        wind['avg_count'] += 1
        wind['max'] = (
            record.avg_windspeed
            if wind['max'] is None
            else max(wind['max'], record.avg_windspeed)
        )

    extremes = bucket['extremes']
    if _is_valid(record.max_temperature) and record.max_temperature > 35:
        extremes['hot_days'] += 1
    if _is_valid(record.min_temperature) and record.min_temperature < 0:
        extremes['cold_days'] += 1
    if _is_valid(rainfall_value):
        if rainfall_value > 20:
            extremes['heavy_rain_days'] += 1
        if rainfall_value == 0:
            extremes['zero_rain_days'] += 1
            extremes['current_dry_streak'] += 1
            extremes['consecutive_dry_days'] = max(
                extremes['consecutive_dry_days'],
                extremes['current_dry_streak']
            )
        else:
            extremes['current_dry_streak'] = 0
    else:
        extremes['current_dry_streak'] = 0


def _finalize_month_bucket(period, bucket):
    temperature = bucket['temperature']
    humidity = bucket['humidity']
    rainfall = bucket['rainfall']
    wind = bucket['wind']
    extremes = bucket['extremes'].copy()
    extremes.pop('current_dry_streak', None)
    rainfall_total = rainfall['total'] if rainfall['avg_count'] else None

    return {
        'period': period,
        'temperature': {
            'min': _round_value(temperature['min']),
            'avg': (
                _round_value(temperature['avg_sum'] / temperature['avg_count'])
                if temperature['avg_count'] else None
            ),
            'max': _round_value(temperature['max']),
            'range': _calc_range(temperature['min'], temperature['max']),
        },
        'humidity': {
            'min': _round_value(humidity['min']),
            'avg': (
                _round_value(humidity['avg_sum'] / humidity['avg_count'])
                if humidity['avg_count'] else None
            ),
            'max': _round_value(humidity['max']),
            'range': _calc_range(humidity['min'], humidity['max']),
        },
        'rainfall': {
            'min': _round_value(rainfall['min']),
            'avg': (
                _round_value(rainfall['avg_sum'] / rainfall['avg_count'])
                if rainfall['avg_count'] else None
            ),
            'max': _round_value(rainfall['max']),
            'range': _calc_range(rainfall['min'], rainfall['max']),
            'total': _round_value(rainfall_total) if rainfall_total is not None else None,
        },
        'wind': {
            'avg': (
                _round_value(wind['avg_sum'] / wind['avg_count'])
                if wind['avg_count'] else None
            ),
            'max': _round_value(wind['max']),
        },
        'extremes': extremes,
    }


def _build_monthly_records(queryset):
    monthly_data = OrderedDict()
    for record in queryset:
        if not record.date:
            continue
        period = date(record.date.year, record.date.month, 1)
        bucket = monthly_data.setdefault(period, _init_month_bucket())
        _accumulate_month_bucket(bucket, record)

    return [
        _finalize_month_bucket(period, bucket)
        for period, bucket in monthly_data.items()
    ]


def _build_daily_records(queryset):
    """Build daily records from queryset."""
    daily_records = []
    for record in queryset:
        if not record.date:
            continue
        daily_records.append({
            'period': record.date,
            'temperature': {
                'min': _round_value(record.min_temperature),
                'avg': _round_value(record.avg_temperature),
                'max': _round_value(record.max_temperature),
            },
            'humidity': {
                'min': _round_value(record.min_humidity),
                'avg': _round_value(record.avg_humidity),
                'max': _round_value(record.max_humidity),
            },
            'rainfall': {
                'total': _round_value(record.daily_rainfall),
            },
        })
    return daily_records


def _build_annual_records(queryset):
    """Build annual records from queryset."""
    annual_data = OrderedDict()
    for record in queryset:
        if not record.date:
            continue
        year = record.date.year
        bucket = annual_data.setdefault(year, _init_month_bucket())
        _accumulate_month_bucket(bucket, record)

    return [
        _finalize_month_bucket(date(year, 1, 1), bucket)
        for year, bucket in annual_data.items()
    ]


def _round_value(value):
    if not _is_valid(value):
        return None
    return round(float(value), 1)


def _calc_range(min_val, max_val):
    if min_val is None or max_val is None:
        return None
    return _round_value(max_val - min_val)


def _build_temperature_table(records):
    rows = []
    min_values, avg_values, max_values = [], [], []
    for record in records:
        metrics = record.get('temperature')
        label = record['period'].strftime('%b')
        if not metrics:
            rows.append({'label': label, 'min': None, 'avg': None, 'max': None, 'range': None})
            continue
        rows.append({
            'label': label,
            'min': metrics.get('min'),
            'avg': metrics.get('avg'),
            'max': metrics.get('max'),
            'range': _calc_range(metrics.get('min'), metrics.get('max')),
        })
        if metrics.get('min') is not None:
            min_values.append(metrics['min'])
        if metrics.get('avg') is not None:
            avg_values.append(metrics['avg'])
        if metrics.get('max') is not None:
            max_values.append(metrics['max'])
    annual_row = {
        'label': 'Annual',
        'min': min(min_values) if min_values else None,
        'avg': _round_value(sum(avg_values) / len(avg_values)) if avg_values else None,
        'max': max(max_values) if max_values else None,
        'range': _calc_range(min(min_values), max(max_values)) if min_values and max_values else None,
    }
    return rows, annual_row


def _build_rainfall_table(records):
    rows = []
    min_values, avg_values, max_values = [], [], []
    for record in records:
        metrics = record.get('rainfall')
        label = record['period'].strftime('%b')
        if not metrics:
            rows.append({'label': label, 'min': None, 'avg': None, 'max': None, 'range': None})
            continue
        rows.append({
            'label': label,
            'min': metrics.get('min'),
            'avg': metrics.get('avg'),
            'max': metrics.get('max'),
            'range': _calc_range(metrics.get('min'), metrics.get('max')),
        })
        if metrics.get('min') is not None:
            min_values.append(metrics['min'])
        if metrics.get('avg') is not None:
            avg_values.append(metrics['avg'])
        if metrics.get('max') is not None:
            max_values.append(metrics['max'])
    annual_row = {
        'label': 'Annual',
        'min': min(min_values) if min_values else None,
        'avg': _round_value(sum(avg_values) / len(avg_values)) if avg_values else None,
        'max': max(max_values) if max_values else None,
        'range': _calc_range(min(min_values), max(max_values)) if min_values and max_values else None,
    }
    return rows, annual_row


def _build_wind_table(records):
    rows = []
    avg_values, max_values = [], []
    for record in records:
        metrics = record.get('wind')
        label = record['period'].strftime('%b')
        if not metrics:
            rows.append({'label': label, 'avg': None, 'max': None})
            continue
        rows.append({
            'label': label,
            'avg': metrics.get('avg'),
            'max': metrics.get('max'),
        })
        if metrics.get('avg') is not None:
            avg_values.append(metrics['avg'])
        if metrics.get('max') is not None:
            max_values.append(metrics['max'])
    annual_row = {
        'label': 'Annual',
        'avg': _round_value(sum(avg_values) / len(avg_values)) if avg_values else None,
        'max': max(max_values) if max_values else None,
    }
    return rows, annual_row


def _build_extremes_table(records):
    rows = []
    totals = [0 for _ in EXTREME_HEADERS]
    has_value = [False for _ in EXTREME_HEADERS]
    for record in records:
        metrics = record.get('extremes') or {}
        label = record['period'].strftime('%b')
        values = []
        for index, (key, _) in enumerate(EXTREME_HEADERS):
            value = metrics.get(key)
            values.append(value)
            if value is not None:
                has_value[index] = True
                totals[index] += value
        rows.append({'label': label, 'values': values})
    annual_values = [
        totals[index] if has_value[index] else None
        for index in range(len(EXTREME_HEADERS))
    ]
    annual_row = {'label': 'Annual', 'values': annual_values}
    return rows, annual_row


def _build_historical_monthly_rainfall(queryset):
    """
    Build historical average monthly rainfall from all data.
    Returns a dict with month numbers (1-12) as keys and average rainfall as values.
    """
    monthly_totals = {}
    year_months = queryset.exclude(daily_rainfall=MISSING).values(
        'year', 'month'
    ).annotate(
        monthly_total=Sum('daily_rainfall')
    ).order_by('year', 'month')

    month_data = {}
    for item in year_months:
        month = item['month']
        if month not in month_data:
            month_data[month] = []
        if item['monthly_total'] is not None:
            month_data[month].append(float(item['monthly_total']))

    for month, totals in month_data.items():
        if totals:
            monthly_totals[month] = round(sum(totals) / len(totals), 1)
        else:
            monthly_totals[month] = None

    return monthly_totals


def _build_chart_payload(records, granularity='monthly', historical_rainfall=None):
    """Build chart payload with appropriate date formatting based on granularity."""
    if granularity == 'daily':
        labels = [record['period'].strftime('%Y-%m-%d') for record in records]
    elif granularity == 'annual':
        labels = [record['period'].strftime('%Y') for record in records]
    else:  # monthly
        labels = [record['period'].strftime('%b %Y') for record in records]

    def collect(metric_key, value_key):
        series = []
        for record in records:
            metric = record.get(metric_key) or {}
            series.append(metric.get(value_key))
        return series

    rainfall_data = {
        'total': [((record.get('rainfall') or {}).get('total')) for record in records],
    }
    if granularity == 'monthly' and historical_rainfall:
        historical_totals = []
        for record in records:
            month = record['period'].month
            historical_totals.append(historical_rainfall.get(month))
        rainfall_data['historical'] = historical_totals

    payload = {
        'labels': labels,
        'temperature': {
            'min': collect('temperature', 'min'),
            'avg': collect('temperature', 'avg'),
            'max': collect('temperature', 'max'),
        },
        'humidity': {
            'min': collect('humidity', 'min'),
            'avg': collect('humidity', 'avg'),
            'max': collect('humidity', 'max'),
        },
        'rainfall': rainfall_data,
    }
    return payload


def _build_csv_response(records, start_date, end_date):
    filename = f"climate_dashboard_{start_date}_{end_date}.csv"
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    writer = csv.writer(response)
    writer.writerow([
        'Period',
        'Temp Min (°C)',
        'Temp Avg (°C)',
        'Temp Max (°C)',
        'Humidity Min (%)',
        'Humidity Avg (%)',
        'Humidity Max (%)',
        'Rainfall Total (mm)',
        'Wind Avg (km/h)',
        'Wind Max (km/h)',
        'Days >35°C',
        'Days <0°C',
        'Rain days >20mm',
        '0 rainfall days',
        'Consecutive dry days',
    ])
    for record in records:
        period_label = record['period'].strftime('%b %Y')
        temperature = record.get('temperature') or {}
        humidity = record.get('humidity') or {}
        rainfall = record.get('rainfall') or {}
        wind = record.get('wind') or {}
        extremes = record.get('extremes') or {}
        writer.writerow([
            period_label,
            temperature.get('min', ''),
            temperature.get('avg', ''),
            temperature.get('max', ''),
            humidity.get('min', ''),
            humidity.get('avg', ''),
            humidity.get('max', ''),
            rainfall.get('total', ''),
            wind.get('avg', ''),
            wind.get('max', ''),
            extremes.get('hot_days', ''),
            extremes.get('cold_days', ''),
            extremes.get('heavy_rain_days', ''),
            extremes.get('zero_rain_days', ''),
            extremes.get('consecutive_dry_days', ''),
        ])
    return response


class ClimateSiteView(TemplateView):
    """View for single site climate data display."""
    template_name = 'climate/single_site.html'
    location_site = None
    location_context = None
    year = None
    start_date = None
    end_date = None
    all_data = False
    year_from_url = False

    def get_context_data(self, **kwargs):
        start_time = time.time()
        context = super(ClimateSiteView, self).get_context_data(**kwargs)

        # Get climate data for this site
        climate_data_by_site = Climate.objects.filter(
            location_site=self.location_site
        )
        climate_data = climate_data_by_site.filter(
            location_site=self.location_site
        )

        if not climate_data.exists():
            raise Http404('No climate data available for this site')

        # Get available years
        context['years'] = list(
            climate_data.values_list(
                'year', flat=True
            ).distinct('year').order_by('year')
        )

        # Parse year if string
        if self.year and isinstance(self.year, str):
            self.year = int(self.year.strip())

        # Default to latest year if not specified
        if not self.year and len(context['years']) > 0:
            self.year = int(context['years'][-1])

        # Filter by date range or year (skipped when all_data is requested)
        if not self.all_data:
            if self.start_date and self.end_date:
                self.start_date = datetime.strptime(self.start_date, '%Y-%m-%d')
                self.end_date = datetime.strptime(self.end_date, '%Y-%m-%d')
                climate_data = climate_data.filter(
                    date__gte=self.start_date,
                    date__lte=self.end_date
                )
            elif self.year:
                climate_data = climate_data.filter(year=self.year)

        # Site information
        context['coord'] = [
            self.location_site.get_centroid().x,
            self.location_site.get_centroid().y
        ]
        context['site_code'] = (
            self.location_site.site_code
            if self.location_site.site_code else self.location_site.name
        )
        context['site_id'] = self.location_site.id
        context['user_site_code'] = self.location_site.legacy_site_code
        context['user_river_name'] = self.location_site.legacy_river_name

        context['all_data'] = self.all_data
        if not self.all_data and len(context['years']) > 0:
            context['year'] = int(
                self.year if self.year else context['years'][-1]
            )

        site_description = self.location_site.site_description
        if not site_description:
            site_description = self.location_site.name
        context['site_description'] = site_description

        try:
            context['river'] = self.location_site.river.name
        except AttributeError:
            context['river'] = '-'

        # Order data
        climate_data = climate_data.order_by('date')

        is_private_user = False
        if self.request.user.is_authenticated:
            is_private_user = self.request.user.groups.filter(
                name='PrivateDataGroup'
            ).exists()

        daily_records = _build_daily_records(climate_data)
        monthly_records = _build_monthly_records(climate_data)
        annual_records = _build_annual_records(climate_data)

        historical_rainfall = _build_historical_monthly_rainfall(climate_data_by_site)

        context['monthly_records'] = monthly_records
        if monthly_records:
            # Build chart payloads for all granularities
            chart_payloads = {
                'daily': _build_chart_payload(daily_records, 'daily'),
                'monthly': _build_chart_payload(
                    monthly_records, 'monthly', historical_rainfall
                ),
                'annual': _build_chart_payload(annual_records, 'annual'),
            }
            chart_payload = json.dumps(chart_payloads, cls=DjangoJSONEncoder)
            availability = {
                'temperature': any(
                    (record.get('temperature') or {}).get('avg') is not None
                    for record in monthly_records
                ),
                'humidity': any(
                    (record.get('humidity') or {}).get('avg') is not None
                    for record in monthly_records
                ),
                'rainfall': any(
                    (record.get('rainfall') or {}).get('total') is not None
                    for record in monthly_records
                ),
                'wind': any(
                    (record.get('wind') or {}).get('avg') is not None
                    for record in monthly_records
                ),
            }
        else:
            chart_payload = json.dumps({})
            availability = {
                'temperature': False,
                'humidity': False,
                'rainfall': False,
                'wind': False,
            }
        context['chart_payload'] = chart_payload
        context['availability'] = availability
        context['has_chart_data'] = any(availability.values())

        # Calculate statistics — NullIf ensures -999 sentinel rows are
        # excluded from every aggregate just like genuine NULLs.
        def _ni(field):
            return NullIf(field, Value(MISSING))

        stats = climate_data.aggregate(
            avg_temp=Avg(_ni('avg_temperature')),
            min_temp=Min(_ni('min_temperature')),
            max_temp=Max(_ni('max_temperature')),
            avg_humidity=Avg(_ni('avg_humidity')),
            min_humidity=Min(_ni('min_humidity')),
            max_humidity=Max(_ni('max_humidity')),
            avg_windspeed=Avg(_ni('avg_windspeed')),
            total_rainfall=Sum(_ni('daily_rainfall')),
            max_rainfall=Max(_ni('daily_rainfall')),
            record_count=Count('id')
        )
        stats_overall = climate_data_by_site.aggregate(
            avg_temp=Avg(_ni('avg_temperature')),
            min_temp=Min(_ni('min_temperature')),
            max_temp=Max(_ni('max_temperature')),
            avg_humidity=Avg(_ni('avg_humidity')),
            min_humidity=Min(_ni('min_humidity')),
            max_humidity=Max(_ni('max_humidity')),
            avg_windspeed=Avg(_ni('avg_windspeed')),
            total_rainfall=Sum(_ni('daily_rainfall')),
            max_rainfall=Max(_ni('daily_rainfall')),
        )
        context['stats'] = stats
        context['stats_overall'] = stats_overall

        # Get records
        context['total_records'] = climate_data.count()
        context['total_overall_records'] = climate_data_by_site.count()
        context['location_site'] = self.location_site
        context['execution_time'] = time.time() - start_time
        context['start_date'] = (
            self.start_date if self.start_date else
            climate_data.first().date
        )
        context['end_date'] = (
            self.end_date if self.end_date else
            climate_data.last().date
        )
        context['climate_data'] = climate_data.first()

        # Site details
        site_details = overview_site_detail(self.location_site.id)
        site_overview = site_details.get('Site details', {})
        if 'User Site Code' in site_overview:
            site_overview.pop('User Site Code')
        context['site_overview'] = site_overview
        context['site_details'] = json.dumps(site_details)

        context['is_private_user'] = is_private_user

        # Monthly averaged records for display table (visible to all)
        monthly_records_list = []
        for record in monthly_records:
            period = record['period']
            temp = record.get('temperature') or {}
            humidity = record.get('humidity') or {}
            wind = record.get('wind') or {}
            rainfall = record.get('rainfall') or {}
            monthly_records_list.append({
                'period': period,
                'period_formatted': period.strftime('%b %Y'),
                'avg_temperature': temp.get('avg'),
                'min_temperature': temp.get('min'),
                'max_temperature': temp.get('max'),
                'avg_humidity': humidity.get('avg'),
                'avg_windspeed': wind.get('avg'),
                'total_rainfall': rainfall.get('total'),
            })
        context['climate_records'] = monthly_records_list

        # Daily records for CSV download (private users only)
        if is_private_user:
            daily_records_list = []
            for record in climate_data:
                daily_records_list.append({
                    'date': record.date.strftime('%Y-%m-%d'),
                    # Use `is not None` so that genuine zeros and -999 sentinels
                    # are both preserved verbatim in the daily CSV download.
                    'avg_temperature': float(record.avg_temperature) if record.avg_temperature is not None else None,
                    'min_temperature': float(record.min_temperature) if record.min_temperature is not None else None,
                    'max_temperature': float(record.max_temperature) if record.max_temperature is not None else None,
                    'avg_humidity': float(record.avg_humidity) if record.avg_humidity is not None else None,
                    'min_humidity': float(record.min_humidity) if record.min_humidity is not None else None,
                    'max_humidity': float(record.max_humidity) if record.max_humidity is not None else None,
                    'avg_windspeed': float(record.avg_windspeed) if record.avg_windspeed is not None else None,
                    'daily_rainfall': float(record.daily_rainfall) if record.daily_rainfall is not None else None,
                    'flag': record.flag
                })
            context['daily_records_json'] = json.dumps(daily_records_list, cls=DjangoJSONEncoder)

        # Determine active filter mode for the dropdown
        import datetime as dt
        current_year = dt.date.today().year
        last_year = current_year - 1
        if self.all_data:
            filter_mode = 'all-data'
        elif self.start_date:
            filter_mode = 'date-range'
        elif self.year_from_url:
            year_int = int(self.year) if self.year else current_year
            if year_int == current_year:
                filter_mode = 'this-year'
            elif year_int == last_year:
                filter_mode = 'last-year'
            else:
                filter_mode = 'select-year'
        else:
            # Defaulted to latest year in data — map to this/last/select-year
            latest = self.year if self.year else current_year
            if latest == current_year:
                filter_mode = 'this-year'
            elif latest == last_year:
                filter_mode = 'last-year'
            else:
                filter_mode = 'select-year'
        context['filter_mode'] = filter_mode
        context['current_year'] = current_year
        context['last_year'] = last_year

        return context

    def get(self, request, *args, **kwargs):
        site_id = kwargs.get('site_id', None)
        self.year = kwargs.get('year', None)
        self.year_from_url = self.year is not None
        self.start_date = request.GET.get('startDate', None)
        self.end_date = request.GET.get('endDate', None)
        self.all_data = request.GET.get('allData', '0') == '1'

        # Default to whole data when no filter is explicitly provided
        if not self.year_from_url and not self.start_date and not self.end_date and not self.all_data:
            self.all_data = True

        if not site_id or not request.GET or not request.GET.get('siteId', None):
            raise Http404()

        self.location_site = get_object_or_404(
            LocationSite,
            pk=site_id
        )
        self.location_context = LocationContext.objects.filter(
            site=self.location_site
        )

        return super(ClimateSiteView, self).get(request, *args, **kwargs)


class ClimateUploadView(DataUploadView):
    """Climate data upload view."""
    template_name = 'climate/climate_uploader.html'
    upload_task = climate_upload
    category = 'climate'


class ClimateDashboardMultipleSitesView(TemplateView):
    """Multiple sites climate dashboard view."""
    template_name = 'climate/multi_site.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        from preferences import preferences
        from bims.models import BaseMapLayer
        context['is_sanparks'] = (
            preferences.SiteSetting.site_code_generator == 'sanparks'
        )
        terrain_layer = BaseMapLayer.objects.filter(
            source_type='map_tiler',
            title__icontains='terrain'
        ).first()
        if terrain_layer:
            context['basemap'] = json.dumps({
                'source_type': 'map_tiler',
                'url': terrain_layer.url,
                'key': terrain_layer.key,
                'title': terrain_layer.title,
            })
        else:
            context['basemap'] = json.dumps({'source_type': 'osm'})
        return context



class ClimateDashboardMultipleSitesApiView(APIView):
    """API endpoint for multi-site climate summary data."""

    def get(self, request):
        parameters = request.GET.dict()
        search_uri = request.build_absolute_uri()

        search_process, created = get_or_create_search_process(
            search_type=SEARCH_RESULTS,
            query=search_uri,
            requester=request.user if request.user.is_authenticated else None,
        )

        if not created:
            results = search_process.get_file_if_exits()
            if results:
                return Response(results)

        process_id = hashlib.sha256(search_uri.encode('utf-8')).hexdigest()
        search_process.set_process_id(process_id)
        search_process.set_status(SEARCH_PROCESSING)

        climate_multi_site_summary.delay(parameters, search_process.id)

        result_file = search_process.get_file_if_exits(finished=False)
        if result_file:
            return Response(result_file)
        return Response({'status': SEARCH_PROCESSING})
