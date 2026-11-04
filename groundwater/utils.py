"""Aggregations used by the groundwater dashboards.

Hourly readings are aggregated in the database, a station can have tens of
thousands of readings per year.
"""
import calendar
from datetime import timedelta

from django.db.models import Avg, Count, Max, Min
from django.db.models.functions import (
    TruncDay, TruncHour, TruncMonth, TruncYear
)

# Hourly charts are limited to the last days of the selected range
HOURLY_WINDOW_DAYS = 31

TRUNCATORS = {
    'hourly': TruncHour,
    'daily': TruncDay,
    'monthly': TruncMonth,
    'annual': TruncYear,
}

PARAMETERS = {
    'level': 'water_level',
    'temperature': 'water_temperature',
    'conductivity': 'electrical_conductivity',
}


def _round(value, digits=2):
    return round(value, digits) if value is not None else None


def summary_stats(queryset):
    """Overall statistics of a queryset of readings."""
    stats = queryset.aggregate(
        count=Count('id'),
        first=Min('timestamp'),
        last=Max('timestamp'),
        level_min=Min('water_level'),
        level_max=Max('water_level'),
        level_avg=Avg('water_level'),
        temperature_min=Min('water_temperature'),
        temperature_max=Max('water_temperature'),
        temperature_avg=Avg('water_temperature'),
        conductivity_min=Min('electrical_conductivity'),
        conductivity_max=Max('electrical_conductivity'),
        conductivity_avg=Avg('electrical_conductivity'),
    )
    for key, value in list(stats.items()):
        if key not in ('count', 'first', 'last'):
            stats[key] = _round(value)
    stats['level_range'] = (
        _round(stats['level_max'] - stats['level_min'])
        if stats['level_max'] is not None and
        stats['level_min'] is not None else None
    )
    if stats['count'] and stats['first'] and stats['last']:
        expected = int(
            (stats['last'] - stats['first']).total_seconds() // 3600) + 1
        stats['expected_readings'] = expected
        stats['completeness'] = round(
            min(stats['count'] / expected, 1) * 100, 1)
    else:
        stats['expected_readings'] = 0
        stats['completeness'] = None
    return stats


def bucket_readings(queryset, granularity):
    """Aggregate the readings by hour, day, month or year."""
    truncator = TRUNCATORS[granularity]
    return list(
        queryset.annotate(
            period=truncator('timestamp')
        ).values('period').annotate(
            readings=Count('id'),
            level_min=Min('water_level'),
            level_avg=Avg('water_level'),
            level_max=Max('water_level'),
            temperature_avg=Avg('water_temperature'),
            conductivity_avg=Avg('electrical_conductivity'),
        ).order_by('period')
    )


def build_chart_payload(queryset):
    """Chart data for every granularity.

    Timestamps are epoch milliseconds, ready for Highcharts.
    """
    payload = {}
    last = queryset.aggregate(last=Max('timestamp'))['last']
    for granularity in TRUNCATORS:
        qs = queryset
        if granularity == 'hourly' and last:
            qs = queryset.filter(
                timestamp__gt=last - timedelta(days=HOURLY_WINDOW_DAYS))
        rows = bucket_readings(qs, granularity)
        payload[granularity] = {
            'timestamps': [
                int(row['period'].timestamp() * 1000) for row in rows
            ],
            'level': {
                'min': [_round(r['level_min']) for r in rows],
                'avg': [_round(r['level_avg']) for r in rows],
                'max': [_round(r['level_max']) for r in rows],
            },
            'temperature': [_round(r['temperature_avg']) for r in rows],
            'conductivity': [_round(r['conductivity_avg']) for r in rows],
        }
    return payload


def availability(stats):
    """Which parameters have data in the given statistics."""
    return {
        'level': stats['level_avg'] is not None,
        'temperature': stats['temperature_avg'] is not None,
        'conductivity': stats['conductivity_avg'] is not None,
    }


def monthly_summary(queryset):
    """Rows of the monthly summary table."""
    rows = []
    for row in bucket_readings(queryset, 'monthly'):
        period = row['period']
        expected = calendar.monthrange(period.year, period.month)[1] * 24
        rows.append({
            'period': period,
            'period_formatted': period.strftime('%b %Y'),
            'readings': row['readings'],
            'completeness': round(
                min(row['readings'] / expected, 1) * 100, 1),
            'level_min': _round(row['level_min']),
            'level_avg': _round(row['level_avg']),
            'level_max': _round(row['level_max']),
            'temperature_avg': _round(row['temperature_avg']),
            'conductivity_avg': _round(row['conductivity_avg']),
        })
    return rows
