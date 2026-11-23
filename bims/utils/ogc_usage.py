# coding=utf-8
"""Usage figures of OGC live layers, for the admin usage page."""
import math
from datetime import timedelta

from django.conf import settings
from django.db.models import Count, Max, Q, Sum
from django.utils import timezone

from bims.models.ogc_layer import OgcLayer, OgcLayerDailyUsage

USAGE_PERIODS = (7, 30, 90)
DEFAULT_USAGE_PERIOD = 30
# Owners are listed once they use this share of OGC_MAX_LAYERS_PER_OWNER
OWNER_LIMIT_WARNING_SHARE = 0.8


def usage_period(value):
    """Return a supported number of days from a query parameter."""
    try:
        days = int(value)
    except (TypeError, ValueError):
        return DEFAULT_USAGE_PERIOD
    return days if days in USAGE_PERIODS else DEFAULT_USAGE_PERIOD


def period_dates(days, today=None):
    """The dates of the period, oldest first, ending today."""
    today = today or timezone.localdate()
    return [today - timedelta(days=offset)
            for offset in range(days - 1, -1, -1)]


def _bars(values):
    """Height of each value as a percentage of the largest one."""
    top = max(values) if values else 0
    return [round(value * 100 / top) if top else 0 for value in values]


def daily_series(usage_rows, dates):
    """Requests, slow requests and timeouts per date, zero-filled.

    :param usage_rows: OgcLayerDailyUsage rows, or dicts with the same keys
    """
    by_date = {}
    for row in usage_rows:
        get = row.get if isinstance(row, dict) else (
            lambda key, r=row: getattr(r, key))
        day = by_date.setdefault(get('date'), {
            'requests': 0, 'slow_requests': 0, 'timeouts': 0,
            'slowest_ms': 0})
        day['requests'] += get('requests') or 0
        day['slow_requests'] += get('slow_requests') or 0
        day['timeouts'] += get('timeouts') or 0
        day['slowest_ms'] = max(day['slowest_ms'], get('slowest_ms') or 0)
    series = []
    for date in dates:
        day = by_date.get(date, {})
        series.append({
            'date': date,
            'requests': day.get('requests', 0),
            'slow_requests': day.get('slow_requests', 0),
            'timeouts': day.get('timeouts', 0),
            'slowest_ms': day.get('slowest_ms', 0),
        })
    for item, height in zip(
            series, _bars([item['requests'] for item in series])):
        item['bar'] = height
    return series


def owners_near_limit(limit=None):
    """Owners whose active layers reach the warning share of the limit."""
    limit = settings.OGC_MAX_LAYERS_PER_OWNER if limit is None else limit
    if not limit:
        return [], 0
    threshold = max(1, math.ceil(limit * OWNER_LIMIT_WARNING_SHARE))
    owners = (
        OgcLayer.objects.filter(is_active=True)
        .values('owner_id', 'owner__username', 'owner__email')
        .annotate(active_layers=Count('id'))
        .filter(active_layers__gte=threshold)
        .order_by('-active_layers', 'owner__username')
    )
    return [
        dict(owner, at_limit=owner['active_layers'] >= limit)
        for owner in owners
    ], threshold


def usage_report(days=DEFAULT_USAGE_PERIOD, today=None):
    """Everything the admin usage page shows, for the last ``days`` days."""
    dates = period_dates(days, today)
    start = dates[0]
    usage = OgcLayerDailyUsage.objects.filter(date__gte=start)

    totals = usage.aggregate(
        requests=Sum('requests'),
        slow_requests=Sum('slow_requests'),
        timeouts=Sum('timeouts'),
        slowest_ms=Max('slowest_ms'),
    )
    totals = {key: value or 0 for key, value in totals.items()}

    per_day = usage.values('date').annotate(
        requests=Sum('requests'),
        slow_requests=Sum('slow_requests'),
        timeouts=Sum('timeouts'),
        slowest_ms=Max('slowest_ms'),
    )

    # Every layer used in the period, plus active layers that were not
    layer_usage = {
        row['layer_id']: row for row in usage.values('layer_id').annotate(
            requests=Sum('requests'),
            slow_requests=Sum('slow_requests'),
            timeouts=Sum('timeouts'),
            slowest_ms=Max('slowest_ms'),
        )
    }
    layers = (
        OgcLayer.objects
        .filter(Q(pk__in=layer_usage.keys()) | Q(is_active=True))
        .select_related('owner')
    )
    empty = {'requests': 0, 'slow_requests': 0, 'timeouts': 0,
             'slowest_ms': 0}
    layer_rows = []
    for layer in layers:
        figures = layer_usage.get(layer.pk, empty)
        layer_rows.append({
            'layer': layer,
            'requests': figures['requests'] or 0,
            'slow_requests': figures['slow_requests'] or 0,
            'timeouts': figures['timeouts'] or 0,
            'slowest_ms': figures['slowest_ms'] or 0,
        })
    layer_rows.sort(key=lambda row: (-row['requests'], row['layer'].name))
    for row, height in zip(
            layer_rows, _bars([row['requests'] for row in layer_rows])):
        row['bar'] = height

    slow_layers = sorted(
        (row for row in layer_rows
         if row['slow_requests'] or row['timeouts']),
        key=lambda row: (
            -row['timeouts'], -row['slow_requests'], -row['slowest_ms']))

    auto_disabled = (
        OgcLayer.objects.filter(is_active=False)
        .exclude(disabled_reason='')
        .select_related('owner')
        .order_by('-last_evaluated', '-created')
    )
    owners, owner_threshold = owners_near_limit()

    return {
        'days': days,
        'periods': USAGE_PERIODS,
        'start': start,
        'end': dates[-1],
        'totals': totals,
        'active_layers': OgcLayer.objects.filter(is_active=True).count(),
        'daily': daily_series(per_day, dates),
        'layers': layer_rows,
        'slow_layers': slow_layers,
        'auto_disabled': list(auto_disabled),
        'owners_near_limit': owners,
        'owner_threshold': owner_threshold,
        'owner_limit': settings.OGC_MAX_LAYERS_PER_OWNER,
        'slow_request_seconds': settings.OGC_SLOW_REQUEST_SECONDS,
    }


def layer_usage_report(layer, days=DEFAULT_USAGE_PERIOD, today=None):
    """Daily usage of one layer for the last ``days`` days."""
    dates = period_dates(days, today)
    rows = OgcLayerDailyUsage.objects.filter(
        layer=layer, date__gte=dates[0])
    daily = daily_series(rows, dates)
    return {
        'days': days,
        'periods': USAGE_PERIODS,
        'start': dates[0],
        'end': dates[-1],
        'layer': layer,
        'daily': daily,
        'daily_newest_first': list(reversed(daily)),
        'totals': {
            'requests': sum(day['requests'] for day in daily),
            'slow_requests': sum(day['slow_requests'] for day in daily),
            'timeouts': sum(day['timeouts'] for day in daily),
            'slowest_ms': max(
                (day['slowest_ms'] for day in daily), default=0),
        },
        'slow_request_seconds': settings.OGC_SLOW_REQUEST_SECONDS,
    }
