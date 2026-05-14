from celery import shared_task


@shared_task(
    name='climate.tasks.climate_multi_site_summary',
    queue='search'
)
def climate_multi_site_summary(parameters, search_process_id):
    from django.db.models import Avg, Min, Max, Sum, Count, Value
    from django.db.models.functions import NullIf
    from climate.views import MISSING

    from bims.api_views.search_module import ClimateModule
    from bims.models import BaseMapLayer, LocationContext
    from bims.models.search_process import (
        SearchProcess,
        SEARCH_PROCESSING,
        SEARCH_FINISHED,
        SEARCH_FAILED,
    )
    from bims.utils.celery import memcache_lock
    from bims.utils.site_code import SANPARK_PARK_KEY
    from preferences import preferences

    try:
        search_process = SearchProcess.objects.get(id=search_process_id)
    except SearchProcess.DoesNotExist:
        return

    lock_id = '{0}-lock-{1}'.format(
        search_process.file_path,
        search_process.process_id,
    )
    oid = '{0}'.format(search_process.process_id)

    with memcache_lock(lock_id, oid) as acquired:
        if not acquired:
            return

        search_process.set_status(SEARCH_PROCESSING)

        try:
            search = ClimateModule(parameters)
            search.run_search()

            climate_qs = search.module
            location_sites = search.sites.order_by('site_code')

            is_sanparks = preferences.SiteSetting.site_code_generator == 'sanparks'

            summary = {
                'site_id': [],
                'site_code': [],
                'park_name': [] if is_sanparks else None,
                'avg_temp': [],
                'min_temp': [],
                'max_temp': [],
                'avg_humidity': [],
                'avg_windspeed': [],
                'total_rainfall': [],
                'max_rainfall': [],
                'record_count': [],
            }

            coordinates = []

            for site in location_sites:
                qs = climate_qs.filter(location_site=site)
                if not qs.exists():
                    continue
                def _ni(field):
                    return NullIf(field, Value(MISSING))

                stats = qs.aggregate(
                    avg_temp=Avg(_ni('avg_temperature')),
                    min_temp=Min(_ni('min_temperature')),
                    max_temp=Max(_ni('max_temperature')),
                    avg_humidity=Avg(_ni('avg_humidity')),
                    avg_windspeed=Avg(_ni('avg_windspeed')),
                    total_rainfall=Sum(_ni('daily_rainfall')),
                    max_rainfall=Max(_ni('daily_rainfall')),
                    record_count=Count('id'),
                )
                summary['site_id'].append(site.id)
                summary['site_code'].append(
                    site.site_code if site.site_code else site.name
                )
                if is_sanparks:
                    park_context = LocationContext.objects.filter(
                        site=site,
                        group__name__iexact=SANPARK_PARK_KEY
                    ).first()
                    park_name = (
                        park_context.value
                        if park_context and park_context.value else ''
                    )
                    summary['park_name'].append(park_name)
                summary['avg_temp'].append(
                    round(stats['avg_temp'], 1) if stats['avg_temp'] is not None else None
                )
                summary['min_temp'].append(
                    round(stats['min_temp'], 1) if stats['min_temp'] is not None else None
                )
                summary['max_temp'].append(
                    round(stats['max_temp'], 1) if stats['max_temp'] is not None else None
                )
                summary['avg_humidity'].append(
                    round(stats['avg_humidity'], 1) if stats['avg_humidity'] is not None else None
                )
                summary['avg_windspeed'].append(
                    round(stats['avg_windspeed'], 2) if stats['avg_windspeed'] is not None else None
                )
                summary['total_rainfall'].append(
                    round(stats['total_rainfall'], 2) if stats['total_rainfall'] is not None else None
                )
                summary['max_rainfall'].append(
                    round(stats['max_rainfall'], 2) if stats['max_rainfall'] is not None else None
                )
                summary['record_count'].append(stats['record_count'])

                coordinates.append({
                    'x': site.get_centroid().x,
                    'y': site.get_centroid().y,
                })

            try:
                bing_map_key = BaseMapLayer.objects.get(source_type='bing').key
            except BaseMapLayer.DoesNotExist:
                bing_map_key = ''

            results = {
                'status': SEARCH_FINISHED,
                'climate_summary_data': summary,
                'coordinates': coordinates,
                'bing_map_key': bing_map_key,
            }

            search_process.set_status(SEARCH_FINISHED, False)
            search_process.save_to_file(results)

        except Exception:
            search_process.set_status(SEARCH_FAILED)
            raise
