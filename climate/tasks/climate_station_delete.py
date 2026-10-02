# coding=utf-8
"""Celery task for deleting climate stations"""
from celery.app import shared_task

BATCH_SIZE = 5000


@shared_task(name='climate.tasks.delete_climate_stations', queue='update')
def delete_climate_stations(station_ids):
    """
    Delete climate stations and their climate data in the background.
    Climate data is deleted in batches first, then the stations.
    """
    from bims.utils.logger import log
    from climate.models import Climate, ClimateStation

    results = {}
    for station in ClimateStation.objects.filter(id__in=station_ids):
        climate_data = Climate.objects.filter(location_site=station)
        total_climate_data = 0
        while True:
            ids = list(climate_data.values_list('id', flat=True)[:BATCH_SIZE])
            if not ids:
                break
            Climate.objects.filter(id__in=ids).delete()
            total_climate_data += len(ids)
        station_name = str(station)
        station.delete()
        log(
            f'Climate station "{station_name}" deleted with '
            f'{total_climate_data} climate record(s)')
        results[station_name] = total_climate_data
    return results
