import logging


from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task(name='bims.tasks.search_task', queue='search')
def search_task(parameters, search_process_id, background=True):
    from bims.utils.celery import memcache_lock
    from bims.api_views.search import CollectionSearch
    from bims.api_views.search_module import (
        PhysicoChemistryModule,
        WaterTemperatureModule,
        ClimateModule,
        GroundwaterModule
    )
    from bims.models.search_process import (
        SearchProcess,
        SEARCH_PROCESSING,
        SEARCH_FINISHED,
        SEARCH_FAILED
    )

    try:
        search_process = SearchProcess.objects.get(id=search_process_id)
    except SearchProcess.DoesNotExist:
        print('search process does not exist')
        return

    if search_process.requester and 'requester' not in parameters:
        parameters['requester'] = search_process.requester.id

    if parameters['module'] == 'water_temperature':
        search = WaterTemperatureModule(parameters)
    elif parameters['module'] == 'physico_chemistry':
        search = PhysicoChemistryModule(parameters)
    elif parameters['module'] == 'climate':
        search = ClimateModule(parameters)
    elif parameters['module'] == 'groundwater':
        search = GroundwaterModule(parameters)
    else:
        search = CollectionSearch(parameters)

    if background:
        lock_id = '{0}-lock-{1}'.format(
            search_process.file_path,
            search_process.process_id
        )
        oid = '{0}'.format(search_process.process_id)
        with memcache_lock(lock_id, oid) as acquired:
            if acquired:
                search_process.set_status(SEARCH_PROCESSING)
                search_results = search.get_summary_data()
                if search_results:
                    search_process.set_search_raw_query(
                        search.location_sites_raw_query
                    )
                    search_process.create_view()
                    search_process.set_status(SEARCH_FINISHED, False)
                    search_results['status'] = SEARCH_FINISHED
                    search_results['extent'] = search.extent()
                    search_process.save_to_file(search_results)
                else:
                    search_process.set_status(SEARCH_FAILED)
                return
        logger.info(
            'Search %s is already being processed by another worker',
            search_process.process_id)
    else:
        search_results = search.get_summary_data()
        if search_results:
            search_process.set_search_raw_query(
                search.location_sites_raw_query
            )
            search_process.create_view()
            search_process.set_status(SEARCH_FINISHED, False)
            search_results['status'] = SEARCH_FINISHED
            search_results['extent'] = search.extent()
            search_process.save_to_file(search_results)
        else:
            search_process.set_status(SEARCH_FAILED)
        return search_results
