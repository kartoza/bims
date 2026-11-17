# coding=utf-8
import logging
from django.db import connection

from celery.bin.upgrade import settings
from django.contrib.auth import get_user_model
from django.core.mail import send_mail
from django.db.models import signals, OuterRef, Exists

from celery import shared_task

from bims.cache import set_cache, HARVESTING_GEOCONTEXT
from bims.utils.logger import log

logger = logging.getLogger(__name__)


@shared_task(name='bims.tasks.location_sites_overview', queue='search')
def location_sites_overview(
        search_parameters=None,
        search_process_id=None
):
    from bims.utils.celery import memcache_lock
    from bims.api_views.location_site_overview import (
        LocationSiteOverviewData
    )
    from bims.models.search_process import (
        SearchProcess,
        SEARCH_PROCESSING,
        SEARCH_FINISHED
    )

    if search_parameters is None:
        search_parameters = {}

    try:
        search_process = SearchProcess.objects.get(
            id=search_process_id
        )
    except SearchProcess.DoesNotExist:
        return

    lock_id = '{0}-lock-{1}'.format(
        search_process.file_path,
        search_process.process_id
    )
    oid = '{0}'.format(search_process.process_id)
    with memcache_lock(lock_id, oid) as acquired:
        if acquired:
            search_process.set_status(SEARCH_PROCESSING)

            overview_data = LocationSiteOverviewData()
            if search_process.requester and 'requester' not in search_parameters:
                search_parameters['requester'] = search_process.requester.id

            overview_data.search_filters = search_parameters

            results = dict()
            results[LocationSiteOverviewData.BIODIVERSITY_DATA] = (
                overview_data.biodiversity_data()
            )
            results[LocationSiteOverviewData.SASS_EXIST] = (
                overview_data.is_sass_exist
            )
            results[LocationSiteOverviewData.CLIMATE_EXIST] = (
                overview_data.is_climate_data_exist
            )
            search_process.set_status(SEARCH_FINISHED, False)
            search_process.save_to_file(results)
    logger.info(
        'Search %s is already being processed by another worker',
        search_process.process_id)


@shared_task(bind=True, name='bims.tasks.update_location_context', queue='geocontext')
def update_location_context(
        self,
        location_site_id,
        generate_site_code=False,
        generate_filter=True,
        only_empty=False):
    from bims.models import LocationSite, location_site_post_save_handler
    from bims.utils.location_context import get_location_context_data
    from bims.models.location_context_group import LocationContextGroup
    from bims.models.location_context_filter_group_order import (
        location_context_post_save_handler
    )
    group_keys = None

    self.update_state(state='STARTED', meta={'process': 'Checking location site'})

    if not location_site_id:
        self.update_state(
            state='PROGRESS',
            meta={'process': 'Updating location context for multiple sites'})
        try:
            get_location_context_data(
                group_keys=group_keys,
                site_id=None,
                only_empty=only_empty,
                should_generate_site_code=generate_site_code
            )
        except Exception:
            set_cache(HARVESTING_GEOCONTEXT, False)
        return

    if isinstance(location_site_id, str):
        self.update_state(
            state='PROGRESS',
            meta={'process': 'Updating location context for multiple IDs'})
        if ',' in location_site_id:
            get_location_context_data(
                group_keys=group_keys,
                site_id=str(location_site_id),
                only_empty=only_empty,
                should_generate_site_code=generate_site_code
            )
            return
    try:
        LocationSite.objects.get(id=location_site_id)
    except LocationSite.DoesNotExist:
        log('Location site does not exist')
        return

    if not generate_filter:
        signals.post_save.disconnect(
            location_context_post_save_handler,
            sender=LocationContextGroup
        )
    signals.post_save.disconnect(
        location_site_post_save_handler,
        sender=LocationSite
    )
    self.update_state(state='PROGRESS', meta={'process': 'Updating location context'})
    get_location_context_data(
        group_keys=group_keys,
        site_id=str(location_site_id),
        only_empty=False,
        should_generate_site_code=generate_site_code
    )
    self.update_state(state='SUCCESS')
    signals.post_save.connect(
        location_site_post_save_handler,
        sender=LocationSite
    )
    if not generate_filter:
        signals.post_save.connect(
            location_context_post_save_handler,
            sender=LocationContextGroup
        )
    return 'Finished updating location context'


@shared_task(name='bims.tasks.update_site_code', queue='geocontext')
def update_site_code(location_site_ids):
    from bims.models import LocationSite
    from bims.models import generate_site_code

    for location_site_id in location_site_ids:
        try:
            location_site = LocationSite.objects.get(id=location_site_id)
        except LocationSite.DoesNotExist:
            continue
        location_site.site_code, catchments_data = generate_site_code(
            location_site=location_site,
            lat=location_site.latitude,
            lon=location_site.longitude
        )
        location_site.save()


def _dangling_queryset(model, *, skip_models=frozenset(), related_querysets=None):
    """
    Return a queryset of <model> instances that have no related
    objects on any *auto-created* reverse relation except those
    explicitly listed in `skip_models`.
    """
    related_querysets = related_querysets or {}
    qs = model.objects.all()

    for rel in model._meta.get_fields():
        if not (rel.auto_created and not rel.concrete):
            continue
        if rel.related_model in skip_models:
            continue

        link_field = (
            rel.field.name
            if hasattr(rel, "field") and rel.field
            else rel.field.m2m_field_name()
        )
        base_qs = related_querysets.get(
            rel.related_model, rel.related_model.objects.all()
        )
        sub_qs = base_qs.filter(**{link_field: OuterRef("pk")})
        qs = qs.filter(~Exists(sub_qs))

    return qs


DRY_RUN_ID_PREVIEW_LIMIT = 200


def _format_id_preview(ids):
    ids = list(ids)
    if not ids:
        return "-"
    preview = ", ".join(map(str, ids[:DRY_RUN_ID_PREVIEW_LIMIT]))
    if len(ids) > DRY_RUN_ID_PREVIEW_LIMIT:
        preview += f", ... (+{len(ids) - DRY_RUN_ID_PREVIEW_LIMIT} more)"
    return preview


@shared_task(name="bims.tasks.remove_dangling_sites", queue="search")
def remove_dangling_sites(dry_run=False):
    """
    1. Delete surveys that have no child data.
    2. Delete location-sites that are no longer referenced.
    3. E-mail a report to all super-users.

    With ``dry_run=True`` nothing is deleted; the report lists what
    would be removed.
    """
    from bims.models import Survey
    from bims.models.location_site import LocationSite
    from bims.models.location_context import LocationContext

    dangling_surveys_qs = _dangling_queryset(Survey)
    surveys_total = dangling_surveys_qs.count()

    if dry_run:
        survey_ids = list(
            dangling_surveys_qs.order_by("pk").values_list("pk", flat=True)
        )
        remaining_surveys_qs = Survey.objects.exclude(pk__in=survey_ids)
        dangling_sites_qs = _dangling_queryset(
            LocationSite,
            skip_models=frozenset({LocationContext}),
            related_querysets={Survey: remaining_surveys_qs},
        )
        site_ids = list(
            dangling_sites_qs.order_by("pk").values_list("pk", flat=True)
        )
        sites_total = len(site_ids)
        log(f"[DRY RUN] {surveys_total} dangling Survey rows would be deleted.")
        log(f"[DRY RUN] {sites_total} dangling LocationSite rows would be deleted.")
    else:
        surveys_deleted, _ = dangling_surveys_qs.delete()
        log(f"Deleted {surveys_deleted}/{surveys_total} dangling Survey rows.")

        dangling_sites_qs = _dangling_queryset(
            LocationSite, skip_models=frozenset({LocationContext})
        )
        sites_total = dangling_sites_qs.count()
        deleted_all, detail = dangling_sites_qs.delete()
        sites_deleted = detail.get('bims.LocationSite', 0)
        log(f"Deleted {sites_deleted}/{sites_total} dangling LocationSite rows.")

    superusers = (
        get_user_model()
        .objects.filter(is_superuser=True, email__isnull=False)
        .values_list("email", flat=True)
    )

    tenant = connection.tenant
    try:
        tenant_name = tenant.name
    except AttributeError:
        tenant_name = ''
    if superusers:
        if dry_run:
            subject = f"[{tenant_name}] [DRY RUN] Dangling data cleanup report"
            message = (
                "Dry run completed. No data was deleted.\n\n"
                f"• Surveys that would be deleted       : {surveys_total}\n"
                f"• Location sites that would be deleted: {sites_total}\n\n"
                f"Survey IDs: {_format_id_preview(survey_ids)}\n\n"
                f"Location site IDs: {_format_id_preview(site_ids)}\n"
            )
        else:
            subject = f"[{tenant_name}] Dangling data cleanup report"
            message = (
                "Automatic cleanup completed.\n\n"
                f"• Surveys deleted       : {surveys_deleted}/{surveys_total}\n"
                f"• Location sites deleted: {sites_deleted}/{sites_total}\n"
            )
        send_mail(
            subject=subject,
            message=message,
            from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None),
            recipient_list=list(superusers),
            fail_silently=True,
        )
