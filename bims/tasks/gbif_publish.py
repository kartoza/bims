import zlib
from contextlib import contextmanager
from django.db import connection, transaction
from django.utils import timezone
from celery import shared_task
from django_tenants.utils import schema_context, get_public_schema_name, get_tenant_model

from bims.utils.gbif_publish import publish_gbif_data_with_config


@contextmanager
def pg_advisory_lock(key1: int, key2: int):
    with connection.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s, %s)", [key1, key2])
        locked = cur.fetchone()[0]
    try:
        yield locked
    finally:
        if locked:
            with connection.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s, %s)", [key1, key2])


def _to_signed_int32(val: int) -> int:
    """Convert unsigned 32-bit int to signed 32-bit int for PostgreSQL."""
    val = val & 0xFFFFFFFF
    if val >= 0x80000000:
        val -= 0x100000000
    return val


def _tenant_lock_keys(schema_name: str, publish_id: int) -> tuple[int, int]:
    tkey = zlib.crc32(f"gbif_publish_{schema_name}".encode("utf-8"))
    return _to_signed_int32(tkey), _to_signed_int32(publish_id)


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    max_retries=3,
    soft_time_limit=30*60,
    time_limit=35*60,
    queue="update",
)
def run_scheduled_gbif_publish(self, schema_name: str, publish_id: int, trigger: str = "scheduled"):
    """
    Execute a scheduled GBIF publish job.

    Publishes occurrence data to GBIF using the configuration specified
    in the GbifPublish schedule.
    """
    from bims.models.gbif_publish import (
        GbifPublish, GbifPublishSession, GbifPublishContact, PublishStatus, PublishTrigger
    )

    Tenant = get_tenant_model()
    with schema_context(get_public_schema_name()):
        if not Tenant.objects.filter(schema_name=schema_name).exists():
            return {"status": "missing_tenant", "schema_name": schema_name}

    with schema_context(schema_name):
        temp_k1, temp_k2 = _tenant_lock_keys(schema_name, publish_id)
        with pg_advisory_lock(temp_k1, temp_k2) as locked:
            if not locked:
                return {
                    "status": "skipped_locked",
                    "schema": schema_name,
                    "publish_id": publish_id,
                }

            with transaction.atomic():
                publish_schedule = (
                    GbifPublish.objects
                    .select_for_update()
                    .get(id=publish_id)
                )

                if not publish_schedule.enabled:
                    return {"status": "disabled"}

                if not publish_schedule.gbif_config:
                    return {"status": "no_config", "publish_id": publish_id}

                if not publish_schedule.gbif_config.is_active:
                    return {"status": "config_inactive", "publish_id": publish_id}

                config = publish_schedule.gbif_config
                source_reference = publish_schedule.source_reference

                if source_reference and not source_reference.publish_to_gbif:
                    return {
                        "status": "excluded_from_gbif",
                        "publish_id": publish_id,
                        "source_reference": str(source_reference),
                    }

                select = ("user", "user__bims_profile", "user__bims_profile__role")
                config_contacts = list(
                    GbifPublishContact.objects
                    .filter(gbif_config=config)
                    .select_related(*select)
                )
                schedule_contacts = list(
                    GbifPublishContact.objects
                    .filter(gbif_publish=publish_schedule)
                    .select_related(*select)
                )
                # Merge: config contacts first, then schedule-specific additions.
                # Deduplicate by pk so the same contact row is never included twice.
                seen_pks = set()
                contacts = []
                for c in config_contacts + schedule_contacts:
                    if c.pk not in seen_pks:
                        seen_pks.add(c.pk)
                        contacts.append(c)

                if not contacts:
                    return {"status": "no_contacts", "publish_id": publish_id}

                session = GbifPublishSession.objects.create(
                    schedule=publish_schedule,
                    source_reference=source_reference,
                    gbif_config=config,
                    status=PublishStatus.RUNNING,
                    trigger=trigger if trigger in [t.value for t in PublishTrigger] else PublishTrigger.SCHEDULED,
                )

            # Reuse the existing GBIF dataset if this schedule has published
            # successfully before (one dataset per module per platform).
            last_success = (
                GbifPublishSession.objects
                .filter(schedule=publish_schedule, status=PublishStatus.SUCCESS)
                .exclude(dataset_key="")
                .order_by("-start_time")
                .first()
            )
            existing_dataset_key = last_success.dataset_key if last_success else ""
            existing_archive_url = last_success.archive_url if last_success else ""

            try:
                result = publish_gbif_data_with_config(
                    config=config,
                    source_reference=source_reference,
                    existing_dataset_key=existing_dataset_key,
                    existing_archive_url=existing_archive_url,
                    contacts=contacts,
                )

                # Update session with success
                session.status = PublishStatus.SUCCESS
                session.end_time = timezone.now()
                session.dataset_key = result.get("dataset_key", "")
                session.records_published = result.get("records_published", 0)
                session.archive_url = result.get("archive_url", "")
                session.save()

                publish_schedule.last_publish = timezone.now()
                publish_schedule.save(update_fields=["last_publish"])

                return {
                    "status": "success",
                    "schema": schema_name,
                    "publish_id": publish_id,
                    "session_id": session.id,
                    "source_reference": str(source_reference) if source_reference else None,
                    "dataset_key": result.get("dataset_key"),
                    "records_published": result.get("records_published", 0),
                }

            except ValueError as e:
                # No records to publish
                session.status = PublishStatus.NO_RECORDS
                session.end_time = timezone.now()
                session.error_message = str(e)
                session.save()

                return {
                    "status": "no_records",
                    "schema": schema_name,
                    "publish_id": publish_id,
                    "session_id": session.id,
                    "error": str(e),
                }

            except Exception as e:
                # Update session with error
                session.status = PublishStatus.ERROR
                session.end_time = timezone.now()
                session.error_message = str(e)
                session.save()

                return {
                    "status": "error",
                    "schema": schema_name,
                    "publish_id": publish_id,
                    "session_id": session.id,
                    "error": str(e),
                }
