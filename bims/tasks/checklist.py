from celery import shared_task
import logging
from django.db import connection
from django_tenants.utils import (
    schema_context,
    get_public_schema_name,
    get_tenant_model,
)

from bims.tasks import IN_CELERY_WORKER_PROCESS

logger = logging.getLogger(__name__)


@shared_task(name='bims.tasks.download_checklist', queue='update')
def download_checklist(download_request_id, send_email=False, user_id=None):
    from bims.utils.celery import memcache_lock
    from bims.api_views.checklist import generate_checklist
    from bims.tasks.email_csv import send_csv_via_email
    from bims.models.download_request import DownloadRequest

    def process_checklist():
        try:
            status = generate_checklist(download_request_id)
            if status:
                # send email here
                if send_email and user_id:
                    download_request = DownloadRequest.objects.get(
                        id=download_request_id
                    )
                    send_csv_via_email(
                        user_id,
                        download_request.request_file.path,
                        download_request.request_category,
                        download_request.approved,
                        download_request.id
                    )
            return status
        except Exception as e:
            logger.error(f"Error generating checklist for request {download_request_id}: {e}")
            raise

    def log_processing_status(download_request_id):
        logger.info(
            'Download checklist %s is already being processed by another worker',
            download_request_id
        )

    schema_name = connection.schema_name

    if IN_CELERY_WORKER_PROCESS:
        lock_id = (
            f'generate-checklist-lock-{download_request_id}-{schema_name}'
        )
        oid = f'{download_request_id} {schema_name}'

        with memcache_lock(lock_id, oid) as acquired:
            if acquired:
                return process_checklist()
            else:
                log_processing_status(download_request_id)
    else:
        return process_checklist()

    log_processing_status(download_request_id)


@shared_task(name='bims.tasks.publish_versions', queue='update')
def publish_versions_task(schema_name, version_ids, published_by_id=None):
    from django.contrib.auth import get_user_model
    from bims.models.checklist_version import ChecklistVersion

    Tenant = get_tenant_model()
    with schema_context(get_public_schema_name()):
        if not Tenant.objects.filter(schema_name=schema_name).exists():
            return {
                'status': 'missing_tenant',
                'schema_name': schema_name,
                'published': 0,
                'already_published': 0,
                'failed': 0,
            }

    with schema_context(schema_name):
        user = None
        if published_by_id:
            user = get_user_model().objects.filter(id=published_by_id).first()

        versions = list(
            ChecklistVersion.objects.filter(pk__in=version_ids)
            .select_related('taxon_group')
        )
        requested_count = len(version_ids)
        missing_count = max(requested_count - len(versions), 0)
        published = 0
        already_published = 0
        failed = 0
        errors = []

        for version in versions:
            if version.status == ChecklistVersion.STATUS_PUBLISHED:
                already_published += 1
                continue

            try:
                version.publish(published_by=user)
                published += 1
            except Exception as exc:
                failed += 1
                errors.append(f'{version}: {exc}')
                logger.exception(
                    'Failed to publish checklist version %s in schema %s',
                    version.pk,
                    schema_name,
                )
                # Clear the publishing flag so the UI doesn't stay stuck
                version.is_publishing = False
                version.save(update_fields=['is_publishing'])

        return {
            'status': 'completed',
            'schema_name': schema_name,
            'requested': requested_count,
            'published': published,
            'already_published': already_published,
            'failed': failed,
            'missing': missing_count,
            'errors': errors,
        }


@shared_task(
    name='bims.tasks.delete_published_checklist_version',
    queue='update',
    ignore_result=True,
)
def delete_published_checklist_version_task(schema_name, version_id):
    from bims.models.checklist_version import ChecklistVersion
    from bims.models.download_request import DownloadRequest
    from bims.models.taxonomy import Taxonomy

    Tenant = get_tenant_model()
    with schema_context(get_public_schema_name()):
        if not Tenant.objects.filter(schema_name=schema_name).exists():
            return

    with schema_context(schema_name):
        version = (
            ChecklistVersion.objects
            .select_related('taxon_group')
            .filter(pk=version_id)
            .first()
        )
        if not version:
            return

        Taxonomy.objects.filter(
            checklist_version_uuid=version.pk
        ).update(checklist_version_uuid=None)
        Taxonomy.objects.filter(
            last_checklist_published_uuid=version.pk
        ).update(last_checklist_published_uuid=None)

        related_downloads = DownloadRequest.objects.filter(
            resource_name=f'Checklist ColDP ZIP {version.pk}'
        )
        for download_request in related_downloads:
            if download_request.request_file:
                download_request.request_file.delete(save=False)
            download_request.delete()

        version.delete()
