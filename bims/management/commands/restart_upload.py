# coding=utf-8
import logging

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from bims.models.upload_session import UploadSession
from bims.views.data_upload import RESUMABLE_TASK_MAP

logger = logging.getLogger('bims')


class Command(BaseCommand):
    help = (
        'Restart a stalled or failed occurrence/taxa upload session. '
        'Re-queues the Celery task from the last saved checkpoint.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            'session_id',
            nargs='?',
            type=int,
            default=None,
            help='ID of the UploadSession to restart. '
                 'If omitted, all non-processed, non-cancelled resumable sessions are restarted.',
        )
        parser.add_argument(
            '--category',
            dest='category',
            default=None,
            choices=list(RESUMABLE_TASK_MAP.keys()),
            help='Filter sessions by category (taxa, collections, physico_chemical).',
        )
        parser.add_argument(
            '--from-start',
            action='store_true',
            dest='from_start',
            default=False,
            help='Reset start_row to 0 and restart the upload from the beginning.',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            dest='dry_run',
            default=False,
            help='Print what would be restarted without actually queuing any tasks.',
        )

    def handle(self, *args, **options):
        session_id = options.get('session_id')
        category = options.get('category')
        from_start = options.get('from_start')
        dry_run = options.get('dry_run')

        if session_id:
            try:
                sessions = [UploadSession.objects.get(id=session_id)]
            except UploadSession.DoesNotExist:
                raise CommandError(f'UploadSession with id={session_id} does not exist.')
        else:
            qs = UploadSession.objects.filter(
                processed=False,
                canceled=False,
                category__in=RESUMABLE_TASK_MAP.keys(),
            )
            if category:
                qs = qs.filter(category=category)
            sessions = list(qs.order_by('id'))

        if not sessions:
            self.stdout.write(self.style.WARNING('No matching upload sessions found.'))
            return

        from celery import current_app

        restarted = 0
        for session in sessions:
            task_name = RESUMABLE_TASK_MAP.get(session.category)
            if not task_name:
                self.stdout.write(
                    self.style.WARNING(
                        f'Session {session.id} (category={session.category}) '
                        f'is not resumable — skipping.'
                    )
                )
                continue

            if session.processed or session.canceled:
                self.stdout.write(
                    self.style.WARNING(
                        f'Session {session.id} is already completed or cancelled — skipping.'
                    )
                )
                continue

            start_row = 0 if from_start else session.start_row
            self.stdout.write(
                f'{"[DRY RUN] " if dry_run else ""}'
                f'Restarting session {session.id} '
                f'(category={session.category}, start_row={start_row}) ...'
            )

            if not dry_run:
                if from_start:
                    session.start_row = 0
                session.last_progress_update = timezone.now()
                session.progress = 'Restarted via management command'
                update_fields = ['last_progress_update', 'progress']
                if from_start:
                    update_fields.append('start_row')
                session.save(update_fields=update_fields)

                current_app.send_task(task_name, args=[session.id])
                self.stdout.write(
                    self.style.SUCCESS(f'  -> Task {task_name} queued for session {session.id}.')
                )

            restarted += 1

        summary = f'{restarted} session(s) {"would be" if dry_run else ""} restarted.'
        self.stdout.write(self.style.SUCCESS(summary))
