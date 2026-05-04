# coding=utf-8
from hashlib import sha256
import json
import os
import errno
from preferences import preferences

from datetime import datetime, timedelta
from django.conf import settings
from django.http import HttpResponseForbidden
from django.template.loader import render_to_string
from django.core.mail import EmailMultiAlternatives
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from bims.tasks.collection_record import download_collection_record_task
from bims.tasks.email_csv import send_csv_via_email
from bims.models.notification import (
    get_recipients_for_notification, DOWNLOAD_REQUEST
)
from bims.utils.domain import get_current_domain
from bims.tasks.send_notification import send_mail_notification

STALE_THRESHOLD_MINUTES = 10


class CsvDownload(APIView):
    """API to make csv download requests via email."""

    def get_hashed_name(self, request):
        query_string = json.dumps(
            request.GET.dict()
        ) + datetime.today().strftime('%Y%m%d')
        return sha256(
            query_string.encode('utf-8')
        ).hexdigest()

    def get(self, request, *args):
        from bims.models.download_request import (
            DownloadRequest,
            normalize_download_params,
            params_from_dashboard_url,
        )
        # User need to be logged in before requesting csv download
        if not request.user.is_authenticated:
            return HttpResponseForbidden('Not logged in')

        # Check if the file exists in the processed directory
        filename = self.get_hashed_name(request)
        folder = settings.PROCESSED_CSV_PATH
        download_request_id = self.request.GET.get('downloadRequestId', '')

        if not os.path.exists(os.path.join(settings.MEDIA_ROOT, folder)):
            os.mkdir(os.path.join(settings.MEDIA_ROOT, folder))

        path_folder = os.path.join(
            settings.MEDIA_ROOT,
            folder,
            request.user.username
        )

        try:
            os.mkdir(path_folder)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
            pass

        path_file = os.path.join(path_folder, filename)

        try:
            download_request = DownloadRequest.objects.get(
                id=download_request_id
            )
        except DownloadRequest.DoesNotExist:
            return Response({
                'status': 'failed',
                'message': 'Download request does not exist'
            })

        parsed_path_file, parsed_params_dict = params_from_dashboard_url(
            download_request
        )
        if parsed_path_file and parsed_params_dict:
            path_file = parsed_path_file
            filename = os.path.basename(path_file)
            params_dict = parsed_params_dict
        else:
            params_dict = normalize_download_params(self.request.GET)

        # Persist the path and params so the resume task can restart the download
        needs_save = False
        if download_request.download_path != path_file:
            download_request.download_path = path_file
            needs_save = True
        if download_request.download_params != params_dict:
            download_request.download_params = params_dict
            needs_save = True
        if needs_save:
            download_request.save(update_fields=['download_path', 'download_params'])

        max_limit = preferences.SiteSetting.max_download_records
        approval_needed = preferences.SiteSetting.enable_download_request_approval
        if max_limit and max_limit > 0 and not download_request.approved:
            from bims.api_views.search import CollectionSearch
            _search = CollectionSearch(params_dict, request.user.id)
            _total = _search.process_search().count()
            if _total > max_limit:
                send_new_csv_notification(
                    request.user,
                    download_request.request_date,
                    approval_needed=True,
                    download_request_id=download_request_id,
                    over_limit=True,
                    total_records=_total,
                    max_records=max_limit,
                )
                return Response({
                    'status': 'pending_approval',
                    'total_records': _total,
                    'max_records': max_limit,
                })

        if not approval_needed and not download_request.approved:
            download_request.approved = True
            download_request.save(update_fields=['approved'])
            send_new_csv_notification(
                request.user,
                download_request.request_date,
                approval_needed=False,
                download_request_id=download_request_id,
            )

        if download_request.request_file and os.path.exists(
                str(download_request.request_file)):
            # Already completed - send via email again
            send_csv_via_email.delay(
                user_id=request.user.id,
                csv_file=str(download_request.request_file),
                download_request_id=download_request_id
            )
            return Response({
                'status': 'processing',
                'filename': filename
            })

        file_exists = os.path.exists(path_file)
        is_stale = False
        if file_exists and download_request.progress_updated_at:
            stale_cutoff = timezone.now() - timedelta(minutes=STALE_THRESHOLD_MINUTES)
            is_stale = download_request.progress_updated_at < stale_cutoff

        if file_exists and not is_stale:
            # Already in progress – do not start a duplicate task
            return Response({
                'status': 'processing',
                'filename': filename
            })

        if is_stale:
            # Delete any partial file so the task starts clean
            try:
                os.remove(path_file)
            except OSError:
                pass
            download_request.progress = None
            download_request.progress_updated_at = None
            download_request.save(update_fields=['progress', 'progress_updated_at'])

        if not file_exists or is_stale:
            download_request.progress_updated_at = timezone.now()
            download_request.save(update_fields=['progress_updated_at'])

            download_collection_record_task.delay(
                path_file,
                params_dict,
                send_email=True,
                user_id=self.request.user.id
            )

        return Response({
            'status': 'processing',
            'filename': filename
        })


def send_new_csv_notification(
        user, date_request, approval_needed=True,
        download_request_id=None, over_limit=False,
        total_records=0, max_records=0):
    """
    Send an email notify admin/staff that new request has been created
    :param user: User object
    :param date_request: Date of request
    :param approval_needed: Whether manual approval is required
    :param download_request_id: PK of the DownloadRequest (for detail URL)
    :param over_limit: True when request was held because record count exceeds max_download_records
    :param total_records: Actual record count (used in over_limit message)
    :param max_records: Configured limit (used in over_limit message)
    """
    email_template = 'csv_download/csv_new'
    recipients = get_recipients_for_notification(
        DOWNLOAD_REQUEST
    )
    ctx = {
        'username': user.username,
        'current_site': get_current_domain(),
        'date_request': date_request,
        'download_request_id': download_request_id,
        'over_limit': over_limit,
        'total_records': total_records,
        'max_records': max_records,
    }
    subject = render_to_string(
        '{0}_subject.txt'.format(email_template),
        ctx
    ).strip()
    if not approval_needed and not over_limit:
        message = render_to_string(
            '{}_message_without_approval.txt'.format(email_template),
            ctx
        ).strip()
    else:
        message = render_to_string(
            '{}_message.txt'.format(email_template),
            ctx
        ).strip()

    send_mail_notification.delay(
        subject,
        message,
        settings.DEFAULT_FROM_EMAIL,
        recipients
    )


def send_rejection_csv(user, rejection_message = ''):
    """
    Send an email notify user that the request has been declined
    :param user: User object
    :param rejection_message: Message of the rejection
    :return:
    """
    email_template = 'csv_download/csv_rejected'
    ctx = {
        'username': user.username,
        'current_site': get_current_domain(),
        'rejection_message': rejection_message
    }
    subject = render_to_string(
        '{0}_subject.txt'.format(email_template),
        ctx
    )
    message = render_to_string(
        '{}_message.txt'.format(email_template),
        ctx
    )
    msg = EmailMultiAlternatives(
        subject,
        message,
        settings.DEFAULT_FROM_EMAIL,
        [user.email])
    msg.content_subtype = 'html'
    msg.send()
