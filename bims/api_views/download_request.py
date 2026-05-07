import io
import os
import zipfile
from datetime import datetime, timedelta

from django.core.exceptions import SuspiciousFileOperation
from django.contrib.sites.models import Site
from django.http import FileResponse
from django.utils import timezone
from django.utils.timezone import make_aware
from preferences import preferences

from rest_framework import status

from bims.download.csv_download import send_new_csv_notification, STALE_THRESHOLD_MINUTES
from bims.models.taxonomy import Taxonomy

from bims.models.location_site import LocationSite
from django.http.response import Http404
from django.shortcuts import get_object_or_404
from rest_framework.views import APIView
from rest_framework.response import Response

from bims.models.download_request import (
    DownloadRequest,
    DownloadRequestPurpose
)
from bims.utils.filepath import ensure_within_dir


class DownloadRequestApi(APIView):
    """
    An API view for handling download requests. The view accepts POST requests
    with resource_name, resource_type, and purpose as required fields. It supports
    optional fields like dashboard_url, site_id, taxon_id, and notes. The view
    processes the download request and sends a notification email if the request
    is successful.
    """

    def post(self, request):
        """
        Handles the POST request for creating a new download request. Validates the
        input, creates the download request, and sends a notification email if
        successful.
        """
        if not request.user.is_authenticated:
            return Response(
                {'error': 'User needs to be logged in first'},
                status=status.HTTP_401_UNAUTHORIZED)

        resource_name = request.POST.get('resource_name')
        resource_type = request.POST.get('resource_type')
        purpose = request.POST.get('purpose')
        dashboard_url = request.POST.get('dashboard_url', '')
        site_id = request.POST.get('site_id', '')
        taxon_id = request.POST.get('taxon_id', '')
        notes = request.POST.get('notes', '')
        auto_approved = request.POST.get('auto_approved', 'False').lower() == 'true'
        location_site = None
        taxon = None
        success = False

        approval_needed = (
            preferences.SiteSetting.enable_download_request_approval
        )

        if resource_type in ['CSV', 'PDF', 'XLS']:
            # Check unfinished big download tasks e.g. download occurrences or taxa list
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            start_date = today - timedelta(days=5)
            start_date = make_aware(start_date)
            end_date = make_aware(today + timedelta(days=1))

            download_requests = DownloadRequest.objects.filter(
                resource_name__in=['Occurrence Data', 'Taxa List'],
                resource_type__in=['CSV', 'PDF', 'XLS'],
                requester=self.request.user,
                request_date__range=(start_date, end_date),
            )
            for download_request in download_requests:
                progress = download_request.progress
                if progress:
                    try:
                        completed, total = progress.split('/')
                        completed = int(completed)
                        total = int(total)

                        if completed < total:
                            return Response(
                                {'error':
                                     'There are still ongoing download requests. '
                                     'Please wait for them to complete before trying again.'},
                                status=status.HTTP_401_UNAUTHORIZED)
                    except ValueError:
                        continue

        if not resource_name or not resource_type or not purpose:
            raise Http404('Missing required field.')

        if site_id:
            location_site = get_object_or_404(
                LocationSite,
                id=site_id
            )

        if taxon_id:
            taxon = get_object_or_404(
                Taxonomy,
                id=taxon_id
            )

        download_request_purpose = get_object_or_404(
            DownloadRequestPurpose,
            id=purpose
        )

        requester = (
            self.request.user if not self.request.user.is_anonymous else None
        )

        download_request, created = DownloadRequest.objects.get_or_create(
            resource_name=resource_name,
            resource_type=resource_type,
            purpose=download_request_purpose,
            requester=requester,
            dashboard_url=dashboard_url,
            location_site=location_site,
            taxon=taxon,
            notes=notes,
            request_date=datetime.now()
        )

        # For occurrence CSV/XLS downloads, defer approval to CsvDownload where
        # the record count can be checked against max_download_records.
        is_occurrence_csv = (
            resource_type in ['CSV', 'XLS'] and
            resource_name == 'Occurrence Data'
        )
        if (not approval_needed or auto_approved) and not is_occurrence_csv:
            download_request.approved = True
            download_request.save()

        if download_request:
            # For occurrence CSV/XLS without forced approval, defer the
            # notification to CsvDownload.get where the record count is known
            # so the correct message (over-limit vs auto-approved) can be sent.
            defer_notification = is_occurrence_csv and not approval_needed
            if not defer_notification:
                send_new_csv_notification(
                    download_request.requester,
                    download_request.request_date,
                    approval_needed,
                    download_request_id=download_request.id,
                )

        return Response({
            'success': success,
            'download_request_id': download_request.id
        })


class DownloadRequestProgressApi(APIView):
    """
    GET /api/download-request/<id>/progress/

    Returns the current progress of a download request.
    Response fields:
      - progress      : raw progress string e.g. "250/1000"
      - completed     : number of rows processed so far
      - total         : total number of rows
      - percentage    : 0-100 integer
      - is_stale      : true when the worker appears to have stopped
      - is_finished   : true when completed == total (or request_file exists)
    """

    def get(self, request, download_request_id):
        if not request.user.is_authenticated:
            return Response(
                {'error': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )
        try:
            dr = DownloadRequest.objects.get(id=download_request_id)
        except DownloadRequest.DoesNotExist:
            return Response(
                {'error': 'Download request not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Only the requester (or staff) may check progress
        if dr.requester and dr.requester != request.user and not request.user.is_staff:
            return Response(
                {'error': 'Permission denied'},
                status=status.HTTP_403_FORBIDDEN
            )

        completed = 0
        total = 0
        percentage = 0
        is_finished = bool(dr.request_file)

        if dr.progress:
            try:
                completed, total = (int(x) for x in dr.progress.split('/'))
                if total > 0:
                    percentage = min(100, int(completed * 100 / total))
                if completed >= total > 0:
                    is_finished = True
            except (ValueError, ZeroDivisionError):
                pass

        is_stale = False
        if not is_finished and dr.progress and dr.progress_updated_at:
            stale_cutoff = timezone.now() - timedelta(minutes=STALE_THRESHOLD_MINUTES)
            is_stale = dr.progress_updated_at < stale_cutoff

        return Response({
            'progress': dr.progress or '',
            'completed': completed,
            'total': total,
            'percentage': percentage,
            'is_stale': is_stale,
            'is_finished': is_finished,
        })


class DownloadRequestUploadApi(APIView):
    """
    POST /api/download-request/<id>/upload/

    Accepts a file upload (chart, image, table) generated client-side and
    saves it as the request_file for the download request. After uploading,
    the file can be downloaded via the file endpoint.
    """

    def post(self, request, download_request_id):
        if not request.user.is_authenticated:
            return Response(
                {'error': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            dr = DownloadRequest.objects.get(id=download_request_id)
        except DownloadRequest.DoesNotExist:
            return Response(
                {'error': 'Download request not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        if dr.requester and dr.requester != request.user and not request.user.is_staff:
            return Response(
                {'error': 'Permission denied'},
                status=status.HTTP_403_FORBIDDEN
            )

        uploaded_file = request.FILES.get('file')
        if not uploaded_file:
            return Response(
                {'error': 'No file provided'},
                status=status.HTTP_400_BAD_REQUEST
            )

        dr.request_file = uploaded_file
        dr.processing = False
        dr.save()

        return Response({
            'success': True,
            'download_url': f'/api/download-request/{dr.id}/file/'
        })


class DownloadRequestFileApi(APIView):
    """
    GET /api/download-request/<id>/file/

    Returns the request file as a zipped download, including the license/readme
    file if configured, matching the format sent via email.
    """

    def get(self, request, download_request_id):
        if not request.user.is_authenticated:
            return Response(
                {'error': 'Authentication required'},
                status=status.HTTP_401_UNAUTHORIZED
            )

        try:
            dr = DownloadRequest.objects.get(id=download_request_id)
        except DownloadRequest.DoesNotExist:
            return Response(
                {'error': 'Download request not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        is_admin = request.user.is_staff or request.user.is_superuser
        is_requester = dr.requester == request.user

        if not is_admin and not is_requester:
            return Response(
                {'error': 'Permission denied'},
                status=status.HTTP_403_FORBIDDEN
            )

        if not (dr.approved or is_admin):
            return Response(
                {'error': 'This download has not been approved yet'},
                status=status.HTTP_403_FORBIDDEN
            )

        if not dr.request_file:
            return Response(
                {'error': 'No file available for this request'},
                status=status.HTTP_404_NOT_FOUND
            )

        if not dr.request_file.storage.exists(dr.request_file.name):
            return Response(
                {'error': 'File not found on server'},
                status=status.HTTP_404_NOT_FOUND
            )

        file_name = dr.request_category or os.path.basename(dr.request_file.name)

        ext = os.path.splitext(dr.request_file.name)[1].lstrip('.')
        if ext == 'zip' or dr.resource_type == DownloadRequest.ZIP:
            download_name = file_name if file_name.lower().endswith('.zip') else f'{file_name}.zip'
            return FileResponse(
                dr.request_file.open('rb'),
                content_type='application/zip',
                as_attachment=True,
                filename=download_name,
            )

        if not ext:
            if dr.resource_type == DownloadRequest.PDF:
                ext = 'pdf'
            elif dr.resource_type == DownloadRequest.XLS:
                ext = 'xlsx'
            else:
                ext = 'csv'

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            with dr.request_file.open('rb') as request_file_handle:
                zf.writestr(f'{file_name}.{ext}', request_file_handle.read())
            readme = preferences.SiteSetting.readme_download
            if readme:
                try:
                    safe_readme_path = ensure_within_dir(
                        readme.path,
                        readme.storage.location,
                    )
                    with open(safe_readme_path, 'rb') as readme_handle:
                        zf.writestr(
                            os.path.basename(readme.name),
                            readme_handle.read(),
                        )
                except (FileNotFoundError, SuspiciousFileOperation, ValueError):
                    pass
        buf.seek(0)

        response = FileResponse(
            buf,
            content_type='application/zip',
            as_attachment=True,
            filename=f'{file_name}.zip',
        )
        return response
