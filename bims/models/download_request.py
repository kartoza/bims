# coding=utf-8
"""Csv download model definition.

"""
import errno
import json
import os
from datetime import datetime
from hashlib import sha256
from urllib.parse import urlparse, parse_qs

from dateutil.relativedelta import relativedelta
from django.contrib.sites.models import Site
from django.db import models
from django.conf import settings
from django.core.exceptions import ValidationError
from bims.tasks.email_csv import send_csv_via_email
from bims.download.csv_download import (
    send_rejection_csv
)


def _last_non_empty_value(values):
    if not values:
        return ''
    non_empty = [value for value in values if value not in (None, '')]
    return non_empty[-1] if non_empty else values[-1]


def normalize_download_params(params):
    """
    Normalize download params into a flat dict of scalar values.

    If the same key appears multiple times, keep the last non-empty value.
    This prevents malformed appended query parameters from being persisted as
    lists and matches current download behavior, which expects scalar values.
    """
    if hasattr(params, 'lists'):
        items = params.lists()
    else:
        items = params.items()

    normalized = {}
    for key, value in items:
        if isinstance(value, (list, tuple)):
            normalized[key] = _last_non_empty_value(list(value))
        else:
            normalized[key] = value
    return normalized


def params_from_dashboard_url(download_request):
    """
    Derive download_params and download_path from dashboard_url when they are
    missing on the DownloadRequest.

    Returns (path_file, params_dict) or (None, None) if it cannot be derived.
    """
    if not download_request.dashboard_url:
        return None, None

    parsed = urlparse(download_request.dashboard_url)

    # Params live in the URL fragment as  #<prefix>/<key=val&key=val...>
    # e.g. #site-detail/taxon=&siteId=123&...
    # Fall back to the regular query string if the fragment is absent.
    fragment = parsed.fragment
    if fragment:
        # Drop any leading path segment (everything up to and including the first '/')
        sep = fragment.find('/')
        param_string = fragment[sep + 1:] if sep != -1 else fragment
    else:
        param_string = parsed.query

    qs = parse_qs(param_string, keep_blank_values=False)
    params_dict = normalize_download_params(qs)
    params_dict['downloadRequestId'] = str(download_request.pk)

    username = (
        download_request.requester.username
        if download_request.requester else 'unknown'
    )
    query_string = json.dumps(params_dict) + datetime.today().strftime('%Y%m%d')
    filename = sha256(query_string.encode('utf-8')).hexdigest()
    folder = settings.PROCESSED_CSV_PATH
    path_folder = os.path.join(settings.MEDIA_ROOT, folder, username)
    try:
        os.makedirs(path_folder, exist_ok=True)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise
    path_file = os.path.join(path_folder, filename)
    return path_file, params_dict


def validate_file_extension(value):
    import os
    ext = os.path.splitext(value.name)[1].lower()
    valid_extensions = ['.csv', '.xlsx', '.xls', '.png', '.svg', '.pdf', '.jpg', '.jpeg']
    if ext not in valid_extensions:
        raise ValidationError('File not supported!')


class DownloadRequestPurpose(models.Model):
    name = models.CharField(
        max_length=512,
        blank=False,
        null=False
    )
    description = models.TextField(
        help_text='Optional',
        null=True,
        blank=True
    )
    order = models.IntegerField(
        null=False,
        blank=False
    )
    source_site = models.ForeignKey(
        Site,
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )

    def __str__(self):
        return self.name

    class Meta:
        ordering = ('order',)


class DownloadRequest(models.Model):
    """Download request model
    """
    CSV = 'CSV'
    CHART = 'CHART'
    TABLE = 'TABLE'
    IMAGE = 'IMAGE'
    XLS = 'XLS'
    PDF = 'PDF'

    RESOURCE_TYPE_CHOICES = [
        (CSV, 'Csv'),
        (XLS, 'Xls'),
        (CHART, 'Chart'),
        (TABLE, 'Table'),
        (IMAGE, 'Image'),
        (PDF, 'Pdf'),
    ]

    requester = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        models.CASCADE,
        related_name='download_request_requester',
        blank=True,
        null=True,
    )
    request_date = models.DateTimeField(
        default=datetime.now
    )
    resource_type = models.CharField(
        max_length=10,
        choices=RESOURCE_TYPE_CHOICES,
        default=CSV
    )
    resource_name = models.CharField(
        max_length=256,
        default='',
        blank=True
    )
    taxon = models.ForeignKey(
        'bims.Taxonomy',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )
    location_site = models.ForeignKey(
        'bims.LocationSite',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )
    survey = models.ForeignKey(
        'bims.Survey',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )
    purpose = models.ForeignKey(
        'bims.DownloadRequestPurpose',
        null=True,
        blank=True,
        on_delete=models.CASCADE
    )
    dashboard_url = models.TextField(
        null=True,
        blank=True
    )
    request_file = models.FileField(
        upload_to='request-files/',
        help_text='Only csv file',
        null=True,
        max_length=300,
        blank=True,
        validators=[validate_file_extension]
    )
    notes = models.TextField(
        null=True,
        blank=True
    )
    processing = models.BooleanField(
        default=True
    )
    approved = models.BooleanField(
        default=False
    )
    request_category = models.CharField(
        max_length=256,
        default='',
        blank=True
    )
    rejected = models.BooleanField(
        default=False
    )
    rejection_message = models.TextField(
        null=True,
        blank=True
    )
    progress = models.CharField(
        max_length=255,
        null=True,
        blank=True
    )
    progress_updated_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text='Timestamp of the last progress update'
    )
    download_path = models.CharField(
        max_length=512,
        null=True,
        blank=True,
        help_text='Filesystem path of the in-progress download file'
    )
    download_params = models.JSONField(
        null=True,
        blank=True,
        help_text='Serialised request parameters used to start the download'
    )
    source_site = models.ForeignKey(
        Site,
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )

    @property
    def file_expiry_date(self):
        """Return the expiry date, or None if files never expire."""
        from preferences import preferences
        months = getattr(preferences.SiteSetting, 'download_request_expiry_months', 2)
        if not months:
            return None
        return self.request_date + relativedelta(months=months)

    @property
    def file_has_expired(self):
        """Return True if the file retention period has passed."""
        from django.utils import timezone
        expiry = self.file_expiry_date
        if expiry is None:
            return False
        now = timezone.now()
        if expiry.tzinfo is None:
            from django.utils.timezone import make_aware
            expiry = make_aware(expiry)
        return now > expiry

    def get_formatted_name(self):
        """Return author formated full name, e.g. Maupetit J"""
        if not self.requester:
            return '-'
        if self.requester.first_name or self.requester.last_name:
            return '%s %s' % (
                self.requester.first_name, self.requester.last_name)
        return self.requester.username

    # noinspection PyClassicStyleClass
    class Meta:
        """Meta class for project."""
        app_label = 'bims'
        verbose_name_plural = 'Download requests'
        verbose_name = 'Download request'
        ordering = ('-request_date',)

    def __str__(self):
        return '{requester} - {date} - {category}'.format(
            requester=self.requester,
            date=self.request_date.strftime('%H:%M:%S'),
            category=self.request_category
        )

    def save(self, *args, **kwargs):
        old_obj = None

        if (
            not self.requester or
            self.resource_type != DownloadRequest.CSV or
            self.resource_type != DownloadRequest.XLS
        ):
            super(DownloadRequest, self).save(*args, **kwargs)
            return

        if self.id:
            old_obj = DownloadRequest.objects.get(id=self.id)

        if old_obj and not self.processing and not self.rejected:
            if self.approved and self.approved != old_obj.approved:
                # send email
                send_csv_via_email.delay(
                    self.requester.id,
                    self.request_file.path,
                    self.request_category,
                    approved=True
                )
        elif self.rejected and not self.approved and not self.processing:
            send_rejection_csv(
                self.requester,
                self.rejection_message
            )

        super(DownloadRequest, self).save(*args, **kwargs)
