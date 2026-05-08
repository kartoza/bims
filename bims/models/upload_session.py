# coding=utf-8
"""Taxa upload session model definition.

"""
import uuid
from django.conf import settings
from datetime import datetime

from django.contrib.sites.models import Site
from django.db import models
from bims.models.taxon_group import TaxonGroup


class UploadSession(models.Model):
    """Upload session model
    """
    CATEGORY_CHOICES = (
        ('taxa', 'Taxa'),
        ('taxa_validation', 'Taxa Validation'),
        ('collections', 'Collections'),
        ('water_temperature', 'Water Temperature'),
        ('physico_chemical', 'Physico Chemical')
    )

    def file_storage(self, filename):
        if self.category == 'water_temperature':
            return 'water_temperature_file/'+filename
        return 'taxa-file/'+filename

    uploader = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        models.SET_NULL,
        related_name='upload_session_uploader',
        blank=True,
        null=True,
    )

    module_group = models.ForeignKey(
        TaxonGroup,
        null=True,
        blank=True,
        on_delete=models.CASCADE
    )

    token = models.UUIDField(
        default=uuid.uuid4,
        editable=False,
        null=True
    )

    uploaded_at = models.DateTimeField(
        default=datetime.now
    )

    category = models.CharField(
        max_length=50,
        choices=CATEGORY_CHOICES,
        blank=True,
        default='',
    )

    processed = models.BooleanField(
        default=False
    )

    canceled = models.BooleanField(
        default=False
    )

    error_notes = models.TextField(
        blank=True,
        null=True
    )

    success_notes = models.TextField(
        blank=True,
        null=True
    )

    progress = models.CharField(
        max_length=512,
        default='',
        blank=True
    )

    process_file = models.FileField(
        upload_to=file_storage,
        max_length=512,
        null=True
    )

    success_file = models.FileField(
        upload_to=file_storage,
        null=True,
        max_length=512,
        blank=True
    )

    error_file = models.FileField(
        upload_to=file_storage,
        null=True,
        max_length=512,
        blank=True
    )

    updated_file = models.FileField(
        upload_to=file_storage,
        null=True,
        max_length=512,
        blank=True
    )
    source_site = models.ForeignKey(
        Site,
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )
    template = models.CharField(
        max_length=200,
        default='',
        blank=True
    )
    harvest_synonyms = models.BooleanField(
        default=False,
        help_text='When checked, synonyms for each accepted taxon will be harvested'
    )

    last_progress_update = models.DateTimeField(
        null=True,
        blank=True,
        help_text='Timestamp of the last progress update, used to detect stale/stuck tasks'
    )

    start_row = models.IntegerField(
        default=0,
        help_text='Row index to resume processing from (0 = start from beginning)'
    )

    log_file = models.FileField(
        upload_to='upload-session-log/',
        null=True,
        blank=True,
        help_text='Log file capturing per-row processing output for this upload session'
    )

    # noinspection PyClassicStyleClass
    class Meta:
        """Meta class for project."""
        app_label = 'bims'
        verbose_name_plural = 'Upload Sessions'
        verbose_name = 'Upload Session'

    def __str__(self):
        return str(self.token)
