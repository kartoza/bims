# coding=utf-8
import os
import uuid
from django.db import models
from django.conf import settings

def upload_path(instance, filename):
    base, ext = os.path.splitext(filename)
    return f"uploads/{instance.created_at:%Y/%m/%d}/{uuid.uuid4().hex}{ext.lower()}"


class UploadType(models.Model):
    """Upload type model for categorizing upload requests"""
    name = models.CharField(
        max_length=255,
        blank=False,
        null=False,
        help_text='Display name for the upload type'
    )
    code = models.CharField(
        max_length=50,
        unique=True,
        blank=False,
        null=False,
        help_text='Unique identifier code (e.g., occurrence, spatial, species-checklist)'
    )
    description = models.TextField(
        help_text='Optional description',
        null=True,
        blank=True
    )
    order = models.IntegerField(
        default=0,
        blank=False,
        null=False,
        help_text='Display order in forms'
    )

    def __str__(self):
        return self.name

    class Meta:
        ordering = ('order',)
        verbose_name = 'Upload Type'
        verbose_name_plural = 'Upload Types'


class UploadRequest(models.Model):
    STATUS_SUBMITTED = 'submitted'
    STATUS_TICKETED = 'ticketed'
    STATUS_ERROR = 'error'
    STATUS_CHOICES = (
        (STATUS_SUBMITTED, 'Submitted'),
        (STATUS_TICKETED, 'Ticket Created'),
        (STATUS_ERROR, 'Error'),
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, blank=True,
        on_delete=models.SET_NULL, related_name='uploads'
    )

    title = models.CharField(max_length=255, blank=True, default='')
    name = models.CharField(max_length=255)
    email = models.EmailField()
    upload_type = models.ForeignKey(
        UploadType,
        on_delete=models.PROTECT,
        related_name='upload_requests',
        help_text='Type of upload request'
    )
    upload_file = models.FileField(upload_to=upload_path)
    notes = models.TextField(blank=True)
    licence = models.ForeignKey(
        'bims.Licence',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )

    source = models.CharField(max_length=255, blank=True)
    client_ip = models.GenericIPAddressField(null=True, blank=True)

    gbif_consent = models.BooleanField(
        default=False,
        help_text='User consented to publishing these data to GBIF',
    )

    github_issue_number = models.IntegerField(null=True, blank=True)
    github_issue_url = models.URLField(blank=True)
    status = models.CharField(
        max_length=20, choices=STATUS_CHOICES, default=STATUS_SUBMITTED)

    class Meta:
        ordering = ('-created_at',)

    def __str__(self):
        return f"{self.upload_type.name} by {self.name} on {self.created_at:%Y-%m-%d}"

    @property
    def file_name(self):
        return os.path.basename(self.upload_file.name)
