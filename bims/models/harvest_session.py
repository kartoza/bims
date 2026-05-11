# coding=utf-8
"""Harvest session model definition.

"""
from django.conf import settings
from datetime import datetime

from django.contrib.sites.models import Site
from django.db import models
from bims.models.taxon_group import TaxonGroup


class HarvestTrigger(models.TextChoices):
    MANUAL = "manual", "Manual"
    SCHEDULED = "scheduled", "Scheduled"


class HarvestSession(models.Model):
    """Harvest session model
    """
    CATEGORY_CHOICES = (
        ('gbif', 'GBIF'),
        ('worms', 'WoRMS'),
    )

    harvester = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        models.SET_NULL,
        related_name='harvest_session_harvester',
        blank=True,
        null=True,
    )

    module_group = models.ForeignKey(
        TaxonGroup,
        null=True,
        blank=True,
        on_delete=models.CASCADE
    )

    parent_species = models.ForeignKey(
        'bims.Taxonomy',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='harvest_sessions',
        help_text='GBIF parent species used for this harvest session'
    )

    start_time = models.DateTimeField(
        default=datetime.now
    )

    category = models.CharField(
        max_length=50,
        choices=CATEGORY_CHOICES,
        blank=True,
        default='',
    )

    finished = models.BooleanField(
        default=False
    )

    canceled = models.BooleanField(
        default=False
    )

    log_file = models.FileField(
        upload_to='harvest-session-log/',
        null=True
    )

    is_fetching_species = models.BooleanField(
        default=False,
        help_text='Flag indicating whether it is only fetching species'
    )

    harvest_synonyms = models.BooleanField(
        default=False,
        help_text='When checked, synonyms for each accepted taxon will be harvested'
    )

    boundary = models.ForeignKey(
        'bims.Boundary',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )

    status = models.TextField(
        null=True,
        blank=True
    )

    source_site = models.ForeignKey(
        Site,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        verbose_name="Associated Site",
        help_text="The site this record is associated with."
    )

    schedule = models.ForeignKey(
        "bims.HarvestSchedule", null=True, blank=True, on_delete=models.SET_NULL, related_name="sessions"
    )
    trigger = models.CharField(
        max_length=10, choices=HarvestTrigger.choices,
        default=HarvestTrigger.MANUAL)
    since_time = models.DateTimeField(null=True, blank=True)
    until_time = models.DateTimeField(null=True, blank=True)

    additional_data = models.JSONField(
        null=True,
        blank=True,
        help_text='JSON field for storing resume state and other metadata'
    )

    def __str__(self):
        return '{start} - {finished}'.format(
            start=self.start_time,
            finished=self.finished
        )
