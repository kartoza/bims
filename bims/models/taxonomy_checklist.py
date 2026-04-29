from django.conf import settings
from django.db import models


class TaxonomyChecklist(models.Model):
    """
    A versioned taxonomy checklist record used for ColDP / ChecklistBank exports.
    """

    title = models.CharField(
        max_length=255,
        default='BIMS Taxonomy Checklist',
        help_text='Human-readable dataset title used in ColDP metadata.yaml.',
    )

    version = models.CharField(
        max_length=50,
        help_text='Version string for this checklist release (e.g. "1.0", "2025-04").',
    )

    description = models.TextField(
        blank=True,
        default='',
        help_text='Free-text description of the dataset.',
    )

    license = models.URLField(
        max_length=255,
        default='https://creativecommons.org/licenses/by/4.0/',
        help_text=(
            'License URL or SPDX identifier for the dataset '
            '(e.g. https://creativecommons.org/licenses/by/4.0/).'
        ),
    )

    citation = models.TextField(
        blank=True,
        default='',
        help_text=(
            'Suggested citation string. '
            'Leave blank to auto-generate from organisation, title and domain.'
        ),
    )

    doi = models.CharField(
        max_length=255,
        blank=True,
        default='',
        help_text=(
            'Persistent identifier (DOI or URL) for this checklist version, '
            'e.g. https://doi.org/10.XXXX/YYYY.'
        ),
    )

    released_at = models.DateField(
        null=True,
        blank=True,
        help_text='Official release date of this checklist version (YYYY-MM-DD).',
    )

    is_published = models.BooleanField(
        default=False,
        help_text=(
            'Mark this checklist as published. '
            'The metadata endpoint serves the latest published checklist.'
        ),
    )

    contact = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='checklist_contact',
        help_text='Primary contact person for this checklist.',
    )

    creators = models.ManyToManyField(
        settings.AUTH_USER_MODEL,
        blank=True,
        related_name='checklist_creator',
        help_text='Dataset creators / authors.',
    )

    class Meta:
        verbose_name = 'Taxonomy Checklist'
        verbose_name_plural = 'Taxonomy Checklists'
        ordering = ['-released_at', '-id']

    def __str__(self):
        return f'{self.title} {self.version}'
