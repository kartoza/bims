from django.db import models
from django.db.models import JSONField


class IUCNAssessment(models.Model):
    taxonomy = models.ForeignKey(
        'Taxonomy',
        related_name='iucn_assessments',
        on_delete=models.CASCADE,
    )
    assessment_id = models.BigIntegerField()
    sis_taxon_id = models.IntegerField(
        null=True,
        blank=True,
    )
    year_published = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
    )
    latest = models.BooleanField(default=False)
    possibly_extinct = models.BooleanField(default=False)
    possibly_extinct_in_the_wild = models.BooleanField(default=False)
    red_list_category_code = models.CharField(
        max_length=20,
        blank=True,
        default='',
    )
    normalized_status = models.ForeignKey(
        'IUCNStatus',
        related_name='assessments',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    url = models.URLField(
        max_length=255,
        blank=True,
        default='',
    )
    scope_code = models.CharField(
        max_length=10,
        blank=True,
        default='',
    )
    scope_label = models.CharField(
        max_length=100,
        blank=True,
        default='',
    )
    raw_data = JSONField(
        null=True,
        blank=True,
    )

    def __str__(self):
        year = self.year_published or 'unknown'
        try:
            taxonomy = self.taxonomy
        except Exception:
            taxonomy = f'<taxonomy {self.taxonomy_id}>'
        return f'{taxonomy} ({year}) {self.red_list_category_code}'

    class Meta:
        app_label = 'bims'
        verbose_name = 'IUCN Assessment'
        verbose_name_plural = 'IUCN Assessments'
        ordering = ['-year_published', '-id']
        indexes = [
            models.Index(
                fields=['taxonomy', 'year_published'],
                name='bims_iucna_taxono_e8bb7c_idx',
            ),
            models.Index(
                fields=['taxonomy', 'latest'],
                name='bims_iucna_taxono_9626f4_idx',
            ),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['taxonomy', 'assessment_id'],
                name='uniq_iucn_assessment_taxonomy_assessment_id',
            ),
        ]
