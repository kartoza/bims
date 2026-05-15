from django.db import models


class TaxonNationalConservationAssessment(models.Model):
    """
    Stores named national conservation assessments for a taxon
    (e.g. SANBI 2016 backcast, SANBI 2026 Red List).
    """
    taxonomy = models.ForeignKey(
        'bims.Taxonomy',
        related_name='national_conservation_assessments',
        on_delete=models.CASCADE,
    )
    assessment_label = models.CharField(
        max_length=200,
        verbose_name='Assessment Label',
        help_text='E.g. "2026 SANBI Red List", "2016 SANBI backcast"',
    )
    iucn_status = models.ForeignKey(
        'bims.IUCNStatus',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        verbose_name='Conservation Status',
    )

    class Meta:
        app_label = 'bims'
        unique_together = [('taxonomy', 'assessment_label')]
        verbose_name = 'Taxon National Conservation Assessment'
        verbose_name_plural = 'Taxon National Conservation Assessments'

    def __str__(self):
        return f'{self.taxonomy} - {self.assessment_label}: {self.iucn_status}'
