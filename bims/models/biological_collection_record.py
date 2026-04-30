# coding=utf-8
"""Biological collection record model definition.

"""
import json
import uuid
from django.conf import settings
from django.db import models
from django.dispatch import receiver
from django.utils import timezone
from django.db.models import JSONField
from django.contrib.contenttypes.models import ContentType

from preferences import preferences
from bims.models.location_site import LocationSite
from bims.utils.gbif import update_collection_record
from bims.models.validation import AbstractValidation
from bims.models.taxonomy import Taxonomy
from bims.models.taxon_group import TaxonGroup
from bims.models.source_reference import SourceReference
from bims.enums.taxonomic_group_category import TaxonomicGroupCategory
from bims.models.bims_document import BimsDocument
from bims.models.survey import Survey
from bims.enums.ecosystem_type import (
    ECOSYSTEM_TYPE_CHOICES, HYDROPERIOD_CHOICES
)
from bims.models.record_type import RecordType
from td_biblio.models import Entry


class BiologicalCollectionQuerySet(models.QuerySet):
    def source_references(self):
        source_references = []
        unique_source_references = []
        is_doc = False
        for col in self:
            try:
                col.source_reference
            except ContentType.DoesNotExist:
                continue

            try:
                title = col.source_reference.title
                authors = col.source_reference.authors
                pub_year = col.source_reference.year
            except AttributeError:
                title = '-'
                authors = '-'
                pub_year = '-'

            if not col.source_reference:
                continue

            if col.source_reference.is_published_report():
                if col.source_reference.source.doc_file:
                    url = col.source_reference.source.doc_file.url
                    is_doc = True
                else:
                    url = col.source_reference.source.doc_url
                bims_document, _ = BimsDocument.objects.get_or_create(
                    document=col.source_reference.source
                )
                try:
                    source = json.loads(
                        col.source_reference.source.supplemental_information
                    )['document_source']
                except:  # noqa
                    source = '-'
            else:
                try:
                    url = col.source_reference.source.doi
                    if not url:
                        url = col.source_reference.source.url
                except AttributeError:
                    if hasattr(col.source_reference, 'document'):
                        if col.source_reference.document:
                            if col.source_reference.document.doc_file:
                                url = (
                                    col.source_reference.document.doc_file.url
                                )
                                is_doc = True
                            else:
                                url = col.source_reference.document.doc_url
                        else:
                            url = '-'
                    else:
                        url = '-'

                try:
                    if col.source_reference.source_name:
                        source = col.source_reference.source_name
                    else:
                        source = (
                            col.source_reference.source.journal.name
                        )
                except AttributeError:
                    if isinstance(col.source_reference.source, Entry):
                        source = '-'
                    else:
                        source = col.source_reference.__str__()

            note = (
                col.source_reference.note if col.source_reference.note else '-'
            )

            if note is None:
                note = '-'

            item = {
                'ID': col.source_reference.id,
                'Reference Category': col.source_reference.reference_type,
                'Author/s': authors,
                'Year': pub_year,
                'Title': title,
                'Source': source,
                'DOI/URL': url,
                'is_doc': is_doc,
                'Notes': note
            }
            if json.dumps(item) not in unique_source_references:
                source_references.append(item)
                unique_source_references.append(json.dumps(item))
        return sorted(
            source_references, key = lambda i: (
                i['Reference Category'], i['Author/s']))


class BiologicalCollectionManager(models.Manager):
    def get_queryset(self):
        return BiologicalCollectionQuerySet(self.model, using=self._db)

    def source_references(self):
        return self.get_queryset().source_references()


ABUNDANCE_TYPE_NUMBER = 'number'
ABUNDANCE_TYPE_PERCENTAGE = 'percentage'
ABUNDANCE_TYPE_DENSITY = 'density'
ABUNDANCE_TYPE_SPECIES_VALVE = 'species_valve_per_frustule_count'
ABUNDANCE_TYPE_DENSITY_CELLS_M2 = 'density_cells_per_m2'
ABUNDANCE_TYPE_DENSITY_CELLS_ML = 'density_cells_per_mL'


class BiologicalCollectionRecord(AbstractValidation):
    """Biological collection model."""
    CATEGORY_CHOICES = (
        ('alien', 'Non-Native'),
        ('indigenous', 'Native'),
    )

    HABITAT_CHOICES = (
        ('euryhaline', 'Euryhaline'),
        ('freshwater', 'Freshwater'),
    )

    ABUNDANCE_TYPE_CHOICES = (
        (ABUNDANCE_TYPE_NUMBER, 'Number'),
        (ABUNDANCE_TYPE_PERCENTAGE, 'Percentage'),
        (ABUNDANCE_TYPE_DENSITY, 'Density'),
        (ABUNDANCE_TYPE_SPECIES_VALVE, 'Species valve/frustule count'),
        (ABUNDANCE_TYPE_DENSITY_CELLS_M2, 'Density (cells/m2)'),
        (ABUNDANCE_TYPE_DENSITY_CELLS_ML, 'Density (cells/mL)'),
    )

    DATA_TYPE_CHOICES = (
        ('private', 'Private'),
        ('public', 'Public'),
        ('sensitive', 'Sensitive'),
    )

    DATE_ACCURACY_CHOICES = (
        ('accurate', 'Accurate'),
        ('artificial', 'Artificial'),
    )

    site = models.ForeignKey(
        LocationSite,
        models.CASCADE,
        related_name='biological_collection_record',
    )
    original_species_name = models.CharField(
        max_length=200,
        blank=True,
        default='',
    )
    present = models.BooleanField(
        default=True,
    )
    collection_date = models.DateField(
        default=timezone.now
    )
    collector = models.CharField(
        max_length=1024,
        blank=True,
        default='',
        help_text='Collector name in string value, this is useful for '
                  'collector values from GBIF and other third party sources',
        verbose_name='collector or observer',
    )

    notes = models.TextField(
        blank=True,
        default='',
    )

    taxonomy = models.ForeignKey(
        Taxonomy,
        models.SET_NULL,
        null=True,
        blank=True,
        verbose_name='Taxonomy'
    )

    collection_habitat = models.CharField(
        max_length=200,
        choices=HABITAT_CHOICES,
        blank=True,
        default=''
    )

    institution_id = models.CharField(
        default=settings.INSTITUTION_ID_DEFAULT,
        help_text='An identifier for the institution having custody of the '
                  'object(s) or information referred to in the record.',
        max_length=200,
        verbose_name='Custodian',
    )

    sampling_method = models.ForeignKey(
        'bims.SamplingMethod',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )

    sampling_effort = models.CharField(
        max_length=50,
        blank=True,
        default=''
    )

    sampling_effort_link = models.ForeignKey(
        'bims.SamplingEffortMeasure',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )

    reference = models.CharField(
        max_length=2048,
        blank=True,
        default=''
    )

    reference_category = models.CharField(
        max_length=200,
        blank=True,
        default=''
    )

    source_collection = models.CharField(
        help_text='e.g. SANBI',
        max_length=200,
        blank=True,
        default=''
    )

    upstream_id = models.CharField(
        help_text='Upstream id, e.g. Gbif key',
        max_length=200,
        blank=True,
        default='',
        db_index=True
    )

    uuid = models.UUIDField(
        help_text='Collection record uuid',
        default=uuid.uuid4,
        unique=True,
        null=True,
        blank=True
    )

    dataset_key = models.CharField(
        help_text='Dataset key',
        default='',
        blank=True,
        null=True,
    )

    additional_data = JSONField(
        blank=True,
        null=True
    )

    abundance_number = models.FloatField(
        blank=True,
        null=True
    )

    abundance_type = models.ForeignKey(
        'bims.AbundanceType',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )

    hydroperiod = models.ForeignKey(
        'bims.Hydroperiod',
        blank=True,
        null=True,
        on_delete=models.SET_NULL
    )

    wetland_indicator_status = models.ForeignKey(
        'bims.WetlandIndicatorStatus',
        blank=True,
        null=True,
        on_delete=models.SET_NULL
    )

    biotope = models.ForeignKey(
        'bims.Biotope',
        null=True,
        blank=True,
        on_delete=models.CASCADE
    )

    specific_biotope = models.ForeignKey(
        'bims.Biotope',
        null=True,
        blank=True,
        related_name='specific_biotope',
        on_delete=models.CASCADE
    )

    substratum = models.ForeignKey(
        'bims.Biotope',
        null=True,
        blank=True,
        related_name='biotope_substratum',
        on_delete=models.CASCADE
    )

    source_reference = models.ForeignKey(
        SourceReference,
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )

    module_group = models.ForeignKey(
        'bims.TaxonGroup',
        help_text='Which module this collection belong to',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )

    objects = BiologicalCollectionManager()

    survey = models.ForeignKey(
        'bims.Survey',
        related_name='biological_collection_record',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )

    record_type = models.ForeignKey(
        'bims.RecordType',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )

    ecosystem_type = models.CharField(
        max_length=128,
        blank=True,
        choices=ECOSYSTEM_TYPE_CHOICES,
        default='',
        null=True
    )

    data_type = models.CharField(
        max_length=128,
        blank=True,
        choices=DATA_TYPE_CHOICES,
        default='',
        help_text='Specify data sharing level of this record',
        db_index=True
    )

    date_accuracy = models.CharField(
        max_length=64,
        blank=True,
        choices=DATE_ACCURACY_CHOICES,
        default='',
        help_text='Indicate if date is accurate or artificial'
    )

    identified_by = models.CharField(
        max_length=512,
        blank=True,
        default='',
        help_text='Name of the person who identified the species',
        verbose_name='Identifier',
    )

    coordinate_precision = models.DecimalField(
        max_digits=10,
        decimal_places=6,
        blank=True,
        null=True,
        help_text='A decimal representation of the precision of the coordinates. '
                  'Examples: 0.00001 (GPS limit), 0.000278 (nearest second), '
                  '0.01667 (nearest minute), 1.0 (nearest degree)'
    )

    coordinate_uncertainty_in_meters = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        blank=True,
        null=True,
        help_text='The horizontal distance (in meters) from the given coordinates '
                  'describing the smallest circle containing the whole of the Location. '
                  'Examples: 30 (GPS after 2000-05-01), 100 (GPS before 2000-05-01)'
    )

    licence = models.ForeignKey(
        'bims.Licence',
        null=True,
        blank=True,
        on_delete=models.SET_NULL)


    @property
    def data_name(self):
        return self.original_species_name

    # noinspection PyClassicStyleClass
    class Meta:
        """Meta class for project."""
        app_label = 'bims'
        permissions = (
            ('can_upload_csv', 'Can upload CSV'),
            ('can_upload_shapefile', 'Can upload Shapefile'),
            ('can_validate_data', 'Can validate data'),
            ('can_add_single_occurrence', 'Can add single Occurrence'),
        )
        indexes = [
            models.Index(fields=['source_collection', 'taxonomy']),
            models.Index(fields=['owner', 'end_embargo_date']),
            models.Index(fields=['data_type'])
        ]

    def on_post_save(self):
        if not self.taxonomy:
            update_collection_record(self)
        if not self.source_collection:
            if preferences.SiteSetting.default_data_source:
                self.source_collection = (
                    preferences.SiteSetting.default_data_source
                )
                self.save()
        if self.ecosystem_type != self.site.ecosystem_type:
            self.ecosystem_type = self.site.ecosystem_type
            self.save()
        if self.taxonomy and not self.module_group:
            # Get taxon group if exists
            taxonomies = [self.taxonomy.id]
            taxon_groups = TaxonGroup.objects.filter(
                taxonomies__in=taxonomies,
                category=TaxonomicGroupCategory.SPECIES_MODULE.name,
            )
            if taxon_groups.exists():
                self.module_group = taxon_groups[0]
                self.save()
        record_types = [
            'visual observation',
            'photographic record'
        ]
        if (
                self.sampling_method and
                not self.record_type and
                self.sampling_method.sampling_method.lower() in record_types
        ):
            record_type, created = RecordType.objects.get_or_create(
                name=self.sampling_method.sampling_method
            )
            self.record_type = record_type
            self.save()

    def save(self, *args, **kwargs):
        if self.abundance_number is not None and self.abundance_number <= 0:
            raise ValueError(
                f"abundance_number must be greater than zero, got {self.abundance_number!r}"
            )

        max_allowed = 10
        attempt = 0
        is_dictionary = False

        while not is_dictionary and attempt < max_allowed:
            if not self.additional_data:
                break
            if isinstance(self.additional_data, dict):
                is_dictionary = True
            else:
                self.additional_data = json.loads(self.additional_data)
                attempt += 1

        if not self.original_species_name:
            self.original_species_name = self.taxonomy.canonical_name

        if not self.survey and self.collection_date:
            try:
                survey, _ = Survey.objects.get_or_create(
                    site=self.site,
                    date=self.collection_date,
                    collector_user=self.collector_user,
                    owner=self.owner
                )
            except Survey.MultipleObjectsReturned:
                survey = Survey.objects.filter(
                    site=self.site,
                    date=self.collection_date,
                    collector_user=self.collector_user,
                    owner=self.owner
                )[0]
            self.survey = survey
            if (
                'gbif' in self.source_collection.lower()
                and not self.survey.validated
            ):
                self.survey.validated = True
                self.survey.save()

        if not self.survey.owner and self.owner:
            self.survey.owner = self.owner
            self.survey.save()

        if self.end_embargo_date:
            location_site = self.site
            other_surveys = Survey.objects.filter(
                site=self.site
            ).exclude(id=self.survey.id)
            if not other_surveys.exists() and not location_site.end_embargo_date:
                location_site.end_embargo_date = self.end_embargo_date
                location_site.save()

        super(BiologicalCollectionRecord, self).save(*args, **kwargs)

    def get_children(self):
        rel_objs = [f for f in self._meta.get_fields(include_parents=False)
                    if (f.one_to_many or f.one_to_one) and
                    f.auto_created and not f.concrete]

        for rel_obj in rel_objs:
            try:
                return getattr(self, rel_obj.get_accessor_name())
            except AttributeError:
                continue

    @staticmethod
    def get_children_model():
        rel_objs = [f for f in BiologicalCollectionRecord._meta.get_fields(
            include_parents=False)
                    if f.one_to_one and
                    f.auto_created and not f.concrete]
        related_models = []
        for rel_obj in rel_objs:
            related_models.append(rel_obj.related_model)
        return related_models

    def __init__(self, *args, **kwargs):
        super(BiologicalCollectionRecord, self).__init__(*args, **kwargs)
        self.__original_validated = self.validated

    def is_cluster_generation_applied(self):
        if self.__original_validated != self.validated:
            return True
        if self.validated:
            return True
        return False

    def __unicode__(self):
        label = '{species} - {date}'.format(
            species=self.original_species_name,
            date=self.collection_date
        )
        return label

    def __str__(self):
        label = '{species} - {date}'.format(
            species=self.original_species_name,
            date=self.collection_date
        )
        return label


@receiver(models.signals.post_save)
def collection_post_save_handler(sender, instance, created, **kwargs):
    """
    Fetch taxon from original species name.
    """

    if not issubclass(sender, BiologicalCollectionRecord):
        return
    models.signals.post_save.disconnect(
        collection_post_save_handler,
    )
    instance.on_post_save()

    if created:
        from bims.api_views.send_notification_to_validator import (
            send_notification_validation
        )
        survey = instance.survey
        if survey and not instance.validated:
            if not survey.validated and not survey.ready_for_validation:
                survey.ready_for_validation = True
                if 'gbif' not in instance.source_collection.lower():
                    send_notification_validation(survey.pk)
                survey.save()

    models.signals.post_save.connect(
        collection_post_save_handler,
    )


@receiver(models.signals.post_save)
def collection_post_save_update_cluster(sender, instance, **kwargs):
    if not issubclass(sender, BiologicalCollectionRecord):
        return

