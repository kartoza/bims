# coding=utf-8
from django_tenants.test.cases import FastTenantTestCase
from bims.forms.bio_records_update import BioRecordsForm
from bims.tests.model_factories import (
    BiologicalCollectionRecordF,
    TaxonomyF,
)


def _base_data(record, **overrides):
    """Return minimal valid POST data for BioRecordsForm bound to *record*."""
    data = {
        'original_species_name': record.original_species_name,
        'present': record.present,
        'collection_date': record.collection_date.strftime('%Y-%m-%d'),
        'abundance_number': '',
        'taxonomy': record.taxonomy.pk,
        'ready_for_validation': False,
    }
    data.update(overrides)
    return data


class TestBioRecordsFormAbundanceValidation(FastTenantTestCase):

    def setUp(self):
        self.taxonomy = TaxonomyF.create()
        self.record = BiologicalCollectionRecordF.create(taxonomy=self.taxonomy)

    def test_valid_positive_abundance_accepted(self):
        data = _base_data(self.record, abundance_number='5.0')
        form = BioRecordsForm(data=data, instance=self.record)
        self.assertTrue(form.is_valid(), form.errors)
        self.assertEqual(form.cleaned_data['abundance_number'], 5.0)

    def test_zero_abundance_rejected(self):
        data = _base_data(self.record, abundance_number='0')
        form = BioRecordsForm(data=data, instance=self.record)
        self.assertFalse(form.is_valid())
        self.assertIn('abundance_number', form.errors)

    def test_zero_float_abundance_rejected(self):
        data = _base_data(self.record, abundance_number='0.0')
        form = BioRecordsForm(data=data, instance=self.record)
        self.assertFalse(form.is_valid())
        self.assertIn('abundance_number', form.errors)

    def test_negative_abundance_rejected(self):
        data = _base_data(self.record, abundance_number='-3')
        form = BioRecordsForm(data=data, instance=self.record)
        self.assertFalse(form.is_valid())
        self.assertIn('abundance_number', form.errors)

    def test_blank_abundance_accepted(self):
        """Abundance is optional; blank should be valid."""
        data = _base_data(self.record, abundance_number='')
        form = BioRecordsForm(data=data, instance=self.record)
        self.assertTrue(form.is_valid(), form.errors)
        self.assertIsNone(form.cleaned_data['abundance_number'])
