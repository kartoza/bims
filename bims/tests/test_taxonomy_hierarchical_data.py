from io import StringIO

from django.core.management import call_command
from django_tenants.test.cases import FastTenantTestCase

from bims.models import Taxonomy
from bims.tests.model_factories import TaxonomyF


class TestHierarchicalDataCache(FastTenantTestCase):

    def setUp(self):
        self.family = TaxonomyF.create(
            canonical_name='Leptophlebiidae', rank='FAMILY')
        self.genus = TaxonomyF.create(
            canonical_name='Thraulus', rank='GENUS', parent=self.family)
        self.subgenus = TaxonomyF.create(
            canonical_name='Masharikella', rank='SUBGENUS', parent=self.genus)

    def _orphan_species(self):
        # Created before its parent chain is linked, as harvesters do
        species = TaxonomyF.create(
            canonical_name='Thraulus iteris', rank='SPECIES')
        self.assertEqual(species.hierarchical_data['family_name'], '')
        return species

    def test_parent_only_save_persists_family_name(self):
        species = self._orphan_species()
        species.parent = self.subgenus
        species.save(update_fields=['parent'])

        species = Taxonomy.objects.get(pk=species.pk)
        self.assertEqual(
            species.hierarchical_data['family_name'], 'Leptophlebiidae')
        self.assertTrue(Taxonomy.objects.filter(
            pk=species.pk,
            hierarchical_data__family_name__iexact='leptophlebiidae',
        ).exists())

    def test_refresh_command_fills_empty_family_name(self):
        species = self._orphan_species()
        Taxonomy.objects.filter(pk=species.pk).update(parent=self.subgenus)

        call_command('refresh_hierarchical_data', stdout=StringIO())

        species = Taxonomy.objects.get(pk=species.pk)
        self.assertEqual(
            species.hierarchical_data['family_name'], 'Leptophlebiidae')
        self.assertEqual(species.hierarchical_data['genus_name'], 'Thraulus')

    def test_refresh_command_dry_run_changes_nothing(self):
        species = self._orphan_species()
        Taxonomy.objects.filter(pk=species.pk).update(parent=self.subgenus)

        call_command(
            'refresh_hierarchical_data', dry_run=True, stdout=StringIO())

        species = Taxonomy.objects.get(pk=species.pk)
        self.assertEqual(species.hierarchical_data['family_name'], '')
