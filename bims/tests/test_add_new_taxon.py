from django.urls import reverse
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient
from unittest.mock import patch

from bims.tests.model_factories import TaxonomyF, TaxonGroupF, UserF, IUCNStatusF
from bims.models import Taxonomy, TaxonGroupTaxonomy


def mock_fetch_all_species_from_gbif(col_id=None, fetch_vernacular_names=True, is_synonym=False, **kwargs):
    iucn_status = IUCNStatusF.create()
    taxonomy = TaxonomyF.create(
        col_id=col_id,
        scientific_name="Mocked Scientific Name",
        canonical_name="Mocked Canonical Name",
        iucn_status=iucn_status
    )
    return taxonomy


class AddNewTaxonTestCase(FastTenantTestCase):
    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.user = UserF.create(username='testuser', password='testpassword')
        self.client.login(username='testuser', password='testpassword')
        self.taxon_group = TaxonGroupF.create(
            name='test'
        )
        self.taxonomy = TaxonomyF()

    @patch('bims.api_views.taxon.fetch_all_species_from_gbif', side_effect=mock_fetch_all_species_from_gbif)
    def test_add_new_taxon_with_col_id(self, mock_fetch):
        data = {
            'colId': 'ABC123',
            'taxonName': 'Test Taxon',
            'taxonGroup': self.taxon_group.name,
            'rank': 'species',
            'authorName': 'Test Author',
        }
        response = self.client.post(reverse('add-new-taxon'), data)
        self.assertEqual(response.status_code, 200)
        self.assertTrue('id' in response.data)
        self.assertTrue('taxon_name' in response.data)
        self.assertEqual(response.data['taxon_name'], 'Mocked Canonical Name')
        from bims.models import Taxonomy
        taxonomy = Taxonomy.objects.get(id=response.data['id'])
        self.assertEqual(taxonomy.last_modified_by, self.user)
        self.assertEqual(taxonomy.col_id, 'ABC123')

    @patch('bims.api_views.taxon.fetch_all_species_from_gbif', side_effect=mock_fetch_all_species_from_gbif)
    @patch('bims.api_views.taxon.resolve_col_id')
    def test_add_new_taxon_with_gbif_key_resolves_to_col_id(self, mock_resolve_col_id, mock_fetch):
        """
        The find-taxon search table only surfaces legacy GBIF suggest keys,
        so picking a result posts gbifKey; the backend must resolve it to a
        col_id (via resolve_col_id) before creating the taxon.
        """
        mock_resolve_col_id.return_value = ('RESOLVED123', {})

        data = {
            'gbifKey': '99999',
            'taxonName': 'Test Taxon',
            'taxonGroup': self.taxon_group.name,
            'rank': 'species',
            'authorName': 'Test Author',
        }
        response = self.client.post(reverse('add-new-taxon'), data)
        self.assertEqual(response.status_code, 200)

        mock_resolve_col_id.assert_called_once_with('99999', canonical_name='Test Taxon')
        mock_fetch.assert_called_once()
        self.assertEqual(mock_fetch.call_args.kwargs.get('col_id'), 'RESOLVED123')

        taxonomy = Taxonomy.objects.get(id=response.data['id'])
        self.assertEqual(taxonomy.col_id, 'RESOLVED123')

    @patch('bims.api_views.taxon.fetch_all_species_from_gbif', side_effect=mock_fetch_all_species_from_gbif)
    @patch('bims.api_views.taxon.resolve_col_id')
    def test_add_new_taxon_with_gbif_key_unresolvable_returns_error(self, mock_resolve_col_id, mock_fetch):
        """If the GBIF key can't be resolved to a col_id, reject with a 400 instead of creating a taxon."""
        mock_resolve_col_id.return_value = (None, None)

        data = {
            'gbifKey': '99999',
            'taxonName': 'Test Taxon',
            'taxonGroup': self.taxon_group.name,
            'rank': 'species',
            'authorName': 'Test Author',
        }
        response = self.client.post(reverse('add-new-taxon'), data)
        self.assertEqual(response.status_code, 400)
        self.assertIn('error', response.data)
        mock_fetch.assert_not_called()

    def test_add_new_taxon_without_col_id(self):
        data = {
            'taxonName': 'Test Taxon Without COL',
            'taxonGroup': self.taxon_group.name,
            'rank': 'species',
            'parentId': self.taxonomy.id,
            'authorName': 'Test Author Without COL',
        }
        response = self.client.post(reverse('add-new-taxon'), data)
        self.assertEqual(response.status_code, 200)
        self.assertTrue('id' in response.data)
        self.assertTrue('taxon_name' in response.data)
        self.assertEqual(response.data['taxon_name'], 'Test Taxon Without COL')
        from bims.models import Taxonomy
        taxonomy = Taxonomy.objects.get(id=response.data['id'])
        self.assertEqual(taxonomy.last_modified_by, self.user)

        data = {
            'taxonName': 'Test Taxon Without COL 2',
            'taxonGroupId': self.taxon_group.id,
            'rank': 'species',
            'familyId': self.taxonomy.id,
            'authorName': 'Test Author Without COL 2',
        }
        response = self.client.post(reverse('add-new-taxon'), data)
        self.assertEqual(response.status_code, 200)
        self.assertTrue('id' in response.data)
        self.assertTrue('taxon_name' in response.data)
        self.assertEqual(response.data['taxon_name'], 'Test Taxon Without COL 2')

    def test_reject_duplicate_taxon_same_name_same_parent(self):
        """
        When a manual taxon (no colId) with the same canonical_name and parent
        already exists, the API asks for confirmation (409) and does not create
        a duplicate until the user confirms.
        """
        parent = TaxonomyF.create(
            canonical_name="Parent Genus",
            scientific_name="Parent Genus",
            rank="genus",
        )
        TaxonomyF.create(
            canonical_name="Parent Genus Duplicate",
            scientific_name="Parent Genus Duplicate",
            rank="species",
            parent=parent,
        )

        data = {
            "taxonName": "Parent Genus Duplicate",
            "taxonGroup": self.taxon_group.name,
            "rank": "species",
            "parentId": parent.id,
            "authorName": "Different Author",
        }
        response = self.client.post(reverse("add-new-taxon"), data)
        self.assertEqual(response.status_code, 409)
        self.assertTrue(response.data["duplicate"])
        self.assertIn("already exists", response.data["error"])

        # No extra row created
        self.assertEqual(
            Taxonomy.objects.filter(
                canonical_name__iexact="Parent Genus Duplicate",
                parent=parent,
            ).count(),
            1,
        )

    def test_reject_duplicate_taxon_same_name_no_parent(self):
        """
        When the same name is submitted twice with no parent, ask to confirm.
        """
        TaxonomyF.create(
            canonical_name="Orphan Duplicate",
            scientific_name="Orphan Duplicate",
            rank="family",
            parent=None,
        )

        data = {
            "taxonName": "Orphan Duplicate",
            "taxonGroup": self.taxon_group.name,
            "rank": "family",
            "authorName": "Some Author",
        }
        response = self.client.post(reverse("add-new-taxon"), data)
        self.assertEqual(response.status_code, 409)
        self.assertIn("error", response.data)

    def _homonym_setup(self, author=None):
        parent = TaxonomyF.create(
            canonical_name="Homonym Genus",
            scientific_name="Homonym Genus",
            rank="genus",
        )
        existing = TaxonomyF.create(
            canonical_name="Homonym Genus danica",
            scientific_name="Homonym Genus danica",
            rank="species",
            parent=parent,
            author=author or "",
        )
        data = {
            "taxonName": "Homonym Genus danica",
            "taxonGroup": self.taxon_group.name,
            "rank": "species",
            "parentId": parent.id,
        }
        return parent, existing, data

    def test_duplicate_without_author_warns_then_allows_on_confirm(self):
        parent, existing, data = self._homonym_setup()
        response = self.client.post(reverse("add-new-taxon"), data)
        self.assertEqual(response.status_code, 409)

        data["confirmDuplicate"] = "true"
        data["authorName"] = "Linnaeus, 1758"
        response = self.client.post(reverse("add-new-taxon"), data)
        self.assertEqual(response.status_code, 200)
        self.assertNotEqual(response.data["id"], existing.id)
        existing.refresh_from_db()
        self.assertFalse(existing.author)
        self.assertEqual(
            Taxonomy.objects.filter(
                canonical_name__iexact="Homonym Genus danica", parent=parent
            ).count(),
            2,
        )

    def test_duplicate_with_same_author_is_always_rejected(self):
        _, _, data = self._homonym_setup(author="Linnaeus, 1758")
        data["authorName"] = "linnaeus, 1758"
        data["confirmDuplicate"] = "true"
        response = self.client.post(reverse("add-new-taxon"), data)
        self.assertEqual(response.status_code, 400)
        self.assertIn("same author", response.data["error"])

    def test_allow_same_name_different_parent(self):
        """
        Two taxa with the same canonical name but different parents should both be
        accepted (they are distinct taxa under different genera).
        """
        parent_a = TaxonomyF.create(
            canonical_name="Genus Alpha",
            scientific_name="Genus Alpha",
            rank="genus",
        )
        parent_b = TaxonomyF.create(
            canonical_name="Genus Beta",
            scientific_name="Genus Beta",
            rank="genus",
        )
        TaxonomyF.create(
            canonical_name="Shared Species",
            scientific_name="Shared Species",
            rank="species",
            parent=parent_a,
        )

        data = {
            "taxonName": "Shared Species",
            "taxonGroup": self.taxon_group.name,
            "rank": "species",
            "parentId": parent_b.id,
        }
        response = self.client.post(reverse("add-new-taxon"), data)
        self.assertEqual(response.status_code, 200)
        self.assertIn("id", response.data)

    def test_add_synonym_automatically_adds_accepted_name_to_group(self):
        """
        When a new synonym is manually added to a taxon group,
        the accepted taxonomy should automatically be added to the same group.
        """
        # Create an accepted taxonomy (not yet in the group)
        accepted_taxonomy = TaxonomyF.create(
            canonical_name="Accepted Species",
            scientific_name="Accepted Species",
            rank="species",
            taxonomic_status="ACCEPTED"
        )

        # Verify the accepted taxonomy is NOT in the taxon group yet
        self.assertFalse(
            TaxonGroupTaxonomy.objects.filter(
                taxonomy=accepted_taxonomy,
                taxongroup=self.taxon_group
            ).exists()
        )

        # Add a brand-new synonym that doesn't exist yet
        data = {
            'taxonName': 'New Synonym Species',
            'taxonGroup': self.taxon_group.name,
            'rank': 'species',
            'taxonomicStatus': 'SYNONYM',
            'acceptedTaxonomyId': accepted_taxonomy.id,
        }
        response = self.client.post(reverse('add-new-taxon'), data)
        self.assertEqual(response.status_code, 200)

        # Verify the newly created synonym was added to the group
        created_synonym = Taxonomy.objects.get(id=response.data['id'])
        self.assertTrue(
            TaxonGroupTaxonomy.objects.filter(
                taxonomy=created_synonym,
                taxongroup=self.taxon_group
            ).exists()
        )

        # Verify the accepted taxonomy was AUTOMATICALLY added to the group
        self.assertTrue(
            TaxonGroupTaxonomy.objects.filter(
                taxonomy=accepted_taxonomy,
                taxongroup=self.taxon_group
            ).exists(),
            "Accepted taxonomy should be automatically added when synonym is added"
        )

        # Verify it's marked as validated (auto_validate = True)
        accepted_in_group = TaxonGroupTaxonomy.objects.get(
            taxonomy=accepted_taxonomy,
            taxongroup=self.taxon_group
        )
        self.assertTrue(accepted_in_group.is_validated)

    def test_add_synonym_when_accepted_already_in_group(self):
        """
        When a new synonym is added and the accepted taxonomy is already in the group,
        ensure_accepted_taxonomy_in_group should not create a duplicate entry.
        """
        # Create an accepted taxonomy and pre-add it to the group
        accepted_taxonomy = TaxonomyF.create(
            canonical_name="Already Present Accepted",
            scientific_name="Already Present Accepted",
            rank="species",
            taxonomic_status="ACCEPTED"
        )
        self.taxon_group.taxonomies.add(
            accepted_taxonomy,
            through_defaults={'is_validated': True}
        )

        # Add a brand-new synonym (doesn't exist yet) referencing the accepted taxonomy
        data = {
            'taxonName': 'New Synonym Of Present',
            'taxonGroup': self.taxon_group.name,
            'rank': 'species',
            'taxonomicStatus': 'SYNONYM',
            'acceptedTaxonomyId': accepted_taxonomy.id,
        }
        response = self.client.post(reverse('add-new-taxon'), data)
        self.assertEqual(response.status_code, 200)

        # Verify there's still only ONE entry for the accepted taxonomy
        self.assertEqual(
            TaxonGroupTaxonomy.objects.filter(
                taxonomy=accepted_taxonomy,
                taxongroup=self.taxon_group
            ).count(),
            1,
            "Should not create duplicate entry for accepted taxonomy"
        )

    def test_add_non_synonym_does_not_add_extra_taxa(self):
        """
        When a brand-new non-synonym (accepted name) is added,
        it should not trigger any additional taxa being added to the group.
        """
        parent = TaxonomyF.create(
            canonical_name="Parent Genus For Accepted",
            scientific_name="Parent Genus For Accepted",
            rank="genus",
        )

        # Count existing taxa in group before
        initial_count = TaxonGroupTaxonomy.objects.filter(
            taxongroup=self.taxon_group
        ).count()

        # Add a brand-new accepted taxon (does not exist yet)
        data = {
            'taxonName': 'Brand New Accepted Species',
            'taxonGroup': self.taxon_group.name,
            'rank': 'species',
            'taxonomicStatus': 'ACCEPTED',
            'parentId': parent.id,
        }
        response = self.client.post(reverse('add-new-taxon'), data)
        self.assertEqual(response.status_code, 200)

        # Verify only ONE taxon was added (the one we requested)
        final_count = TaxonGroupTaxonomy.objects.filter(
            taxongroup=self.taxon_group
        ).count()
        self.assertEqual(
            final_count,
            initial_count + 1,
            "Should only add the requested taxon, not any additional taxa"
        )
