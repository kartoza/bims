from django.urls import reverse
from django.test import override_settings
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient
from django.contrib.auth import get_user_model
from preferences import preferences

from bims.models.taxonomy_update_proposal import TaxonomyUpdateProposal
from bims.models.taxonomy import Taxonomy
from bims.tests.model_factories import (
    TaxonomyF,
    TaxonGroupF,
    IUCNStatusF
)

User = get_user_model()


class EditTaxonFadaTest(FastTenantTestCase):
    """Test cases for FADA_ID field in edit taxon functionality."""

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.superuser = User.objects.create_user(
            username='superuser',
            email='superuser@example.com',
            password='password',
            is_superuser=True
        )
        self.expert_user = User.objects.create_user(
            'expert',
            'expert@example.com',
            'password'
        )
        self.parent = TaxonomyF.create(
            rank='GENUS',
            canonical_name='TestGenus'
        )
        self.taxonomy = TaxonomyF.create(
            scientific_name='Test Name',
            canonical_name='Test Canonical Name',
            parent=self.parent,
            rank='SPECIES'
        )
        self.taxon_group = TaxonGroupF.create(
            name='Test Group',
            taxonomies=(self.taxonomy,),
            experts=(self.expert_user,)
        )
        self.iucn_status = IUCNStatusF.create()

    def test_fada_id_field_saved_in_proposal(self):
        """Test that FADA_ID is properly saved when editing a taxon."""
        self.client.login(username='superuser', password='password')

        url = reverse('edit_taxon', kwargs={
            'id': self.taxonomy.id,
            'taxon_group_id': self.taxon_group.id
        })

        fada_id_value = 'FADA123456'

        data = {
            'taxon_name': 'Updated Taxon',
            'rank': 'SPECIES',
            'author': 'Test Author',
            'iucn_status': self.iucn_status.id,
            'taxonomic_status': 'ACCEPTED',
            'accepted_taxonomy': '',
            'tags': [],
            'parent': self.parent.id,
            'fada_id': fada_id_value,
        }

        response = self.client.post(url, data)

        # Check that redirect occurred (success)
        self.assertEqual(response.status_code, 302)

        # Verify that a proposal was created with the FADA_ID
        proposal = TaxonomyUpdateProposal.objects.filter(
            original_taxonomy=self.taxonomy,
            taxon_group=self.taxon_group,
            status='pending'
        ).first()

        self.assertIsNotNone(proposal)
        self.assertEqual(proposal.fada_id, fada_id_value)

        self.client.logout()

    def test_fada_id_field_updated_in_existing_proposal(self):
        """Test that FADA_ID is updated in an existing proposal."""
        self.client.login(username='expert', password='password')

        url = reverse('edit_taxon', kwargs={
            'id': self.taxonomy.id,
            'taxon_group_id': self.taxon_group.id
        })

        # Create initial proposal
        initial_fada_id = 'FADA000001'
        data = {
            'taxon_name': 'Updated Taxon',
            'rank': 'SPECIES',
            'author': 'Test Author',
            'iucn_status': self.iucn_status.id,
            'taxonomic_status': 'ACCEPTED',
            'accepted_taxonomy': '',
            'tags': [],
            'parent': self.parent.id,
            'fada_id': initial_fada_id,
        }

        response = self.client.post(url, data)
        self.assertEqual(response.status_code, 302)

        # Verify initial FADA_ID
        proposal = TaxonomyUpdateProposal.objects.filter(
            original_taxonomy=self.taxonomy,
            taxon_group=self.taxon_group,
            status='pending'
        ).first()
        self.assertEqual(proposal.fada_id, initial_fada_id)

        # Update with new FADA_ID
        updated_fada_id = 'FADA999999'
        data['fada_id'] = updated_fada_id

        response = self.client.post(url, data)
        self.assertEqual(response.status_code, 302)

        # Verify updated FADA_ID
        proposal.refresh_from_db()
        self.assertEqual(proposal.fada_id, updated_fada_id)

        self.client.logout()

    def test_fada_id_empty_string_becomes_none(self):
        """Test that empty FADA_ID string is converted to None."""
        self.client.login(username='superuser', password='password')

        url = reverse('edit_taxon', kwargs={
            'id': self.taxonomy.id,
            'taxon_group_id': self.taxon_group.id
        })

        data = {
            'taxon_name': 'Updated Taxon',
            'rank': 'SPECIES',
            'author': 'Test Author',
            'iucn_status': self.iucn_status.id,
            'taxonomic_status': 'ACCEPTED',
            'accepted_taxonomy': '',
            'tags': [],
            'parent': self.parent.id,
            'fada_id': '',  # Empty string
        }

        response = self.client.post(url, data)
        self.assertEqual(response.status_code, 302)

        # Verify that FADA_ID is None (not empty string)
        proposal = TaxonomyUpdateProposal.objects.filter(
            original_taxonomy=self.taxonomy,
            taxon_group=self.taxon_group,
            status='pending'
        ).first()

        self.assertIsNotNone(proposal)
        self.assertIsNone(proposal.fada_id)

        self.client.logout()

    def test_fada_id_whitespace_stripped(self):
        """Test that whitespace is stripped from FADA_ID."""
        self.client.login(username='superuser', password='password')

        url = reverse('edit_taxon', kwargs={
            'id': self.taxonomy.id,
            'taxon_group_id': self.taxon_group.id
        })

        data = {
            'taxon_name': 'Updated Taxon',
            'rank': 'SPECIES',
            'author': 'Test Author',
            'iucn_status': self.iucn_status.id,
            'taxonomic_status': 'ACCEPTED',
            'accepted_taxonomy': '',
            'tags': [],
            'parent': self.parent.id,
            'fada_id': '  FADA123  ',  # With whitespace
        }

        response = self.client.post(url, data)
        self.assertEqual(response.status_code, 302)

        # Verify that FADA_ID has whitespace stripped
        proposal = TaxonomyUpdateProposal.objects.filter(
            original_taxonomy=self.taxonomy,
            taxon_group=self.taxon_group,
            status='pending'
        ).first()

        self.assertIsNotNone(proposal)
        self.assertEqual(proposal.fada_id, 'FADA123')

        self.client.logout()

    def test_fada_id_in_proposal(self):
        """Test that FADA_ID is stored in the proposal correctly."""
        self.client.login(username='expert', password='password')

        url = reverse('edit_taxon', kwargs={
            'id': self.taxonomy.id,
            'taxon_group_id': self.taxon_group.id
        })

        fada_id_value = 'FADA_IN_PROPOSAL'

        data = {
            'taxon_name': 'TestGenus InProposal',
            'rank': 'SPECIES',
            'author': 'Test Author',
            'iucn_status': self.iucn_status.id,
            'taxonomic_status': 'ACCEPTED',
            'accepted_taxonomy': '',
            'tags': [],
            'parent': self.parent.id,
            'fada_id': fada_id_value,
        }

        # Create proposal with FADA_ID
        response = self.client.post(url, data)
        self.assertEqual(response.status_code, 302)

        proposal = TaxonomyUpdateProposal.objects.filter(
            original_taxonomy=self.taxonomy,
            taxon_group=self.taxon_group,
            status='pending'
        ).first()

        self.assertIsNotNone(proposal)
        self.assertEqual(proposal.fada_id, fada_id_value)
        # Verify the proposal has all necessary fields
        self.assertEqual(proposal.status, 'pending')
        self.assertEqual(proposal.original_taxonomy, self.taxonomy)

        self.client.logout()

    def test_fada_id_preserved_when_proposal_approved(self):
        """Test that FADA_ID is preserved when proposal is approved."""
        self.client.login(username='expert', password='password')

        url = reverse('edit_taxon', kwargs={
            'id': self.taxonomy.id,
            'taxon_group_id': self.taxon_group.id
        })

        fada_id_value = 'FADA_APPROVED'

        data = {
            'taxon_name': 'TestGenus Approved',
            'rank': 'SPECIES',
            'author': 'Test Author',
            'iucn_status': self.iucn_status.id,
            'taxonomic_status': 'ACCEPTED',
            'accepted_taxonomy': '',
            'tags': [],
            'parent': self.parent.id,
            'fada_id': fada_id_value,
        }

        # Create proposal with FADA_ID
        response = self.client.post(url, data)
        self.assertEqual(response.status_code, 302)

        proposal = TaxonomyUpdateProposal.objects.filter(
            original_taxonomy=self.taxonomy,
            taxon_group=self.taxon_group,
            status='pending'
        ).first()

        self.assertIsNotNone(proposal)
        self.assertEqual(proposal.fada_id, fada_id_value)

        # Approve the proposal
        proposal.approve(self.expert_user)

        # Verify FADA_ID was copied to the taxonomy
        self.taxonomy.refresh_from_db()
        self.assertEqual(self.taxonomy.fada_id, fada_id_value)

        # Verify proposal status is approved
        proposal.refresh_from_db()
        self.assertEqual(proposal.status, 'approved')

        self.client.logout()

    def test_fada_id_not_required(self):
        """Test that FADA_ID is optional and form works without it."""
        self.client.login(username='superuser', password='password')

        url = reverse('edit_taxon', kwargs={
            'id': self.taxonomy.id,
            'taxon_group_id': self.taxon_group.id
        })

        # Don't include fada_id in data
        data = {
            'taxon_name': 'Updated Taxon',
            'rank': 'SPECIES',
            'author': 'Test Author',
            'iucn_status': self.iucn_status.id,
            'taxonomic_status': 'ACCEPTED',
            'accepted_taxonomy': '',
            'tags': [],
            'parent': self.parent.id,
        }

        response = self.client.post(url, data)

        # Should still succeed
        self.assertEqual(response.status_code, 302)

        # Verify proposal was created
        proposal = TaxonomyUpdateProposal.objects.filter(
            original_taxonomy=self.taxonomy,
            taxon_group=self.taxon_group,
            status='pending'
        ).first()

        self.assertIsNotNone(proposal)
        # FADA_ID should be None
        self.assertIsNone(proposal.fada_id)

        self.client.logout()

    def test_col_id_field_saved_in_proposal(self):
        """Test that col_id is properly saved when editing a taxon."""
        self.client.login(username='superuser', password='password')

        url = reverse('edit_taxon', kwargs={
            'id': self.taxonomy.id,
            'taxon_group_id': self.taxon_group.id
        })

        col_id_value = 'ABC123'

        data = {
            'taxon_name': 'Updated Taxon',
            'rank': 'SPECIES',
            'author': 'Test Author',
            'iucn_status': self.iucn_status.id,
            'taxonomic_status': 'ACCEPTED',
            'accepted_taxonomy': '',
            'tags': [],
            'parent': self.parent.id,
            'col_id': col_id_value,
        }

        response = self.client.post(url, data)

        self.assertEqual(response.status_code, 302)

        proposal = TaxonomyUpdateProposal.objects.filter(
            original_taxonomy=self.taxonomy,
            taxon_group=self.taxon_group,
            status='pending'
        ).first()

        self.assertIsNotNone(proposal)
        self.assertEqual(proposal.col_id, col_id_value)

        self.client.logout()

    def test_col_id_preserved_when_proposal_approved(self):
        """Test that col_id is copied to the taxonomy when the proposal is approved."""
        self.client.login(username='expert', password='password')

        url = reverse('edit_taxon', kwargs={
            'id': self.taxonomy.id,
            'taxon_group_id': self.taxon_group.id
        })

        col_id_value = 'XYZ789'

        data = {
            'taxon_name': 'TestGenus Approved',
            'rank': 'SPECIES',
            'author': 'Test Author',
            'iucn_status': self.iucn_status.id,
            'taxonomic_status': 'ACCEPTED',
            'accepted_taxonomy': '',
            'tags': [],
            'parent': self.parent.id,
            'col_id': col_id_value,
        }

        response = self.client.post(url, data)
        self.assertEqual(response.status_code, 302)

        proposal = TaxonomyUpdateProposal.objects.filter(
            original_taxonomy=self.taxonomy,
            taxon_group=self.taxon_group,
            status='pending'
        ).first()

        self.assertIsNotNone(proposal)
        self.assertEqual(proposal.col_id, col_id_value)

        proposal.approve(self.expert_user)

        self.taxonomy.refresh_from_db()
        self.assertEqual(self.taxonomy.col_id, col_id_value)

        proposal.refresh_from_db()
        self.assertEqual(proposal.status, 'approved')

        self.client.logout()

    def test_col_id_empty_string_becomes_none(self):
        """Test that submitting an empty col_id results in None, not ''."""
        self.client.login(username='superuser', password='password')

        url = reverse('edit_taxon', kwargs={
            'id': self.taxonomy.id,
            'taxon_group_id': self.taxon_group.id
        })

        data = {
            'taxon_name': 'Updated Taxon',
            'rank': 'SPECIES',
            'author': 'Test Author',
            'iucn_status': self.iucn_status.id,
            'taxonomic_status': 'ACCEPTED',
            'accepted_taxonomy': '',
            'tags': [],
            'parent': self.parent.id,
            'col_id': '',
        }

        response = self.client.post(url, data)
        self.assertEqual(response.status_code, 302)

        proposal = TaxonomyUpdateProposal.objects.filter(
            original_taxonomy=self.taxonomy,
            taxon_group=self.taxon_group,
            status='pending'
        ).first()

        self.assertIsNotNone(proposal)
        self.assertIsNone(proposal.col_id)

        self.client.logout()

    def test_doubtful_status_without_parent_keeps_parent(self):
        """An empty parent must not detach a doubtful taxon from its genus."""
        self.client.login(username='superuser', password='password')
        url = reverse('edit_taxon', kwargs={
            'id': self.taxonomy.id,
            'taxon_group_id': self.taxon_group.id
        })
        response = self.client.post(url, {
            'taxon_name': 'epithet',
            'rank': 'SPECIES',
            'author': 'Test Author',
            'taxonomic_status': 'DOUBTFUL',
            'accepted_taxonomy': '',
            'parent': '',
        })
        self.assertEqual(response.status_code, 302)

        proposal = TaxonomyUpdateProposal.objects.get(
            original_taxonomy=self.taxonomy,
            taxon_group=self.taxon_group,
            status='pending'
        )
        self.assertEqual(proposal.taxonomic_status, 'DOUBTFUL')
        self.assertEqual(proposal.parent, self.parent)
        self.assertEqual(proposal.canonical_name, 'TestGenus epithet')

        proposal.approve(self.superuser, suppress_emails=True)
        self.taxonomy.refresh_from_db()
        self.assertEqual(self.taxonomy.parent, self.parent)


class EditTaxonReadOnlyGroupTest(FastTenantTestCase):
    """
    Hard-block: editing is denied for any taxon that belongs to a read-only
    group, regardless of which group URL the user navigates through.
    """

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.superuser = User.objects.create_user(
            username='ro_superuser',
            email='ro_superuser@example.com',
            password='password',
            is_superuser=True,
        )
        self.expert_user = User.objects.create_user(
            username='ro_expert',
            email='ro_expert@example.com',
            password='password',
        )
        self.regular_user = User.objects.create_user(
            username='ro_regular',
            email='ro_regular@example.com',
            password='password',
        )
        # A taxon that belongs to BOTH a read-only group and a normal group
        self.shared_taxonomy = TaxonomyF.create(
            scientific_name='Shared Species',
            canonical_name='Shared Species',
            rank='SPECIES',
        )
        self.readonly_group = TaxonGroupF.create(
            name='Readonly Group',
            is_readonly=True,
            upstream_url='https://upstream.example.org',
            upstream_id='10',
            taxonomies=(self.shared_taxonomy,),
            experts=(self.expert_user,),
        )
        self.normal_group = TaxonGroupF.create(
            name='Normal Group',
            is_readonly=False,
            taxonomies=(self.shared_taxonomy,),
            experts=(self.expert_user,),
        )
        # A taxon that belongs ONLY to normal groups
        self.free_taxonomy = TaxonomyF.create(
            scientific_name='Free Species',
            canonical_name='Free Species',
            rank='SPECIES',
        )
        self.free_group = TaxonGroupF.create(
            name='Free Group',
            is_readonly=False,
            taxonomies=(self.free_taxonomy,),
            experts=(self.expert_user,),
        )

    def _url(self, taxonomy, taxon_group):
        return reverse('edit_taxon', kwargs={
            'id': taxonomy.id,
            'taxon_group_id': taxon_group.id,
        })

    # ------------------------------------------------------------------
    # Blocked via the read-only group's own URL
    # ------------------------------------------------------------------

    def test_superuser_blocked_via_readonly_group_url(self):
        self.client.login(username='ro_superuser', password='password')
        response = self.client.get(self._url(self.shared_taxonomy, self.readonly_group))
        self.assertEqual(response.status_code, 403)

    def test_expert_blocked_via_readonly_group_url(self):
        self.client.login(username='ro_expert', password='password')
        response = self.client.get(self._url(self.shared_taxonomy, self.readonly_group))
        self.assertEqual(response.status_code, 403)

    # ------------------------------------------------------------------
    # Hard block: also denied via the NORMAL group's URL
    # ------------------------------------------------------------------

    def test_superuser_blocked_via_normal_group_url_when_taxon_in_readonly_group(self):
        self.client.login(username='ro_superuser', password='password')
        response = self.client.get(self._url(self.shared_taxonomy, self.normal_group))
        self.assertEqual(response.status_code, 403)

    def test_expert_blocked_via_normal_group_url_when_taxon_in_readonly_group(self):
        self.client.login(username='ro_expert', password='password')
        response = self.client.get(self._url(self.shared_taxonomy, self.normal_group))
        self.assertEqual(response.status_code, 403)

    def test_regular_user_blocked_via_normal_group_url_when_taxon_in_readonly_group(self):
        self.client.login(username='ro_regular', password='password')
        response = self.client.get(self._url(self.shared_taxonomy, self.normal_group))
        self.assertEqual(response.status_code, 403)

    def test_post_blocked_via_normal_group_url_when_taxon_in_readonly_group(self):
        self.client.login(username='ro_superuser', password='password')
        response = self.client.post(
            self._url(self.shared_taxonomy, self.normal_group),
            {'taxon_name': 'Should Not Save', 'rank': 'SPECIES'},
        )
        self.assertEqual(response.status_code, 403)

    # ------------------------------------------------------------------
    # Blocking response renders the template (not a bare 403 page)
    # ------------------------------------------------------------------

    def test_403_response_uses_edit_taxon_template(self):
        self.client.login(username='ro_superuser', password='password')
        response = self.client.get(self._url(self.shared_taxonomy, self.normal_group))
        self.assertTemplateUsed(response, 'edit_taxon.html')

    def test_403_response_includes_blocking_group_name(self):
        self.client.login(username='ro_superuser', password='password')
        response = self.client.get(self._url(self.shared_taxonomy, self.normal_group))
        self.assertIn(b'Readonly Group', response.content)

    def test_403_response_includes_upstream_url(self):
        self.client.login(username='ro_superuser', password='password')
        response = self.client.get(self._url(self.shared_taxonomy, self.normal_group))
        self.assertIn(b'upstream.example.org', response.content)

    def test_403_response_context_has_readonly_blocked_by(self):
        self.client.login(username='ro_superuser', password='password')
        response = self.client.get(self._url(self.shared_taxonomy, self.normal_group))
        self.assertIn('readonly_blocked_by', response.context)
        group_names = [g.name for g in response.context['readonly_blocked_by']]
        self.assertIn('Readonly Group', group_names)

    # ------------------------------------------------------------------
    # Taxon belonging only to normal groups is not blocked
    # ------------------------------------------------------------------

    def test_superuser_allowed_for_taxon_with_no_readonly_group(self):
        self.client.login(username='ro_superuser', password='password')
        response = self.client.get(self._url(self.free_taxonomy, self.free_group))
        self.assertEqual(response.status_code, 200)

    def test_expert_allowed_for_taxon_with_no_readonly_group(self):
        self.client.login(username='ro_expert', password='password')
        response = self.client.get(self._url(self.free_taxonomy, self.free_group))
        self.assertEqual(response.status_code, 200)
