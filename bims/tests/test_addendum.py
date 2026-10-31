from django.contrib.auth import get_user_model
from django.urls import reverse
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient

from bims.api_views.taxon_update import (
    create_taxon_proposal,
    update_taxon_proposal,
)
from bims.enums.taxon_addendum import TaxonAddendum
from bims.models.taxonomy import Taxonomy
from bims.models.taxonomy_update_proposal import TaxonomyUpdateProposal
from bims.serializers.checklist_serializer import (
    ChecklistPDFSerializer,
    ChecklistSerializer,
)
from bims.serializers.coldp_serializer import ColDPTaxonSerializer
from bims.serializers.taxon_serializer import TaxonSerializer
from bims.tests.model_factories import TaxonGroupF, TaxonomyF
from bims.utils.taxonomy import (
    build_name_with_addendum,
    get_addendum_display,
    resolve_addendum_code,
    strip_addendum_from_name,
)
from bims.views.download_csv_taxa_list import TaxaCSVSerializer

User = get_user_model()


class TestAddendumHelpers(FastTenantTestCase):

    def test_get_addendum_display_abbreviated(self):
        self.assertEqual(
            get_addendum_display(TaxonAddendum.SENSU_LATO.name, abbreviate=True),
            's.l.'
        )

    def test_get_addendum_display_full_text(self):
        self.assertEqual(
            get_addendum_display(TaxonAddendum.SENSU_LATO.name, abbreviate=False),
            'sensu lato'
        )

    def test_partim_display_is_full_word(self):
        self.assertEqual(
            get_addendum_display(TaxonAddendum.PARTIM.name, abbreviate=True),
            'partim',
        )
        self.assertEqual(
            get_addendum_display(TaxonAddendum.PARTIM.name, abbreviate=False),
            'partim',
        )

    def test_partim_resolved_and_stripped_from_name(self):
        self.assertEqual(resolve_addendum_code('Partim'), TaxonAddendum.PARTIM.name)
        self.assertEqual(
            strip_addendum_from_name('Aquanothrus montanus partim'),
            ('Aquanothrus montanus', TaxonAddendum.PARTIM.name),
        )
        self.assertEqual(
            build_name_with_addendum(
                'Aquanothrus montanus', 'Engelbrecht, 1975',
                TaxonAddendum.PARTIM.name),
            'Aquanothrus montanus partim Engelbrecht, 1975',
        )

    def test_get_addendum_display_empty_code(self):
        self.assertEqual(get_addendum_display(''), '')
        self.assertEqual(get_addendum_display(None), '')

    def test_get_addendum_display_unknown_code(self):
        self.assertEqual(get_addendum_display('NOT_A_REAL_CODE'), '')

    def test_build_name_with_addendum(self):
        name = build_name_with_addendum(
            'Aquanothrus montanus',
            'Engelbrecht, 1975',
            TaxonAddendum.SENSU_LATO.name,
            abbreviate=True,
        )
        self.assertEqual(name, 'Aquanothrus montanus s.l. Engelbrecht, 1975')

    def test_build_name_with_addendum_no_code(self):
        name = build_name_with_addendum(
            'Aquanothrus montanus', 'Engelbrecht, 1975', '', abbreviate=True
        )
        self.assertEqual(name, 'Aquanothrus montanus Engelbrecht, 1975')

    def test_resolve_addendum_code_matches_known_variants(self):
        for raw in ('sensu lato', 'SENSU LATO', 's.l.', 'S.L.', 'SENSU_LATO'):
            self.assertEqual(
                resolve_addendum_code(raw), TaxonAddendum.SENSU_LATO.name
            )

    def test_resolve_addendum_code_ignores_unknown(self):
        self.assertEqual(resolve_addendum_code('nomen dubium'), '')
        self.assertEqual(resolve_addendum_code(''), '')
        self.assertEqual(resolve_addendum_code(None), '')


class TestStripAddendumFromName(FastTenantTestCase):

    def test_strips_dotted_abbreviation(self):
        name, code = strip_addendum_from_name('Aquanothrus montanus s.l.')
        self.assertEqual(name, 'Aquanothrus montanus')
        self.assertEqual(code, TaxonAddendum.SENSU_LATO.name)

    def test_strips_spaced_abbreviation(self):
        name, code = strip_addendum_from_name('Aquanothrus montanus s. l.')
        self.assertEqual(name, 'Aquanothrus montanus')
        self.assertEqual(code, TaxonAddendum.SENSU_LATO.name)

    def test_strips_undotted_abbreviation_case_insensitive(self):
        name, code = strip_addendum_from_name('Aquanothrus montanus SL')
        self.assertEqual(name, 'Aquanothrus montanus')
        self.assertEqual(code, TaxonAddendum.SENSU_LATO.name)

    def test_strips_full_text(self):
        name, code = strip_addendum_from_name(
            'Aquanothrus montanus sensu lato'
        )
        self.assertEqual(name, 'Aquanothrus montanus')
        self.assertEqual(code, TaxonAddendum.SENSU_LATO.name)

    def test_leaves_name_without_addendum_unchanged(self):
        name, code = strip_addendum_from_name('Aquanothrus montanus')
        self.assertEqual(name, 'Aquanothrus montanus')
        self.assertEqual(code, '')

    def test_does_not_strip_mid_name_occurrence(self):
        # 'sl' embedded inside a real epithet must not be mistaken for the
        # addendum - only a trailing, whitespace-separated token counts.
        name, code = strip_addendum_from_name('Islandia islandica')
        self.assertEqual(name, 'Islandia islandica')
        self.assertEqual(code, '')

    def test_empty_name(self):
        self.assertEqual(strip_addendum_from_name(''), ('', ''))
        self.assertEqual(strip_addendum_from_name(None), ('', ''))


class TestAddendumModelField(FastTenantTestCase):

    def test_field_blank_by_default(self):
        taxon = TaxonomyF.create()
        self.assertEqual(taxon.addendum, '')

    def test_field_accepts_sensu_lato(self):
        taxon = TaxonomyF.create(addendum=TaxonAddendum.SENSU_LATO.name)
        taxon.refresh_from_db()
        self.assertEqual(taxon.addendum, TaxonAddendum.SENSU_LATO.name)

    def test_field_choices_contain_sensu_lato(self):
        choices = dict(Taxonomy._meta.get_field('addendum').choices)
        self.assertEqual(choices.get('SENSU_LATO'), 'sensu lato')


class TestAddendumSerializers(FastTenantTestCase):

    def setUp(self):
        self.taxon = TaxonomyF.create(
            scientific_name='Aquanothrus montanus Engelbrecht, 1975',
            canonical_name='Aquanothrus montanus',
            author='Engelbrecht, 1975',
            addendum=TaxonAddendum.SENSU_LATO.name,
        )
        self.plain_taxon = TaxonomyF.create(
            scientific_name='Homo sapiens L.',
            canonical_name='Homo sapiens',
            author='L.',
        )

    def test_checklist_pdf_serializer_appends_abbreviation(self):
        data = ChecklistPDFSerializer(self.taxon).data
        self.assertEqual(
            data['scientific_name'], 'Aquanothrus montanus s.l. Engelbrecht, 1975'
        )

    def test_checklist_pdf_serializer_no_addendum(self):
        data = ChecklistPDFSerializer(self.plain_taxon).data
        self.assertNotIn('s.l.', data['scientific_name'])

    def test_checklist_serializer_appends_abbreviation(self):
        data = ChecklistSerializer(self.taxon).data
        self.assertEqual(
            data['scientific_name'], 'Aquanothrus montanus s.l. Engelbrecht, 1975'
        )

    def test_checklist_serializer_no_addendum(self):
        data = ChecklistSerializer(self.plain_taxon).data
        self.assertEqual(data['scientific_name'], 'Homo sapiens L.')

    def test_coldp_name_phrase_set(self):
        data = ColDPTaxonSerializer(self.taxon).data
        self.assertEqual(data['namePhrase'], 'sensu lato')

    def test_coldp_name_phrase_empty(self):
        data = ColDPTaxonSerializer(self.plain_taxon).data
        self.assertEqual(data['namePhrase'], '')

    def test_taxa_csv_serializer_taxon_and_authority(self):
        serializer = TaxaCSVSerializer(self.taxon)
        data = serializer.data
        self.assertIn('s.l.', data['taxon'])
        self.assertEqual(
            data['scientific_name_and_authority'],
            'Aquanothrus montanus s.l. Engelbrecht, 1975'
        )
        self.assertEqual(data['addendum'], 'sensu lato')

    def test_taxa_csv_serializer_no_addendum(self):
        serializer = TaxaCSVSerializer(self.plain_taxon)
        data = serializer.data
        self.assertNotIn('s.l.', data['taxon'])
        self.assertEqual(data['addendum'], '')

    def test_taxon_serializer_addendum_and_scientific_name(self):
        data = TaxonSerializer(self.taxon, context={'validated': True}).data
        self.assertEqual(data['addendum'], 'sensu lato')
        self.assertIn('sensu lato', data['scientific_name'])

    def test_taxon_serializer_no_addendum(self):
        data = TaxonSerializer(self.plain_taxon, context={'validated': True}).data
        self.assertEqual(data['addendum'], '')
        self.assertEqual(data['scientific_name'], 'Homo sapiens L.')


class TestAddendumProposalLifecycle(FastTenantTestCase):

    def setUp(self):
        self.superuser = User.objects.create_user(
            username='superuser',
            email='superuser@example.com',
            password='password',
            is_superuser=True,
        )
        self.taxon = TaxonomyF.create(
            scientific_name='Test Name',
            canonical_name='Test Canonical Name',
        )
        self.taxon_group = TaxonGroupF.create(
            name='Test Group',
            taxonomies=(self.taxon,),
        )

    def test_create_proposal_carries_addendum(self):
        proposal = create_taxon_proposal(
            taxon=self.taxon,
            taxon_group=self.taxon_group,
            data={'addendum': TaxonAddendum.SENSU_LATO.name},
            creator=self.superuser,
        )
        self.assertEqual(proposal.addendum, TaxonAddendum.SENSU_LATO.name)

    def test_update_proposal_changes_addendum(self):
        proposal = create_taxon_proposal(
            taxon=self.taxon,
            taxon_group=self.taxon_group,
            data={'addendum': TaxonAddendum.SENSU_LATO.name},
            creator=self.superuser,
        )
        updated = update_taxon_proposal(
            proposal=proposal,
            data={'addendum': ''},
            user=self.superuser,
        )
        self.assertEqual(updated.addendum, '')

    def test_approve_copies_addendum_to_taxonomy(self):
        proposal = create_taxon_proposal(
            taxon=self.taxon,
            taxon_group=self.taxon_group,
            data={'addendum': TaxonAddendum.SENSU_LATO.name},
            creator=self.superuser,
        )
        proposal.approve(self.superuser, suppress_emails=True)

        self.taxon.refresh_from_db()
        self.assertEqual(self.taxon.addendum, TaxonAddendum.SENSU_LATO.name)

    def test_approve_without_addendum_leaves_it_blank(self):
        proposal = create_taxon_proposal(
            taxon=self.taxon,
            taxon_group=self.taxon_group,
            data={},
            creator=self.superuser,
        )
        proposal.approve(self.superuser, suppress_emails=True)

        self.taxon.refresh_from_db()
        self.assertEqual(self.taxon.addendum, '')


class TestPartimEditFlow(FastTenantTestCase):
    """Edit form -> proposal -> approval -> serialized output for 'partim'."""

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.superuser = User.objects.create_user(
            username='superuser', email='superuser@example.com',
            password='password', is_superuser=True)
        self.parent = TaxonomyF.create(rank='GENUS', canonical_name='Aquanothrus')
        self.taxon = TaxonomyF.create(
            scientific_name='Aquanothrus montanus Engelbrecht, 1975',
            canonical_name='Aquanothrus montanus',
            author='Engelbrecht, 1975',
            rank='SPECIES',
            parent=self.parent,
        )
        self.taxon_group = TaxonGroupF.create(
            name='Partim Group', taxonomies=(self.taxon,))
        self.client.login(username='superuser', password='password')

    def _edit_url(self):
        return reverse('edit_taxon', kwargs={
            'id': self.taxon.id, 'taxon_group_id': self.taxon_group.id})

    def test_edit_form_offers_partim_option(self):
        response = self.client.get(self._edit_url())
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, '<option value="PARTIM"')
        self.assertContains(response, '<option value="SENSU_LATO"')

    def test_set_partim_via_edit_form_then_approve(self):
        response = self.client.post(self._edit_url(), {
            'taxon_name': 'Aquanothrus montanus',
            'rank': 'SPECIES',
            'author': 'Engelbrecht, 1975',
            'taxonomic_status': 'ACCEPTED',
            'accepted_taxonomy': '',
            'tags': [],
            'parent': self.parent.id,
            'addendum': TaxonAddendum.PARTIM.name,
        })
        self.assertEqual(response.status_code, 302)

        proposal = TaxonomyUpdateProposal.objects.get(
            original_taxonomy=self.taxon, status='pending')
        self.assertEqual(proposal.addendum, TaxonAddendum.PARTIM.name)

        # Not applied to the live taxon until the proposal is approved
        self.taxon.refresh_from_db()
        self.assertEqual(self.taxon.addendum, '')

        proposal.approve(self.superuser, suppress_emails=True)
        self.taxon.refresh_from_db()
        self.assertEqual(self.taxon.addendum, TaxonAddendum.PARTIM.name)

        # Shown as the full word in every output
        expected = 'Aquanothrus montanus partim Engelbrecht, 1975'
        self.assertEqual(
            ChecklistSerializer(self.taxon).data['scientific_name'], expected)
        self.assertEqual(
            ChecklistPDFSerializer(self.taxon).data['scientific_name'], expected)
        csv_data = TaxaCSVSerializer(self.taxon).data
        self.assertEqual(csv_data['scientific_name_and_authority'], expected)
        self.assertEqual(csv_data['addendum'], 'partim')
        self.assertEqual(
            ColDPTaxonSerializer(self.taxon).data['namePhrase'], 'partim')
        self.assertEqual(
            TaxonSerializer(self.taxon, context={'validated': True}).data['addendum'],
            'partim')

    def test_clearing_addendum_via_edit_form(self):
        self.taxon.addendum = TaxonAddendum.PARTIM.name
        self.taxon.save()
        self.client.post(self._edit_url(), {
            'taxon_name': 'Aquanothrus montanus',
            'rank': 'SPECIES',
            'author': 'Engelbrecht, 1975',
            'taxonomic_status': 'ACCEPTED',
            'accepted_taxonomy': '',
            'tags': [],
            'parent': self.parent.id,
            'addendum': '',
        })
        proposal = TaxonomyUpdateProposal.objects.get(
            original_taxonomy=self.taxon, status='pending')
        proposal.approve(self.superuser, suppress_emails=True)
        self.taxon.refresh_from_db()
        self.assertEqual(self.taxon.addendum, '')
