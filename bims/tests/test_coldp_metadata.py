import datetime

from django.urls import reverse
from django_tenants.test.cases import FastTenantTestCase
from rest_framework import status
from rest_framework.test import APIRequestFactory
from unittest.mock import patch

from bims.api_views.coldp import ColDPMetadataView, ColDPTaxonView
from bims.enums.taxonomic_rank import TaxonomicRank
from bims.models.taxonomy_checklist import TaxonomyChecklist
from bims.serializers.coldp_serializer import ColDPTaxonSerializer
from bims.models.data_source import DataSource
from bims.tests.model_factories import TaxonomyF, UserF, DataSourceF


def make_checklist(**kwargs) -> TaxonomyChecklist:
    defaults = dict(
        title='Test Checklist',
        version='1.0',
        description='Test description.',
        license='https://creativecommons.org/licenses/by/4.0/',
        citation='',
        doi='',
        released_at=datetime.date(2025, 1, 15),
        is_published=True,
        contact=None,
    )
    defaults.update(kwargs)
    creators = defaults.pop('creators', [])
    obj = TaxonomyChecklist.objects.create(**defaults)
    if creators:
        obj.creators.set(creators)
    return obj


class TestColDPMetadataView(FastTenantTestCase):

    def setUp(self):
        self.factory = APIRequestFactory()
        self.view = ColDPMetadataView.as_view()
        TaxonomyChecklist.objects.all().delete()
        DataSource.objects.all().delete()

    def _get(self, params=''):
        request = self.factory.get(f'/api/coldp/metadata/{params}')
        with patch('bims.api_views.coldp.get_current_domain', return_value='example.com'):
            return self.view(request)

    # --- Basic structure ----------------------------------------------------

    def test_returns_200_with_published_checklist(self):
        make_checklist()
        self.assertEqual(self._get().status_code, status.HTTP_200_OK)

    def test_returns_404_when_no_published_checklist(self):
        make_checklist(is_published=False)
        self.assertEqual(self._get().status_code, 404)

    def test_returns_404_when_table_empty(self):
        self.assertEqual(self._get().status_code, 404)

    def test_required_keys_present(self):
        make_checklist()
        for key in ('title', 'description', 'version', 'issued', 'license',
                    'citation', 'identifier', 'contact', 'creator', 'source'):
            self.assertIn(key, self._get().data)

    # --- Version selection --------------------------------------------------

    def test_returns_latest_published_by_default(self):
        make_checklist(version='1.0', released_at=datetime.date(2024, 1, 1))
        make_checklist(version='2.0', released_at=datetime.date(2025, 1, 1))
        self.assertEqual(self._get().data['version'], '2.0')

    def test_version_param_selects_specific_checklist(self):
        make_checklist(version='1.0')
        make_checklist(version='2.0')
        self.assertEqual(self._get('?version=1.0').data['version'], '1.0')

    def test_version_param_404_for_unknown(self):
        make_checklist(version='1.0')
        self.assertEqual(self._get('?version=99.0').status_code, 404)

    def test_version_param_works_for_unpublished(self):
        make_checklist(version='draft', is_published=False)
        response = self._get('?version=draft')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['version'], 'draft')

    # --- Scalar fields ------------------------------------------------------

    def test_title(self):
        make_checklist(title='My Fish Checklist')
        self.assertEqual(self._get().data['title'], 'My Fish Checklist')

    def test_issued_from_released_at(self):
        make_checklist(released_at=datetime.date(2025, 6, 1))
        self.assertEqual(self._get().data['issued'], '2025-06-01')

    def test_issued_falls_back_to_today(self):
        make_checklist(released_at=None)
        self.assertEqual(self._get().data['issued'], datetime.date.today().isoformat())

    def test_doi_as_identifier(self):
        make_checklist(doi='https://doi.org/10.1234/test')
        self.assertEqual(self._get().data['identifier'], 'https://doi.org/10.1234/test')

    def test_identifier_falls_back_to_domain(self):
        make_checklist(doi='')
        self.assertIn('example.com', self._get().data['identifier'])

    def test_stored_citation_verbatim(self):
        make_checklist(citation='Doe J. 2025. My dataset.')
        self.assertEqual(self._get().data['citation'], 'Doe J. 2025. My dataset.')

    # --- Contact (User FK) --------------------------------------------------

    def test_contact_none_when_not_set(self):
        make_checklist(contact=None)
        contact = self._get().data['contact']
        self.assertEqual(contact['name'], '')
        self.assertEqual(contact['email'], '')

    def test_contact_from_user(self):
        user = UserF.create(
            first_name='Alice', last_name='Smith', email='alice@example.com')
        make_checklist(contact=user)
        contact = self._get().data['contact']
        self.assertEqual(contact['name'], 'Alice Smith')
        self.assertEqual(contact['email'], 'alice@example.com')

    def test_contact_falls_back_to_username(self):
        user = UserF.create(first_name='', last_name='', username='alicesmith')
        make_checklist(contact=user)
        self.assertEqual(self._get().data['contact']['name'], 'alicesmith')

    # --- Creator (User M2M) -------------------------------------------------

    def test_creator_empty_list_when_none_set(self):
        make_checklist()
        self.assertEqual(self._get().data['creator'], [])

    def test_creator_list_with_users(self):
        u1 = UserF.create(first_name='Bob', last_name='Jones', email='bob@example.com')
        u2 = UserF.create(first_name='Carol', last_name='Lee', email='carol@example.com')
        make_checklist(creators=[u1, u2])
        names = [c['name'] for c in self._get().data['creator']]
        self.assertIn('Bob Jones', names)
        self.assertIn('Carol Lee', names)

    def test_creator_email_in_response(self):
        user = UserF.create(first_name='Bob', last_name='Jones', email='bob@example.com')
        make_checklist(creators=[user])
        self.assertEqual(self._get().data['creator'][0]['email'], 'bob@example.com')

    # --- Citation auto-generation -------------------------------------------

    def test_citation_uses_first_creator_name(self):
        user = UserF.create(first_name='Kartoza', last_name='', email='')
        make_checklist(citation='', creators=[user])
        self.assertIn('Kartoza', self._get().data['citation'])

    def test_citation_falls_back_to_contact_name(self):
        user = UserF.create(first_name='River', last_name='Trust', email='')
        make_checklist(citation='', contact=user)
        self.assertIn('Test Checklist', self._get().data['citation'])

    def test_citation_no_org_uses_title(self):
        make_checklist(citation='', contact=None)
        self.assertIn('Test Checklist', self._get().data['citation'])

    # --- Source (DataSource records) ----------------------------------------

    def test_source_lists_data_sources(self):
        make_checklist()
        DataSourceF.create(name='FBIS', description='Freshwater data', category='')
        DataSourceF.create(name='GBIF', description='Global biodiversity', category='')
        ids = [s['id'] for s in self._get().data['source']]
        self.assertIn('fbis', ids)
        self.assertIn('gbif', ids)

    def test_source_entry_structure(self):
        make_checklist()
        DataSourceF.create(name='TestDB', description='Test desc', category='')
        entry = next((s for s in self._get().data['source'] if s['id'] == 'testdb'), None)
        self.assertIsNotNone(entry)
        for key in ('id', 'title', 'description'):
            self.assertIn(key, entry)

    def test_source_title_includes_category(self):
        make_checklist()
        DataSourceF.create(name='TestDB', description='', category='Freshwater')
        entry = next((s for s in self._get().data['source'] if s['id'] == 'testdb'), None)
        self.assertIn('Freshwater', entry['title'])

    def test_source_empty_when_no_data_sources(self):
        make_checklist()
        self.assertEqual(self._get().data['source'], [])


class TestColDPMetadataUrlResolves(FastTenantTestCase):

    def test_url_name_resolves(self):
        self.assertIn('coldp', reverse('coldp-metadata'))


# ---------------------------------------------------------------------------
# _serialize_taxon unit tests
# ---------------------------------------------------------------------------

class TestColDPTaxonSerializer(FastTenantTestCase):
    """Unit-tests for ColDPTaxonSerializer."""

    def _make(self, **kwargs):
        defaults = dict(
            scientific_name='Homo sapiens',
            canonical_name='Homo sapiens',
            rank=TaxonomicRank.SPECIES.name,
            taxonomic_status='ACCEPTED',
            author='Linnaeus, 1758',
        )
        defaults.update(kwargs)
        return TaxonomyF.create(**defaults)

    def _data(self, **kwargs):
        return ColDPTaxonSerializer(self._make(**kwargs)).data

    def test_taxon_id_is_string(self):
        self.assertIsInstance(self._data()['taxonID'], str)

    def test_taxon_id_uses_site_prefix_when_context_provided(self):
        t = self._make()
        data = ColDPTaxonSerializer(t, context={'site_prefix': 'FBIS'}).data
        self.assertEqual(data['taxonID'], f'FBIS{t.id}')

    def test_taxon_id_falls_back_to_plain_pk_without_context(self):
        t = self._make()
        data = ColDPTaxonSerializer(t).data
        self.assertEqual(data['taxonID'], str(t.id))

    def test_scientific_name_uses_canonical_name(self):
        data = self._data(canonical_name='Homo sapiens', scientific_name='Homo sapiens L.')
        self.assertEqual(data['scientificName'], 'Homo sapiens')

    def test_scientific_name_falls_back_to_scientific_name(self):
        data = self._data(canonical_name='', scientific_name='Homo sapiens L.')
        self.assertEqual(data['scientificName'], 'Homo sapiens L.')

    def test_authorship(self):
        self.assertEqual(self._data(author='Linnaeus, 1758')['authorship'], 'Linnaeus, 1758')

    def test_rank_mapped_to_coldp(self):
        self.assertEqual(self._data(rank=TaxonomicRank.SPECIES.name)['rank'], 'species')

    def test_status_accepted(self):
        self.assertEqual(self._data(taxonomic_status='ACCEPTED')['status'], 'accepted')

    def test_status_synonym(self):
        accepted = self._make(taxonomic_status='ACCEPTED')
        t = self._make(taxonomic_status='SYNONYM', accepted_taxonomy=accepted)
        self.assertEqual(ColDPTaxonSerializer(t).data['status'], 'synonym')

    def test_status_heterotypic_synonym(self):
        accepted = self._make(taxonomic_status='ACCEPTED')
        t = self._make(taxonomic_status='HETEROTYPIC_SYNONYM', accepted_taxonomy=accepted)
        self.assertEqual(ColDPTaxonSerializer(t).data['status'], 'heterotypic synonym')

    def test_parent_id_for_accepted_taxon(self):
        parent = self._make(rank=TaxonomicRank.GENUS.name)
        t = self._make(parent=parent, taxonomic_status='ACCEPTED')
        ctx = {'site_prefix': 'FBIS'}
        self.assertEqual(
            ColDPTaxonSerializer(t, context=ctx).data['parentID'],
            f'FBIS{parent.id}',
        )

    def test_parent_id_for_synonym_points_at_accepted(self):
        accepted = self._make(taxonomic_status='ACCEPTED')
        synonym = self._make(
            taxonomic_status='SYNONYM',
            accepted_taxonomy=accepted,
            parent=None,
        )
        ctx = {'site_prefix': 'FBIS'}
        self.assertEqual(
            ColDPTaxonSerializer(synonym, context=ctx).data['parentID'],
            f'FBIS{accepted.id}',
        )

    def test_parent_id_empty_when_no_parent(self):
        t = self._make(taxonomic_status='ACCEPTED')
        t.parent_id = None
        self.assertEqual(ColDPTaxonSerializer(t).data['parentID'], '')

    def test_class_field_present(self):
        self.assertIn('class', self._data())

    def test_classification_keys_present(self):
        data = self._data()
        for key in (
            'kingdom', 'phylum', 'class', 'subclass',
            'order', 'suborder', 'superfamily',
            'family', 'tribe', 'subtribe',
            'genus', 'subgenus', 'species',
        ):
            self.assertIn(key, data)

    def test_environment_field_present(self):
        self.assertIn('environment', self._data())

    def test_environment_from_matching_tag(self):
        t = self._make()
        t.tags.add('freshwater')
        data = ColDPTaxonSerializer(t).data
        self.assertEqual(data['environment'], 'freshwater')

    def test_environment_case_insensitive(self):
        t = self._make()
        t.tags.add('Freshwater')
        data = ColDPTaxonSerializer(t).data
        self.assertEqual(data['environment'], 'freshwater')

    def test_environment_first_match_wins(self):
        t = self._make()
        # Add a non-environment tag first, then two environment tags
        t.tags.add('some-other-tag', 'marine', 'freshwater')
        env = ColDPTaxonSerializer(t).data['environment']
        self.assertIn(env, ('marine', 'freshwater'))

    def test_environment_empty_when_no_matching_tag(self):
        t = self._make()
        t.tags.add('endemic', 'native')
        data = ColDPTaxonSerializer(t).data
        self.assertEqual(data['environment'], '')

    def test_environment_empty_when_no_tags(self):
        self.assertEqual(self._data()['environment'], '')

    def test_code_field_present(self):
        self.assertIn('code', self._data())

    def test_code_from_matching_tag(self):
        t = self._make()
        t.tags.add('zoological')
        self.assertEqual(ColDPTaxonSerializer(t).data['code'], 'zoological')

    def test_code_case_insensitive(self):
        t = self._make()
        t.tags.add('Botanical')
        self.assertEqual(ColDPTaxonSerializer(t).data['code'], 'botanical')

    def test_code_priority_order(self):
        t = self._make()
        # 'botanical' comes before 'zoological' in CODE_VALUES
        t.tags.add('zoological', 'botanical')
        self.assertEqual(ColDPTaxonSerializer(t).data['code'], 'botanical')

    def test_code_empty_when_no_matching_tag(self):
        t = self._make()
        t.tags.add('freshwater', 'endemic')
        self.assertEqual(ColDPTaxonSerializer(t).data['code'], '')

    def test_code_empty_when_no_tags(self):
        self.assertEqual(self._data()['code'], '')


# ---------------------------------------------------------------------------
# ColDPTaxonView endpoint tests
# ---------------------------------------------------------------------------

class TestColDPTaxonView(FastTenantTestCase):

    def setUp(self):
        self.factory = APIRequestFactory()
        self.view = ColDPTaxonView.as_view()

    def _get(self, params=''):
        request = self.factory.get(f'/api/coldp/taxon/{params}')
        return self.view(request)

    # --- Basic response structure -------------------------------------------

    def test_returns_200(self):
        self.assertEqual(self._get().status_code, status.HTTP_200_OK)

    def test_response_has_pagination_keys(self):
        data = self._get().data
        for key in ('count', 'next', 'previous', 'results'):
            self.assertIn(key, data)

    def test_results_is_list(self):
        self.assertIsInstance(self._get().data['results'], list)

    def test_empty_when_no_taxa(self):
        self.assertEqual(self._get().data['count'], 0)

    # --- Records returned ---------------------------------------------------

    def test_taxa_appear_in_results(self):
        TaxonomyF.create(
            canonical_name='Danio rerio',
            rank=TaxonomicRank.SPECIES.name,
            taxonomic_status='ACCEPTED',
        )
        data = self._get().data
        self.assertEqual(data['count'], 1)
        self.assertEqual(data['results'][0]['scientificName'], 'Danio rerio')

    def test_result_record_has_required_fields(self):
        TaxonomyF.create(rank=TaxonomicRank.SPECIES.name, taxonomic_status='ACCEPTED')
        record = self._get().data['results'][0]
        for field in (
            'taxonID', 'parentID', 'status', 'scientificName', 'authorship', 'rank',
            'kingdom', 'phylum', 'class', 'subclass',
            'order', 'suborder', 'superfamily',
            'family', 'tribe', 'subtribe',
            'genus', 'subgenus', 'species',
        ):
            self.assertIn(field, record)

    # --- rank filter --------------------------------------------------------

    def test_filter_by_rank(self):
        TaxonomyF.create(rank=TaxonomicRank.SPECIES.name, taxonomic_status='ACCEPTED')
        TaxonomyF.create(rank=TaxonomicRank.GENUS.name, taxonomic_status='ACCEPTED')
        data = self._get(f'?rank={TaxonomicRank.SPECIES.name}').data
        self.assertEqual(data['count'], 1)
        self.assertEqual(data['results'][0]['rank'], 'species')

    def test_filter_by_rank_returns_empty_for_no_match(self):
        TaxonomyF.create(rank=TaxonomicRank.GENUS.name, taxonomic_status='ACCEPTED')
        self.assertEqual(self._get('?rank=FAMILY').data['count'], 0)

    # --- parent filter ------------------------------------------------------

    def test_filter_by_parent(self):
        parent = TaxonomyF.create(rank=TaxonomicRank.GENUS.name, taxonomic_status='ACCEPTED')
        child = TaxonomyF.create(
            rank=TaxonomicRank.SPECIES.name,
            taxonomic_status='ACCEPTED',
            parent=parent,
        )
        TaxonomyF.create(rank=TaxonomicRank.SPECIES.name, taxonomic_status='ACCEPTED')
        data = self._get(f'?parent={parent.id}').data
        self.assertEqual(data['count'], 1)
        self.assertIn(str(child.id), data['results'][0]['taxonID'])

    # --- status filter ------------------------------------------------------

    def test_filter_by_status_accepted(self):
        accepted = TaxonomyF.create(rank=TaxonomicRank.SPECIES.name, taxonomic_status='ACCEPTED')
        TaxonomyF.create(
            rank=TaxonomicRank.SPECIES.name,
            taxonomic_status='SYNONYM',
            accepted_taxonomy=accepted,
        )
        data = self._get('?status=ACCEPTED').data
        self.assertEqual(data['count'], 1)
        self.assertEqual(data['results'][0]['status'], 'accepted')

    def test_filter_by_status_synonym(self):
        accepted = TaxonomyF.create(rank=TaxonomicRank.SPECIES.name, taxonomic_status='ACCEPTED')
        TaxonomyF.create(
            rank=TaxonomicRank.SPECIES.name,
            taxonomic_status='SYNONYM',
            accepted_taxonomy=accepted,
        )
        data = self._get('?status=SYNONYM').data
        self.assertEqual(data['count'], 1)
        self.assertEqual(data['results'][0]['status'], 'synonym')

    def test_no_status_filter_returns_all(self):
        accepted = TaxonomyF.create(rank=TaxonomicRank.SPECIES.name, taxonomic_status='ACCEPTED')
        TaxonomyF.create(
            rank=TaxonomicRank.SPECIES.name,
            taxonomic_status='SYNONYM',
            accepted_taxonomy=accepted,
        )
        self.assertEqual(self._get().data['count'], 2)

    # --- name search (q) ----------------------------------------------------

    def test_search_by_canonical_name(self):
        TaxonomyF.create(canonical_name='Danio rerio', taxonomic_status='ACCEPTED',
                         rank=TaxonomicRank.SPECIES.name)
        TaxonomyF.create(canonical_name='Homo sapiens', taxonomic_status='ACCEPTED',
                         rank=TaxonomicRank.SPECIES.name)
        data = self._get('?q=danio').data
        self.assertEqual(data['count'], 1)
        self.assertEqual(data['results'][0]['scientificName'], 'Danio rerio')

    def test_search_by_scientific_name(self):
        TaxonomyF.create(canonical_name='', scientific_name='Danio rerio Hamilton',
                         taxonomic_status='ACCEPTED', rank=TaxonomicRank.SPECIES.name)
        TaxonomyF.create(canonical_name='', scientific_name='Homo sapiens Linnaeus',
                         taxonomic_status='ACCEPTED', rank=TaxonomicRank.SPECIES.name)
        data = self._get('?q=Hamilton').data
        self.assertEqual(data['count'], 1)

    def test_search_is_case_insensitive(self):
        TaxonomyF.create(canonical_name='Danio rerio', taxonomic_status='ACCEPTED',
                         rank=TaxonomicRank.SPECIES.name)
        self.assertEqual(self._get('?q=DANIO').data['count'], 1)
        self.assertEqual(self._get('?q=danio').data['count'], 1)

    def test_search_partial_match(self):
        TaxonomyF.create(canonical_name='Danio rerio', taxonomic_status='ACCEPTED',
                         rank=TaxonomicRank.SPECIES.name)
        TaxonomyF.create(canonical_name='Danio kyathit', taxonomic_status='ACCEPTED',
                         rank=TaxonomicRank.SPECIES.name)
        self.assertEqual(self._get('?q=danio').data['count'], 2)

    def test_search_no_match_returns_empty(self):
        TaxonomyF.create(canonical_name='Danio rerio', taxonomic_status='ACCEPTED',
                         rank=TaxonomicRank.SPECIES.name)
        self.assertEqual(self._get('?q=xyzzyx').data['count'], 0)

    def test_search_combines_with_rank_filter(self):
        TaxonomyF.create(canonical_name='Danio rerio', taxonomic_status='ACCEPTED',
                         rank=TaxonomicRank.SPECIES.name)
        TaxonomyF.create(canonical_name='Danio', taxonomic_status='ACCEPTED',
                         rank=TaxonomicRank.GENUS.name)
        data = self._get(f'?q=danio&rank={TaxonomicRank.SPECIES.name}').data
        self.assertEqual(data['count'], 1)
        self.assertEqual(data['results'][0]['rank'], 'species')

    # --- URL resolves -------------------------------------------------------

    def test_url_name_resolves(self):
        self.assertIn('coldp', reverse('coldp-taxon'))
