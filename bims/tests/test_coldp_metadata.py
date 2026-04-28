import datetime

from django.urls import reverse
from django_tenants.test.cases import FastTenantTestCase
from rest_framework import status
from rest_framework.test import APIRequestFactory
from unittest.mock import patch

from bims.api_views.coldp import ColDPMetadataView
from bims.models.taxonomy_checklist import TaxonomyChecklist
from bims.tests.model_factories import DataSourceF, UserF


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
        self.assertIn('River Trust', self._get().data['citation'])

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
