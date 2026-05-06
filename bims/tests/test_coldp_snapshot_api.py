# coding=utf-8
"""Tests for GET /api/coldp/taxon/<checklist_uuid>/"""
import uuid

from django.urls import reverse
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient
from rest_framework import status

from bims.models.checklist_version import ChecklistVersion, ChecklistSnapshot
from bims.models.licence import Licence
from bims.tests.model_factories import TaxonGroupF, TaxonomyF, UserF, TaxonGroupTaxonomyF


def _licence():
    obj, _ = Licence.objects.get_or_create(
        identifier='CC-BY-4.0',
        defaults={
            'name': 'Creative Commons Attribution 4.0',
            'url': 'https://creativecommons.org/licenses/by/4.0/',
        },
    )
    return obj


def _make_published_version(taxon_group, version='1.0', user=None):
    cv = ChecklistVersion.objects.create(
        taxon_group=taxon_group,
        version=version,
        license=_licence(),
        status=ChecklistVersion.STATUS_DRAFT,
    )
    cv.publish(published_by=user)
    return cv


class TestColDPSnapshotView(FastTenantTestCase):

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.user = UserF.create()
        self.group = TaxonGroupF.create(name='Fish')

        self.taxon1 = TaxonomyF.create(scientific_name='Labeo umbratus', rank='SPECIES')
        self.taxon2 = TaxonomyF.create(scientific_name='Labeo capensis', rank='SPECIES')

        TaxonGroupTaxonomyF.create(
            taxongroup=self.group,
            taxonomy=self.taxon1,
            is_validated=True
        )

        TaxonGroupTaxonomyF.create(
            taxongroup=self.group,
            taxonomy=self.taxon2,
            is_validated=True
        )

        self.version = _make_published_version(self.group, version='1.0', user=self.user)

    def _url(self, pk):
        return reverse('coldp-snapshot', args=[pk])

    # ------------------------------------------------------------------
    # Basic responses
    # ------------------------------------------------------------------

    def test_returns_200_for_published_version(self):
        response = self.client.get(self._url(self.version.pk))
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_returns_404_for_unknown_uuid(self):
        response = self.client.get(self._url(uuid.uuid4()))
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_returns_404_for_draft_version(self):
        draft = ChecklistVersion.objects.create(
            taxon_group=self.group,
            version='2.0-draft',
            license=_licence(),
            status=ChecklistVersion.STATUS_DRAFT,
        )
        response = self.client.get(self._url(draft.pk))
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    # ------------------------------------------------------------------
    # Response structure
    # ------------------------------------------------------------------

    def test_response_contains_version_fields(self):
        response = self.client.get(self._url(self.version.pk))
        self.assertIn('version', response.data)
        self.assertIn('checklist_version_id', response.data)
        self.assertEqual(response.data['version'], '1.0')
        self.assertEqual(response.data['checklist_version_id'], str(self.version.pk))

    def test_response_is_paginated(self):
        response = self.client.get(self._url(self.version.pk))
        for key in ('count', 'next', 'previous', 'results'):
            self.assertIn(key, response.data)

    def test_result_count_matches_taxa(self):
        response = self.client.get(self._url(self.version.pk))
        self.assertEqual(response.data['count'], 2)

    def test_coldp_fields_present_in_row(self):
        response = self.client.get(self._url(self.version.pk))
        row = response.data['results'][0]
        for field in (
            'taxonID', 'parentID', 'basionymID', 'rank', 'scientificName',
            'authorship', 'status', 'nameStatus', 'kingdom', 'phylum', 'class',
            'order', 'family', 'genus', 'vernacularNames', 'distributions',
            'referenceID', 'remarks', 'changeType',
        ):
            self.assertIn(field, row, msg=f'Missing field: {field}')

    def test_taxon_id_matches_taxonomy_pk(self):
        response = self.client.get(self._url(self.version.pk))
        taxon_ids = {r['taxonID'] for r in response.data['results']}
        self.assertIn(str(self.taxon1.pk), taxon_ids)
        self.assertIn(str(self.taxon2.pk), taxon_ids)

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    def test_filter_by_rank(self):
        genus_taxon = TaxonomyF.create(scientific_name='Labeo', rank='GENUS')
        TaxonGroupTaxonomyF.create(
            taxongroup=self.group,
            taxonomy=genus_taxon,
            is_validated=True
        )
        # republish to include new taxon
        v2 = _make_published_version(self.group, version='2.0', user=self.user)

        response = self.client.get(self._url(v2.pk), {'rank': 'GENUS'})
        self.assertEqual(response.data['count'], 1)
        self.assertEqual(response.data['results'][0]['rank'], 'GENUS')

    def test_filter_by_change_type_added(self):
        response = self.client.get(
            self._url(self.version.pk), {'change_type': 'added'}
        )
        # First version — all rows are added
        self.assertEqual(response.data['count'], 2)
        for row in response.data['results']:
            self.assertEqual(row['changeType'], 'added')

    def test_filter_by_change_type_unchanged(self):
        v2 = _make_published_version(self.group, version='2.0', user=self.user)
        # Set previous_version so diff runs
        v2.previous_version = self.version
        v2.save(update_fields=['previous_version'])
        # Republish with previous_version set
        v3 = ChecklistVersion.objects.create(
            taxon_group=self.group,
            version='3.0',
            license=_licence(),
            previous_version=self.version,
            status=ChecklistVersion.STATUS_DRAFT,
        )
        v3.publish(published_by=self.user)

        response = self.client.get(
            self._url(v3.pk), {'change_type': 'unchanged'}
        )
        for row in response.data['results']:
            self.assertEqual(row['changeType'], 'unchanged')

    def test_filter_by_q_name(self):
        response = self.client.get(self._url(self.version.pk), {'q': 'umbratus'})
        self.assertEqual(response.data['count'], 1)
        self.assertEqual(
            response.data['results'][0]['scientificName'], 'Labeo umbratus'
        )

    def test_invalid_change_type_ignored(self):
        response = self.client.get(
            self._url(self.version.pk), {'change_type': 'banana'}
        )
        # Invalid value → no filter applied → all rows returned
        self.assertEqual(response.data['count'], 2)

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    def test_page_size_respected(self):
        response = self.client.get(self._url(self.version.pk), {'page_size': 1})
        self.assertEqual(len(response.data['results']), 1)
        self.assertIsNotNone(response.data['next'])
