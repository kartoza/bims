# coding=utf-8
from django.test import TestCase
from django.contrib.sites.models import Site
from unittest.mock import patch, MagicMock

from django_tenants.test.cases import FastTenantTestCase
from datetime import datetime

from bims.factories import EntryFactory
from bims.tasks import get_source_reference_filter, generate_source_reference_filter
from bims.tests.model_factories import (
    SourceReferenceF,
    SourceReferenceBibliographyF,
    SourceReferenceDocumentF,
    SiteF
)


class TestSourceReferenceFilters(FastTenantTestCase):

    def setUp(self):
        # Set up data for testing
        self.site = SiteF.create()
        self.entry = EntryFactory.create(
            title='Test'
        )
        source_reference = SourceReferenceBibliographyF.create(
            source=self.entry,
            active_sites=(self.site.id,)
        )
        self.document = SourceReferenceDocumentF.create(
            active_sites=(self.site.id,)
        )
        self.references = [
            source_reference,
            self.document
        ]

    @patch('django.core.cache.cache.get')
    def test_get_source_reference_filter_populated_cache(self, mock_get):
        mock_get.return_value = [{'id': 1, 'reference': 'John Doe | 2020 | Study on BIMS', 'type': 'Peer-reviewed scientific article'}]

        result = get_source_reference_filter(request=None, tenant=self.tenant)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['id'], 1)
        self.assertEqual(result[0]['reference'], 'John Doe | 2020 | Study on BIMS')

    @patch('bims.models.SourceReference.objects.filter')
    @patch('django.core.cache.cache.set')
    @patch('django.contrib.sites.models.Site.objects.get_current', return_value=MagicMock(id=1))
    def test_generate_source_reference_filter(self, mock_get_current, mock_set, mock_filter):
        mock_filter.return_value.distinct.return_value = self.references

        generate_source_reference_filter(self.tenant.id)

        mock_set.assert_called_once_with(
            f'source_reference_filter_{self.tenant}',
            [
                {'id': self.references[0].id, 'reference': f'- | {self.entry.publication_date.year} | Test', 'type': 'Peer-reviewed scientific article'},
                {'id': self.references[1].id, 'reference': f'- | {datetime.now().year} | {self.document.source}', 'type': 'Published book, report or thesis'}]
            , timeout=None)
