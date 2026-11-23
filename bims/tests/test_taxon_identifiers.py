# coding=utf-8
"""Test Taxon Identifiers."""
import os
import mock
import json
from django.test import TestCase
from bims.utils.gbif import update_taxonomy_from_gbif, search_taxon_identifier

test_data_directory = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'data')


def mocked_gbif_request(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        @property
        def headers(self):
            return {
                'content-type': 'application/json'
            }

        def json(self):
            return self.json_data

        def raise_for_status(self):
            return self.status_code

    if 'params' in kwargs:
        key = kwargs['params']['q']
    else:
        url = args[0]
        url_splitted = url.split('/')
        key = url_splitted[len(url_splitted) - 1]

    response_file = 'gbif_species_%s.json' % key
    response_path = os.path.join(
            test_data_directory, response_file)

    if os.path.exists(response_path):
        response_data = open(response_path)
        json_data = json.load(response_data)
        response_data.close()
        return MockResponse(
            json_data,
            200
        )

    return MockResponse(None, 404)


class TestTaxonIdentifier(TestCase):
    """Test Taxon Identifier."""
    def setUp(self):
        self.gbif_key = 121

    @mock.patch('bims.utils.gbif.gbif_get', mock.Mock(
        side_effect=mocked_gbif_request))
    def test_process_taxon(self):
        taxon_identifier = update_taxonomy_from_gbif(self.gbif_key, False)
        scientific_name = 'Elasmobranchii'
        self.assertEqual(scientific_name, taxon_identifier.scientific_name)

    @mock.patch('bims.utils.gbif.gbif_get', mock.Mock(
        side_effect=mocked_gbif_request))
    def test_get_all_parent(self):
        taxon_identifier = update_taxonomy_from_gbif(self.gbif_key)
        parent = 0
        while taxon_identifier.parent:
            self.assertIsNotNone(taxon_identifier.parent)
            parent += 1
            taxon_identifier = taxon_identifier.parent
        self.assertTrue(parent == 2)

    @mock.patch('bims.utils.gbif.gbif_get', mock.Mock(
        side_effect=mocked_gbif_request))
    def test_search_taxon_identifier(self):
        species_name = 'Enteromius anoplus (Weber, 1897)'
        species = search_taxon_identifier(species_name, False)  # noqa
        # self.assertEqual(species_name, species.scientific_name)  # noqa

    @mock.patch('bims.utils.gbif.gbif_get', mock.Mock(
        side_effect=mocked_gbif_request))
    def test_get_children(self):
        taxon_identifier = update_taxonomy_from_gbif(self.gbif_key)
        parent = taxon_identifier.parent
        self.assertTrue(len(parent.get_direct_children()) > 0)
