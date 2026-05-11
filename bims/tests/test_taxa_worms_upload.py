# bims/tests/test_worms_taxa_upload.py
import os
import json

from django.core.files import File
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient
from mock import mock

from bims.models import TaxonGroupTaxonomy
from bims.tests.model_factories import (
    UploadSessionF,
    TaxonGroupF,
    UserF,
)
from bims.models import Taxonomy
from bims.scripts.taxa_upload_worms import WormsTaxaCSVUpload, WormsTaxaProcessor

test_data_directory = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'data'
)


class TestWormsTaxaUpload(FastTenantTestCase):

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.taxon_group = TaxonGroupF.create()
        self.owner = UserF.create(first_name='tester')

        with open(os.path.join(
            test_data_directory, 'worms_sample.csv'
        ), 'rb') as file:
            self.upload_session = UploadSessionF.create(
                uploader=self.owner,
                process_file=File(file),
                module_group=self.taxon_group
            )

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload_worms.preferences')
    def test_worms_upload_validated(self, mock_preferences, mock_finish):
        mock_finish.return_value = None
        mock_preferences.SiteSetting.auto_validate_taxa_on_upload = True

        uploader = WormsTaxaCSVUpload()
        uploader.upload_session = self.upload_session
        uploader.start('cp1252')

        self.assertEqual(uploader.error_list, [])

        # Species created with proper rank and author
        sp = Taxonomy.objects.get(
            canonical_name='[non-Uristidae] albinus',
            rank='SPECIES',
        )
        self.assertIn('1932', (sp.author or ''))
        self.assertIsNotNone(sp.parent)
        self.assertEqual(sp.parent.canonical_name, '[non-Uristidae]')
        self.assertEqual(sp.parent.rank, 'GENUS')

        # Marine habitat tag attached
        self.assertTrue(
            sp.tags.filter(name='Marine').exists()
        )

        # Source reference (citation) saved
        self.assertIsNotNone(sp.source_reference)
        self.assertIn('marinespecies.org', (sp.source_reference.note or ''))

        # Added to group as validated
        self.assertTrue(
            TaxonGroupTaxonomy.objects.filter(
                taxonomy=sp,
                taxongroup=self.taxon_group,
                is_validated=True
            ).exists()
        )

        # Subfamily row produces SUBFAMILY with FAMILY parent
        subfam = Taxonomy.objects.get(
            canonical_name='[unassigned] Cypraeidae',
            rank='SUBFAMILY',
        )
        self.assertIsNotNone(subfam.parent)
        self.assertEqual(subfam.parent.canonical_name, 'Cypraeidae')
        self.assertEqual(subfam.parent.rank, 'FAMILY')

        # Temporary name mapped
        temp_tax = Taxonomy.objects.get(
            canonical_name='[unassigned] Decapodiformes',
            rank='ORDER',
        )
        self.assertEqual(temp_tax.taxonomic_status, 'TEMPORARY NAME')

        # Accepted species has ACCEPTED status and no accepted_taxonomy link
        accepted = Taxonomy.objects.get(
            canonical_name='[non-Uristidae] dawsoni',
            rank='SPECIES',
        )
        self.assertEqual(accepted.taxonomic_status, 'ACCEPTED')
        self.assertIsNone(accepted.accepted_taxonomy)

        # Alternative representation -> synonym + accepted link created
        alt_rep = Taxonomy.objects.get(
            canonical_name='“Montereina” aurea',
            rank='SPECIES',
        )
        self.assertEqual(alt_rep.taxonomic_status, 'SYNONYM')
        self.assertIsNotNone(alt_rep.accepted_taxonomy)
        self.assertEqual(alt_rep.accepted_taxonomy.canonical_name, 'Peltodoris aurea')

        acostitrapa = Taxonomy.objects.filter(
            canonical_name='×Acostitrapa',
            rank='GENUS'
        )
        self.assertTrue(acostitrapa.exists())

        # Additional data keeps AphiaID (and other columns)
        extras = alt_rep.additional_data
        self.assertIn('AphiaID', extras)

        # Terrestrial-only -> Terrestrial tag present
        terr = Taxonomy.objects.get(
            canonical_name='[unassigned] Scolodontidae',
            rank='SUBFAMILY',
        )
        self.assertTrue(terr.tags.filter(name='Terrestrial').exists())

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload_worms.preferences')
    def test_worms_upload_unvalidated(self, mock_preferences, mock_finish):
        mock_finish.return_value = None
        mock_preferences.SiteSetting.auto_validate_taxa_on_upload = False

        with open(os.path.join(
            test_data_directory, 'worms_sample.csv'
        ), 'rb') as file:
            upload_session = UploadSessionF.create(
                uploader=self.owner,
                process_file=File(file),
                module_group=self.taxon_group
            )

        uploader = WormsTaxaCSVUpload()
        uploader.upload_session = upload_session
        uploader.start('utf-8')

        self.assertEqual(uploader.error_list, [])

        self.assertTrue(
            TaxonGroupTaxonomy.objects.filter(
                taxonomy__canonical_name='[non-Uristidae] albinus',
                taxongroup=self.taxon_group,
                is_validated=False
            ).exists()
        )

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload_worms.preferences')
    def test_worms_parent_reuse(self, mock_preferences, mock_finish):
        mock_finish.return_value = None
        mock_preferences.SiteSetting.auto_validate_taxa_on_upload = True

        uploader = WormsTaxaCSVUpload()
        uploader.upload_session = self.upload_session
        uploader.start('utf-8')

        self.assertEqual(uploader.error_list, [])

        albinus = Taxonomy.objects.get(
            canonical_name='[non-Uristidae] albinus',
            rank='SPECIES',
        )
        dawsoni = Taxonomy.objects.get(
            canonical_name='[non-Uristidae] dawsoni',
            rank='SPECIES',
        )
        self.assertIsNotNone(albinus.parent)
        self.assertEqual(albinus.parent_id, dawsoni.parent_id)
        self.assertEqual(albinus.parent.canonical_name, '[non-Uristidae]')
        self.assertEqual(albinus.parent.rank, 'GENUS')

    @mock.patch('bims.scripts.taxa_upload_worms.preferences')
    def test_worms_accepted_rank_and_parent_from_accepted_row(self, mock_preferences):
        mock_preferences.SiteSetting.auto_validate_taxa_on_upload = True

        class _Processor(WormsTaxaProcessor):
            def handle_error(self, row, message):
                raise AssertionError(message)

            def finish_processing_row(self, row, taxonomy):
                pass

            def fetch_accepted_row(self, accepted_aphia_id: int):
                if accepted_aphia_id != 2002:
                    raise AssertionError(accepted_aphia_id)
                return {
                    "AphiaID": 2002,
                    "ScientificName": "Neophron percnopterus ginginianus",
                    "Authority": "(Latham, 1790)",
                    "AphiaID_accepted": 2002,
                    "ScientificName_accepted": "Neophron percnopterus ginginianus",
                    "Authority_accepted": "(Latham, 1790)",
                    "Kingdom": "Animalia",
                    "Phylum": "Chordata",
                    "Class": "Aves",
                    "Order": "Accipitriformes",
                    "Family": "Accipitridae",
                    "Genus": "Neophron",
                    "Subgenus": "",
                    "Species": "percnopterus",
                    "Subspecies": "ginginianus",
                    "taxonRank": "subspecies",
                    "Marine": 0,
                    "Brackish": 0,
                    "Fresh": 0,
                    "Terrestrial": 1,
                    "taxonomicStatus": "accepted",
                    "Qualitystatus": "",
                    "Unacceptreason": "",
                    "DateLastModified": "",
                    "LSID": "",
                    "Parent AphiaID": 2001,
                    "Storedpath": "",
                    "Citation": "",
                }

        row = {
            "AphiaID": 1001,
            "ScientificName": "Vultur ginginianus",
            "Authority": "Latham, 1790",
            "AphiaID_accepted": 2002,
            "ScientificName_accepted": "Neophron percnopterus ginginianus",
            "Authority_accepted": "(Latham, 1790)",
            "Kingdom": "Animalia",
            "Phylum": "Chordata",
            "Class": "Aves",
            "Order": "Accipitriformes",
            "Family": "Accipitridae",
            "Genus": "Vultur",
            "Subgenus": "",
            "Species": "ginginianus",
            "Subspecies": "",
            "taxonRank": "species",
            "Marine": 0,
            "Brackish": 0,
            "Fresh": 0,
            "Terrestrial": 1,
            "taxonomicStatus": "unaccepted",
            "Qualitystatus": "",
            "Unacceptreason": "",
            "DateLastModified": "",
            "LSID": "",
            "Parent AphiaID": 1000,
            "Storedpath": "",
            "Citation": "",
        }

        processor = _Processor()
        processor.process_worms_data(row, self.taxon_group)

        synonym = Taxonomy.objects.get(
            canonical_name='Vultur ginginianus',
            rank='SPECIES',
        )
        self.assertIsNotNone(synonym.accepted_taxonomy)
        self.assertEqual(
            synonym.accepted_taxonomy.canonical_name,
            'Neophron percnopterus ginginianus'
        )
        self.assertEqual(synonym.accepted_taxonomy.rank, 'SUBSPECIES')
        self.assertEqual(synonym.accepted_taxonomy.parent.rank, 'SPECIES')
        self.assertEqual(
            synonym.accepted_taxonomy.parent.canonical_name,
            'Neophron percnopterus'
        )
        self.assertEqual(synonym.accepted_taxonomy.parent.parent.rank, 'GENUS')
        self.assertEqual(
            synonym.accepted_taxonomy.parent.parent.canonical_name,
            'Neophron'
        )

    # ------------------------------------------------------------------
    # aphia_id stored on taxonomy
    # ------------------------------------------------------------------

    @mock.patch('bims.scripts.taxa_upload_worms.preferences')
    def test_aphia_id_stored_on_taxonomy(self, mock_preferences):
        mock_preferences.SiteSetting.auto_validate_taxa_on_upload = True

        class _P(WormsTaxaProcessor):
            def handle_error(self, row, message): pass
            def finish_processing_row(self, row, taxonomy): pass

        row = {
            "AphiaID": 9999,
            "ScientificName": "Testus maximus",
            "Authority": "Smith, 2000",
            "AphiaID_accepted": 9999,
            "ScientificName_accepted": "Testus maximus",
            "Authority_accepted": "Smith, 2000",
            "Kingdom": "Animalia",
            "Phylum": "Chordata",
            "Class": "Aves",
            "Order": "Testiformes",
            "Family": "Testidae",
            "Genus": "Testus",
            "Subgenus": "",
            "Species": "maximus",
            "Subspecies": "",
            "taxonRank": "species",
            "taxonomicStatus": "accepted",
            "Marine": 0, "Brackish": 0, "Fresh": 1, "Terrestrial": 0,
            "Qualitystatus": "", "Unacceptreason": "",
            "DateLastModified": "", "LSID": "", "Parent AphiaID": "",
            "Storedpath": "", "Citation": "",
        }
        _P().process_worms_data(row, self.taxon_group)

        t = Taxonomy.objects.get(canonical_name='Testus maximus', rank='SPECIES')
        self.assertEqual(t.aphia_id, 9999)

    # ------------------------------------------------------------------
    # _lineage_species_name builds full binomial
    # ------------------------------------------------------------------

    def test_lineage_species_name_builds_binomial(self):
        p = WormsTaxaProcessor()
        row = {
            "Genus": "Cathartes",
            "Species": "burrovianus",   # epithet only
        }
        self.assertEqual(p._lineage_species_name(row), "Cathartes burrovianus")

    def test_lineage_species_name_already_binomial(self):
        """If Species already contains the genus prefix, don't double it."""
        p = WormsTaxaProcessor()
        row = {
            "Genus": "Cathartes",
            "Species": "Cathartes burrovianus",
        }
        self.assertEqual(p._lineage_species_name(row), "Cathartes burrovianus")

    def test_lineage_species_name_no_species(self):
        p = WormsTaxaProcessor()
        row = {"Genus": "Cathartes", "Species": ""}
        self.assertIsNone(p._lineage_species_name(row))

    # ------------------------------------------------------------------
    # UNACCEPTED → SYNONYM
    # ------------------------------------------------------------------

    @mock.patch('bims.scripts.taxa_upload_worms.preferences')
    def test_unaccepted_status_maps_to_synonym(self, mock_preferences):
        mock_preferences.SiteSetting.auto_validate_taxa_on_upload = True

        class _P(WormsTaxaProcessor):
            def handle_error(self, row, message): pass
            def finish_processing_row(self, row, taxonomy): pass

        row = {
            "AphiaID": 8001,
            "ScientificName": "Oldname antiquus",
            "Authority": "",
            "AphiaID_accepted": "",
            "ScientificName_accepted": "",
            "Authority_accepted": "",
            "Kingdom": "Animalia", "Phylum": "Chordata", "Class": "Aves",
            "Order": "Testiformes", "Family": "Testidae",
            "Genus": "Oldname", "Subgenus": "",
            "Species": "antiquus", "Subspecies": "",
            "taxonRank": "species",
            "taxonomicStatus": "unaccepted",
            "Marine": 0, "Brackish": 0, "Fresh": 0, "Terrestrial": 1,
            "Qualitystatus": "", "Unacceptreason": "",
            "DateLastModified": "", "LSID": "", "Parent AphiaID": "",
            "Storedpath": "", "Citation": "",
        }
        _P().process_worms_data(row, self.taxon_group)

        t = Taxonomy.objects.get(canonical_name='Oldname antiquus')
        self.assertEqual(t.taxonomic_status, 'SYNONYM')

    # ------------------------------------------------------------------
    # Subspecies parent chain: Species intermediate created correctly
    # ------------------------------------------------------------------

    @mock.patch('bims.scripts.taxa_upload_worms.preferences')
    def test_subspecies_gets_species_as_parent(self, mock_preferences):
        mock_preferences.SiteSetting.auto_validate_taxa_on_upload = True

        class _P(WormsTaxaProcessor):
            def handle_error(self, row, message): pass
            def finish_processing_row(self, row, taxonomy): pass

        row = {
            "AphiaID": 7001,
            "ScientificName": "Cathartes burrovianus urubutinga",
            "Authority": "(Latham, 1790)",
            "AphiaID_accepted": 7001,
            "ScientificName_accepted": "Cathartes burrovianus urubutinga",
            "Authority_accepted": "(Latham, 1790)",
            "Kingdom": "Animalia", "Phylum": "Chordata", "Class": "Aves",
            "Order": "Cathartiformes", "Family": "Cathartidae",
            "Genus": "Cathartes", "Subgenus": "",
            "Species": "burrovianus",   # epithet only — tests _lineage_species_name
            "Subspecies": "urubutinga",
            "taxonRank": "subspecies",
            "taxonomicStatus": "accepted",
            "Marine": 0, "Brackish": 0, "Fresh": 0, "Terrestrial": 1,
            "Qualitystatus": "", "Unacceptreason": "",
            "DateLastModified": "", "LSID": "", "Parent AphiaID": "",
            "Storedpath": "", "Citation": "",
        }
        _P().process_worms_data(row, self.taxon_group)

        ssp = Taxonomy.objects.get(
            canonical_name='Cathartes burrovianus urubutinga',
            rank='SUBSPECIES',
        )
        self.assertIsNotNone(ssp.parent)
        self.assertEqual(ssp.parent.rank, 'SPECIES')
        self.assertEqual(ssp.parent.canonical_name, 'Cathartes burrovianus')
        self.assertEqual(ssp.parent.parent.rank, 'GENUS')
        self.assertEqual(ssp.parent.parent.canonical_name, 'Cathartes')
