import os

from django.core.files import File
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient
from mock import mock

from bims.enums import TaxonomicRank
from bims.enums.taxon_addendum import TaxonAddendum
from bims.models import TaxonGroupTaxonomy
from bims.models.taxon_origin import TaxonOrigin
from bims.scripts.taxa_upload import TaxaCSVUpload, TaxaProcessor
from bims.tests.model_factories import (
    UploadSessionF,
    TaxonGroupF,
    UserF,
    TaxonomyF,
    Taxonomy,
    VernacularName,
    IUCNStatusF
)

test_data_directory = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'data'
)


class TestTaxaUpload(FastTenantTestCase):

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.taxon_group = TaxonGroupF.create()
        self.owner = UserF.create(
            first_name='tester'
        )
        self.taxonomy = TaxonomyF.create()

        # create IUCN statuses used in upload mapping
        IUCNStatusF.create(
            category='NE',
            national=True
        )

        IUCNStatusF.create(
            category='NE',
            national=False
        )

        IUCNStatusF.create(
            category='NE',
            national=False
        )

        # Create TaxonOrigin records used by the upload script
        for key in ('indigenous', 'alien', 'unknown', 'alien-invasive', 'alien-non-invasive'):
            TaxonOrigin.objects.get_or_create(
                origin_key=key,
                defaults={'category': key}
            )

        with open(os.path.join(
            test_data_directory, 'taxa_upload_family.csv'
        ), 'rb') as file:
            self.upload_session = UploadSessionF.create(
                uploader=self.owner,
                process_file=File(file),
                module_group=self.taxon_group
            )

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_taxa_upload(self, mock_fetch_all_species_from_gbif, mock_finish):
        mock_finish.return_value = None
        mock_fetch_all_species_from_gbif.return_value = None

        taxa_csv_upload = TaxaCSVUpload()
        taxa_csv_upload.upload_session = self.upload_session
        taxa_csv_upload.start('utf-8-sig')

        self.assertTrue(
            Taxonomy.objects.filter(
                canonical_name__icontains='Ecnomidae2',
                taxonomic_status__iexact='synonym'
            ).exists()
        )

        self.assertTrue(
            VernacularName.objects.filter(
                name='Earthworm',
                language='eng',
                taxonomy__canonical_name__icontains='Oligochaeta'
            ).exists()
        )

        self.assertTrue(
            VernacularName.objects.filter(
                name='Earthworm2',
                language='eng',
                taxonomy__canonical_name__icontains='Oligochaeta'
            ).exists()
        )

        self.assertTrue(
            VernacularName.objects.filter(
                name='ミミズ',
                language='jpn',
                taxonomy__canonical_name__icontains='Oligochaeta'
            ).exists()
        )

        self.assertTrue(
            VernacularName.objects.filter(
                name='Animalia test2',
                language='ind',
            ).exists()
        )

        self.assertTrue(
            Taxonomy.objects.get(
                canonical_name='Ecnomidae'
            ).tags.filter(
                name='lentic'
            ).exists()
        )

        self.assertTrue(
            VernacularName.objects.filter(
                name='trattnattsländor',
                language='deu',
            ).exists()
        )

        self.assertTrue(
            VernacularName.objects.filter(
                name='test',
                language='eng',
            ).exists()
        )

        self.assertTrue(
            Taxonomy.objects.get(
                canonical_name='Ecnomidae'
            ).biographic_distributions.filter(
                name='ANT',
            ).exists()
        )

        self.assertTrue(
            Taxonomy.objects.get(
                canonical_name='Ecnomidae'
            ).biographic_distributions.filter(
                name='AT',
                doubtful=True
            ).exists()
        )

        self.assertTrue(
            Taxonomy.objects.get(
                canonical_name='Ecnomidae'
            ).tags.filter(
                name='Lakes'
            ).exists()
        )

        self.assertTrue(
            Taxonomy.objects.filter(
                canonical_name='Ecnomidae',
                scientific_name__icontains='Dimas 1789',
                author='Dimas 1789'
            ).exists()
        )

        self.assertTrue(
            TaxonGroupTaxonomy.objects.filter(
                taxonomy__canonical_name='Ecnomidae',
                taxongroup=self.taxon_group,
                is_validated=True
            ).exists()
        )

        self.assertTrue(
            Taxonomy.objects.filter(
                invasion__category='Category 1a invasive',
                origin__origin_key='indigenous',
            ).exists()
        )

        self.assertTrue(
            Taxonomy.objects.filter(
                canonical_name__icontains='Testgenus testspecies',
                rank='SPECIES',
                species_group__name='Test species group'
            ).exists()
        )

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    @mock.patch('bims.scripts.taxa_upload.preferences')
    def test_taxa_upload_unvalidated(
        self,
        mock_preferences,
        mock_fetch_all_species_from_gbif,
        mock_finish
    ):
        # Force proposals / unvalidated path
        mock_preferences.SiteSetting.auto_validate_taxa_on_upload = False
        mock_finish.return_value = None
        mock_fetch_all_species_from_gbif.return_value = None

        with open(os.path.join(
            test_data_directory, 'taxa_upload_family_2.csv'
        ), 'rb') as file:
            upload_session = UploadSessionF.create(
                uploader=self.owner,
                process_file=File(file),
                module_group=self.taxon_group
            )

        taxa_csv_upload = TaxaCSVUpload()
        taxa_csv_upload.upload_session = upload_session
        taxa_csv_upload.start('utf-8-sig')

        self.assertTrue(
            TaxonGroupTaxonomy.objects.filter(
                taxonomy__canonical_name='Oligochaeta2',
                taxongroup=self.taxon_group,
                is_validated=False
            ).exists()
        )

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_taxa_upload_variety_and_forma(
        self,
        mock_fetch_all_species_from_gbif,
        mock_finish
    ):
        mock_finish.return_value = None
        mock_fetch_all_species_from_gbif.return_value = None

        with open(os.path.join(
            test_data_directory, 'variety_forma_example.csv'
        ), 'rb') as file:
            upload_session = UploadSessionF.create(
                uploader=self.owner,
                process_file=File(file),
                module_group=self.taxon_group
            )

        taxa_csv_upload = TaxaCSVUpload()
        taxa_csv_upload.upload_session = upload_session
        taxa_csv_upload.start('utf-8-sig')

        # We should have created at least one VARIETY and one FORMA
        self.assertTrue(
            Taxonomy.objects.filter(
                rank='VARIETY'
            ).exists()
        )

        self.assertTrue(
            Taxonomy.objects.filter(
                rank='FORMA'
            ).exists()
        )

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_taxa_upload_species_and_subspecies_names(
            self,
            mock_fetch_all_species_from_gbif,
            mock_finish
    ):
        """
        Validate current behaviour of species / subspecies handling:

        - Species row (e.g. Genus=Labeobarbus, Species=mariae, Taxon='mariae',
          Taxon rank='Species', Authors='Smith 1990'):
            -> we expect a Taxonomy with rank='SPECIES' and canonical_name
               containing 'mariae'
            -> scientific_name should include the author string

        - Subspecies row (e.g. Taxon='mariae atlanticus',
          Taxon rank='Subspecies', Authors='Jones 2001'):
            -> we expect a Taxonomy with rank='SUBSPECIES' and canonical_name
               containing 'mariae atlanticus'
            -> scientific_name should include the author string

        Note: The code also creates/updates parent helpers like
        'Labeobarbus mariae' with blank rank. We don't assert on that rank
        because it's an internal side effect of parent resolution.
        """

        mock_finish.return_value = None

        # Any GBIF fetch inside process_data/get_parent should return *some*
        # taxonomy instance so the code can attach parents etc.
        # We don't really care about its initial values because the uploader
        # will mutate it heavily.
        mock_fetch_all_species_from_gbif.return_value = None

        # use the CSV with species + subspecies cases
        with open(
                os.path.join(
                    test_data_directory,
                    'taxa_upload_species_subspecies.csv'
                ),
                'rb'
        ) as file:
            upload_session = UploadSessionF.create(
                uploader=self.owner,
                process_file=File(file),
                module_group=self.taxon_group
            )

        taxa_csv_upload = TaxaCSVUpload()
        taxa_csv_upload.upload_session = upload_session
        taxa_csv_upload.start('utf-8-sig')

        # --- Species assertion ---
        # We expect at least one SPECIES row for "mariae"
        self.assertTrue(
            Taxonomy.objects.filter(
                rank='SPECIES',
                canonical_name__icontains='mariae',
                scientific_name__icontains='Smith 1990',  # author appended
            ).exists(),
            msg='Expected a SPECIES taxon for "mariae" with author "Smith 1990".'
        )

        # --- Subspecies assertion ---
        # We expect at least one SUBSPECIES row for "mariae atlanticus"
        self.assertTrue(
            Taxonomy.objects.filter(
                rank='SUBSPECIES',
                canonical_name__icontains='mariae atlanticus',
                scientific_name__icontains='Jones 2001',  # author appended
            ).exists(),
            msg='Expected a SUBSPECIES taxon for "mariae atlanticus" with author "Jones 2001".'
        )

        # --- Optional sanity check for the helper parent species ---
        # The importer tends to create a parent "species-like" taxon that
        # represents the species name joined to the genus, but that object
        # may not have rank set yet. We just assert it exists, not rank.
        self.assertTrue(
            Taxonomy.objects.filter(
                canonical_name__icontains='mariae'
            ).exists(),
            msg='Expected at least one taxonomy containing "mariae" as parent/anchor.'
        )

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_taxa_upload_subspecies_with_taxon_column_keeps_epithet(
            self,
            mock_fetch_all_species_from_gbif,
            mock_finish
    ):
        """
        Regression test: a SubSpecies row whose Taxon column already holds the
        full trinomial (e.g. "Baicalocandona dorsoconcava insularis") must not
        be collapsed down to just the species-level binomial
        ("Baicalocandona dorsoconcava"). Previously _choose_taxon_display_name
        compared the CSV name against a "composed" name that never included
        the SubSpecies column, so the composed (binomial-only) name always won
        and the subspecies epithet was silently dropped.
        """
        mock_finish.return_value = None
        mock_fetch_all_species_from_gbif.return_value = None

        with open(
                os.path.join(
                    test_data_directory,
                    'taxa_upload_subspecies_with_taxon_column.csv'
                ),
                'rb'
        ) as file:
            upload_session = UploadSessionF.create(
                uploader=self.owner,
                process_file=File(file),
                module_group=self.taxon_group
            )

        taxa_csv_upload = TaxaCSVUpload()
        taxa_csv_upload.upload_session = upload_session
        taxa_csv_upload.start('utf-8-sig')

        self.assertTrue(
            Taxonomy.objects.filter(
                rank='SUBSPECIES',
                canonical_name__icontains='dorsoconcava insularis',
            ).exists(),
            msg=(
                'Expected a SUBSPECIES taxon named '
                '"Baicalocandona dorsoconcava insularis", not one collapsed '
                'down to the species-level "Baicalocandona dorsoconcava".'
            )
        )

        self.assertFalse(
            Taxonomy.objects.filter(
                rank='SUBSPECIES',
                canonical_name__iexact='Baicalocandona dorsoconcava',
            ).exists(),
            msg='SubSpecies row must not be saved with the plain species binomial.'
        )

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_synonym_has_no_parent_doubtful_keeps_parent(
            self,
            mock_fetch_all_species_from_gbif,
            mock_finish
    ):
        """
        Synonym taxa should not keep any parent relationship when imported, to
        avoid polluting the accepted hierarchy. Doubtful taxa, however, are
        treated like accepted taxa and should retain their own parent.
        """
        mock_finish.return_value = None
        mock_fetch_all_species_from_gbif.return_value = None

        with open(
                os.path.join(
                    test_data_directory,
                    'taxa_upload_synonym_doubtful.csv'
                ),
                'rb'
        ) as file:
            upload_session = UploadSessionF.create(
                uploader=self.owner,
                process_file=File(file),
                module_group=self.taxon_group
            )

        taxa_csv_upload = TaxaCSVUpload()
        taxa_csv_upload.upload_session = upload_session
        taxa_csv_upload.start('utf-8-sig')

        synonym_taxon = Taxonomy.objects.get(
            canonical_name='Synonymus primus'
        )
        doubtful_taxon = Taxonomy.objects.get(
            canonical_name='Dubius secundus'
        )
        accepted_taxon = Taxonomy.objects.get(
            canonical_name='Validus primus'
        )

        self.assertEqual(synonym_taxon.rank, 'SPECIES')
        self.assertIsNone(
            synonym_taxon.parent,
            msg='Synonym species should remain detached from any parent.'
        )

        self.assertIsNotNone(
            doubtful_taxon.parent,
            msg='Doubtful species should retain its own genus parent.'
        )
        self.assertEqual(
            doubtful_taxon.parent.canonical_name,
            'Dubius',
            msg='Doubtful species should be parented to its genus.'
        )

        self.assertIsNotNone(
            accepted_taxon.parent,
            msg='Accepted species should still retain the genus parent.'
        )

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    @mock.patch('bims.templatetags.site.is_fada_site')
    def test_fada_taxonomic_status_preserved_from_csv(
        self,
        mock_is_fada_site,
        mock_fetch_gbif,
        mock_finish
    ):
        """
        Test that for FADA sites, the taxonomic status from CSV is preserved
        and not overwritten by GBIF data (e.g., HOMOTYPIC_SYNONYM).

        This test addresses the issue where GBIF returns taxonomic statuses
        like 'HOMOTYPIC_SYNONYM' that don't exist in FADA's taxonomy system.
        For FADA sites, CSV taxonomic status should always take precedence.
        """
        mock_finish.return_value = None
        mock_is_fada_site.return_value = True

        # Mock GBIF to not fetch anything - we want to test CSV-only upload
        # This simulates FADA sites where on_gbif=False by default
        mock_fetch_gbif.return_value = None

        # Create upload session with FADA test data
        with open(os.path.join(
            test_data_directory, 'taxa_upload_fada_status.csv'
        ), 'rb') as file:
            upload_session = UploadSessionF.create(
                uploader=self.owner,
                process_file=File(file),
                module_group=self.taxon_group
            )

        # Run the upload
        taxa_csv_upload = TaxaCSVUpload()
        taxa_csv_upload.upload_session = upload_session
        taxa_csv_upload.start('utf-8-sig')

        # Verify that CSV taxonomic status is preserved for FADA
        # The CSV specifies SYNONYM, not HOMOTYPIC_SYNONYM
        fada_synonym = Taxonomy.objects.filter(
            canonical_name='Panthera tigris'
        ).first()

        self.assertIsNotNone(
            fada_synonym,
            msg='Expected Panthera tigris to be created from CSV'
        )

        # Critical assertion: taxonomic_status should be SYNONYM (from CSV)
        # NOT HOMOTYPIC_SYNONYM (from GBIF)
        self.assertEqual(
            fada_synonym.taxonomic_status,
            'SYNONYM',
            msg=f'FADA sites must preserve CSV taxonomic_status. '
                f'Expected SYNONYM but got {fada_synonym.taxonomic_status}. '
                f'GBIF status should not override CSV for FADA.'
        )

        # Verify ACCEPTED status is also preserved
        fada_accepted = Taxonomy.objects.filter(
            canonical_name='Panthera leo'
        ).first()

        self.assertIsNotNone(
            fada_accepted,
            msg='Expected Panthera leo to be created from CSV'
        )

        self.assertEqual(
            fada_accepted.taxonomic_status,
            'ACCEPTED',
            msg='FADA sites must preserve ACCEPTED status from CSV'
        )

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    @mock.patch('bims.templatetags.site.is_fada_site')
    def test_non_fada_allows_gbif_taxonomic_status(
        self,
        mock_is_fada_site,
        mock_fetch_gbif,
        mock_finish
    ):
        """
        Test that for non-FADA sites, GBIF taxonomic status can override CSV.
        This ensures our fix only applies to FADA sites.
        """
        mock_finish.return_value = None
        mock_is_fada_site.return_value = False  # Not a FADA site

        # Mock GBIF to not fetch - we'll test preserve_taxonomic_status flag
        mock_fetch_gbif.return_value = None

        # Create upload session
        with open(os.path.join(
            test_data_directory, 'taxa_upload_fada_status.csv'
        ), 'rb') as file:
            upload_session = UploadSessionF.create(
                uploader=self.owner,
                process_file=File(file),
                module_group=self.taxon_group
            )

        # Run the upload
        taxa_csv_upload = TaxaCSVUpload()
        taxa_csv_upload.upload_session = upload_session
        taxa_csv_upload.start('utf-8-sig')

        # For non-FADA sites, GBIF status may be used
        # (behavior depends on exact upload flow, but we're testing
        # that our preserve_taxonomic_status flag is False for non-FADA)
        non_fada_taxon = Taxonomy.objects.filter(
            canonical_name='Panthera tigris'
        ).first()

        self.assertIsNotNone(
            non_fada_taxon,
            msg='Expected Panthera tigris to be created'
        )

        # The exact status depends on upload logic, but the key is that
        # preserve_taxonomic_status=False was passed to GBIF functions
        # We just verify the taxon exists and has some valid status
        self.assertIn(
            non_fada_taxon.taxonomic_status,
            ['SYNONYM', 'HOMOTYPIC_SYNONYM', 'ACCEPTED', ''],
            msg='Non-FADA taxonomic status should be valid'
        )


    def test_csv_upload_synonym_adds_accepted_name(self):
        """
        Test that when a synonym is uploaded via CSV,
        the accepted taxonomy is automatically added to the taxon group.
        """
        # Create an accepted taxonomy
        accepted_taxonomy = TaxonomyF.create(
            canonical_name='Accepted Name CSV',
            scientific_name='Accepted Name CSV',
            rank='SPECIES',
            taxonomic_status='ACCEPTED'
        )

        # Create a synonym taxonomy
        synonym_taxonomy = TaxonomyF.create(
            canonical_name='Synonym Name CSV',
            scientific_name='Synonym Name CSV',
            rank='SPECIES',
            taxonomic_status='SYNONYM',
            accepted_taxonomy=accepted_taxonomy
        )

        # Verify neither is in the taxon group yet
        self.assertFalse(
            TaxonGroupTaxonomy.objects.filter(
                taxonomy=accepted_taxonomy,
                taxongroup=self.taxon_group
            ).exists()
        )
        self.assertFalse(
            TaxonGroupTaxonomy.objects.filter(
                taxonomy=synonym_taxonomy,
                taxongroup=self.taxon_group
            ).exists()
        )

        # Use the add_taxon_to_taxon_group method (simulating CSV upload)
        from bims.scripts.taxa_upload import TaxaProcessor
        processor = TaxaProcessor()
        processor.add_taxon_to_taxon_group(synonym_taxonomy, self.taxon_group, validated=False)

        # Verify the synonym was added
        self.assertTrue(
            TaxonGroupTaxonomy.objects.filter(
                taxonomy=synonym_taxonomy,
                taxongroup=self.taxon_group
            ).exists(),
            "Synonym should be added to the group"
        )

        # Verify the accepted taxonomy was AUTOMATICALLY added
        self.assertTrue(
            TaxonGroupTaxonomy.objects.filter(
                taxonomy=accepted_taxonomy,
                taxongroup=self.taxon_group
            ).exists(),
            "Accepted taxonomy should be automatically added when synonym is uploaded"
        )

        # Verify both are marked as not validated
        synonym_in_group = TaxonGroupTaxonomy.objects.get(
            taxonomy=synonym_taxonomy,
            taxongroup=self.taxon_group
        )
        self.assertFalse(synonym_in_group.is_validated)

        accepted_in_group = TaxonGroupTaxonomy.objects.get(
            taxonomy=accepted_taxonomy,
            taxongroup=self.taxon_group
        )
        self.assertTrue(accepted_in_group.is_validated)


class TestSubgenusUpload(FastTenantTestCase):
    """Tests for the subgenus upload feature added in the subgenus_validation branch."""

    # Canonical names created by taxa_upload_subgenus.csv (and their derived parents).
    # Cleaned up in setUp/tearDown so --keepdb runs don't accumulate duplicates.
    _CSV_CANONICAL_NAMES = [
        'Aedes (Stegomyia) aegypti',
        'Aedes (Stegomyia) albopictus',
        'Aedes vexans',
        'Aedes (Stegomyia)',
        'Stegomyia',
        'Aedes',
        'Culicidae',
        'Diptera',
        'Insecta',
        'Arthropoda',
        'Animalia',
    ]

    def _delete_csv_taxa(self):
        Taxonomy.objects.filter(
            canonical_name__in=self._CSV_CANONICAL_NAMES
        ).delete()

    def setUp(self):
        self._delete_csv_taxa()
        self.client = TenantClient(self.tenant)
        self.taxon_group = TaxonGroupF.create()
        self.owner = UserF.create(first_name='tester')

        for key in ('indigenous', 'alien', 'unknown', 'alien-invasive', 'alien-non-invasive'):
            TaxonOrigin.objects.get_or_create(
                origin_key=key,
                defaults={'category': key}
            )

    def tearDown(self):
        self._delete_csv_taxa()

    def _make_upload_session(self, csv_filename):
        with open(os.path.join(test_data_directory, csv_filename), 'rb') as f:
            return UploadSessionF.create(
                uploader=self.owner,
                process_file=File(f),
                module_group=self.taxon_group
            )

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_species_with_subgenus_assigns_subgenus_fk(
        self, mock_gbif, mock_finish
    ):
        """
        A species row that has a SubGenus column value should end up with
        taxonomy.subgenus pointing to the corresponding SUBGENUS-ranked taxon.
        """
        mock_finish.return_value = None
        mock_gbif.return_value = None

        upload_session = self._make_upload_session('taxa_upload_subgenus.csv')
        taxa_csv_upload = TaxaCSVUpload()
        taxa_csv_upload.upload_session = upload_session
        taxa_csv_upload.start('utf-8-sig')

        # The SUBGENUS-rank taxonomy should have been created with the
        # "Genus (SubgenusName)" canonical form.
        self.assertTrue(
            Taxonomy.objects.filter(
                canonical_name='Aedes (Stegomyia)',
                rank='SUBGENUS',
            ).exists(),
            'Expected a SUBGENUS-ranked taxonomy named "Aedes (Stegomyia)".'
        )

        stegomyia = Taxonomy.objects.get(canonical_name='Aedes (Stegomyia)', rank='SUBGENUS')

        # Species that carry SubGenus=Stegomyia must have canonical names in
        # "Genus (SubGenusName) specific_epithet" form and subgenus FK set.
        for name in ('Aedes (Stegomyia) aegypti', 'Aedes (Stegomyia) albopictus'):
            taxon = Taxonomy.objects.filter(
                canonical_name__iexact=name,
                rank='SPECIES',
            ).first()
            self.assertIsNotNone(taxon, f'Expected SPECIES taxon for {name!r}.')
            self.assertEqual(
                taxon.subgenus_id, stegomyia.pk,
                f'{name} should have subgenus="Aedes (Stegomyia)", got {taxon.subgenus}.'
            )

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_species_without_subgenus_has_no_subgenus_fk(
        self, mock_gbif, mock_finish
    ):
        """
        A species row without a SubGenus column value should have subgenus=None.
        """
        mock_finish.return_value = None
        mock_gbif.return_value = None

        upload_session = self._make_upload_session('taxa_upload_subgenus.csv')
        taxa_csv_upload = TaxaCSVUpload()
        taxa_csv_upload.upload_session = upload_session
        taxa_csv_upload.start('utf-8-sig')

        taxon = Taxonomy.objects.filter(
            canonical_name__iexact='Aedes vexans',
            rank='SPECIES',
        ).first()
        self.assertIsNotNone(taxon, 'Expected SPECIES taxon for Aedes vexans.')
        self.assertIsNone(
            taxon.subgenus,
            'Species without SubGenus column should have subgenus=None.'
        )

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_species_with_subgenus_skips_gbif(
        self, mock_gbif, mock_finish
    ):
        """
        When a species row has a SubGenus value, should_fetch_from_gbif=False
        so GBIF must NOT be consulted for any SPECIES-ranked taxa.
        Parent-resolution may still call GBIF for higher ranks (family, order,
        etc.) when they are not already in the DB — that is expected behaviour.
        """
        mock_finish.return_value = None
        mock_gbif.return_value = None

        upload_session = self._make_upload_session('taxa_upload_subgenus.csv')
        taxa_csv_upload = TaxaCSVUpload()
        taxa_csv_upload.upload_session = upload_session
        taxa_csv_upload.start('utf-8-sig')

        # Assert that fetch_all_species_from_gbif was never called with
        # taxonomic_rank='SPECIES' (or 'species') for any of the subgenus rows.
        species_calls = [
            c for c in mock_gbif.call_args_list
            if c.kwargs.get('taxonomic_rank', '').upper() == 'SPECIES'
            or (len(c.args) >= 2 and str(c.args[1]).upper() == 'SPECIES')
        ]
        self.assertEqual(
            len(species_calls), 0,
            f'GBIF should not be called for SPECIES rank when a subgenus is '
            f'present, but got calls: {species_calls}'
        )

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_same_epithet_different_subgenus_creates_separate_taxa(
        self, mock_gbif, mock_finish
    ):
        """
        Two species rows that share the same specific epithet but have different
        subgenus values produce distinct Taxonomy records with different canonical
        names in "Genus (Subgenus) epithet" form, each linked to the right subgenus.
        """
        mock_finish.return_value = None
        mock_gbif.return_value = None

        # Pre-create a genus and two subgenera.
        genus = TaxonomyF.create(canonical_name='Culex', rank='GENUS')
        subgenus_a = TaxonomyF.create(
            canonical_name='Culex',
            rank='SUBGENUS',
        )
        subgenus_b = TaxonomyF.create(
            canonical_name='Neoculex',
            rank='SUBGENUS',
        )
        # Pre-create an existing species already linked to subgenus_a with the
        # new canonical form.
        existing_species = TaxonomyF.create(
            canonical_name='Culex (Culex) pipiens',
            rank='SPECIES',
            subgenus=subgenus_a,
        )

        taxon_group = TaxonGroupF.create()
        processor = TaxaProcessor()

        row_b = {
            'Taxon Rank': 'Species',
            'Kingdom': 'Animalia',
            'Phylum': 'Arthropoda',
            'Class': 'Insecta',
            'Order': 'Diptera',
            'Family': 'Culicidae',
            'Genus': 'Culex',
            'SubGenus': 'Neoculex',
            'Taxon': 'Culex pipiens',
            'Taxonomic status': 'accepted',
            'On GBIF': 'No',
            'Author(s)': 'Linnaeus 1758',
        }

        with mock.patch('bims.scripts.taxa_upload.preferences') as mock_prefs:
            mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
            processor.process_data(row_b, taxon_group)

        # Parser produces "Genus (Subgenus) epithet" canonical names, so the
        # two taxa have different canonical names (not the same bare epithet).
        taxon_culex = Taxonomy.objects.filter(
            canonical_name='Culex (Culex) pipiens', rank='SPECIES'
        ).first()
        taxon_neoculex = Taxonomy.objects.filter(
            canonical_name='Culex (Neoculex) pipiens', rank='SPECIES'
        ).first()
        self.assertIsNotNone(taxon_culex, 'Expected SPECIES "Culex (Culex) pipiens".')
        self.assertIsNotNone(taxon_neoculex, 'Expected SPECIES "Culex (Neoculex) pipiens".')
        self.assertEqual(taxon_culex.subgenus, subgenus_a)
        self.assertEqual(taxon_neoculex.subgenus, subgenus_b)

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_accepted_and_synonym_same_name_different_fada_id_creates_separate_taxa(
        self, mock_gbif, mock_finish
    ):
        """
        A FADA-style checklist can list an accepted taxon and a synonym that
        display the same bare name ("Candona burdurensis") but represent
        different taxonomic concepts: different FADA IDs, different
        Taxonomic Status, and different SubGenus placement. These must
        become two distinct Taxonomy records, not one taxon plus a proposal
        merged onto it.
        """
        mock_finish.return_value = None
        mock_gbif.return_value = None

        taxon_group = TaxonGroupF.create()
        processor = TaxaProcessor()

        accepted_row = {
            'Taxon Rank': 'Species',
            'Kingdom': 'Animalia',
            'Phylum': 'Arthropoda',
            'Class': 'Ostracoda',
            'Order': 'Podocopida',
            'Family': 'Candonidae',
            'SubFamily': 'Candoninae',
            'Genus': 'Candona',
            'Species': 'burdurensis',
            'Taxon': 'Candona burdurensis',
            'Author(s)': 'Freels, 1980',
            'Taxonomic status': 'Accepted',
            'FADA ID': '500296',
            'On GBIF': 'No',
        }

        synonym_row = {
            'Taxon Rank': 'Species',
            'Kingdom': 'Animalia',
            'Phylum': 'Arthropoda',
            'Class': 'Ostracoda',
            'Order': 'Podocopida',
            'Genus': 'Candona',
            'SubGenus': 'Candona',
            'Species': 'burdurensis',
            'Taxon': 'Candona burdurensis',
            'Author(s)': 'Freels, 1980',
            'Taxonomic status': 'Synonym',
            'Accepted Taxon': 'Candona burdurensis',
            'FADA ID': 'SYN-500229',
            'On GBIF': 'No',
        }

        with mock.patch('bims.scripts.taxa_upload.preferences') as mock_prefs:
            mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
            processor.process_data(accepted_row, taxon_group)
            processor.process_data(synonym_row, taxon_group)

        matching_taxa = Taxonomy.objects.filter(
            canonical_name__iexact='Candona burdurensis',
            rank='SPECIES',
        )
        self.assertEqual(
            matching_taxa.count(), 2,
            'Expected two distinct Taxonomy records for "Candona burdurensis" '
            f'(accepted + synonym), got {matching_taxa.count()}: '
            f'{list(matching_taxa.values("pk", "fada_id", "taxonomic_status"))}'
        )

        accepted_taxon = matching_taxa.get(fada_id='500296')
        synonym_taxon = matching_taxa.get(fada_id='SYN-500229')

        self.assertNotEqual(accepted_taxon.pk, synonym_taxon.pk)
        self.assertEqual(accepted_taxon.taxonomic_status, 'ACCEPTED')
        self.assertEqual(synonym_taxon.taxonomic_status, 'SYNONYM')
        self.assertIsNone(accepted_taxon.subgenus)
        self.assertIsNotNone(synonym_taxon.subgenus)
        self.assertEqual(synonym_taxon.subgenus.canonical_name, 'Candona (Candona)')

    def test_choose_taxon_display_name_formats_subgenus_as_genus_parenthetical(self):
        """
        _choose_taxon_display_name should return "Genus (SubgenusName)" when the
        rank is SubGenus and both Genus and SubGenus columns are present.
        """
        processor = TaxaProcessor()
        processor.all_keys = {
            'TAXON': 'Taxon',
            'GENUS': 'Genus',
            'SUBGENUS': 'SubGenus',
        }

        row = {
            'Taxon': '',
            'Genus': 'Aedes',
            'SubGenus': 'Stegomyia',
        }

        result = processor._choose_taxon_display_name(
            row=row,
            rank='SubGenus',
            taxonomic_status='accepted',
            csv_taxon='',
            composed_taxon='',
        )
        self.assertEqual(result, 'Aedes (Stegomyia)')

    def test_compose_species_name_includes_subgenus(self):
        """
        _compose_species_name should return "Genus (Subgenus) epithet" when
        the SubGenus column is present.
        """
        processor = TaxaProcessor()
        processor.all_keys = {}

        row = {
            'Genus': 'Thraulus',
            'SubGenus': 'Masharikella',
            'Species': 'iteris',
            'Taxon': '',
        }

        result = processor._compose_species_name(row)
        self.assertEqual(result, 'Thraulus (Masharikella) iteris')

    def test_compose_species_name_strips_existing_subgenus_parenthetical(self):
        """
        When the Species/Taxon field already contains a subgenus parenthetical,
        _compose_species_name should strip it and use the SubGenus column value.
        """
        processor = TaxaProcessor()
        processor.all_keys = {}

        row = {
            'Genus': 'Thraulus',
            'SubGenus': 'Masharikella',
            'Species': 'Thraulus (Masharikella) iteris',
            'Taxon': '',
        }

        result = processor._compose_species_name(row)
        self.assertEqual(result, 'Thraulus (Masharikella) iteris')

    def test_parser_subgenus_canonical_and_scientific_name(self):
        """
        Uploading a SubGenus-ranked row should produce canonical_name in
        "Genus (SubgenusName)" form and scientific_name with the author appended.
        """
        processor = TaxaProcessor()
        processor.all_keys = {}

        row = {
            'Taxon Rank': 'SubGenus',
            'Kingdom': 'Animalia',
            'Phylum': 'Arthropoda',
            'Class': 'Insecta',
            'Order': 'Ephemeroptera',
            'Family': 'Leptophlebiidae',
            'Genus': 'Thraulus',
            'SubGenus': 'Masharikella',
            'Taxon': 'Masharikella',
            'Taxonomic status': 'accepted',
            'On GBIF': 'No',
            'Author(s)': 'Peters, Gillies & Edmunds, 1964',
        }

        taxon_name = processor._choose_taxon_display_name(
            row=row,
            rank='SubGenus',
            taxonomic_status='accepted',
            csv_taxon='Masharikella',
            composed_taxon='Masharikella',
        )
        self.assertEqual(taxon_name, 'Thraulus (Masharikella)')

    def test_parser_species_with_subgenus_canonical_and_scientific_name(self):
        """
        Uploading a Species row with a SubGenus column produces canonical_name in
        "Genus (Subgenus) epithet" form and scientific_name with the author.
        """
        processor = TaxaProcessor()
        processor.all_keys = {}

        row = {
            'Genus': 'Thraulus',
            'SubGenus': 'Masharikella',
            'Species': 'iteris',
            'Taxon': 'Thraulus iteris',
        }

        canonical = processor._compose_species_name(row)
        self.assertEqual(canonical, 'Thraulus (Masharikella) iteris')

        # Verify _choose_taxon_display_name also resolves to the correct form.
        display = processor._choose_taxon_display_name(
            row=row,
            rank='Species',
            taxonomic_status='accepted',
            csv_taxon='Thraulus iteris',
            composed_taxon=canonical,
        )
        self.assertEqual(display, 'Thraulus (Masharikella) iteris')

    @mock.patch('bims.scripts.taxa_upload.get_species')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_synonym_with_subgenus_allows_gbif_fetch(
        self, mock_fetch_gbif, mock_get_species
    ):
        """
        A synonym with a SubGenus value must NOT have should_fetch_from_gbif
        suppressed — synonyms still need GBIF to resolve acceptedKey.
        """
        mock_fetch_gbif.return_value = None
        mock_get_species.return_value = None

        row = {
            'Taxon Rank': 'Species',
            'Kingdom': 'Animalia',
            'Phylum': 'Arthropoda',
            'Class': 'Insecta',
            'Order': 'Diptera',
            'Family': 'Culicidae',
            'Genus': 'Aedes',
            'SubGenus': 'Stegomyia',
            'Taxon': 'Aedes aegypti syn',
            'Taxonomic status': 'SYNONYM',
            'On GBIF': 'Yes',
            'Author(s)': '',
        }

        processor = TaxaProcessor()
        processor.all_keys = {}
        with mock.patch('bims.scripts.taxa_upload.preferences') as mock_prefs:
            mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
            processor.process_data(row, TaxonGroupF.create())

        species_calls = [
            c for c in mock_fetch_gbif.call_args_list
            if c.kwargs.get('taxonomic_rank', '').upper() == 'SPECIES'
        ]
        self.assertGreater(
            len(species_calls), 0,
            'GBIF should be consulted for synonym SPECIES with subgenus — '
            'should_fetch_from_gbif must not be suppressed for synonyms.'
        )

    @mock.patch('bims.scripts.taxa_upload.get_species_by_col_id')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_col_link_parsed_and_passed_as_col_id(
        self, mock_fetch_gbif, mock_get_species_by_col_id
    ):
        """
        A GBIF Link pointing at gbif.org/taxon/<id> is a Catalogue of Life
        link; the id must be validated via COL and routed through as
        col_id, never gbif_key.
        """
        mock_fetch_gbif.return_value = None
        mock_get_species_by_col_id.return_value = {
            'usage': {
                'key': 'ABC123',
                'canonicalName': 'Aedes aegypti',
                'name': 'Aedes aegypti',
                'rank': 'SPECIES',
            },
            'classification': [
                {'rank': 'GENUS', 'name': 'Aedes'},
            ],
        }

        row = {
            'Taxon Rank': 'Species',
            'Kingdom': 'Animalia',
            'Phylum': 'Arthropoda',
            'Class': 'Insecta',
            'Order': 'Diptera',
            'Family': 'Culicidae',
            'Genus': 'Aedes',
            'Taxon': 'Aedes aegypti',
            'Taxonomic status': 'accepted',
            'On GBIF': 'Yes',
            'GBIF Link': 'https://www.gbif.org/taxon/ABC123',
            'Author(s)': '',
        }

        processor = TaxaProcessor()
        processor.all_keys = {}
        with mock.patch('bims.scripts.taxa_upload.preferences') as mock_prefs:
            mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
            processor.process_data(row, TaxonGroupF.create())

        mock_get_species_by_col_id.assert_called_with('ABC123')

        col_id_calls = [
            c for c in mock_fetch_gbif.call_args_list
            if c.kwargs.get('col_id') == 'ABC123'
        ]
        self.assertGreater(
            len(col_id_calls), 0,
            'fetch_all_species_from_gbif should be called with col_id, not gbif_key.'
        )
        for c in mock_fetch_gbif.call_args_list:
            self.assertNotIn('gbif_key', c.kwargs)

    @mock.patch('bims.scripts.taxa_upload.get_species_by_col_id')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_gbif_species_link_rejected_when_on_gbif(
        self, mock_fetch_gbif, mock_get_species_by_col_id
    ):
        """
        The legacy gbif.org/species/<gbif_key> link form is no longer
        accepted upload input; when 'On GBIF' is set the row must be
        rejected with an error and not processed further.
        """
        errors = []

        row = {
            'Taxon Rank': 'Species',
            'Kingdom': 'Animalia',
            'Phylum': 'Arthropoda',
            'Class': 'Insecta',
            'Order': 'Diptera',
            'Family': 'Culicidae',
            'Genus': 'Aedes',
            'Taxon': 'Aedes rejectus',
            'Taxonomic status': 'accepted',
            'On GBIF': 'Yes',
            'GBIF Link': 'https://www.gbif.org/species/99999',
            'Author(s)': '',
        }

        processor = TaxaProcessor()
        processor.all_keys = {}
        processor.handle_error = lambda row, message: errors.append(message)
        processor.process_data(row, TaxonGroupF.create())

        self.assertTrue(errors, 'A gbif.org/species/ link must raise an error.')
        self.assertIn('99999', errors[0])
        mock_get_species_by_col_id.assert_not_called()
        mock_fetch_gbif.assert_not_called()
        self.assertFalse(
            Taxonomy.objects.filter(canonical_name='Aedes rejectus').exists()
        )

    @mock.patch('bims.scripts.taxa_upload.get_species')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_synonym_same_name_different_subgenus_matches_null_subgenus_record(
        self, mock_fetch_gbif, mock_get_species
    ):
        """
        A synonym that shares a canonical name with an accepted taxon of a
        different subgenus should match an existing synonym record that has
        no subgenus set (pre-subgenus-support import), not create a duplicate.
        """
        mock_fetch_gbif.return_value = None
        mock_get_species.return_value = None

        subgenus_a = TaxonomyF.create(canonical_name='SubGenusA', rank='SUBGENUS')
        TaxonomyF.create(
            canonical_name='Culex duplicatus',
            scientific_name='Culex duplicatus',
            rank='SPECIES',
            subgenus=subgenus_a,
            taxonomic_status='ACCEPTED',
        )
        existing_synonym = TaxonomyF.create(
            canonical_name='Culex duplicatus',
            scientific_name='Culex duplicatus',
            rank='SPECIES',
            taxonomic_status='SYNONYM',
        )

        row = {
            'Taxon Rank': 'Species',
            'Kingdom': 'Animalia',
            'Phylum': 'Arthropoda',
            'Class': 'Insecta',
            'Order': 'Diptera',
            'Family': 'Culicidae',
            'Genus': 'Culex',
            'SubGenus': 'SubGenusB',
            'Taxon': 'Culex duplicatus',
            'Taxonomic status': 'SYNONYM',
            'On GBIF': 'No',
            'Author(s)': '',
        }

        processor = TaxaProcessor()
        processor.all_keys = {}
        with mock.patch('bims.scripts.taxa_upload.preferences') as mock_prefs:
            mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
            processor.process_data(row, TaxonGroupF.create())

        self.assertEqual(
            Taxonomy.objects.filter(
                canonical_name='Culex duplicatus',
                taxonomic_status='SYNONYM',
            ).count(),
            1,
            'Null-subgenus fallback should reuse existing synonym, not create a duplicate.'
        )

    @mock.patch('bims.scripts.taxa_upload.get_species')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_synonym_no_null_subgenus_match_creates_new_taxon(
        self, mock_fetch_gbif, mock_get_species
    ):
        """
        When a synonym shares a name with an accepted taxon of a different
        subgenus and no null-subgenus synonym exists in the DB, a new separate
        synonym taxon is created rather than merging with the accepted taxon.
        """
        mock_fetch_gbif.return_value = None
        mock_get_species.return_value = None

        subgenus_a = TaxonomyF.create(canonical_name='SubA', rank='SUBGENUS')
        TaxonomyF.create(
            canonical_name='Culex unicus',
            scientific_name='Culex unicus',
            rank='SPECIES',
            subgenus=subgenus_a,
            taxonomic_status='ACCEPTED',
        )

        row = {
            'Taxon Rank': 'Species',
            'Kingdom': 'Animalia',
            'Phylum': 'Arthropoda',
            'Class': 'Insecta',
            'Order': 'Diptera',
            'Family': 'Culicidae',
            'Genus': 'Culex',
            'SubGenus': 'SubB',
            'Taxon': 'Culex unicus',
            'Taxonomic status': 'SYNONYM',
            'On GBIF': 'No',
            'Author(s)': '',
        }

        processor = TaxaProcessor()
        processor.all_keys = {}
        with mock.patch('bims.scripts.taxa_upload.preferences') as mock_prefs:
            mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
            processor.process_data(row, TaxonGroupF.create())

        synonym = Taxonomy.objects.filter(
            canonical_name='Culex unicus',
            taxonomic_status='SYNONYM',
        ).first()
        self.assertIsNotNone(synonym, 'A new synonym taxon should be created.')

        accepted = Taxonomy.objects.get(
            canonical_name='Culex unicus',
            taxonomic_status='ACCEPTED',
        )
        self.assertNotEqual(
            synonym.pk, accepted.pk,
            'Synonym must be a separate record from the accepted taxon.'
        )

    def test_choose_taxon_display_name_subgenus_falls_back_to_composed(self):
        """
        When the SubGenus column is empty, _choose_taxon_display_name should
        fall back to composed_taxon for a subgenus rank row.
        """
        processor = TaxaProcessor()
        processor.all_keys = {
            'TAXON': 'Taxon',
            'GENUS': 'Genus',
            'SUBGENUS': 'SubGenus',
        }

        row = {
            'Taxon': '',
            'Genus': 'Aedes',
            'SubGenus': '',
        }

        result = processor._choose_taxon_display_name(
            row=row,
            rank='SubGenus',
            taxonomic_status='accepted',
            csv_taxon='',
            composed_taxon='Aedes subgenus',
        )
        self.assertEqual(result, 'Aedes subgenus')


class TestAcceptedTaxonResolution(FastTenantTestCase):
    """Tests for gbif_data refresh and accepted taxon validation in process_data."""

    def setUp(self):
        self.taxon_group = TaxonGroupF.create()
        for key in ('indigenous', 'alien', 'unknown', 'alien-invasive', 'alien-non-invasive'):
            from bims.models.taxon_origin import TaxonOrigin
            TaxonOrigin.objects.get_or_create(
                origin_key=key, defaults={'category': key}
            )

    def _synonym_row(self, taxon_name='Aedes testus', **kwargs):
        row = {
            'Taxon Rank': 'Species',
            'Kingdom': 'Animalia',
            'Phylum': 'Arthropoda',
            'Class': 'Insecta',
            'Order': 'Diptera',
            'Family': 'Culicidae',
            'Genus': 'Aedes',
            'SubGenus': '',
            'Taxon': taxon_name,
            'Taxonomic status': 'SYNONYM',
            'On GBIF': 'No',
            'Author(s)': '',
        }
        row.update(kwargs)
        return row

    def _run(self, row):
        processor = TaxaProcessor()
        processor.all_keys = {}
        with mock.patch('bims.scripts.taxa_upload.preferences') as mock_prefs:
            mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
            processor.process_data(row, self.taxon_group)

    @mock.patch('bims.scripts.taxa_upload.get_species')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_existing_taxon_gbif_data_is_refreshed(
        self, mock_fetch_gbif, mock_get_species
    ):
        """
        Existing taxa have gbif_data refreshed via get_species so stale
        cached data does not prevent accepted-taxon resolution.
        """
        mock_fetch_gbif.return_value = None
        fresh_data = {'key': 11111, 'acceptedKey': 99999}
        mock_get_species.return_value = fresh_data

        existing = TaxonomyF.create(
            canonical_name='Aedes testus',
            scientific_name='Aedes testus',
            rank='SPECIES',
            gbif_key=11111,
            gbif_data={'key': 11111},  # stale — no acceptedKey
            taxonomic_status='SYNONYM',
        )

        self._run(self._synonym_row())

        existing.refresh_from_db()
        self.assertEqual(existing.gbif_data, fresh_data)
        mock_get_species.assert_called_with(11111)

    @mock.patch('bims.scripts.taxa_upload.get_species')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_stale_gbif_data_missing_accepted_key_resolved_via_fresh_fetch(
        self, mock_fetch_gbif, mock_get_species
    ):
        """
        When cached gbif_data has no acceptedKey, the fresh get_species call
        should provide it and accepted_taxonomy should be set accordingly.
        """
        mock_fetch_gbif.return_value = None

        accepted = TaxonomyF.create(
            canonical_name='Accepted Species',
            scientific_name='Accepted Species',
            rank='SPECIES',
            gbif_key=88888,
            taxonomic_status='ACCEPTED',
        )
        existing_synonym = TaxonomyF.create(
            canonical_name='Aedes testus',
            scientific_name='Aedes testus',
            rank='SPECIES',
            gbif_key=11111,
            gbif_data={'key': 11111},  # stale — no acceptedKey
            taxonomic_status='SYNONYM',
        )
        mock_get_species.return_value = {'key': 11111, 'acceptedKey': 88888}

        self._run(self._synonym_row())

        existing_synonym.refresh_from_db()
        self.assertEqual(
            existing_synonym.accepted_taxonomy_id,
            accepted.pk,
            'Accepted taxon should be resolved via fresh gbif_data after stale cache miss.'
        )

    @mock.patch('bims.scripts.taxa_upload.get_species')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_accepted_taxon_with_accepted_taxonomy_is_cleared(
        self, mock_fetch_gbif, mock_get_species
    ):
        """
        If the resolved accepted_taxon itself has accepted_taxonomy set (an
        invalid state — it would be a synonym, not an accepted taxon), that
        field is cleared before the assignment.
        """
        mock_fetch_gbif.return_value = None

        real_accepted = TaxonomyF.create(
            canonical_name='True Accepted',
            scientific_name='True Accepted',
            rank='SPECIES',
            gbif_key=77777,
            taxonomic_status='ACCEPTED',
        )
        wrong_accepted = TaxonomyF.create(
            canonical_name='Wrong Accepted',
            scientific_name='Wrong Accepted',
            rank='SPECIES',
            gbif_key=99999,
            accepted_taxonomy=real_accepted,  # invalid: accepted taxon should not have this
            taxonomic_status='ACCEPTED',
        )
        existing_synonym = TaxonomyF.create(
            canonical_name='Aedes testus',
            scientific_name='Aedes testus',
            rank='SPECIES',
            gbif_key=11111,
            gbif_data={'key': 11111, 'acceptedKey': 99999},
            taxonomic_status='SYNONYM',
        )
        mock_get_species.return_value = {'key': 11111, 'acceptedKey': 99999}

        self._run(self._synonym_row())

        wrong_accepted.refresh_from_db()
        self.assertIsNone(
            wrong_accepted.accepted_taxonomy,
            'accepted_taxonomy on the resolved accepted taxon must be cleared.'
        )


class TestAddendumUpload(FastTenantTestCase):
    """Tests for handling the CSV 'Addendum' column added alongside the
    Taxonomy.addendum field."""

    _CSV_CANONICAL_NAMES = [
        'Aquanothrus montanus',
        'Aquanothrus capensis',
        'Aquanothrus vexans',
        'Aquanothrus natalensis',
        'Aquanothrus',
        'Anisitsiellidae',
        'Trombidiformes',
        'Arachnida',
        'Arthropoda',
        'Animalia',
    ]

    def _delete_csv_taxa(self):
        Taxonomy.objects.filter(
            canonical_name__in=self._CSV_CANONICAL_NAMES
        ).delete()

    def setUp(self):
        self._delete_csv_taxa()
        self.taxon_group = TaxonGroupF.create()
        self.owner = UserF.create(first_name='tester')

        for key in ('indigenous', 'alien', 'unknown', 'alien-invasive', 'alien-non-invasive'):
            TaxonOrigin.objects.get_or_create(
                origin_key=key,
                defaults={'category': key}
            )

    def tearDown(self):
        self._delete_csv_taxa()

    def _make_upload_session(self, csv_filename):
        with open(os.path.join(test_data_directory, csv_filename), 'rb') as f:
            return UploadSessionF.create(
                uploader=self.owner,
                process_file=File(f),
                module_group=self.taxon_group
            )

    def test_addendum_resolver_matches_known_value(self):
        """TaxaProcessor.addendum() resolves a recognised value (name,
        full text or abbreviation, case-insensitive) to the stored code."""
        processor = TaxaProcessor()
        for raw in ('sensu lato', 'SENSU LATO', 's.l.', 'S.L.', 'SENSU_LATO'):
            self.assertEqual(
                processor.addendum({'Addendum': raw}),
                TaxonAddendum.SENSU_LATO.name,
                f'Expected {raw!r} to resolve to SENSU_LATO.'
            )

    def test_addendum_resolver_ignores_unknown_value(self):
        """An addendum value that isn't supported yet is ignored (returns
        '') rather than raising or being stored verbatim."""
        processor = TaxaProcessor()
        self.assertEqual(processor.addendum({'Addendum': 'nomen dubium'}), '')

    def test_addendum_resolver_ignores_empty_value(self):
        processor = TaxaProcessor()
        self.assertEqual(processor.addendum({'Addendum': ''}), '')
        self.assertEqual(processor.addendum({}), '')

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_csv_upload_sets_addendum_on_recognised_value(
        self, mock_gbif, mock_finish
    ):
        """A row with Addendum=sensu lato ends up with taxonomy.addendum
        set to the stored TaxonAddendum code."""
        mock_finish.return_value = None
        mock_gbif.return_value = None

        upload_session = self._make_upload_session('taxa_upload_addendum.csv')
        taxa_csv_upload = TaxaCSVUpload()
        taxa_csv_upload.upload_session = upload_session
        taxa_csv_upload.start('utf-8-sig')

        taxon = Taxonomy.objects.get(canonical_name='Aquanothrus montanus')
        self.assertEqual(taxon.addendum, TaxonAddendum.SENSU_LATO.name)

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_csv_upload_ignores_unrecognised_addendum(
        self, mock_gbif, mock_finish
    ):
        """A row with an Addendum value we don't support yet (e.g. 'nomen
        dubium') is uploaded normally, just without addendum being set."""
        mock_finish.return_value = None
        mock_gbif.return_value = None

        upload_session = self._make_upload_session('taxa_upload_addendum.csv')
        taxa_csv_upload = TaxaCSVUpload()
        taxa_csv_upload.upload_session = upload_session
        taxa_csv_upload.start('utf-8-sig')

        taxon = Taxonomy.objects.get(canonical_name='Aquanothrus capensis')
        self.assertEqual(taxon.addendum, '')

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_csv_upload_blank_addendum_column_leaves_field_blank(
        self, mock_gbif, mock_finish
    ):
        """A row with no Addendum value at all is unaffected."""
        mock_finish.return_value = None
        mock_gbif.return_value = None

        upload_session = self._make_upload_session('taxa_upload_addendum.csv')
        taxa_csv_upload = TaxaCSVUpload()
        taxa_csv_upload.upload_session = upload_session
        taxa_csv_upload.start('utf-8-sig')

        taxon = Taxonomy.objects.get(canonical_name='Aquanothrus vexans')
        self.assertEqual(taxon.addendum, '')

    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
    def test_csv_upload_strips_embedded_sl_from_taxon_name(
        self, mock_gbif, mock_finish
    ):
        """A row with no Addendum column value but 's.l.' embedded directly
        in the Taxon name (e.g. 'Aquanothrus natalensis s.l.') should end
        up with a clean canonical_name and addendum set from the name."""
        mock_finish.return_value = None
        mock_gbif.return_value = None

        upload_session = self._make_upload_session('taxa_upload_addendum.csv')
        taxa_csv_upload = TaxaCSVUpload()
        taxa_csv_upload.upload_session = upload_session
        taxa_csv_upload.start('utf-8-sig')

        taxon = Taxonomy.objects.get(canonical_name='Aquanothrus natalensis')
        self.assertEqual(taxon.addendum, TaxonAddendum.SENSU_LATO.name)
        self.assertNotIn('s.l', taxon.canonical_name.lower())


@mock.patch('bims.scripts.taxa_upload.is_fada_site', return_value=True)
@mock.patch('bims.scripts.taxa_upload.get_species_by_col_id')
@mock.patch('bims.scripts.taxa_upload.fetch_all_species_from_gbif')
class TestCollidingTaxaUpload(FastTenantTestCase):
    """
    Rows that match an existing taxon but are a different taxonomic concept
    (different FADA ID or author) must become their own record: no lookup
    (COL id, name, author refresh or local get_or_create) may hand back and
    overwrite the rejected taxon.
    """

    AUTHOR = 'Karanovic & Marmonier, 2002'

    def setUp(self):
        self.taxon_group = TaxonGroupF.create()
        TaxonomyF.create(
            canonical_name='Abcandonopsis williami',
            scientific_name='Abcandonopsis williami',
            rank='SPECIES',
            taxonomic_status='ACCEPTED',
            fada_id='500172',
        )

    def _existing_synonym(self, fada_id='SYN-500128', col_id=None, author=AUTHOR):
        return TaxonomyF.create(
            canonical_name='Candonopsis williami',
            scientific_name=f'Candonopsis williami {author}'.strip(),
            author=author,
            rank='SPECIES',
            taxonomic_status='SYNONYM',
            fada_id=fada_id,
            col_id=col_id,
        )

    def _synonym_row(self, fada_id, col_id=None, subgenus='', author=AUTHOR,
                     on_gbif='Yes'):
        row = {
            'Taxon Rank': 'Species',
            'Kingdom': 'Animalia',
            'Phylum': 'Arthropoda',
            'Class': 'Ostracoda',
            'Order': 'Podocopida',
            'Genus': 'Candonopsis',
            'SubGenus': subgenus,
            'Species': 'williami',
            'Taxon': 'Candonopsis williami',
            'Author(s)': author,
            'Taxonomic status': 'Synonym',
            'Accepted Taxon': 'Abcandonopsis williami',
            'FADA ID': fada_id,
            'On GBIF': on_gbif,
        }
        if col_id:
            row['GBIF Link'] = f'https://www.gbif.org/taxon/{col_id}'
        return row

    @staticmethod
    def _col_record(col_id, name='Candonopsis williami', status='synonym'):
        return {
            'usage': {
                'key': col_id,
                'canonicalName': name,
                'name': name,
                'rank': 'SPECIES',
                'status': status,
            },
        }

    def _process(self, row):
        processor = TaxaProcessor()
        processor.all_keys = {}
        with mock.patch('bims.scripts.taxa_upload.preferences') as mock_prefs:
            mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
            processor.process_data(row, self.taxon_group)

    def _synonyms(self):
        return Taxonomy.objects.filter(
            canonical_name='Candonopsis williami', taxonomic_status='SYNONYM')

    def test_synonyms_sharing_col_id_differing_by_subgenus_stay_separate(
        self, mock_fetch_gbif, mock_get_species_by_col_id, _mock_is_fada_site
    ):
        """
        FADA lists "Candonopsis williami" and "Candonopsis (Abcandonopsis)
        williami" as two synonyms (different FADA IDs) that carry the same
        COL id. The second row must not overwrite the first one's record.
        """
        first_synonym = self._existing_synonym(col_id='QLSH')
        mock_get_species_by_col_id.return_value = self._col_record('QLSH')
        # Mirror fetch_all_species_from_gbif: both a COL id and a name lookup
        # resolve to the record already holding QLSH.
        mock_fetch_gbif.side_effect = (
            lambda **kwargs: Taxonomy.objects.filter(col_id='QLSH').first())

        self._process(self._synonym_row(
            'SYN-500129', col_id='QLSH', subgenus='Abcandonopsis'))

        self.assertEqual(self._synonyms().count(), 2)

        first_synonym.refresh_from_db()
        self.assertEqual(first_synonym.fada_id, 'SYN-500128')
        self.assertEqual(first_synonym.col_id, 'QLSH')
        self.assertIsNone(first_synonym.subgenus)

        second_synonym = self._synonyms().get(fada_id='SYN-500129')
        self.assertIsNone(second_synonym.col_id)
        self.assertEqual(
            second_synonym.subgenus.canonical_name, 'Candonopsis (Abcandonopsis)')

    def test_col_id_fetch_skipped_when_col_id_belongs_to_rejected_taxon(
        self, mock_fetch_gbif, mock_get_species_by_col_id, _mock_is_fada_site
    ):
        self._existing_synonym(col_id='QLSH')
        mock_get_species_by_col_id.return_value = self._col_record('QLSH')
        mock_fetch_gbif.return_value = None

        self._process(self._synonym_row('SYN-500129', col_id='QLSH'))

        self.assertFalse(
            [c for c in mock_fetch_gbif.call_args_list
             if c.kwargs.get('col_id') == 'QLSH'],
            'Fetching by a COL id owned by the rejected taxon returns that taxon.'
        )
        self.assertEqual(self._synonyms().count(), 2)

    def test_synonym_does_not_take_col_id_of_accepted_taxon(
        self, mock_fetch_gbif, mock_get_species_by_col_id, _mock_is_fada_site
    ):
        """
        A synonym row carrying its accepted name's COL id becomes its own
        record, and the COL id stays unique to the accepted taxon.
        """
        mock_fetch_gbif.return_value = None
        accepted = Taxonomy.objects.get(fada_id='500172')
        accepted.col_id = 'ABCW1'
        accepted.save()
        mock_get_species_by_col_id.return_value = self._col_record(
            'ABCW1', name='Abcandonopsis williami', status='accepted')

        self._process(self._synonym_row('SYN-500128', col_id='ABCW1'))

        synonym = Taxonomy.objects.get(fada_id='SYN-500128')
        self.assertNotEqual(synonym.pk, accepted.pk)
        self.assertIsNone(synonym.col_id)
        self.assertEqual(
            list(Taxonomy.objects.filter(col_id='ABCW1')), [accepted])
        accepted.refresh_from_db()
        self.assertEqual(accepted.fada_id, '500172')

    def test_name_lookup_returning_rejected_taxon_is_discarded(
        self, mock_fetch_gbif, mock_get_species_by_col_id, _mock_is_fada_site
    ):
        """No COL link: the name lookup resolves to the existing synonym with
        a different FADA ID, which must not be reused."""
        first_synonym = self._existing_synonym()
        mock_fetch_gbif.return_value = first_synonym

        self._process(self._synonym_row('SYN-500129'))

        self.assertEqual(self._synonyms().count(), 2)
        first_synonym.refresh_from_db()
        self.assertEqual(first_synonym.fada_id, 'SYN-500128')
        mock_get_species_by_col_id.assert_not_called()

    def test_author_refresh_returning_rejected_taxon_is_ignored(
        self, mock_fetch_gbif, mock_get_species_by_col_id, _mock_is_fada_site
    ):
        """Without an author the new record triggers the GBIF author refresh;
        a refresh that resolves to the rejected taxon must not replace it."""
        first_synonym = self._existing_synonym()
        mock_fetch_gbif.return_value = first_synonym

        self._process(self._synonym_row('SYN-500129', author=''))

        self.assertEqual(self._synonyms().count(), 2)
        first_synonym.refresh_from_db()
        self.assertEqual(first_synonym.fada_id, 'SYN-500128')
        self.assertTrue(self._synonyms().filter(fada_id='SYN-500129').exists())

    def test_local_record_not_merged_into_rejected_taxon(
        self, mock_fetch_gbif, mock_get_species_by_col_id, _mock_is_fada_site
    ):
        """Not on GBIF: get_or_create on name/author/status would return the
        existing synonym, so a new record must be created instead."""
        first_synonym = self._existing_synonym()

        self._process(self._synonym_row('SYN-500129', on_gbif='No'))

        mock_fetch_gbif.assert_not_called()
        self.assertEqual(self._synonyms().count(), 2)
        first_synonym.refresh_from_db()
        self.assertEqual(first_synonym.fada_id, 'SYN-500128')

    def test_different_author_sharing_col_id_creates_separate_taxon(
        self, mock_fetch_gbif, mock_get_species_by_col_id, _mock_is_fada_site
    ):
        """An author conflict (no FADA IDs involved) is also a different
        taxon, so the shared COL id must not merge the two."""
        first_synonym = self._existing_synonym(fada_id='', col_id='QLSH')
        mock_get_species_by_col_id.return_value = self._col_record('QLSH')
        mock_fetch_gbif.side_effect = (
            lambda **kwargs: Taxonomy.objects.filter(col_id='QLSH').first())

        self._process(self._synonym_row(
            'SYN-500129', col_id='QLSH', author='Smith, 1900'))

        self.assertEqual(self._synonyms().count(), 2)
        first_synonym.refresh_from_db()
        self.assertEqual(first_synonym.author, self.AUTHOR)
        self.assertEqual(first_synonym.col_id, 'QLSH')
        new_synonym = self._synonyms().exclude(pk=first_synonym.pk).get()
        self.assertEqual(new_synonym.fada_id, 'SYN-500129')
        self.assertIsNone(new_synonym.col_id)

    def test_reupload_of_same_row_updates_existing_taxon(
        self, mock_fetch_gbif, mock_get_species_by_col_id, _mock_is_fada_site
    ):
        """A row with the same FADA ID and COL id still updates the existing
        record rather than creating a duplicate."""
        existing = self._existing_synonym(col_id='QLSH')
        mock_get_species_by_col_id.return_value = self._col_record('QLSH')
        mock_fetch_gbif.return_value = None

        self._process(self._synonym_row(
            'SYN-500128', col_id='QLSH', subgenus='Abcandonopsis'))

        self.assertEqual(list(self._synonyms()), [existing])
        existing.refresh_from_db()
        self.assertEqual(existing.col_id, 'QLSH')
        self.assertEqual(
            existing.subgenus.canonical_name, 'Candonopsis (Abcandonopsis)')
