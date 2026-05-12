# coding=utf-8
from unittest import mock

from django.core.files.base import ContentFile
from django.db import connection
from django_tenants.test.cases import FastTenantTestCase

from bims.models import Taxonomy
from bims.models.harvest_session import HarvestSession
from bims.scripts.taxa_upload_taxonworks import TaxonWorksTaxaProcessor
from bims.tasks.harvest_taxonworks_species import harvest_taxonworks_species
from bims.tests.model_factories import TaxonGroupF, UserF

_PATCH_DISCONNECT = 'bims.signals.utils.disconnect_bims_signals'
_PATCH_CONNECT = 'bims.signals.utils.connect_bims_signals'
_PATCH_PREFS = 'bims.scripts.taxa_upload_taxonworks.preferences'
_PATCH_ROOT = 'bims.utils.taxonworks.get_taxon_name'
_PATCH_ALL = 'bims.utils.taxonworks.get_all_taxon_names'


def _record(taxon_id, name, rank, parent_id=None, valid=True,
            valid_id=None, author='', updated_at='2024-01-01T00:00:00.000Z',
            extinct=False):
    return {
        "id": taxon_id,
        "name": name.split()[-1] if rank in {'species', 'subspecies'} else name,
        "parent_id": parent_id,
        "cached": name,
        "cached_html": f"† <i>{name}</i>" if extinct else name,
        "rank": rank,
        "rank_string": rank,
        "type": "Protonym",
        "project_id": 55,
        "cached_valid_taxon_name_id": valid_id or taxon_id,
        "cached_author": author,
        "cached_author_year": author,
        "cached_is_valid": valid,
        "created_at": "2023-01-01T00:00:00.000Z",
        "updated_at": updated_at,
        "name_string": f"{name} {author}".strip(),
        "original_combination": None,
    }


class TestHarvestTaxonWorksSpeciesTask(FastTenantTestCase):
    def setUp(self):
        self.taxon_group = TaxonGroupF.create()
        self.user = UserF.create()
        self.schema_name = connection.schema_name

    def _make_session(self, additional=None):
        data = additional if additional is not None else {
            'base_url': 'https://sfg.taxonworks.org',
            'project_token': 'token',
            'taxon_name_id': 100,
            'exclude_extinct': True,
        }
        session = HarvestSession.objects.create(
            harvester=self.user,
            module_group=self.taxon_group,
            category='taxonworks',
            additional_data=data,
        )
        session.log_file.save(
            f'taxonworks-test-{session.id}.log',
            ContentFile(b''),
        )
        return session

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_ALL)
    @mock.patch(_PATCH_ROOT)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_session_marked_finished(
        self, mock_dis, mock_con, mock_root, mock_all, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        root = _record(100, 'Plecoptera', 'order', parent_id=10, author='Burmeister, 1839')
        mock_root.return_value = root
        mock_all.return_value = [root]

        session = self._make_session()
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertIn('Finished', session.status)
        taxon = Taxonomy.objects.get(canonical_name='Plecoptera', rank='ORDER')
        self.assertEqual(taxon.additional_data['_taxonworks_taxon_name_id'], 100)

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_ALL)
    @mock.patch(_PATCH_ROOT)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_descendants_processed_and_extinct_skipped(
        self, mock_dis, mock_con, mock_root, mock_all, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        root = _record(100, 'Plecoptera', 'order', parent_id=10)
        family = _record(101, 'Perlidae', 'family', parent_id=100)
        extinct_genus = _record(102, 'Thaumatophora', 'genus', parent_id=100, extinct=True)
        species = _record(103, 'Perla marginata', 'species', parent_id=101)
        mock_root.return_value = root
        mock_all.return_value = [root, family, extinct_genus, species]

        session = self._make_session()
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        self.assertTrue(Taxonomy.objects.filter(canonical_name='Perlidae', rank='FAMILY').exists())
        self.assertTrue(Taxonomy.objects.filter(canonical_name='Perla marginata', rank='SPECIES').exists())
        self.assertFalse(Taxonomy.objects.filter(canonical_name='Thaumatophora').exists())

    @mock.patch(_PATCH_PREFS)
    @mock.patch(_PATCH_ALL)
    @mock.patch(_PATCH_ROOT)
    @mock.patch(_PATCH_CONNECT)
    @mock.patch(_PATCH_DISCONNECT)
    def test_invalid_taxon_links_to_valid_taxonomy(
        self, mock_dis, mock_con, mock_root, mock_all, mock_prefs
    ):
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        root = _record(100, 'Plecoptera', 'order', parent_id=10)
        genus = _record(110, 'Neophron', 'genus', parent_id=100)
        species_parent = _record(112, 'Neophron percnopterus', 'species', parent_id=110)
        accepted = _record(111, 'Neophron percnopterus ginginianus', 'subspecies', parent_id=112)
        synonym = _record(
            113,
            'Vultur ginginianus',
            None,
            parent_id=100,
            valid=False,
            valid_id=111,
        )
        synonym['type'] = 'Combination'
        mock_root.return_value = root
        mock_all.return_value = [root, genus, species_parent, accepted, synonym]

        session = self._make_session()
        harvest_taxonworks_species(session.id, schema_name=self.schema_name)

        synonym_taxon = Taxonomy.objects.get(canonical_name='Vultur ginginianus', rank='SPECIES')
        self.assertEqual(synonym_taxon.taxonomic_status, 'SYNONYM')
        self.assertIsNotNone(synonym_taxon.accepted_taxonomy)
        self.assertEqual(
            synonym_taxon.accepted_taxonomy.canonical_name,
            'Neophron percnopterus ginginianus'
        )
        self.assertEqual(synonym_taxon.accepted_taxonomy.rank, 'SUBSPECIES')


# ---------------------------------------------------------------------------
# TaxonWorksTaxaProcessor — GBIF lineage fallback
# ---------------------------------------------------------------------------

_GBIF_SEARCH = 'bims.scripts.taxa_upload_taxonworks.search_exact_match'
_GBIF_GET = 'bims.scripts.taxa_upload_taxonworks.get_species'
_PATCH_PREFS2 = 'bims.scripts.taxa_upload_taxonworks.preferences'

_GBIF_FAMILY_DATA = {
    'key': 9999,
    'rank': 'FAMILY',
    'canonicalName': 'Perlidae',
    'scientificName': 'Perlidae',
    'kingdom': 'Animalia',
    'phylum': 'Arthropoda',
    'class': 'Insecta',
    'order': 'Plecoptera',
    'family': 'Perlidae',
    'kingdomKey': 1,
    'phylumKey': 2,
    'classKey': 3,
    'orderKey': 4,
    'familyKey': 9999,
}


class TestTaxonWorksGbifLineage(FastTenantTestCase):
    """TaxonWorksTaxaProcessor._ensure_gbif_lineage fills missing ancestors."""

    def setUp(self):
        self.taxon_group = TaxonGroupF.create()

    def _make_processor(self):
        return TaxonWorksTaxaProcessor(
            base_url='https://sfg.taxonworks.org',
            project_token='tok',
        )

    # -- _walk_to_kingdom ---------------------------------------------------

    def test_walk_to_kingdom_returns_true_for_kingdom(self):
        kingdom = Taxonomy.objects.create(
            canonical_name='Animalia', scientific_name='Animalia',
            legacy_canonical_name='Animalia', rank='KINGDOM',
        )
        proc = self._make_processor()
        self.assertTrue(proc._walk_to_kingdom(kingdom))

    def test_walk_to_kingdom_returns_true_via_parent(self):
        kingdom = Taxonomy.objects.create(
            canonical_name='Animalia2', scientific_name='Animalia2',
            legacy_canonical_name='Animalia2', rank='KINGDOM',
        )
        family = Taxonomy.objects.create(
            canonical_name='TestFam', scientific_name='TestFam',
            legacy_canonical_name='TestFam', rank='FAMILY',
            parent=kingdom,
        )
        proc = self._make_processor()
        self.assertTrue(proc._walk_to_kingdom(family))

    def test_walk_to_kingdom_returns_false_when_no_kingdom(self):
        family = Taxonomy.objects.create(
            canonical_name='OrphanFam', scientific_name='OrphanFam',
            legacy_canonical_name='OrphanFam', rank='FAMILY',
        )
        proc = self._make_processor()
        self.assertFalse(proc._walk_to_kingdom(family))

    # -- _ensure_gbif_lineage -----------------------------------------------

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_gbif_lineage_fills_missing_ancestors(
        self, mock_search, mock_get
    ):
        """A FAMILY with no parent gets Kingdom→Phylum→Class→Order chain."""
        mock_search.return_value = 9999
        mock_get.return_value = _GBIF_FAMILY_DATA

        family = Taxonomy.objects.create(
            canonical_name='Perlidae', scientific_name='Perlidae',
            legacy_canonical_name='Perlidae', rank='FAMILY',
        )
        proc = self._make_processor()
        proc._ensure_gbif_lineage(family)
        family.refresh_from_db()

        self.assertIsNotNone(family.parent)
        self.assertEqual(family.parent.rank, 'ORDER')
        self.assertEqual(family.parent.canonical_name, 'Plecoptera')
        self.assertEqual(family.parent.parent.rank, 'CLASS')
        self.assertEqual(family.parent.parent.canonical_name, 'Insecta')
        self.assertEqual(family.parent.parent.parent.rank, 'PHYLUM')
        self.assertEqual(family.parent.parent.parent.parent.rank, 'KINGDOM')
        self.assertEqual(family.parent.parent.parent.parent.canonical_name, 'Animalia')

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_gbif_lineage_skipped_when_kingdom_already_exists(
        self, mock_search, mock_get
    ):
        """No GBIF call when the taxonomy already has a Kingdom ancestor."""
        kingdom = Taxonomy.objects.create(
            canonical_name='ExistingKingdom', scientific_name='ExistingKingdom',
            legacy_canonical_name='ExistingKingdom', rank='KINGDOM',
        )
        family = Taxonomy.objects.create(
            canonical_name='AlreadyLinkedFam', scientific_name='AlreadyLinkedFam',
            legacy_canonical_name='AlreadyLinkedFam', rank='FAMILY',
            parent=kingdom,
        )
        proc = self._make_processor()
        proc._ensure_gbif_lineage(family)

        mock_search.assert_not_called()
        mock_get.assert_not_called()

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_gbif_lineage_graceful_when_no_gbif_match(
        self, mock_search, mock_get
    ):
        """If GBIF returns no match, parent stays None without raising."""
        mock_search.return_value = None

        family = Taxonomy.objects.create(
            canonical_name='UnknownFam', scientific_name='UnknownFam',
            legacy_canonical_name='UnknownFam', rank='FAMILY',
        )
        proc = self._make_processor()
        proc._ensure_gbif_lineage(family)  # should not raise

        family.refresh_from_db()
        self.assertIsNone(family.parent)
        mock_get.assert_not_called()

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_gbif_lineage_reuses_existing_taxonomy(
        self, mock_search, mock_get
    ):
        """If a Kingdom/Phylum node already exists in DB it is reused, not duplicated."""
        mock_search.return_value = 9999
        mock_get.return_value = _GBIF_FAMILY_DATA

        # Pre-create Animalia with the same gbif_key that the mock returns
        existing_kingdom = Taxonomy.objects.create(
            canonical_name='Animalia', scientific_name='Animalia',
            legacy_canonical_name='Animalia', rank='KINGDOM',
            gbif_key=1,
        )

        family = Taxonomy.objects.create(
            canonical_name='Perlidae2', scientific_name='Perlidae2',
            legacy_canonical_name='Perlidae2', rank='FAMILY',
        )
        proc = self._make_processor()
        proc._ensure_gbif_lineage(family)
        family.refresh_from_db()

        # Walk to kingdom
        cursor = family
        while cursor.parent:
            cursor = cursor.parent
        self.assertEqual(cursor.id, existing_kingdom.id)
        # No extra Animalia rows should have been created
        self.assertEqual(
            Taxonomy.objects.filter(canonical_name='Animalia', rank='KINGDOM').count(), 1
        )

    @mock.patch(_PATCH_PREFS2)
    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_process_taxonworks_record_calls_gbif_lineage(
        self, mock_search, mock_get, mock_prefs
    ):
        """
        Full process_taxonworks_record: a family-level record with no
        TaxonWorks parents above it gets ancestors from GBIF.
        """
        mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
        mock_search.return_value = 9999
        mock_get.return_value = _GBIF_FAMILY_DATA

        family_record = {
            'id': 200,
            'name': 'Perlidae',
            'cached': 'Perlidae',
            'parent_id': None,
            'rank': 'family',
            'rank_string': 'family',
            'type': 'Protonym',
            'project_id': 1,
            'cached_valid_taxon_name_id': 200,
            'cached_is_valid': True,
            'cached_author': '',
            'cached_author_year': '',
            'name_string': 'Perlidae',
            'updated_at': '2024-01-01T00:00:00.000Z',
            'created_at': '2023-01-01T00:00:00.000Z',
        }

        proc = self._make_processor()
        taxonomy = proc.process_taxonworks_record(family_record, self.taxon_group)

        self.assertIsNotNone(taxonomy)
        taxonomy.refresh_from_db()
        self.assertIsNotNone(taxonomy.parent)
        self.assertEqual(taxonomy.parent.rank, 'ORDER')


# ---------------------------------------------------------------------------
# TaxonWorksTaxaProcessor — species / subspecies hierarchy validation
# ---------------------------------------------------------------------------

_NO_GBIF_SEARCH = 'bims.scripts.taxa_upload_taxonworks.search_exact_match'
_NO_GBIF_GET = 'bims.scripts.taxa_upload_taxonworks.get_species'


class TestSpeciesHierarchyValidation(FastTenantTestCase):

    def setUp(self):
        self.taxon_group = TaxonGroupF.create()

    def _proc(self):
        return TaxonWorksTaxaProcessor(
            base_url='https://sfg.taxonworks.org',
            project_token='tok',
        )

    def _species(self, name, parent=None):
        return Taxonomy.objects.create(
            canonical_name=name, scientific_name=name,
            legacy_canonical_name=name, rank='SPECIES', parent=parent,
        )

    def _genus(self, name, parent=None):
        return Taxonomy.objects.create(
            canonical_name=name, scientific_name=name,
            legacy_canonical_name=name, rank='GENUS', parent=parent,
        )

    # -- _find_genus_ancestor -----------------------------------------------

    def test_find_genus_direct_parent(self):
        genus = self._genus('Homo')
        species = self._species('Homo sapiens', parent=genus)
        proc = self._proc()
        self.assertEqual(proc._find_genus_ancestor(species.parent), genus)

    def test_find_genus_through_subgenus(self):
        genus = self._genus('Homo')
        subgenus = Taxonomy.objects.create(
            canonical_name='Homo (Homo)', scientific_name='Homo (Homo)',
            legacy_canonical_name='Homo (Homo)', rank='SUBGENUS', parent=genus,
        )
        species = self._species('Homo sapiens', parent=subgenus)
        proc = self._proc()
        self.assertEqual(proc._find_genus_ancestor(species.parent), genus)

    def test_find_genus_through_superspecies(self):
        genus = self._genus('Canis')
        supersp = Taxonomy.objects.create(
            canonical_name='Canis lupus group', scientific_name='Canis lupus group',
            legacy_canonical_name='Canis lupus group', rank='SUPERSPECIES', parent=genus,
        )
        species = self._species('Canis lupus', parent=supersp)
        proc = self._proc()
        self.assertEqual(proc._find_genus_ancestor(species.parent), genus)

    def test_find_genus_returns_none_for_family_parent(self):
        family = Taxonomy.objects.create(
            canonical_name='Hominidae', scientific_name='Hominidae',
            legacy_canonical_name='Hominidae', rank='FAMILY',
        )
        species = self._species('Homo sapiens', parent=family)
        proc = self._proc()
        self.assertIsNone(proc._find_genus_ancestor(species.parent))

    # -- _get_or_create_genus -----------------------------------------------

    def test_get_or_create_genus_reuses_existing(self):
        existing = self._genus('Felis')
        proc = self._proc()
        result = proc._get_or_create_genus('Felis')
        self.assertEqual(result.id, existing.id)
        self.assertEqual(Taxonomy.objects.filter(canonical_name='Felis', rank='GENUS').count(), 1)

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_get_or_create_genus_uses_gbif(self, mock_search, mock_get):
        mock_search.return_value = 42
        mock_get.return_value = {
            'key': 42, 'rank': 'GENUS',
            'canonicalName': 'Panthera', 'scientificName': 'Panthera Oken, 1816',
        }
        proc = self._proc()
        genus = proc._get_or_create_genus('Panthera')
        self.assertEqual(genus.rank, 'GENUS')
        self.assertEqual(genus.canonical_name, 'Panthera')
        self.assertEqual(genus.gbif_key, 42)

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_get_or_create_genus_stub_when_no_gbif(self, mock_search, mock_get):
        mock_search.return_value = None
        proc = self._proc()
        genus = proc._get_or_create_genus('UnknownGenus')
        self.assertEqual(genus.rank, 'GENUS')
        self.assertEqual(genus.canonical_name, 'UnknownGenus')
        mock_get.assert_not_called()

    # -- _validate_species_hierarchy: SPECIES --------------------------------

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_species_with_genus_parent_unchanged(self, mock_search, mock_get):
        genus = self._genus('Aquila')
        species = self._species('Aquila chrysaetos', parent=genus)
        proc = self._proc()
        proc._validate_species_hierarchy(species)
        species.refresh_from_db()
        self.assertEqual(species.parent_id, genus.id)
        mock_search.assert_not_called()

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_species_under_family_gets_genus_inserted(self, mock_search, mock_get):
        """Species directly under Family → a Genus must be inserted."""
        mock_search.return_value = None  # force stub creation
        family = Taxonomy.objects.create(
            canonical_name='Accipitridae', scientific_name='Accipitridae',
            legacy_canonical_name='Accipitridae', rank='FAMILY',
        )
        species = self._species('Aquila chrysaetos', parent=family)
        proc = self._proc()
        proc._validate_species_hierarchy(species)
        species.refresh_from_db()
        self.assertEqual(species.parent.rank, 'GENUS')
        self.assertEqual(species.parent.canonical_name, 'Aquila')
        # genus should be placed above family
        self.assertEqual(species.parent.parent_id, family.id)

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_species_with_no_parent_creates_genus(self, mock_search, mock_get):
        mock_search.return_value = None
        species = self._species('Canis lupus')
        proc = self._proc()
        proc._validate_species_hierarchy(species)
        species.refresh_from_db()
        self.assertEqual(species.parent.rank, 'GENUS')
        self.assertEqual(species.parent.canonical_name, 'Canis')

    # -- _validate_species_hierarchy: SUBSPECIES -----------------------------

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_subspecies_with_correct_chain_unchanged(self, mock_search, mock_get):
        genus = self._genus('Canis')
        species = self._species('Canis lupus', parent=genus)
        subsp = Taxonomy.objects.create(
            canonical_name='Canis lupus familiaris',
            scientific_name='Canis lupus familiaris',
            legacy_canonical_name='Canis lupus familiaris',
            rank='SUBSPECIES', parent=species,
        )
        proc = self._proc()
        proc._validate_species_hierarchy(subsp)
        subsp.refresh_from_db()
        self.assertEqual(subsp.parent_id, species.id)
        mock_search.assert_not_called()

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_subspecies_under_genus_creates_species(self, mock_search, mock_get):
        """Subspecies directly under Genus → a Species must be inserted."""
        mock_search.return_value = None
        genus = self._genus('Canis')
        subsp = Taxonomy.objects.create(
            canonical_name='Canis lupus familiaris',
            scientific_name='Canis lupus familiaris',
            legacy_canonical_name='Canis lupus familiaris',
            rank='SUBSPECIES', parent=genus,
        )
        proc = self._proc()
        proc._validate_species_hierarchy(subsp)
        subsp.refresh_from_db()
        self.assertEqual(subsp.parent.rank, 'SPECIES')
        self.assertEqual(subsp.parent.canonical_name, 'Canis lupus')
        self.assertEqual(subsp.parent.parent_id, genus.id)

    @mock.patch('bims.utils.gbif.get_species')
    @mock.patch('bims.utils.gbif.search_exact_match')
    def test_subspecies_with_no_parent_creates_species_and_genus(self, mock_search, mock_get):
        mock_search.return_value = None
        subsp = Taxonomy.objects.create(
            canonical_name='Homo sapiens neanderthalensis',
            scientific_name='Homo sapiens neanderthalensis',
            legacy_canonical_name='Homo sapiens neanderthalensis',
            rank='SUBSPECIES',
        )
        proc = self._proc()
        proc._validate_species_hierarchy(subsp)
        subsp.refresh_from_db()
        self.assertEqual(subsp.parent.rank, 'SPECIES')
        self.assertEqual(subsp.parent.canonical_name, 'Homo sapiens')
        self.assertEqual(subsp.parent.parent.rank, 'GENUS')
        self.assertEqual(subsp.parent.parent.canonical_name, 'Homo')
