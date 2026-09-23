# coding=utf-8
"""
Tests for the BIMS-to-BIMS species harvester.

Covers:
  - bims_instance utilities (get_taxon_by_id, get_taxon_groups, get_all_taxa,
    retry / failure behaviour)
  - _parse_tag_list helper
  - _apply_tags (tag + TagGroup creation)
  - _find_or_create_taxonomy (match by gbif_key, name+rank, create, parent
    resolution, additional_data merge, tag application)
  - harvest_bims_species Celery task (session lifecycle, taxon group
    assignment, new-group import mode, cancellation)
  - BimsFetchTaxonGroupsView AJAX endpoint
  - TaxaList public access + field stripping
  - TaxonDetail public access + field stripping
"""
from unittest import mock
import tempfile

import requests as _requests
from django.contrib.auth import get_user_model
from django.core.files.base import ContentFile
from django.db import connection
from django.test import RequestFactory, TestCase
from django_tenants.test.cases import FastTenantTestCase
from rest_framework.test import APIRequestFactory
from taggit.models import Tag

from bims.models import Taxonomy
from bims.models.harvest_session import HarvestSession
from bims.models.tag_group import TagGroup
from bims.models.taxon_group_taxonomy import TaxonGroupTaxonomy
from bims.tasks.harvest_bims_species import (
    _apply_tags,
    _compute_taxon_checksum,
    _find_or_create_taxonomy,
    _parse_tag_list,
    _remove_stale_taxa,
    harvest_bims_species,
)
from bims.tests.model_factories import TaxonGroupF, UserF

User = get_user_model()

_PATCH_DISCONNECT = 'bims.signals.utils.disconnect_bims_signals'
_PATCH_CONNECT = 'bims.signals.utils.connect_bims_signals'
_PATCH_PREFS = 'preferences.preferences'
_PATCH_GET_ALL_TAXA = 'bims.utils.bims_instance.get_all_taxa'
_PATCH_GET_TAXON_BY_ID = 'bims.utils.bims_instance.get_taxon_by_id'
_PATCH_GET_TAXON_BY_ID_UTIL = 'bims.utils.bims_instance.get_taxon_by_id'
_PATCH_GET_GROUPS = 'bims.utils.bims_instance.get_taxon_groups'


# ---------------------------------------------------------------------------
# Helper: build a minimal remote-BIMS taxon dict (mirrors the sample data)
# ---------------------------------------------------------------------------

def _taxon(
    taxon_id, canonical_name, rank='SPECIES',
    gbif_key=None, parent=None, scientific_name=None,
    author='', taxonomic_status='ACCEPTED',
    tag_list='', additional_data=None, accepted_taxonomy=None,
    aphia_id=None, col_id=None, fada_id=None,
    taxonworks_id=None, taxonworks_otu_id=None,
):
    return {
        'id': taxon_id,
        'canonical_name': canonical_name,
        'scientific_name': scientific_name or canonical_name,
        'rank': rank,
        'gbif_key': gbif_key,
        'parent': parent,
        'accepted_taxonomy': accepted_taxonomy,
        'author': author,
        'taxonomic_status': taxonomic_status,
        'tag_list': tag_list,
        'additional_data': additional_data or {},
        'aphia_id': aphia_id,
        'col_id': col_id,
        'fada_id': fada_id,
        'taxonworks_id': taxonworks_id,
        'taxonworks_otu_id': taxonworks_otu_id,
    }


# ===========================================================================
# _parse_tag_list
# ===========================================================================

class TestParseTagList(FastTenantTestCase):

    def test_empty_string(self):
        self.assertEqual(_parse_tag_list(''), [])

    def test_none(self):
        self.assertEqual(_parse_tag_list(None), [])

    def test_single_plain_tag(self):
        self.assertEqual(_parse_tag_list('test'), [('test', None)])

    def test_single_tag_with_colour(self):
        result = _parse_tag_list('aquatic (#51FF3E)')
        self.assertEqual(result, [('aquatic', '#51FF3E')])

    def test_multiple_tags_with_colours(self):
        result = _parse_tag_list('aquatic (#51FF3E), freshwater (#FF5733)')
        self.assertEqual(result, [
            ('aquatic', '#51FF3E'),
            ('freshwater', '#FF5733'),
        ])

    def test_mixed_tags(self):
        result = _parse_tag_list('plain, coloured (#AABBCC)')
        self.assertEqual(result, [
            ('plain', None),
            ('coloured', '#AABBCC'),
        ])

    def test_colour_hex_is_uppercased(self):
        result = _parse_tag_list('tag (#abc123)')
        self.assertEqual(result[0][1], '#ABC123')

    def test_three_char_hex(self):
        result = _parse_tag_list('tag (#F3E)')
        self.assertEqual(result[0][1], '#F3E')


# ===========================================================================
# _apply_tags
# ===========================================================================

class TestApplyTags(FastTenantTestCase):

    def _taxonomy(self, name='TestTaxon'):
        return Taxonomy.objects.create(
            canonical_name=name,
            scientific_name=name,
            rank='SPECIES',
        )

    def test_plain_tag_added_to_taxonomy(self):
        taxonomy = self._taxonomy()
        _apply_tags(taxonomy, 'test')
        self.assertIn('test', [t.name for t in taxonomy.tags.all()])

    def test_coloured_tag_creates_tag_group(self):
        taxonomy = self._taxonomy('Fish1')
        _apply_tags(taxonomy, 'aquatic (#51FF3E)')
        tag = Tag.objects.get(name='aquatic')
        self.assertIn('aquatic', [t.name for t in taxonomy.tags.all()])
        group = TagGroup.objects.filter(tags=tag).first()
        self.assertIsNotNone(group)
        self.assertEqual(group.colour.upper(), '#51FF3E')

    def test_coloured_tag_reuses_existing_tag_group(self):
        # Pre-create a TagGroup with the same colour
        existing_group = TagGroup.objects.create(name='MyGroup', colour='#51FF3E')
        taxonomy = self._taxonomy('Fish2')
        _apply_tags(taxonomy, 'aquatic (#51FF3E)')
        tag = Tag.objects.get(name='aquatic')
        groups = TagGroup.objects.filter(tags=tag)
        # Should use the existing group, not create a new one
        self.assertEqual(groups.count(), 1)
        self.assertEqual(groups.first().id, existing_group.id)

    def test_multiple_tags_all_added(self):
        taxonomy = self._taxonomy('Fish3')
        _apply_tags(taxonomy, 'aquatic (#51FF3E), freshwater (#FF5733)')
        tag_names = [t.name for t in taxonomy.tags.all()]
        self.assertIn('aquatic', tag_names)
        self.assertIn('freshwater', tag_names)
        self.assertEqual(TagGroup.objects.count(), 2)

    def test_tag_group_not_duplicated_on_rerun(self):
        taxonomy = self._taxonomy('Fish4')
        _apply_tags(taxonomy, 'aquatic (#51FF3E)')
        _apply_tags(taxonomy, 'aquatic (#51FF3E)')
        self.assertEqual(TagGroup.objects.filter(colour__iexact='#51FF3E').count(), 1)


# ===========================================================================
# _find_or_create_taxonomy
# ===========================================================================

class TestFindOrCreateTaxonomy(FastTenantTestCase):

    def setUp(self):
        self.remote_cache = {}

    # -- matching existing records ------------------------------------------

    def test_finds_by_gbif_key(self):
        existing = Taxonomy.objects.create(
            canonical_name='Abactochromis labrosus', scientific_name='Abactochromis labrosus',
            rank='SPECIES', gbif_key=5961874,
        )
        data = _taxon(441, 'Abactochromis labrosus', gbif_key=5961874)
        result = _find_or_create_taxonomy(data, 'http://bims.test', self.remote_cache)
        self.assertEqual(result.id, existing.id)
        self.assertEqual(Taxonomy.objects.filter(gbif_key=5961874).count(), 1)

    def test_finds_by_canonical_name_and_rank(self):
        existing = Taxonomy.objects.create(
            canonical_name='Abcandonopsis aula', scientific_name='Abcandonopsis aula',
            rank='SPECIES',
        )
        data = _taxon(2718, 'Abcandonopsis aula', rank='SPECIES')
        result = _find_or_create_taxonomy(data, 'http://bims.test', self.remote_cache)
        self.assertEqual(result.id, existing.id)

    def test_creates_new_when_no_match(self):
        data = _taxon(999, 'Newgenus newspecies', rank='SPECIES', gbif_key=12345)
        result = _find_or_create_taxonomy(data, 'http://bims.test', self.remote_cache)
        self.assertIsNotNone(result)
        self.assertEqual(result.canonical_name, 'Newgenus newspecies')
        self.assertEqual(result.rank, 'SPECIES')
        self.assertEqual(result.gbif_key, 12345)

    def test_creates_new_with_aphia_id_and_col_id(self):
        data = _taxon(
            998, 'Newgenus wormsii', rank='SPECIES',
            aphia_id=131160, col_id='ABCD1234',
        )
        result = _find_or_create_taxonomy(data, 'http://bims.test', self.remote_cache)
        self.assertEqual(result.aphia_id, 131160)
        self.assertEqual(result.col_id, 'ABCD1234')

    def test_creates_new_with_fada_id(self):
        data = _taxon(997, 'Newgenus fadaensis', rank='SPECIES', fada_id='FADA-1')
        result = _find_or_create_taxonomy(data, 'http://bims.test', self.remote_cache)
        self.assertEqual(result.fada_id, 'FADA-1')

    def test_create_skips_fada_id_already_taken_by_other_taxon(self):
        Taxonomy.objects.create(
            canonical_name='Owner species', scientific_name='Owner species',
            rank='SPECIES', fada_id='FADA-DUP',
        )
        data = _taxon(996, 'Newgenus duplicatus', rank='SPECIES', fada_id='FADA-DUP')
        result = _find_or_create_taxonomy(data, 'http://bims.test', self.remote_cache)
        self.assertIsNotNone(result)
        self.assertIsNone(result.fada_id)

    def test_creates_new_with_taxonworks_ids(self):
        data = _taxon(
            995, 'Newgenus taxonworksii', rank='SPECIES',
            taxonworks_id=42, taxonworks_otu_id=99,
        )
        result = _find_or_create_taxonomy(data, 'http://bims.test', self.remote_cache)
        self.assertEqual(result.taxonworks_id, 42)
        self.assertEqual(result.taxonworks_otu_id, 99)

    def test_returns_none_for_empty_canonical_name(self):
        data = _taxon(1, '', rank='SPECIES')
        result = _find_or_create_taxonomy(data, 'http://bims.test', self.remote_cache)
        self.assertIsNone(result)

    # -- caching ------------------------------------------------------------

    def test_result_cached_by_remote_id(self):
        data = _taxon(55, 'Cached species', rank='SPECIES')
        result = _find_or_create_taxonomy(data, 'http://bims.test', self.remote_cache)
        self.assertIn(55, self.remote_cache)
        self.assertEqual(self.remote_cache[55].id, result.id)

    def test_cache_prevents_second_db_lookup(self):
        data = _taxon(77, 'Cached genus', rank='GENUS')
        first = _find_or_create_taxonomy(data, 'http://bims.test', self.remote_cache)
        # Second call with same remote_cache should return same object without
        # creating a duplicate
        second = _find_or_create_taxonomy(data, 'http://bims.test', self.remote_cache)
        self.assertEqual(first.id, second.id)
        self.assertEqual(Taxonomy.objects.filter(canonical_name='Cached genus', rank='GENUS').count(), 1)

    # -- parent resolution --------------------------------------------------

    @mock.patch(_PATCH_GET_TAXON_BY_ID)
    def test_parent_resolved_from_remote(self, mock_get_taxon):
        parent_data = _taxon(100, 'Parentgenus', rank='GENUS')
        mock_get_taxon.return_value = parent_data

        child_data = _taxon(101, 'Parentgenus childspecies', rank='SPECIES', parent=100)
        result = _find_or_create_taxonomy(child_data, 'http://bims.test', self.remote_cache)

        self.assertIsNotNone(result.parent)
        self.assertEqual(result.parent.canonical_name, 'Parentgenus')
        self.assertEqual(result.parent.rank, 'GENUS')

    @mock.patch(_PATCH_GET_TAXON_BY_ID)
    def test_parent_from_cache_not_refetched(self, mock_get_taxon):
        parent = Taxonomy.objects.create(
            canonical_name='CachedParent', scientific_name='CachedParent', rank='GENUS',
        )
        self.remote_cache[200] = parent
        child_data = _taxon(201, 'CachedParent child', rank='SPECIES', parent=200)
        result = _find_or_create_taxonomy(child_data, 'http://bims.test', self.remote_cache)
        mock_get_taxon.assert_not_called()
        self.assertEqual(result.parent.id, parent.id)

    @mock.patch(_PATCH_GET_TAXON_BY_ID)
    def test_missing_parent_api_response_still_creates_child(self, mock_get_taxon):
        mock_get_taxon.return_value = None
        child_data = _taxon(300, 'Orphan species', rank='SPECIES', parent=999)
        result = _find_or_create_taxonomy(child_data, 'http://bims.test', self.remote_cache)
        self.assertIsNotNone(result)
        self.assertIsNone(result.parent)

    @mock.patch(_PATCH_GET_TAXON_BY_ID)
    def test_existing_taxonomy_gets_parent_filled(self, mock_get_taxon):
        parent_data = _taxon(400, 'LateParent', rank='GENUS')
        mock_get_taxon.return_value = parent_data

        existing = Taxonomy.objects.create(
            canonical_name='LateParent child', scientific_name='LateParent child',
            rank='SPECIES',
        )
        child_data = _taxon(401, 'LateParent child', rank='SPECIES', parent=400)
        _find_or_create_taxonomy(child_data, 'http://bims.test', self.remote_cache)
        existing.refresh_from_db()
        self.assertIsNotNone(existing.parent)
        self.assertEqual(existing.parent.canonical_name, 'LateParent')

    @mock.patch(_PATCH_GET_TAXON_BY_ID)
    def test_synonym_parent_ignored_and_accepted_taxonomy_resolved(self, mock_get_taxon):
        accepted_data = _taxon(1001, 'Thraulus', rank='GENUS')
        mock_get_taxon.return_value = accepted_data

        synonym_data = _taxon(
            1000,
            'Thraululus',
            rank='GENUS',
            parent=9999,
            taxonomic_status='SYNONYM',
            accepted_taxonomy=1001,
        )
        result = _find_or_create_taxonomy(
            synonym_data, 'http://bims.test', self.remote_cache
        )

        self.assertIsNone(result.parent)
        self.assertIsNotNone(result.accepted_taxonomy)
        self.assertEqual(result.accepted_taxonomy.canonical_name, 'Thraulus')
        mock_get_taxon.assert_called_once_with('http://bims.test', 1001)

    @mock.patch(_PATCH_GET_TAXON_BY_ID)
    def test_synonym_genus_parent_cycle_does_not_recurse(self, mock_get_taxon):
        accepted_data = _taxon(2001, 'Thraulus', rank='GENUS', parent=2000)
        synonym_data = _taxon(
            2000,
            'Thraululus',
            rank='GENUS',
            taxonomic_status='SYNONYM',
            accepted_taxonomy=2001,
        )

        def _side_effect(base_url, taxon_id):
            return {
                2000: synonym_data,
                2001: accepted_data,
            }.get(taxon_id)

        mock_get_taxon.side_effect = _side_effect

        result = _find_or_create_taxonomy(
            synonym_data, 'http://bims.test', self.remote_cache
        )

        result.refresh_from_db()
        accepted = Taxonomy.objects.get(canonical_name='Thraulus', rank='GENUS')
        self.assertEqual(result.accepted_taxonomy_id, accepted.pk)
        self.assertIsNone(result.parent)
        self.assertIsNone(accepted.parent)

    # -- additional_data merging -------------------------------------------

    @mock.patch(_PATCH_GET_TAXON_BY_ID)
    def test_additional_data_merged_into_new_taxon(self, mock_get_taxon):
        mock_get_taxon.return_value = None
        remote_additional = {'Kingdom': 'Animalia', 'Family': 'Cichlidae'}
        data = _taxon(500, 'Merged species', rank='SPECIES', additional_data=remote_additional)
        result = _find_or_create_taxonomy(data, 'http://bims.test', self.remote_cache)
        self.assertEqual(result.additional_data['Kingdom'], 'Animalia')
        self.assertEqual(result.additional_data['Family'], 'Cichlidae')

    @mock.patch(_PATCH_GET_TAXON_BY_ID)
    def test_local_additional_data_wins_on_conflict(self, mock_get_taxon):
        mock_get_taxon.return_value = None
        existing = Taxonomy.objects.create(
            canonical_name='Conflict species', scientific_name='Conflict species',
            rank='SPECIES', additional_data={'Key': 'local_value'},
        )
        remote_additional = {'Key': 'remote_value', 'NewKey': 'new'}
        data = _taxon(501, 'Conflict species', rank='SPECIES', additional_data=remote_additional)
        _find_or_create_taxonomy(data, 'http://bims.test', self.remote_cache)
        existing.refresh_from_db()
        self.assertEqual(existing.additional_data['Key'], 'local_value')
        self.assertEqual(existing.additional_data['NewKey'], 'new')

    # -- upstream_taxon_id matching (priority 1) ----------------------------

    def test_finds_by_upstream_taxon_id_in_group(self):
        """Membership with upstream_taxon_id matches before gbif_key / name."""
        taxon_group = TaxonGroupF.create()
        existing = Taxonomy.objects.create(
            canonical_name='Upstream matched species',
            scientific_name='Upstream matched species',
            rank='SPECIES',
        )
        TaxonGroupTaxonomy.objects.create(
            taxongroup=taxon_group,
            taxonomy=existing,
            upstream_taxon_id='777',
        )
        data = _taxon(777, 'Upstream matched species', rank='SPECIES')
        result = _find_or_create_taxonomy(
            data, 'http://bims.test', self.remote_cache, target_group=taxon_group
        )
        self.assertEqual(result.id, existing.id)
        self.assertEqual(Taxonomy.objects.filter(
            canonical_name='Upstream matched species', rank='SPECIES'
        ).count(), 1)

    def test_upstream_taxon_id_match_takes_priority_over_gbif_key(self):
        """upstream_taxon_id match wins even when gbif_key points elsewhere."""
        taxon_group = TaxonGroupF.create()
        correct = Taxonomy.objects.create(
            canonical_name='Correct species', scientific_name='Correct species',
            rank='SPECIES', gbif_key=None,
        )
        decoy = Taxonomy.objects.create(
            canonical_name='Decoy species', scientific_name='Decoy species',
            rank='SPECIES', gbif_key=99999,
        )
        TaxonGroupTaxonomy.objects.create(
            taxongroup=taxon_group,
            taxonomy=correct,
            upstream_taxon_id='888',
        )
        data = _taxon(888, 'Decoy species', rank='SPECIES', gbif_key=99999)
        result = _find_or_create_taxonomy(
            data, 'http://bims.test', self.remote_cache, target_group=taxon_group
        )
        self.assertEqual(result.id, correct.id)

    def test_no_target_group_skips_upstream_id_lookup(self):
        """Without target_group the upstream_taxon_id path is not used."""
        existing = Taxonomy.objects.create(
            canonical_name='Gbif species', scientific_name='Gbif species',
            rank='SPECIES', gbif_key=55555,
        )
        data = _taxon(55, 'Gbif species', rank='SPECIES', gbif_key=55555)
        result = _find_or_create_taxonomy(
            data, 'http://bims.test', self.remote_cache, target_group=None
        )
        self.assertEqual(result.id, existing.id)

    # -- tag application ----------------------------------------------------

    @mock.patch(_PATCH_GET_TAXON_BY_ID)
    def test_tags_applied_to_created_taxonomy(self, mock_get_taxon):
        mock_get_taxon.return_value = None
        data = _taxon(600, 'Tagged species', rank='SPECIES',
                      tag_list='aquatic (#51FF3E), freshwater (#FF5733)')
        result = _find_or_create_taxonomy(data, 'http://bims.test', self.remote_cache)
        tag_names = [t.name for t in result.tags.all()]
        self.assertIn('aquatic', tag_names)
        self.assertIn('freshwater', tag_names)

    @mock.patch(_PATCH_GET_TAXON_BY_ID)
    def test_plain_tag_applied(self, mock_get_taxon):
        mock_get_taxon.return_value = None
        data = _taxon(601, 'Plain tagged', rank='SPECIES', tag_list='test')
        result = _find_or_create_taxonomy(data, 'http://bims.test', self.remote_cache)
        self.assertIn('test', [t.name for t in result.tags.all()])


# ===========================================================================
# harvest_bims_species Celery task
# ===========================================================================

class TestHarvestBimsSpeciesTask(FastTenantTestCase):

    def setUp(self):
        self.taxon_group = TaxonGroupF.create()
        self.user = UserF.create()
        self.schema_name = connection.schema_name

    def _make_session(self, import_mode='existing', module_group=None, base_url='http://bims.test'):
        if module_group is None:
            module_group = self.taxon_group if import_mode == 'existing' else None
        session = HarvestSession.objects.create(
            harvester=self.user,
            module_group=module_group,
            category='bims',
            additional_data={
                'base_url': base_url,
                'remote_group_id': 3,
                'remote_group_name': 'Fish',
                'import_mode': import_mode,
            },
        )
        session.log_file.save(f'bims-test-{session.id}.log', ContentFile(b''))
        return session

    # -- helpers ------------------------------------------------------------

    def _run(self, session, taxa=None, get_taxon_by_id_map=None, auto_validate=True):
        """Run the task with all external calls mocked."""
        taxa = taxa or []
        get_taxon_by_id_map = get_taxon_by_id_map or {}

        def _side_get_taxon(base_url, taxon_id):
            return get_taxon_by_id_map.get(taxon_id)

        with mock.patch(_PATCH_DISCONNECT), \
             mock.patch(_PATCH_CONNECT), \
             mock.patch(_PATCH_PREFS) as mock_prefs, \
             mock.patch(_PATCH_GET_ALL_TAXA, return_value=iter(taxa)), \
             mock.patch(_PATCH_GET_TAXON_BY_ID, side_effect=_side_get_taxon), \
             mock.patch(_PATCH_GET_GROUPS, return_value=[]):
            mock_prefs.SiteSetting.auto_validate_taxa_on_upload = auto_validate
            harvest_bims_species(session.id, schema_name=self.schema_name)

    # -- session lifecycle --------------------------------------------------

    def test_session_marked_finished(self):
        taxa = [_taxon(1, 'Abactochromis labrosus', rank='SPECIES', gbif_key=5961874)]
        session = self._make_session()
        self._run(session, taxa=taxa)
        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertIn('Finished', session.status)
        self.assertEqual(session.additional_data['total_processed'], 1)
        self.assertEqual(session.additional_data['total_skipped'], 0)
        self.assertIn('finished_at', session.additional_data)

    def test_session_aborts_on_missing_config(self):
        session = HarvestSession.objects.create(
            harvester=self.user,
            category='bims',
            additional_data={},
        )
        session.log_file.save('bims-abort-test.log', ContentFile(b''))
        with mock.patch(_PATCH_DISCONNECT), mock.patch(_PATCH_CONNECT):
            harvest_bims_species(session.id, schema_name=self.schema_name)
        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertIn('Failed', session.status)

    def test_nonexistent_session_does_not_raise(self):
        with mock.patch(_PATCH_DISCONNECT), mock.patch(_PATCH_CONNECT):
            harvest_bims_species(99999, schema_name=self.schema_name)

    # -- taxon creation and group assignment --------------------------------

    def test_taxa_added_to_existing_taxon_group(self):
        taxa = [
            _taxon(1, 'Abactochromis labrosus', rank='SPECIES', gbif_key=5961874),
            _taxon(2, 'Abcandonopsis aula', rank='SPECIES'),
        ]
        session = self._make_session()
        self._run(session, taxa=taxa)
        self.assertEqual(self.taxon_group.taxonomies.count(), 2)

    def test_existing_taxonomy_matched_by_gbif_key(self):
        existing = Taxonomy.objects.create(
            canonical_name='Abactochromis labrosus', scientific_name='Abactochromis labrosus',
            rank='SPECIES', gbif_key=5961874,
        )
        taxa = [_taxon(1, 'Abactochromis labrosus', rank='SPECIES', gbif_key=5961874)]
        session = self._make_session()
        self._run(session, taxa=taxa)
        # Must not create a duplicate
        self.assertEqual(
            Taxonomy.objects.filter(gbif_key=5961874).count(), 1
        )
        self.assertIn(existing, self.taxon_group.taxonomies.all())

    def test_additional_data_imported(self):
        remote_data = {'Kingdom': 'Animalia', 'Family': 'Cichlidae'}
        taxa = [_taxon(1, 'Data species', rank='SPECIES', additional_data=remote_data)]
        session = self._make_session()
        self._run(session, taxa=taxa)
        taxonomy = Taxonomy.objects.get(canonical_name='Data species', rank='SPECIES')
        self.assertEqual(taxonomy.additional_data['Kingdom'], 'Animalia')

    def test_tags_imported(self):
        taxa = [_taxon(1, 'Tagged fish', rank='SPECIES',
                       tag_list='aquatic (#51FF3E), freshwater (#FF5733)')]
        session = self._make_session()
        self._run(session, taxa=taxa)
        taxonomy = Taxonomy.objects.get(canonical_name='Tagged fish', rank='SPECIES')
        tag_names = [t.name for t in taxonomy.tags.all()]
        self.assertIn('aquatic', tag_names)
        self.assertIn('freshwater', tag_names)
        self.assertTrue(TagGroup.objects.filter(colour__iexact='#51FF3E').exists())
        self.assertTrue(TagGroup.objects.filter(colour__iexact='#FF5733').exists())

    def test_existing_group_taxa_marked_unvalidated_when_auto_validate_disabled(self):
        taxa = [_taxon(1, 'Needs review', rank='SPECIES')]
        session = self._make_session()
        self._run(session, taxa=taxa, auto_validate=False)
        through_model = self.taxon_group.taxonomies.through
        relation = through_model.objects.get(
            taxongroup=self.taxon_group,
            taxonomy=Taxonomy.objects.get(canonical_name='Needs review', rank='SPECIES'),
        )
        self.assertFalse(relation.is_validated)

    def test_parent_resolved_and_linked(self):
        parent_data = _taxon(100, 'Cichlidae', rank='FAMILY')
        taxa = [_taxon(101, 'Cichlidae species', rank='SPECIES', parent=100)]
        session = self._make_session()
        self._run(session, taxa=taxa, get_taxon_by_id_map={100: parent_data})
        child = Taxonomy.objects.get(canonical_name='Cichlidae species', rank='SPECIES')
        self.assertIsNotNone(child.parent)
        self.assertEqual(child.parent.canonical_name, 'Cichlidae')

    def test_taxon_skipped_on_empty_canonical_name(self):
        taxa = [_taxon(1, '')]
        session = self._make_session()
        self._run(session, taxa=taxa)
        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertEqual(self.taxon_group.taxonomies.count(), 0)
        self.assertEqual(session.additional_data['total_processed'], 0)
        self.assertEqual(session.additional_data['total_skipped'], 1)

    # -- upstream_taxon_id stored on membership -----------------------------

    def test_upstream_taxon_id_stored_on_membership(self):
        taxa = [_taxon(42, 'Upstream id species', rank='SPECIES')]
        session = self._make_session()
        self._run(session, taxa=taxa)
        taxonomy = Taxonomy.objects.get(canonical_name='Upstream id species', rank='SPECIES')
        membership = self.taxon_group.taxonomies.through.objects.get(
            taxongroup=self.taxon_group,
            taxonomy=taxonomy,
        )
        self.assertEqual(membership.upstream_taxon_id, '42')

    def test_upstream_taxon_id_updated_on_reharvest(self):
        """A second harvest run updates upstream_taxon_id on the existing membership."""
        existing = Taxonomy.objects.create(
            canonical_name='Reharvest species', scientific_name='Reharvest species',
            rank='SPECIES',
        )
        # Create membership without upstream_taxon_id (simulates pre-feature data)
        TaxonGroupTaxonomy.objects.create(
            taxongroup=self.taxon_group,
            taxonomy=existing,
            upstream_taxon_id='',
        )
        taxa = [_taxon(99, 'Reharvest species', rank='SPECIES')]
        session = self._make_session()
        self._run(session, taxa=taxa)
        membership = self.taxon_group.taxonomies.through.objects.get(
            taxongroup=self.taxon_group,
            taxonomy=existing,
        )
        self.assertEqual(membership.upstream_taxon_id, '99')

    def test_reharvest_matches_by_upstream_taxon_id_not_name(self):
        """On re-harvest the existing membership is found by upstream_taxon_id,
        so a renamed canonical_name on the remote does not create a duplicate."""
        existing = Taxonomy.objects.create(
            canonical_name='Original name', scientific_name='Original name',
            rank='SPECIES',
        )
        TaxonGroupTaxonomy.objects.create(
            taxongroup=self.taxon_group,
            taxonomy=existing,
            upstream_taxon_id='200',
        )
        # Remote now returns the same ID but a different canonical_name
        taxa = [_taxon(200, 'Renamed species', rank='SPECIES')]
        session = self._make_session()
        self._run(session, taxa=taxa)
        # Must not create a new Taxonomy record
        self.assertEqual(
            Taxonomy.objects.filter(rank='SPECIES').count(), 1
        )
        self.assertEqual(self.taxon_group.taxonomies.count(), 1)
        self.assertEqual(self.taxon_group.taxonomies.first().id, existing.id)

    # -- import modes -------------------------------------------------------

    def test_import_mode_new_creates_taxon_group(self):
        session = self._make_session(import_mode='new')
        taxa = [_taxon(1, 'Fish species', rank='SPECIES')]
        with mock.patch(_PATCH_DISCONNECT), \
             mock.patch(_PATCH_CONNECT), \
             mock.patch(_PATCH_PREFS) as mock_prefs, \
             mock.patch(_PATCH_GET_ALL_TAXA, return_value=iter(taxa)), \
             mock.patch(_PATCH_GET_TAXON_BY_ID, return_value=None), \
             mock.patch(_PATCH_GET_GROUPS, return_value=[]), \
             mock.patch('django.contrib.sites.models.Site.objects.get_current') as mock_get_current_site:
            from django.contrib.sites.models import Site
            mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
            site, _ = Site.objects.get_or_create(
                id=1,
                defaults={'domain': 'example.com', 'name': 'example.com'},
            )
            mock_get_current_site.return_value = site
            harvest_bims_species(session.id, schema_name=self.schema_name)
        from bims.models.taxon_group import TaxonGroup
        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertIsNotNone(session.module_group)
        self.assertEqual(session.module_group.name, 'Fish')
        self.assertTrue(
            TaxonGroup.objects.filter(
                id=session.module_group_id,
                category='SPECIES_MODULE',
            ).exists()
        )

    def test_import_mode_new_uses_remote_group_name_lookup_when_missing(self):
        session = self._make_session(import_mode='new')
        session.additional_data['remote_group_name'] = ''
        session.save(update_fields=['additional_data'])
        taxa = [_taxon(1, 'Fish species', rank='SPECIES')]
        with mock.patch(_PATCH_DISCONNECT), \
             mock.patch(_PATCH_CONNECT), \
             mock.patch(_PATCH_PREFS) as mock_prefs, \
             mock.patch(_PATCH_GET_ALL_TAXA, return_value=iter(taxa)), \
             mock.patch(_PATCH_GET_TAXON_BY_ID, return_value=None), \
             mock.patch(_PATCH_GET_GROUPS, return_value=[{'id': 3, 'name': 'Remote Fish'}]), \
             mock.patch('django.contrib.sites.models.Site.objects.get_current') as mock_get_current_site:
            from django.contrib.sites.models import Site
            mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
            site, _ = Site.objects.get_or_create(
                id=1,
                defaults={'domain': 'example.com', 'name': 'example.com'},
            )
            mock_get_current_site.return_value = site
            harvest_bims_species(session.id, schema_name=self.schema_name)
        session.refresh_from_db()
        self.assertEqual(session.module_group.name, 'Remote Fish')

    def test_import_mode_existing_no_group_aborts(self):
        session = HarvestSession.objects.create(
            harvester=self.user,
            module_group=None,
            category='bims',
            additional_data={
                'base_url': 'http://bims.test',
                'remote_group_id': 3,
                'remote_group_name': 'Fish',
                'import_mode': 'existing',
            },
        )
        session.log_file.save('bims-no-group.log', ContentFile(b''))
        with mock.patch(_PATCH_DISCONNECT), mock.patch(_PATCH_CONNECT):
            harvest_bims_species(session.id, schema_name=self.schema_name)
        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertIn('Failed', session.status)

    # -- cancellation -------------------------------------------------------

    def test_canceled_session_stops_processing(self):
        # Generate many taxa but cancel the session mid-run
        taxa = [_taxon(i, f'Species {i}', rank='SPECIES') for i in range(1, 20)]

        call_count = 0

        def _canceling_get_all(*args, **kwargs):
            nonlocal call_count
            for taxon in taxa:
                call_count += 1
                if call_count == 5:
                    HarvestSession.objects.filter(
                        id=session.id
                    ).update(canceled=True)
                yield taxon

        session = self._make_session()
        with mock.patch(_PATCH_DISCONNECT), \
             mock.patch(_PATCH_CONNECT), \
             mock.patch(_PATCH_PREFS) as mock_prefs, \
             mock.patch(_PATCH_GET_ALL_TAXA, side_effect=_canceling_get_all), \
             mock.patch(_PATCH_GET_TAXON_BY_ID, return_value=None), \
             mock.patch(_PATCH_GET_GROUPS, return_value=[]):
            mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
            harvest_bims_species(session.id, schema_name=self.schema_name)

        session.refresh_from_db()
        self.assertFalse(session.finished)
        self.assertIn('Canceled', session.status)
        self.assertEqual(session.additional_data['total_processed'], self.taxon_group.taxonomies.count())
        self.assertEqual(session.additional_data['total_skipped'], 0)
        # Fewer than all taxa should have been processed
        self.assertLess(self.taxon_group.taxonomies.count(), 19)


# ===========================================================================
# Conflict resolution policy (_find_or_create_taxonomy with is_readonly flag)
# ===========================================================================

class TestConflictResolutionPolicy(FastTenantTestCase):
    """
    Verifies the two conflict resolution strategies applied by
    _find_or_create_taxonomy depending on is_readonly.
    """

    def setUp(self):
        self.remote_cache = {}
        self.log_lines = []

    def _log(self, msg):
        self.log_lines.append(msg)

    def _run(self, taxon_data, is_readonly=False, target_group=None):
        return _find_or_create_taxonomy(
            taxon_data, 'http://bims.test', self.remote_cache,
            target_group=target_group,
            is_readonly=is_readonly,
            _log=self._log,
        )

    # -- additional_data: local wins (non-readonly) -------------------------

    def test_non_readonly_additional_data_local_wins(self):
        existing = Taxonomy.objects.create(
            canonical_name='Local wins sp', scientific_name='Local wins sp',
            rank='SPECIES', additional_data={'key': 'local_value'},
        )
        data = _taxon(1, 'Local wins sp', rank='SPECIES',
                      additional_data={'key': 'remote_value', 'new_key': 'new'})
        self._run(data, is_readonly=False)
        existing.refresh_from_db()
        self.assertEqual(existing.additional_data['key'], 'local_value')
        self.assertEqual(existing.additional_data['new_key'], 'new')

    # -- additional_data: remote wins (readonly) ----------------------------

    def test_readonly_additional_data_remote_wins(self):
        existing = Taxonomy.objects.create(
            canonical_name='Remote wins sp', scientific_name='Remote wins sp',
            rank='SPECIES', additional_data={'key': 'local_value'},
        )
        data = _taxon(2, 'Remote wins sp', rank='SPECIES',
                      additional_data={'key': 'remote_value', 'new_key': 'new'})
        self._run(data, is_readonly=True)
        existing.refresh_from_db()
        self.assertEqual(existing.additional_data['key'], 'remote_value')
        self.assertEqual(existing.additional_data['new_key'], 'new')

    def test_readonly_additional_data_override_is_logged(self):
        Taxonomy.objects.create(
            canonical_name='Log override sp', scientific_name='Log override sp',
            rank='SPECIES', additional_data={'key': 'local_value'},
        )
        data = _taxon(3, 'Log override sp', rank='SPECIES',
                      additional_data={'key': 'remote_value'})
        self._run(data, is_readonly=True)
        self.assertTrue(any('DIVERGENCE' in line and 'key' in line
                            for line in self.log_lines))

    def test_non_readonly_additional_data_no_divergence_log(self):
        Taxonomy.objects.create(
            canonical_name='No log sp', scientific_name='No log sp',
            rank='SPECIES', additional_data={'key': 'local_value'},
        )
        data = _taxon(4, 'No log sp', rank='SPECIES',
                      additional_data={'key': 'remote_value'})
        self._run(data, is_readonly=False)
        self.assertFalse(any('DIVERGENCE' in line for line in self.log_lines))

    # -- canonical_name: never updated (non-readonly) -----------------------

    def test_non_readonly_canonical_name_not_updated(self):
        existing = Taxonomy.objects.create(
            canonical_name='Original name', scientific_name='Original name',
            rank='SPECIES',
        )
        data = _taxon(5, 'Changed name', rank='SPECIES')
        # Force match by gbif_key or name — here by name since it still matches
        # Use a fresh taxon so it matches by 'Original name' via gbif_key shortcut
        existing2 = Taxonomy.objects.create(
            canonical_name='Stable name', scientific_name='Stable name',
            rank='SPECIES', gbif_key=77001,
        )
        data2 = _taxon(50, 'Different remote name', rank='SPECIES', gbif_key=77001)
        self._run(data2, is_readonly=False)
        existing2.refresh_from_db()
        self.assertEqual(existing2.canonical_name, 'Stable name')

    # -- canonical_name: updated when different (readonly) ------------------

    def test_readonly_canonical_name_updated(self):
        existing = Taxonomy.objects.create(
            canonical_name='Old name', scientific_name='Old name',
            rank='SPECIES', gbif_key=88001,
        )
        data = _taxon(6, 'New name', rank='SPECIES', gbif_key=88001)
        self._run(data, is_readonly=True)
        existing.refresh_from_db()
        self.assertEqual(existing.canonical_name, 'New name')

    def test_readonly_canonical_name_change_logged(self):
        Taxonomy.objects.create(
            canonical_name='Will change', scientific_name='Will change',
            rank='SPECIES', gbif_key=88002,
        )
        data = _taxon(7, 'Has changed', rank='SPECIES', gbif_key=88002)
        self._run(data, is_readonly=True)
        self.assertTrue(any('DIVERGENCE' in line and 'canonical_name' in line
                            for line in self.log_lines))

    # -- author: never updated (non-readonly) -------------------------------

    def test_non_readonly_author_not_updated(self):
        existing = Taxonomy.objects.create(
            canonical_name='Author sp', scientific_name='Author sp',
            rank='SPECIES', author='Local Author',
        )
        data = _taxon(8, 'Author sp', rank='SPECIES', author='Remote Author')
        self._run(data, is_readonly=False)
        existing.refresh_from_db()
        self.assertEqual(existing.author, 'Local Author')

    # -- author: updated when different (readonly) --------------------------

    def test_readonly_author_updated(self):
        existing = Taxonomy.objects.create(
            canonical_name='Auth update sp', scientific_name='Auth update sp',
            rank='SPECIES', author='Old Author', gbif_key=90001,
        )
        data = _taxon(9, 'Auth update sp', rank='SPECIES',
                      author='New Author', gbif_key=90001)
        self._run(data, is_readonly=True)
        existing.refresh_from_db()
        self.assertEqual(existing.author, 'New Author')

    def test_readonly_author_change_logged(self):
        Taxonomy.objects.create(
            canonical_name='Auth log sp', scientific_name='Auth log sp',
            rank='SPECIES', author='Old Author', gbif_key=90002,
        )
        data = _taxon(10, 'Auth log sp', rank='SPECIES',
                      author='New Author', gbif_key=90002)
        self._run(data, is_readonly=True)
        self.assertTrue(any('DIVERGENCE' in line and 'author' in line
                            for line in self.log_lines))

    # -- taxonomic_status: never updated (non-readonly) ---------------------

    def test_non_readonly_taxonomic_status_not_updated(self):
        existing = Taxonomy.objects.create(
            canonical_name='Status sp', scientific_name='Status sp',
            rank='SPECIES', taxonomic_status='ACCEPTED', gbif_key=91001,
        )
        data = _taxon(11, 'Status sp', rank='SPECIES',
                      taxonomic_status='SYNONYM', gbif_key=91001)
        self._run(data, is_readonly=False)
        existing.refresh_from_db()
        self.assertEqual(existing.taxonomic_status, 'ACCEPTED')

    # -- taxonomic_status: updated when different (readonly) ----------------

    def test_readonly_taxonomic_status_updated(self):
        existing = Taxonomy.objects.create(
            canonical_name='Status update sp', scientific_name='Status update sp',
            rank='SPECIES', taxonomic_status='ACCEPTED', gbif_key=91002,
        )
        data = _taxon(12, 'Status update sp', rank='SPECIES',
                      taxonomic_status='SYNONYM', gbif_key=91002)
        self._run(data, is_readonly=True)
        existing.refresh_from_db()
        self.assertEqual(existing.taxonomic_status, 'SYNONYM')

    def test_readonly_taxonomic_status_change_logged(self):
        Taxonomy.objects.create(
            canonical_name='Status log sp', scientific_name='Status log sp',
            rank='SPECIES', taxonomic_status='ACCEPTED', gbif_key=91003,
        )
        data = _taxon(13, 'Status log sp', rank='SPECIES',
                      taxonomic_status='DOUBTFUL', gbif_key=91003)
        self._run(data, is_readonly=True)
        self.assertTrue(any('DIVERGENCE' in line and 'taxonomic_status' in line
                            for line in self.log_lines))

    # -- parent: filled when missing (non-readonly) -------------------------

    def test_non_readonly_parent_filled_when_missing(self):
        parent = Taxonomy.objects.create(
            canonical_name='Parent genus', scientific_name='Parent genus',
            rank='GENUS',
        )
        existing = Taxonomy.objects.create(
            canonical_name='Parentless sp', scientific_name='Parentless sp',
            rank='SPECIES', gbif_key=92001,
        )
        self.remote_cache[500] = parent
        data = _taxon(14, 'Parentless sp', rank='SPECIES',
                      gbif_key=92001, parent=500)
        self._run(data, is_readonly=False)
        existing.refresh_from_db()
        self.assertEqual(existing.parent_id, parent.pk)

    def test_non_readonly_existing_parent_not_replaced(self):
        old_parent = Taxonomy.objects.create(
            canonical_name='Old parent', scientific_name='Old parent', rank='GENUS',
        )
        new_parent = Taxonomy.objects.create(
            canonical_name='New parent', scientific_name='New parent', rank='GENUS',
        )
        existing = Taxonomy.objects.create(
            canonical_name='Has parent sp', scientific_name='Has parent sp',
            rank='SPECIES', gbif_key=92002, parent=old_parent,
        )
        self.remote_cache[600] = new_parent
        data = _taxon(15, 'Has parent sp', rank='SPECIES',
                      gbif_key=92002, parent=600)
        self._run(data, is_readonly=False)
        existing.refresh_from_db()
        self.assertEqual(existing.parent_id, old_parent.pk)

    # -- parent: always synced to upstream (readonly) -----------------------

    def test_readonly_parent_updated_to_match_upstream(self):
        old_parent = Taxonomy.objects.create(
            canonical_name='Old parent ro', scientific_name='Old parent ro',
            rank='GENUS',
        )
        new_parent = Taxonomy.objects.create(
            canonical_name='New parent ro', scientific_name='New parent ro',
            rank='GENUS',
        )
        existing = Taxonomy.objects.create(
            canonical_name='Parent sync sp', scientific_name='Parent sync sp',
            rank='SPECIES', gbif_key=93001, parent=old_parent,
        )
        self.remote_cache[700] = new_parent
        data = _taxon(16, 'Parent sync sp', rank='SPECIES',
                      gbif_key=93001, parent=700)
        self._run(data, is_readonly=True)
        existing.refresh_from_db()
        self.assertEqual(existing.parent_id, new_parent.pk)

    def test_readonly_parent_change_logged(self):
        old_parent = Taxonomy.objects.create(
            canonical_name='Old par log', scientific_name='Old par log', rank='GENUS',
        )
        new_parent = Taxonomy.objects.create(
            canonical_name='New par log', scientific_name='New par log', rank='GENUS',
        )
        Taxonomy.objects.create(
            canonical_name='Parent log sp', scientific_name='Parent log sp',
            rank='SPECIES', gbif_key=93002, parent=old_parent,
        )
        self.remote_cache[800] = new_parent
        data = _taxon(17, 'Parent log sp', rank='SPECIES',
                      gbif_key=93002, parent=800)
        self._run(data, is_readonly=True)
        self.assertTrue(any('DIVERGENCE' in line and 'parent' in line
                            for line in self.log_lines))

    # -- aphia_id / col_id: filled when missing (non-readonly) --------------

    def test_non_readonly_aphia_id_and_col_id_filled_when_missing(self):
        existing = Taxonomy.objects.create(
            canonical_name='Worms filled sp', scientific_name='Worms filled sp',
            rank='SPECIES', gbif_key=94001,
        )
        data = _taxon(18, 'Worms filled sp', rank='SPECIES',
                      gbif_key=94001, aphia_id=131160, col_id='ABCD1234')
        self._run(data, is_readonly=False)
        existing.refresh_from_db()
        self.assertEqual(existing.aphia_id, 131160)
        self.assertEqual(existing.col_id, 'ABCD1234')

    def test_non_readonly_existing_aphia_id_and_col_id_not_replaced(self):
        existing = Taxonomy.objects.create(
            canonical_name='Worms stable sp', scientific_name='Worms stable sp',
            rank='SPECIES', gbif_key=94002, aphia_id=1, col_id='OLDID',
        )
        data = _taxon(19, 'Worms stable sp', rank='SPECIES',
                      gbif_key=94002, aphia_id=131160, col_id='ABCD1234')
        self._run(data, is_readonly=False)
        existing.refresh_from_db()
        self.assertEqual(existing.aphia_id, 1)
        self.assertEqual(existing.col_id, 'OLDID')

    # -- aphia_id / col_id: always synced to upstream (readonly) ------------

    def test_readonly_aphia_id_and_col_id_updated_to_match_upstream(self):
        existing = Taxonomy.objects.create(
            canonical_name='Worms sync sp', scientific_name='Worms sync sp',
            rank='SPECIES', gbif_key=94003, aphia_id=1, col_id='OLDID',
        )
        data = _taxon(20, 'Worms sync sp', rank='SPECIES',
                      gbif_key=94003, aphia_id=131160, col_id='ABCD1234')
        self._run(data, is_readonly=True)
        existing.refresh_from_db()
        self.assertEqual(existing.aphia_id, 131160)
        self.assertEqual(existing.col_id, 'ABCD1234')

    # -- fada_id: filled when missing (non-readonly) -------------------------

    def test_non_readonly_fada_id_filled_when_missing(self):
        existing = Taxonomy.objects.create(
            canonical_name='Fada filled sp', scientific_name='Fada filled sp',
            rank='SPECIES', gbif_key=94004,
        )
        data = _taxon(21, 'Fada filled sp', rank='SPECIES',
                      gbif_key=94004, fada_id='FADA-2')
        self._run(data, is_readonly=False)
        existing.refresh_from_db()
        self.assertEqual(existing.fada_id, 'FADA-2')

    def test_non_readonly_existing_fada_id_not_replaced(self):
        existing = Taxonomy.objects.create(
            canonical_name='Fada stable sp', scientific_name='Fada stable sp',
            rank='SPECIES', gbif_key=94005, fada_id='FADA-OLD',
        )
        data = _taxon(22, 'Fada stable sp', rank='SPECIES',
                      gbif_key=94005, fada_id='FADA-NEW')
        self._run(data, is_readonly=False)
        existing.refresh_from_db()
        self.assertEqual(existing.fada_id, 'FADA-OLD')

    def test_non_readonly_fada_id_conflict_not_assigned(self):
        Taxonomy.objects.create(
            canonical_name='Fada owner sp', scientific_name='Fada owner sp',
            rank='SPECIES', fada_id='FADA-TAKEN',
        )
        existing = Taxonomy.objects.create(
            canonical_name='Fada wants sp', scientific_name='Fada wants sp',
            rank='SPECIES', gbif_key=94006,
        )
        data = _taxon(23, 'Fada wants sp', rank='SPECIES',
                      gbif_key=94006, fada_id='FADA-TAKEN')
        self._run(data, is_readonly=False)
        existing.refresh_from_db()
        self.assertIsNone(existing.fada_id)

    # -- fada_id: always synced to upstream (readonly) -----------------------

    def test_readonly_fada_id_updated_to_match_upstream(self):
        existing = Taxonomy.objects.create(
            canonical_name='Fada sync sp', scientific_name='Fada sync sp',
            rank='SPECIES', gbif_key=94007, fada_id='FADA-OLD2',
        )
        data = _taxon(24, 'Fada sync sp', rank='SPECIES',
                      gbif_key=94007, fada_id='FADA-NEW2')
        self._run(data, is_readonly=True)
        existing.refresh_from_db()
        self.assertEqual(existing.fada_id, 'FADA-NEW2')

    def test_readonly_fada_id_conflict_not_assigned(self):
        Taxonomy.objects.create(
            canonical_name='Fada owner ro sp', scientific_name='Fada owner ro sp',
            rank='SPECIES', fada_id='FADA-TAKEN-RO',
        )
        existing = Taxonomy.objects.create(
            canonical_name='Fada wants ro sp', scientific_name='Fada wants ro sp',
            rank='SPECIES', gbif_key=94008,
        )
        data = _taxon(25, 'Fada wants ro sp', rank='SPECIES',
                      gbif_key=94008, fada_id='FADA-TAKEN-RO')
        self._run(data, is_readonly=True)
        existing.refresh_from_db()
        self.assertIsNone(existing.fada_id)

    # -- taxonworks_id / taxonworks_otu_id: filled when missing (non-readonly)

    def test_non_readonly_taxonworks_ids_filled_when_missing(self):
        existing = Taxonomy.objects.create(
            canonical_name='TW filled sp', scientific_name='TW filled sp',
            rank='SPECIES', gbif_key=94009,
        )
        data = _taxon(26, 'TW filled sp', rank='SPECIES',
                      gbif_key=94009, taxonworks_id=42, taxonworks_otu_id=99)
        self._run(data, is_readonly=False)
        existing.refresh_from_db()
        self.assertEqual(existing.taxonworks_id, 42)
        self.assertEqual(existing.taxonworks_otu_id, 99)

    def test_non_readonly_existing_taxonworks_ids_not_replaced(self):
        existing = Taxonomy.objects.create(
            canonical_name='TW stable sp', scientific_name='TW stable sp',
            rank='SPECIES', gbif_key=94010, taxonworks_id=1, taxonworks_otu_id=2,
        )
        data = _taxon(27, 'TW stable sp', rank='SPECIES',
                      gbif_key=94010, taxonworks_id=42, taxonworks_otu_id=99)
        self._run(data, is_readonly=False)
        existing.refresh_from_db()
        self.assertEqual(existing.taxonworks_id, 1)
        self.assertEqual(existing.taxonworks_otu_id, 2)

    # -- taxonworks_id / taxonworks_otu_id: always synced (readonly) --------

    def test_readonly_taxonworks_ids_updated_to_match_upstream(self):
        existing = Taxonomy.objects.create(
            canonical_name='TW sync sp', scientific_name='TW sync sp',
            rank='SPECIES', gbif_key=94011, taxonworks_id=1, taxonworks_otu_id=2,
        )
        data = _taxon(28, 'TW sync sp', rank='SPECIES',
                      gbif_key=94011, taxonworks_id=42, taxonworks_otu_id=99)
        self._run(data, is_readonly=True)
        existing.refresh_from_db()
        self.assertEqual(existing.taxonworks_id, 42)
        self.assertEqual(existing.taxonworks_otu_id, 99)

    # -- tags: always additive regardless of group type ---------------------

    def test_tags_additive_for_non_readonly(self):
        existing = Taxonomy.objects.create(
            canonical_name='Tag sp nr', scientific_name='Tag sp nr', rank='SPECIES',
        )
        existing.tags.add('existing_tag')
        data = _taxon(18, 'Tag sp nr', rank='SPECIES', tag_list='new_tag')
        self._run(data, is_readonly=False)
        tag_names = [t.name for t in existing.tags.all()]
        self.assertIn('existing_tag', tag_names)
        self.assertIn('new_tag', tag_names)

    def test_tags_additive_for_readonly(self):
        existing = Taxonomy.objects.create(
            canonical_name='Tag sp ro', scientific_name='Tag sp ro', rank='SPECIES',
            gbif_key=94001,
        )
        existing.tags.add('existing_tag')
        data = _taxon(19, 'Tag sp ro', rank='SPECIES',
                      gbif_key=94001, tag_list='new_tag')
        self._run(data, is_readonly=True)
        tag_names = [t.name for t in existing.tags.all()]
        self.assertIn('existing_tag', tag_names)
        self.assertIn('new_tag', tag_names)

    # -- no spurious updates when data matches ------------------------------

    def test_readonly_no_update_when_data_identical(self):
        existing = Taxonomy.objects.create(
            canonical_name='Identical sp', scientific_name='Identical sp',
            rank='SPECIES', author='Same Author',
            taxonomic_status='ACCEPTED', gbif_key=95001,
            additional_data={'key': 'value'},
        )
        data = _taxon(20, 'Identical sp', rank='SPECIES',
                      author='Same Author', taxonomic_status='ACCEPTED',
                      gbif_key=95001, additional_data={'key': 'value'})
        self._run(data, is_readonly=True)
        self.assertFalse(any('DIVERGENCE' in line for line in self.log_lines))


# ===========================================================================
# Read-only taxon group behaviour in harvest task
# ===========================================================================

class TestHarvestBimsReadOnly(FastTenantTestCase):
    """
    Tests for is_readonly / upstream_url / upstream_id behaviour in the
    harvest_bims_species Celery task.
    """

    def setUp(self):
        self.user = UserF.create()
        self.schema_name = connection.schema_name

    def _make_session(self, import_mode='new', module_group=None,
                      base_url='http://bims.test', remote_group_id=3,
                      remote_group_name='Fish', remote_group_logo='',
                      mark_readonly=False):
        session = HarvestSession.objects.create(
            harvester=self.user,
            module_group=module_group,
            category='bims',
            additional_data={
                'base_url': base_url,
                'remote_group_id': remote_group_id,
                'remote_group_name': remote_group_name,
                'remote_group_logo': remote_group_logo,
                'import_mode': import_mode,
                'mark_readonly': mark_readonly,
            },
        )
        session.log_file.save(f'bims-ro-test-{session.id}.log', ContentFile(b''))
        return session

    def _run(self, session, taxa=None):
        taxa = taxa or []
        with mock.patch(_PATCH_DISCONNECT), \
             mock.patch(_PATCH_CONNECT), \
             mock.patch(_PATCH_PREFS) as mock_prefs, \
             mock.patch(_PATCH_GET_ALL_TAXA, return_value=iter(taxa)), \
             mock.patch(_PATCH_GET_TAXON_BY_ID, return_value=None), \
             mock.patch(_PATCH_GET_GROUPS, return_value=[]), \
             mock.patch('django.contrib.sites.models.Site.objects.get_current') as mock_site:
            from django.contrib.sites.models import Site
            mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
            site, _ = Site.objects.get_or_create(
                id=1,
                defaults={'domain': 'example.com', 'name': 'example.com'},
            )
            mock_site.return_value = site
            harvest_bims_species(session.id, schema_name=self.schema_name)

    # -- new mode: mark_readonly=True stamps upstream metadata ---------------

    def test_new_mode_mark_readonly_sets_is_readonly(self):
        session = self._make_session(import_mode='new', mark_readonly=True)
        self._run(session)
        session.refresh_from_db()
        self.assertTrue(session.module_group.is_readonly)

    def test_new_mode_mark_readonly_sets_upstream_url(self):
        session = self._make_session(import_mode='new', mark_readonly=True,
                                     base_url='http://bims.test')
        self._run(session)
        session.refresh_from_db()
        self.assertEqual(session.module_group.upstream_url, 'http://bims.test')

    def test_new_mode_mark_readonly_sets_upstream_id(self):
        session = self._make_session(import_mode='new', mark_readonly=True,
                                     remote_group_id=7)
        self._run(session)
        session.refresh_from_db()
        self.assertEqual(session.module_group.upstream_id, '7')

    def test_new_mode_without_mark_readonly_leaves_fields_blank(self):
        session = self._make_session(import_mode='new', mark_readonly=False)
        self._run(session)
        session.refresh_from_db()
        self.assertFalse(session.module_group.is_readonly)
        self.assertEqual(session.module_group.upstream_url, '')
        self.assertEqual(session.module_group.upstream_id, '')

    # -- new mode: re-harvest updates upstream metadata on existing group ----

    def test_new_mode_reharvest_updates_upstream_metadata(self):
        from bims.models.taxon_group import TaxonGroup
        from bims.enums.taxonomic_group_category import TaxonomicGroupCategory
        # Pre-create a group with the same name but missing upstream info
        existing_group = TaxonGroup.objects.create(
            name='Fish',
            category=TaxonomicGroupCategory.SPECIES_MODULE.name,
            is_readonly=False,
            upstream_url='',
            upstream_id='',
        )
        session = self._make_session(import_mode='new', mark_readonly=True,
                                     remote_group_name='Fish',
                                     base_url='http://bims.test',
                                     remote_group_id=3)
        self._run(session)
        existing_group.refresh_from_db()
        self.assertTrue(existing_group.is_readonly)
        self.assertEqual(existing_group.upstream_url, 'http://bims.test')
        self.assertEqual(existing_group.upstream_id, '3')

    def test_new_mode_downloads_remote_group_logo(self):
        response = mock.Mock()
        response.content = b'remote-logo-bytes'
        response.raise_for_status.return_value = None
        session = self._make_session(
            import_mode='new',
            remote_group_name='Fish',
            remote_group_logo='media/module_logo/fish.png',
        )
        with tempfile.TemporaryDirectory() as media_root:
            with self.settings(MEDIA_ROOT=media_root):
                with mock.patch(_PATCH_DISCONNECT), \
                     mock.patch(_PATCH_CONNECT), \
                     mock.patch(_PATCH_PREFS) as mock_prefs, \
                     mock.patch(_PATCH_GET_ALL_TAXA, return_value=iter([])), \
                     mock.patch(_PATCH_GET_TAXON_BY_ID, return_value=None), \
                     mock.patch('bims.tasks.harvest_bims_species.requests.get', return_value=response) as mock_get, \
                     mock.patch('django.contrib.sites.models.Site.objects.get_current') as mock_site:
                    from django.contrib.sites.models import Site
                    mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
                    site, _ = Site.objects.get_or_create(
                        id=1,
                        defaults={'domain': 'example.com', 'name': 'example.com'},
                    )
                    mock_site.return_value = site
                    harvest_bims_species(session.id, schema_name=self.schema_name)
                session.refresh_from_db()
                saved_logo_name = session.module_group.logo.name
                with session.module_group.logo.open('rb') as logo_file:
                    saved_logo_content = logo_file.read()

        self.assertTrue(saved_logo_name.endswith('fish.png'))
        self.assertEqual(saved_logo_content, b'remote-logo-bytes')
        mock_get.assert_called_once_with(
            'http://bims.test/media/module_logo/fish.png',
            timeout=30,
        )

    # -- existing mode: read-only group with matching source proceeds --------

    def test_existing_readonly_group_matching_source_proceeds(self):
        taxon_group = TaxonGroupF.create(
            is_readonly=True,
            upstream_url='http://bims.test',
            upstream_id='3',
        )
        session = self._make_session(
            import_mode='existing',
            module_group=taxon_group,
            base_url='http://bims.test',
            remote_group_id=3,
        )
        taxa = [_taxon(1, 'Matching species', rank='SPECIES')]
        self._run(session, taxa=taxa)
        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertNotIn('Failed', session.status)
        self.assertEqual(taxon_group.taxonomies.count(), 1)

    # -- existing mode: read-only group with wrong URL aborts ----------------

    def test_existing_readonly_group_wrong_url_aborts(self):
        taxon_group = TaxonGroupF.create(
            is_readonly=True,
            upstream_url='http://correct-bims.test',
            upstream_id='3',
        )
        session = self._make_session(
            import_mode='existing',
            module_group=taxon_group,
            base_url='http://wrong-bims.test',
            remote_group_id=3,
        )
        self._run(session)
        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertIn('Failed', session.status)
        self.assertIn('mismatch', session.status)
        self.assertEqual(taxon_group.taxonomies.count(), 0)

    # -- existing mode: read-only group with wrong group ID aborts -----------

    def test_existing_readonly_group_wrong_id_aborts(self):
        taxon_group = TaxonGroupF.create(
            is_readonly=True,
            upstream_url='http://bims.test',
            upstream_id='3',
        )
        session = self._make_session(
            import_mode='existing',
            module_group=taxon_group,
            base_url='http://bims.test',
            remote_group_id=99,
        )
        self._run(session)
        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertIn('Failed', session.status)
        self.assertEqual(taxon_group.taxonomies.count(), 0)

    # -- existing mode: non-readonly group is not blocked --------------------

    def test_existing_non_readonly_group_not_blocked(self):
        taxon_group = TaxonGroupF.create(is_readonly=False)
        session = self._make_session(
            import_mode='existing',
            module_group=taxon_group,
            base_url='http://any-url.test',
            remote_group_id=999,
        )
        taxa = [_taxon(1, 'Free species', rank='SPECIES')]
        self._run(session, taxa=taxa)
        session.refresh_from_db()
        self.assertTrue(session.finished)
        self.assertNotIn('Failed', session.status)
        self.assertEqual(taxon_group.taxonomies.count(), 1)


# ===========================================================================
# BimsFetchTaxonGroupsView AJAX endpoint
# ===========================================================================

_PATCH_GET_GROUPS_UTIL = 'bims.api_views.bims_fetch_taxon_groups.get_taxon_groups'
_PATCH_NORMALIZE = 'bims.api_views.bims_fetch_taxon_groups.normalize_bims_base_url'


class TestBimsFetchTaxonGroupsView(FastTenantTestCase):

    def setUp(self):
        from bims.api_views.bims_fetch_taxon_groups import BimsFetchTaxonGroupsView
        self.view = BimsFetchTaxonGroupsView.as_view()
        self.factory = RequestFactory()
        self.user = UserF.create()
        self.user.user_permissions.add(
            *list(
                __import__('django.contrib.auth.models', fromlist=['Permission'])
                .Permission.objects.filter(codename='can_harvest_species')
            )
        )
        # Grant permission via superuser shortcut for simplicity
        self.user.is_superuser = True
        self.user.save()

    def _get(self, params):
        request = self.factory.get('/api/bims-fetch-taxon-groups/', params)
        request.user = self.user
        return self.view(request)

    def test_returns_groups_from_remote(self):
        remote_groups = [
            {'id': 1, 'name': 'Fish', 'logo': ''},
            {'id': 2, 'name': 'Reptiles', 'logo': ''},
        ]
        with mock.patch(_PATCH_GET_GROUPS_UTIL, return_value=remote_groups), \
             mock.patch(_PATCH_NORMALIZE, side_effect=lambda x: x):
            response = self._get({'base_url': 'http://bims.test'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['results'], remote_groups)

    def test_missing_base_url_returns_400(self):
        response = self._get({})
        self.assertEqual(response.status_code, 400)

    def test_empty_base_url_returns_400(self):
        response = self._get({'base_url': ''})
        self.assertEqual(response.status_code, 400)

    def test_empty_group_list_returns_empty_results(self):
        with mock.patch(_PATCH_GET_GROUPS_UTIL, return_value=[]), \
             mock.patch(_PATCH_NORMALIZE, side_effect=lambda x: x):
            response = self._get({'base_url': 'http://bims.test'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['results'], [])

    def test_unauthenticated_user_gets_403(self):
        from django.contrib.auth.models import AnonymousUser
        request = self.factory.get('/api/bims-fetch-taxon-groups/', {'base_url': 'http://bims.test'})
        request.user = AnonymousUser()
        # UserPassesTestMixin raises PermissionDenied → 403
        from django.core.exceptions import PermissionDenied
        from bims.api_views.bims_fetch_taxon_groups import BimsFetchTaxonGroupsView
        view = BimsFetchTaxonGroupsView()
        view.request = request
        self.assertFalse(view.test_func())


# ===========================================================================
# bims_instance utility functions
# ===========================================================================

_PATCH_REQUESTS_GET = 'bims.utils.bims_instance.requests.get'
_PATCH_SLEEP = 'bims.utils.bims_instance.time.sleep'


class _MockResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise _requests.HTTPError(f'HTTP {self.status_code}')


class TestBimsInstanceUtilities(TestCase):
    """Unit tests for bims/utils/bims_instance.py (no DB needed)."""

    # -- normalize_bims_base_url -------------------------------------------

    def test_normalize_strips_trailing_slash(self):
        from bims.utils.bims_instance import normalize_bims_base_url
        self.assertEqual(normalize_bims_base_url('http://bims.test/'), 'http://bims.test')

    def test_normalize_strips_multiple_trailing_slashes(self):
        from bims.utils.bims_instance import normalize_bims_base_url
        self.assertEqual(normalize_bims_base_url('http://bims.test///'), 'http://bims.test')

    def test_normalize_leaves_clean_url_unchanged(self):
        from bims.utils.bims_instance import normalize_bims_base_url
        self.assertEqual(normalize_bims_base_url('http://bims.test'), 'http://bims.test')

    def test_normalize_empty_string(self):
        from bims.utils.bims_instance import normalize_bims_base_url
        self.assertEqual(normalize_bims_base_url(''), '')

    # -- get_taxon_groups --------------------------------------------------

    @mock.patch(_PATCH_REQUESTS_GET)
    def test_get_taxon_groups_returns_list(self, mock_get):
        from bims.utils.bims_instance import get_taxon_groups
        payload = [{'id': 1, 'name': 'Fish', 'logo': ''}]
        mock_get.return_value = _MockResponse(payload)
        result = get_taxon_groups('http://bims.test')
        self.assertEqual(result, payload)
        mock_get.assert_called_once_with(
            'http://bims.test/api/module-list/', params=None, timeout=mock.ANY
        )

    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_REQUESTS_GET)
    def test_get_taxon_groups_returns_empty_on_http_error(self, mock_get, mock_sleep):
        from bims.utils.bims_instance import get_taxon_groups
        mock_get.side_effect = _requests.HTTPError('500')
        result = get_taxon_groups('http://bims.test')
        self.assertEqual(result, [])

    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_REQUESTS_GET)
    def test_get_taxon_groups_retries_on_failure(self, mock_get, mock_sleep):
        from bims.utils.bims_instance import get_taxon_groups, RETRY_ATTEMPTS
        mock_get.side_effect = _requests.ConnectionError('down')
        get_taxon_groups('http://bims.test')
        self.assertEqual(mock_get.call_count, RETRY_ATTEMPTS)

    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_REQUESTS_GET)
    def test_get_taxon_groups_succeeds_on_second_attempt(self, mock_get, mock_sleep):
        from bims.utils.bims_instance import get_taxon_groups
        payload = [{'id': 2, 'name': 'Reptiles', 'logo': ''}]
        mock_get.side_effect = [
            _requests.ConnectionError('down'),
            _MockResponse(payload),
        ]
        result = get_taxon_groups('http://bims.test')
        self.assertEqual(result, payload)
        self.assertEqual(mock_get.call_count, 2)

    # -- get_taxon_by_id ---------------------------------------------------

    @mock.patch(_PATCH_REQUESTS_GET)
    def test_get_taxon_by_id_returns_dict(self, mock_get):
        from bims.utils.bims_instance import get_taxon_by_id
        payload = {'id': 441, 'canonical_name': 'Abactochromis labrosus', 'rank': 'SPECIES'}
        mock_get.return_value = _MockResponse(payload)
        result = get_taxon_by_id('http://bims.test', 441)
        self.assertEqual(result, payload)
        mock_get.assert_called_once_with(
            'http://bims.test/api/taxon/441/', params=None, timeout=mock.ANY
        )

    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_REQUESTS_GET)
    def test_get_taxon_by_id_returns_none_on_404(self, mock_get, mock_sleep):
        from bims.utils.bims_instance import get_taxon_by_id
        mock_get.side_effect = _requests.HTTPError('404')
        result = get_taxon_by_id('http://bims.test', 999)
        self.assertIsNone(result)

    # -- get_taxa_page -----------------------------------------------------

    @mock.patch(_PATCH_REQUESTS_GET)
    def test_get_taxa_page_passes_correct_params(self, mock_get):
        from bims.utils.bims_instance import get_taxa_page, PAGE_SIZE
        payload = {'count': 1, 'next': None, 'previous': None, 'results': []}
        mock_get.return_value = _MockResponse(payload)
        result = get_taxa_page('http://bims.test', taxon_group_id=3, page=2)
        self.assertEqual(result, payload)
        actual_params = mock_get.call_args[1]['params']
        self.assertEqual(actual_params['taxonGroup'], 3)
        self.assertEqual(actual_params['page'], 2)
        self.assertEqual(actual_params['page_size'], PAGE_SIZE)

    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_REQUESTS_GET)
    def test_get_taxa_page_returns_empty_dict_on_error(self, mock_get, mock_sleep):
        from bims.utils.bims_instance import get_taxa_page
        mock_get.side_effect = _requests.ConnectionError('down')
        result = get_taxa_page('http://bims.test', taxon_group_id=3)
        self.assertEqual(result, {})

    # -- get_all_taxa (generator) ------------------------------------------

    @mock.patch(_PATCH_REQUESTS_GET)
    def test_get_all_taxa_yields_all_results_single_page(self, mock_get):
        from bims.utils.bims_instance import get_all_taxa
        taxa = [
            {'id': 1, 'canonical_name': 'Species A', 'rank': 'SPECIES'},
            {'id': 2, 'canonical_name': 'Species B', 'rank': 'SPECIES'},
        ]
        mock_get.return_value = _MockResponse({
            'count': 2, 'next': None, 'previous': None, 'results': taxa,
        })
        result = list(get_all_taxa('http://bims.test', taxon_group_id=3))
        self.assertEqual(result, taxa)
        self.assertEqual(mock_get.call_count, 1)

    @mock.patch(_PATCH_REQUESTS_GET)
    def test_get_all_taxa_paginates_until_no_next(self, mock_get):
        from bims.utils.bims_instance import get_all_taxa
        page1 = {'count': 4, 'next': 'http://bims.test/api/taxa-list/?page=2',
                 'previous': None,
                 'results': [{'id': i, 'canonical_name': f'Sp {i}', 'rank': 'SPECIES'}
                              for i in range(1, 3)]}
        page2 = {'count': 4, 'next': None, 'previous': 'page1',
                 'results': [{'id': i, 'canonical_name': f'Sp {i}', 'rank': 'SPECIES'}
                              for i in range(3, 5)]}
        mock_get.side_effect = [_MockResponse(page1), _MockResponse(page2)]
        result = list(get_all_taxa('http://bims.test', taxon_group_id=3))
        self.assertEqual(len(result), 4)
        self.assertEqual(mock_get.call_count, 2)

    @mock.patch(_PATCH_SLEEP, return_value=None)
    @mock.patch(_PATCH_REQUESTS_GET)
    def test_get_all_taxa_stops_on_empty_response(self, mock_get, mock_sleep):
        from bims.utils.bims_instance import get_all_taxa
        mock_get.side_effect = _requests.ConnectionError('down')
        result = list(get_all_taxa('http://bims.test', taxon_group_id=3))
        self.assertEqual(result, [])


# ===========================================================================
# TaxaList public access and field stripping
# ===========================================================================

_PUBLIC_STRIPPED_FIELDS = {
    'can_be_validated', 'taxon_group',
    'proposal_id', 'can_edit', 'children_count',
    'other_group_count', 'rejected', 'ready_for_validation',
    'validation_message', 'end_embargo_date', 'verified',
}


class TestTaxaListPublicAccess(FastTenantTestCase):

    def setUp(self):
        from bims.api_views.taxon import TaxaList
        self.view = TaxaList.as_view()
        self.factory = APIRequestFactory()
        self.taxon_group = TaxonGroupF.create()
        # One validated taxon in the group
        self.taxonomy = Taxonomy.objects.create(
            canonical_name='Abactochromis labrosus',
            scientific_name='Abactochromis labrosus (Trewavas, 1935)',
            rank='SPECIES',
            gbif_key=5961874,
        )
        self.taxon_group.taxonomies.add(
            self.taxonomy,
            through_defaults={'is_validated': True},
        )
        self.auth_user = UserF.create()

    def _get(self, user=None, params=None):
        from django.contrib.auth.models import AnonymousUser
        params = params or {'taxonGroup': self.taxon_group.id}
        request = self.factory.get('/api/taxa-list/', params)
        request.user = user or AnonymousUser()
        return self.view(request)

    # -- public access allowed without auth --------------------------------

    def test_unauthenticated_returns_200(self):
        response = self._get()
        self.assertEqual(response.status_code, 200)

    def test_unauthenticated_returns_results(self):
        response = self._get()
        self.assertGreater(response.data.get('count', 0), 0)

    # -- field stripping ---------------------------------------------------

    def test_public_response_strips_internal_fields(self):
        response = self._get()
        results = response.data.get('results', [])
        self.assertTrue(len(results) > 0)
        first = results[0]
        for field in _PUBLIC_STRIPPED_FIELDS:
            self.assertNotIn(field, first, msg=f'Field "{field}" should be stripped for public access')

    def test_authenticated_response_includes_internal_fields(self):
        response = self._get(user=self.auth_user)
        results = response.data.get('results', [])
        self.assertTrue(len(results) > 0)
        first = results[0]
        # Authenticated responses should include these fields
        for field in ('DT_RowId', 'can_edit', 'children_count', 'other_group_count'):
            self.assertIn(field, first, msg=f'Field "{field}" should be present for authenticated access')

    # -- public users always see validated taxa ----------------------------

    def test_public_validated_param_overridden_to_true(self):
        # Add an unvalidated taxon
        unvalidated = Taxonomy.objects.create(
            canonical_name='Unvalidated sp', scientific_name='Unvalidated sp', rank='SPECIES',
        )
        self.taxon_group.taxonomies.add(
            unvalidated, through_defaults={'is_validated': False}
        )
        # Even requesting validated=False, public users get only validated
        response = self._get(params={'taxonGroup': self.taxon_group.id, 'validated': 'False'})
        names = [r['canonical_name'] for r in response.data.get('results', [])]
        self.assertNotIn('Unvalidated sp', names)
        self.assertIn('Abactochromis labrosus', names)


# ===========================================================================
# TaxonDetail public access and field stripping
# ===========================================================================

@mock.patch(
    'bims.api_views.taxon.get_vernacular_names',
    return_value={'results': []},
)
class TestTaxonDetailPublicAccess(FastTenantTestCase):

    def setUp(self):
        from bims.api_views.taxon import TaxonDetail
        self.view = TaxonDetail.as_view()
        self.factory = APIRequestFactory()
        self.taxon_group = TaxonGroupF.create()
        self.taxonomy = Taxonomy.objects.create(
            canonical_name='Abactochromis labrosus',
            scientific_name='Abactochromis labrosus (Trewavas, 1935)',
            rank='SPECIES',
            gbif_key=5961874,
            additional_data={'Kingdom': 'Animalia'},
        )
        self.taxon_group.taxonomies.add(
            self.taxonomy, through_defaults={'is_validated': True}
        )
        self.auth_user = UserF.create()

    def _get(self, pk, user=None):
        from django.contrib.auth.models import AnonymousUser
        request = self.factory.get(f'/api/taxon/{pk}/')
        request.user = user or AnonymousUser()
        return self.view(request, pk=pk)

    def test_unauthenticated_returns_200(self, _mock_vernacular):
        response = self._get(self.taxonomy.pk)
        self.assertEqual(response.status_code, 200)

    def test_unknown_pk_returns_404(self, _mock_vernacular):
        response = self._get(99999)
        self.assertEqual(response.status_code, 404)

    def test_public_response_strips_internal_fields(self, _mock_vernacular):
        response = self._get(self.taxonomy.pk)
        data = response.data
        for field in _PUBLIC_STRIPPED_FIELDS:
            self.assertNotIn(field, data, msg=f'Field "{field}" should be stripped for public access')

    def test_public_response_includes_core_fields(self, _mock_vernacular):
        response = self._get(self.taxonomy.pk)
        data = response.data
        for field in ('id', 'canonical_name', 'scientific_name', 'rank',
                      'gbif_key', 'author', 'taxonomic_status'):
            self.assertIn(field, data, msg=f'Core field "{field}" should be present')

    def test_authenticated_response_includes_internal_fields(self, _mock_vernacular):
        response = self._get(self.taxonomy.pk, user=self.auth_user)
        data = response.data
        for field in ('DT_RowId', 'can_edit', 'children_count', 'other_group_count'):
            self.assertIn(field, data, msg=f'Field "{field}" should be present for authenticated access')

# ===========================================================================
# TaxonSerializer PUBLIC_EXCLUDED_FIELDS (unit-level)
# ===========================================================================

class TestTaxonSerializerPublicExclusion(FastTenantTestCase):

    def setUp(self):
        self.taxonomy = Taxonomy.objects.create(
            canonical_name='Test species',
            scientific_name='Test species Author',
            rank='SPECIES',
        )

    def _serialize(self, is_public):
        from bims.serializers.taxon_serializer import TaxonSerializer
        s = TaxonSerializer(self.taxonomy, context={'is_public': is_public})
        return s.data

    def test_all_public_excluded_fields_absent_when_public(self):
        data = self._serialize(is_public=True)
        from bims.serializers.taxon_serializer import TaxonSerializer
        for field in TaxonSerializer.PUBLIC_EXCLUDED_FIELDS:
            self.assertNotIn(field, data,
                             msg=f'"{field}" must be absent for public serialization')

    def test_public_excluded_fields_present_when_authenticated(self):
        data = self._serialize(is_public=False)
        # These are always computed and present for authenticated users
        for field in ('DT_RowId', 'can_edit', 'children_count', 'other_group_count'):
            self.assertIn(field, data,
                          msg=f'"{field}" must be present for authenticated serialization')

    def test_core_fields_always_present(self):
        for is_public in (True, False):
            data = self._serialize(is_public=is_public)
            for field in ('id', 'canonical_name', 'scientific_name', 'rank'):
                self.assertIn(field, data,
                              msg=f'Core field "{field}" absent when is_public={is_public}')


# ---------------------------------------------------------------------------
# _compute_taxon_checksum
# ---------------------------------------------------------------------------

class TestComputeTaxonChecksum(TestCase):
    """Unit tests for the checksum helper (no DB needed)."""

    def _td(self, **kwargs):
        base = {
            'id': 1,
            'canonical_name': 'Foo bar',
            'rank': 'SPECIES',
            'author': 'L.',
            'taxonomic_status': 'ACCEPTED',
            'gbif_key': 12345,
            'parent': 99,
            'additional_data': {'key': 'val'},
            'tag_list': 'freshwater (#51FF3E)',
        }
        base.update(kwargs)
        return base

    def test_returns_64_char_hex_string(self):
        cs = _compute_taxon_checksum(self._td())
        self.assertRegex(cs, r'^[0-9a-f]{64}$')

    def test_same_data_same_checksum(self):
        td = self._td()
        self.assertEqual(
            _compute_taxon_checksum(td),
            _compute_taxon_checksum(dict(td)),
        )

    def test_different_canonical_name_different_checksum(self):
        self.assertNotEqual(
            _compute_taxon_checksum(self._td(canonical_name='Foo bar')),
            _compute_taxon_checksum(self._td(canonical_name='Foo baz')),
        )

    def test_different_author_different_checksum(self):
        self.assertNotEqual(
            _compute_taxon_checksum(self._td(author='L.')),
            _compute_taxon_checksum(self._td(author='Smith')),
        )

    def test_different_additional_data_different_checksum(self):
        self.assertNotEqual(
            _compute_taxon_checksum(self._td(additional_data={'k': 'v1'})),
            _compute_taxon_checksum(self._td(additional_data={'k': 'v2'})),
        )

    def test_irrelevant_field_ignored(self):
        """The 'id' field is NOT part of the checksum — only content matters."""
        cs1 = _compute_taxon_checksum(self._td(id=1))
        cs2 = _compute_taxon_checksum(self._td(id=999))
        self.assertEqual(cs1, cs2)

    def test_rank_normalised_to_upper(self):
        """rank is normalised to uppercase before hashing."""
        self.assertEqual(
            _compute_taxon_checksum(self._td(rank='species')),
            _compute_taxon_checksum(self._td(rank='SPECIES')),
        )

    def test_missing_fields_dont_raise(self):
        """Completely sparse dict should not raise."""
        cs = _compute_taxon_checksum({'canonical_name': 'Min'})
        self.assertRegex(cs, r'^[0-9a-f]{64}$')


# ---------------------------------------------------------------------------
# Checksum-based skip and timestamp update in the harvest task loop
# ---------------------------------------------------------------------------

class TestChecksumSkipAndTimestamp(FastTenantTestCase):
    """
    Verify that:
    - A taxon whose upstream payload is unchanged (matching checksum) is
      skipped and NOT re-processed.
    - A taxon whose upstream payload has changed IS re-processed and its
      membership checksum / last_synced_at are updated.
    - A brand-new taxon gets its membership stamped with checksum and
      last_synced_at on first harvest.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.schema_name = connection.schema_name

    def _run_task(self, taxa_list, target_group, mark_readonly=False, base_url='http://remote.bims/'):
        session = HarvestSession.objects.create(
            module_group=target_group,
            additional_data={
                'base_url': base_url,
                'remote_group_id': 1,
                'import_mode': 'existing',
            },
        )
        prefs_mock = mock.MagicMock()
        prefs_mock.SiteSetting.auto_validate_taxa_on_upload = False

        with mock.patch(_PATCH_DISCONNECT), \
             mock.patch(_PATCH_CONNECT), \
             mock.patch('preferences.preferences', prefs_mock), \
             mock.patch(_PATCH_GET_ALL_TAXA, return_value=taxa_list), \
             mock.patch(_PATCH_GET_TAXON_BY_ID, return_value=None), \
             mock.patch(_PATCH_GET_GROUPS, return_value=[]):
            harvest_bims_species(session.id, self.schema_name)

        session.refresh_from_db()
        return session

    def test_new_taxon_gets_checksum_and_last_synced_at(self):
        group = TaxonGroupF()
        td = _taxon(1, 'Canis lupus', rank='SPECIES')
        self._run_task([td], group)

        membership = TaxonGroupTaxonomy.objects.get(
            taxongroup=group,
            upstream_taxon_id='1',
        )
        expected_cs = _compute_taxon_checksum(td)
        self.assertEqual(membership.upstream_checksum, expected_cs)
        self.assertIsNotNone(membership.last_synced_at)

    def test_unchanged_taxon_is_skipped_on_reharvest(self):
        group = TaxonGroupF()
        td = _taxon(1, 'Canis lupus', rank='SPECIES')

        # First harvest — creates the membership
        self._run_task([td], group)
        membership_before = TaxonGroupTaxonomy.objects.get(
            taxongroup=group, upstream_taxon_id='1',
        )
        ts_before = membership_before.last_synced_at
        cs_before = membership_before.upstream_checksum

        # Second harvest with identical payload — should be skipped
        session2 = self._run_task([td], group)

        membership_after = TaxonGroupTaxonomy.objects.get(
            taxongroup=group, upstream_taxon_id='1',
        )
        # Checksum unchanged after skip
        self.assertEqual(membership_after.upstream_checksum, cs_before)
        # last_synced_at is refreshed even on skip so stale-detection works
        self.assertGreaterEqual(membership_after.last_synced_at, ts_before)
        # Session status reflects 0 processed (only skipped)
        self.assertIn('0', session2.status)

    def test_changed_taxon_is_reprocessed(self):
        group = TaxonGroupF()
        td_v1 = _taxon(1, 'Canis lupus', rank='SPECIES', author='L.')
        td_v2 = _taxon(1, 'Canis lupus', rank='SPECIES', author='Linnaeus')  # author changed

        # First harvest
        self._run_task([td_v1], group)
        membership_v1 = TaxonGroupTaxonomy.objects.get(
            taxongroup=group, upstream_taxon_id='1',
        )
        ts_v1 = membership_v1.last_synced_at
        cs_v1 = membership_v1.upstream_checksum

        # Second harvest with changed author — must re-process
        self._run_task([td_v2], group)
        membership_v2 = TaxonGroupTaxonomy.objects.get(
            taxongroup=group, upstream_taxon_id='1',
        )
        expected_cs = _compute_taxon_checksum(td_v2)
        self.assertEqual(membership_v2.upstream_checksum, expected_cs)
        self.assertNotEqual(membership_v2.upstream_checksum, cs_v1)
        # last_synced_at should be updated (≥ v1 timestamp)
        self.assertGreaterEqual(membership_v2.last_synced_at, ts_v1)

    def test_taxon_without_remote_id_always_processed(self):
        """
        Taxa with no remote id cannot be keyed by upstream_taxon_id.
        The checksum-skip path is bypassed entirely for such taxa, so
        they are always processed.
        """
        group = TaxonGroupF()
        td = _taxon(None, 'Unknown taxon', rank='GENUS')
        td['id'] = None

        # Two harvests — both should show the taxon was processed (not skipped)
        self._run_task([td], group)
        s2 = self._run_task([td], group)
        # Without remote id there is no checksum-based skip; status shows ≥1 processed
        self.assertNotIn('0 taxa', s2.status)

    def test_membership_checksum_matches_helper(self):
        """Stored checksum must equal _compute_taxon_checksum output."""
        group = TaxonGroupF()
        td = _taxon(42, 'Panthera leo', rank='SPECIES',
                    additional_data={'notes': 'big cat'},
                    tag_list='african (#AA0000)')
        self._run_task([td], group)
        membership = TaxonGroupTaxonomy.objects.get(
            taxongroup=group, upstream_taxon_id='42',
        )
        self.assertEqual(membership.upstream_checksum, _compute_taxon_checksum(td))


class TestRemoveStaleTaxa(FastTenantTestCase):
    """
    Verify _remove_stale_taxa behaviour after a read-only group harvest.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.schema_name = connection.schema_name

    def _run_task(self, taxa_list, target_group, base_url='http://remote.bims/'):
        session = HarvestSession.objects.create(
            module_group=target_group,
            additional_data={
                'base_url': base_url,
                'remote_group_id': 1,
                'import_mode': 'existing',
            },
        )
        prefs_mock = mock.MagicMock()
        prefs_mock.SiteSetting.auto_validate_taxa_on_upload = False

        with mock.patch(_PATCH_DISCONNECT), \
             mock.patch(_PATCH_CONNECT), \
             mock.patch('preferences.preferences', prefs_mock), \
             mock.patch(_PATCH_GET_ALL_TAXA, return_value=taxa_list), \
             mock.patch(_PATCH_GET_TAXON_BY_ID, return_value=None), \
             mock.patch(_PATCH_GET_GROUPS, return_value=[]):
            harvest_bims_species(session.id, self.schema_name)

        session.refresh_from_db()
        return session

    def _make_stale_membership(self, group, taxonomy, days_ago=2):
        """Create a TaxonGroupTaxonomy with last_synced_at in the past."""
        from django.utils import timezone
        import datetime
        membership, _ = TaxonGroupTaxonomy.objects.get_or_create(
            taxongroup=group,
            taxonomy=taxonomy,
            defaults={'upstream_taxon_id': str(taxonomy.pk)},
        )
        stale_time = timezone.now() - datetime.timedelta(days=days_ago)
        TaxonGroupTaxonomy.objects.filter(pk=membership.pk).update(
            last_synced_at=stale_time
        )
        return membership

    def test_stale_taxon_removed_and_deleted_when_orphaned(self):
        """A stale taxon with no other groups and no occurrences is deleted."""
        from bims.tests.model_factories import TaxonGroupF
        group = TaxonGroupF(is_readonly=True)
        taxon = Taxonomy.objects.create(
            canonical_name='Stale species',
            scientific_name='Stale species',
            rank='SPECIES',
        )
        self._make_stale_membership(group, taxon)

        from django.utils import timezone
        _remove_stale_taxa(group, timezone.now())

        self.assertFalse(
            TaxonGroupTaxonomy.objects.filter(taxongroup=group, taxonomy=taxon).exists()
        )
        self.assertFalse(Taxonomy.objects.filter(pk=taxon.pk).exists())

    def test_stale_taxon_unlinked_but_kept_when_in_other_group(self):
        """A stale taxon that belongs to another group is unlinked but not deleted."""
        from bims.tests.model_factories import TaxonGroupF
        group = TaxonGroupF(is_readonly=True)
        other_group = TaxonGroupF()
        taxon = Taxonomy.objects.create(
            canonical_name='Shared species',
            scientific_name='Shared species',
            rank='SPECIES',
        )
        self._make_stale_membership(group, taxon)
        other_group.taxonomies.add(taxon)

        from django.utils import timezone
        _remove_stale_taxa(group, timezone.now())

        self.assertFalse(
            TaxonGroupTaxonomy.objects.filter(taxongroup=group, taxonomy=taxon).exists()
        )
        self.assertTrue(Taxonomy.objects.filter(pk=taxon.pk).exists())

    def test_stale_taxon_unlinked_but_kept_when_has_occurrences(self):
        """A stale taxon with occurrence records is unlinked but not deleted."""
        from bims.models import BiologicalCollectionRecord
        from bims.tests.model_factories import TaxonGroupF, BiologicalCollectionRecordF
        group = TaxonGroupF(is_readonly=True)
        taxon = Taxonomy.objects.create(
            canonical_name='Observed species',
            scientific_name='Observed species',
            rank='SPECIES',
        )
        self._make_stale_membership(group, taxon)
        BiologicalCollectionRecordF(taxonomy=taxon)

        from django.utils import timezone
        _remove_stale_taxa(group, timezone.now())

        self.assertFalse(
            TaxonGroupTaxonomy.objects.filter(taxongroup=group, taxonomy=taxon).exists()
        )
        self.assertTrue(Taxonomy.objects.filter(pk=taxon.pk).exists())

    def test_manually_added_taxon_not_removed(self):
        """A taxon with last_synced_at=None (manually added) is left alone."""
        from bims.tests.model_factories import TaxonGroupF
        group = TaxonGroupF(is_readonly=True)
        taxon = Taxonomy.objects.create(
            canonical_name='Manual species',
            scientific_name='Manual species',
            rank='SPECIES',
        )
        TaxonGroupTaxonomy.objects.create(
            taxongroup=group,
            taxonomy=taxon,
            last_synced_at=None,
        )

        from django.utils import timezone
        _remove_stale_taxa(group, timezone.now())

        self.assertTrue(
            TaxonGroupTaxonomy.objects.filter(taxongroup=group, taxonomy=taxon).exists()
        )
        self.assertTrue(Taxonomy.objects.filter(pk=taxon.pk).exists())

    def test_taxon_with_pending_proposal_skipped(self):
        """A stale taxon with a pending update proposal is not removed."""
        from bims.models import TaxonomyUpdateProposal
        from bims.tests.model_factories import TaxonGroupF
        group = TaxonGroupF(is_readonly=True)
        taxon = Taxonomy.objects.create(
            canonical_name='Proposed species',
            scientific_name='Proposed species',
            rank='SPECIES',
        )
        self._make_stale_membership(group, taxon)
        TaxonomyUpdateProposal.objects.create(
            original_taxonomy=taxon,
            taxon_group=group,
            status='pending',
        )

        from django.utils import timezone
        _remove_stale_taxa(group, timezone.now())

        self.assertTrue(
            TaxonGroupTaxonomy.objects.filter(taxongroup=group, taxonomy=taxon).exists()
        )

    def test_fresh_taxon_not_removed(self):
        """A taxon touched during the current harvest is not flagged as stale."""
        from django.utils import timezone
        from bims.tests.model_factories import TaxonGroupF
        group = TaxonGroupF(is_readonly=True)
        td = _taxon(99, 'Fresh species', rank='SPECIES')

        harvest_start = timezone.now()
        self._run_task([td], group)
        _remove_stale_taxa(group, harvest_start)

        self.assertTrue(
            TaxonGroupTaxonomy.objects.filter(
                taxongroup=group, upstream_taxon_id='99'
            ).exists()
        )
