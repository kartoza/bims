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
from bims.tasks.harvest_bims_species import (
    _apply_tags,
    _find_or_create_taxonomy,
    _parse_tag_list,
    harvest_bims_species,
)
from bims.tests.model_factories import TaxonGroupF, UserF

User = get_user_model()

_PATCH_DISCONNECT = 'bims.signals.utils.disconnect_bims_signals'
_PATCH_CONNECT = 'bims.signals.utils.connect_bims_signals'
_PATCH_PREFS = 'bims.tasks.harvest_bims_species.preferences'
_PATCH_GET_ALL_TAXA = 'bims.tasks.harvest_bims_species.get_all_taxa'
_PATCH_GET_TAXON_BY_ID = 'bims.tasks.harvest_bims_species.get_taxon_by_id'
_PATCH_GET_TAXON_BY_ID_UTIL = 'bims.utils.bims_instance.get_taxon_by_id'
_PATCH_GET_GROUPS = 'bims.tasks.harvest_bims_species.get_taxon_groups'


# ---------------------------------------------------------------------------
# Helper: build a minimal remote-BIMS taxon dict (mirrors the sample data)
# ---------------------------------------------------------------------------

def _taxon(
    taxon_id, canonical_name, rank='SPECIES',
    gbif_key=None, parent=None, scientific_name=None,
    author='', taxonomic_status='ACCEPTED',
    tag_list='', additional_data=None,
):
    return {
        'id': taxon_id,
        'canonical_name': canonical_name,
        'scientific_name': scientific_name or canonical_name,
        'rank': rank,
        'gbif_key': gbif_key,
        'parent': parent,
        'author': author,
        'taxonomic_status': taxonomic_status,
        'tag_list': tag_list,
        'additional_data': additional_data or {},
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
             mock.patch(_PATCH_GET_TAXON_BY_ID, side_effect=_side_get_taxon):
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

    # -- import modes -------------------------------------------------------

    def test_import_mode_new_creates_taxon_group(self):
        session = self._make_session(import_mode='new')
        taxa = [_taxon(1, 'Fish species', rank='SPECIES')]
        with mock.patch(_PATCH_DISCONNECT), \
             mock.patch(_PATCH_CONNECT), \
             mock.patch(_PATCH_PREFS) as mock_prefs, \
             mock.patch(_PATCH_GET_ALL_TAXA, return_value=iter(taxa)), \
             mock.patch(_PATCH_GET_TAXON_BY_ID, return_value=None), \
             mock.patch('django.contrib.sites.models.Site.objects.get_current') as mock_get_current_site:
            mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
            mock_get_current_site.return_value = mock.MagicMock()
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
            mock_prefs.SiteSetting.auto_validate_taxa_on_upload = True
            mock_get_current_site.return_value = mock.MagicMock()
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
             mock.patch(_PATCH_GET_TAXON_BY_ID, return_value=None):
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
            'http://bims.test/api/module-list/', timeout=mock.ANY
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
            'http://bims.test/api/taxon/441/', timeout=mock.ANY
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
    'can_be_validated', 'taxon_group', 'additional_data',
    'DT_RowId', 'proposal_id', 'can_edit', 'children_count',
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

    def test_unauthenticated_returns_200(self):
        response = self._get(self.taxonomy.pk)
        self.assertEqual(response.status_code, 200)

    def test_unknown_pk_returns_404(self):
        response = self._get(99999)
        self.assertEqual(response.status_code, 404)

    def test_public_response_strips_internal_fields(self):
        response = self._get(self.taxonomy.pk)
        data = response.data
        for field in _PUBLIC_STRIPPED_FIELDS:
            self.assertNotIn(field, data, msg=f'Field "{field}" should be stripped for public access')

    def test_public_response_includes_core_fields(self):
        response = self._get(self.taxonomy.pk)
        data = response.data
        for field in ('id', 'canonical_name', 'scientific_name', 'rank',
                      'gbif_key', 'author', 'taxonomic_status'):
            self.assertIn(field, data, msg=f'Core field "{field}" should be present')

    def test_authenticated_response_includes_internal_fields(self):
        response = self._get(self.taxonomy.pk, user=self.auth_user)
        data = response.data
        for field in ('DT_RowId', 'can_edit', 'children_count', 'other_group_count'):
            self.assertIn(field, data, msg=f'Field "{field}" should be present for authenticated access')

    def test_public_additional_data_stripped(self):
        response = self._get(self.taxonomy.pk)
        self.assertNotIn('additional_data', response.data)

    def test_authenticated_additional_data_present(self):
        response = self._get(self.taxonomy.pk, user=self.auth_user)
        self.assertIn('additional_data', response.data)


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
