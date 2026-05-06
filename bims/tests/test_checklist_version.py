# coding=utf-8
"""
Tests for ChecklistVersion and ChecklistSnapshot workflow.
"""
from django.test import TestCase

from bims.models.checklist_version import ChecklistVersion, ChecklistSnapshot
from bims.models.licence import Licence
from bims.models.taxon_group_taxonomy import TaxonGroupTaxonomy
from bims.tests.model_factories import TaxonomyF, TaxonGroupF, UserF


def _licence():
    licence, _ = Licence.objects.get_or_create(
        identifier='CC-BY-4.0',
        defaults={
            'name': 'Creative Commons Attribution 4.0',
            'url': 'https://creativecommons.org/licenses/by/4.0/',
        },
    )
    return licence


def _make_version(taxon_group, version='1.0', **kwargs):
    return ChecklistVersion.objects.create(
        taxon_group=taxon_group,
        version=version,
        license=_licence(),
        **kwargs,
    )


def _add_validated(group, *taxa):
    """Add taxa to a group with is_validated=True."""
    for taxon in taxa:
        group.taxonomies.add(taxon, through_defaults={'is_validated': True})


def _add_unvalidated(group, *taxa):
    """Add taxa to a group with is_validated=False (default)."""
    for taxon in taxa:
        group.taxonomies.add(taxon, through_defaults={'is_validated': False})


class TestChecklistVersionStatus(TestCase):

    def setUp(self):
        self.group = TaxonGroupF.create(name='Fish')
        self.user = UserF.create()

    def test_new_version_is_draft(self):
        cv = _make_version(self.group)
        self.assertEqual(cv.status, ChecklistVersion.STATUS_DRAFT)

    def test_publish_transitions_to_published(self):
        cv = _make_version(self.group)
        cv.publish(published_by=self.user)
        cv.refresh_from_db()
        self.assertEqual(cv.status, ChecklistVersion.STATUS_PUBLISHED)

    def test_publish_is_idempotent(self):
        cv = _make_version(self.group)
        cv.publish(published_by=self.user)
        cv.publish(published_by=self.user)  # second call should be a no-op
        cv.refresh_from_db()
        self.assertEqual(cv.status, ChecklistVersion.STATUS_PUBLISHED)

    def test_published_at_set_on_publish(self):
        cv = _make_version(self.group)
        self.assertIsNone(cv.published_at)
        cv.publish(published_by=self.user)
        cv.refresh_from_db()
        self.assertIsNotNone(cv.published_at)

    def test_published_by_recorded(self):
        cv = _make_version(self.group)
        cv.publish(published_by=self.user)
        cv.refresh_from_db()
        self.assertEqual(cv.published_by, self.user)

    def test_is_publishing_cleared_after_publish(self):
        cv = _make_version(self.group)
        cv.publish(published_by=self.user)
        cv.refresh_from_db()
        self.assertFalse(cv.is_publishing)

    def test_is_publishing_default_false(self):
        cv = _make_version(self.group)
        self.assertFalse(cv.is_publishing)


class TestChecklistVersionSnapshot(TestCase):

    def setUp(self):
        self.group = TaxonGroupF.create(name='Frogs')
        self.user = UserF.create()
        self.taxon1 = TaxonomyF.create(scientific_name='Rana temporaria', rank='SPECIES')
        self.taxon2 = TaxonomyF.create(scientific_name='Bufo bufo', rank='SPECIES')
        _add_validated(self.group, self.taxon1, self.taxon2)

    def test_publish_creates_snapshot_rows(self):
        cv = _make_version(self.group)
        cv.publish(published_by=self.user)
        self.assertEqual(cv.snapshot_rows.count(), 2)

    def test_taxa_count_matches_snapshot_rows(self):
        cv = _make_version(self.group)
        cv.publish(published_by=self.user)
        cv.refresh_from_db()
        # taxa_count only counts non-deleted rows
        self.assertEqual(cv.taxa_count, 2)

    def test_snapshot_row_fields(self):
        cv = _make_version(self.group)
        cv.publish(published_by=self.user)
        row = cv.snapshot_rows.get(checklist_id=str(self.taxon1.pk))
        self.assertEqual(row.scientific_name, 'Rana temporaria')
        self.assertEqual(row.rank, 'SPECIES')

    def test_snapshot_unique_per_version_taxon(self):
        """Duplicate publish should not create extra snapshot rows (ignore_conflicts)."""
        cv = _make_version(self.group)
        cv.publish(published_by=self.user)
        before = cv.snapshot_rows.count()
        rows = [cv.build_snapshot_row(self.taxon1, ChecklistSnapshot.CHANGE_UNCHANGED)]
        ChecklistSnapshot.objects.bulk_create(rows, ignore_conflicts=True)
        self.assertEqual(cv.snapshot_rows.count(), before)

    def test_version_chain(self):
        cv1 = _make_version(self.group, version='1.0')
        cv1.publish(published_by=self.user)
        cv2 = _make_version(self.group, version='2.0', previous_version=cv1)
        self.assertEqual(cv2.previous_version, cv1)

    def test_empty_group_publishes_zero_taxa(self):
        empty_group = TaxonGroupF.create(name='EmptyModule')
        cv = _make_version(empty_group)
        cv.publish(published_by=self.user)
        cv.refresh_from_db()
        self.assertEqual(cv.taxa_count, 0)
        self.assertEqual(cv.snapshot_rows.count(), 0)

    def test_child_group_taxa_included_in_snapshot(self):
        """Taxa belonging to child TaxonGroups are included in the parent snapshot."""
        child_group = TaxonGroupF.create(name='FrogSubgroup', parent=self.group)
        child_taxon = TaxonomyF.create(scientific_name='Xenopus laevis', rank='SPECIES')
        _add_validated(child_group, child_taxon)

        cv = _make_version(self.group)
        cv.publish(published_by=self.user)

        snapshot_ids = set(cv.snapshot_rows.values_list('checklist_id', flat=True))
        self.assertIn(str(child_taxon.pk), snapshot_ids)

    def test_grandchild_group_taxa_included_in_snapshot(self):
        """Taxa from grandchild TaxonGroups (2 levels deep) are also included."""
        child_group = TaxonGroupF.create(name='FrogFamily', parent=self.group)
        grandchild_group = TaxonGroupF.create(name='FrogGenus', parent=child_group)
        deep_taxon = TaxonomyF.create(
            scientific_name='Arthroleptis stenodactylus', rank='SPECIES'
        )
        _add_validated(grandchild_group, deep_taxon)

        cv = _make_version(self.group)
        cv.publish(published_by=self.user)

        snapshot_ids = set(cv.snapshot_rows.values_list('checklist_id', flat=True))
        self.assertIn(str(deep_taxon.pk), snapshot_ids)

    def test_taxa_count_includes_child_groups(self):
        """taxa_count reflects taxa from all descendant groups combined."""
        child_group = TaxonGroupF.create(name='FrogChild', parent=self.group)
        extra = TaxonomyF.create(scientific_name='Hyperolius marmoratus', rank='SPECIES')
        _add_validated(child_group, extra)

        cv = _make_version(self.group)
        cv.publish(published_by=self.user)
        cv.refresh_from_db()

        # parent group has 2 taxa, child adds 1 → total 3
        self.assertEqual(cv.taxa_count, 3)


class TestChecklistVersionValidation(TestCase):
    """Only validated taxa (is_validated=True on TaxonGroupTaxonomy) are snapshotted."""

    def setUp(self):
        self.group = TaxonGroupF.create(name='Mammals')
        self.user = UserF.create()

    def test_unvalidated_taxa_excluded_from_snapshot(self):
        validated = TaxonomyF.create(scientific_name='Leo leo', rank='SPECIES')
        unvalidated = TaxonomyF.create(scientific_name='Felis catus', rank='SPECIES')
        _add_validated(self.group, validated)
        _add_unvalidated(self.group, unvalidated)

        cv = _make_version(self.group)
        cv.publish(published_by=self.user)

        snapshot_ids = set(cv.snapshot_rows.values_list('checklist_id', flat=True))
        self.assertIn(str(validated.pk), snapshot_ids)
        self.assertNotIn(str(unvalidated.pk), snapshot_ids)

    def test_only_unvalidated_taxa_produces_empty_snapshot(self):
        taxon = TaxonomyF.create(scientific_name='Mus musculus', rank='SPECIES')
        _add_unvalidated(self.group, taxon)

        cv = _make_version(self.group)
        cv.publish(published_by=self.user)
        cv.refresh_from_db()

        self.assertEqual(cv.taxa_count, 0)
        self.assertEqual(cv.snapshot_rows.count(), 0)

    def test_validating_a_taxon_includes_it_in_next_version(self):
        taxon = TaxonomyF.create(scientific_name='Canis lupus', rank='SPECIES')
        _add_unvalidated(self.group, taxon)

        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)
        self.assertEqual(v1.taxa_count, 0)

        # Now validate the taxon
        TaxonGroupTaxonomy.objects.filter(
            taxongroup=self.group, taxonomy=taxon
        ).update(is_validated=True)

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)
        v2.refresh_from_db()

        self.assertEqual(v2.taxa_count, 1)
        row = v2.snapshot_rows.get(checklist_id=str(taxon.pk))
        self.assertEqual(row.change_type, ChecklistSnapshot.CHANGE_ADDED)


class TestChecklistVersionDeletions(TestCase):
    """Verify deletion detection when taxa are removed from a group between versions."""

    def setUp(self):
        self.group = TaxonGroupF.create(name='Insects')
        self.user = UserF.create()
        self.taxon_a = TaxonomyF.create(scientific_name='Apis mellifera', rank='SPECIES')
        self.taxon_b = TaxonomyF.create(scientific_name='Bombus terrestris', rank='SPECIES')
        _add_validated(self.group, self.taxon_a, self.taxon_b)

    def test_removed_taxon_is_marked_deleted(self):
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)

        # Remove taxon_b from the group before publishing v2
        self.group.taxonomies.remove(self.taxon_b)

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)

        deleted_row = v2.snapshot_rows.get(checklist_id=str(self.taxon_b.pk))
        self.assertEqual(deleted_row.change_type, ChecklistSnapshot.CHANGE_DELETED)

    def test_deletions_count_is_correct(self):
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)

        self.group.taxonomies.remove(self.taxon_b)

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)
        v2.refresh_from_db()

        self.assertEqual(v2.deletions_count, 1)

    def test_taxa_count_excludes_deleted(self):
        """taxa_count should only count active (non-deleted) taxa."""
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)

        self.group.taxonomies.remove(self.taxon_b)

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)
        v2.refresh_from_db()

        # v2 has 1 active taxon + 1 deleted row in snapshot, but taxa_count = 1
        self.assertEqual(v2.taxa_count, 1)

    def test_deleted_row_carries_previous_snapshot_data(self):
        """Deleted rows preserve the scientific_name from the previous version."""
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)

        self.group.taxonomies.remove(self.taxon_b)

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)

        deleted_row = v2.snapshot_rows.get(checklist_id=str(self.taxon_b.pk))
        self.assertEqual(deleted_row.scientific_name, 'Bombus terrestris')

    def test_no_deletions_without_previous_version(self):
        """First version has no previous snapshot so deletions_count must be 0."""
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)
        v1.refresh_from_db()

        self.assertEqual(v1.deletions_count, 0)

    def test_no_deletions_when_all_taxa_retained(self):
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)
        v2.refresh_from_db()

        self.assertEqual(v2.deletions_count, 0)

    def test_multiple_deletions(self):
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)

        self.group.taxonomies.remove(self.taxon_a, self.taxon_b)

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)
        v2.refresh_from_db()

        self.assertEqual(v2.deletions_count, 2)
        self.assertEqual(v2.taxa_count, 0)

    def test_changelog_summary_includes_deletions(self):
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)

        self.group.taxonomies.remove(self.taxon_b)
        new_taxon = TaxonomyF.create(scientific_name='Vespa crabro', rank='SPECIES')
        _add_validated(self.group, new_taxon)

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)
        v2.refresh_from_db()

        summary = v2.changelog_summary
        self.assertIn('deletions', summary)
        self.assertEqual(summary['additions'], 1)
        self.assertEqual(summary['deletions'], 1)
        self.assertEqual(summary['total'], 2)  # additions + updates + deletions


class TestChecklistVersionChangeType(TestCase):
    """Verify change_type is derived by diffing against the previous snapshot."""

    def setUp(self):
        self.group = TaxonGroupF.create(name='Reptiles')
        self.user = UserF.create()
        self.taxon = TaxonomyF.create(scientific_name='Agama agama', rank='SPECIES')
        _add_validated(self.group, self.taxon)

    def test_first_version_all_added(self):
        """No previous_version → every row is CHANGE_ADDED."""
        cv = _make_version(self.group, version='1.0')
        cv.publish(published_by=self.user)
        row = cv.snapshot_rows.get(checklist_id=str(self.taxon.pk))
        self.assertEqual(row.change_type, ChecklistSnapshot.CHANGE_ADDED)

    def test_unchanged_taxon_is_unchanged(self):
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)

        row = v2.snapshot_rows.get(checklist_id=str(self.taxon.pk))
        self.assertEqual(row.change_type, ChecklistSnapshot.CHANGE_UNCHANGED)

    def test_updated_taxon_is_updated(self):
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)

        self.taxon.scientific_name = 'Agama agama renamed'
        self.taxon.save(update_fields=['scientific_name'])

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)

        row = v2.snapshot_rows.get(checklist_id=str(self.taxon.pk))
        self.assertEqual(row.change_type, ChecklistSnapshot.CHANGE_UPDATED)

    def test_new_taxon_in_subsequent_version_is_added(self):
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)

        new_taxon = TaxonomyF.create(scientific_name='Gecko gecko', rank='SPECIES')
        _add_validated(self.group, new_taxon)

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)

        existing_row = v2.snapshot_rows.get(checklist_id=str(self.taxon.pk))
        new_row = v2.snapshot_rows.get(checklist_id=str(new_taxon.pk))
        self.assertEqual(existing_row.change_type, ChecklistSnapshot.CHANGE_UNCHANGED)
        self.assertEqual(new_row.change_type, ChecklistSnapshot.CHANGE_ADDED)

    def test_additions_updates_deletions_counters(self):
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)

        # Add new, rename existing, remove existing
        new_taxon = TaxonomyF.create(scientific_name='Chameleon chameleon', rank='SPECIES')
        _add_validated(self.group, new_taxon)
        self.taxon.scientific_name = 'Agama agama updated'
        self.taxon.save(update_fields=['scientific_name'])

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)
        v2.refresh_from_db()

        self.assertEqual(v2.additions_count, 1)
        self.assertEqual(v2.updates_count, 1)
        self.assertEqual(v2.deletions_count, 0)

    def test_deleted_change_type_constant(self):
        self.assertEqual(ChecklistSnapshot.CHANGE_DELETED, 'deleted')


class TestChecklistVersionQueryPatterns(TestCase):

    def setUp(self):
        self.group = TaxonGroupF.create(name='Birds')
        self.user = UserF.create()
        self.taxon = TaxonomyF.create(scientific_name='Passer domesticus', rank='SPECIES')
        _add_validated(self.group, self.taxon)

        self.v1 = _make_version(self.group, version='1.0')
        self.v1.publish(published_by=self.user)

        self.v2 = _make_version(self.group, version='2.0', previous_version=self.v1)
        self.v2.publish(published_by=self.user)

    def test_taxon_appears_in_both_versions(self):
        snapshots = ChecklistSnapshot.objects.filter(
            checklist_id=str(self.taxon.pk),
            checklist_version__status=ChecklistVersion.STATUS_PUBLISHED,
        ).order_by('-checklist_version__published_at')
        self.assertEqual(snapshots.count(), 2)

    def test_latest_version_is_first(self):
        snapshots = ChecklistSnapshot.objects.filter(
            checklist_id=str(self.taxon.pk),
            checklist_version__status=ChecklistVersion.STATUS_PUBLISHED,
        ).order_by('-checklist_version__published_at')
        self.assertEqual(snapshots.first().checklist_version, self.v2)

    def test_reverse_lookup_from_version(self):
        versions = ChecklistVersion.objects.filter(
            snapshot_rows__checklist_id=str(self.taxon.pk),
            status=ChecklistVersion.STATUS_PUBLISHED,
        ).order_by('-published_at')
        self.assertEqual(list(versions), [self.v2, self.v1])

    def test_diff_between_versions(self):
        new_taxon = TaxonomyF.create(scientific_name='Corvus corax', rank='SPECIES')
        _add_validated(self.group, new_taxon)
        v3 = _make_version(self.group, version='3.0', previous_version=self.v2)
        v3.publish(published_by=self.user)

        v2_active_ids = set(
            self.v2.snapshot_rows
            .exclude(change_type=ChecklistSnapshot.CHANGE_DELETED)
            .values_list('checklist_id', flat=True)
        )
        v3_active_ids = set(
            v3.snapshot_rows
            .exclude(change_type=ChecklistSnapshot.CHANGE_DELETED)
            .values_list('checklist_id', flat=True)
        )
        added = v3_active_ids - v2_active_ids
        self.assertIn(str(new_taxon.pk), added)

    def test_unique_together_constraint(self):
        from django.db import IntegrityError
        with self.assertRaises(IntegrityError):
            ChecklistSnapshot.objects.create(
                checklist_version=self.v1,
                checklist_id=str(self.taxon.pk),
                scientific_name='duplicate',
            )


class TestChecklistVersionChangelogSummary(TestCase):

    def setUp(self):
        self.group = TaxonGroupF.create(name='Mammals')
        self.user = UserF.create()

    def test_changelog_summary_keys(self):
        cv = _make_version(self.group)
        summary = cv.changelog_summary
        self.assertIn('additions', summary)
        self.assertIn('updates', summary)
        self.assertIn('deletions', summary)
        self.assertIn('total', summary)

    def test_changelog_summary_total_includes_deletions(self):
        cv = _make_version(self.group)
        cv.additions_count = 3
        cv.updates_count = 2
        cv.deletions_count = 1
        cv.save(update_fields=['additions_count', 'updates_count', 'deletions_count'])
        self.assertEqual(cv.changelog_summary['total'], 6)

    def test_changelog_summary_zero_deletions_by_default(self):
        cv = _make_version(self.group)
        self.assertEqual(cv.changelog_summary['deletions'], 0)
