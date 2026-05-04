# coding=utf-8
"""
Tests for ChecklistVersion and ChecklistSnapshot workflow.
"""
from django.test import TestCase

from bims.models.checklist_version import ChecklistVersion, ChecklistSnapshot
from bims.models.licence import Licence
from bims.tests.model_factories import TaxonomyF, TaxonGroupF, UserF


def _licence():
    licence, _ = Licence.objects.get_or_create(
        identifier='CC-BY-4.0',
        defaults={'name': 'Creative Commons Attribution 4.0', 'url': 'https://creativecommons.org/licenses/by/4.0/'},
    )
    return licence


def _make_version(taxon_group, version='1.0', **kwargs):
    return ChecklistVersion.objects.create(
        taxon_group=taxon_group,
        version=version,
        license=_licence(),
        **kwargs,
    )


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
        cv.publish(published_by=self.user)   # second call should be a no-op
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


class TestChecklistVersionSnapshot(TestCase):

    def setUp(self):
        self.group = TaxonGroupF.create(name='Frogs')
        self.user = UserF.create()
        self.taxon1 = TaxonomyF.create(scientific_name='Rana temporaria', rank='SPECIES')
        self.taxon2 = TaxonomyF.create(scientific_name='Bufo bufo', rank='SPECIES')
        self.group.taxonomies.add(self.taxon1, self.taxon2)

    def test_publish_creates_snapshot_rows(self):
        cv = _make_version(self.group)
        cv.publish(published_by=self.user)
        self.assertEqual(cv.snapshot_rows.count(), 2)

    def test_taxa_count_matches_snapshot_rows(self):
        cv = _make_version(self.group)
        cv.publish(published_by=self.user)
        cv.refresh_from_db()
        self.assertEqual(cv.taxa_count, cv.snapshot_rows.count())

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
        # Manually call bulk_create again — should not raise, no duplicates added
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
        child_group.taxonomies.add(child_taxon)

        cv = _make_version(self.group)
        cv.publish(published_by=self.user)

        snapshot_ids = set(cv.snapshot_rows.values_list('checklist_id', flat=True))
        self.assertIn(str(child_taxon.pk), snapshot_ids)

    def test_grandchild_group_taxa_included_in_snapshot(self):
        """Taxa from grandchild TaxonGroups (2 levels deep) are also included."""
        child_group = TaxonGroupF.create(name='FrogFamily', parent=self.group)
        grandchild_group = TaxonGroupF.create(name='FrogGenus', parent=child_group)
        deep_taxon = TaxonomyF.create(scientific_name='Arthroleptis stenodactylus', rank='SPECIES')
        grandchild_group.taxonomies.add(deep_taxon)

        cv = _make_version(self.group)
        cv.publish(published_by=self.user)

        snapshot_ids = set(cv.snapshot_rows.values_list('checklist_id', flat=True))
        self.assertIn(str(deep_taxon.pk), snapshot_ids)

    def test_taxa_count_includes_child_groups(self):
        """taxa_count reflects taxa from all descendant groups combined."""
        child_group = TaxonGroupF.create(name='FrogChild', parent=self.group)
        extra = TaxonomyF.create(scientific_name='Hyperolius marmoratus', rank='SPECIES')
        child_group.taxonomies.add(extra)

        cv = _make_version(self.group)
        cv.publish(published_by=self.user)
        cv.refresh_from_db()

        # parent group has 2 taxa, child adds 1 → total 3
        self.assertEqual(cv.taxa_count, 3)
        self.assertEqual(cv.snapshot_rows.count(), 3)


class TestChecklistVersionChangeType(TestCase):
    """Verify change_type is derived by diffing against the previous snapshot."""

    def setUp(self):
        self.group = TaxonGroupF.create(name='Reptiles')
        self.user = UserF.create()
        self.taxon = TaxonomyF.create(scientific_name='Agama agama', rank='SPECIES')
        self.group.taxonomies.add(self.taxon)

    def test_first_version_all_added(self):
        """No previous_version → every row is CHANGE_ADDED."""
        cv = _make_version(self.group, version='1.0')
        cv.publish(published_by=self.user)
        row = cv.snapshot_rows.get(checklist_id=str(self.taxon.pk))
        self.assertEqual(row.change_type, 'added')

    def test_unchanged_taxon_is_unchanged(self):
        """Taxon with no field changes between versions → CHANGE_UNCHANGED."""
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)

        row = v2.snapshot_rows.get(checklist_id=str(self.taxon.pk))
        self.assertEqual(row.change_type, 'unchanged')

    def test_updated_taxon_is_updated(self):
        """Taxon whose scientific_name changed between versions → CHANGE_UPDATED."""
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)

        self.taxon.scientific_name = 'Agama agama renamed'
        self.taxon.save(update_fields=['scientific_name'])

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)

        row = v2.snapshot_rows.get(checklist_id=str(self.taxon.pk))
        self.assertEqual(row.change_type, 'updated')

    def test_new_taxon_in_subsequent_version_is_added(self):
        """Taxon absent from previous version → CHANGE_ADDED in next version."""
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)

        new_taxon = TaxonomyF.create(scientific_name='Gecko gecko', rank='SPECIES')
        self.group.taxonomies.add(new_taxon)

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)

        existing_row = v2.snapshot_rows.get(checklist_id=str(self.taxon.pk))
        new_row = v2.snapshot_rows.get(checklist_id=str(new_taxon.pk))
        self.assertEqual(existing_row.change_type, 'unchanged')
        self.assertEqual(new_row.change_type, 'added')

    def test_additions_and_updates_counters(self):
        """additions_count and updates_count reflect actual diffs."""
        v1 = _make_version(self.group, version='1.0')
        v1.publish(published_by=self.user)

        # Add a new taxon and rename the existing one
        new_taxon = TaxonomyF.create(scientific_name='Chameleon chameleon', rank='SPECIES')
        self.group.taxonomies.add(new_taxon)
        self.taxon.scientific_name = 'Agama agama updated'
        self.taxon.save(update_fields=['scientific_name'])

        v2 = _make_version(self.group, version='2.0', previous_version=v1)
        v2.publish(published_by=self.user)
        v2.refresh_from_db()

        self.assertEqual(v2.additions_count, 1)
        self.assertEqual(v2.updates_count, 1)


class TestChecklistVersionQueryPatterns(TestCase):
    """Verify the documented snapshot query patterns work correctly."""

    def setUp(self):
        self.group = TaxonGroupF.create(name='Birds')
        self.user = UserF.create()
        self.taxon = TaxonomyF.create(scientific_name='Passer domesticus', rank='SPECIES')
        self.group.taxonomies.add(self.taxon)

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
        """Documented diff pattern: added/removed taxa between two versions."""
        new_taxon = TaxonomyF.create(scientific_name='Corvus corax', rank='SPECIES')
        self.group.taxonomies.add(new_taxon)
        v3 = _make_version(self.group, version='3.0', previous_version=self.v2)
        v3.publish(published_by=self.user)

        v2_ids = set(self.v2.snapshot_rows.values_list('checklist_id', flat=True))
        v3_ids = set(v3.snapshot_rows.values_list('checklist_id', flat=True))
        added = v3_ids - v2_ids
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
        self.assertIn('total', summary)

    def test_changelog_summary_total(self):
        cv = _make_version(self.group)
        cv.additions_count = 3
        cv.updates_count = 2
        cv.save(update_fields=['additions_count', 'updates_count'])
        self.assertEqual(cv.changelog_summary['total'], 5)
