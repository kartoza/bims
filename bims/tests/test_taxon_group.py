# coding=utf-8
"""Test Taxon Group."""
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission, Group
from django.contrib.sites.models import Site
from django.test import TestCase
from rest_framework.test import force_authenticate, APIRequestFactory

from bims.api_views.taxon_group import (
    update_taxon_group_orders,
    remove_taxa_from_taxon_group,
    add_taxa_to_taxon_group
)
from bims.tests.model_factories import (
    TaxonGroupF,
    TaxonomyF,
    BiologicalCollectionRecordF
)
from bims.models.taxon_group import (
    TaxonGroup, TAXON_GROUP_LEVEL_1,
    TAXON_GROUP_LEVEL_3, TAXON_GROUP_LEVEL_2
)
from bims.models.biological_collection_record import BiologicalCollectionRecord
from bims.models import Taxonomy, TaxonomicGroupCategory
from mobile.api_views.taxon_group import TaxonGroupTotalValidated


class TestTaxonGroup(TestCase):
    """Test Taxon Groups."""

    def setUp(self):
        pass

    def test_update_taxon_group_orders(self):
        taxon_group_1 = TaxonGroupF.create(
            id=1,
            display_order=0
        )
        taxon_group_2 = TaxonGroupF.create(
            id=2,
            display_order=1
        )
        self.assertEqual(taxon_group_1.display_order, 0)
        self.assertEqual(taxon_group_2.display_order, 1)
        update_taxon_group_orders([2, 1])
        self.assertEqual(TaxonGroup.objects.get(id=1).display_order, 1)
        self.assertEqual(TaxonGroup.objects.get(id=2).display_order, 0)

    def test_remove_taxa_from_taxon_group(self):
        taxonomy_1 = TaxonomyF.create()
        taxonomy_2 = TaxonomyF.create()
        taxonomy_3 = TaxonomyF.create()
        taxon_group_1 = TaxonGroupF.create(
            taxonomies=(taxonomy_1, taxonomy_2)
        )
        biological_1 = BiologicalCollectionRecordF.create(
            taxonomy=taxonomy_1,
            module_group=taxon_group_1
        )
        BiologicalCollectionRecordF.create(
            taxonomy=taxonomy_3,
            module_group=taxon_group_1
        )
        self.assertEqual(taxon_group_1.taxonomies.all().count(), 2)
        self.assertEqual(biological_1.module_group, taxon_group_1)
        remove_taxa_from_taxon_group(
            [taxonomy_1.id],
            taxon_group_1.id
        )
        # Removing taxon that doesn't exist in the group
        remove_taxa_from_taxon_group(
            [taxonomy_3.id],
            taxon_group_1.id
        )
        self.assertEqual(
            TaxonGroup.objects.get(
                id=taxon_group_1.id).taxonomies.all().count(),
            1
        )
        self.assertEqual(
            TaxonGroup.objects.get(
                id=taxon_group_1.id).taxonomies.all().count(),
            1
        )
        self.assertFalse(
            BiologicalCollectionRecord.objects.filter(
                module_group=taxon_group_1
            ).exists()
        )

    def test_remove_taxa_deletes_when_no_other_group(self):
        """A taxon with no other group membership is permanently deleted after removal."""
        taxonomy = TaxonomyF.create()
        taxon_group = TaxonGroupF.create(taxonomies=(taxonomy,))
        taxonomy_id = taxonomy.id

        remove_taxa_from_taxon_group([taxonomy_id], taxon_group.id)

        self.assertFalse(
            Taxonomy.objects.filter(id=taxonomy_id).exists(),
            "Taxonomy should be permanently deleted when it belongs to no other group"
        )

    def test_remove_taxa_keeps_record_when_in_other_group(self):
        """A taxon still belonging to another group is only unlinked, not deleted."""
        taxonomy = TaxonomyF.create()
        taxon_group_1 = TaxonGroupF.create(taxonomies=(taxonomy,))
        taxon_group_2 = TaxonGroupF.create(taxonomies=(taxonomy,))
        taxonomy_id = taxonomy.id

        remove_taxa_from_taxon_group([taxonomy_id], taxon_group_1.id)

        self.assertTrue(
            Taxonomy.objects.filter(id=taxonomy_id).exists(),
            "Taxonomy should remain in the database if it still belongs to another group"
        )
        self.assertTrue(
            taxon_group_2.taxonomies.filter(id=taxonomy_id).exists(),
            "Taxonomy should still be linked to the other group"
        )

    def test_remove_taxa_keeps_module_of_other_groups(self):
        """Removing a taxon from a division group keeps the module of its
        occurrences in the species module."""
        taxonomy = TaxonomyF.create()
        module = TaxonGroupF.create(
            category=TaxonomicGroupCategory.SPECIES_MODULE.name,
            taxonomies=(taxonomy,))
        division = TaxonGroupF.create(
            category=TaxonomicGroupCategory.DIVISION_GROUP.name,
            taxonomies=(taxonomy,))
        record = BiologicalCollectionRecordF.create(
            taxonomy=taxonomy, module_group=module)

        remove_taxa_from_taxon_group([taxonomy.id], division.id)

        record.refresh_from_db()
        self.assertEqual(record.module_group, module)

    def test_remove_taxa_moves_records_to_the_other_module(self):
        """Occurrences of the module the taxon is removed from move to the
        other species module of the taxon."""
        taxonomy = TaxonomyF.create()
        module_1 = TaxonGroupF.create(
            category=TaxonomicGroupCategory.SPECIES_MODULE.name,
            taxonomies=(taxonomy,))
        module_2 = TaxonGroupF.create(
            category=TaxonomicGroupCategory.SPECIES_MODULE.name,
            taxonomies=(taxonomy,))
        record = BiologicalCollectionRecordF.create(
            taxonomy=taxonomy, module_group=module_1)

        remove_taxa_from_taxon_group([taxonomy.id], module_1.id)

        record.refresh_from_db()
        self.assertEqual(record.module_group, module_2)

    def test_remove_taxa_with_include_children_deletes_orphaned_parent_and_children(self):
        """include_children=True permanently deletes orphaned parent and child taxa."""
        parent = TaxonomyF.create()
        child = TaxonomyF.create(parent=parent)
        taxon_group = TaxonGroupF.create(taxonomies=(parent, child))

        remove_taxa_from_taxon_group([parent.id], taxon_group.id, include_children=True)

        self.assertFalse(
            Taxonomy.objects.filter(id=parent.id).exists(),
            "Orphaned parent should be permanently deleted when include_children=True"
        )
        self.assertFalse(
            Taxonomy.objects.filter(id=child.id).exists(),
            "Orphaned child should be permanently deleted when include_children=True"
        )

    def test_remove_taxa_with_include_children_keeps_child_in_other_group(self):
        """A child linked to another group is only unlinked from the current group, not deleted."""
        parent = TaxonomyF.create()
        child = TaxonomyF.create(parent=parent)
        taxon_group_1 = TaxonGroupF.create(taxonomies=(parent, child))
        taxon_group_2 = TaxonGroupF.create(taxonomies=(child,))
        child_id = child.id

        remove_taxa_from_taxon_group([parent.id], taxon_group_1.id, include_children=True)

        self.assertTrue(
            Taxonomy.objects.filter(id=child_id).exists(),
            "Child in another group should not be deleted"
        )
        self.assertTrue(
            taxon_group_2.taxonomies.filter(id=child_id).exists(),
            "Child should remain linked to the other group"
        )

    def test_remove_taxa_with_include_children_does_not_delete_parent_with_child_in_other_group(self):
        """Parent should not be permanently deleted if it has children in other groups, even with include_children=True."""
        parent = TaxonomyF.create()
        child_in_current = TaxonomyF.create(parent=parent)
        child_in_other = TaxonomyF.create(parent=parent)
        taxon_group_1 = TaxonGroupF.create(taxonomies=(parent, child_in_current))
        taxon_group_2 = TaxonGroupF.create(taxonomies=(child_in_other,))
        parent_id = parent.id

        remove_taxa_from_taxon_group([parent_id], taxon_group_1.id, include_children=True)

        self.assertTrue(
            Taxonomy.objects.filter(id=parent_id).exists(),
            "Parent should NOT be permanently deleted because it has a child in another group"
        )
        self.assertFalse(
            taxon_group_1.taxonomies.filter(id=parent_id).exists(),
            "Parent should be unlinked from the current group"
        )
        self.assertTrue(
            taxon_group_2.taxonomies.filter(id=child_in_other.id).exists(),
            "Child in other group should remain untouched"
        )

    def test_remove_taxa_without_include_children_does_not_permanently_delete_parent_with_children(self):
        """When include_children=False and the parent has children in the group, the parent is only unlinked."""
        parent = TaxonomyF.create()
        child = TaxonomyF.create(parent=parent)
        taxon_group = TaxonGroupF.create(taxonomies=(parent, child))
        parent_id = parent.id

        remove_taxa_from_taxon_group([parent_id], taxon_group.id, include_children=False)

        self.assertTrue(
            Taxonomy.objects.filter(id=parent_id).exists(),
            "Parent should NOT be permanently deleted when it still has children in the group"
        )
        self.assertFalse(
            taxon_group.taxonomies.filter(id=parent_id).exists(),
            "Parent should be unlinked from the group"
        )
        self.assertTrue(
            taxon_group.taxonomies.filter(id=child.id).exists(),
            "Child should remain in the group"
        )

    def test_remove_taxa_without_children_deletes_if_orphaned(self):
        """A taxon with no children in the group and no other group is permanently deleted."""
        taxonomy = TaxonomyF.create()
        taxon_group = TaxonGroupF.create(taxonomies=(taxonomy,))
        taxonomy_id = taxonomy.id

        remove_taxa_from_taxon_group([taxonomy_id], taxon_group.id, include_children=False)

        self.assertFalse(
            Taxonomy.objects.filter(id=taxonomy_id).exists(),
            "Orphaned taxon with no children in the group should be permanently deleted"
        )

    def test_remove_taxa_deletes_unused_ancestor_chain(self):
        """Deleting an orphaned leaf also deletes ancestors that become unused as a result."""
        grandparent = TaxonomyF.create()
        parent = TaxonomyF.create(parent=grandparent)
        leaf = TaxonomyF.create(parent=parent)
        taxon_group = TaxonGroupF.create(taxonomies=(leaf,))

        remove_taxa_from_taxon_group([leaf.id], taxon_group.id)

        self.assertFalse(
            Taxonomy.objects.filter(id=leaf.id).exists(),
            "Orphaned leaf should be permanently deleted"
        )
        self.assertFalse(
            Taxonomy.objects.filter(id=parent.id).exists(),
            "Parent should be deleted once it has no remaining children and is unused"
        )
        self.assertFalse(
            Taxonomy.objects.filter(id=grandparent.id).exists(),
            "Grandparent should be deleted once the whole chain below it is unused"
        )

    def test_remove_taxa_keeps_ancestor_still_in_use(self):
        """Ancestor cleanup stops at an ancestor that's still linked to a group, referenced by an
        occurrence, or has another child."""
        grandparent = TaxonomyF.create()
        parent = TaxonomyF.create(parent=grandparent)
        leaf = TaxonomyF.create(parent=parent)
        other_group = TaxonGroupF.create(taxonomies=(grandparent,))
        taxon_group = TaxonGroupF.create(taxonomies=(leaf,))

        remove_taxa_from_taxon_group([leaf.id], taxon_group.id)

        self.assertFalse(
            Taxonomy.objects.filter(id=leaf.id).exists(),
            "Orphaned leaf should be permanently deleted"
        )
        self.assertFalse(
            Taxonomy.objects.filter(id=parent.id).exists(),
            "Parent should still be deleted since it is unused"
        )
        self.assertTrue(
            Taxonomy.objects.filter(id=grandparent.id).exists(),
            "Grandparent should be kept because it still belongs to another taxon group"
        )
        self.assertTrue(
            other_group.taxonomies.filter(id=grandparent.id).exists()
        )

    def _patch_restrict(self, enabled):
        return patch(
            'bims.api_views.taxon_group.preferences',
            new=SimpleNamespace(
                SiteSetting=SimpleNamespace(restrict_taxon_to_single_group=enabled)
            )
        )

    def test_add_taxa_to_taxon_group(self):
        taxonomy_1 = TaxonomyF.create()
        taxonomy_2 = TaxonomyF.create()
        BiologicalCollectionRecordF.create(
            taxonomy=taxonomy_1
        )
        taxon_group_1 = TaxonGroupF.create()
        with self._patch_restrict(False):
            add_taxa_to_taxon_group(
                [taxonomy_1.id, taxonomy_2.id],
                taxon_group_1.id
            )
        self.assertEqual(
            TaxonGroup.objects.get(
                id=taxon_group_1.id).taxonomies.all().count(),
            2
        )

    def test_add_taxa_restricted_when_already_in_another_group(self):
        """When restriction is on, a taxon already in another group is rejected."""
        taxonomy = TaxonomyF.create()
        existing_group = TaxonGroupF.create(taxonomies=(taxonomy,))
        target_group = TaxonGroupF.create()

        with self._patch_restrict(True):
            rejected = add_taxa_to_taxon_group([taxonomy.id], target_group.id)

        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0]['id'], taxonomy.id)
        self.assertFalse(
            target_group.taxonomies.filter(id=taxonomy.id).exists(),
            'Taxon should not be added to the target group when restriction is on'
        )
        self.assertTrue(
            existing_group.taxonomies.filter(id=taxonomy.id).exists(),
            'Taxon should remain in its original group'
        )

    def test_add_taxa_allowed_when_restriction_disabled(self):
        """When restriction is off, a taxon already in another group can still be added."""
        taxonomy = TaxonomyF.create()
        existing_group = TaxonGroupF.create(taxonomies=(taxonomy,))
        target_group = TaxonGroupF.create()

        with self._patch_restrict(False):
            rejected = add_taxa_to_taxon_group([taxonomy.id], target_group.id)

        self.assertEqual(rejected, [])
        self.assertTrue(
            target_group.taxonomies.filter(id=taxonomy.id).exists(),
            'Taxon should be added to the target group when restriction is off'
        )

    def test_add_taxa_to_same_group_not_rejected(self):
        """Re-adding a taxon to the group it already belongs to is not rejected."""
        taxonomy = TaxonomyF.create()
        taxon_group = TaxonGroupF.create(taxonomies=(taxonomy,))

        with self._patch_restrict(True):
            rejected = add_taxa_to_taxon_group([taxonomy.id], taxon_group.id)

        self.assertEqual(rejected, [])
        self.assertTrue(taxon_group.taxonomies.filter(id=taxonomy.id).exists())

    def test_add_taxa_restriction_returns_empty_for_new_taxon(self):
        """A taxon with no group membership is never rejected even when restriction is on."""
        taxonomy = TaxonomyF.create()
        taxon_group = TaxonGroupF.create()

        with self._patch_restrict(True):
            rejected = add_taxa_to_taxon_group([taxonomy.id], taxon_group.id)

        self.assertEqual(rejected, [])
        self.assertTrue(taxon_group.taxonomies.filter(id=taxonomy.id).exists())

    def test_add_taxon_group_level_2(self):
        taxon_group = TaxonGroupF.create(
            level=TAXON_GROUP_LEVEL_2,
            site=Site.objects.get_current()
        )
        self.assertTrue(
            Permission.objects.filter(
                name=taxon_group.permission_name
            ).exists()
        )
        self.assertTrue(
            Group.objects.filter(
                name=taxon_group.group_name
            ).exists()
        )
        self.assertTrue(str(taxon_group), f'{taxon_group.name} - {taxon_group.level}')

    def test_add_taxon_group_level_3(self):
        ancestor_taxon_group = TaxonGroupF.create(
            level=TAXON_GROUP_LEVEL_1,
            site=Site.objects.get_current()
        )
        parent_taxon_group = TaxonGroupF.create(
            level=TAXON_GROUP_LEVEL_2,
            site=Site.objects.get_current(),
            parent=ancestor_taxon_group
        )
        taxon_group = TaxonGroupF.create(
            level=TAXON_GROUP_LEVEL_3,
            site=Site.objects.get_current(),
            parent=parent_taxon_group
        )
        child_permission = Permission.objects.filter(
            name=taxon_group.permission_name
        ).first()
        parent_permission = Permission.objects.filter(
            name=parent_taxon_group.permission_name
        )
        parent_group = Group.objects.filter(
            name=parent_taxon_group.group_name
        ).first()
        self.assertIsNotNone(parent_permission)
        self.assertTrue(
            Group.objects.filter(
                name=taxon_group.group_name
            ).exists()
        )
        self.assertTrue(
            parent_group.permissions.filter(
                id__in=[child_permission.id]
            ).exists()
        )

    def test_add_taxon_group_level_3_without_site(self):
        taxon_group = TaxonGroupF.create(
            level=TAXON_GROUP_LEVEL_3,
            site=None
        )
        self.assertFalse(
            Permission.objects.filter(
                name=taxon_group.permission_name
            ).exists()
        )

    def test_update_taxon_group(self):
        taxon_group = TaxonGroupF.create(
            name='test1',
            level=TAXON_GROUP_LEVEL_3,
            site=Site.objects.get_current()
        )
        self.assertTrue(
            'test1' in Permission.objects.filter(
                name=taxon_group.permission_name
            ).first().name
        )
        taxon_group.name = 'test2'
        taxon_group.save()
        self.assertFalse(
            'test1' in Permission.objects.filter(
                name=taxon_group.permission_name
            ).first().name
        )


class TestTaxonGroupReadOnly(TestCase):
    """Tests for the is_readonly / upstream_url / upstream_id fields."""

    def test_defaults_are_not_readonly(self):
        tg = TaxonGroupF.create()
        self.assertFalse(tg.is_readonly)
        self.assertEqual(tg.upstream_url, '')
        self.assertEqual(tg.upstream_id, '')

    def test_can_set_readonly_with_upstream_info(self):
        tg = TaxonGroupF.create(
            is_readonly=True,
            upstream_url='https://fbis.example.org',
            upstream_id='42',
        )
        tg.refresh_from_db()
        self.assertTrue(tg.is_readonly)
        self.assertEqual(tg.upstream_url, 'https://fbis.example.org')
        self.assertEqual(tg.upstream_id, '42')

    def test_readonly_group_can_be_updated_to_false(self):
        tg = TaxonGroupF.create(is_readonly=True, upstream_url='https://x.test', upstream_id='1')
        tg.is_readonly = False
        tg.upstream_url = ''
        tg.upstream_id = ''
        tg.save()
        tg.refresh_from_db()
        self.assertFalse(tg.is_readonly)
        self.assertEqual(tg.upstream_url, '')
        self.assertEqual(tg.upstream_id, '')

    def test_upstream_id_stored_as_string(self):
        tg = TaxonGroupF.create(is_readonly=True, upstream_id='999')
        tg.refresh_from_db()
        self.assertEqual(tg.upstream_id, '999')


class TestTaxonGroupTotalValidated(TestCase):
    def setUp(self):
        self.factory = APIRequestFactory()
        User = get_user_model()
        self.superuser = User.objects.create_superuser(
            username="admin",
            email="admin@example.com",
            password="x",
        )
        self.user = User.objects.create_user(
            username="user",
            email="user@example.com",
            password="x",
        )

    def _link(self, group, taxonomy, *, is_validated=True):
        """
        Attach a taxonomy to a taxon group with the is_validated flag.
        Works for explicit-through M2M using through_defaults.
        """
        group.taxonomies.add(taxonomy, through_defaults={"is_validated": is_validated})

    def _call(self, group, user):
        view = TaxonGroupTotalValidated.as_view()
        req = self.factory.get(f"/fake/taxon-group/{group.id}/totals/")
        force_authenticate(req, user=user)
        resp = view(req, id=group.id)
        self.assertEqual(resp.status_code, 200)
        return resp.data

    def test_superuser_sees_all_buckets(self):
        # Parent group
        tg = TaxonGroupF.create()

        # Accepted validated (ACCEPTED and BLANK count as accepted; DOUBTFUL is its own bucket)
        t_acc = TaxonomyF.create(taxonomic_status="ACCEPTED")
        t_doubt = TaxonomyF.create(taxonomic_status="DOUBTFUL")
        t_blank = TaxonomyF.create(taxonomic_status="")  # blank -> accepted

        # Synonym validated
        t_syn = TaxonomyF.create(taxonomic_status="SYNONYM")

        # Unvalidated
        t_acc_u = TaxonomyF.create(taxonomic_status="ACCEPTED")
        t_syn_u = TaxonomyF.create(taxonomic_status="MISAPPLIED")  # synonym subclass

        # Link them with validation flags
        self._link(tg, t_acc, is_validated=True)
        self._link(tg, t_doubt, is_validated=True)
        self._link(tg, t_blank, is_validated=True)
        self._link(tg, t_syn, is_validated=True)

        self._link(tg, t_acc_u, is_validated=False)
        self._link(tg, t_syn_u, is_validated=False)

        data = self._call(tg, self.superuser)

        self.assertEqual(data["accepted_validated"], 2)     # acc + blank
        self.assertEqual(data["doubtful_validated"], 1)
        self.assertEqual(data["synonym_validated"], 1)      # synonym
        self.assertEqual(data["accepted_unvalidated"], 1)   # acc_u
        self.assertEqual(data["synonym_unvalidated"], 1)    # misapplied

        self.assertEqual(data["total_validated"], 4)
        self.assertEqual(data["total_unvalidated"], 2)

    def test_regular_user_hides_unvalidated_counts(self):
        tg = TaxonGroupF.create()

        t_acc = TaxonomyF.create(taxonomic_status="ACCEPTED")
        t_syn = TaxonomyF.create(taxonomic_status="SYNONYM")
        t_acc_u = TaxonomyF.create(taxonomic_status="ACCEPTED")
        t_syn_u = TaxonomyF.create(taxonomic_status="HOMOTYPIC_SYNONYM")

        self._link(tg, t_acc, is_validated=True)
        self._link(tg, t_syn, is_validated=True)
        self._link(tg, t_acc_u, is_validated=False)
        self._link(tg, t_syn_u, is_validated=False)

        data = self._call(tg, self.user)

        # Validated visible
        self.assertEqual(data["accepted_validated"], 1)
        self.assertEqual(data["synonym_validated"], 1)
        self.assertEqual(data["total_validated"], 2)

        # Unvalidated hidden for non-expert/non-superuser
        self.assertEqual(data["accepted_unvalidated"], 0)
        self.assertEqual(data["synonym_unvalidated"], 0)
        self.assertEqual(data["total_unvalidated"], 0)

    def test_recurses_into_children(self):
        parent = TaxonGroupF.create()
        child = TaxonGroupF.create(parent=parent)

        # Put everything on the child and assert parent includes them
        t_acc = TaxonomyF.create(taxonomic_status="ACCEPTED")
        t_syn = TaxonomyF.create(taxonomic_status="PROPARTE_SYNONYM")

        self._link(child, t_acc, is_validated=True)
        self._link(child, t_syn, is_validated=False)

        data_super = self._call(parent, self.superuser)
        self.assertEqual(data_super["accepted_validated"], 1)
        self.assertEqual(data_super["synonym_unvalidated"], 1)
        self.assertEqual(data_super["total_validated"], 1)
        self.assertEqual(data_super["total_unvalidated"], 1)

        data_user = self._call(parent, self.user)
        self.assertEqual(data_user["accepted_validated"], 1)
        self.assertEqual(data_user["synonym_unvalidated"], 0)  # hidden
        self.assertEqual(data_user["total_unvalidated"], 0)

    def test_contributor_sees_unvalidated_counts(self):
        tg = TaxonGroupF.create(contributors=(self.user,))

        t_acc = TaxonomyF.create(taxonomic_status="ACCEPTED")
        t_syn = TaxonomyF.create(taxonomic_status="SYNONYM")
        t_acc_u = TaxonomyF.create(taxonomic_status="ACCEPTED")
        t_syn_u = TaxonomyF.create(taxonomic_status="HETEROTYPIC_SYNONYM")

        self._link(tg, t_acc, is_validated=True)
        self._link(tg, t_syn, is_validated=True)
        self._link(tg, t_acc_u, is_validated=False)
        self._link(tg, t_syn_u, is_validated=False)

        data = self._call(tg, self.user)

        self.assertEqual(data["accepted_validated"], 1)
        self.assertEqual(data["synonym_validated"], 1)
        self.assertEqual(data["accepted_unvalidated"], 1)
        self.assertEqual(data["synonym_unvalidated"], 1)
        self.assertEqual(data["total_unvalidated"], 2)

    def test_contributor_recurses_into_children(self):
        parent = TaxonGroupF.create(contributors=(self.user,))
        child = TaxonGroupF.create(parent=parent)

        t_acc = TaxonomyF.create(taxonomic_status="ACCEPTED")
        t_syn_u = TaxonomyF.create(taxonomic_status="SYNONYM")

        self._link(child, t_acc, is_validated=True)
        self._link(child, t_syn_u, is_validated=False)

        data = self._call(parent, self.user)
        self.assertEqual(data["accepted_validated"], 1)
        self.assertEqual(data["synonym_unvalidated"], 1)
        self.assertEqual(data["total_unvalidated"], 1)

    def test_non_contributor_still_hidden(self):
        tg = TaxonGroupF.create()  # user is neither expert nor contributor

        t_acc_u = TaxonomyF.create(taxonomic_status="ACCEPTED")
        self._link(tg, t_acc_u, is_validated=False)

        data = self._call(tg, self.user)
        self.assertEqual(data["accepted_unvalidated"], 0)
        self.assertEqual(data["total_unvalidated"], 0)
