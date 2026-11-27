import json
from django.contrib.auth.mixins import UserPassesTestMixin
from django.contrib.sites.models import Site
from django.http import Http404
from preferences import preferences
from rest_framework.response import Response
from rest_framework.views import APIView

from bims.api_views.taxon_update import create_taxon_proposal, is_expert
from bims.utils.taxonomy import ensure_fada_id
from bims.models import (
    TaxonGroup, Taxonomy, BiologicalCollectionRecord,
    TaxonExtraAttribute, TaxonomicGroupCategory,
    TaxonomyUpdateProposal, OccurrenceUploadTemplate, TaxonGroupTaxonomy
)
from bims.models.meta_group import MetaGroup


def update_taxon_group_orders(taxon_group_ids):
    """
    Update taxon group orders
    :param taxon_group_ids: list of taxon taxon group ids
    """
    taxon_groups = TaxonGroup.objects.filter(
        id__in=taxon_group_ids
    )
    for taxon_group in taxon_groups:
        try:
            order = taxon_group_ids.index(taxon_group.id)
            taxon_group.display_order = order
            taxon_group.save()
        except ValueError:
            continue


def _delete_unused_ancestors(parent):
    """
    Walk up the parent chain starting at `parent`, deleting each ancestor that has
    become unused (not linked to any taxon group, not referenced by an occurrence,
    and has no remaining children). Stops as soon as an ancestor still in use is found.
    """
    while parent is not None:
        next_parent = parent.parent
        still_in_use = (
            TaxonGroupTaxonomy.objects.filter(taxonomy=parent).exists()
            or BiologicalCollectionRecord.objects.filter(taxonomy=parent).exists()
            or Taxonomy.objects.filter(parent=parent).exists()
        )
        if still_in_use:
            break
        parent.delete()
        parent = next_parent


def _remove_single_taxon(taxonomy, taxon_group, delete_if_orphaned=True):
    """
    Remove one taxonomy from a taxon group.
    :param delete_if_orphaned: when True, permanently delete the taxonomy if it no longer
                               belongs to any other group. Defaults to True.
    """
    taxon_group.taxonomies.remove(taxonomy)
    # Only the occurrences of the module the taxon is removed from lose it;
    # they move to the other module of the taxon when it has exactly one.
    # Occurrences of other modules (e.g. when the taxon is removed from a
    # division group) keep theirs.
    remaining_modules = list(TaxonGroup.objects.filter(
        taxonomies=taxonomy,
        category=TaxonomicGroupCategory.SPECIES_MODULE.name,
    ).exclude(id=taxon_group.id)[:2])
    BiologicalCollectionRecord.objects.filter(
        taxonomy=taxonomy,
        module_group=taxon_group,
    ).update(
        module_group=(
            remaining_modules[0] if len(remaining_modules) == 1 else None
        )
    )
    if delete_if_orphaned and not TaxonGroupTaxonomy.objects.filter(taxonomy=taxonomy).exists():
        parent = taxonomy.parent
        taxonomy.delete()
        _delete_unused_ancestors(parent)


def remove_taxa_from_taxon_group(taxa_ids, taxon_group_id, include_children=False):
    """
    Remove taxa from a taxon group.
    - When include_children=True: parent and all child taxa are removed; orphaned taxa are
      permanently deleted.
    - When include_children=False: only the parent is removed. If the parent still has children
      remaining in the group it is only unlinked (not permanently deleted), to preserve the
      parent–child relationship. If it has no children in the group it is deleted if orphaned.
    :param taxa_ids: list of taxon ids
    :param taxon_group_id: id of the taxon group
    :param include_children: if True, also remove child taxa from the group
    """
    taxa = Taxonomy.objects.filter(
        id__in=taxa_ids
    )
    try:
        taxon_group = TaxonGroup.objects.get(
            id=taxon_group_id
        )
    except TaxonGroup.DoesNotExist:
        return
    for taxonomy in taxa:
        children_in_group = taxonomy.get_all_children().filter(
            taxongrouptaxonomy__taxongroup=taxon_group
        ).distinct()

        if include_children:
            for child in children_in_group:
                _remove_single_taxon(child, taxon_group, delete_if_orphaned=True)
            # Do not permanently delete the parent if it still has children in OTHER groups;
            # those children reference it via the parent FK and must not be left orphaned.
            has_children_in_other_groups = Taxonomy.objects.filter(
                parent=taxonomy
            ).exclude(
                taxongrouptaxonomy__taxongroup=taxon_group
            ).filter(
                taxongrouptaxonomy__isnull=False
            ).distinct().exists()
            _remove_single_taxon(taxonomy, taxon_group, delete_if_orphaned=not has_children_in_other_groups)
        else:
            # Protect the parent from permanent deletion if it still has children in this group
            has_children_in_group = children_in_group.exists()
            _remove_single_taxon(taxonomy, taxon_group, delete_if_orphaned=not has_children_in_group)


def add_taxa_to_taxon_group(taxa_ids, taxon_group_id):
    """
    Add taxa to taxon group.
    Returns a list of dicts for taxa that were rejected because they already
    belong to another group (only when restrict_taxon_to_single_group is on).
    Each dict has 'id' and 'name'.
    """
    taxa = Taxonomy.objects.filter(
        id__in=taxa_ids
    )
    try:
        taxon_group = TaxonGroup.objects.get(
            id=taxon_group_id
        )
    except TaxonGroup.DoesNotExist:
        return []

    restrict = preferences.SiteSetting.restrict_taxon_to_single_group
    rejected = []

    for taxonomy in taxa:
        if restrict:
            already_in_other_group = TaxonGroupTaxonomy.objects.filter(
                taxonomy=taxonomy
            ).exclude(taxongroup=taxon_group).exists()
            if already_in_other_group:
                rejected.append({
                    'id': taxonomy.id,
                    'name': taxonomy.canonical_name or taxonomy.scientific_name,
                })
                continue

        if not taxon_group.taxonomies.filter(id=taxonomy.id).exists():
            create_taxon_proposal(taxonomy, taxon_group)
        taxon_group.taxonomies.add(
            taxonomy,
            through_defaults={
                'is_validated': False
            }
        )
        ensure_fada_id(taxonomy)

    return rejected


class TaxaUpdateMixin(UserPassesTestMixin, APIView):
    def test_func(self):
        return self.request.user.has_perm('bims.change_taxongroup')


class UpdateTaxonGroupOrder(TaxaUpdateMixin):
    """Api to update taxon groups order.
    Post data required:
    {
        'taxonGroups': [1,2] // List of taxon groups id sorted by their order
    }
    """
    def post(self, request, *args):
        taxon_groups_array = self.request.POST.get('taxonGroups', None)
        if not taxon_groups_array:
            raise Http404('Missing taxon groups')
        taxon_group_ids = json.loads(taxon_groups_array)
        update_taxon_group_orders(taxon_group_ids)
        return Response('Updated')


class RemoveTaxaFromTaxonGroup(TaxaUpdateMixin):
    """Api to remove taxa from taxon group.
    Post data required:
    {
        'taxaIds': [1,2], // List of taxa id
        'taxonGroupId': 1, // id of the taxon group
        'includeChildren': 'true' // optional, also remove child taxa
    }
    """
    def post(self, request, *args):
        taxa_ids = self.request.POST.get('taxaIds', None)
        taxon_group_id = self.request.POST.get('taxonGroupId', None)
        include_children = self.request.POST.get('includeChildren', 'false').lower() == 'true'
        if not taxa_ids or not taxon_group_id:
            raise Http404('Missing required parameter')
        taxa_ids = json.loads(taxa_ids)
        taxon_group_id = int(taxon_group_id)
        remove_taxa_from_taxon_group(taxa_ids, taxon_group_id, include_children)
        return Response(
            {
                'taxonomy_count': TaxonGroup.objects.get(
                    id=taxon_group_id
                ).taxonomies.all().count()
            }
        )


class AddTaxaToTaxonGroup(TaxaUpdateMixin):
    """Api to add taxa to taxon group.
    Post data required:
    {
        'taxaIds': [1,2], // List of taxa id
        'taxonGroupId': 1 // id of the taxon group
    }
    """
    def test_func(self):
        if super().test_func():
            return True
        taxon_group_id = self.request.POST.get('taxonGroupId')
        taxon_group = TaxonGroup.objects.filter(id=taxon_group_id).first()
        return is_expert(self.request.user, taxon_group)

    def post(self, request, *args):
        taxa_ids = self.request.POST.get('taxaIds', None)
        taxon_group_id = self.request.POST.get('taxonGroupId', None)
        if not taxa_ids or not taxon_group_id:
            raise Http404('Missing required parameter')
        taxa_ids = json.loads(taxa_ids)
        taxon_group_id = int(taxon_group_id)
        rejected = add_taxa_to_taxon_group(taxa_ids, taxon_group_id)
        return Response(
            {
                'taxonomy_count': TaxonGroup.objects.get(
                    id=taxon_group_id
                ).taxonomies.all().count(),
                'rejected': rejected,
            }
        )



class UpdateTaxonGroup(TaxaUpdateMixin):
    """
    API to update or add a new taxon group.

    POST form-data:
    - module_id (optional): if provided, update that TaxonGroup; if not, create new.
    - module_name
    - module_logo (file)
    - parent-taxon (id)
    - gbif-species (taxonomy id)
    - extra_attribute: can repeat multiple times
    - taxon-group-experts: can repeat multiple times
    - taxon-group-contributors: can repeat multiple times
    - taxa_upload_template: file
    - occurrence_upload_template: file           [legacy single upload]
    - occurrence_upload_templates: <multiple files> [new multi-upload]
    - delete_occurrence_template: <id> repeated   [mark rows to delete]
    """

    def post(self, request, *args):
        module_name = request.POST.get('module_name')
        module_logo = request.FILES.get('module_logo', None)
        module_id = request.POST.get('module_id', None)
        extra_attributes = request.POST.getlist('extra_attribute', [])
        new_expert_ids = request.POST.getlist('taxon-group-experts', [])
        new_contributor_ids = request.POST.getlist('taxon-group-contributors', [])
        gbif_species = request.POST.get('gbif-species', None)
        parent_taxon_id = request.POST.get('parent-taxon', None)
        meta_group_id = request.POST.get('meta_group', None)
        taxonworks_base_url = (request.POST.get('taxonworks_base_url') or '').strip()

        taxa_upload_template = request.FILES.get('taxa_upload_template', None)

        legacy_occurrence_upload_template = request.FILES.get(
            'occurrence_upload_template', None
        )

        occurrence_upload_templates_files = request.FILES.getlist(
            'occurrence_upload_templates'
        )

        delete_occurrence_template_ids = request.POST.getlist(
            'delete_occurrence_template', []
        )

        if module_id:
            try:
                taxon_group = TaxonGroup.objects.get(id=module_id)
            except TaxonGroup.DoesNotExist:
                raise Http404('Taxon group does not exist')
        else:
            taxon_group = TaxonGroup()

        taxon_group.site = Site.objects.get_current()

        if not taxon_group.parent:
            taxon_group.category = TaxonomicGroupCategory.SPECIES_MODULE.name

        if module_name:
            taxon_group.name = module_name

        if module_logo:
            taxon_group.logo = module_logo

        if gbif_species:
            taxon_group.gbif_parent_species_id = gbif_species

        if taxa_upload_template:
            taxon_group.taxa_upload_template = taxa_upload_template

        if legacy_occurrence_upload_template:
            taxon_group.occurrence_upload_template = legacy_occurrence_upload_template

        if parent_taxon_id:
            try:
                parent_taxon_group = TaxonGroup.objects.get(id=parent_taxon_id)
                taxon_group.parent = parent_taxon_group
            except TaxonGroup.DoesNotExist:
                pass
        else:
            taxon_group.parent = None

        if meta_group_id:
            try:
                taxon_group.meta_group = MetaGroup.objects.get(id=meta_group_id)
            except MetaGroup.DoesNotExist:
                pass
        else:
            taxon_group.meta_group = None

        taxon_group.taxonworks_base_url = taxonworks_base_url

        taxon_group.save()

        TaxonExtraAttribute.objects.filter(
            taxon_group=taxon_group
        ).exclude(
            name__in=extra_attributes
        ).delete()

        for extra_attr in extra_attributes:
            if not extra_attr:
                continue
            try:
                TaxonExtraAttribute.objects.get_or_create(
                    taxon_group=taxon_group,
                    name=extra_attr
                )
            except TaxonExtraAttribute.MultipleObjectsReturned:
                pass

        if delete_occurrence_template_ids:
            OccurrenceUploadTemplate.objects.filter(
                taxon_group=taxon_group,
                id__in=delete_occurrence_template_ids
            ).delete()

        for f in occurrence_upload_templates_files:
            if not f:
                continue
            OccurrenceUploadTemplate.objects.create(
                taxon_group=taxon_group,
                file=f,
                label=''
            )

        if new_expert_ids:
            cleaned_expert_ids = []
            for expert_id in new_expert_ids:
                try:
                    cleaned_expert_ids.append(int(expert_id))
                except (ValueError, TypeError):
                    continue
            taxon_group.experts.set(cleaned_expert_ids)
        else:
            taxon_group.experts.clear()

        if new_contributor_ids:
            cleaned_contributor_ids = []
            for contributor_id in new_contributor_ids:
                try:
                    cleaned_contributor_ids.append(int(contributor_id))
                except (ValueError, TypeError):
                    continue
            taxon_group.contributors.set(cleaned_contributor_ids)
        else:
            taxon_group.contributors.clear()

        return Response(
            'Taxon group updated' if module_id else 'New taxon group added'
        )
