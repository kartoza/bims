# coding=utf8
import ast
import logging
import re

from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema

from django.contrib.auth import get_user_model
from django.db import transaction, IntegrityError
from django.forms import model_to_dict
from django.http import Http404, JsonResponse
from django.db.models import Count, Case, Value, When, F, CharField, Prefetch, Q
from django.contrib.auth.mixins import LoginRequiredMixin
from rest_framework import status
from rest_framework.generics import UpdateAPIView, get_object_or_404
from rest_framework.permissions import IsAuthenticated, IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_403_FORBIDDEN, HTTP_200_OK
from rest_framework.views import APIView
from rest_framework.pagination import PageNumberPagination
from taggit.models import Tag

from bims.api_views.merge_sites import IsSuperUser
from bims.api_views.taxon_update import is_expert
from bims.models.taxonomy import Taxonomy, TaxonTag, CustomTaggedTaxonomy
from bims.serializers.taxon_detail_serializer import TaxonDetailSerializer
from bims.serializers.taxon_serializer import TaxonSerializer
from bims.models.biological_collection_record import (
    BiologicalCollectionRecord
)
from bims.models import TaxonGroup, VernacularName, TaxonGroupTaxonomy
from bims.enums.taxonomic_rank import TaxonomicRank
from bims.utils.gbif import suggest_search, update_taxonomy_from_gbif, get_vernacular_names
from bims.serializers.tag_serializer import TagSerializer, TaxonomyTagUpdateSerializer
from bims.models.taxonomy_update_proposal import TaxonomyUpdateProposal
from bims.utils.iucn import get_iucn_status
from bims.tasks.taxa import fetch_iucn_status, approve_unvalidated_taxa_by_group, \
    clear_taxa_not_associated_in_taxon_group

logger = logging.getLogger('bims')

User = get_user_model()


class TaxonDetail(APIView):
    """
    Retrieve a taxon instance.
    """

    def get_object(self, pk):
        try:
            return Taxonomy.objects.get(pk=pk)
        except Taxonomy.DoesNotExist:
            raise Http404

    def get_taxonomic_rank_values(self, taxonomy, max_depth=24):
        """Build rank values walking up parents, but stop on cycles or silly depth."""
        values = []
        visited = set()
        current = taxonomy
        depth = 0

        while current and getattr(current, "id", None) not in visited and depth < max_depth:
            if current.id is not None:
                visited.add(current.id)
            try:
                rank_key = TaxonomicRank[current.rank].value.lower()
                values.append({rank_key: current.canonical_name})
            except KeyError:
                pass

            current = current.parent
            depth += 1

        if current is not None:
            logger.warning(
                "Detected taxonomy parent cycle or depth limit (start_id=%s, depth=%s).",
                getattr(taxonomy, "id", None), depth
            )
        return values

    def get_serializer_data(self, pk):
        taxon = self.get_object(pk)

        serializer = TaxonDetailSerializer(taxon)
        return serializer.data

    def get(self, request, pk, format=None):

        taxon = self.get_object(pk)
        data = self.get_serializer_data(pk)

        records = BiologicalCollectionRecord.objects.filter(
            taxonomy=taxon
        )

        # Endemism
        if taxon.endemism:
            data['endemism'] = taxon.endemism.name

        # Origins
        origin_value = ''
        origin = records.values_list(
            'taxonomy__origin__category', flat=True).distinct()
        if origin:
            origin_value = origin[0] or ''
        data['origin'] = origin_value

        data['count'] = records.count()
        data['total_sites'] = records.distinct('site').count()

        # Taxonomic rank tree
        taxonomic_rank = self.get_taxonomic_rank_values(taxon)
        for rank in taxonomic_rank:
            rank_key = list(rank.keys())[0]
            if rank_key not in data or data[rank_key] == '':
                data.update(rank)
        common_names = []
        results = []

        if 'common_name' in data and data['common_name']:
            common_names.append(data['common_name'])
        # Common name
        if taxon.vernacular_names.exists() and not common_names:
            common_names = list(set(
                taxon.vernacular_names.filter(language='eng').values_list('name', flat=True)))
            common_names.sort()

        if len(common_names) == 0 and taxon.gbif_key:
            vernacular_names = get_vernacular_names(taxon.gbif_key)
            if vernacular_names:
                results = vernacular_names['results']
            if len(results) == 0:
                data['common_name'] = 'Unknown'
            else:
                for result in results:
                    if 'language' in result and result['language'] == 'eng':
                        fields = {'language': result['language']}
                        data['common_name'] = result['vernacularName']
                        if 'source' in result:
                            fields['source'] = result['source']
                        if 'taxonKey' in result:
                            fields['taxon_key'] = int(result['taxonKey'])
                        vernacular_name, status = (
                            VernacularName.objects.get_or_create(
                                name=result['vernacularName'],
                                **fields
                            )
                        )
                        taxon.vernacular_names.add(vernacular_name)
                        break
        elif len(common_names) > 0:
            data['common_name'] = common_names[0]

        return Response(data)


class FindTaxon(APIView):
    """
    Find taxon in gbif and local database
    """
    limit_default = 20
    scientific_name = 'scientificName'
    canonical_name = 'canonicalName'
    rank = 'rank'
    key = 'key'
    taxa_id = 'taxaId'
    source = 'source'
    stored_local = 'storedLocal'
    validated = 'validated'
    taxon_group_ids = 'taxonGroupIds'
    status = 'status'

    @staticmethod
    def _get_taxon_group_and_phylum_keys(taxon_group_name=None, taxon_group_id=None):
        if not taxon_group_name and not taxon_group_id:
            return None, []

        try:
            if taxon_group_id:
                taxon_group = TaxonGroup.objects.get(id=taxon_group_id)
            else:
                taxon_group = TaxonGroup.objects.get(name=taxon_group_name)
        except (TaxonGroup.DoesNotExist, TaxonGroup.MultipleObjectsReturned):
            return None, []

        phylum_keys = list(
            taxon_group.taxonomies.filter(
                parent__rank=TaxonomicRank.PHYLUM
            ).values_list('parent__gbif_key', flat=True)
        )
        return taxon_group, phylum_keys

    def get(self, request, *args, **kwargs):
        taxon_list = []
        seen_keys = set()

        query_dict = request.GET.dict()
        taxon_name = (query_dict.get('q') or '').strip()

        taxon_group_name = query_dict.pop('taxonGroup', None)
        taxon_group_id = query_dict.pop('taxonGroupId', None)

        taxon_group, phylum_keys = self._get_taxon_group_and_phylum_keys(
            taxon_group_name=taxon_group_name,
            taxon_group_id=taxon_group_id
        )

        if 'limit' not in query_dict:
            query_dict['limit'] = self.limit_default

        gbif_response = suggest_search(query_dict) or []

        for gbif in gbif_response:
            key = gbif.get('key')
            if not key or key in seen_keys:
                continue

            phylum_key = gbif.get('phylumKey')
            if phylum_keys and phylum_key not in phylum_keys:
                continue

            seen_keys.add(key)

            taxa_qs = Taxonomy.objects.filter(gbif_key=key)
            stored_local = taxa_qs.exists()
            taxa_id = None
            validated = False
            taxon_group_ids = []
            status = gbif.get('status', '')

            if stored_local:
                taxon = taxa_qs.first()
                taxa_id = taxon.id
                status = taxon.taxonomic_status

                taxon_group_ids = list(
                    taxon.taxongrouptaxonomy_set.values_list('taxongroup_id', flat=True)
                )

                if taxon_group:
                    tgt = TaxonGroupTaxonomy.objects.filter(
                        taxonomy=taxon,
                        taxongroup=taxon_group
                    ).order_by('-id').first()
                    validated = bool(tgt and tgt.is_validated)
                else:
                    validated = TaxonGroupTaxonomy.objects.filter(
                        taxonomy=taxon,
                        is_validated=True
                    ).exists()

            canonical_name = gbif.get('canonicalName') or gbif.get('scientificName', '')

            taxon_list.append({
                self.scientific_name: gbif.get('scientificName', ''),
                self.canonical_name: canonical_name,
                self.rank: gbif.get('rank', ''),
                self.key: key,
                self.taxa_id: taxa_id or '',
                self.source: 'gbif',
                self.stored_local: stored_local,
                self.validated: validated,
                self.taxon_group_ids: taxon_group_ids,
                self.status: status,
            })

        if not taxon_list and taxon_name:
            taxa_qs = Taxonomy.objects.filter(
                canonical_name__icontains=taxon_name
            )

            if taxon_group:
                taxa_qs = taxa_qs.filter(
                    taxongrouptaxonomy__taxongroup=taxon_group
                )

            taxa_qs = taxa_qs.distinct()[: self.limit_default]

            for taxon in taxa_qs:
                tgt_qs = TaxonGroupTaxonomy.objects.filter(
                    taxonomy=taxon
                )
                if taxon_group:
                    tgt_qs = tgt_qs.filter(taxongroup=taxon_group)

                tgt = tgt_qs.order_by('-id').first()
                validated = bool(tgt and tgt.is_validated)

                taxon_list.append({
                    self.scientific_name: taxon.scientific_name,
                    self.canonical_name: taxon.canonical_name,
                    self.rank: taxon.rank,
                    self.key: taxon.gbif_key,
                    self.source: 'local' if not taxon.gbif_key else 'gbif',
                    self.stored_local: True,
                    self.taxa_id: taxon.id,
                    self.validated: validated,
                    self.taxon_group_ids: list(
                        taxon.taxongrouptaxonomy_set.values_list('taxongroup_id', flat=True)
                    ),
                    self.status: taxon.taxonomic_status,
                })

        return Response(taxon_list)


class TaxonProposalDetail(TaxonDetail):
    def get_object(self, pk):
        try:
            update_proposal = TaxonomyUpdateProposal.objects.get(pk=pk)
            return update_proposal.original_taxonomy
        except TaxonomyUpdateProposal.DoesNotExist:
            raise Http404

    def get_serializer_data(self, pk):
        serializer = TaxonDetailSerializer(
            TaxonomyUpdateProposal.objects.get(pk=pk)
        )
        return serializer.data


class AddNewTaxon(LoginRequiredMixin, APIView):
    """Add new taxon, then return the id of newly created taxon"""

    def post(self, request, *args):
        response = {
            'id': '',
            'taxon_name': '',
        }
        taxonomy = None
        gbif_key = self.request.POST.get('gbifKey', None)
        taxon_name = self.request.POST.get('taxonName', None)
        taxon_group = self.request.POST.get('taxonGroup', None)
        taxon_group_id = self.request.POST.get('taxonGroupId', None)
        author_name = self.request.POST.get('authorName', '')
        rank = self.request.POST.get('rank', None)
        family_id = self.request.POST.get('familyId', None)
        accepted_taxonomy_id = self.request.POST.get('acceptedTaxonomyId', None)
        taxonomic_status = (self.request.POST.get('taxonomicStatus') or '').strip().upper()
        is_synonym_or_doubtful = (
            taxonomic_status == 'DOUBTFUL' or 'SYNONYM' in taxonomic_status
        )
        parent = None

        if family_id:
            parent = Taxonomy.objects.get(id=int(family_id))
        parent_id = self.request.POST.get('parentId', None)
        if parent_id:
            parent = Taxonomy.objects.get(id=int(parent_id))

        if gbif_key:
            taxonomy = update_taxonomy_from_gbif(
                key=gbif_key,
                fetch_parent=not is_synonym_or_doubtful,
                get_vernacular=not is_synonym_or_doubtful
            )

        elif taxon_name and rank:
            if rank.lower() == 'species' and parent and parent.rank.lower() == 'genus':
                if parent.canonical_name not in taxon_name:
                    taxon_name = parent.canonical_name + ' ' + taxon_name
            elif rank.lower() == 'subspecies' and parent and parent.rank.lower() == 'species':
                species_name = parent.species_name
                if species_name not in taxon_name:
                    taxon_name = species_name + ' ' + taxon_name

            taxon_name = taxon_name.strip()

            existing_qs = Taxonomy.objects.filter(
                canonical_name__iexact=taxon_name
            )

            if existing_qs.exists():
                taxonomy = existing_qs.first()
            else:
                try:
                    taxonomy, created = Taxonomy.objects.get_or_create(
                        scientific_name=taxon_name,
                        canonical_name=taxon_name,
                        rank=rank
                    )
                except IntegrityError:
                    taxonomy = Taxonomy.objects.get(
                        scientific_name=taxon_name,
                        canonical_name=taxon_name,
                        rank=rank
                    )

        if taxon_group_id:
            taxon_group = TaxonGroup.objects.get(id=taxon_group_id)
            taxon_group.taxonomies.add(
                taxonomy,
                through_defaults={
                    'is_validated': False
                }
            )
        else:
            if taxon_group:
                try:
                    taxon_group = TaxonGroup.objects.get(name=taxon_group)
                    taxon_group.taxonomies.add(
                        taxonomy,
                        through_defaults={
                            'is_validated': False
                        }
                    )
                    taxon_group_id = taxon_group.id
                except TaxonGroup.DoesNotExist:
                    pass

        if taxon_group and taxonomy:
            from bims.api_views.taxon_update import ensure_accepted_taxonomy_in_group
            ensure_accepted_taxonomy_in_group(taxonomy, taxon_group)

        if taxonomy:
            response['id'] = taxonomy.id
            response['taxon_name'] = taxonomy.canonical_name

            if not taxonomy.taxonomic_status:
                taxonomy.taxonomic_status = taxonomic_status
                taxonomy.save()

            if author_name:
                taxonomy.author = author_name
                taxonomy.save()

            if not TaxonGroupTaxonomy.objects.filter(
                taxonomy=taxonomy,
                taxongroup=taxon_group,
                is_validated=True
            ).exists():
                taxonomy.owner = self.request.user
                taxonomy.last_modified_by = self.request.user
                taxonomy.ready_to_be_validate()
                taxonomy.send_new_taxon_email(taxon_group_id)

            if parent and not is_synonym_or_doubtful:
                taxonomy.parent = parent
                taxonomy.save()

            if accepted_taxonomy_id:
                try:
                    accepted_taxonomy = Taxonomy.objects.get(id=int(accepted_taxonomy_id))
                    taxonomy.accepted_taxonomy = accepted_taxonomy
                    taxonomy.save()
                except (Taxonomy.DoesNotExist, ValueError):
                    pass

            from bims.templatetags.site import is_fada_site
            if is_fada_site() and not taxonomy.fada_id:
                taxonomy.fada_id = f'FADA-{taxonomy.id}'
                taxonomy.save(update_fields=['fada_id'])

        with transaction.atomic():
            taxonomy_data = model_to_dict(
                taxonomy,
                exclude=[
                    'id',
                    'iucn_status',
                    'national_conservation_status',
                    'vernacular_names',
                    'author',
                    'tags',
                    'biographic_distributions',
                    'accepted_taxonomy',
                    'owner',
                    'parent',
                    'last_modified_by',
                    'origin',
                    'endemism',
                    # Taxonomy-only fields not present on TaxonomyUpdateProposal
                    'checklist_version_uuid',
                    'last_checklist_published_uuid',
                ]
            )
            proposal_author = author_name or taxonomy.author
            taxonomy_update_proposal, created = (
                TaxonomyUpdateProposal.objects.get_or_create(
                    original_taxonomy=taxonomy,
                    taxon_group=taxon_group,
                    status='pending',
                    new_data=True,
                    owner=taxonomy.owner,
                    parent=taxonomy.parent,
                    accepted_taxonomy=taxonomy.accepted_taxonomy,
                    taxon_group_under_review=taxon_group,
                    author=proposal_author,
                    iucn_status=taxonomy.iucn_status,
                    national_conservation_status=taxonomy.national_conservation_status,
                    last_modified_by=self.request.user,
                    origin=taxonomy.origin,
                    endemism=taxonomy.endemism,
                    **taxonomy_data
                )
            )
            if created:
                vernacular_names_instances = list(taxonomy.vernacular_names.all())
                taxonomy_update_proposal.vernacular_names.set(
                    vernacular_names_instances
                )

        return Response(response)


class TaxaPagination(PageNumberPagination):
    page_size = 20
    page_size_query_param = 'page_size'


def split_authors(author_string):
    regex = r'"(.*?)"'
    matches = re.findall(regex, author_string)
    decoded_matches = [match for match in matches]
    return decoded_matches


class TaxaList(APIView):
    """Returns list of taxa filtered by taxon group"""
    pagination_class = TaxaPagination

    @staticmethod
    def get_descendant_group_ids(taxon_group):
        """Recursively collect all descendant group IDs"""
        group_ids = [taxon_group.id]
        child_groups = TaxonGroup.objects.filter(
            parent=taxon_group)
        for child in child_groups:
            group_ids.extend(TaxaList.get_descendant_group_ids(
                child))
        return group_ids

    @staticmethod
    def get_taxa_by_parameters(request):
        taxon_group_id = request.GET.get('taxonGroup', '')
        rank = request.GET.get('rank', '')
        ranks = request.GET.get('ranks', '').split(',')
        ranks = list(filter(None, ranks))
        origins = request.GET.get('origins', '').split(',')
        origins = list(filter(None, origins))
        tags = request.GET.get('tags', '').split(',')
        tags = list(filter(None, tags))
        tag_filter_type = request.GET.get('tagFT', 'OR')
        cons_status = request.GET.get('cons_status', '').split(',')
        cons_status = list(filter(None, cons_status))
        endemism = request.GET.get('endemism', '').split(',')
        endemism = list(filter(None, endemism))
        taxonomic_status = request.GET.get('taxonomic_status', '').split(',')
        taxonomic_status = list(filter(None, taxonomic_status))
        taxon_name = request.GET.get('taxon', '').strip()
        is_gbif = request.GET.get('is_gbif', '')
        is_iucn = request.GET.get('is_iucn', '')
        validated = request.GET.get('validated', 'True')
        order = request.GET.get('o', '')
        author_names = request.GET.get('author', '')
        family_name = request.GET.get('family', '')
        genus_name = request.GET.get('genus', '')
        species_name = request.GET.get('species', '')
        taxon_group_ids = None

        authors = []
        if author_names:
            authors = split_authors(author_names)

        biodiversity_distributions = (
            request.GET.get('bD', '').split(',')
        )
        biodiversity_distributions = (
            list(filter(None, biodiversity_distributions))
        )
        biodiversity_distributions_filter_type = (
            request.GET.get('bDFT', 'OR')
        )

        if order == 'endemism_name':
            order = 'endemism__name'

        if 'accepted_taxonomy_name' in order:
            order = order.replace(
                'accepted_taxonomy_name',
                'accepted_taxonomy__canonical_name'
            )

        # Filter by parent
        parent_ids = request.GET.get('parent', '').split(',')
        parent_ids = list(filter(None, parent_ids))
        id = request.GET.get('id', '')
        if id:
            return Taxonomy.objects.filter(id=id)

        if taxon_group_id:
            try:
                taxon_group = TaxonGroup.objects.get(id=taxon_group_id)
            except TaxonGroup.DoesNotExist:
                raise Http404('Taxon group does not exist')
        else:
            taxon_group = None

        taxon_list = Taxonomy.objects.all()

        if parent_ids:
            parents = Taxonomy.objects.filter(
                Q(id__in=parent_ids)
            )
            if parents.exists():
                taxon_list = parents.first().get_all_children()
            else:
                taxon_list = parents

        if taxon_group:
            taxon_group_ids = TaxaList.get_descendant_group_ids(
                taxon_group)
            taxon_list = taxon_list.filter(
                taxongroup__id__in=taxon_group_ids,
                taxongrouptaxonomy__is_rejected=False,
            ).distinct().order_by('canonical_name')
        else:
            taxon_list = taxon_list.filter(
                taxongrouptaxonomy__is_rejected=False,
            ).distinct().order_by('canonical_name')

        if len(authors) > 0:
            taxon_list = taxon_list.filter(
                author__in=authors
            )

        if rank:
            taxon_list = taxon_list.filter(rank=rank)
        if len(ranks) > 0:
            taxon_list = taxon_list.filter(
                rank__in=ranks
            )
        if len(origins) > 0:
            taxon_list = taxon_list.filter(
                origin__origin_key__in=origins
            )
        if len(cons_status) > 0:
            taxon_list = taxon_list.filter(
                iucn_status__category__in=cons_status
            )
        if len(endemism) > 0:
            taxon_list = taxon_list.filter(
                endemism__name__in=endemism
            )
        if len(taxonomic_status) > 0:
            queries = Q()
            for status in taxonomic_status:
                queries |= Q(taxonomic_status__iexact=status)
            taxon_list = taxon_list.filter(queries)
        if taxon_name:
            taxon_list = taxon_list.filter(
                Q(canonical_name__icontains=taxon_name) |
                Q(accepted_taxonomy__canonical_name__icontains=taxon_name) |
                Q(scientific_name__icontains=taxon_name)
            )
        if family_name:
            taxon_list = taxon_list.filter(
                hierarchical_data__family_name__iexact=family_name
            )
        if genus_name:
            taxon_list = taxon_list.filter(
                hierarchical_data__genus_name__iexact=genus_name
            )
        if species_name:
            taxon_list = taxon_list.filter(
                hierarchical_data__species_name__iexact=species_name
            )

        if tags:
            taxon_list = taxon_list.prefetch_related(
                'tags',
            )
            if tag_filter_type == 'AND':
                for tag in tags:
                    taxon_list = taxon_list.filter(tags__name=tag)
            else:
                taxon_list = taxon_list.filter(
                    tags__name__in=tags
                ).distinct()
        if biodiversity_distributions:
            taxon_list = taxon_list.prefetch_related(
                'biographic_distributions'
            )
            if biodiversity_distributions_filter_type == 'AND':
                for b_tag in biodiversity_distributions:
                    taxon_list = taxon_list.filter(
                        customtaggedtaxonomy__tag__name=b_tag
                    )
            else:
                taxon_list = taxon_list.filter(
                    customtaggedtaxonomy__tag__name__in=biodiversity_distributions
                ).distinct()
        if validated:
            try:
                validated = validated.replace('/', '').lower() == 'true'
                if not validated:
                    # Check if the user is a superuser or has expert permissions for the taxon group
                    is_user_expert = is_expert(
                        request.user,
                        TaxonGroup.objects.get(id=taxon_group_id)
                    )
                    if request.user.is_superuser or is_user_expert:
                        validated_filters = {
                            'taxongrouptaxonomy__is_validated': False,
                        }
                    else:
                        taxon_list = taxon_list.none()
                        return taxon_list
                else:
                    validated_filters = {
                        'taxongrouptaxonomy__is_validated': True,
                    }
                if taxon_group_ids:
                    validated_filters[
                        'taxongrouptaxonomy__taxongroup__in'
                    ] = taxon_group_ids
                taxon_list = taxon_list.filter(
                    **validated_filters
                )
            except ValueError:
                pass
        if is_gbif:
            try:
                is_gbif = is_gbif.lower() == 'true'
                if is_gbif:
                    taxon_list = taxon_list.exclude(
                        gbif_key__isnull=True
                    )
                else:
                    taxon_list = taxon_list.filter(
                        gbif_key__isnull=True
                    )
            except ValueError:
                pass
        if is_iucn:
            try:
                is_iucn = is_iucn.lower() == 'true'
                if is_iucn:
                    taxon_list = taxon_list.exclude(
                        iucn_redlist_id__isnull=True
                    )
                else:
                    taxon_list = taxon_list.filter(
                        iucn_redlist_id__isnull=True
                    )
            except ValueError:
                pass
        from bims.templatetags.site import is_fada_site
        if is_fada_site():
            taxon_list = taxon_list.exclude(
                Q(fada_id__isnull=True) | Q(fada_id='')
            )

        if order:
            if 'total_records' in order:
                taxon_list = taxon_list.annotate(
                    total_records=Count('biologicalcollectionrecord')
                ).order_by(order)
            elif 'family' in order or 'species' in order or 'genus' in order:
                rank_name = order.split('-')[-1]

                taxon_list = taxon_list.annotate(
                    **{rank_name: F(f'hierarchical_data__{rank_name}_name')}
                ).annotate(
                        order_priority=Case(
                            When(**{f"{rank_name}__isnull": False}, then=Value(0)),
                            When(**{f"{rank_name}__exact": ''}, then=Value(1)),
                            default=Value(1),
                            output_field=CharField(),
                        )
                    ).order_by('order_priority', order, 'id')
            elif 'origin' not in order:
                taxon_list = taxon_list.order_by(order)
            else:
                reverse = '-' in order
                taxon_list = sorted(
                    taxon_list,
                    key=lambda x: x.origin.order if x.origin else 9999,
                    reverse=reverse,
                )
        return taxon_list

    @property
    def paginator(self):
        if not hasattr(self, '_paginator'):
            if self.pagination_class is None:
                self._paginator = None
            else:
                self._paginator = self.pagination_class()
        else:
            pass
        return self._paginator

    def paginate_queryset(self, queryset):
        if self.paginator is None:
            return None
        return self.paginator.paginate_queryset(
            queryset,
            self.request,
            view=self)

    def get_paginated_response(self, data):
        assert self.paginator is not None
        return self.paginator.get_paginated_response(data)

    @swagger_auto_schema(
        operation_summary='List taxa',
        operation_description=(
            'Returns a paginated list of taxa (Taxonomy records) with rich '
            'filtering support.\n\n'
            '**Authentication**\n'
            'Public access is allowed when the site setting '
            '`allow_public_taxa_view` is enabled. Public users always see '
            'only validated taxa regardless of the `validated` parameter.\n\n'
            '**Pagination**\n'
            'Results are page-number paginated. Use `page` and `page_size` '
            'to navigate. The default page size is 20.'
        ),
        manual_parameters=[
            openapi.Parameter(
                'taxonGroup', openapi.IN_QUERY,
                description='Filter by taxon group ID (integer). Includes all descendant groups.',
                type=openapi.TYPE_INTEGER,
            ),
            openapi.Parameter(
                'id', openapi.IN_QUERY,
                description='Return a single taxon by its exact ID. All other filters are ignored.',
                type=openapi.TYPE_INTEGER,
            ),
            openapi.Parameter(
                'taxon', openapi.IN_QUERY,
                description='Case-insensitive substring search across canonical name, scientific name, and accepted taxonomy name.',
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                'rank', openapi.IN_QUERY,
                description='Filter by a single taxonomic rank (e.g. `SPECIES`, `GENUS`, `FAMILY`).',
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                'ranks', openapi.IN_QUERY,
                description='Filter by multiple taxonomic ranks as a comma-separated list (e.g. `SPECIES,GENUS`).',
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                'parent', openapi.IN_QUERY,
                description='Comma-separated list of parent taxon IDs. Returns all descendants of the given parents.',
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                'origins', openapi.IN_QUERY,
                description='Comma-separated origin keys to filter by (e.g. `alien,native`).',
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                'cons_status', openapi.IN_QUERY,
                description='Comma-separated IUCN Red List category codes (e.g. `EN,VU,CR`).',
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                'endemism', openapi.IN_QUERY,
                description='Comma-separated endemism names to filter by.',
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                'taxonomic_status', openapi.IN_QUERY,
                description='Comma-separated taxonomic status values (case-insensitive, e.g. `accepted,synonym`).',
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                'validated', openapi.IN_QUERY,
                description=(
                    'Filter by validation status. `True` (default) returns only validated taxa. '
                    '`False` returns unvalidated taxa; requires expert or superuser permissions '
                    'for the requested taxon group.'
                ),
                type=openapi.TYPE_STRING,
                enum=['True', 'False'],
                default='True',
            ),
            openapi.Parameter(
                'is_gbif', openapi.IN_QUERY,
                description='`True` — only taxa with a GBIF key; `False` — only taxa without one.',
                type=openapi.TYPE_STRING,
                enum=['True', 'False'],
            ),
            openapi.Parameter(
                'is_iucn', openapi.IN_QUERY,
                description='`True` — only taxa with an IUCN Red List ID; `False` — only taxa without one.',
                type=openapi.TYPE_STRING,
                enum=['True', 'False'],
            ),
            openapi.Parameter(
                'author', openapi.IN_QUERY,
                description=(
                    'Filter by author name(s). Use quoted strings for multi-word authors, '
                    'e.g. `"Smith, J." "Jones"`.'
                ),
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                'family', openapi.IN_QUERY,
                description='Filter by family name (exact, case-insensitive).',
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                'genus', openapi.IN_QUERY,
                description='Filter by genus name (exact, case-insensitive).',
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                'species', openapi.IN_QUERY,
                description='Filter by species epithet (exact, case-insensitive).',
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                'tags', openapi.IN_QUERY,
                description='Comma-separated tag names to filter by.',
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                'tagFT', openapi.IN_QUERY,
                description='Tag filter type: `OR` (default) matches any tag; `AND` requires all tags.',
                type=openapi.TYPE_STRING,
                enum=['OR', 'AND'],
                default='OR',
            ),
            openapi.Parameter(
                'bD', openapi.IN_QUERY,
                description='Comma-separated biographic distribution tag names to filter by.',
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                'bDFT', openapi.IN_QUERY,
                description='Biographic distribution filter type: `OR` (default) or `AND`.',
                type=openapi.TYPE_STRING,
                enum=['OR', 'AND'],
                default='OR',
            ),
            openapi.Parameter(
                'o', openapi.IN_QUERY,
                description=(
                    'Ordering field. Prefix with `-` for descending order. '
                    'Supported values include `canonical_name`, `-canonical_name`, '
                    '`total_records`, `-total_records`, `endemism_name`, '
                    '`family`, `genus`, `species`, `accepted_taxonomy_name`, and `origin`.'
                ),
                type=openapi.TYPE_STRING,
            ),
            openapi.Parameter(
                'page', openapi.IN_QUERY,
                description='Page number (1-based).',
                type=openapi.TYPE_INTEGER,
                default=1,
            ),
            openapi.Parameter(
                'page_size', openapi.IN_QUERY,
                description='Number of results per page (default: 20).',
                type=openapi.TYPE_INTEGER,
                default=20,
            ),
        ],
        responses={
            200: openapi.Response(
                description='Paginated list of taxa.',
            ),
            403: openapi.Response(description='Authentication required or insufficient permissions.'),
        },
        tags=['Taxa'],
    )
    def get(self, request, *args):
        from preferences import preferences
        if not request.user.is_authenticated:
            if not preferences.SiteSetting.allow_public_taxa_view:
                return Response(
                    {'detail': 'Authentication required.'},
                    status=HTTP_403_FORBIDDEN
                )
            # Public users may only see validated taxa — override any
            # 'validated' param they may have passed in the URL.
            mutable = request.GET.copy()
            mutable['validated'] = 'True'
            request._request.GET = mutable

        taxon_list = self.get_taxa_by_parameters(request)

        self.pagination_class.page_size = request.GET.get('page_size', 20)
        page = self.paginate_queryset(taxon_list)
        validated = request.GET.get('validated', 'True').lower() == 'true'
        if page is not None:
            taxon_group_id = request.GET.get('taxonGroup', None)
            serializer = self.get_paginated_response(
                TaxonSerializer(page, many=True, context={
                    'taxon_group_id': taxon_group_id,
                    'user': request.user.id,
                    'validated': validated
                }).data)
            serializer.data['is_expert'] = is_expert(
                self.request.user,
                TaxonGroup.objects.get(id=taxon_group_id)
            ) if taxon_group_id else False
        else:
            serializer = TaxonSerializer(
                taxon_list,
                many=True,
                context={
                    'user': request.user.id
                }
            )
        return Response(serializer.data)


class TaxaGroupSummary(APIView):
    """
    Returns a count of matching taxa broken down by taxon group.

    Accepts the same filter parameters as `/api/taxa-list/` (except
    `taxonGroup`, `page`, and `page_size` which are not applicable here).
    Useful for showing how many results exist across groups without
    fetching the full paginated list.
    """

    def get(self, request, *args, **kwargs):
        from preferences import preferences
        if not request.user.is_authenticated:
            if not preferences.SiteSetting.allow_public_taxa_view:
                return Response(
                    {'detail': 'Authentication required.'},
                    status=HTTP_403_FORBIDDEN
                )
            mutable = request.GET.copy()
            mutable['validated'] = 'True'
            request._request.GET = mutable

        taxon_list = TaxaList.get_taxa_by_parameters(request)
        return Response(list(
            TaxonGroupTaxonomy.objects.filter(
                taxonomy__in=taxon_list
            ).values('taxongroup', 'taxongroup__name').annotate(
                total=Count('taxongroup')
            )
        ))


class TaxonTagAutocompleteAPIView(APIView):
    def get(self, request, format=None):
        """
        Modes:
        - Autocomplete: ?q=te
        - Bootstrap by IDs: ?ids=1,4,9
        """
        query = request.query_params.get('q', '')
        ids_param = request.query_params.get('ids', '')
        biographic = ast.literal_eval(
            request.query_params.get('biographic', 'False')
        )

        if biographic:
            base_qs = TaxonTag.objects.all()
        else:
            base_qs = Tag.objects.filter(taxonomy__isnull=False)

        if ids_param:
            try:
                ids_list = [
                    int(x.strip())
                    for x in ids_param.split(',')
                    if x.strip()
                ]
            except ValueError:
                ids_list = []
            taxonomy_tags = base_qs.filter(id__in=ids_list).distinct()
        else:
            taxonomy_tags = (
                base_qs.filter(name__icontains=query)
                .distinct()[:10]
            )

        serializer = TagSerializer(taxonomy_tags, many=True, context={
            'is_biographic': biographic
        })
        return Response(serializer.data)


class AddTagAPIView(UpdateAPIView):
    queryset = Taxonomy.objects.all()
    serializer_class = TaxonomyTagUpdateSerializer
    permission_classes = [IsAuthenticated]

    def get_object(self):
        taxonomy_id = self.kwargs.get('pk')
        return Taxonomy.objects.get(pk=taxonomy_id)


class IUCNStatusFetchView(APIView):
    permission_classes = [IsAdminUser]

    def get(self, request, *args, **kwargs):
        taxonomy_id = self.kwargs.get('pk')

        if not taxonomy_id:
            return Response(
                {"detail": "Missing taxon_id"},
                status=status.HTTP_400_BAD_REQUEST)

        taxon = Taxonomy.objects.get(id=taxonomy_id)
        iucn_status, sis_id, iucn_url = get_iucn_status(taxon)

        if iucn_status:
            return Response({
                "status_category": iucn_status.category,
                "sis_id": sis_id,
                "iucn_url": iucn_url
            })
        return Response(
            {"detail": "Not found"},
            status=status.HTTP_404_NOT_FOUND)


class TaxonTreeJsonView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, taxon_id, format=None):
        taxon = None
        try:
            taxon = Taxonomy.objects.get(id=taxon_id)
        except Taxonomy.DoesNotExist:
            try:
                taxon = TaxonomyUpdateProposal.objects.get(id=taxon_id)
            except TaxonomyUpdateProposal.DoesNotExist:
                raise Http404

        nodes = []
        current = taxon
        seen_ids = set()
        max_depth = 64
        depth = 0

        while current and depth < max_depth:
            cur_id = getattr(current, "id", None)
            if cur_id is not None:
                if cur_id in seen_ids:
                    logger.warning(
                        "TaxonTreeJsonView: detected cycle starting at id=%s (depth=%s)",
                        getattr(taxon, "id", None), depth
                    )
                    break
                seen_ids.add(cur_id)

            parent = getattr(current, "parent", None)
            nodes.append({
                'id': cur_id,
                'parent': getattr(parent, "id", None) if parent else '#',
                'text': f'{getattr(current, "canonical_name", "")} ({getattr(current, "rank", "")})',
                'state': {'opened': True},
            })

            current = parent
            depth += 1

        if depth >= max_depth:
            logger.warning(
                "TaxonTreeJsonView: depth limit hit (start_id=%s, limit=%s)",
                getattr(taxon, "id", None), max_depth
            )

        return JsonResponse(nodes, safe=False)


class HarvestIUCNStatus(APIView):
    """
    Enqueue a Celery task that pulls/refreshes IUCN Red-List info
    for all taxa still missing a status (or for an optional list of IDs).
    """
    permission_classes = (IsAdminUser,)

    def post(self, request, *args, **kwargs):
        if not request.user.is_superuser:
            return Response({"error": "Permission denied."},
                            status=HTTP_403_FORBIDDEN)

        taxa_ids = request.data.get("taxa_ids")
        fetch_iucn_status.delay(taxa_ids or None)

        return Response(
            {"message": "Harvesting IUCN status in the background."},
            status=HTTP_200_OK
        )


class ApproveTaxonGroupProposalsView(APIView):
    """
    POST: Trigger background approval of all proposals under a TaxonGroup.
    Body:
      {
        "taxon_group_id": 123,
        "include_children": true,
        "statuses": ["pending"]
      }
    """
    permission_classes = [IsAuthenticated]

    def post(self, request, *args, **kwargs):
        payload = request.data or {}

        try:
            taxon_group_id = int(payload.get("taxon_group_id"))
        except (TypeError, ValueError):
            return Response(
                {"detail": "Missing or invalid 'taxon_group_id'."},
                status=status.HTTP_400_BAD_REQUEST
            )

        include_children = payload.get("include_children", True)

        group = get_object_or_404(TaxonGroup, pk=taxon_group_id)
        user = request.user
        if not is_expert(user, group):
            return Response(
                {"detail": "You do not have permission to approve proposals for this group."},
                status=status.HTTP_403_FORBIDDEN
            )

        task = approve_unvalidated_taxa_by_group.delay(
            taxon_group_id=group.id,
            initiated_by_user_id=user.id,
            include_children=bool(include_children),
        )

        logger.info(
            "User %s queued batch-approve for TaxonGroup %s "
            "(task_id=%s, include_children=%s)",
            user.id, group.id, task.id, include_children
        )

        return Response(
            {
                "message": "Batch approval started.",
                "task_id": task.id,
                "taxon_group_id": group.id,
                "include_children": include_children,
            },
            status=status.HTTP_202_ACCEPTED
        )


class ClearTaxaNotAssociatedInTaxonGroup(APIView):
    permission_classes = (IsSuperUser,)

    def post(self, request, *args, **kwargs):
        if not request.user.is_superuser:
            return Response({"error": "Permission denied."}, status=HTTP_403_FORBIDDEN)

        dry_run = bool(request.data.get("dry_run", False))
        clear_taxa_not_associated_in_taxon_group.delay(dry_run=dry_run, keep_referenced_by_occurrences=True)

        return Response(
            {
                "message": (
                    "Starting background cleanup of Taxonomy rows that are not associated with any taxon group. "
                    + (" (dry-run)" if dry_run else "")
                )
            },
            status=HTTP_200_OK,
        )
