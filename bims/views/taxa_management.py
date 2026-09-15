# coding=utf-8
"""Taxa management view
"""
import json
from urllib.parse import urlencode

from django.contrib.auth.views import redirect_to_login
from django.http import Http404, HttpResponseRedirect
from django.urls import reverse
from django.views.generic import TemplateView
from preferences import preferences

from bims.cache import get_cache, set_cache
from bims.models.taxon_group import TaxonGroup, TAXON_GROUP_CACHE
from bims.enums.taxonomic_rank import TaxonomicRank
from bims.models.taxonomy import Taxonomy
from bims.models.endemism import Endemism
from bims.models.iucn_status import IUCNStatus
from bims.models.taxon_origin import TaxonOrigin
from bims.models.meta_group import MetaGroup
from bims.serializers.taxon_serializer import TaxonGroupSerializer


class TaxaManagementView(TemplateView):
    template_name = 'taxa_management.html'

    def remove_selected_param_from_url(self, request):
        query_params = request.GET.copy()
        query_params.pop('selected', None)
        base_url = reverse('taxa-management')
        new_url = f"{base_url}?{urlencode(query_params)}"
        return HttpResponseRedirect(new_url)

    def dispatch(self, request, *args, **kwargs):
        if not request.user.is_authenticated:
            if not preferences.SiteSetting.allow_public_taxa_view:
                return redirect_to_login(request.get_full_path())

        fada_id_param = request.GET.get('fada_id', '').strip()
        if fada_id_param:
            raw_id = (
                fada_id_param[5:]
                if fada_id_param.startswith('fada:')
                else fada_id_param
            )
            try:
                taxon = Taxonomy.objects.get(fada_id=raw_id)
                taxon_group = TaxonGroup.objects.filter(
                    taxonomies=taxon,
                    category='SPECIES_MODULE',
                    parent__isnull=True,
                ).first()
                params = {}
                if taxon_group:
                    params['selected'] = taxon_group.id
                params['taxon'] = taxon.canonical_name or taxon.scientific_name or ''
                return HttpResponseRedirect(
                    f'{reverse("taxa-management")}?{urlencode(params)}'
                )
            except Taxonomy.DoesNotExist:
                pass

        selected = request.GET.get('selected')
        if selected:
            try:
                TaxonGroup.objects.get(id=selected)
            except TaxonGroup.DoesNotExist:
                return self.remove_selected_param_from_url(request)
        return super().dispatch(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        selected = self.request.GET.get('selected')
        taxon_group_cache = get_cache(TAXON_GROUP_CACHE)
        # if taxon_group_cache:
        #     context['taxa_groups'] = taxon_group_cache
        # else:
        taxa_groups_query = TaxonGroup.objects.filter(
            category='SPECIES_MODULE',
            parent__isnull=True
        ).order_by('display_order')
        context['taxa_groups'] = TaxonGroupSerializer(
            taxa_groups_query, many=True).data
        # set_cache(TAXON_GROUP_CACHE, context['taxa_groups'])
        context['selected_taxon_group'] = None
        if selected:
            try:
                context['selected_taxon_group'] = TaxonGroup.objects.get(
                    id=selected
                )
            except TaxonGroup.DoesNotExist:
                pass

        if not context['selected_taxon_group']:
            context['selected_taxon_group'] = TaxonGroup.objects.filter(
                category='SPECIES_MODULE',
            ).first()

        context['taxa_groups_json'] = json.dumps(context['taxa_groups'])

        latest_versions = {}
        try:
            from bims.models.checklist_version import ChecklistVersion
            for tg in taxa_groups_query:
                lv = (
                    ChecklistVersion.objects
                    .filter(taxon_group=tg, status=ChecklistVersion.STATUS_PUBLISHED)
                    .order_by('-published_at')
                    .first()
                )
                if lv:
                    has_changes = (
                        tg.taxonomies.filter(created_at__gt=lv.published_at).exists()
                    )
                    latest_versions[str(tg.id)] = {
                        'version': lv.version,
                        'published_at': lv.published_at.strftime('%d %B %Y') if lv.published_at else '',
                        'doi': lv.doi or '',
                        'id': str(lv.id),
                        'has_changes_since': has_changes,
                    }
        except Exception:
            pass
        context['checklist_versions_json'] = json.dumps(latest_versions)
        context['meta_groups'] = MetaGroup.objects.all().order_by('display_order', 'name').values('id', 'name')
        context['taxon_rank'] = [
            rank.name for rank in TaxonomicRank
            if rank in (TaxonomicRank.GENUS, TaxonomicRank.SPECIES)
        ]
        context['all_ranks'] = [rank.name for rank in TaxonomicRank]
        context['all_origins'] = [
            {'value': o.origin_key, 'label': o.category}
            for o in TaxonOrigin.objects.order_by('order')]
        context['all_endemism'] = Endemism.objects.exclude(
            name='').values_list('name', flat=True).distinct()
        context['all_cons_status'] = [
            {'value': status[0], 'label': status[1]} for status in IUCNStatus.CATEGORY_CHOICES]
        context['all_taxonomic_status'] = [
            'SYNONYM', 'ACCEPTED', 'DOUBTFUL', 'UNACCEPTED',
        ]
        user = self.request.user
        is_authenticated = user.is_authenticated
        context['is_public_view'] = not is_authenticated
        selected_taxon_id = self.request.GET.get('selected')
        context['is_expert'] = (
            is_authenticated and (
                user.is_superuser or
                self.is_user_expert_for_taxon(selected_taxon_id)
            )
        )
        context['is_contributor'] = (
            is_authenticated and
            not user.is_superuser and
            not self.is_user_expert_for_taxon(selected_taxon_id) and
            self.is_user_contributor_for_taxon(selected_taxon_id)
        )
        return context

    def is_user_expert_for_taxon(self, selected_taxon_id):
        if not selected_taxon_id:
            return False
        try:
            selected_taxon_group = TaxonGroup.objects.get(
                id=selected_taxon_id)
            return selected_taxon_group.experts.filter(
                id=self.request.user.id).exists()
        except TaxonGroup.DoesNotExist:
            raise Http404("Taxon Group does not exist")

    def is_user_contributor_for_taxon(self, selected_taxon_id):
        if not selected_taxon_id:
            return False
        try:
            selected_taxon_group = TaxonGroup.objects.get(
                id=selected_taxon_id)
            return selected_taxon_group.contributors.filter(
                id=self.request.user.id).exists()
        except TaxonGroup.DoesNotExist:
            return False
