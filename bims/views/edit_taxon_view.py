from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.db import transaction
from django.shortcuts import get_object_or_404, redirect
from django.template.response import TemplateResponse
from django.urls import reverse
from django.views.generic import UpdateView

from bims.api_views.taxon_update import is_expert, create_taxon_proposal, update_taxon_proposal
from bims.models import TaxonGroup, TaxonomyUpdateProposal, IUCNStatus, Endemism, TaxonGroupTaxonomy, SpeciesGroup
from bims.models.taxonomy import Taxonomy
from bims.models.taxon_url import TaxonURL


class EditTaxonView(LoginRequiredMixin, UserPassesTestMixin, UpdateView):
    template_name = 'edit_taxon.html'
    model = Taxonomy
    pk_url_kwarg = 'id'
    fields = [
        'tags',
        'canonical_name',
        'rank',
        'author',
        'iucn_status',
        'parent',
        'taxonomic_status',
        'accepted_taxonomy',
        'col_id',
        'fada_id'
    ]
    success_url = '/taxa_management/'

    # Populated by test_func() when a read-only group blocks editing.
    _readonly_blocking_groups = None

    def get_object(self, queryset=None):
        taxon = get_object_or_404(
            Taxonomy,
            pk=self.kwargs['id'],
        )
        proposal = TaxonomyUpdateProposal.objects.filter(
            original_taxonomy=taxon,
            taxon_group=self.kwargs['taxon_group_id'],
            status='pending'
        ).first()
        return proposal or taxon

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['rank_choices'] = self.model._meta.get_field('rank').choices
        context['addendum_choices'] = (
            self.model._meta.get_field('addendum').choices
        )
        context['taxon_group_id'] = self.kwargs.get('taxon_group_id', '')
        context['iucn_status_choices'] = IUCNStatus.objects.filter(
            national=False
        ).distinct(
            'order', 'category', 'national'
        )
        context['next'] = self.request.GET.get('next', '')
        # When a pending proposal exists, show its proposed URLs so the user
        # edits the proposed state rather than the live taxonomy state.
        proposed_urls = None
        if isinstance(self.object, TaxonomyUpdateProposal):
            ad = self.object.additional_data or {}
            if 'proposed_urls' in ad:
                proposed_urls = ad['proposed_urls']
        context['taxon_urls'] = (
            proposed_urls
            if proposed_urls is not None
            else TaxonURL.objects.filter(taxonomy_id=self.kwargs['id'])
        )
        taxon_obj = self.object
        status = (taxon_obj.taxonomic_status or '').upper()
        is_synonym = 'SYNONYM' in status
        if is_synonym and getattr(taxon_obj, 'accepted_taxonomy', None):
            context['hierarchy_taxon'] = taxon_obj.accepted_taxonomy
        else:
            context['hierarchy_taxon'] = taxon_obj
        context['taxon_ranks'] = [
            {'rank': 'Kingdom', 'field': 'kingdom_name'},
            {'rank': 'Phylum', 'field': 'phylum_name'},
            {'rank': 'Class', 'field': 'class_name'},
            {'rank': 'Order', 'field': 'order_name'},
            {'rank': 'Family', 'field': 'family_name'},
            {'rank': 'SubFamily', 'field': 'sub_family_name'},
            {'rank': 'Tribe', 'field': 'tribe_name'},
            {'rank': 'SubTribe', 'field': 'sub_tribe_name'},
            {'rank': 'Genus', 'field': 'genus_name'},
            {'rank': 'Species', 'field': 'specific_epithet'},
            {'rank': 'SubSpecies', 'field': 'sub_species_name'}
        ]
        return context

    def test_func(self):
        # Block editing if the taxonomy belongs to ANY read-only group,
        # regardless of which group the user is editing through.
        readonly_groups = list(
            TaxonGroup.objects.filter(
                is_readonly=True,
                taxongrouptaxonomy__taxonomy_id=self.kwargs['id'],
            ).distinct()
        )
        if readonly_groups:
            self._readonly_blocking_groups = readonly_groups
            return False

        taxon_group = get_object_or_404(
            TaxonGroup,
            pk=self.kwargs['taxon_group_id']
        )
        if self.request.user.is_superuser:
            return True
        return taxon_group.experts.filter(
            id=self.request.user.id
        ).exists()

    def handle_no_permission(self):
        # For read-only blocks, render the edit template with an explanatory
        # banner rather than a bare 403 so the user understands why.
        if self._readonly_blocking_groups:
            taxon = get_object_or_404(Taxonomy, pk=self.kwargs['id'])
            return TemplateResponse(
                self.request,
                self.template_name,
                {
                    'object': taxon,
                    'taxon_group_id': self.kwargs.get('taxon_group_id', ''),
                    'readonly_blocked_by': self._readonly_blocking_groups,
                },
                status=403,
            )
        return super().handle_no_permission()

    def is_taxon_edited(self, taxon):
        return TaxonomyUpdateProposal.objects.filter(
            original_taxonomy=taxon,
            status='pending'
        ).exists()

    def form_valid(self, form):
        taxon = form.instance
        if isinstance(taxon, TaxonomyUpdateProposal):
            taxon = taxon.original_taxonomy
        taxon_group_id = self.kwargs.get('taxon_group_id')
        taxon_group = get_object_or_404(TaxonGroup, pk=taxon_group_id)
        taxon_edited = self.is_taxon_edited(taxon)

        if taxon_edited and not is_expert(self.request.user, taxon_group):
            messages.error(self.request, 'Taxon is still being edited')
            return redirect(self.get_success_url())

        data = form.cleaned_data
        current_rank = data.get('rank') or taxon.rank
        taxon_name = self.request.POST.get('taxon_name', '')

        species_group_raw = (self.request.POST.get('species-group') or '').strip()

        if not species_group_raw or species_group_raw.lower() == 'null' or species_group_raw.lower() == 'none':
            data['species_group'] = None
        elif species_group_raw.startswith('NEW:'):
            data['species_group'] = SpeciesGroup.objects.create(
                name=species_group_raw.split(':', 1)[-1].strip()
            )
        else:
            try:
                data['species_group'] = SpeciesGroup.objects.get(id=int(species_group_raw))
            except (ValueError, SpeciesGroup.DoesNotExist):
                data['species_group'] = None

        parent = data.get('parent')
        status = (data.get('taxonomic_status') or taxon.taxonomic_status or '').upper()
        is_synonym = 'SYNONYM' in status

        if not parent and not is_synonym:
            # Accepted/doubtful taxa must keep their parent. An empty parent
            # field (e.g. cleared by toggling the status through "Synonym")
            # would otherwise detach the taxon from its genus on approval.
            # form.instance already has the empty value applied, so read the
            # stored parent (pending proposal first, then the taxon) from the DB.
            pending = TaxonomyUpdateProposal.objects.filter(
                original_taxonomy_id=taxon.pk,
                status='pending',
                taxon_group_id=taxon_group_id,
            ).select_related('parent').first()
            parent = pending.parent if pending else None
            if not parent:
                stored_parent_id = Taxonomy.objects.filter(
                    pk=taxon.pk).values_list('parent_id', flat=True).first()
                if stored_parent_id:
                    parent = Taxonomy.objects.filter(pk=stored_parent_id).first()
            data['parent'] = parent

        if parent:
            if 'subspecies' in current_rank.lower():
                if parent.rank and parent.rank.lower() == 'species':
                    if len(taxon_name.split(' ')) > 1 and not taxon_name.startswith(parent.specific_epithet or ''):
                        taxon_name = f'{parent.specific_epithet or ''} {taxon_name}'.strip()
            if 'species' in current_rank.lower():
                genus_name = parent.genus_name or ''
                if genus_name and not taxon_name.startswith(genus_name):
                    taxon_name = f'{genus_name} {taxon_name}'

        data['common_name'] = self.request.POST.get('common_name', '')
        data['tags'] = self.request.POST.getlist('tags')
        data['biographic_distributions'] = (
            self.request.POST.getlist('biographic_distributions')
        )

        data['canonical_name'] = taxon_name
        data['scientific_name'] = (
            f'{taxon_name} {self.request.POST.get('author', '')}'
        )
        data['Taxonomic Comments'] = (
            self.request.POST.get('additional_data__Taxonomic_Comments', '')).strip()
        data['Taxonomic References'] = (
            self.request.POST.get('additional_data__Taxonomic_References', '')).strip()
        data['Biogeographic Comments'] = (
            self.request.POST.get('additional_data__Biogeographic_Comments', '')).strip()
        data['Biogeographic References'] = (
            self.request.POST.get('additional_data__Biogeographic_References', '')).strip()
        data['Environmental Comments'] = (
            self.request.POST.get('additional_data__Environmental_Comments', '')).strip()
        data['Environmental References'] = (
            self.request.POST.get('additional_data__Environmental_References', '')).strip()
        data['Conservation Comments'] = (
            self.request.POST.get('additional_data__Conservation_Comments', '')).strip()
        data['Conservation References'] = (
            self.request.POST.get('additional_data__Conservation_References', '')).strip()
        data['fada_id'] = self.request.POST.get('fada_id', '').strip() or None
        data['col_id'] = self.request.POST.get('col_id', '').strip() or None
        data['addendum'] = self.request.POST.get('addendum', '').strip()

        subgenus_id = self.request.POST.get('subgenus', '').strip()
        if is_synonym and subgenus_id:
            try:
                data['subgenus'] = Taxonomy.objects.get(pk=int(subgenus_id))
            except (ValueError, Taxonomy.DoesNotExist):
                data['subgenus'] = None
        elif is_synonym:
            data['subgenus'] = None

        new_proposal = False

        with transaction.atomic():
            iucn_status = None
            endemism = None
            try:
                iucn_status = IUCNStatus.objects.get(
                    category=data.get('iucn_status'),
                    national=False
                )
            except IUCNStatus.DoesNotExist:
                if taxon.iucn_status:
                    iucn_status = taxon.iucn_status
            except IUCNStatus.MultipleObjectsReturned:
                iucn_status = IUCNStatus.objects.get(
                    category=data.get('iucn_status'),
                    national=False
                ).first()

            try:
                endemism = Endemism.objects.get(name=data.get('endemism'))
            except Endemism.DoesNotExist:
                if taxon.endemism:
                    endemism = taxon.endemism

            proposal = TaxonomyUpdateProposal.objects.filter(
                original_taxonomy=taxon,
                status='pending',
                taxon_group=taxon_group
            ).first()

            if not proposal:
                proposal = create_taxon_proposal(
                    taxon=taxon,
                    data=data,
                    taxon_group=taxon_group,
                    iucn_status=iucn_status,
                    endemism=endemism,
                    creator=self.request.user
                )
                TaxonGroupTaxonomy.objects.filter(
                    taxonomy=taxon,
                    taxongroup=taxon_group
                ).update(
                    is_validated=False
                )

                new_proposal = True

                taxon.send_updated_taxon_email(taxon_group.id, self.request.user)

                messages.success(
                    self.request,
                    'Taxonomy update proposal created successfully')
            else:
                update_taxon_proposal(
                    proposal=proposal,
                    data=data,
                    iucn_status=iucn_status,
                    endemism=endemism,
                    user=self.request.user
                )
                messages.success(
                    self.request,
                    'Taxonomy updated successfully')

            # Store proposed URL changes in the proposal so they are only
            # applied once the proposal is approved (and reverted on rejection).
            uris = self.request.POST.getlist('url_uri')
            labels = self.request.POST.getlist('url_label')
            ids = self.request.POST.getlist('url_id')
            proposed_urls = []
            for i, (uri, label) in enumerate(zip(uris, labels)):
                uri = uri.strip()
                label = label.strip()
                if not uri or not label:
                    continue
                raw_id = ids[i] if i < len(ids) else ''
                proposed_urls.append({
                    'id': int(raw_id) if raw_id else None,
                    'uri': uri,
                    'label': label,
                })
            ad = dict(proposal.additional_data or {})
            ad['proposed_urls'] = proposed_urls
            proposal.additional_data = ad
            proposal.save(update_fields=['additional_data'])

        # The proposal is automatically approved if the user is a superuser
        # if proposal and self.request.user.is_superuser and new_proposal:
        #     proposal.approve(self.request.user)

        return redirect(self.get_success_url())

    def get_success_url(self):
        next_path = self.request.POST.get('next')
        if next_path:
            return next_path
        return reverse('taxa-management')
