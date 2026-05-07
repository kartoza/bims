# coding=utf-8
"""Checklist management view"""
from braces.views import LoginRequiredMixin
from django.views.generic import TemplateView

from bims.enums import TaxonomicGroupCategory
from bims.models import TaxonGroup
from bims.models.licence import Licence


class ChecklistView(LoginRequiredMixin, TemplateView):
    template_name = 'checklist/checklist_page.html'

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx['groups'] = list(
            TaxonGroup.objects.filter(
                category=TaxonomicGroupCategory.SPECIES_MODULE.name
            ).order_by('display_order').values('id', 'name')
        )
        ctx['licences'] = list(Licence.objects.values('id', 'identifier', 'name'))
        publish_group_ids = list(
            TaxonGroup.objects.filter(
                category=TaxonomicGroupCategory.SPECIES_MODULE.name,
                experts=self.request.user
            ).values_list('id', flat=True)
        )
        ctx['publish_group_ids'] = publish_group_ids
        ctx['can_publish'] = self.request.user.is_superuser or bool(publish_group_ids)
        ctx['can_create_version'] = self.request.user.is_superuser
        return ctx
