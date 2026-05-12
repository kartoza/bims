# coding=utf-8
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.http import JsonResponse
from django.views import View

from bims.utils.taxonworks import find_taxon_name_by_name


class TaxonWorksNameSearchView(UserPassesTestMixin, LoginRequiredMixin, View):
    """
    GET /api/taxonworks-name-search/?base_url=...&project_token=...&name=...

    Proxies a name search to the TaxonWorks API and returns a JSON list of
    matching taxon name records trimmed to the fields needed by the UI.
    """

    def test_func(self):
        return self.request.user.has_perm('bims.can_harvest_species')

    def get(self, request, *args, **kwargs):
        base_url = (request.GET.get('base_url') or '').strip()
        project_token = (request.GET.get('project_token') or '').strip()
        name = (request.GET.get('name') or '').strip()

        if not base_url or not project_token or not name:
            return JsonResponse(
                {'error': 'base_url, project_token and name are required.'},
                status=400,
            )

        records = find_taxon_name_by_name(base_url, project_token, name)

        results = [
            {
                'id': r.get('id'),
                'name': r.get('cached') or r.get('name', ''),
                'rank': (r.get('rank') or '').capitalize(),
                'valid': r.get('cached_is_valid', True),
            }
            for r in records
            if r.get('id')
        ]

        return JsonResponse({'results': results})
