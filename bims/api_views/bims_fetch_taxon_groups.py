# coding=utf-8
"""AJAX endpoint to fetch taxon groups from a remote BIMS instance."""
from django.contrib.auth.mixins import UserPassesTestMixin
from rest_framework.response import Response
from rest_framework.views import APIView

from bims.utils.bims_instance import get_taxon_groups, normalize_bims_base_url


class BimsFetchTaxonGroupsView(UserPassesTestMixin, APIView):
    """
    Return the list of taxon groups available on a remote BIMS instance.
    Used by the BIMS harvester frontend to populate the remote group selector.

    GET params:
      - base_url: the root URL of the remote BIMS instance
    """

    def test_func(self):
        return self.request.user.has_perm('bims.can_harvest_species')

    def get(self, request, *args, **kwargs):
        base_url = (request.GET.get('base_url') or '').strip()
        if not base_url:
            return Response({'error': 'base_url is required'}, status=400)

        base_url = normalize_bims_base_url(base_url)
        groups = get_taxon_groups(base_url)
        if groups is None:
            return Response(
                {'error': 'Failed to fetch taxon groups from remote BIMS instance'},
                status=502,
            )
        return Response({'results': groups})
