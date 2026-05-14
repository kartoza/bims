# coding=utf-8
from django.contrib.sites.models import Site
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.views import APIView
from rest_framework.response import Response
from sorl.thumbnail import get_thumbnail
from bims.models.taxon_group import TaxonGroup
from bims.enums.taxonomic_group_category import TaxonomicGroupCategory


class ModuleList(APIView):
    """Return list of species module"""

    @swagger_auto_schema(
        operation_summary='List taxon groups (species modules)',
        operation_description=(
            'Returns all top-level taxon groups that are categorised as '
            'species modules for the current site. Each item includes the '
            'group ID, name, and thumbnail logo path.'
        ),
        security=[],
        responses={
            200: openapi.Response(
                description='List of taxon groups.',
                schema=openapi.Schema(
                    type=openapi.TYPE_ARRAY,
                    items=openapi.Schema(
                        type=openapi.TYPE_OBJECT,
                        properties={
                            'id': openapi.Schema(type=openapi.TYPE_INTEGER, description='Taxon group ID'),
                            'name': openapi.Schema(type=openapi.TYPE_STRING, description='Taxon group name'),
                            'logo': openapi.Schema(type=openapi.TYPE_STRING, description='Thumbnail logo path'),
                        },
                    ),
                ),
            ),
        },
        tags=['Taxon Groups'],
    )
    def get(self, request, *args):
        taxon_group_list = []
        site = Site.objects.get_current()
        taxon_groups = TaxonGroup.objects.filter(
            category=TaxonomicGroupCategory.SPECIES_MODULE.name,
            site=site
        )
        for _module in taxon_groups:
            try:
                logo = get_thumbnail(
                    _module.logo, 'x80', crop='center'
                ).name
            except ValueError:
                logo = ''
            taxon_group_list.append({
                'name': _module.name,
                'id': _module.id,
                'logo': logo
            })
        return Response(taxon_group_list)
