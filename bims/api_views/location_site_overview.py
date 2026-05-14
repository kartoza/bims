import hashlib
import json
import time
from collections import OrderedDict

from braces.views import SuperuserRequiredMixin
from django.contrib.sites.models import Site
from django.db import connection

from bims.models.search_process import SITES_SUMMARY, SEARCH_PROCESSING

from bims.models.water_temperature import WaterTemperature
from django.db.models import F, Value, Case, When, Count, Q, CharField
from rest_framework.views import APIView
from rest_framework.response import Response
from sorl.thumbnail import get_thumbnail
from django.http import Http404
from bims.api_views.search import CollectionSearch
from bims.models import (
    TaxonGroup,
    IUCNStatus, ChemicalRecord,
)
from climate.models import Climate
from bims.enums import TaxonomicGroupCategory
from bims.tasks import location_sites_overview
from bims.utils.api_view import BimsApiView
from bims.utils.search_process import get_or_create_search_process
from sass.models.site_visit_taxon import SiteVisitTaxon
from bims.models.location_site import LocationSite
from bims.serializers.location_site_detail_serializer import (
    LocationSiteDetailSerializer,
)


class LocationSiteOverviewData(object):
    BIODIVERSITY_DATA = 'biodiversity_data'
    MODULE = 'module'
    SASS_EXIST = 'sass_exist'
    GROUP_ICON = 'icon'
    GROUP_OCCURRENCES = 'occurrences'
    GROUP_SITES = 'sites'
    GROUP_ENDEMISM = 'endemism'
    GROUP_NUM_OF_TAXA = 'number_of_taxa'
    GROUP_ORIGIN = 'origin'
    GROUP_CONS_STATUS = 'cons_status'
    WATER_TEMPERATURE_EXIST = 'water_temperature_exist'
    WATER_TEMPERATURE_DATA = 'water_temperature_data'
    PHYSICO_CHEMICAL_EXIST = 'physico_chemical_exist'
    CLIMATE_EXIST = 'climate_exist'

    search_filters = None
    is_sass_exist = False
    is_climate_data_exist = False

    search_process = None

    def biodiversity_data(self):
        if not self.search_filters:
            return {}

        search = CollectionSearch(self.search_filters)
        collection_results = search.process_search()

        groups = TaxonGroup.objects.filter(
            category=TaxonomicGroupCategory.SPECIES_MODULE.name
        ).order_by('display_order')

        group_ids = [g.id for g in groups]
        if not group_ids:
            return {}

        annotated = collection_results.filter(
            module_group_id__in=group_ids
        ).annotate(
            endemism_name=Case(
                When(taxonomy__endemism__isnull=False,
                     then=F('taxonomy__endemism__name')),
                default=Value('Unknown'),
                output_field=CharField(),
            ),
            origin_name=Case(
                When(taxonomy__origin__isnull=True,
                     then=Value('Unknown')),
                default=F('taxonomy__origin__category'),
                output_field=CharField(),
            ),
            iucn_category=Case(
                When(taxonomy__iucn_status__isnull=False,
                     then=F('taxonomy__iucn_status__category')),
                default=Value('Not evaluated'),
                output_field=CharField(),
            ),
        )

        totals = {
            row['module_group_id']: row
            for row in annotated.values('module_group_id').annotate(
                total=Count('id'),
                unique_sites=Count('site_id', distinct=True),
                unique_taxa=Count('taxonomy_id', distinct=True),
            )
        }

        endemism_rows = annotated.values(
            'module_group_id', 'endemism_name'
        ).annotate(count=Count('id')).order_by('module_group_id', 'endemism_name')
        endemism_by_group = {}
        for row in endemism_rows:
            endemism_by_group.setdefault(row['module_group_id'], []).append(
                {'endemism_name': row['endemism_name'], 'count': row['count']}
            )

        origin_rows = annotated.values(
            'module_group_id', 'origin_name'
        ).annotate(count=Count('id')).order_by('module_group_id', 'origin_name')
        origin_by_group = {}
        for row in origin_rows:
            origin_by_group.setdefault(row['module_group_id'], []).append(
                {'origin_name': row['origin_name'],
                 'name': row['origin_name'],
                 'count': row['count']}
            )

        iucn_category_choices = dict(IUCNStatus.CATEGORY_CHOICES)
        SPECIES_RANKS = ['SPECIES', 'SUBSPECIES', 'VARIETY']
        cons_rows = annotated.filter(
            taxonomy__taxonomic_status='ACCEPTED',
            taxonomy__rank__in=SPECIES_RANKS,
        ).values('module_group_id', 'iucn_category').annotate(
            colour=F('taxonomy__iucn_status__colour'),
            count=Count('id'),
        ).order_by('module_group_id', 'iucn_category')
        cons_by_group = {}
        for row in cons_rows:
            entry = {
                'iucn_category': row['iucn_category'],
                'colour': row['colour'],
                'count': row['count'],
            }
            if row['iucn_category'] in iucn_category_choices:
                entry['name'] = iucn_category_choices[row['iucn_category']]
            cons_by_group.setdefault(row['module_group_id'], []).append(entry)

        try:
            first = collection_results.first()
            if first is not None:
                if isinstance(first, SiteVisitTaxon):
                    self.is_sass_exist = annotated.filter(
                        site_visit__isnull=False
                    ).exists()
                else:
                    self.is_sass_exist = annotated.filter(
                        sitevisittaxon__isnull=False
                    ).exists()
        except Exception:  # noqa
            self.is_sass_exist = False

        biodiversity_data = OrderedDict()
        for group in groups:
            group_data = {}
            try:
                group_data[self.GROUP_ICON] = get_thumbnail(
                    group.logo, 'x140', crop='center'
                ).name
            except ValueError:
                pass

            group_data[self.MODULE] = group.id
            t = totals.get(group.id, {})
            group_data[self.GROUP_OCCURRENCES] = t.get('total', 0)
            group_data[self.GROUP_SITES] = t.get('unique_sites', 0)
            group_data[self.GROUP_NUM_OF_TAXA] = t.get('unique_taxa', 0)
            group_data[self.GROUP_ENDEMISM] = endemism_by_group.get(group.id, [])
            group_data[self.GROUP_ORIGIN] = origin_by_group.get(group.id, [])
            group_data[self.GROUP_CONS_STATUS] = cons_by_group.get(group.id, [])
            biodiversity_data[group.name] = group_data

        return biodiversity_data

class MultiLocationSitesOverview(SuperuserRequiredMixin, APIView, LocationSiteOverviewData):

    def get(self, request):
        """
        Get overview data for multiple sites
        """
        start_time = time.time()
        self.search_filters = request.GET

        response_data = dict()
        response_data[self.BIODIVERSITY_DATA] = self.biodiversity_data()
        response_data[self.SASS_EXIST] = self.is_sass_exist
        response_data['duration'] = time.time() - start_time
        return Response(response_data)


class MultiLocationSitesBackgroundOverview(BimsApiView):
    def get(self, request):
        parameters = request.GET
        search_uri = request.build_absolute_uri()

        search_process, created = get_or_create_search_process(
            search_type=SITES_SUMMARY,
            query=search_uri,
            requester=self.request.user
        )
        results = search_process.get_file_if_exits()
        if results:
            return Response(results)

        data_for_process_id = dict()
        data_for_process_id['search_uri'] = search_uri

        process_id = hashlib.sha256(
            str(
                json.dumps(
                    data_for_process_id, sort_keys=True
                )
            ).encode('utf-8')
        ).hexdigest()

        search_process.set_process_id(process_id)
        search_process.set_status(SEARCH_PROCESSING)

        task = location_sites_overview.delay(
            search_parameters=parameters,
            search_process_id=search_process.id
        )

        result_file = search_process.get_file_if_exits(
            finished=False
        )
        if result_file:
            result_file['task_id'] = task.id
            return Response(result_file)

        return Response({'status': 'Process does not exists'})


class SingleLocationSiteOverview(APIView, LocationSiteOverviewData):

    def get_object(self, pk):
        try:
            return LocationSite.objects.get(pk=pk)
        except LocationSite.DoesNotExist:
            raise Http404

    def get(self, request):
        start_time = time.time()
        self.search_filters = dict(request.GET)
        if not request.user.is_anonymous:
            self.search_filters['requester'] = request.user.id
        response_data = dict()
        response_data[self.BIODIVERSITY_DATA] = self.biodiversity_data()
        response_data[self.SASS_EXIST] = self.is_sass_exist

        site_id = request.GET.get('siteId')
        location_site = self.get_object(site_id)
        response_data[self.WATER_TEMPERATURE_EXIST] = (
            WaterTemperature.objects.filter(
                location_site=location_site
            ).exists()
        )
        response_data[self.PHYSICO_CHEMICAL_EXIST] = (
            ChemicalRecord.objects.filter(
                Q(location_site=location_site) |
                Q(survey__site=location_site)
            ).exists()
        )
        response_data[self.CLIMATE_EXIST] = (
            Climate.objects.filter(
                location_site=location_site
            ).exists()
        )
        serializer = LocationSiteDetailSerializer(
            location_site)
        response_data.update(serializer.data)
        end_time = time.time()
        response_data['duration'] = end_time - start_time
        return Response(response_data)
