import uuid
import logging
import time
from datetime import datetime

from django.contrib.sites.models import Site
from django_tenants.utils import get_tenant
from preferences import preferences

from bims.models.source_reference import SourceReference
from dateutil.parser import parse
from django.contrib.auth import get_user_model
from django.views.generic import TemplateView
from django.contrib.auth.decorators import login_required
from django.contrib.gis.geos import Point
from django.utils.decorators import method_decorator
from django.db.models import F, Q
from django.shortcuts import get_object_or_404
from django.urls import reverse, reverse_lazy
from django.contrib.gis.measure import Distance
from django.http import HttpResponseRedirect, Http404
from bims.utils.get_key import get_key
from bims.models import (
    LocationSite, Biotope, SamplingMethod,
    BiologicalCollectionRecord,
    Taxonomy,
    LocationType,
    TaxonGroup,
    SiteImage,
    LocationContext,
    BIOTOPE_TYPE_SPECIFIC,
    BIOTOPE_TYPE_BROAD,
    BIOTOPE_TYPE_SUBSTRATUM,
    Survey,
    Chem, ChemicalRecord,
    BaseMapLayer, COLLECTION_RECORD_KEY,
    Hydroperiod,
    WetlandIndicatorStatus,
    AbundanceType,
    SamplingEffortMeasure
)
from bims.enums.taxonomic_rank import TaxonomicRank
from bims.views.mixin.session_form.mixin import SessionFormMixin
from bims.models.algae_data import AlgaeData
from bims.models.record_type import RecordType
from bims.serializers.abundance_type import AbundanceTypeSerializer
from bims.serializers.sampling_effort_measure import (
    SamplingEffortMeasureSerializer
)
from bims.utils.search_process import clear_finished_search_in_background

logger = logging.getLogger('bims')

RIVER_CATCHMENT_ORDER = [
    'primary_catchment_area',
]


def add_survey_occurrences(self, post_data, site_images=None) -> Survey:
    date_string = post_data.get('date', None)
    end_embargo_date = post_data.get('end_embargo_date', None)
    owner_id = post_data.get('owner_id', '').strip()
    biotope_id = post_data.get('biotope', None)
    hydroperiod = post_data.get('hydroperiod', None)
    wetland_indicator_status = post_data.get(
        'wetland_indicator_status', None)
    specific_biotope_id = post_data.get('specific_biotope', None)
    substratum_id = post_data.get('substratum', None)
    sampling_method_id = post_data.get('sampling_method', None)
    abundance_type = post_data.get('abundance_type', None)
    reference = post_data.get('study_reference', '')
    reference_category = post_data.get('reference_category', '')
    record_type_str = post_data.get('record_type', None)
    ecosystem_type = ''
    if record_type_str is not None and record_type_str != '':
        record_type, _ = RecordType.objects.get_or_create(
            name=record_type_str
        )
    else:
        record_type = None
    site_id = post_data.get('site-id', None)
    source_reference_id = post_data.get('source_reference_id', None)

    biotope = None
    specific_biotope = None
    substratum = None
    sampling_method = None
    source_reference = None

    if biotope_id:
        biotope = Biotope.objects.get(
            id=biotope_id
        )
    if specific_biotope_id:
        specific_biotope = Biotope.objects.get(
            id=specific_biotope_id
        )
    if substratum_id:
        substratum = Biotope.objects.get(
            id=substratum_id
        )
    if sampling_method_id:
        sampling_method = SamplingMethod.objects.get(
            id=sampling_method_id
        )
    if source_reference_id:
        source_reference = SourceReference.objects.get(
            id=source_reference_id
        )

    abundance_type = AbundanceType.objects.filter(
        name__iexact=abundance_type
    ).first()

    sampling_effort = (
        post_data.get('sampling_effort', '')
    ).strip()

    sampling_effort_measure = (
        post_data.get('sampling_effort_type', None)
    )

    sampling_effort_link = None
    if sampling_effort_measure:
        sampling_effort_link = SamplingEffortMeasure.objects.filter(
            id=sampling_effort_measure
        ).first()
        if not sampling_effort_link:
            sampling_effort_link = SamplingEffortMeasure.objects.filter(
                name__iexact=sampling_effort_measure
            ).first()
    else:
        sampling_effort_measure = None

    if sampling_effort_link:
        sampling_effort_measure = sampling_effort_link

    collection_date = parse(date_string)

    if end_embargo_date:
        end_embargo_date = parse(
            timestr=end_embargo_date, dayfirst=True)
    else:
        end_embargo_date = None

    # Create or get location site
    site_name = post_data.get('site_name', '')
    site_code = post_data.get('site_code', '')
    site_description = post_data.get('site_description', '')
    latitude = post_data.get('latitude', 0.0)
    longitude = post_data.get('longitude', 0.0)
    site_point = Point(
        float(longitude),
        float(latitude))

    # If collector id exist then get the user object
    owner = None
    if owner_id:
        try:
            owner = get_user_model().objects.get(
                id=int(owner_id))
        except get_user_model().DoesNotExist:
            pass
    else:
        owner = self.request.user

    if site_name or site_code:
        location_type, created = LocationType.objects.get_or_create(
            name='PointObservation',
            allowed_geometry='POINT'
        )
        self.location_site, status = LocationSite.objects.get_or_create(
            name=site_name,
            site_code=site_code,
            site_description=site_description,
            location_type=location_type,
            geometry_point=site_point
        )
    else:
        self.location_site = LocationSite.objects.get(
            id=site_id
        )
        ecosystem_type = self.location_site.ecosystem_type

    taxa_id_list = post_data.get('taxa-id-list', '').split(',')
    taxa_id_list = filter(None, taxa_id_list)

    # Create a survey
    self.survey = Survey.objects.create(
        owner=owner,
        date=collection_date,
        site=self.location_site,
        collector_user=self.request.user,
        validated=False
    )

    if site_images:
        for img_file in site_images:
            SiteImage.objects.create(
                survey=self.survey,
                site=self.location_site,
                image=img_file,
                date=collection_date,
                form_uploader=COLLECTION_RECORD_KEY,
            )

    # -- Algae data
    curation_process = post_data.get('curation_process', None)
    indicator_chl_a = post_data.get('indicator_chl_a', None)
    indicator_afdm = post_data.get('indicator_afdm', None)
    ai = post_data.get('ai', '')
    if not ai:
        ai = None
    if curation_process or indicator_afdm or indicator_chl_a:
        algae_data = AlgaeData.objects.filter(
            survey=self.survey
        )
        if algae_data.exists():
            if algae_data.count() > 1:
                algae_data.exclude(id=algae_data[0].id).delete()
        else:
            AlgaeData.objects.create(survey=self.survey)
            algae_data = AlgaeData.objects.filter(survey=self.survey)
        algae_data.update(
            curation_process=curation_process,
            indicator_afdm=indicator_afdm,
            indicator_chl_a=indicator_chl_a,
            ai=ai
        )

    # -- Biomass chemical records
    chem_units = {}
    chl_type = post_data.get('chl_type', None)
    afdm_type = post_data.get('afdm_type', None)
    chl_a = post_data.get('chl_a', None)
    if chl_type and chl_a:
        chem_units[chl_type] = chl_a
        # Check existing data first, then remove it
        chla_codes = ['CHLA-B', 'CHLA-W']
        chla_records = ChemicalRecord.objects.filter(
            date=self.survey.date,
            location_site=self.survey.site,
            survey=self.survey,
            chem__in=Chem.objects.filter(
                chem_code__in=chla_codes
            )
        )
        if chla_records.exists():
            chla_records.delete()
    afdm = post_data.get('afdm', None)
    if afdm_type and afdm:
        chem_units[afdm_type] = afdm
        # Check existing data first, then remove it
        afdm_codes = ['AFDM-B', 'AFDM-W']
        afdm_records = ChemicalRecord.objects.filter(
            date=self.survey.date,
            location_site=self.survey.site,
            survey=self.survey,
            chem__in=Chem.objects.filter(
                chem_code__in=afdm_codes
            )
        )
        if afdm_records.exists():
            afdm_records.delete()
    for chem_unit in chem_units:
        chem = Chem.objects.filter(
            chem_code__iexact=chem_unit
        )
        if chem.exists():
            chem = chem[0]
        else:
            chem = Chem.objects.create(
                chem_code=chem_unit
            )
        chem_record, _ = ChemicalRecord.objects.get_or_create(
            date=self.survey.date,
            chem=chem,
            location_site=self.survey.site,
            survey=self.survey,
            value=chem_units[chem_unit]
        )

    if hydroperiod:
        hydroperiod = Hydroperiod.objects.get(
            name=hydroperiod
        )
    else:
        hydroperiod = None

    if wetland_indicator_status:
        wetland_indicator_status = WetlandIndicatorStatus.objects.get(
            name=wetland_indicator_status
        )
    else:
        wetland_indicator_status = None

    collection_record_ids = []
    for taxon in taxa_id_list:
        observed_key = '{}-observed'.format(taxon)
        abundance_key = '{}-abundance'.format(taxon)
        taxonomy = Taxonomy.objects.get(
            id=taxon
        )
        try:
            if post_data[observed_key] == 'True':
                abundance = post_data[abundance_key]
                try:
                    abundance = float(abundance) if abundance else None
                except ValueError:
                    abundance = None
                if abundance is not None and abundance <= 0:
                    logger.warning(
                        'Skipping taxon %s: abundance value %s is not greater than zero.',
                        taxon, abundance
                    )
                    continue
                collection_record, status = (
                    BiologicalCollectionRecord.objects.get_or_create(
                        collection_date=collection_date,
                        taxonomy=taxonomy,
                        original_species_name=taxonomy.canonical_name,
                        site=self.location_site,
                        collector_user=self.request.user,
                        sampling_method=sampling_method,
                        abundance_number=abundance,
                        owner=owner,
                        biotope=biotope,
                        specific_biotope=specific_biotope,
                        substratum=substratum,
                        reference=reference,
                        reference_category=reference_category,
                        sampling_effort=sampling_effort,
                        sampling_effort_link=sampling_effort_measure,
                        abundance_type=abundance_type,
                        survey=self.survey,
                        record_type=record_type,
                        source_reference=source_reference,
                        hydroperiod=hydroperiod,
                        ecosystem_type=ecosystem_type,
                        wetland_indicator_status=wetland_indicator_status,
                    )
                )
                collection_record_ids.append(collection_record.id)
                if status:
                    logger.info(
                        'Collection record added with id {}'.format(
                            collection_record.id
                        )
                    )
        except KeyError:
            continue

    Survey.objects.filter(
        id=self.survey.id
    ).update(
        end_embargo_date=end_embargo_date
    )
    BiologicalCollectionRecord.objects.filter(
        id__in=collection_record_ids
    ).update(
        end_embargo_date=end_embargo_date
    )

    return self.survey


class CollectionFormView(TemplateView, SessionFormMixin):
    """View for fish form"""
    template_name = 'collections_form_page.html'
    location_site = None
    session_identifier = 'collection-form'
    taxon_group_name = ''
    taxon_group_id = None
    survey = None
    all_taxa = None

    def get_all_taxa(self, taxon_group):
        """
        Get all taxon
        :param taxon_group: taxon group object
        :return: array of taxa id
        """
        return Taxonomy.objects.filter(
            id__in=BiologicalCollectionRecord.objects.filter(
                module_group=taxon_group
            ).distinct('taxonomy').values_list('taxonomy')
        ).values_list('id', flat=True)

    def nearest_taxa(self):
        """
        Find nearest taxa within radius
        :return: list of taxa
        """
        radius = 25
        return list(LocationSite.objects.filter(
            geometry_point__distance_lte=(
                self.location_site.geometry_point,
                Distance(km=radius))
        ).values(
            taxon_id=F('biological_collection_record__taxonomy'),
            taxon_name=F(
                'biological_collection_record__taxonomy__'
                'canonical_name'),
            rank=F('biological_collection_record__taxonomy__rank')
        ).distinct('taxon_name').filter(
            taxon_id__isnull=False,
            taxon_id__in=self.all_taxa,
        ).order_by(
            'taxon_name'
        ))

    def taxa_from_river_catchment(self):
        """
        Get taxa from nearest river_catchment
        :return: list of taxa
        """
        river_catchment_query = {}
        location_contexts = LocationContext.objects.filter(
            site=self.location_site
        )

        for river_catchment in RIVER_CATCHMENT_ORDER:
            river_catchment_value = location_contexts.value_from_key(
                river_catchment
            )
            if river_catchment_value != '-':
                river_catchment_query = {
                    'locationcontext__value': river_catchment_value
                }

        taxa_list = []
        if river_catchment_query:
            taxa_list = list(
                LocationSite.objects.filter(**river_catchment_query).values(
                    taxon_id=F('biological_collection_record__taxonomy'),
                    taxon_name=F(
                        'biological_collection_record__taxonomy__'
                        'canonical_name'),
                    rank=F('biological_collection_record__taxonomy__rank')
                ).distinct('taxon_name').filter(
                    taxon_id__isnull=False,
                    taxon_id__in=self.all_taxa,
                ).order_by(
                    'taxon_name'
                )
            )
        return taxa_list

    def get_context_data(self, **kwargs):
        context = super(CollectionFormView, self).get_context_data(**kwargs)
        if not self.location_site:
            return context
        context['location_site'] = self.location_site
        context['geoserver_public_location'] = get_key(
            'GEOSERVER_PUBLIC_LOCATION')
        context['location_site_name'] = self.location_site.name
        context['location_site_code'] = self.location_site.site_code
        context['location_site_lat'] = self.location_site.get_centroid().y
        context['location_site_long'] = self.location_site.get_centroid().x
        context['site_id'] = self.location_site.id

        abundance_types = AbundanceType.objects.filter(
            specific_module__name=self.taxon_group_name
        )
        if not abundance_types:
            abundance_types = AbundanceType.objects.filter(
                specific_module__isnull=True
            )
        context['abundance_types'] = AbundanceTypeSerializer(
            abundance_types,
            many=True
        ).data

        sampling_effort_measures = SamplingEffortMeasure.objects.filter(
            Q(specific_module__name=self.taxon_group_name) |
            Q(specific_module__isnull=True)
        )
        context['sampling_effort_measures'] = SamplingEffortMeasureSerializer(
            sampling_effort_measures,
            many=True
        ).data

        context['hydroperiod_choices'] = list(
            Hydroperiod.objects.all().values_list('name', flat=True)
        )
        context['record_types'] = list(
            RecordType.objects.filter(
                is_hidden_in_form=False
            ).values_list('name', flat=True)
        )

        try:
            context['bing_key'] = BaseMapLayer.objects.get(source_type='bing').key
        except BaseMapLayer.DoesNotExist:
            context['bing_key'] = ''

        # -- Taxa list
        if self.taxon_group_id:
            taxon_group = TaxonGroup.objects.get(
                id=self.taxon_group_id
            )
        else:
            taxon_group, created = TaxonGroup.objects.get_or_create(
                name=self.taxon_group_name
            )
        self.all_taxa = self.get_all_taxa(taxon_group)

        # Get from same river catchment
        taxa = self.taxa_from_river_catchment()

        if not taxa:
            # Get nearest taxa
            taxa = self.nearest_taxa()

        context['taxa'] = taxa

        context['taxon_rank'] = list(
            rank.name for rank in TaxonomicRank.hierarchy()
        )
        context['reference_category'] = list(
            BiologicalCollectionRecord.objects.filter(
                reference_category__isnull=False).exclude(
                reference_category='').distinct(
                'reference_category').values(
                name=F('reference_category'))
        )
        context['taxon_group_name'] = (
            taxon_group.singular_name
            if taxon_group.singular_name
            else taxon_group.name
        )
        context['taxon_group_id'] = taxon_group.id

        context['broad_biotope_list'] = list(
            Biotope.objects.filter(
                taxon_group=taxon_group,
                biotope_type=BIOTOPE_TYPE_BROAD
            ).values(
                'id', 'name', 'description', 'display_order'
            ).order_by('name')
        )
        context['specific_biotope_list'] = list(
            Biotope.objects.filter(
                taxon_group=taxon_group,
                biotope_type=BIOTOPE_TYPE_SPECIFIC
            ).values(
                'id', 'name', 'description', 'display_order'
            ).order_by('name')
        )

        context['substratum_list'] = list(
            Biotope.objects.filter(
                taxon_group=taxon_group,
                biotope_type=BIOTOPE_TYPE_SUBSTRATUM
            ).values(
                'id', 'name', 'description', 'display_order'
            ).order_by('name')
        )

        sampling_method_lower_list = []
        context['sampling_method_list'] = []
        sampling_method_list = list(
            SamplingMethod.objects.filter(
                taxon_group=taxon_group
            ).values(
                'id', 'sampling_method'
            ).order_by('sampling_method')
        )
        for sampling_method in sampling_method_list:
            sampling_method_name = (
                sampling_method['sampling_method'].replace('-', ' ')
            ).strip()
            if sampling_method_name.lower() not in sampling_method_lower_list:
                sampling_method_lower_list.append(sampling_method_name.lower())
                context['sampling_method_list'].append(sampling_method)

        return context

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
        site_id = request.GET.get('siteId', None)
        if site_id:
            self.location_site = get_object_or_404(
                LocationSite,
                pk=site_id
            )
        else:
            raise Http404()

        return super(CollectionFormView, self).get(request, *args, **kwargs)

    def extra_post(self, post):
        """
        Override this method to process the POST request.
        :param post: POST request
        """
        return

    @method_decorator(login_required)
    def post(self, request, *args, **kwargs):
        post_data = request.POST.dict()
        post_data['source_site'] = Site.objects.get_current().id
        post_data['source_reference_id'] = request.POST.get('source_reference')
        survey = add_survey_occurrences(
            self, post_data, request.FILES.getlist('site-images'))

        session_uuid = '%s' % uuid.uuid4()
        self.add_last_session(request, session_uuid, {
            'edited_at': int(time.mktime(datetime.now().timetuple())),
            'survey': survey.id,
            'location_site': self.location_site.name,
            'form': self.session_identifier
        })
        next_url = (
           '{base_url}?collectors=[{id}]&validated=["in review"]&'
           'o=date'.format(
                base_url=reverse_lazy('site-visit-list'),
                id=self.request.user.id
            )
        )
        redirect_url = next_url

        # Create a survey
        if (
                (
                    self.location_site.ecosystem_type and
                    'river' in self.location_site.ecosystem_type.lower()
                ) or preferences.SiteSetting.default_data_source == 'fbis'
        ):
            redirect_url = '{base_url}?survey={survey_id}&next={next}'.format(
                base_url=reverse('abiotic-form'),
                survey_id=self.survey.id,
                next=next_url
            )

        self.extra_post(request.POST)

        clear_finished_search_in_background(get_tenant(request))

        return HttpResponseRedirect(redirect_url)


class ModuleFormView(CollectionFormView):

    @method_decorator(login_required)
    def get(self, request, *args, **kwargs):
        module_id = request.GET.get('module', None)
        if not module_id:
            raise Http404('Missing module id')
        try:
            taxon_group = TaxonGroup.objects.get(
                id=module_id
            )
        except TaxonGroup.DoesNotExist:
            raise Http404('Missing module')
        self.taxon_group_name = (
            taxon_group.singular_name if taxon_group.singular_name else
            taxon_group.name
        )
        self.taxon_group_id = taxon_group.id
        self.session_identifier = '{}-form'.format(
            taxon_group.name.lower()
        )
        return super(ModuleFormView, self).get(request, *args, **kwargs)


class FishFormView(CollectionFormView):
    session_identifier = 'fish-form'
    taxon_group_name = 'Fish'


class InvertFormView(CollectionFormView):
    session_identifier = 'invert-form'
    taxon_group_name = 'Invertebrates'


class AlgaeFormView(CollectionFormView):
    session_identifier = 'algae-form'
    taxon_group_name = 'Algae'

    def extra_post(self, post):
        """
        Override this method to process the POST request.
        :param post: POST request
        """
        curation_process = post.get('curation_process', None)
        indicator_chl_a = post.get('indicator_chl_a', None)
        indicator_afdm = post.get('indicator_afdm', None)
        ai = post.get('ai', '')
        if not ai:
            ai = None
        algae_data = AlgaeData.objects.filter(
            survey=self.survey
        )
        if algae_data.exists():
            if algae_data.count() > 1:
                algae_data.exclude(id=algae_data[0].id).delete()
        else:
            AlgaeData.objects.create(survey=self.survey)
            algae_data = AlgaeData.objects.filter(survey=self.survey)
        algae_data.update(
            curation_process=curation_process,
            indicator_afdm=indicator_afdm,
            indicator_chl_a=indicator_chl_a,
            ai=ai
        )

        # -- Biomass chemical records
        chem_units = {}
        chl_type = post.get('chl_type', None)
        afdm_type = post.get('afdm_type', None)
        chl_a = post.get('chl_a', None)
        if chl_type and chl_a:
            chem_units[chl_type] = chl_a
            # Check existing data first, then remove it
            chla_codes = ['CHLA-B', 'CHLA-W']
            chla_records = ChemicalRecord.objects.filter(
                date=self.survey.date,
                location_site=self.survey.site,
                survey=self.survey,
                chem__in=Chem.objects.filter(
                    chem_code__in=chla_codes
                )
            )
            if chla_records.exists():
                chla_records.delete()
        afdm = post.get('afdm', None)
        if afdm_type and afdm:
            chem_units[afdm_type] = afdm
            # Check existing data first, then remove it
            afdm_codes = ['AFDM-B', 'AFDM-W']
            afdm_records = ChemicalRecord.objects.filter(
                date=self.survey.date,
                location_site=self.survey.site,
                survey=self.survey,
                chem__in=Chem.objects.filter(
                    chem_code__in=afdm_codes
                )
            )
            if afdm_records.exists():
                afdm_records.delete()
        for chem_unit in chem_units:
            chem = Chem.objects.filter(
                chem_code__iexact=chem_unit
            )
            if chem.exists():
                chem = chem[0]
            else:
                chem = Chem.objects.create(
                    chem_code=chem_unit
                )
            chem_record, _ = ChemicalRecord.objects.get_or_create(
                date=self.survey.date,
                chem=chem,
                location_site=self.survey.site,
                survey=self.survey,
                value=chem_units[chem_unit]
            )
