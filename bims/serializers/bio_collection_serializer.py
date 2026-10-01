import csv
import json
import logging
import uuid

from bims.models.chem import Chem
from preferences import preferences
from rest_framework import serializers
from rest_framework_gis.serializers import (
    GeoFeatureModelSerializer, GeometrySerializerMethodField)
from django.contrib.sites.models import Site
from django.urls import reverse
from django.db.models import Q

from bims.models.taxon_extra_attribute import TaxonExtraAttribute
from bims.models.biological_collection_record import BiologicalCollectionRecord
from bims.serializers.taxon_serializer import (
    TaxonSerializer,
    TaxonExportSerializer
)
from bims.models.dataset import Dataset
from bims.models.source_reference import (
    SourceReferenceBibliography,
    SourceReferenceDocument,
    SourceReferenceDatabase
)
from bims.models.chemical_record import (
    ChemicalRecord
)
from bims.models.iucn_status import IUCNStatus
from bims.models.location_context import LocationContext
from bims.models.algae_data import AlgaeData
from bims.models.survey import SurveyData, SurveyDataValue, Survey
from bims.scripts.collection_csv_keys import *  # noqa
from bims.models.location_context_group import LocationContextGroup
from bims.models.location_context_filter_group_order import (
    LocationContextFilterGroupOrder
)
from bims.models.taxonomy import Taxonomy
from bims.models.taxon_group_taxonomy import TaxonGroupTaxonomy

ORIGIN = {
    'alien': 'Non-Native',
    'indigenous': 'Native',
}

logger = logging.getLogger(__name__)
TEMPLATE_HEADER_KEYS = 'upload_template_headers'
SANPARK_PARK_NAME = 'SANParks and MPAs'
PARK_GROUP_KEYS = {
    'park_or_mpa_name', 'park_or_mpa',
    'parks_and_mpas', 'sanparks_and_mpas',
    'sanparks_mpas', 'parks_mpas'
}

FBIS_SURVEY_DATA_KEYS = (
    'Water Level',
    'Water Turbidity',
    'Embeddedness'
)
FBIS_CHEMICAL_KEYS = (
    TEMP, CONDUCTIVITY, PH, DISSOLVED_OXYGEN_MG, DISSOLVED_OXYGEN_PERCENT,
    TURBIDITY, DEPTH_M, NBV, ORTHOPHOSPHATE, TOT, SILICA, NH3_N, NH4_N,
    NO3_NO2_N, NO2_N, NO3_N, TIN, CHLA_B, AFDM,
)

# Relations read by BioCollectionOneRowSerializer for every row, loaded
# up front so serializing a batch doesn't query once per row per field.
EXPORT_SELECT_RELATED = (
    'site',
    'site__river',
    'site__location_type',
    'taxonomy',
    'taxonomy__origin',
    'taxonomy__endemism',
    'taxonomy__iucn_status',
    'taxonomy__national_conservation_status',
    'taxonomy__accepted_taxonomy__iucn_status',
    'owner',
    'collector_user',
    'analyst',
    'sampling_method',
    'sampling_effort_link',
    'abundance_type',
    'hydroperiod',
    'wetland_indicator_status',
    'biotope',
    'specific_biotope',
    'substratum',
    'module_group',
    'record_type',
    'licence',
)
EXPORT_PREFETCH_RELATED = (
    'decisionsupporttool_set__dst_name',
)

# Context caches holding data bulk-loaded for the current batch only; they
# are replaced on every prefetch_batch call so memory stays bounded.
BATCH_CACHE_KEYS = (
    'survey_lookup',
    'location_context',
    'chem_records_cached',
    'fbis_chem_values',
    'survey_data_values',
    'algae',
)


def context_memo(context, bucket, key, compute):
    """Return context[bucket][key], computing and storing it on a miss.
    Unlike get_context_cache, falsy results are cached too."""
    cache = context.setdefault(bucket, {})
    if key not in cache:
        cache[key] = compute()
    return cache[key]


def is_fbis(context):
    return context_memo(
        context, 'constants', 'is_fbis',
        lambda: preferences.SiteSetting.default_data_source == 'fbis'
    )


def fbis_survey_data(context):
    """SurveyData objects for the FBIS survey columns, keyed by column."""
    return context_memo(
        context, 'constants', 'fbis_survey_data',
        lambda: {
            key: SurveyData.objects.filter(name__iexact=key).first()
            for key in FBIS_SURVEY_DATA_KEYS
        }
    )


def fbis_chemical_units(context):
    """(chem id, column label) for each FBIS chemical column that exists."""
    def build():
        units = []
        for chem_key in FBIS_CHEMICAL_KEYS:
            chem = Chem.objects.filter(
                chem_code__iexact=chem_key
            ).select_related('chem_unit').first()
            if not chem:
                continue
            unit = chem.chem_unit.unit if chem.chem_unit else ''
            units.append((chem.id, f'{chem.chem_description} ({unit})'))
        return units
    return context_memo(context, 'constants', 'fbis_chemical_units', build)


def geocontext_groups(context):
    """Location context groups exported as columns, in filter display
    order, as dicts of name/key/id."""
    groups = context.get('geocontext_groups')
    if groups is not None:
        return groups
    ordered_group_ids = list(
        LocationContextFilterGroupOrder.objects
        .order_by('filter__display_order', 'group_display_order')
        .values_list('group_id', flat=True)
        .distinct()
    )
    group_lookup = {
        g.id: g for g in
        LocationContextGroup.objects.filter(id__in=ordered_group_ids)
    }
    groups = []
    seen = set()
    for gid in ordered_group_ids:
        if gid in seen or gid not in group_lookup:
            continue
        seen.add(gid)
        grp = group_lookup[gid]
        if (grp.key and grp.key.lower() in PARK_GROUP_KEYS) or (
                'park' in grp.name.lower() and 'mpa' in grp.name.lower()
        ):
            display_name = SANPARK_PARK_NAME
        else:
            display_name = grp.name
        groups.append({'name': display_name, 'key': grp.key, 'id': grp.id})
    context['geocontext_groups'] = groups
    return groups


def survey_lookup_key(record):
    return (
        record.site_id,
        record.collection_date,
        record.collector_user_id,
        record.owner_id,
    )


def _first_per_key(rows, key_len):
    """Map the first key_len columns of each row to the next column,
    keeping the first row seen for each key."""
    result = {}
    for row in rows:
        result.setdefault(tuple(row[:key_len]), row[key_len])
    return result


def prefetch_batch(records, context):
    """Bulk-load the per-row lookups of BioCollectionOneRowSerializer for a
    batch of records into the serializer context, so serializing the batch
    runs a fixed number of queries instead of several per row."""
    for key in BATCH_CACHE_KEYS:
        context[key] = {}
    if not records:
        return

    # Records without a survey are matched to one by site, date, collector
    # and owner, as long as the match is unambiguous.
    missing_survey = [r for r in records if not r.survey_id]
    if missing_survey:
        candidates = {}
        for survey in Survey.objects.filter(
            site_id__in={r.site_id for r in missing_survey},
            date__in={r.collection_date for r in missing_survey},
        ).only('id', 'site_id', 'date', 'collector_user_id', 'owner_id'):
            candidates.setdefault((
                survey.site_id, survey.date,
                survey.collector_user_id, survey.owner_id
            ), []).append(survey.id)
        for record in missing_survey:
            key = survey_lookup_key(record)
            matches = candidates.get(key, [])
            context['survey_lookup'][key] = (
                matches[0] if len(matches) == 1 else None
            )

    survey_ids = {r.survey_id for r in records if r.survey_id}
    survey_ids.update(
        s for s in context['survey_lookup'].values() if s
    )
    site_ids = {r.site_id for r in records}
    site_dates = {(r.site_id, r.collection_date) for r in records}
    dates = {d for _, d in site_dates}

    group_ids = [g['id'] for g in geocontext_groups(context)]
    if group_ids:
        values = _first_per_key(
            LocationContext.objects.filter(
                site_id__in=site_ids,
                group_id__in=group_ids
            ).order_by('pk').values_list('site_id', 'group_id', 'value'),
            2
        )
        for site_id in site_ids:
            for group_id in group_ids:
                context['location_context'][(site_id, group_id)] = (
                    values.get((site_id, group_id)) or '-'
                )

    if is_fbis(context):
        chem_units = fbis_chemical_units(context)
        chem_ids = [chem_id for chem_id, _ in chem_units]
        values = _first_per_key(
            ChemicalRecord.objects.filter(
                chem_id__in=chem_ids,
                survey__site_id__in=site_ids,
                survey__date__in=dates,
            ).order_by('pk').values_list(
                'survey__site_id', 'survey__date', 'chem_id', 'value'
            ),
            3
        )
        for site_id, date in site_dates:
            for chem_id in chem_ids:
                key = (site_id, date, chem_id)
                context['fbis_chem_values'][key] = values.get(key, '-')

        survey_data_ids = [
            sd.id for sd in fbis_survey_data(context).values() if sd
        ]
        if survey_data_ids and survey_ids:
            values = _first_per_key(
                SurveyDataValue.objects.filter(
                    survey_id__in=survey_ids,
                    survey_data_id__in=survey_data_ids,
                ).order_by('pk').values_list(
                    'survey_id', 'survey_data_id',
                    'survey_data_option__option'
                ),
                2
            )
            for survey_id in survey_ids:
                for survey_data_id in survey_data_ids:
                    key = (survey_id, survey_data_id)
                    context['survey_data_values'][key] = values.get(key)
    else:
        values = _first_per_key(
            ChemicalRecord.objects.filter(
                survey__site_id__in=site_ids,
                survey__date__in=dates,
            ).order_by(
                'survey__site_id', 'survey__date', 'chem__chem_code', 'pk'
            ).values_list(
                'survey__site_id', 'survey__date', 'chem__chem_code', 'value'
            ),
            3
        )
        chem_records = context['chem_records_cached']
        for site_date in site_dates:
            chem_records[site_date] = {}
        for (site_id, date, chem_code), value in values.items():
            row = chem_records.get((site_id, date))
            if row is not None:
                row[chem_code.upper()] = value

    algae_survey_ids = {
        r.survey_id or context['survey_lookup'].get(survey_lookup_key(r))
        for r in records
        if r.module_group and 'algae' in r.module_group.name.lower()
    }
    algae_survey_ids.discard(None)
    if algae_survey_ids:
        algae_by_survey = {}
        for algae in AlgaeData.objects.filter(
            survey_id__in=algae_survey_ids
        ).order_by('pk'):
            algae_by_survey.setdefault(algae.survey_id, algae)
        for survey_id in algae_survey_ids:
            context['algae'][survey_id] = algae_by_survey.get(survey_id)

    # Divisions depend only on the taxon, so they stay cached across batches.
    divisions = context.setdefault('division', {})
    taxon_ids = {
        r.taxonomy_id for r in records
        if r.taxonomy_id and r.taxonomy_id not in divisions
    }
    if taxon_ids:
        names = _first_per_key(
            TaxonGroupTaxonomy.objects.filter(
                taxonomy_id__in=taxon_ids,
                taxongroup__category__icontains='division'
            ).order_by(
                'taxongroup__display_order', 'taxongroup_id'
            ).values_list('taxonomy_id', 'taxongroup__name'),
            1
        )
        for taxon_id in taxon_ids:
            divisions[taxon_id] = names.get((taxon_id,))


class BioCollectionSerializer(serializers.ModelSerializer):
    """
    Serializer for biological collection record.
    """
    location = serializers.SerializerMethodField()
    owner = serializers.SerializerMethodField()
    owner_email = serializers.SerializerMethodField()
    taxonomy = serializers.SerializerMethodField()
    site_name = serializers.SerializerMethodField()

    def get_site_name(self, obj):
        return obj.site.name

    def get_taxonomy(self, obj):
        return TaxonSerializer(obj.taxonomy).data

    def get_owner(self, obj):
        return obj.owner.username

    def get_owner_email(self, obj):
        return obj.owner.email

    def get_location(self, obj):
        return obj.site.get_geometry().geojson

    class Meta:
        model = BiologicalCollectionRecord
        fields = '__all__'


class SerializerContextCache(serializers.ModelSerializer):

    def get_context_cache(self, key, identifier):
        context_data = self.context.get(key)
        if not context_data:
            return None
        if identifier in context_data:
            return context_data[identifier]
        return None

    def set_context_cache(self, key, identifier, value):
        context_data = self.context.get(key)
        if not context_data:
            context_data = {}
        context_data[identifier] = value
        self.context[key] = context_data


class BioCollectionOneRowSerializer(
    SerializerContextCache
):
    """
    Serializer for biological collection record.
    """
    uuid = serializers.SerializerMethodField()
    user_river_name = serializers.SerializerMethodField()
    wetland_name = serializers.SerializerMethodField()
    user_wetland_name = serializers.SerializerMethodField()
    site_code = serializers.SerializerMethodField()
    user_site_code = serializers.SerializerMethodField()
    site_description = serializers.SerializerMethodField()
    user_geomorphological_zone = serializers.SerializerMethodField()
    river_name = serializers.SerializerMethodField()
    latitude = serializers.SerializerMethodField()
    longitude = serializers.SerializerMethodField()
    origin = serializers.SerializerMethodField()
    sampling_date = serializers.SerializerMethodField()
    sampling_method = serializers.SerializerMethodField()
    broad_biotope = serializers.SerializerMethodField()
    specific_biotope = serializers.SerializerMethodField()
    substratum = serializers.SerializerMethodField()
    taxon = serializers.SerializerMethodField()
    collector_or_owner = serializers.SerializerMethodField()
    title = serializers.SerializerMethodField()
    reference_category = serializers.SerializerMethodField()
    endemism = serializers.SerializerMethodField()
    conservation_status_global = serializers.SerializerMethodField()
    conservation_status_national = serializers.SerializerMethodField()
    phylum = serializers.SerializerMethodField()
    class_name = serializers.SerializerMethodField()
    order = serializers.SerializerMethodField()
    family = serializers.SerializerMethodField()
    genus = serializers.SerializerMethodField()
    kingdom = serializers.SerializerMethodField()
    taxon_rank = serializers.SerializerMethodField()
    species = serializers.SerializerMethodField()
    sub_species = serializers.SerializerMethodField()
    notes = serializers.SerializerMethodField()
    doi_or_url = serializers.SerializerMethodField()
    sampling_effort_measure = serializers.SerializerMethodField()
    sampling_effort_value = serializers.SerializerMethodField()
    abundance_measure = serializers.SerializerMethodField()
    abundance_value = serializers.SerializerMethodField()
    collector_or_owner_institute = serializers.SerializerMethodField()
    analyst = serializers.SerializerMethodField()
    analyst_institute = serializers.SerializerMethodField()
    authors = serializers.SerializerMethodField()
    source = serializers.SerializerMethodField()
    year = serializers.SerializerMethodField()
    upstream_id = serializers.SerializerMethodField()
    taxon_key = serializers.SerializerMethodField()
    species_key = serializers.SerializerMethodField()
    basis_of_record = serializers.SerializerMethodField()
    institution_code = serializers.SerializerMethodField()
    collection_code = serializers.SerializerMethodField()
    catalog_number = serializers.SerializerMethodField()
    identified_by = serializers.SerializerMethodField()
    rights_holder = serializers.SerializerMethodField()
    recorded_by = serializers.SerializerMethodField()
    decision_support_tool = serializers.SerializerMethodField()
    record_type = serializers.SerializerMethodField()
    ecosystem_type = serializers.SerializerMethodField()
    hydroperiod = serializers.SerializerMethodField()
    wetland_indicator_status = serializers.SerializerMethodField()
    cites_listing = serializers.SerializerMethodField()
    data_type = serializers.SerializerMethodField()
    dataset = serializers.SerializerMethodField()
    end_embargo_date = serializers.SerializerMethodField()
    licence = serializers.SerializerMethodField()
    gbif_coordinate_uncertainty_m = serializers.SerializerMethodField()
    gbif_coordinate_precision = serializers.SerializerMethodField()

    @staticmethod
    def _has_value(v) -> bool:
        """Treat None/''/whitespace/empty containers as no value.
        Numbers (incl. 0) and booleans (incl. False) count as values."""
        if v is None:
            return False
        if isinstance(v, str):
            return v.strip() != ''
        if isinstance(v, (list, tuple, set, dict)):
            return len(v) > 0
        return True

    @staticmethod
    def _get_additional_value(additional, header):
        """Try several key variants to find the value in additional_data."""
        if not isinstance(additional, dict):
            return None
        variants = [
            header,
            header.strip(),
            header.lower(),
            header.replace(' ', '_'),
            header.lower().replace(' ', '_'),
            header.replace('/', ' or '),
            header.lower().replace('/', ' or '),
        ]
        for k in variants:
            if k in additional:
                return additional[k]
        return None

    def _memo(self, bucket, key, compute):
        return context_memo(self.context, bucket, key, compute)

    def get_dataset(self, obj: BiologicalCollectionRecord):
        if not obj.dataset_key:
            return ''

        def abbreviation():
            dataset = Dataset.objects.filter(uuid=obj.dataset_key).first()
            return dataset.abbreviation if dataset else ''

        return self._memo('dataset', obj.dataset_key, abbreviation)

    def taxon_name_by_rank(
            self,
            obj: BiologicalCollectionRecord,
            rank_identifier: str):
        return self._memo(
            rank_identifier,
            obj.taxonomy.id,
            lambda: getattr(obj.taxonomy, rank_identifier) or '-'
        )

    def spatial_data(self, obj, key):
        def value():
            data = LocationContext.objects.filter(
                site_id=obj.site_id,
                group__id=key
            ).order_by('pk').first()
            return (data.value if data else None) or '-'

        return self._memo('location_context', (obj.site_id, key), value)

    def _source_reference_value(self, obj, attr):
        """Attribute of the record's source reference, cached per source
        reference since many records share one."""
        return self._memo(
            'source_reference_' + attr,
            obj.source_reference_id,
            lambda: getattr(obj.source_reference, attr)
        )

    def __init__(self, *args, **kwargs):
        super(BioCollectionOneRowSerializer, self).__init__(*args, **kwargs)
        self.context.setdefault('chem_records_cached', {})
        exclude_fields = self.context.get('exclude_fields', [])
        if 'header' not in self.context:
            self.context['header'] = []

        for field in exclude_fields:
            if field in self.fields:
                self.fields.pop(field)

    def chem_data(self, obj, chem):
        return chem

    def get_ecosystem_type(self, obj: BiologicalCollectionRecord):
        return obj.site.ecosystem_type

    def get_wetland_name(self, obj: BiologicalCollectionRecord):
        return obj.site.wetland_name if obj.site.wetland_name else '-'

    def get_user_wetland_name(self, obj: BiologicalCollectionRecord):
        return obj.site.user_wetland_name if obj.site.user_wetland_name else '-'

    def get_hydroperiod(self, obj: BiologicalCollectionRecord):
        if obj.hydroperiod:
            return obj.hydroperiod.name
        return '-'

    def get_wetland_indicator_status(self, obj: BiologicalCollectionRecord):
        if obj.wetland_indicator_status:
            return obj.wetland_indicator_status.name
        return '-'

    def get_abundance_measure(self, obj):
        if obj.abundance_type:
            return obj.abundance_type.name
        return '-'

    def get_abundance_value(self, obj):
        if obj.abundance_number:
            return obj.abundance_number
        return '-'

    def get_uuid(self, obj):
        if obj.uuid:
            try:
                return str(obj.uuid)
            except ValueError:
                return obj.uuid
        return '-'

    def get_user_river_name(self, obj):
        if obj.site.legacy_river_name:
            return obj.site.legacy_river_name
        return '-'

    def get_user_geomorphological_zone(self, obj):
        if obj.site.refined_geomorphological:
            return obj.site.refined_geomorphological
        return '-'

    def get_sampling_effort_measure(self, obj):
        if obj.sampling_effort_link:
            return obj.sampling_effort_link.name
        return '-'

    def get_sampling_effort_value(self, obj):
        if obj.sampling_effort:
            return obj.sampling_effort.split(' ')[0]
        return '-'

    def get_conservation_status_global(self, obj):
        taxon = obj.taxonomy
        if (
            taxon and
            not taxon.iucn_status and
            taxon.is_synonym and
            taxon.accepted_taxonomy
        ):
            taxon = taxon.accepted_taxonomy
        if taxon and taxon.iucn_status:
            category = dict(IUCNStatus.CATEGORY_CHOICES)
            try:
                return category[taxon.iucn_status.category]
            except KeyError:
                pass
        return 'Not evaluated'

    def get_conservation_status_national(self, obj):
        if obj.taxonomy and obj.taxonomy.national_conservation_status:
            category = dict(IUCNStatus.CATEGORY_CHOICES)
            try:
                return category[
                    obj.taxonomy.national_conservation_status.category]
            except KeyError:
                pass
        return '-'

    def get_site_code(self, obj):
        return obj.site.site_code

    def get_cites_listing(self, obj: BiologicalCollectionRecord):
        return self._memo(
            'cites_listing',
            obj.taxonomy.id,
            lambda: obj.taxonomy.cites_listing
        )

    def get_user_site_code(self, obj):
        return obj.site.legacy_site_code

    def get_river_name(self, obj):
        if obj.site.river:
            return obj.site.river.name
        return 'Unknown'

    def get_site_description(self, obj):
        if obj.site.site_description:
            return obj.site.site_description.replace(';', ',')
        return obj.site.name.replace(';', ',')

    def get_latitude(self, obj):
        lat = obj.site.get_centroid().y
        return lat

    def get_longitude(self, obj):
        lon = obj.site.get_centroid().x
        return lon

    def get_origin(self, obj):
        if obj.taxonomy and obj.taxonomy.origin:
            return obj.taxonomy.origin.category
        return 'Unknown'

    def get_endemism(self, obj):
        if obj.taxonomy.endemism:
            return obj.taxonomy.endemism.name
        return 'Unknown'

    def get_sampling_date(self, obj):
        if obj.collection_date:
            return obj.collection_date.isoformat().split('T')[0]

    def get_title(self, obj):
        if obj.source_reference_id:
            return self._source_reference_value(obj, 'title')
        else:
            return '-'

    def get_taxon(self, obj):
        taxon = self.get_context_cache(
            'taxon',
            obj.taxonomy.id
        )
        if taxon:
            return taxon
        if obj.taxonomy:
            if obj.taxonomy.canonical_name:
                self.set_context_cache(
                    'taxon',
                    obj.taxonomy.id,
                    obj.taxonomy.canonical_name
                )
                return obj.taxonomy.canonical_name
        if obj.original_species_name:
            self.set_context_cache(
                'taxon',
                obj.taxonomy.id,
                obj.original_species_name
            )
            return obj.original_species_name
        return '-'

    def get_class_name(self, obj):
        return self.taxon_name_by_rank(
            obj,
            'class_name'
        )

    def get_phylum(self, obj):
        return self.taxon_name_by_rank(
            obj,
            'phylum_name'
        )

    def get_order(self, obj):
        return self.taxon_name_by_rank(
            obj,
            'order_name'
        )

    def get_family(self, obj):
        return self.taxon_name_by_rank(
            obj,
            'family_name'
        )

    def get_genus(self, obj):
        return self.taxon_name_by_rank(
            obj,
            'genus_name'
        )

    def get_species(self, obj):
        species_name = self.taxon_name_by_rank(
            obj,
            'species_name'
        )
        if species_name:
            genus_name = self.taxon_name_by_rank(
                obj,
                'genus_name'
            )
            if genus_name:
                species_name = species_name.replace(genus_name, '')
            return species_name.strip()
        return '-'

    def get_sub_species(self, obj: BiologicalCollectionRecord):
        sub_species_name = self.taxon_name_by_rank(
            obj,
            'sub_species_name'
        )
        if sub_species_name:
            genus = self.get_genus(obj)
            if genus:
                sub_species_name = sub_species_name.replace(genus, '', 1)

            species = self.get_species(obj)
            if species:
                sub_species_name = sub_species_name.replace(species, '', 1)

            return sub_species_name.strip()
        return '-'

    def get_kingdom(self, obj):
        return self.taxon_name_by_rank(
            obj,
            'kingdom_name'
        )

    def get_taxon_rank(self, obj):
        taxon_rank = obj.taxonomy.get_rank_display()
        return taxon_rank if taxon_rank else '-'

    def get_reference_category(self, obj):
        if obj.source_reference_id:
            return self._source_reference_value(obj, 'reference_type')
        else:
            return '-'

    def get_authors(self, obj):
        if obj.source_reference_id:
            return self._source_reference_value(obj, 'authors')
        return '-'

    def get_source(self, obj):
        if obj.source_reference_id:
            reference_source = self._source_reference_value(
                obj, 'reference_source'
            )
            if reference_source:
                return reference_source
        return '-'

    def get_year(self, obj):
        if obj.source_reference_id:
            return self._source_reference_value(obj, 'year')
        return '-'

    def get_collector_or_owner(self, obj):
        if obj.owner:
            return '{first_name} {last_name}'.format(
                first_name=obj.owner.first_name,
                last_name=obj.owner.last_name
            )
        if obj.additional_data:
            # If this is BioBase data, return a collector name from author of
            # reference
            additional_data = {}
            if isinstance(obj.additional_data, str):
                additional_data = json.loads(obj.additional_data)
            elif isinstance(obj.additional_data, dict):
                additional_data = obj.additional_data
            if 'BioBaseData' in additional_data:
                if isinstance(
                        obj.source_reference, SourceReferenceBibliography):
                    source = obj.source_reference.source
                    author_str = '%(last_name)s %(first_initial)s'
                    s = ', '.join(
                        [author_str % a.__dict__ for a in
                         source.get_authors()])
                    s = ', and '.join(s.rsplit(', ', 1))  # last author case
                    return s
        if obj.collector_user:
            return '{first_name} {last_name}'.format(
                first_name=obj.collector_user.first_name,
                last_name=obj.collector_user.last_name
            )
        if obj.collector:
            return obj.collector
        try:
            return '{first_name} {last_name}'.format(
                first_name=obj.owner.first_name,
                last_name=obj.owner.last_name
            )
        except Exception as e:  # noqa
            return '-'

    def get_collector_or_owner_institute(self, obj):
        if (
            obj.institution_id and
            obj.institution_id not in {'bims', 'healthyrivers'}
        ):
            return obj.institution_id

        if (
            obj.owner and
            obj.owner.organization and
            'admin' not in obj.owner.username
        ):
            return obj.owner.organization

        if obj.collector_user and obj.collector_user.organization:
            return obj.collector_user.organization

        return '-'

    def get_analyst(self, obj):
        if obj.analyst:
            return '{first_name} {last_name}'.format(
                first_name=obj.analyst.first_name,
                last_name=obj.analyst.last_name
            )
        return '-'

    def get_analyst_institute(self, obj):
        if obj.analyst:
            return obj.analyst.organization
        return '-'

    def get_notes(self, obj):
        return obj.notes

    def get_doi_or_url(self, obj):
        if obj.source_reference_id:
            if 'source_reference' not in self.context:
                self.context['source_reference'] = {}
            if obj.source_reference_id in self.context['source_reference']:
                return (
                    self.context['source_reference'][obj.source_reference_id]
                )
            url = ''
            document = None
            if isinstance(obj.source_reference,
                          SourceReferenceBibliography):
                url = obj.source_reference.source.doi
                if not url and obj.source_reference.document:
                    document = obj.source_reference.document
            elif isinstance(obj.source_reference,
                            SourceReferenceDocument):
                document = obj.source_reference.source
            elif isinstance(obj.source_reference,
                            SourceReferenceDatabase):
                if obj.source_reference.source.url:
                    url = obj.source_reference.source.url
                if obj.source_reference.document:
                    document = obj.source_reference.document
            if not url and document:
                if document.doc_file:
                    url = ''.join(
                        [Site.objects.get_current().domain,
                         document.doc_file.url])
                else:
                    url = document.doc_url
            self.context['source_reference'][obj.source_reference.id] = (
                url
            )
            return url
        return '-'

    def get_sampling_method(self, obj):
        if obj.sampling_method:
            return obj.sampling_method.sampling_method.capitalize()
        return '-'

    def get_broad_biotope(self, obj):
        if obj.biotope:
            return obj.biotope.name.capitalize()
        return '-'

    def get_specific_biotope(self, obj):
        if obj.specific_biotope:
            return obj.specific_biotope.name.capitalize()
        return '-'

    def get_substratum(self, obj):
        if obj.substratum:
            return obj.substratum.name.capitalize()
        return '-'

    def occurrences_fields(self, obj, field):
        if obj.additional_data and isinstance(obj.additional_data, dict):
            if field in obj.additional_data:
                return obj.additional_data[field]
        return '-'

    def get_decision_support_tool(self, obj):
        # Iterating .all() uses the prefetched rows when available.
        dst_names = sorted({
            dst.dst_name.name
            for dst in obj.decisionsupporttool_set.all()
            if dst.dst_name_id
        })
        if dst_names:
            return ', '.join(dst_names)
        return '-'

    def get_upstream_id(self, obj: BiologicalCollectionRecord):
        if obj.upstream_id:
            return obj.upstream_id
        if obj.additional_data and isinstance(obj.additional_data, dict):
            if 'eventID' in obj.additional_data:
                return obj.additional_data['eventID']
        return ''

    def get_taxon_key(self, obj):
        return self.occurrences_fields(obj, 'taxonKey')

    def get_species_key(self, obj):
        return self.occurrences_fields(obj, 'speciesKey')

    def get_basis_of_record(self, obj):
        return self.occurrences_fields(obj, 'basisOfRecord')

    def get_institution_code(self, obj):
        return self.occurrences_fields(obj, 'institutionCode')

    def get_collection_code(self, obj):
        return self.occurrences_fields(obj, 'collectionCode')

    def get_catalog_number(self, obj):
        return self.occurrences_fields(obj, 'catalogNumber')

    def get_identified_by(self, obj):
        return self.occurrences_fields(obj, 'identifiedBy')

    def get_rights_holder(self, obj):
        return self.occurrences_fields(obj, 'rightsHolder')

    def get_recorded_by(self, obj):
        return self.occurrences_fields(obj, 'recordedBy')

    def get_record_type(self, obj):
        if obj.record_type:
            return obj.record_type.name
        return '-'

    def get_data_type(self, obj: BiologicalCollectionRecord):
        if obj.data_type:
            return obj.data_type.capitalize()
        return 'Public'

    def get_end_embargo_date(self, obj: BiologicalCollectionRecord):
        if obj.end_embargo_date:
            return obj.end_embargo_date.isoformat()
        return ''

    def get_licence(self, obj: BiologicalCollectionRecord):
        if obj.licence:
            return obj.licence.identifier
        return ''

    def get_gbif_coordinate_uncertainty_m(self, obj: BiologicalCollectionRecord):
        value = getattr(obj, 'coordinate_uncertainty_in_meters', None)
        if value:
            return f"{value:.2f}"
        return ''

    def get_gbif_coordinate_precision(self, obj: BiologicalCollectionRecord):
        value = getattr(obj, 'coordinate_precision', None)
        if value:
            return f"{value:.6f}"
        return ''

    class Meta:
        model = BiologicalCollectionRecord
        fields = [
            'uuid',
            'user_river_name',
            'river_name',
            'user_wetland_name',
            'wetland_name',
            'user_site_code',
            'site_code',
            'ecosystem_type',
            'site_description',
            'user_geomorphological_zone',
            'latitude',
            'longitude',
            'sampling_date',
            'kingdom',
            'phylum',
            'class_name',
            'order',
            'family',
            'genus',
            'species',
            'sub_species',
            'taxon',
            'taxon_rank',
            'sampling_method',
            'sampling_effort_measure',
            'sampling_effort_value',
            'abundance_measure',
            'abundance_value',
            'hydroperiod',
            'wetland_indicator_status',
            'broad_biotope',
            'specific_biotope',
            'substratum',
            'origin',
            'endemism',
            'conservation_status_global',
            'conservation_status_national',
            'collector_or_owner',
            'collector_or_owner_institute',
            'analyst',
            'analyst_institute',
            'authors',
            'year',
            'source',
            'reference_category',
            'title',
            'doi_or_url',
            'notes',
            'upstream_id',
            'taxon_key',
            'species_key',
            'basis_of_record',
            'institution_code',
            'collection_code',
            'catalog_number',
            'identified_by',
            'rights_holder',
            'recorded_by',
            'decision_support_tool',
            'record_type',
            'dataset',
            'dataset_key',
            'cites_listing',
            'data_type',
            'end_embargo_date',
            'licence',
            'gbif_coordinate_uncertainty_m',
            'gbif_coordinate_precision',
        ]

    def _get_geocontext_parks_group(self):
        return self._memo(
            'constants', 'parks_group', self._find_geocontext_parks_group
        )

    def _find_geocontext_parks_group(self):
        groups = geocontext_groups(self.context)
        for g in groups:
            if (g.get('key') or '').lower() in PARK_GROUP_KEYS or (
                g.get('name') or ''
            ).lower() in {
                'sanparks and mpas', 'parks and mpas', 'sanparks & mpas'
            }:
                return g
        try:
            grp = LocationContextGroup.objects.filter(
                Q(key__in=list(PARK_GROUP_KEYS)) |
                Q(name__iexact=SANPARK_PARK_NAME)
            ).first()
            if grp:
                display_name = SANPARK_PARK_NAME
                if 'header' not in self.context:
                    self.context['header'] = []
                if display_name not in self.context['header']:
                    self.context['header'].append(display_name)
                grp_obj = {'name': display_name, 'key': grp.key, 'id': grp.id}
                groups.append(grp_obj)
                return grp_obj
        except Exception:
            pass
        return None

    def _get_sanparks_mpa_value(self, instance):
        grp = self._get_geocontext_parks_group()
        if grp:
            return self.spatial_data(instance, grp['id'])
        return '-'

    @staticmethod
    def _find_survey_id(instance: BiologicalCollectionRecord):
        """Survey matching the record's site, date, collector and owner,
        if exactly one exists."""
        survey_ids = list(
            Survey.objects.filter(
                site_id=instance.site_id,
                date=instance.collection_date,
                collector_user_id=instance.collector_user_id,
                owner_id=instance.owner_id
            ).values_list('id', flat=True)[:2]
        )
        return survey_ids[0] if len(survey_ids) == 1 else None

    @staticmethod
    def _chem_record_data(instance: BiologicalCollectionRecord):
        chem_record_data = {}
        chem_records = ChemicalRecord.objects.filter(
            survey__site_id=instance.site_id,
            survey__date=instance.collection_date
        ).order_by('chem__chem_code', 'pk').distinct(
            'chem__chem_code'
        ).values_list('chem__chem_code', 'value')
        for chem_code, value in chem_records:
            chem_record_data[chem_code.upper()] = value
        return chem_record_data

    @staticmethod
    def _fbis_chem_value(instance: BiologicalCollectionRecord, chem_id):
        values = list(
            ChemicalRecord.objects.filter(
                chem_id=chem_id,
                survey__site_id=instance.site_id,
                survey__date=instance.collection_date
            ).order_by('pk').values_list('value', flat=True)[:1]
        )
        return values[0] if values else '-'

    @staticmethod
    def _division_name(taxonomy: Taxonomy):
        division = taxonomy.taxongroup_set.filter(
            category__icontains='division'
        ).first()
        return division.name if division else None

    def to_representation(self, instance: BiologicalCollectionRecord):
        result = super(
            BioCollectionOneRowSerializer, self).to_representation(
            instance)

        # Read-only: exporting must not create surveys.
        survey_id = instance.survey_id
        if not survey_id:
            survey_id = self._memo(
                'survey_lookup',
                survey_lookup_key(instance),
                lambda: self._find_survey_id(instance)
            )

        if 'chem_records_cached' not in self.context:
            self.context['chem_records_cached'] = {}
        if 'header' not in self.context or not self.context['header']:
            self.context['header'] = list(result.keys())
        if 'show_link' in self.context and self.context['show_link']:
            self.context['header'] = ['Link'] + self.context['header']
        header = self.context['header']

        is_algae = False
        if instance.module_group:
            is_algae = 'algae' in instance.module_group.name.lower()

        if is_algae:
            algae_keys = [
                'Curation process',
                'Biomass Indicator: Chl A',
                'Biomass Indicator: AFDM',
                'Autotrophic Index (AI)',
            ]

            algae_data = None
            if survey_id:
                algae_data = self._memo(
                    'algae',
                    survey_id,
                    lambda: AlgaeData.objects.filter(
                        survey_id=survey_id
                    ).order_by('pk').first()
                )

            for algae_key in algae_keys:
                if algae_key not in header:
                    header.append(algae_key)
                if algae_data:
                    if algae_key == 'Curation process':
                        result[algae_key] = algae_data.curation_process
                    elif algae_key == 'Biomass Indicator: Chl A':
                        result[algae_key] = algae_data.indicator_chl_a
                    elif algae_key == 'Biomass Indicator: AFDM':
                        result[algae_key] = algae_data.indicator_afdm
                    elif algae_key == 'Autotrophic Index (AI)':
                        result[algae_key] = algae_data.ai

        # FBIS ONLY
        if is_fbis(self.context):
            for survey_data_key, survey_data in fbis_survey_data(
                    self.context).items():
                if survey_data_key not in header:
                    header.append(survey_data_key)
                if not survey_data:
                    continue
                sdv_data = None
                if survey_id:
                    sdv_data = self._memo(
                        'survey_data_values',
                        (survey_id, survey_data.id),
                        lambda: SurveyDataValue.objects.filter(
                            survey_id=survey_id,
                            survey_data=survey_data
                        ).order_by('pk').values_list(
                            'survey_data_option__option', flat=True
                        ).first()
                    )
                result[survey_data_key] = sdv_data

            for chem_id, chemical_unit in fbis_chemical_units(self.context):
                if chemical_unit not in header:
                    header.append(chemical_unit)
                chem_data = self._memo(
                    'fbis_chem_values',
                    (instance.site_id, instance.collection_date, chem_id),
                    lambda: self._fbis_chem_value(instance, chem_id)
                )
                if chem_data:
                    result[chemical_unit] = chem_data

        else:
            chem_record_data = self._memo(
                'chem_records_cached',
                (instance.site_id, instance.collection_date),
                lambda: self._chem_record_data(instance)
            )
            for chem_code in chem_record_data:
                if chem_code not in header:
                    header.append(chem_code)
            result.update(chem_record_data)

        # Taxon attribute
        taxon_group = instance.module_group

        if TEMPLATE_HEADER_KEYS in self.context and self.context[TEMPLATE_HEADER_KEYS]:

            template_headers = list(self.context[TEMPLATE_HEADER_KEYS])
            self.context.setdefault('added_headers', set())

            additional_data = None
            if instance.additional_data:
                additional_data = instance.additional_data
                if isinstance(additional_data, str):
                    try:
                        additional_data = json.loads(additional_data)
                    except Exception:
                        additional_data = {}

            for tpl_header in template_headers:
                norm = tpl_header.strip().lower().replace(' ', '_')

                if norm == 'author(s)':
                    continue

                value = self._get_additional_value(
                    additional_data, tpl_header
                )

                if tpl_header == PARK_OR_MPA_NAME:
                    if not self._has_value(value) and instance.source_collection == 'gbif':
                        sp = self._get_sanparks_mpa_value(instance)
                        if self._has_value(sp) and sp != '-':
                            value = sp
                    if (not self._has_value(value)) and instance.site and (
                            instance.site.owner_id or instance.site.creator_id):
                        value = instance.site.name

                if not self._has_value(value):
                    continue

                if tpl_header not in header:
                    header.append(tpl_header)
                self.context['added_headers'].add(tpl_header)
                result[tpl_header] = value

                if tpl_header == PARK_OR_MPA_NAME:
                    result.pop('site_description', None)
                    if 'site_description' in header:
                        header.remove('site_description')

        for grp in geocontext_groups(self.context):
            if grp['name'] not in header:
                header.append(grp['name'])
            result[grp['name']] = self.spatial_data(instance, grp['id'])

        if 'show_link' in self.context and self.context['show_link']:
            result['Link'] = ''.join(
                [Site.objects.get_current().domain,
                 reverse(
                     'admin:{}_{}_change'.format(
                         instance._meta.app_label,
                         instance._meta.model_name
                     ),
                     args=[instance.id]
                 )])

        # Check DIVISION
        division_name = self._memo(
            'division',
            instance.taxonomy.id,
            lambda: self._division_name(instance.taxonomy)
        )
        if division_name:
            division_key = 'Division'
            if division_key not in header:
                header.append(division_key)
            result[division_key] = division_name

        if taxon_group:
            taxon_extra_attributes = self._memo(
                'taxon_extra_attributes',
                taxon_group.id,
                lambda: list(TaxonExtraAttribute.objects.filter(
                    taxon_group=taxon_group
                ))
            )
            for taxon_extra_attribute in taxon_extra_attributes:
                taxon_attribute_name = taxon_extra_attribute.name
                if taxon_attribute_name.lower().strip() == 'cites listing':
                    continue
                key_title = taxon_attribute_name.lower().replace(' ', '_')
                cache_key = '{id}-{extra_id}'.format(
                    id=instance.taxonomy.id,
                    extra_id=taxon_extra_attribute.id
                )
                if key_title not in header:
                    header.append(key_title)
                taxon_attribute_data = self.get_context_cache(
                    cache_key,
                    taxon_attribute_name
                )
                try:
                    if not taxon_attribute_data:
                        if (
                                taxon_attribute_name in
                                instance.taxonomy.additional_data
                        ):
                            taxon_attribute_data = (
                                instance.taxonomy.additional_data
                                [taxon_attribute_name]
                            )
                        self.set_context_cache(
                            cache_key,
                            taxon_attribute_name,
                            taxon_attribute_data
                        )
                except (TypeError, KeyError):
                    pass

                result[key_title] = (
                    taxon_attribute_data
                    if taxon_attribute_data
                    else '-'
                )

        # For gbif
        if instance.source_collection == 'gbif':
            key = 'CoL ID'
            if key not in header:
                header.append(key)
            result[key] = instance.taxonomy.col_id

        # For VM
        if instance.source_collection == 'virtual_museum':
            key = 'VM-Number'
            if key not in header:
                header.append(key)
            result[key] = self.get_upstream_id(instance)

        return result


class BioCollectionGeojsonSerializer(GeoFeatureModelSerializer):
    geometry = GeometrySerializerMethodField()
    location_site = serializers.SerializerMethodField()
    species_name = serializers.SerializerMethodField()
    notes = serializers.SerializerMethodField()
    category = serializers.SerializerMethodField()
    date = serializers.SerializerMethodField()
    collector = serializers.SerializerMethodField()

    def get_location_site(self, obj):
        if obj.site:
            return obj.site.name
        return ''

    def get_species_name(self, obj):
        return obj.original_species_name

    def get_notes(self, obj):
        return obj.notes

    def get_category(self, obj):
        return obj.category

    def get_date(self, obj):
        if obj.collection_date:
            return obj.collection_date.isoformat().split('T')[0]

    def get_collector(self, obj):
        return obj.collector

    def get_geometry(self, obj):
        if obj.site:
            return obj.site.get_geometry()
        return None

    class Meta:
        model = BiologicalCollectionRecord
        geo_field = 'geometry'
        fields = [
            'location_site', 'species_name', 'notes', 'category',
            'date', 'collector']

    def to_representation(self, instance):
        result = super(
            BioCollectionGeojsonSerializer, self).to_representation(
            instance)
        try:
            taxonomy = TaxonExportSerializer(instance.taxonomy).data
            result['properties'].update(taxonomy)
        except KeyError:
            pass
        return result


class BioCollectionOneRowWithLinkSerialier(BioCollectionOneRowSerializer):
    pass


class BioCollectionBasicSerializer(serializers.ModelSerializer):
    class Meta:
        model = BiologicalCollectionRecord
