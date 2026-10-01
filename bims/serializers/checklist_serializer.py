from datetime import datetime

from django.contrib.contenttypes.models import ContentType
from django.db.models import F
from preferences import preferences
from rest_framework import serializers

from bims.models import TaxonGroupTaxonomy, LocationContext, CertaintyHierarchy
from bims.models.taxonomy import Taxonomy
from bims.serializers.bio_collection_serializer import SerializerContextCache
from bims.models.biological_collection_record import BiologicalCollectionRecord
from bims.models.dataset import Dataset
from bims.utils.site_code import SANPARK_PARK_KEY

HIERARCHY_OF_CERTAINTY = [
    "High",
    "Herbarium verified",
    "Expert verified",
    "Photo ID",
    "Medium",
    "Unverified",
    "Low",
    "Misidentification",
    "Unknown",
]

HIERARCHY_OF_ACCURACY_OF_COORDINATES = [
    "GPS point",
    "Nbal Centroid",
    "Section Centroid",
    "Park Centroid",
    "QDS Centroid",
    "Inaccurate GPS location",
    "Verified place name",
    "Historical range map",
    "Unverified place name",
    "Unknown"
]


def apply_gbif_record_threshold(queryset, threshold=10000, limit=100):
    """
    Apply the GBIF record threshold rule to a queryset.

    If the queryset has more than `threshold` records, limit it to the first `limit` records.
    This is used to avoid performance issues when calculating aggregate statistics
    on large datasets.

    Args:
        queryset: Django QuerySet to potentially limit
        threshold: Maximum number of records before limiting (default: 10000)
        limit: Number of records to keep if threshold is exceeded (default: 100)

    Returns:
        QuerySet: Either the original queryset or a limited version
    """
    total_count = queryset.count()
    if total_count > threshold:
        return queryset[:limit]
    return queryset


def _norm_confidence(s):
    return (s or "").strip().lower()


def get_dataset_occurrences(occurrences):
    rows = occurrences.values_list('additional_data__datasetKey', 'dataset_key')
    dataset_keys_set = set()
    for json_key, field_key in rows:
        if isinstance(json_key, (list, tuple)):
            dataset_keys_set.update(k for k in json_key if k)
        elif json_key:
            dataset_keys_set.add(json_key)
        if field_key:
            dataset_keys_set.add(field_key)
    dataset_keys = list(dataset_keys_set)

    if dataset_keys:
        dataset_keys = list(filter(None, set(dataset_keys)))
        datasets = Dataset.objects.filter(
            uuid__in=dataset_keys
        )
        dataset_names = []
        if datasets.exists():
            for dataset in datasets:
                dataset_names.append(
                    dataset.abbreviation if dataset.abbreviation else dataset.name
                )
                dataset_keys.remove(str(dataset.uuid))
        if dataset_keys:
            dataset_names.extend(dataset_keys)
        if len(dataset_names) > 0:
            return list(set(dataset_names))
    dataset_names = list(occurrences.values_list(
        'additional_data__datasetName',
        flat=True
    ))
    dataset_names = [name for name in dataset_names if name is not None]
    if dataset_names:
        return list(set(dataset_names))
    return []


class ChecklistBaseSerializer(SerializerContextCache):
    id = serializers.SerializerMethodField()

    def get_id(self, obj: Taxonomy) -> str:
        if obj.fada_id:
            return f'fada:{obj.fada_id}'
        prefix = (
            getattr(preferences.SiteSetting, 'default_data_source', '') or ''
        ).lower()
        return f'{prefix}:{obj.id}' if prefix else str(obj.id)

    def get_bio_data(self, obj: Taxonomy):
        if not hasattr(self, '_bio_data_cache'):
            self._bio_data_cache = {}
        bio_context = self.context.get(
            'collection_records', BiologicalCollectionRecord.objects.all())
        if obj.id not in self._bio_data_cache:
            record_ids_by_taxon = self.context.get('record_ids_by_taxon')
            if record_ids_by_taxon is not None:
                bio_records = BiologicalCollectionRecord.objects.filter(
                    id__in=record_ids_by_taxon.get(obj.id, [])
                )
            else:
                bio_records = bio_context.filter(taxonomy=obj)
            self._bio_data_cache[obj.id] = bio_records
        return self._bio_data_cache[obj.id]

    def clean_text(self, text):
        if text is None:
            return ''
        # Remove or replace unwanted characters
        cleaned_text = text.replace('\n', ' ').replace('\r', ' ')
        return cleaned_text

    def get_sources(self, obj: Taxonomy):
        bio = self.get_bio_data(obj)
        if not bio.exists():
            return ''
        sources = []
        if bio.filter(source_collection__iexact='gbif').exists():
            sources = get_dataset_occurrences(bio)
        bio = bio.distinct('source_reference')
        try:
            for collection in bio:
                try:
                    if (
                        collection.source_reference and
                        str(collection.source_reference) != 'Global Biodiversity Information Facility (GBIF)'
                    ):
                        if collection.additional_data and 'Citation' in collection.additional_data:
                            citation_name = collection.additional_data['Citation']
                            if not Dataset.objects.filter(citation=citation_name).exists():
                                Dataset.objects.create(
                                    citation=citation_name,
                                    abbreviation=citation_name,
                                    name=str(collection.source_reference)
                                )
                            else:
                                dataset = Dataset.objects.filter(citation=citation_name).first()
                                if dataset.abbreviation != citation_name:
                                    citation_name = dataset.abbreviation
                            sources.append(
                                citation_name
                            )
                        else:
                            sources.append(
                                str(collection.source_reference)
                            )
                except ContentType.DoesNotExist:
                    continue
        except TypeError:
            pass
        if sources:
            return ', '.join(set(filter(None, sources)))
        return '-'


class ChecklistPDFSerializer(ChecklistBaseSerializer):

    common_name = serializers.SerializerMethodField()
    threat_status = serializers.SerializerMethodField()
    sources = serializers.SerializerMethodField()
    scientific_name = serializers.SerializerMethodField()
    occurrence_records = serializers.SerializerMethodField()

    def get_occurrence_records(self, obj: Taxonomy):
        bio = self.get_bio_data(obj)
        if not bio.exists():
            return 0
        return bio.count()

    def get_scientific_name(self, obj: Taxonomy):
        from bims.utils.taxonomy import (
            canonical_with_subgenus, build_name_with_addendum
        )
        subgenus_str = obj.sub_genus_name or (
            obj.subgenus.canonical_name if obj.subgenus_id else ''
        )
        canonical = canonical_with_subgenus(
            obj.canonical_name, obj.genus_name, subgenus_str
        )
        if obj.addendum:
            sci = build_name_with_addendum(
                canonical, obj.author, obj.addendum, abbreviate=True
            )
            return self.clean_text(sci)
        sci = obj.scientific_name or ''
        if canonical and canonical not in sci:
            author = obj.author or ''
            sci = f'{canonical} {author}'.strip() if author else canonical
        return self.clean_text(sci)

    def get_common_name(self, obj: Taxonomy):
        vernacular_names = list(
            obj.vernacular_names.filter(
                language__istartswith='en'
            ).values_list('name', flat=True))
        if len(vernacular_names) == 0:
            return ''
        else:
            return vernacular_names[0]

    def get_threat_status(self, obj: Taxonomy):
        if obj.iucn_status:
            return obj.iucn_status.category
        return 'NE'

    class Meta:
        model = Taxonomy
        fields = [
            'id',
            'scientific_name',
            'common_name',
            'threat_status',
            'sources',
            'occurrence_records'
        ]


class ChecklistSerializer(ChecklistBaseSerializer):
    """
    Serializer for checklist
    """
    kingdom = serializers.SerializerMethodField()
    phylum = serializers.SerializerMethodField()
    class_name = serializers.SerializerMethodField()
    family = serializers.SerializerMethodField()
    order = serializers.SerializerMethodField()
    scientific_name = serializers.SerializerMethodField()
    synonyms = serializers.SerializerMethodField()
    common_name = serializers.SerializerMethodField()
    most_recent_record = serializers.SerializerMethodField()
    origin = serializers.SerializerMethodField()
    invasion = serializers.SerializerMethodField()
    endemism = serializers.SerializerMethodField()
    global_conservation_status = serializers.SerializerMethodField()
    national_conservation_status = serializers.SerializerMethodField()
    sources = serializers.SerializerMethodField()
    cites_listing = serializers.SerializerMethodField()
    certainty = serializers.SerializerMethodField()
    accuracy_of_coordinates = serializers.SerializerMethodField()
    gbif_coordinate_uncertainty_m = serializers.SerializerMethodField()
    gbif_coordinate_precision = serializers.SerializerMethodField()
    park_or_mpa_name = serializers.SerializerMethodField()
    creation_date = serializers.SerializerMethodField()
    occurrence_records = serializers.SerializerMethodField()
    tops = serializers.SerializerMethodField()
    include_or_exclude = serializers.SerializerMethodField()
    exclude_source = serializers.SerializerMethodField()

    def taxon_name_by_rank(
            self,
            obj: Taxonomy,
            rank_identifier: str):
        taxon_name = self.get_context_cache(
            rank_identifier,
            obj.id
        )
        if taxon_name:
            return taxon_name
        taxon_name = obj.__getattribute__(rank_identifier)
        if taxon_name:
            self.set_context_cache(
                rank_identifier,
                obj.id,
                taxon_name
            )
            return taxon_name
        return '-'

    def get_creation_date(self, obj: Taxonomy):
        today = datetime.today()
        return today.strftime('%d/%m/%Y')

    def get_taxon_group_taxon_data(self, obj: Taxonomy):
        taxon_group_id = self.context.get('taxon_group_id', None)
        if not taxon_group_id:
            return None
        if not hasattr(self, '_taxon_group_cache'):
            self._taxon_group_cache = {}
        if obj.id not in self._taxon_group_cache:
            taxon_group_taxon = TaxonGroupTaxonomy.objects.filter(
                taxonomy=obj,
                taxongroup_id=taxon_group_id,
                is_validated=True
            ).first()
            if taxon_group_taxon:
                self._taxon_group_cache[obj.id] = taxon_group_taxon
            else:
                return None
        return self._taxon_group_cache[obj.id]

    def get_kingdom(self, obj: Taxonomy):
        return self.taxon_name_by_rank(
            obj,
            'kingdom_name'
        )
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

    def get_scientific_name(self, obj: Taxonomy):
        if obj.addendum:
            from bims.utils.taxonomy import build_name_with_addendum
            return build_name_with_addendum(
                obj.canonical_name or obj.scientific_name,
                obj.author,
                obj.addendum,
                abbreviate=True,
            )
        return obj.scientific_name or ''

    def get_synonyms(self, obj: Taxonomy):
        synonyms = Taxonomy.objects.filter(
            accepted_taxonomy=obj
        ).values_list('scientific_name', flat=True)
        return ','.join(list(synonyms))

    def get_common_name(self, obj: Taxonomy):
        vernacular_names = list(
            obj.vernacular_names.filter(
                language__istartswith='en').values_list('name', flat=True))
        if len(vernacular_names) == 0:
            return ''
        else:
            return vernacular_names[0]

    def get_most_recent_record(self, obj: Taxonomy):
        bio = self.get_bio_data(obj)
        if not bio.exists():
            return ''
        return bio.order_by('collection_date').last().collection_date.year

    def get_occurrence_records(self, obj: Taxonomy):
        bio = self.get_bio_data(obj)
        if not bio.exists():
            return 0
        return bio.count()

    def get_certainty_hierarchy(self):
        if not hasattr(self, '_certainty_hierarchy_cache'):
            db_values = list(
                CertaintyHierarchy.objects.order_by('order').values_list('name', flat=True)
            )
            self._certainty_hierarchy_cache = db_values or HIERARCHY_OF_CERTAINTY
        return self._certainty_hierarchy_cache

    def get_certainty(self, obj: Taxonomy):
        bio = self.get_bio_data(obj)
        if not bio.exists():
            return ''

        bio_with_certainty = bio.exclude(
            certainty_of_identification=''
        )
        if not bio_with_certainty.exists():
            return ''

        certainties_lc = {
            _norm_confidence(c)
            for c in bio_with_certainty.values_list('certainty_of_identification', flat=True)
            if c is not None and c.strip() != ''
        }

        for canonical in self.get_certainty_hierarchy():
            if _norm_confidence(canonical) in certainties_lc:
                return canonical

        return 'Unknown'

    def get_accuracy_of_coordinates(self, obj: Taxonomy):
        bio = self.get_bio_data(obj)
        if not bio.exists():
            return ''

        qs = (
            bio
            .exclude(site__isnull=True)
            .exclude(site__accuracy_of_coordinates__isnull=True)
            .exclude(site__accuracy_of_coordinates__exact='')
        )

        accuracies_lc = {
            _norm_confidence(val)
            for val in qs.values_list('site__accuracy_of_coordinates', flat=True).distinct()
        }

        for canonical in HIERARCHY_OF_ACCURACY_OF_COORDINATES:
            if _norm_confidence(canonical) in accuracies_lc:
                return canonical

        return 'Unknown'

    def get_gbif_coordinate_uncertainty_m(self, obj: Taxonomy):
        """
        Get the lowest coordinate uncertainty in meters from GBIF sources.
        Rule: If total records > 10,000, use only first 100 records.
        """
        from django.db.models import Min

        bio = self.get_bio_data(obj)

        gbif_records = bio.filter(
            coordinate_uncertainty_in_meters__isnull=False
        )

        if not gbif_records.exists():
            return ''

        gbif_records = apply_gbif_record_threshold(gbif_records)

        result = gbif_records.aggregate(
            min_uncertainty=Min('coordinate_uncertainty_in_meters')
        )

        min_uncertainty = result.get('min_uncertainty')
        if min_uncertainty is not None:
            return f"{min_uncertainty:.2f}"
        return ''

    def get_gbif_coordinate_precision(self, obj: Taxonomy):
        """
        Get the highest coordinate precision from GBIF sources.
        Rule: If total records > 10,000, use only first 100 records.
        Higher precision = smaller decimal value (e.g., 0.00001 is more precise than 0.01667)
        """
        from django.db.models import Min

        bio = self.get_bio_data(obj)

        gbif_records = bio.filter(
            coordinate_precision__isnull=False
        )

        if not gbif_records.exists():
            return ''

        gbif_records = apply_gbif_record_threshold(gbif_records)

        result = gbif_records.aggregate(
            max_precision=Min('coordinate_precision')
        )

        max_precision = result.get('max_precision')
        if max_precision is not None:
            return f"{max_precision:.6f}"
        return ''

    def get_park_or_mpa_name(self, obj: Taxonomy):
        bio = self.get_bio_data(obj)
        if not bio.exists():
            return '-'

        site_ids = (
            bio
            .exclude(site__isnull=True)
            .values_list('site_id', flat=True)
            .distinct()
        )
        if not site_ids:
            return '-'

        park_names = (
            LocationContext.objects
            .filter(
                site_id__in=site_ids,
                group__name__icontains=SANPARK_PARK_KEY
            )
            .values_list('value', flat=True)
            .distinct()
        )

        park_names = [name.strip() for name in park_names if name and name.strip()]
        if park_names:
            return ','.join(sorted(set(park_names)))
        return '-'

    def get_origin(self, obj: Taxonomy):
        taxon_group_taxon = self.get_taxon_group_taxon_data(obj)
        if not taxon_group_taxon:
            taxon_group_taxon = obj
        origin = taxon_group_taxon.origin
        return origin.category if origin else ''

    def get_invasion(self, obj: Taxonomy):
        if obj.invasion:
            return obj.invasion.category
        return ''

    def get_tops(self, obj: Taxonomy):
        addl = getattr(obj, 'additional_data', None) or {}
        return addl.get('tops') or addl.get('TOPS') or ''

    def get_endemism(self, obj: Taxonomy):
        taxon_group_taxon = self.get_taxon_group_taxon_data(obj)
        if not taxon_group_taxon:
            taxon_group_taxon = obj
        return taxon_group_taxon.endemism.name if taxon_group_taxon.endemism else ''

    def get_global_conservation_status(self, obj):
        if obj.iucn_status:
            return obj.iucn_status.category
        return 'NE'

    def get_national_conservation_status(self, obj):
        if obj.national_conservation_status:
            return obj.national_conservation_status.category
        return ''

    def get_cites_listing(self, obj: Taxonomy):
        return obj.cites_listing

    def get_fields(self):
        fields = super().get_fields()
        if not preferences.SiteSetting.project_name == 'sanparks':
            fields.pop('park_or_mpa_name', None)
            fields.pop('tops', None)
            fields.pop('include_or_exclude', None)
            fields.pop('exclude_source', None)
        return fields

    def get_include_or_exclude(self, obj: Taxonomy):
        bio = self.get_bio_data(obj)
        if not bio.exists():
            return ''

        qs = (
            bio
            .exclude(source_collection__iexact='gbif')
            .annotate(include_or_exclude=F('additional_data__Include/Exclude'))
            .values_list('include_or_exclude', flat=True)
        )

        values = {
            (v or '').strip()
            for v in qs
            if v and str(v).strip()
        }
        if not values:
            return ''

        if 'Exclude' in values:
            return 'Exclude'
        if 'Include' in values:
            return 'Include'
        return ''

    def get_exclude_source(self, obj: Taxonomy):
        bio = self.get_bio_data(obj)
        if not bio.exists():
            return ''

        exclude_qs = (
            bio
            .exclude(source_collection__iexact='gbif')
            .annotate(include_or_exclude=F('additional_data__Include/Exclude'))
            .filter(include_or_exclude='Exclude')
        )

        if not exclude_qs.exists():
            return ''

        sources = []
        for collection in exclude_qs:
            source_ref = collection.source_reference
            if not source_ref:
                continue

            if str(source_ref) == 'Global Biodiversity Information Facility (GBIF)':
                continue

            additional_data = collection.additional_data or {}
            citation_name = additional_data.get('Citation')

            if citation_name:
                dataset = Dataset.objects.filter(citation=citation_name).first()
                if not dataset:
                    Dataset.objects.create(
                        citation=citation_name,
                        abbreviation=citation_name,
                        name=str(source_ref),
                    )
                else:
                    if dataset.abbreviation != citation_name:
                        citation_name = dataset.abbreviation
                sources.append(citation_name)
            else:
                sources.append(str(source_ref))

        sources = [s for s in sources if s]
        if not sources:
            return ''

        return ', '.join(sorted(set(sources)))

    def to_representation(self, instance):
        ret = super().to_representation(instance)
        invasion_label = preferences.SiteSetting.invasion_label or 'Invasion'
        if invasion_label != 'invasion' and 'invasion' in ret:
            ret[invasion_label] = ret.pop('invasion')
        return ret

    class Meta:
        model = Taxonomy
        fields = [
            'id',
            'kingdom',
            'phylum',
            'class_name',
            'order',
            'family',
            'scientific_name',
            'synonyms',
            'rank',
            'common_name',
            'most_recent_record',
            'origin',
            'endemism',
            'invasion',
            'global_conservation_status',
            'national_conservation_status',
            'sources',
            'cites_listing',
            'tops',
            'certainty',
            'accuracy_of_coordinates',
            'gbif_coordinate_uncertainty_m',
            'gbif_coordinate_precision',
            'park_or_mpa_name',
            'creation_date',
            'occurrence_records',
            'include_or_exclude',
            'exclude_source'
        ]

