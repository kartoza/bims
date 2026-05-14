import json
import re
from collections.abc import Iterable

from preferences import preferences
from django.contrib.auth import get_user_model
from django.contrib.sites.models import Site
from rest_framework import serializers
from taggit.models import Tag

from bims.models import Taxonomy, BiologicalCollectionRecord, TaxonomyUpdateProposal, TaxonomyUpdateReviewer
from bims.models.iucn_status import IUCNStatus
from bims.models.taxon_group import TaxonGroup, OccurrenceUploadTemplate
from bims.models.taxon_group_taxonomy import TaxonGroupTaxonomy


class TaxonSerializer(serializers.ModelSerializer):
    """
    Serializer for taxon collection model.
    """
    iucn_status_sensitive = serializers.SerializerMethodField()
    iucn_status_name = serializers.SerializerMethodField()
    iucn_status_full_name = serializers.SerializerMethodField()
    iucn_status_colour = serializers.SerializerMethodField()
    iucn_url = serializers.SerializerMethodField()
    record_type = serializers.SerializerMethodField()
    taxon_group = serializers.SerializerMethodField()
    origin_name = serializers.SerializerMethodField()
    endemism_name = serializers.SerializerMethodField()
    common_name = serializers.SerializerMethodField()
    total_records = serializers.SerializerMethodField()
    accepted_taxonomy_name = serializers.SerializerMethodField()
    tag_list = serializers.SerializerMethodField()
    validated = serializers.SerializerMethodField()
    scientific_name = serializers.SerializerMethodField()
    canonical_name = serializers.SerializerMethodField()
    rank = serializers.SerializerMethodField()
    can_be_validated = serializers.SerializerMethodField()
    family = serializers.SerializerMethodField()
    genus = serializers.SerializerMethodField()
    species = serializers.SerializerMethodField()
    biographic_distributions = serializers.SerializerMethodField()
    author = serializers.SerializerMethodField()
    DT_RowId = serializers.SerializerMethodField()
    proposal_id = serializers.SerializerMethodField()
    can_edit = serializers.SerializerMethodField()
    taxonomic_status = serializers.SerializerMethodField()
    fada_id = serializers.SerializerMethodField()
    children_count = serializers.SerializerMethodField()
    other_group_count = serializers.SerializerMethodField()

    def get_fada_id(self, obj):
        """Only return fada_id for FADA sites."""
        from bims.templatetags import is_fada_site
        if is_fada_site():
            return self.get_proposed_or_current(obj, 'fada_id')
        return None

    def _format_distributions(self, qs):
        qs = qs.order_by('name')
        bio_tags = list(qs.values('name', 'doubtful'))
        return ", ".join(
            [
                f'{re.sub(r"\s*/\s*", "/", " ".join((bio_tag["name"] or "").split())).strip()}'
                f'{"(?)" if bio_tag["doubtful"] else ""}'
                for bio_tag in bio_tags
            ]
        )

    def _normalize_tag_token(self, s: str) -> str:
        s = ' '.join((s or '').split())
        s = re.sub(r'\s*/\s*', '/', s)
        return s.lower()

    def _tags_key_set(self, tags_qs):
        return {self._normalize_tag_token(t.name) for t in tags_qs.all()}

    def _format_tags(self, tags_qs):
        tag_info = []
        cached_tag_groups = self.context.setdefault('tag_groups', {})

        for tag in tags_qs.order_by('tag_groups__order', 'name'):
            if tag.id in cached_tag_groups:
                label = cached_tag_groups[tag.id]
            else:
                tg_qs = tag.tag_groups.all()
                disp_name = re.sub(r'\s*/\s*', '/', ' '.join((tag.name or '').split())).strip()
                if tg_qs.exists():
                    group = tg_qs.first()
                    label = f"{disp_name} ({group.colour})"
                else:
                    label = disp_name
                cached_tag_groups[tag.id] = label
            tag_info.append(label)

        seen = set()
        ordered = []
        for lbl in tag_info:
            key = self._normalize_tag_token(lbl)
            if key in seen:
                continue
            seen.add(key)
            ordered.append(lbl)

        return ", ".join(ordered)

    def get_iucn_url(self, obj: Taxonomy):
        if obj.iucn_data:
            try:
                iucn_data = json.loads(obj.iucn_data.replace('\'', '"'))
                return iucn_data.get('url', '')
            except json.decoder.JSONDecodeError:
                return ''
        return ''

    def get_taxonomic_status(self, obj: Taxonomy):
        return self.get_proposed_or_current(obj, 'taxonomic_status')

    def taxonomy_obj(self, obj):
        if isinstance(obj, TaxonomyUpdateProposal):
            return obj.original_taxonomy
        return obj

    def get_author(self, obj: Taxonomy):
        return self.get_proposed_or_current(obj, 'author')

    def get_DT_RowId(self, obj: Taxonomy):
        return f'row_{obj.id}'

    def get_biographic_distributions(self, obj: Taxonomy):
        original_str = self._format_distributions(obj.biographic_distributions.all())
        if self.context.get('validated', False):
            return original_str

        proposal = self.get_pending_proposal(obj)
        if not proposal:
            return original_str

        proposed_qs = proposal.biographic_distributions.all()
        proposed_str = self._format_distributions(proposed_qs)

        orig_set = {
            (self._normalize_tag_token(x.name), bool(x.doubtful))
            for x in obj.biographic_distributions.all()
        }
        prop_set = {
            (self._normalize_tag_token(x.name), bool(x.doubtful))
            for x in proposed_qs
        }

        if orig_set != prop_set:
            return f"{original_str if original_str else '-'} → {proposed_str}"
        return original_str

    def _find_ancestor_by_rank(self, obj: Taxonomy, target_rank: str, max_depth: int = 10):
        """Traverse parent hierarchy to find an ancestor with the specified rank."""
        current = obj.parent
        depth = 0
        while current and depth < max_depth:
            if current.rank and current.rank.upper() == target_rank.upper():
                return current
            current = current.parent
            depth += 1
        return None

    def _get_genus_name(self, obj: Taxonomy):
        """Get genus name from hierarchy or hierarchical_data."""
        if obj.hierarchical_data and 'genus_name' in obj.hierarchical_data:
            return obj.hierarchical_data['genus_name']
        genus_ancestor = self._find_ancestor_by_rank(obj, 'GENUS')
        if genus_ancestor:
            return genus_ancestor.canonical_name
        return obj.genus_name

    def _get_subgenus_name(self, obj: Taxonomy):
        """Get subgenus name from hierarchy."""
        if obj.rank and obj.rank.upper() == 'SUBGENUS':
            return obj.canonical_name
        if obj.subgenus:
            return obj.subgenus.canonical_name
        subgenus_ancestor = self._find_ancestor_by_rank(obj, 'SUBGENUS')
        if subgenus_ancestor:
            return subgenus_ancestor.canonical_name
        return None

    def get_genus(self, obj: Taxonomy):
        validated = self.context.get('validated', False)
        if not validated:
            return obj.genus_name
        if obj.hierarchical_data and 'genus_name' in obj.hierarchical_data:
            return obj.hierarchical_data['genus_name']
        return obj.genus_name

    def get_family(self, obj: Taxonomy):
        validated = self.context.get('validated', False)
        if not validated:
            return self.get_proposed_or_current(obj, 'family_name')
        if obj.hierarchical_data and 'family_name' in obj.hierarchical_data:
            return obj.hierarchical_data['family_name']
        return obj.family_name

    def get_species(self, obj: Taxonomy):
        validated = self.context.get('validated', False)
        species_name = ''
        if not validated:
            species_name = obj.species_name
        if not species_name and obj.hierarchical_data and 'species_name' in obj.hierarchical_data:
            species_name = obj.hierarchical_data['species_name']
        if not species_name:
            species_name = obj.species_name

        if species_name:
            genus_name = self.get_genus(obj)
            if genus_name in species_name:
                try:
                    species_name = (
                        species_name.split(genus_name)[-1].strip()
                    )
                except ValueError:
                    pass

        return species_name

    PUBLIC_EXCLUDED_FIELDS = {
        'can_be_validated',
        'taxon_group',
        'DT_RowId',
        'proposal_id',
        'can_edit',
        'children_count',
        'other_group_count',
        'rejected',
        'ready_for_validation',
        'validation_message',
        'end_embargo_date',
        'verified',
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.taxonomy_proposals = {}
        self.context['tag_groups'] = {}

        if isinstance(self.instance, Iterable):
            self.taxonomy_proposals = {
                obj.original_taxonomy_id: obj for obj in TaxonomyUpdateProposal.objects.filter(
                    original_taxonomy__in=[obj.id for obj in self.instance], status='pending'
                )}

    def to_representation(self, instance):
        data = super().to_representation(instance)
        if self.context.get('is_public'):
            for field in self.PUBLIC_EXCLUDED_FIELDS:
                data.pop(field, None)
        return data

    def get_pending_proposal(self, obj):
        return self.taxonomy_proposals.get(obj.id)

    def get_proposal_id(self, obj):
        validated = self.context.get('validated', False)
        if not validated:
            proposal = self.get_pending_proposal(obj)
            if proposal:
                return proposal.id
        return None

    def get_proposed_or_current(self, obj, field, original_value='', attr = None):
        validated = self.context.get('validated', False)
        if not original_value:
            original_data = getattr(obj, field)
            original_value = str(original_data if original_data else '')
        if validated:
            return original_value
        proposal = self.get_pending_proposal(obj)
        proposal_value = ''
        if proposal:
            proposal_data = getattr(proposal, field)
            proposal_value = str((getattr(proposal_data, attr) if attr else proposal_data) if proposal_data else '')
        return (
            f"{original_value if original_value else '-'} → "
            f"{proposal_value}"
            if proposal and original_value.strip() != proposal_value.strip()
            else original_value
        )

    def get_origin_name(self, obj):
        origin_name = obj.origin.category if obj.origin else 'Unknown'
        proposal = self.get_pending_proposal(obj)
        if proposal:
            proposal_origin = getattr(proposal, 'origin', None)
            proposal_value = proposal_origin.category if proposal_origin else ''
            if proposal_value and proposal_value != origin_name:
                return f"{origin_name} → {proposal_value}"
        return origin_name

    def get_can_edit(self, obj):
        can_edit = self.context.get('can_edit', None)
        if can_edit is None:
            taxon_group_id = self.context.get('taxon_group_id', None)
            user_id = self.context.get('user', None)
            if not user_id or not taxon_group_id:
                return None
            can_edit = (
                TaxonGroup.objects.filter(
                    experts=user_id,
                    id=taxon_group_id
                ).exists() or
                get_user_model().objects.get(id=user_id).is_superuser
            )
            self.context['can_edit'] = can_edit
            return can_edit
        return can_edit

    def get_iucn_status_full_name(self, obj):
        if obj.iucn_status:
            try:
                iucn_status = (
                    dict(IUCNStatus.CATEGORY_CHOICES)[obj.iucn_status.category]
                )
            except KeyError:
                iucn_status = 'Not evaluated'
        else:
            iucn_status = 'Not evaluated'
        proposal = self.get_pending_proposal(obj)
        if proposal:
            try:
                proposal_value = (
                    dict(IUCNStatus.CATEGORY_CHOICES)[str(
                        getattr(proposal, 'iucn_status'))]
                )
                if proposal_value != iucn_status:
                    return (
                        f"{iucn_status} → "
                        f"{proposal_value}"
                    )
            except KeyError:
                pass
        return iucn_status

    def get_scientific_name(self, obj):
        return self.get_proposed_or_current(obj, 'scientific_name')

    def get_endemism_name(self, obj):
        try:
            return self.get_proposed_or_current(
                obj, 'endemism')
        except AttributeError:
            return '-'

    def get_canonical_name(self, obj):
        canonical_name = self.get_proposed_or_current(obj, 'canonical_name')

        if not obj.rank:
            return canonical_name

        current_rank = obj.rank.upper()

        if current_rank == 'SUBGENUS':
            genus_name = self._get_genus_name(obj)
            if genus_name and canonical_name:
                subgenus_name = canonical_name
                if (
                    len(canonical_name.split(' ')) > 1
                    and canonical_name.lower().startswith(genus_name.lower())
                ):
                    subgenus_name = canonical_name[len(genus_name):].strip()
                return f"{genus_name} ({subgenus_name})"
            return canonical_name

        subgenus_name = self._get_subgenus_name(obj)
        if subgenus_name:
            genus_name = self._get_genus_name(obj)
            if genus_name:
                name_parts = canonical_name.split()
                if len(name_parts) > 1:
                    epithet = ' '.join(name_parts[1:])
                    if epithet.lower().startswith(subgenus_name.lower()):
                        epithet = epithet[len(subgenus_name):].strip()
                    return f"{genus_name} ({subgenus_name}) {epithet}"

        return canonical_name

    def get_rank(self, obj):
        return self.get_proposed_or_current(obj, 'rank')

    def get_can_be_validated(self, obj: Taxonomy):
        validated = self.context.get('validated', False)
        if validated:
            return False
        taxon_group_id = self.context.get('taxon_group_id', None)
        user_id = self.context.get('user', None)
        if taxon_group_id:
            can_be_validated = TaxonomyUpdateProposal.objects.filter(
                original_taxonomy=self.taxonomy_obj(obj),
                taxon_group_under_review=taxon_group_id,
                status='pending'
            ).exists()

            # Check if user can validate the taxon
            if can_be_validated and user_id:
                return self.get_can_edit(obj)

            return can_be_validated

    def get_validated(self, obj: Taxonomy):
        taxon_group_id = self.context.get('taxon_group_id', None)
        taxon_group_ids = self.context.get('taxon_group_ids', [])
        if len(taxon_group_ids) == 0 and taxon_group_id:
            taxon_group_ids.append(int(taxon_group_id))
            if TaxonGroup.objects.filter(
                    parent__id=taxon_group_id
            ).exists():
                taxon_group_children = TaxonGroup.objects.get(
                    id=taxon_group_id
                ).get_all_children()
                for children in taxon_group_children:
                    if children.id not in taxon_group_ids:
                        taxon_group_ids.append(children.id)
                self.context['taxon_group_ids'] = taxon_group_ids
        if taxon_group_ids:
            return TaxonGroupTaxonomy.objects.filter(
                taxongroup__in=taxon_group_ids,
                taxonomy=self.taxonomy_obj(obj),
                is_validated=True
            ).exists()
        return obj.validated

    def get_accepted_taxonomy_name(self, obj):
        return self.get_proposed_or_current(obj, 'accepted_taxonomy')

    def get_total_records(self, obj):
        return BiologicalCollectionRecord.objects.filter(
            taxonomy=self.taxonomy_obj(obj)
        ).count()

    def get_common_name(self, obj):
        vernacular_names = list(
            obj.vernacular_names.filter(language__istartswith='en').values_list('name', flat=True))
        if len(vernacular_names) == 0:
            return ''
        else:
            return vernacular_names[0]

    def get_tag_list(self, obj):
        original_str = self._format_tags(obj.tags)
        if self.context.get('validated', False):
            return original_str

        proposal = self.get_pending_proposal(obj)
        if not proposal:
            return original_str

        proposed_str = self._format_tags(proposal.tags)
        if self._tags_key_set(obj.tags) != self._tags_key_set(proposal.tags):
            return f"{original_str if original_str else '-'} → {proposed_str}"
        return original_str

    def get_record_type(self, obj):
        return 'bio'

    def get_taxon_group(self, obj: Taxonomy):
        taxon_group_id = self.context.get('taxon_group_id', None)
        if taxon_group_id:
            taxon_group = TaxonGroup.objects.get(id=taxon_group_id)
            if not TaxonGroupTaxonomy.objects.filter(
                taxongroup_id=taxon_group_id,
                taxonomy=obj
            ).exists():
                children = taxon_group.get_all_children()
                children_ids = [child.id for child in children]
                taxon_group_taxa = TaxonGroupTaxonomy.objects.filter(
                    taxongroup_id__in=children_ids,
                    taxonomy=obj
                )
                if taxon_group_taxa.exists():
                    taxon_group = taxon_group_taxa.first().taxongroup
            return {
                'id': taxon_group.id,
                'logo': taxon_group.logo.name,
                'name': taxon_group.name
            }
        taxonomy_obj = self.taxonomy_obj(obj)
        taxon_module = taxonomy_obj.taxongroup_set.first()
        if taxon_module:
            return {
                'id': taxon_module.id,
                'logo': taxon_module.logo.name,
                'name': taxon_module.name
            }
        return {}

    def get_iucn_status_sensitive(self, obj):
        if obj.iucn_status:
            return obj.iucn_status.sensitive
        else:
            return False

    def get_iucn_status_name(self, obj):
        if obj.iucn_status:
            return obj.iucn_status.category
        else:
            return 'NE'

    def get_iucn_status_colour(self, obj):
        if obj.iucn_status:
            return obj.iucn_status.colour
        else:
            return None

    def get_children_count(self, obj):
        taxon_group_id = self.context.get('taxon_group_id')
        if not taxon_group_id:
            return 0
        return Taxonomy.objects.filter(
            parent=obj,
            taxongrouptaxonomy__taxongroup_id=taxon_group_id
        ).distinct().count()

    def get_other_group_count(self, obj):
        """Number of groups this taxon belongs to, excluding the current taxon group."""
        taxon_group_id = self.context.get('taxon_group_id')
        qs = TaxonGroupTaxonomy.objects.filter(taxonomy=self.taxonomy_obj(obj))
        if taxon_group_id:
            qs = qs.exclude(taxongroup_id=taxon_group_id)
        return qs.values('taxongroup').distinct().count()

    class Meta:
        model = Taxonomy
        exclude = ('gbif_data', 'vernacular_names', 'iucn_data', 'hierarchical_data')


class TaxonExportSerializer(serializers.ModelSerializer):
    """
    Serializer for taxon collection model.
    """
    iucn_status_sensitive = serializers.SerializerMethodField()
    iucn_status_name = serializers.SerializerMethodField()

    def get_iucn_status_sensitive(self, obj):
        if obj.iucn_status:
            return obj.iucn_status.sensitive
        else:
            return None

    def get_iucn_status_name(self, obj):
        if obj.iucn_status:
            return obj.iucn_status.category
        else:
            return None

    class Meta:
        model = Taxonomy
        fields = [
            'scientific_name', 'class_name',
            'iucn_status_sensitive', 'iucn_status_name'
        ]


class TaxonOccurencesSerializer(serializers.ModelSerializer):
    """
    Serializer for taxon collection model in occurrences format.
    """

    record_type = serializers.SerializerMethodField()
    count = serializers.SerializerMethodField()

    def get_record_type(self, obj):
        return 'taxa'

    def get_count(self, obj):
        if hasattr(obj, 'num_occurrences'):
            return obj.num_occurrences
        else:
            return 0

    class Meta:
        model = Taxonomy
        fields = [
            'id', 'common_name', 'highlighted_common_name',
            'taxon_class', 'record_type',
            'count'
        ]


class TaxonSimpleSerializer(serializers.ModelSerializer):
    cons_status = serializers.SerializerMethodField()

    def get_cons_status(self, obj: Taxonomy):
        return obj.iucn_status.category if obj.iucn_status else '-'

    class Meta:
        model = Taxonomy
        fields = [
            'id', 'canonical_name',
            'scientific_name', 'cons_status']


class TaxonGroupExpertSerializer(serializers.ModelSerializer):
    full_name = serializers.SerializerMethodField()

    def get_full_name(self, obj: get_user_model()):
        if obj.first_name and obj.last_name:
            return f'{obj.first_name} {obj.last_name}'
        return obj.username

    class Meta:
        model = get_user_model()
        fields = ['id', 'username', 'full_name', 'email']


class OccurrenceUploadTemplateSerializer(serializers.ModelSerializer):
    url = serializers.SerializerMethodField()
    name = serializers.SerializerMethodField()

    class Meta:
        model = OccurrenceUploadTemplate
        fields = ("id", "url", "name", "label", "created_at")

    def get_url(self, obj: OccurrenceUploadTemplate) -> str:
        return obj.file.url if obj.file else ""

    def get_name(self, obj: OccurrenceUploadTemplate) -> str:
        if obj.file:
            return obj.file.name.split("/")[-1]
        return obj.label or ""


class TaxonGroupSerializer(serializers.ModelSerializer):
    extra_attributes = serializers.SerializerMethodField()
    taxa_count = serializers.SerializerMethodField()
    experts = serializers.SerializerMethodField()
    gbif_parent_species = serializers.SerializerMethodField()
    children = serializers.SerializerMethodField()
    validated_count = serializers.SerializerMethodField()
    unvalidated_count = serializers.SerializerMethodField()
    taxa_upload_template = serializers.SerializerMethodField()
    occurrence_upload_template = serializers.SerializerMethodField()
    occurrence_upload_templates = serializers.SerializerMethodField()

    def get_occurrence_upload_templates(self, obj: TaxonGroup):
        items = obj.list_all_occurrence_templates()
        return json.dumps(OccurrenceUploadTemplateSerializer(items, many=True).data)

    def get_taxa_upload_template(self, obj: TaxonGroup):
        if not obj.taxa_upload_template:
            if preferences.SiteSetting.taxonomic_upload_template:
                return preferences.SiteSetting.taxonomic_upload_template.url
        else:
            return obj.taxa_upload_template.url
        return ''

    def get_occurrence_upload_template(self, obj: TaxonGroup):
        if not obj.occurrence_upload_template:
            if preferences.SiteSetting.occurrence_upload_template:
                return preferences.SiteSetting.occurrence_upload_template.url
        else:
            return obj.occurrence_upload_template.url
        return ''

    def get_children(self, obj: TaxonGroup):
        children = TaxonGroup.objects.filter(parent=obj)
        if children.exists():
            return TaxonGroupSerializer(
                children, many=True, context=self.context
            ).data
        return []

    def get_gbif_parent_species(self, obj: TaxonGroup):
        if obj.gbif_parent_species:
            return json.dumps(TaxonSerializer(
                obj.gbif_parent_species
            ).data)
        return ''

    def get_taxa_count(self, obj: TaxonGroup):
        from bims.templatetags.site import is_fada_site
        fada = is_fada_site()
        unique_taxonomy_ids = set()

        def collect_taxonomy_ids(taxon_group):
            qs = TaxonGroupTaxonomy.objects.filter(taxongroup=taxon_group)
            if fada:
                qs = qs.exclude(
                    taxonomy__fada_id__isnull=True
                ).exclude(taxonomy__fada_id='')
            unique_taxonomy_ids.update(qs.values_list('id', flat=True))
            for child in TaxonGroup.objects.filter(parent=taxon_group):
                collect_taxonomy_ids(child)

        collect_taxonomy_ids(obj)
        return len(unique_taxonomy_ids)

    def get_unvalidated_count(self, obj: TaxonGroup):
        return 0

    def get_validated_count(self, obj: TaxonGroup):
        return 0

    def get_extra_attributes(self, obj):
        return list(
            obj.taxonextraattribute_set.all().values_list('name', flat=True)
        )

    def get_experts(self, obj: TaxonGroup):
        return TaxonGroupExpertSerializer(
            obj.experts.all(),
            many=True
        ).data

    class Meta:
        model = TaxonGroup
        fields = ['id',
                  'parent',
                  'gbif_parent_species',
                  'name', 'category', 'logo', 'extra_attributes',
                  'taxa_count', 'unvalidated_count', 'validated_count',
                  'experts', 'children',
                  'taxa_upload_template',
                  'occurrence_upload_template',
                  'occurrence_upload_templates'
                  ]
