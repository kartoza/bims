from rest_framework import serializers

from bims.models.taxonomy import Taxonomy


STATUS_COLDP_MAP = {
    'ACCEPTED': 'accepted',
    'SYNONYM': 'synonym',
    'HETEROTYPIC_SYNONYM': 'heterotypic synonym',
    'HOMOTYPIC_SYNONYM': 'homotypic synonym',
    'PROPARTE_SYNONYM': 'pro parte synonym',
    'MISAPPLIED': 'misapplied',
    'DOUBTFUL': 'ambiguous synonym',
}

SYNONYM_STATUSES = frozenset(STATUS_COLDP_MAP) - {'ACCEPTED'}

ENVIRONMENT_VALUES = frozenset({'brackish', 'freshwater', 'marine', 'terrestrial'})

CODE_VALUES = (
    'botanical', 'zoological', 'bacterial', 'virus',
    'cultivar', 'phytosociological', 'bio',
)


class ColDPTaxonSerializer(serializers.ModelSerializer):
    """
    Serializes a Taxonomy instance to the ColDP NameUsage flat format.

    Field names follow the ColDP specification exactly (camelCase).
    See docs/taxonomy_coldp_mapping.md for the full field mapping.
    """

    taxonID = serializers.SerializerMethodField()
    parentID = serializers.SerializerMethodField()
    status = serializers.SerializerMethodField()
    scientificName = serializers.SerializerMethodField()
    authorship = serializers.SerializerMethodField()
    rank = serializers.SerializerMethodField()
    environment = serializers.SerializerMethodField()
    code = serializers.SerializerMethodField()
    kingdom = serializers.SerializerMethodField()
    phylum = serializers.SerializerMethodField()
    subphylum = serializers.SerializerMethodField()
    order = serializers.SerializerMethodField()
    suborder = serializers.SerializerMethodField()
    superfamily = serializers.SerializerMethodField()
    family = serializers.SerializerMethodField()
    tribe = serializers.SerializerMethodField()
    subtribe = serializers.SerializerMethodField()
    genus = serializers.SerializerMethodField()
    subgenus = serializers.SerializerMethodField()
    species = serializers.SerializerMethodField()

    class Meta:
        model = Taxonomy
        fields = [
            'taxonID',
            'parentID',
            'status',
            'scientificName',
            'authorship',
            'rank',
            'environment',
            'code',
            'kingdom',
            'phylum',
            'subphylum',
            'order',
            'suborder',
            'superfamily',
            'family',
            'tribe',
            'subtribe',
            'genus',
            'subgenus',
            'species',
        ]

    def _taxon_id(self, pk) -> str:
        """Build a site-scoped taxon ID, e.g. FBIS123 or FADA123."""
        prefix = (self.context or {}).get('site_prefix', '')
        return f'{prefix}{pk}' if prefix else str(pk)

    def get_taxonID(self, obj):
        return self._taxon_id(obj.id)

    def get_parentID(self, obj):
        status_raw = (obj.taxonomic_status or '').upper()
        if status_raw in SYNONYM_STATUSES and obj.accepted_taxonomy_id:
            return self._taxon_id(obj.accepted_taxonomy_id)
        if obj.parent_id:
            return self._taxon_id(obj.parent_id)
        return ''

    def get_status(self, obj):
        status_raw = (obj.taxonomic_status or '').upper()
        return STATUS_COLDP_MAP.get(status_raw, status_raw.lower())

    def get_rank(self, obj):
        rank = (obj.rank or '').lower()
        if rank in ('forma', 'form'):
            return 'form'
        return rank

    def get_scientificName(self, obj):
        return obj.canonical_name or obj.scientific_name or ''

    def get_authorship(self, obj):
        return obj.author or ''

    def get_environment(self, obj):
        """
        Return the first tag whose name matches a ColDP environment enum value:
        brackish | freshwater | marine | terrestrial (case-insensitive).
        """
        for tag in obj.tags.all():
            if tag.name.lower() in ENVIRONMENT_VALUES:
                return tag.name.lower()
        return ''

    def get_code(self, obj):
        """
        Return the nomenclatural code derived from tags.
        First tag matching: botanical, zoological, bacterial, virus,
        cultivar, phytosociological, bio (case-insensitive).
        """
        tag_names = {tag.name.lower() for tag in obj.tags.all()}
        for code in CODE_VALUES:
            if code in tag_names:
                return code
        return ''

    def get_kingdom(self, obj):
        return obj.kingdom_name or ''

    def get_phylum(self, obj):
        return obj.phylum_name or ''

    def get_subphylum(self, obj):
        return obj.subphylum_name or ''

    def get_order(self, obj):
        return obj.order_name or ''

    def get_suborder(self, obj):
        return obj.suborder_name or ''

    def get_superfamily(self, obj):
        return obj.superfamily_name or ''

    def get_family(self, obj):
        return obj.family_name or ''

    def get_tribe(self, obj):
        return obj.tribe_name or ''

    def get_subtribe(self, obj):
        return obj.subtribe_name or ''

    def get_genus(self, obj):
        return obj.genus_name or ''

    def get_subgenus(self, obj):
        return obj.sub_genus_name or ''

    def get_species(self, obj):
        return obj.species_name or ''

    def to_representation(self, instance):
        data = super().to_representation(instance)
        data['class'] = instance.class_name or ''
        data['subclass'] = instance.subclass_name or ''
        return data
