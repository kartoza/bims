import json
import re
from datetime import date

from django.conf import settings
from django.template.loader import render_to_string
from django.utils.functional import cached_property
from taggit.managers import TaggableManager
from taggit.models import GenericTaggedItemBase, TagBase, TaggedItemBase

from bims.models.cites_listing_info import CITESListingInfo
from bims.models.source_reference import SourceReference
from bims.models.validation import AbstractValidation
from django.db import models
from django.dispatch import receiver

from bims.enums.taxonomic_status import TaxonomicStatus
from bims.enums.taxonomic_rank import TaxonomicRank
from bims.models.iucn_status import IUCNStatus
from bims.models.endemism import Endemism
from bims.utils.domain import get_current_domain
from bims.utils.iucn import get_iucn_status
from bims.models.vernacular_name import VernacularName
from bims.models.notification import (
    get_recipients_for_notification,
    NEW_TAXONOMY
)
from django.db.models import JSONField, OuterRef, Subquery, signals

ORIGIN_CATEGORIES = {
    'non-native': 'alien',
    'native': 'indigenous',
    'unknown': 'unknown',
    'non-native: invasive': 'alien-invasive',
    'non-native: non-invasive': 'alien-non-invasive'
}


class TaxonTag(TagBase):
    name = models.CharField(
        verbose_name="A taxon tag name",
        unique=False,
        max_length=100
    )
    doubtful = models.BooleanField(default=False)

    class Meta:
        verbose_name = "Taxonomy Tag"
        verbose_name_plural = "Taxonomy Tags"


class SpeciesGroup(models.Model):
    name = models.CharField(max_length=200, unique=True)
    description = models.TextField(blank=True, default='')

    def __str__(self):
        return self.name


class CustomTaggedTaxonomy(TaggedItemBase):
    content_object = models.ForeignKey(
        'Taxonomy',
        on_delete=models.CASCADE)
    tag = models.ForeignKey(
        TaxonTag,
        on_delete=models.CASCADE,
        related_name="%(app_label)s_%(class)s_items",
    )

    class Meta:
        verbose_name = "Custom Tagged Taxonomy"
        verbose_name_plural = "Custom Tagged Taxa"


class TaxonomyField(models.CharField):

    description = 'A taxonomy field.'

    def __init__(self, taxonomy_key=None, *args, **kwargs):
        kwargs['max_length'] = 100
        kwargs['blank'] = True
        kwargs['default'] = ''
        self.taxonomy_key = taxonomy_key
        super(TaxonomyField, self).__init__(*args, **kwargs)


class AbstractTaxonomy(AbstractValidation):
    CATEGORY_CHOICES = (
        ('alien', 'Non-Native'),
        ('indigenous', 'Native'),
        ('unknown', 'Unknown'),
        ('alien-invasive', 'Non-native: invasive'),
        ('alien-non-invasive', 'Non-native: non-invasive')
    )
    tags = TaggableManager(
        blank=True,
    )

    biographic_distributions = TaggableManager(
        through=CustomTaggedTaxonomy,
        related_name='bio_distribution',
        blank=True,
        verbose_name='Biographic Distributions'
    )

    invasion = models.ForeignKey(
        'bims.Invasion',
        null=True,
        blank=True,
        on_delete=models.SET_NULL
    )

    fada_id = models.CharField(
        verbose_name='FADA ID',
        unique=True,
        null=True,
        blank=True,
        max_length=50
    )

    gbif_key = models.IntegerField(
        verbose_name='GBIF Key',
        null=True,
        blank=True
    )

    aphia_id = models.IntegerField(
        verbose_name='WoRMS AphiaID',
        null=True,
        blank=True,
        db_index=True,
    )

    verified = models.BooleanField(
        help_text='The data has been verified',
        default=False
    )

    scientific_name = models.CharField(
        verbose_name='Scientific Name',
        max_length=512,
        null=False,
        blank=False
    )

    canonical_name = models.CharField(
        verbose_name='Canonical Name',
        max_length=200,
        null=True,
        blank=True,
        db_index=True
    )

    legacy_canonical_name = models.CharField(
        verbose_name='Legacy Canonical Name',
        max_length=700,
        blank=True,
        default=''
    )

    rank = models.CharField(
        verbose_name='Taxonomic Rank',
        max_length=50,
        choices=[(rank.name, rank.value) for rank in TaxonomicRank],
        blank=True,
    )

    vernacular_names = models.ManyToManyField(
        to=VernacularName,
        blank=True,
    )

    taxonomic_status = models.CharField(
        verbose_name='Taxonomic Status',
        max_length=50,
        choices=[(status.name, status.value) for status in TaxonomicStatus],
        blank=True
    )

    parent = models.ForeignKey(
        verbose_name='Parent',
        to='self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )

    national_conservation_status = models.ForeignKey(
        IUCNStatus,
        models.SET_NULL,
        related_name='national_conservation_status',
        verbose_name='National Conservation Status',
        null=True,
        blank=True,
    )

    include_in_rli = models.BooleanField(
        verbose_name='Include in RLI',
        default=False,
    )

    iucn_status = models.ForeignKey(
        IUCNStatus,
        models.SET_NULL,
        verbose_name='Global Red List Status (IUCN)',
        null=True,
        blank=True,
    )

    iucn_redlist_id = models.IntegerField(
        verbose_name='IUCN taxon id',
        null=True,
        blank=True
    )

    iucn_data = models.TextField(
        verbose_name='Data from IUCN',
        null=True,
        blank=True
    )

    endemism = models.ForeignKey(
        Endemism,
        models.SET_NULL,
        verbose_name='Endemism',
        null=True,
        blank=True
    )

    origin = models.ForeignKey(
        'bims.TaxonOrigin',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        help_text='Origin'
    )

    author = models.CharField(
        verbose_name='Author',
        max_length=200,
        null=True,
        blank=True
    )

    gbif_data = JSONField(
        verbose_name='Json data from gbif',
        null=True,
        blank=True
    )

    additional_data = JSONField(
        verbose_name='Additional json data',
        null=True,
        blank=True
    )

    import_date = models.DateField(
        default=date.today,
        blank=True,
        null=True,
    )

    accepted_taxonomy = models.ForeignKey(
        related_name='synonym',
        to='self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )

    hierarchical_data = JSONField(
        verbose_name='Hierarchical Data',
        null=True,
        blank=True
    )

    source_reference = models.ForeignKey(
        SourceReference,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='%(class)s_source_reference',
    )

    species_group = models.ForeignKey(
        SpeciesGroup,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        help_text='Informal group of closely related species'
    )

    created_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    last_modified_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        models.SET_NULL,
        null=True,
        blank=True,
        related_name='%(class)s_last_modified_by',
    )

    subgenus = models.ForeignKey(
        'self',
        null=True,
        blank=True,
        related_name='%(class)s_subgenus',
        on_delete=models.SET_NULL,
        limit_choices_to={'rank': TaxonomicRank.SUBGENUS.name},
    )

    class Meta:
        abstract = True



    @property
    def data_name(self):
        return self.canonical_name

    @cached_property
    def common_name(self):
        vernacular_names = list(
            self.vernacular_names.filter(
                language__istartswith='en'
            ).values_list('name', flat=True))
        if len(vernacular_names) == 0:
            return ''
        else:
            return vernacular_names[0]

    @cached_property
    def cites_listing(self):
        cites_listing_info = CITESListingInfo.objects.filter(
            taxonomy_id=self.id
        )
        if cites_listing_info.exists():
            return ','.join(list(cites_listing_info.values_list(
                'appendix', flat=True
            )))
        if self.additional_data:
            if 'CITES Listing' in self.additional_data:
                return self.additional_data['CITES Listing']
            if 'Cites listing' in self.additional_data:
                return self.additional_data['Cites listing']
            if 'CITES listing' in self.additional_data:
                return self.additional_data['CITES listing']
        return ''

    @property
    def taxon_class(self):
        if self.rank != TaxonomicRank.CLASS.name and self.parent:
            return self.parent.taxon_class
        if self.rank == TaxonomicRank.CLASS.name:
            return self
        return None

    def get_parent_by_rank(self, rank):
        limit = 20
        current_try = 0
        _taxon = self
        _parent = _taxon.parent if _taxon.parent else None
        _rank = _taxon.rank
        while (
                _parent and _rank
                and _rank != rank
                and current_try < limit
        ):
            current_try += 1
            _taxon = _parent
            _rank = _taxon.rank
            _parent = _taxon.parent if _taxon.parent else None

        if _rank == rank:
            return _taxon
        return None

    @staticmethod
    def _specific_epithet_from_canonical(canonical_name: str) -> str:
        """
        Return the specific epithet from a (sub)species canonical name.

        Strips leading genus and any subgenus token in parentheses:
          "Peltoperla (Peltoperla) anna"  → "anna"
          "Homo sapiens"                  → "sapiens"
          "Canis lupus familiaris"        → "lupus familiaris"  (subspecies)
        """
        tokens = (canonical_name or '').split()
        # Keep only tokens that are not a parenthetical subgenus group
        meaningful = [t for t in tokens
                      if not (t.startswith('(') and t.endswith(')'))]
        # Everything after the first (genus) token is the epithet part
        if len(meaningful) >= 2:
            return ' '.join(meaningful[1:])
        return canonical_name or ''

    @staticmethod
    def _genus_prefix_from_canonical(canonical_name: str) -> str:
        """
        Return the genus (+ subgenus) prefix from a species canonical name.

        Finds the last non-parenthetical token (the epithet) and returns
        everything that precedes it:
          "Peltoperla (Peltoperla) anna"  → "Peltoperla (Peltoperla)"
          "Homo sapiens"                  → "Homo"
        Returns '' when the name has only one meaningful word.
        """
        tokens = (canonical_name or '').split()
        meaningful = [t for t in tokens
                      if not (t.startswith('(') and t.endswith(')'))]
        if len(meaningful) < 2:
            return ''
        epithet = meaningful[-1]
        # Locate the epithet from the right of the original token list
        for i in range(len(tokens) - 1, -1, -1):
            if tokens[i] == epithet:
                return ' '.join(tokens[:i]).strip()
        return tokens[0] if tokens else ''

    def _get_rank_name_from_canonical(self, rank):
        """Infer a higher-rank name from the canon when parents disagree."""
        canonical = (self.canonical_name or '').strip()
        if not canonical or not rank:
            return ''

        rank_name = rank.name if isinstance(rank, TaxonomicRank) else rank
        rank_name = (rank_name or '').upper()
        rank_depth = {
            TaxonomicRank.GENUS.name: 1,
            TaxonomicRank.SPECIES.name: 2,
            TaxonomicRank.SUBSPECIES.name: 3,
            TaxonomicRank.VARIETY.name: 3,
            TaxonomicRank.FORMA.name: 3,
            TaxonomicRank.FORM.name: 3,
        }

        depth = rank_depth.get(rank_name)
        if not depth:
            return ''

        tokens = canonical.split()
        if not tokens:
            return ''

        if rank_name == TaxonomicRank.GENUS.name:
            # For GENUS, preserve subgenus notation: "Peltoperla (Peltoperla) anna"
            # → "Peltoperla (Peltoperla)"
            prefix = self._genus_prefix_from_canonical(canonical)
            if prefix:
                return prefix
            return tokens[0]

        if rank_name == TaxonomicRank.SPECIES.name:
            # Return only the specific epithet, not the full binomial.
            return self._specific_epithet_from_canonical(canonical)

        markers = {
            'sp', 'spp', 'subsp', 'ssp', 'var', 'subvar', 'forma',
            'form', 'f', 'subspecies', 'variety'
        }
        cleaned_tokens = []
        for token in tokens:
            if '(' in token or ')' in token:
                continue
            normalized = token.lower().strip('.').strip('()')
            if normalized in markers:
                continue
            cleaned_tokens.append(token)
            if len(cleaned_tokens) >= depth:
                break

        if len(cleaned_tokens) < depth:
            return ''

        return ' '.join(cleaned_tokens[:depth])

    def get_taxon_rank_name(self, rank):
        limit = 20
        current_try = 0

        target_rank = rank.name if isinstance(rank, TaxonomicRank) else rank
        _rank = self.rank

        status = (self.taxonomic_status or '').upper()
        is_synonym_or_doubtful = (
            status == 'DOUBTFUL' or 'SYNONYM' in status
        )
        rank_differs = target_rank and self.rank and self.rank != target_rank

        if is_synonym_or_doubtful and rank_differs:
            canonical_rank_name = self._get_rank_name_from_canonical(target_rank)
            if canonical_rank_name:
                return canonical_rank_name

        if (
                is_synonym_or_doubtful and rank_differs and
                self.accepted_taxonomy
        ):
            _taxon = self.accepted_taxonomy
        else:
            _taxon = self

        _parent = _taxon.parent if _taxon.parent else None
        _species_canonical = (
            _taxon.canonical_name
            if _rank in ('SPECIES', 'SUBSPECIES') else None
        )

        while (
                _parent and _rank
                and _rank != target_rank
                and current_try < limit
        ):
            current_try += 1
            if _rank in ('SPECIES', 'SUBSPECIES') and _species_canonical is None:
                _species_canonical = _taxon.canonical_name
            _taxon = _parent
            _rank = _taxon.rank
            _parent = _taxon.parent if _taxon.parent else None

        if _rank == target_rank:
            if target_rank == 'SPECIES':
                return self._specific_epithet_from_canonical(_taxon.canonical_name)
            if target_rank == 'GENUS' and _species_canonical:
                prefix = self._genus_prefix_from_canonical(_species_canonical)
                if prefix:
                    return prefix
            return _taxon.canonical_name
        return ''

    @property
    def is_synonym_or_doubtful(self):
        return self.taxonomic_status == 'DOUBTFUL' or 'SYNONYM' in self.taxonomic_status

    @property
    def class_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.CLASS.name)

    @property
    def kingdom_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.KINGDOM.name)

    @property
    def phylum_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.PHYLUM.name)

    @property
    def order_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.ORDER.name)

    @property
    def family_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.FAMILY.name)

    @property
    def family(self):
        return self.get_parent_by_rank(TaxonomicRank.FAMILY.name)

    @property
    def genus(self):
        return self.get_parent_by_rank(TaxonomicRank.GENUS.name)

    @property
    def genus_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.GENUS.name)

    @property
    def sub_genus_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.SUBGENUS.name)

    @cached_property
    def full_species_name(self):
        genus_name = self.get_taxon_rank_name(TaxonomicRank.GENUS.name)
        species_name = self.species_name
        if genus_name not in species_name:
            return genus_name + ' ' + species_name
        return species_name

    @property
    def species_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.SPECIES.name)

    @property
    def specific_epithet(self):
        species_name = self.species_name
        genus_name = self.get_taxon_rank_name(TaxonomicRank.GENUS.name)
        if genus_name in species_name:
            pattern = r'^' + re.escape(genus_name) + r'\s+'
            return re.sub(pattern, '', species_name).strip()
        return species_name

    @property
    def sub_species_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.SUBSPECIES.name)

    @property
    def sub_family_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.SUBFAMILY.name)

    @property
    def tribe_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.TRIBE.name)

    @property
    def sub_tribe_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.SUBTRIBE.name)

    # Aliases used by ColDP export (consistent naming without underscore prefix)
    @property
    def subfamily_name(self):
        return self.sub_family_name

    @property
    def subtribe_name(self):
        return self.sub_tribe_name

    # Intermediate rank properties for ColDP export
    @property
    def subphylum_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.SUBPHYLUM.name)

    @property
    def superclass_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.SUPERCLASS.name)

    @property
    def subclass_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.SUBCLASS.name)

    @property
    def superorder_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.SUPERORDER.name)

    @property
    def suborder_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.SUBORDER.name)

    @property
    def infraorder_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.INFRAORDER.name)

    @property
    def superfamily_name(self):
        return self.get_taxon_rank_name(TaxonomicRank.SUPERFAMILY.name)

    @property
    def variety_name(self):
        """
        Return just the variety epithet, e.g. "major",
        """
        variety_taxon = (
            self if self.rank == TaxonomicRank.VARIETY.name
            else self.get_parent_by_rank(TaxonomicRank.VARIETY.name)
        )

        if not variety_taxon:
            if self.additional_data and self.additional_data.get('Variety'):
                return self.additional_data.get('Variety').strip()
            return ''

        canon = (variety_taxon.canonical_name or '').strip()
        if not canon:
            if variety_taxon.additional_data and variety_taxon.additional_data.get('Variety'):
                return variety_taxon.additional_data.get('Variety').strip()
            return ''

        tokens = canon.split()
        return tokens[-1] if tokens else ''

    @property
    def taxon_name(self):
        """
        Return the most specific part of the taxon's name.
        """
        canon = (self.canonical_name or "").strip()
        if not canon:
            return ""

        tokens = canon.split()
        rank = (self.rank or "").lower()

        if rank == "subspecies" or rank == "species":
            genus_name = self.genus_name
            if tokens and tokens[0].lower() == genus_name.lower() and len(tokens) > 1:
                canon = " ".join(tokens[1:])
                tokens = canon.split()
            if rank == "subspecies" and self.parent and self.parent.rank.lower() == "species":
                if len(tokens) > 1 and tokens[0].lower == self.parent.taxon_name:
                    canon = " ".join(tokens[1:])

        return canon

    @property
    def is_species(self):
        return (
            self.rank == TaxonomicRank.SPECIES.name or
            self.rank == TaxonomicRank.SUBSPECIES.name
        )

    @property
    def name(self):
        if self.canonical_name:
            return self.canonical_name
        elif self.scientific_name:
            return self.scientific_name
        return '-'

    @property
    def created_at_date(self):
        """Return import_date as the canonical creation date for this taxon."""
        if self.import_date:
            return self.import_date
        return self.created_at

    @cached_property
    def last_modified(self):
        from easyaudit.models import CRUDEvent
        last_update_event = CRUDEvent.objects.filter(
            object_id=self.id,
            content_type__model=self._meta.model_name,
            event_type=CRUDEvent.UPDATE
        ).order_by('-datetime').first()

        if last_update_event:
            return last_update_event.datetime
        return self.import_date

    @cached_property
    def user_last_modified(self):
        if self.last_modified_by:
            return self.last_modified_by

        # For legacy data
        if isinstance(self, Taxonomy):
            from bims.models.taxonomy_update_proposal import (
                TaxonomyUpdateProposal
            )
            taxon_proposal = TaxonomyUpdateProposal.objects.filter(
                original_taxonomy=self
            ).exclude(status='rejected').order_by('-id')
            if taxon_proposal.exists():
                latest = taxon_proposal.first()
                return latest.last_modified_by or latest.collector_user

        from easyaudit.models import CRUDEvent
        crud_event = CRUDEvent.objects.filter(
            content_type__model=self._meta.model_name,
            object_id=self.id,
            event_type=CRUDEvent.UPDATE
        ).order_by('-datetime')
        if not crud_event.exists():
            from bims.models.taxonomy_update_proposal import TaxonomyUpdateProposal
            if isinstance(self, TaxonomyUpdateProposal):
                orig_taxon = self.original_taxonomy
                crud_event = CRUDEvent.objects.filter(
                    content_type__model=orig_taxon._meta.model_name,
                    object_id=orig_taxon.id,
                    event_type=CRUDEvent.UPDATE
                ).order_by('-datetime')

        if crud_event.exists():
            return crud_event.first().user

        return None

class Taxonomy(AbstractTaxonomy):
    CATEGORY_CHOICES = (
        (ORIGIN_CATEGORIES['non-native'], 'Non-Native'),
        (ORIGIN_CATEGORIES['native'], 'Native'),
        (ORIGIN_CATEGORIES['unknown'], 'Unknown'),
        (ORIGIN_CATEGORIES['non-native: invasive'], 'Non-native: invasive'),
        (
            ORIGIN_CATEGORIES['non-native: non-invasive'],
            'Non-native: non-invasive'
        )
    )
    CATEGORY_CHOICES_DICT = {
        ORIGIN_CATEGORIES['non-native']: 'Non-Native',
        ORIGIN_CATEGORIES['native']: 'Native',
        ORIGIN_CATEGORIES['unknown']: 'Unknown',
        ORIGIN_CATEGORIES['non-native: invasive']: 'Non-native: invasive',
        ORIGIN_CATEGORIES['non-native: non-invasive']: 'Non-native: non-invasive'
    }

    checklist_version_uuid = models.UUIDField(
        null=True,
        blank=True,
        help_text=(
            'UUID of the ChecklistVersion in which this taxon first appeared '
            'in a published ColDP release.'
        ),
    )

    last_checklist_published_uuid = models.UUIDField(
        null=True,
        blank=True,
        help_text=(
            'UUID of the most recent published ChecklistVersion that included '
            'a change (addition or update) to this taxon.'
        ),
    )

    def save_json_data(self, json_field):
        max_allowed = 10
        attempt = 0
        is_dictionary = False
        json_data = {}
        if not json_field:
            return json_data
        while not is_dictionary and attempt < max_allowed:
            if not json_field:
                break
            if isinstance(json_field, dict):
                is_dictionary = True
                json_data = json_field
            else:
                json_data = json.loads(json_field)
                attempt += 1
        return json_data

    # noinspection PyClassicStyleClass
    class Meta:
        """Meta class for project."""
        app_label = 'bims'
        verbose_name_plural = 'Taxa'
        verbose_name = 'Taxonomy'

    def __unicode__(self):
        return '%s - %s' % (
            self.scientific_name,
            self.rank
        )

    def __str__(self):
        return '%s - %s' % (
            self.scientific_name,
            self.rank
        )

    def get_direct_children(self):
        children = Taxonomy.objects.filter(
            parent=self
        )
        return children

    def get_all_children(self):
        query = {}
        parent = ''
        or_condition = models.Q()
        for i in range(6):  # species to class
            parent += 'parent__'
            query[parent + 'in'] = [self]
        for key, value in query.items():
            or_condition |= models.Q(**{key: value})
        return Taxonomy.objects.filter(or_condition)

    def parent_by_rank(self, rank):
        taxon = self
        current_rank = taxon.rank
        while current_rank != rank and taxon.parent:
            taxon = taxon.parent
            current_rank = taxon.rank
        if current_rank == rank:
            return taxon
        return None

    def save(self, *args, **kwargs):
        update_taxon_with_gbif = False

        rank_order = [r.name for r in TaxonomicRank.hierarchy()]
        cur_rank = (self.rank or "").upper()

        try:
            cur_idx = rank_order.index(cur_rank)
        except ValueError:
            cur_idx = None

        family_idx = rank_order.index(TaxonomicRank.FAMILY.name)
        genus_idx = rank_order.index(TaxonomicRank.GENUS.name)
        species_idx = rank_order.index(TaxonomicRank.SPECIES.name)
        genus_name = ''

        if self.gbif_data:
            self.gbif_data = self.save_json_data(self.gbif_data)
        if self.additional_data:
            self.additional_data = self.save_json_data(self.additional_data)
        if self.additional_data and 'fetch_gbif' in self.additional_data:
            update_taxon_with_gbif = True
            del self.additional_data['fetch_gbif']
        species_name = ''
        if not self.hierarchical_data or 'species_name' not in self.hierarchical_data:
            species_name = self.species_name
            genus_name = self.genus_name
            if genus_name in species_name and genus_name:
                species_name = species_name.split(genus_name)[-1].strip()
        if not self.hierarchical_data:
            self.hierarchical_data = {
                'family_name': self.get_taxon_rank_name(TaxonomicRank.FAMILY.name),
                'genus_name': self.get_taxon_rank_name(TaxonomicRank.GENUS.name),
                'species_name': species_name
            }
        elif ('family_name' not in self.hierarchical_data or not self.hierarchical_data['family_name']) and cur_idx and cur_idx >= family_idx:
            self.hierarchical_data['family_name'] = self.get_taxon_rank_name(TaxonomicRank.FAMILY.name)
        elif ('genus_name' not in self.hierarchical_data or not self.hierarchical_data['genus_name']) and cur_idx and cur_idx >= genus_idx:
            self.hierarchical_data['genus_name'] = genus_name
        elif ('species_name' not in self.hierarchical_data or not self.hierarchical_data['species_name']) and cur_idx and cur_idx >= species_idx:
            self.hierarchical_data['species_name'] = species_name

        super(Taxonomy, self).save(*args, **kwargs)

        if update_taxon_with_gbif:
            from bims.utils.fetch_gbif import fetch_all_species_from_gbif
            fetch_all_species_from_gbif(
                species=self.scientific_name,
                gbif_key=self.gbif_key,
                fetch_vernacular_names=True)

    def get_experts_email(self, taxon_group, max_depth=10):
        experts = set()
        depth = 0
        while taxon_group and depth < max_depth:
            expert_email = taxon_group.experts.values_list('email', flat=True)
            experts.update(expert_email)
            taxon_group = taxon_group.parent
            depth += 1
        return list(experts)

    def send_new_taxon_email(self, taxon_group_id=None):
        from bims.models import TaxonGroup
        from bims.tasks.send_notification import send_mail_notification

        current_site = get_current_domain()
        recipients = get_recipients_for_notification(NEW_TAXONOMY)
        taxon_group = TaxonGroup.objects.get(id=taxon_group_id)

        # Get experts
        experts_email = self.get_experts_email(
            taxon_group=taxon_group
        )
        recipients = list(set(experts_email + recipients))

        email_body = render_to_string(
            'notifications/taxonomy/added_message.txt',
            {
                'taxonomy': self,
                'current_site': current_site,
                'taxon_group': taxon_group
            }
        )
        subject = '[{}] Taxon Validation Required'.format(current_site)
        from_email = settings.DEFAULT_FROM_EMAIL
        recipient_list = recipients

        send_mail_notification.delay(subject, email_body, from_email, recipient_list)

    def send_updated_taxon_email(self, taxon_group_id=None, creator=None):
        from bims.models import TaxonGroup
        from bims.tasks.send_notification import send_mail_notification

        current_site = get_current_domain()
        recipients = get_recipients_for_notification(NEW_TAXONOMY)
        taxon_group = TaxonGroup.objects.get(id=taxon_group_id)

        # Get experts
        experts_email = self.get_experts_email(
            taxon_group=taxon_group
        )
        recipients = list(set(experts_email + recipients))

        email_body = render_to_string(
            'notifications/taxonomy/updated_message.txt',
            {
                'taxonomy': self,
                'current_site': current_site,
                'taxon_group': taxon_group,
                'creator': creator
            }
        )
        subject = '[{}] Taxon Validation Required'.format(current_site)
        from_email = settings.DEFAULT_FROM_EMAIL
        recipient_list = recipients

        send_mail_notification.delay(subject, email_body, from_email, recipient_list)


def _has_meaningful_iucn_data(taxon: Taxonomy | None) -> bool:
    """Return True when a taxon already carries a usable IUCN category or URL."""
    if not taxon:
        return False

    status = getattr(taxon, "iucn_status", None)
    if status and getattr(status, "category", None) and status.category != "NE":
        return True

    data = getattr(taxon, "iucn_data", None) or {}
    if isinstance(data, dict):
        return bool(data.get("url"))
    return bool(data)


def _extract_iucn_payload(taxon: Taxonomy) -> dict[str, object]:
    """Build field/value mapping for propagating IUCN info."""
    payload: dict[str, object] = {}

    status = getattr(taxon, "iucn_status", None)
    if status and getattr(status, "category", None) and status.category != "NE":
        if getattr(taxon, "iucn_status_id", None):
            payload["iucn_status_id"] = taxon.iucn_status_id

    sis_id = getattr(taxon, "iucn_redlist_id", None)
    if sis_id:
        payload["iucn_redlist_id"] = sis_id

    data = getattr(taxon, "iucn_data", None)
    if data:
        payload["iucn_data"] = data

    return payload


@receiver(models.signals.pre_save, sender=Taxonomy)
def taxonomy_pre_save_handler(sender, instance: Taxonomy, **kwargs):
    if 'subgenus' in instance.rank.lower():
        canonical_name = instance.canonical_name
        genus_name = instance.parent.canonical_name if instance.parent else ''
        if len(canonical_name.split(' ')) < 2 and genus_name:
            instance.scientific_name = f'{genus_name} ({canonical_name})'
            if instance.author:
                instance.scientific_name += f' {instance.author}'

    if instance.author and len(instance.scientific_name.split(instance.author)) > 2:
        instance.scientific_name = instance.scientific_name.replace(
            f'{instance.scientific_name} ', instance.author, 1
        )
    is_synonym = "SYNONYM" in (instance.taxonomic_status or "").upper()

    # Set IUCN status and redlist ID before saving taxonomy
    if instance.is_species and not instance.iucn_status:
        iucn_status, sis_id, iucn_url = get_iucn_status(taxon=instance)
        if iucn_status:
            instance.iucn_status = iucn_status

        if sis_id:
            instance.iucn_redlist_id = sis_id

        if iucn_url:
            instance.iucn_data = {
                'url': iucn_url
            }

    accepted = getattr(instance, "accepted_taxonomy", None)

    if (
        is_synonym
        and accepted
        and getattr(accepted, "id", None)
        and _has_meaningful_iucn_data(instance)
    ):
        update_payload = _extract_iucn_payload(instance)
        if update_payload:
            Taxonomy.objects.filter(pk=accepted.pk).update(**update_payload)

    if (
        is_synonym
        and accepted
        and getattr(accepted, "id", None)
        and (not instance.iucn_status or not instance.iucn_redlist_id)
        and _has_meaningful_iucn_data(accepted)
    ):
        if not instance.iucn_status and accepted.iucn_status_id:
            instance.iucn_status_id = accepted.iucn_status_id
        if not instance.iucn_redlist_id and accepted.iucn_redlist_id:
            instance.iucn_redlist_id = accepted.iucn_redlist_id
        if not instance.iucn_data:
            accepted_data = getattr(accepted, "iucn_data", None)
            if isinstance(accepted_data, dict) and accepted_data.get("url"):
                instance.iucn_data = {"url": accepted_data["url"]}

    if (
        not is_synonym
        and getattr(instance, "pk", None)
        and _has_meaningful_iucn_data(instance)
    ):
        update_payload = _extract_iucn_payload(instance)
        if update_payload:
            for synonym in Taxonomy.objects.filter(accepted_taxonomy=instance):
                Taxonomy.objects.filter(pk=synonym.pk).update(**update_payload)

    # If this accepted taxon has no IUCN data, pull it from one of its synonyms.
    # Use direct field assignment (no .save()) to avoid re-triggering this signal.
    if (
        not is_synonym
        and getattr(instance, "pk", None)
        and not _has_meaningful_iucn_data(instance)
    ):
        synonym_with_iucn = (
            Taxonomy.objects.filter(accepted_taxonomy=instance)
            .exclude(iucn_status__isnull=True)
            .exclude(iucn_status__category="NE")
            .select_related("iucn_status")
            .first()
        )
        if synonym_with_iucn:
            if not instance.iucn_status_id and synonym_with_iucn.iucn_status_id:
                instance.iucn_status_id = synonym_with_iucn.iucn_status_id
            if not instance.iucn_redlist_id and synonym_with_iucn.iucn_redlist_id:
                instance.iucn_redlist_id = synonym_with_iucn.iucn_redlist_id
            if not instance.iucn_data:
                syn_data = getattr(synonym_with_iucn, "iucn_data", None)
                if isinstance(syn_data, dict) and syn_data.get("url"):
                    instance.iucn_data = {"url": syn_data["url"]}

    # --- Origin propagation ---------------------------------------------------
    instance_origin_id = getattr(instance, "origin_id", None)

    if is_synonym and accepted and getattr(accepted, "id", None):
        if instance_origin_id:
            Taxonomy.objects.filter(
                pk=accepted.pk,
                endemism__isnull=True
            ).update(origin_id=instance_origin_id)
        elif not instance_origin_id and getattr(accepted, "origin_id", None):
            instance.origin_id = accepted.origin_id

    if not is_synonym and getattr(instance, "pk", None) and instance_origin_id:
        Taxonomy.objects.filter(
            accepted_taxonomy=instance
        ).update(origin_id=instance_origin_id)

    # --- Endemism propagation -------------------------------------------------
    instance_endemism_id = getattr(instance, "endemism_id", None)

    if is_synonym and accepted and getattr(accepted, "id", None):
        if instance_endemism_id:
            Taxonomy.objects.filter(
                pk=accepted.pk,
                endemism__isnull=True
            ).update(endemism_id=instance_endemism_id)
        elif not instance_endemism_id and getattr(accepted, "endemism_id", None):
            instance.endemism_id = accepted.endemism_id

    if not is_synonym and getattr(instance, "pk", None) and instance_endemism_id:
        Taxonomy.objects.filter(
            accepted_taxonomy=instance
        ).update(endemism_id=instance_endemism_id)

    # --- National conservation status propagation -----------------------------
    instance_ncs_id = getattr(instance, "national_conservation_status_id", None)

    if is_synonym and accepted and getattr(accepted, "id", None):
        if instance_ncs_id:
            Taxonomy.objects.filter(
                pk=accepted.pk,
                national_conservation_status__isnull=True
            ).update(
                national_conservation_status_id=instance_ncs_id
            )
        elif not instance_ncs_id and getattr(accepted, "national_conservation_status_id", None):
            instance.national_conservation_status_id = accepted.national_conservation_status_id

    if not is_synonym and getattr(instance, "pk", None) and instance_ncs_id:
        Taxonomy.objects.filter(
            accepted_taxonomy=instance,
            national_conservation_status__isnull=True
        ).update(national_conservation_status_id=instance_ncs_id)

    # --- Subgenus propagation ----
    if instance.parent and instance.parent.rank.lower() == 'subgenus' and not instance.subgenus:
        instance.subgenus = instance.parent


class TaxonImage(models.Model):

    taxon_image = models.ImageField(
        upload_to='taxon_images',
        null=True,
        blank=True
    )
    taxonomy = models.ForeignKey(
        Taxonomy, on_delete=models.CASCADE
    )
    source = models.CharField(
        max_length=256,
        blank=True,
        default=''
    )
    uploader = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        help_text='User who uploaded the taxon image (Optional)',
        related_name='taxon_image_uploader'
    )
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        help_text='Owner of the taxon image (Optional)',
        related_name='taxon_image_owner'
    )
    date = models.DateField(
        null=True,
        blank=True
    )
    survey = models.ForeignKey(
        'bims.Survey',
        null=True,
        blank=True,
        on_delete=models.CASCADE
    )


def check_taxa_duplicates(taxon_name, taxon_rank):
    """
    Check for taxa duplicates, then merge if found
    :param taxon_name: Name of the taxon to check
    :param taxon_rank: Rank of the taxon to check
    :return: Merged taxonomy
    """
    from django.db.models import Q, Min
    from bims.utils.gbif import get_species
    from bims.utils.fetch_gbif import merge_taxa_data

    taxon_rank = taxon_rank.strip().upper()
    taxon_name = taxon_name.strip()
    taxa = Taxonomy.objects.filter(
        Q(canonical_name__iexact=taxon_name) |
        Q(legacy_canonical_name__icontains=taxon_name),
        rank=taxon_rank
    )
    if not taxa.count() > 1:
        return
    preferred_taxa = taxa
    accepted_taxa = taxa.filter(taxonomic_status='ACCEPTED')
    if accepted_taxa.exists():
        preferred_taxa = accepted_taxa
    preferred_taxon = preferred_taxa.values('gbif_key', 'id').annotate(
        Min('gbif_key')).order_by('gbif_key')[0]
    preferred_taxon_gbif_data = get_species(preferred_taxon['gbif_key'])
    preferred_taxon = Taxonomy.objects.get(
        id=preferred_taxon['id']
    )
    for taxon in taxa[1:]:
        gbif_data = get_species(taxon.gbif_key)
        if not preferred_taxon_gbif_data:
            preferred_taxon = taxon
            preferred_taxon_gbif_data = gbif_data
            continue
        if not gbif_data:
            continue
        if gbif_data['taxonomicStatus'] == 'ACCEPTED':
            preferred_taxon_gbif_data = gbif_data
            preferred_taxon = taxon
            continue
        if 'issues' in gbif_data and len(gbif_data['issues']) > 0:
            continue
        if 'nubKey' not in gbif_data:
            continue
        if (
            'taxonomicStatus' in gbif_data and
            gbif_data['taxonomicStatus'] != 'ACCEPTED'
        ):
            continue
        if 'key' not in preferred_taxon_gbif_data:
            preferred_taxon = taxon
            preferred_taxon_gbif_data = gbif_data
            continue
        if (
            'key' in gbif_data and
            gbif_data['key'] > preferred_taxon_gbif_data['key']
        ):
            continue
        preferred_taxon = taxon
        preferred_taxon_gbif_data = gbif_data

    merge_taxa_data(
        taxa_list=taxa.exclude(id=preferred_taxon.id),
        excluded_taxon=preferred_taxon
    )
    return preferred_taxon
