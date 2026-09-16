from django.shortcuts import get_object_or_404
from rest_framework.response import Response
from rest_framework.views import APIView

from bims.api_views.taxon_update import is_expert, is_contributor
from bims.enums import TaxonomicStatus
from bims.models.taxon_group import TaxonGroup
from bims.serializers.taxon_serializer import TaxonGroupSerializer
from bims.enums.taxonomic_group_category import TaxonomicGroupCategory


class TaxonGroupList(APIView):
    """View to return list of taxon group"""
    def get(self, request, *args, **kwargs):
        taxon_group = TaxonGroup.objects.filter(
            category=TaxonomicGroupCategory.SPECIES_MODULE.name
        )
        return Response(TaxonGroupSerializer(
            taxon_group, many=True
        ).data)
from django.db.models import Q
from django.shortcuts import get_object_or_404
from rest_framework.response import Response
from rest_framework.views import APIView


class TaxonGroupTotalValidated(APIView):
    """
    Return counts for a taxon group by:
      - accepted_validated
      - accepted_unvalidated
      - synonym_validated
      - synonym_unvalidated
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.accepted_validated = 0
        self.accepted_unvalidated = 0
        self.synonym_validated = 0
        self.synonym_unvalidated = 0
        self.doubtful_validated = 0
        self.doubtful_unvalidated = 0
        self.unaccepted_validated = 0
        self.unaccepted_unvalidated = 0

    def _status_queries(self):
        accepted_names = {
            TaxonomicStatus.ACCEPTED.name,
            TaxonomicStatus.DOUBTFUL.name,
        }
        synonym_names = {
            TaxonomicStatus.SYNONYM.name,
            TaxonomicStatus.HETEROTYPIC_SYNONYM.name,
            TaxonomicStatus.HOMOTYPIC_SYNONYM.name,
            TaxonomicStatus.PROPARTE_SYNONYM.name,
            TaxonomicStatus.MISAPPLIED.name,
        }

        accepted_q = (
            Q(taxonomic_status__in=accepted_names) |
            Q(taxonomic_status__isnull=True) |
            Q(taxonomic_status="")
        )
        synonym_q = Q(taxonomic_status__in=synonym_names)
        doubtful_q = Q(taxonomic_status=TaxonomicStatus.DOUBTFUL.name)
        unaccepted_q = Q(taxonomic_status='UNACCEPTED')

        return accepted_q, synonym_q, doubtful_q, unaccepted_q

    def collect_taxonomy_ids(self, taxon_group, can_view_unvalidated=False):
        """
        Recursively aggregate counts for the given taxon group + children.
        can_view_unvalidated is computed once for the top-level group in get()
        and passed down so child groups inherit the same permission.
        """
        from bims.templatetags.site import is_fada_site
        accepted_q, synonym_q, doubtful_q, unaccepted_q = self._status_queries()

        qs = taxon_group.taxonomies.all()
        if is_fada_site():
            qs = qs.exclude(
                (Q(fada_id__isnull=True) | Q(fada_id='')) & Q(aphia_id__isnull=True) & Q(taxonworks_id__isnull=True)
            )

        # Validated
        self.accepted_validated += qs.filter(
            accepted_q,
            taxongrouptaxonomy__taxongroup=taxon_group,
            taxongrouptaxonomy__is_validated=True
        ).distinct().count()

        self.synonym_validated += qs.filter(
            synonym_q,
            taxongrouptaxonomy__taxongroup=taxon_group,
            taxongrouptaxonomy__is_validated=True
        ).distinct().count()

        self.doubtful_validated += qs.filter(
            doubtful_q,
            taxongrouptaxonomy__taxongroup=taxon_group,
            taxongrouptaxonomy__is_validated=True
        ).distinct().count()

        self.unaccepted_validated += qs.filter(
            unaccepted_q,
            taxongrouptaxonomy__taxongroup=taxon_group,
            taxongrouptaxonomy__is_validated=True
        ).distinct().count()

        # Unvalidated (masked if no permission)
        if can_view_unvalidated:
            self.accepted_unvalidated += qs.filter(
                accepted_q,
                taxongrouptaxonomy__taxongroup=taxon_group,
                taxongrouptaxonomy__is_validated=False
            ).distinct().count()

            self.synonym_unvalidated += qs.filter(
                synonym_q,
                taxongrouptaxonomy__taxongroup=taxon_group,
                taxongrouptaxonomy__is_validated=False
            ).distinct().count()

            self.doubtful_unvalidated += qs.filter(
                doubtful_q,
                taxongrouptaxonomy__taxongroup=taxon_group,
                taxongrouptaxonomy__is_validated=False
            ).distinct().count()

            self.unaccepted_unvalidated += qs.filter(
                unaccepted_q,
                taxongrouptaxonomy__taxongroup=taxon_group,
                taxongrouptaxonomy__is_validated=False
            ).distinct().count()

        # Recurse into children, propagating the same permission flag
        for child in TaxonGroup.objects.filter(parent=taxon_group):
            self.collect_taxonomy_ids(child, can_view_unvalidated=can_view_unvalidated)

    def get(self, request, *args, **kwargs):
        """
        GET: return counts split by accepted/synonym and validated/unvalidated.
        Unvalidated counts are 0 for non-privileged users.
        """
        taxon_group_id = kwargs.get("id")
        taxon_group = get_object_or_404(TaxonGroup, id=taxon_group_id)

        is_user_expert = is_expert(request.user, taxon_group)
        is_user_contributor = is_contributor(request.user, taxon_group)
        can_view_unvalidated = request.user.is_superuser or is_user_expert or is_user_contributor

        self.collect_taxonomy_ids(taxon_group, can_view_unvalidated=can_view_unvalidated)

        accepted_unvalidated = self.accepted_unvalidated if can_view_unvalidated else 0
        synonym_unvalidated = self.synonym_unvalidated if can_view_unvalidated else 0
        doubtful_unvalidated = self.doubtful_unvalidated if can_view_unvalidated else 0
        unaccepted_unvalidated = self.unaccepted_unvalidated if can_view_unvalidated else 0

        total_validated = (
            self.accepted_validated
            + self.synonym_validated
            + self.unaccepted_validated
        )
        total_unvalidated = (
            accepted_unvalidated
            + synonym_unvalidated
            + unaccepted_unvalidated
        )

        return Response({
            "accepted_validated": self.accepted_validated,
            "accepted_unvalidated": accepted_unvalidated,
            "synonym_validated": self.synonym_validated,
            "synonym_unvalidated": synonym_unvalidated,
            "doubtful_validated": self.doubtful_validated,
            "doubtful_unvalidated": doubtful_unvalidated,
            "unaccepted_validated": self.unaccepted_validated,
            "unaccepted_unvalidated": unaccepted_unvalidated,

            "total_validated": total_validated,
            "total_unvalidated": total_unvalidated,
        })
