# coding=utf-8
"""
ChecklistVersion and ChecklistSnapshot — versioned taxonomy publishing
in Catalogue of Life Data Package (ColDP) format.

Architecture
------------
ChecklistVersion
    One record per module (TaxonGroup) release.  Carries version string,
    DOI, status, and the approved-proposal changelog.

ChecklistSnapshot
    Pre-rendered, write-once table of ColDP NameUsage rows — one row per
    taxon per published version.
"""
import uuid as _uuid

from django.conf import settings
from django.db import models


class ChecklistSnapshot(models.Model):
    """
    One pre-rendered checklist NameUsage row per taxon per ChecklistVersion.
    """

    CHANGE_ADDED     = 'added'
    CHANGE_UPDATED   = 'updated'
    CHANGE_UNCHANGED = 'unchanged'

    CHANGE_CHOICES = [
        (CHANGE_ADDED,     'Added'),
        (CHANGE_UPDATED,   'Updated'),
        (CHANGE_UNCHANGED, 'Unchanged'),
    ]

    checklist_version = models.ForeignKey(
        'ChecklistVersion',
        on_delete=models.CASCADE,
        related_name='snapshot_rows',
        db_column='checklist_version_id',
        db_index=True,
    )
    checklist_id = models.CharField(
        max_length=255,
        db_index=True,
        help_text='Stable taxon identifier used in checklist (str of Taxonomy.pk).',
    )
    parent_checklist_id = models.CharField(
        max_length=255,
        blank=True,
        default='',
        help_text='checklist_id of the parent taxon.',
    )
    basionym_checklist_id = models.CharField(
        max_length=255,
        blank=True,
        default='',
        help_text='checklist_id of the accepted taxon for synonyms.',
    )
    rank = models.CharField(max_length=50, blank=True, default='')
    scientific_name = models.CharField(max_length=512, db_index=True)
    authorship = models.CharField(max_length=255, blank=True, default='')
    taxonomic_status = models.CharField(
        max_length=50,
        blank=True,
        default='',
        help_text='accepted, synonym, ambiguous synonym, misapplied, etc.',
    )
    name_status = models.CharField(
        max_length=50,
        blank=True,
        default='',
        help_text='establishmentMeans / name status from ColDP.',
    )

    kingdom  = models.CharField(max_length=200, blank=True, default='')
    phylum   = models.CharField(max_length=200, blank=True, default='')
    klass    = models.CharField(max_length=200, blank=True, default='',
                                db_column='class')
    order    = models.CharField(max_length=200, blank=True, default='')
    family   = models.CharField(max_length=200, blank=True, default='')
    genus    = models.CharField(max_length=200, blank=True, default='')

    vernacular_names = models.JSONField(
        default=list,
        help_text='Snapshot of [{name, language}] at publish time.',
    )
    distributions = models.JSONField(
        default=list,
        help_text='Snapshot of [{area, status}] at publish time.',
    )
    reference_id = models.CharField(
        max_length=255,
        blank=True,
        default='',
        help_text='ColDP Reference.ID for the source reference.',
    )
    remarks = models.TextField(
        blank=True,
        default='',
        help_text='Free-text remarks field in ColDP NameUsage.',
    )

    change_type = models.CharField(
        max_length=10,
        choices=CHANGE_CHOICES,
        default=CHANGE_UNCHANGED,
        db_index=True,
        help_text='Whether this taxon was added, updated, or unchanged in this version.',
    )

    class Meta:
        unique_together     = [('checklist_version', 'checklist_id')]
        verbose_name        = 'Checklist Snapshot Row'
        verbose_name_plural = 'Checklist Snapshot Rows'
        indexes = [
            models.Index(fields=['checklist_id', 'checklist_version']),
        ]

    def __str__(self):
        return f'{self.scientific_name} [{self.checklist_version}]'


class ChecklistVersion(models.Model):

    STATUS_DRAFT     = 'draft'
    STATUS_PUBLISHED = 'published'

    STATUS_CHOICES = [
        (STATUS_DRAFT,     'Draft'),
        (STATUS_PUBLISHED, 'Published'),
    ]

    id = models.UUIDField(
        primary_key=True,
        default=_uuid.uuid4,
        editable=False,
        help_text=(
            'Stable UUID for this release. '
            'Embedded in generated PDFs and returned by the API.'
        ),
    )

    taxon_group = models.ForeignKey(
        'bims.TaxonGroup',
        on_delete=models.CASCADE,
        related_name='checklist_versions',
        db_column='taxon_group_id',
        help_text='The module (TaxonGroup) this version belongs to.',
    )

    checklist = models.ForeignKey(
        'bims.TaxonomyChecklist',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='versions',
        db_column='checklist_id',
        help_text='Optional parent dataset record (ColDP dataset-level metadata).',
    )

    version = models.CharField(
        max_length=50,
        help_text='Human-readable version string, e.g. "1.0", "2025.1".',
    )

    previous_version = models.ForeignKey(
        'self',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='next_versions',
        db_column='previous_version_id',
        help_text='Immediately preceding published version for this module.',
    )

    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default=STATUS_DRAFT,
    )

    doi = models.CharField(
        max_length=255,
        blank=True,
        default='',
        help_text='DOI assigned at publish time, e.g. https://doi.org/10.XXXX/YYYY.',
    )

    dataset_key = models.CharField(
        max_length=255,
        blank=True,
        default='',
        help_text='ChecklistBank / COL dataset key returned after upload.',
    )

    license = models.ForeignKey(
        'bims.Licence',
        null=False,
        blank=False,
        on_delete=models.CASCADE
    )

    notes = models.TextField(
        blank=True,
        default='',
        help_text='Internal release notes visible to editors.',
    )

    taxa_count      = models.IntegerField(default=0)
    additions_count = models.IntegerField(default=0)
    updates_count   = models.IntegerField(default=0)

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='checklist_versions_created',
        db_column='created_by_id',
    )
    published_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='checklist_versions_published',
        db_column='published_by_id',
    )

    created_at   = models.DateTimeField(auto_now_add=True)
    published_at = models.DateTimeField(null=True, blank=True)

    _DIFF_FIELDS = (
        'scientific_name', 'rank', 'authorship', 'taxonomic_status',
        'parent_checklist_id', 'basionym_checklist_id',
        'kingdom', 'phylum', 'klass', 'order', 'family', 'genus',
        'vernacular_names', 'distributions', 'reference_id',
    )

    def publish(self, published_by=None):
        """
        Transition this version to published status.
        """
        from django.utils import timezone
        from bims.models.taxonomy import Taxonomy

        if self.status == self.STATUS_PUBLISHED:
            return

        # Collect all descendant taxon group IDs
        descendant_groups = self.taxon_group.get_all_children()
        taxon_group_ids = [self.taxon_group_id]
        taxon_group_ids.extend(group.id for group in descendant_groups)

        # Build a lookup of previous snapshot rows keyed by checklist_id
        prev_snapshot = {}
        if self.previous_version_id:
            prev_snapshot = {
                row['checklist_id']: row
                for row in ChecklistSnapshot.objects.filter(
                    checklist_version_id=self.previous_version_id
                ).values('checklist_id', *self._DIFF_FIELDS)
            }

        rows = []
        additions = 0
        updates = 0

        for taxonomy in (
            Taxonomy.objects.filter(
                taxongrouptaxonomy__taxongroup_id__in=taxon_group_ids
            )
            .distinct()
            .select_related('parent', 'accepted_taxonomy', 'source_reference')
            .prefetch_related('vernacular_names', 'biographic_distributions')
        ):
            row = self.build_snapshot_row(taxonomy, ChecklistSnapshot.CHANGE_UNCHANGED)
            cid = str(taxonomy.pk)

            if cid not in prev_snapshot:
                row.change_type = ChecklistSnapshot.CHANGE_ADDED
                additions += 1
            else:
                prev = prev_snapshot[cid]
                changed = any(
                    getattr(row, field) != prev[field]
                    for field in self._DIFF_FIELDS
                )
                if changed:
                    row.change_type = ChecklistSnapshot.CHANGE_UPDATED
                    updates += 1

            rows.append(row)

        ChecklistSnapshot.objects.bulk_create(rows, ignore_conflicts=True)

        self.taxa_count      = len(rows)
        self.additions_count = additions
        self.updates_count   = updates
        self.status          = self.STATUS_PUBLISHED
        self.published_at    = timezone.now()
        self.published_by    = published_by
        self.save(update_fields=[
            'status', 'published_at', 'published_by',
            'taxa_count', 'additions_count', 'updates_count',
        ])

    def build_snapshot_row(self, taxonomy, change_type):
        """
        Construct a ChecklistSnapshot instance (not yet saved) from a
        Taxonomy object.  All lookups happen here so export is a plain
        table dump later.
        """

        vernacular_names = [
            {'name': v.name, 'language': v.language}
            for v in taxonomy.vernacular_names.all()
        ]
        distributions = [
            {'area': tag.name}
            for tag in taxonomy.biographic_distributions.all()
        ]

        return ChecklistSnapshot(
            checklist_version=self,
            checklist_id=str(taxonomy.pk),
            parent_checklist_id=str(taxonomy.parent_id) if taxonomy.parent_id else '',
            basionym_checklist_id=(
                str(taxonomy.accepted_taxonomy_id)
                if taxonomy.accepted_taxonomy_id else ''
            ),
            rank=taxonomy.rank or '',
            scientific_name=taxonomy.scientific_name or '',
            authorship=taxonomy.author or '',
            taxonomic_status=taxonomy.taxonomic_status or '',
            kingdom=taxonomy.kingdom_name,
            phylum=taxonomy.phylum_name,
            klass=taxonomy.class_name,
            order=taxonomy.order_name,
            family=taxonomy.family_name,
            genus=taxonomy.genus_name,
            vernacular_names=vernacular_names,
            distributions=distributions,
            reference_id=(
                str(taxonomy.source_reference_id)
                if taxonomy.source_reference_id else ''
            ),
            change_type=change_type,
        )

    @property
    def changelog_summary(self):
        return {
            'additions': self.additions_count,
            'updates':   self.updates_count,
            'total':     self.additions_count + self.updates_count,
        }

    class Meta:
        verbose_name        = 'Checklist Version'
        verbose_name_plural = 'Checklist Versions'
        ordering            = ['-created_at']
        unique_together     = [('taxon_group', 'version')]

    def __str__(self):
        return f'{self.taxon_group.name} v{self.version} [{self.status}]'
