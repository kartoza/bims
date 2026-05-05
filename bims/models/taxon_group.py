# coding=utf-8
"""Taxon group model definition.

"""
from django.conf import settings
from django.contrib.auth.models import Permission, Group
from django.contrib.gis.db import models
from django.contrib.sites.models import Site
from django.dispatch import receiver

from bims.cache import get_cache, delete_cache, set_cache
from bims.enums.taxonomic_group_category import TaxonomicGroupCategory
from bims.permissions.generate_permission import generate_permission
from bims.utils.decorator import prevent_recursion


TAXON_GROUP_LEVEL_1 = 'level_1'
TAXON_GROUP_LEVEL_2 = 'level_2'
TAXON_GROUP_LEVEL_3 = 'level_3'

TAXON_GROUP_CACHE = 'TGC'


class TaxonGroup(models.Model):

    CHART_CHOICES = (
        ('conservation status', 'Conservation Status'),
        ('division', 'Division'),
        ('sass', 'SASS'),
        ('origin', 'Origin'),
        ('endemism', 'Endemism'),
    )

    LEVEL_CHOICES = (
        (TAXON_GROUP_LEVEL_1, 'Level 1: Organism'),
        (TAXON_GROUP_LEVEL_2, 'Level 2: Regions and/or ecosystem type'),
        (TAXON_GROUP_LEVEL_3, 'Level 3: Country'),
    )

    name = models.CharField(
        max_length=200,
        null=False,
        blank=False
    )

    singular_name = models.CharField(
        max_length=200,
        null=True,
        blank=True
    )

    category = models.CharField(
        verbose_name='Taxonomic Group Category',
        max_length=50,
        choices=[(rank.name, rank.value) for rank in TaxonomicGroupCategory],
        blank=True,
    )

    logo = models.ImageField(
        upload_to='module_logo',
        null=True,
        blank=True
    )

    taxonomies = models.ManyToManyField(
        'bims.Taxonomy',
        blank=True,
        through='bims.TaxonGroupTaxonomy',
    )

    source_collection = models.CharField(
        help_text='Additional filter for search collections',
        max_length=200,
        null=True,
        blank=True
    )

    display_order = models.IntegerField(
        null=True,
        blank=True
    )

    chart_data = models.CharField(
        help_text='Data to display on chart',
        max_length=100,
        choices=CHART_CHOICES,
        null=True,
        blank=True,
        default=''
    )

    site = models.ForeignKey(
        Site,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        verbose_name="Associated Site",
        help_text="The site this taxon group is associated with."
    )

    additional_sites = models.ManyToManyField(
        to=Site,
        related_name='additional_sites',
        blank=True,
        help_text="A list of additional sites associated with this taxon group."
    )

    level = models.CharField(
        help_text='Level of the taxon group',
        max_length=100,
        choices=LEVEL_CHOICES,
        null=True,
        blank=True
    )

    parent = models.ForeignKey(
        verbose_name='Parent',
        to='self',
        on_delete=models.CASCADE,
        null=True,
        blank=True
    )

    experts = models.ManyToManyField(
        settings.AUTH_USER_MODEL,
        blank=True,
        null=True,
    )

    gbif_parent_species = models.ForeignKey(
        'bims.Taxonomy',
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='taxon_groups',
        help_text='Used to fetch species from GBIF'
    )

    taxa_upload_template = models.FileField(
        null=True,
        blank=True,
        help_text=(
            'File template for taxa data upload'
        )
    )

    occurrence_upload_template = models.FileField(
        null=True,
        blank=True,
        help_text=(
            'File template for occurrence data upload'
        )
    )

    col_enabled = models.BooleanField(
        default=False,
        help_text=(
            'Enable ColDP / ChecklistBank publishing for this module. '
            'Only modules with this flag set are listed in the public '
            'checklist API and included in external COL feeds.'
        ),
    )

    class Meta:
        ordering = ('display_order',)
        permissions = (
            ('can_validate_taxon_group', 'Can validate taxon group'),
        )

    def __str__(self):
        return f'{self.name} - {self.level}'

    def list_all_occurrence_templates(self) -> list:
        """
        Returns a list of template-like objects.
        """
        templates = list(self.occurrence_upload_templates.all())
        if self.occurrence_upload_template:
            temp = OccurrenceUploadTemplate(
                taxon_group=self,
                file=self.occurrence_upload_template,
                label='Legacy template'
            )
            templates.insert(0, temp)
        return templates

    def get_top_level_parent(self):
        """
        Recursively finds the top-level parent of the current taxon group.

        If the current taxon group is a top-level group (has no parent),
        it returns itself.

        :return: TaxonGroup: The top-level parent taxon group.
        """
        if not self.parent:
            return self
        else:
            return self.parent.get_top_level_parent()

    def get_all_experts(self) -> set:
        """
        Recursively collects all unique experts from the current
        TaxonGroup and its entire ancestry.

        :return: A set of unique expert objects associated with the
            TaxonGroup and its ancestors.
        """
        all_experts = set()

        def collect_experts(current_taxon_group):
            all_experts.update(current_taxon_group.experts.all())

            if current_taxon_group.parent:
                collect_experts(current_taxon_group.parent)

        collect_experts(self)
        return all_experts

    def get_all_children(self):
        def get_children(parent):
            children = list(TaxonGroup.objects.filter(parent=parent))
            for child in children:
                children.extend(get_children(child))
            return children
        return get_children(self)

    @property
    def permission_name(self):
        return f'Can validate {self.name} - {self.level}'

    @property
    def group_name(self):
        levels = dict(TaxonGroup.LEVEL_CHOICES)
        if self.level and self.level in levels:
            level = levels[self.level]
        else:
            level = ''
        return f'{level} : {self.name}'

    @property
    def permission_codename(self):
        return self.permission_name.lower().replace(' ', '_')


class OccurrenceUploadTemplate(models.Model):
    taxon_group = models.ForeignKey(
        'bims.TaxonGroup',
        related_name='occurrence_upload_templates',
        on_delete=models.CASCADE,
    )
    file = models.FileField(
        upload_to='occurrence_templates/',
        help_text='Occurrence data upload template file.'
    )
    label = models.CharField(
        max_length=150,
        blank=True,
        help_text='Optional display name (e.g. “Standard”, “With traits”).'
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ('created_at',)

    def __str__(self):
        base = self.label or self.file.name.split('/')[-1]
        return f'{base} ({self.taxon_group.name})'


@receiver(models.signals.pre_save)
def taxon_group_pre_save(sender, instance: TaxonGroup, **kwargs):
    if not issubclass(sender, TaxonGroup):
        return

    if instance.pk:
        current_instance = TaxonGroup.objects.filter(
            pk=instance.pk).first()

        if current_instance:
            if (
                current_instance.name != instance.name or
                    current_instance.level != instance.level
            ):
                old_permission_codename = (
                    current_instance.permission_codename
                )
                new_permission_codename = (
                    instance.permission_codename
                )
                new_permission_name = instance.permission_name
                Permission.objects.filter(
                    codename=old_permission_codename
                ).update(
                    name=new_permission_name,
                    codename=new_permission_codename
                )
                if instance.level:
                    old_group_name = current_instance.group_name
                    Group.objects.filter(
                        name=old_group_name
                    ).update(
                        name=instance.group_name
                    )


def add_permission_to_parent(taxon_group: TaxonGroup, permission: Permission):
    if taxon_group.parent:
        add_permission_to_parent(taxon_group.parent, permission)
    group = Group.objects.filter(
        name=taxon_group.group_name
    ).first()
    if group:
        group.permissions.add(permission)


def cache_taxon_groups_data(delete_first=False):
    from bims.serializers.taxon_serializer import TaxonGroupSerializer
    taxa_groups_query = TaxonGroup.objects.filter(
        category='SPECIES_MODULE',
        parent__isnull=True
    ).order_by('display_order')
    taxon_groups_data = TaxonGroupSerializer(
        taxa_groups_query, many=True).data
    if delete_first:
        delete_cache(TAXON_GROUP_CACHE)
    set_cache(TAXON_GROUP_CACHE, taxon_groups_data)


@receiver(models.signals.post_save)
@prevent_recursion
def taxon_group_post_save(sender, instance: TaxonGroup, created, **kwargs):

    from bims.tasks.module_summary import generate_module_summary
    from bims.tasks.taxon_group import update_taxon_group_cache
    from django.db import connection
    from django.conf import settings

    if not issubclass(sender, TaxonGroup):
        return

    if not getattr(settings, 'TESTING', False):
        schema_name = str(connection.schema_name)
        generate_module_summary.delay(schema_name)

        taxon_group_cache = get_cache(TAXON_GROUP_CACHE)
        if taxon_group_cache:
            update_taxon_group_cache.delay(delete_first=True)

    if not instance.site:
        return

    if instance.level in (
        TAXON_GROUP_LEVEL_1,
        TAXON_GROUP_LEVEL_2,
        TAXON_GROUP_LEVEL_3
    ):
        permission = generate_permission(
            instance.permission_name,
            'can_validate_taxon_group'
        )
        group, created = Group.objects.get_or_create(
            name=instance.group_name)
        group.permissions.add(permission)

        if instance.parent:
            add_permission_to_parent(
                instance.parent, permission)
