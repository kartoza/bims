# coding=utf-8
"""Rebuild empty family/genus/species names in Taxonomy.hierarchical_data.

The taxa list filters (e.g. by family) read the names cached in
hierarchical_data. When a taxon was created before its parent chain was
linked, and the parent was then set with save(update_fields=['parent']),
the cache kept an empty family_name, so the taxon disappeared from a family
filter even though its parent chain is correct (e.g. Thraulus species under
a subgenus).

Only taxa at or below the rank of the missing name are touched, and names
are recomputed from the current parent chain.
"""
try:
    from django_tenants.utils import get_tenant_model, schema_context
except ImportError:  # pragma: no cover - tenant support optional
    get_tenant_model = None
    schema_context = None

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q

from bims.enums.taxonomic_rank import TaxonomicRank

KEY_RANKS = [
    ('family_name', TaxonomicRank.FAMILY.name),
    ('genus_name', TaxonomicRank.GENUS.name),
]


def _ranks_from(rank_name):
    order = [r.name for r in TaxonomicRank.hierarchy()]
    return order[order.index(rank_name):]


def _empty(key):
    return (
        Q(hierarchical_data__isnull=True)
        | ~Q(hierarchical_data__has_key=key)
        | Q(**{f'hierarchical_data__{key}': ''})
        | Q(**{f'hierarchical_data__{key}__isnull': True})
    )


def _species_epithet(taxon):
    species_name = taxon.species_name or ''
    genus_name = taxon.genus_name or ''
    if genus_name and genus_name in species_name:
        species_name = species_name.split(genus_name)[-1].strip()
    return species_name


class Command(BaseCommand):
    help = (
        "Rebuild empty family/genus/species names cached in "
        "Taxonomy.hierarchical_data. Supports --tenant/--all-tenants."
    )

    def add_arguments(self, parser):
        parser.add_argument("--tenant", dest="tenant", default=None)
        parser.add_argument(
            "--all-tenants", dest="all_tenants", action="store_true",
            default=False,
        )
        parser.add_argument(
            "--dry-run", dest="dry_run", action="store_true", default=False,
            help="Report what would change without saving.",
        )

    def handle(self, *args, **options):
        schema = options.get("tenant")
        if schema and options.get("all_tenants"):
            raise CommandError("Use either --tenant or --all-tenants, not both.")
        if not (schema or options.get("all_tenants")):
            self._run(options["dry_run"])
            return
        if schema_context is None or get_tenant_model is None:
            raise CommandError("django-tenants is required for tenant iteration.")
        if schema:
            schemas = [schema]
        else:
            schemas = list(
                get_tenant_model().objects.exclude(schema_name="public")
                .values_list("schema_name", flat=True)
            )
        for name in schemas:
            self.stdout.write(f"== {name} ==")
            with schema_context(name):
                self._run(options["dry_run"])

    def _run(self, dry_run):
        from bims.models.taxonomy import Taxonomy

        stale = Q()
        for key, rank in KEY_RANKS:
            stale |= Q(rank__in=_ranks_from(rank)) & _empty(key)
        stale |= (
            Q(rank__in=_ranks_from(TaxonomicRank.SPECIES.name))
            & _empty('species_name')
        )

        updated = 0
        checked = 0
        for taxon in Taxonomy.objects.filter(stale).iterator(chunk_size=500):
            checked += 1
            data = dict(taxon.hierarchical_data or {})
            new = dict(data)
            for key, rank in KEY_RANKS:
                if not new.get(key) and taxon.rank in _ranks_from(rank):
                    new[key] = taxon.get_taxon_rank_name(rank) or ''
            if (
                not new.get('species_name')
                and taxon.rank in _ranks_from(TaxonomicRank.SPECIES.name)
            ):
                new['species_name'] = _species_epithet(taxon)
            if new == data:
                continue
            updated += 1
            if dry_run:
                self.stdout.write(
                    f"  id={taxon.id} {taxon.rank} '{taxon.canonical_name}': "
                    f"{data} -> {new}"
                )
                continue
            # update() skips Taxonomy.save() side effects (GBIF fetch etc.)
            Taxonomy.objects.filter(pk=taxon.pk).update(hierarchical_data=new)

        verb = "Would update" if dry_run else "Updated"
        self.stdout.write(self.style.SUCCESS(
            f"{verb} {updated} of {checked} taxa with empty cached names."
        ))
