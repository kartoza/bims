# coding=utf-8
"""
Tenant-aware management command that inspects the additional_data JSON field
on every BiologicalCollectionRecord and, where the record is missing a broad
or specific biotope FK, looks up (or creates) the matching Biotope object and
assigns it.

Keys checked (case-insensitive substring match):
  broad    – "Broad biotope/habitat"
  specific – "Specific biotope/habitat"
"""

import re
import logging

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django_tenants.utils import get_tenant_model, tenant_context

logger = logging.getLogger(__name__)

# Keys we look for inside additional_data (lowercased for comparison)
BROAD_KEYS = {"broad biotope/habitat", "broad biotope", "broad habitat"}
SPECIFIC_KEYS = {"specific biotope/habitat", "specific biotope", "specific habitat"}


def _normalise(value):
    """Strip and collapse whitespace."""
    return re.sub(r'\s+', ' ', str(value).strip())


class Command(BaseCommand):
    help = (
        "For each tenant: scan BiologicalCollectionRecord.additional_data and "
        "populate the broad/specific biotope FK from 'Broad biotope/habitat' "
        "and 'Specific biotope/habitat' entries when they are missing."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "-s", "--schema-name",
            dest="schema_names",
            action="append",
            help="Limit to specific tenant schema(s). Can be passed multiple times.",
        )
        parser.add_argument(
            "--all-tenants",
            action="store_true",
            help="Process all tenants (default when no --schema-name is given).",
        )
        parser.add_argument(
            "--overwrite",
            action="store_true",
            help=(
                "Also overwrite records that already have a biotope FK set "
                "(default: skip records that already have values)."
            ),
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report what would change without writing to the database.",
        )

    # ------------------------------------------------------------------
    # handle
    # ------------------------------------------------------------------

    def handle(self, *args, **options):
        schema_names = options.get("schema_names") or []
        all_tenants = options.get("all_tenants", False)
        dry_run = options.get("dry_run", False)
        overwrite = options.get("overwrite", False)

        if schema_names and all_tenants:
            raise CommandError("Use either --schema-name or --all-tenants, not both.")

        tenants = self._get_tenants(schema_names)
        if not tenants:
            self.stdout.write(self.style.WARNING("No tenants found."))
            return

        mode = "DRY RUN" if dry_run else "LIVE"
        self.stdout.write(
            f"{mode}: processing {len(tenants)} tenant(s), "
            f"overwrite={'yes' if overwrite else 'no'}."
        )

        for tenant in tenants:
            self.stdout.write(
                f"\n── Tenant: {tenant.name} (schema: {tenant.schema_name})"
            )
            with tenant_context(tenant):
                if dry_run:
                    self._process_tenant(dry_run=True, overwrite=overwrite)
                else:
                    with transaction.atomic():
                        self._process_tenant(dry_run=False, overwrite=overwrite)

        self.stdout.write(self.style.SUCCESS("\nDone."))

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------

    def _get_tenants(self, schema_names):
        Tenant = get_tenant_model()
        qs = Tenant.objects.exclude(schema_name="public")
        if schema_names:
            qs = qs.filter(schema_name__in=schema_names)
            missing = set(schema_names) - set(qs.values_list("schema_name", flat=True))
            if missing:
                raise CommandError(
                    f"Tenant schema(s) not found: {', '.join(sorted(missing))}"
                )
        return list(qs.order_by("schema_name"))

    def _process_tenant(self, dry_run, overwrite):
        from bims.models.biological_collection_record import BiologicalCollectionRecord
        from bims.models.biotope import Biotope, BIOTOPE_TYPE_BROAD, BIOTOPE_TYPE_SPECIFIC

        qs = BiologicalCollectionRecord.objects.exclude(
            additional_data=None
        )
        if not overwrite:
            qs = qs.filter(biotope__isnull=True, specific_biotope__isnull=True)

        total = qs.count()
        self.stdout.write(f"  Records to process: {total}")

        broad_updated = 0
        specific_updated = 0
        skipped = 0

        for record in qs.iterator(chunk_size=500):
            data = record.additional_data
            if not isinstance(data, dict):
                skipped += 1
                continue

            # Build a lowercase-keyed view for easy lookup
            lower_data = {k.lower().strip(): v for k, v in data.items()}

            broad_value = self._find_value(lower_data, BROAD_KEYS)
            specific_value = self._find_value(lower_data, SPECIFIC_KEYS)

            changed = False

            if broad_value and (overwrite or not record.biotope_id):
                biotope = self._get_or_create_biotope(
                    broad_value, BIOTOPE_TYPE_BROAD, dry_run
                )
                if biotope:
                    if not dry_run:
                        record.biotope = biotope
                    broad_updated += 1
                    changed = True

            if specific_value and (overwrite or not record.specific_biotope_id):
                biotope = self._get_or_create_biotope(
                    specific_value, BIOTOPE_TYPE_SPECIFIC, dry_run
                )
                if biotope:
                    if not dry_run:
                        record.specific_biotope = biotope
                    specific_updated += 1
                    changed = True

            if changed and not dry_run:
                BiologicalCollectionRecord.objects.filter(pk=record.pk).update(
                    biotope=record.biotope,
                    specific_biotope=record.specific_biotope,
                )

        prefix = "[DRY RUN] " if dry_run else ""
        self.stdout.write(
            self.style.SUCCESS(
                f"  {prefix}broad biotope set: {broad_updated} | "
                f"specific biotope set: {specific_updated} | "
                f"skipped (no dict): {skipped}"
            )
        )

    @staticmethod
    def _find_value(lower_data, key_set):
        """Return the first non-empty value whose key matches any entry in key_set."""
        for key in key_set:
            if key in lower_data:
                v = _normalise(lower_data[key])
                if v:
                    return v
        return None

    @staticmethod
    def _get_or_create_biotope(name, biotope_type, dry_run):
        """
        Return an existing Biotope with the given name+type, or create one.
        Returns None in dry-run mode (so no DB writes occur).
        """
        from bims.models.biotope import Biotope

        try:
            biotope, created = Biotope.objects.get_or_create(
                name=name,
                biotope_type=biotope_type,
            )
            return None if dry_run and created else biotope
        except Biotope.MultipleObjectsReturned:
            return Biotope.objects.filter(
                name=name, biotope_type=biotope_type
            ).first()
