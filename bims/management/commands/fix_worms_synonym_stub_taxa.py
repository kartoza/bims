# coding=utf-8
"""Find and repair "accepted taxon" stubs created by the WoRMS synonym
harvest bug.

Before the fix in bims/scripts/taxa_upload_worms.py, when the harvester
processed a WoRMS synonym it fabricated the synonym's accepted taxon as a
bare local stub (name/rank only, taken from the synonym row's own
"accepted" columns) instead of fetching that taxon's own WoRMS record.
That stub was then unconditionally marked validated and given a FADA-ID
via ensure_accepted_taxonomy_in_group(), regardless of the
auto_validate_taxa_on_upload site setting and without ever being
independently confirmed against WoRMS.

This command finds taxa matching that stub signature - no aphia_id of
their own, yet pointed at by at least one WoRMS-sourced synonym - and,
unless --dry-run is passed, re-fetches the real record from WoRMS using
the AphiaID_accepted recorded on the referencing synonym, then re-syncs
the taxon's core fields and validation/FADA state to match.
"""
import sys

try:
    from django_tenants.utils import get_tenant_model, schema_context
except ImportError:  # pragma: no cover - tenant support optional
    get_tenant_model = None
    schema_context = None

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction


class Command(BaseCommand):
    help = (
        "Find taxa that were fabricated as the 'accepted' target of a "
        "WoRMS synonym harvest (never independently confirmed against "
        "WoRMS) and repair them. Supports multi-tenant deployments via "
        "--tenant/--all-tenants."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--tenant",
            dest="tenant",
            default=None,
            help="Tenant schema name to run against.",
        )
        parser.add_argument(
            "--all-tenants",
            dest="all_tenants",
            action="store_true",
            default=False,
            help="Iterate through every tenant schema (excludes public).",
        )
        parser.add_argument(
            "--dry-run",
            dest="dry_run",
            action="store_true",
            default=False,
            help="Report suspect taxa without changing anything.",
        )

    def handle(self, *args, **options):
        schema = options.get("tenant")
        all_tenants = options.get("all_tenants")

        if schema and all_tenants:
            raise CommandError("Use either --tenant or --all-tenants, not both.")

        if schema:
            self._run_for_schema(schema, options)
            return

        if all_tenants:
            if schema_context is None or get_tenant_model is None:
                raise CommandError("django-tenants is required for tenant iteration.")

            TenantModel = get_tenant_model()
            tenants_qs = TenantModel.objects.exclude(schema_name="public")
            if not tenants_qs.exists():
                self.stdout.write(self.style.WARNING("No tenant schemas found."))
                return

            for tenant in tenants_qs:
                self._run_in_schema(tenant.schema_name, options)
            return

        self.stdout.write(self.style.HTTP_INFO("Running in current schema"))
        self._fix(options)

    def _run_for_schema(self, schema_name: str, options):
        if schema_context is None or get_tenant_model is None:
            self.stderr.write("This command requires django-tenants but it is not available.")
            sys.exit(1)

        tenant = self._get_tenant(schema_name)
        if not tenant:
            sys.exit(1)

        self._run_in_schema(tenant.schema_name, options)

    def _run_in_schema(self, schema_name: str, options):
        with schema_context(schema_name):
            self.stdout.write(self.style.HTTP_INFO(f"Running in tenant schema: {schema_name}"))
            self._fix(options)

    def _get_tenant(self, schema_name):
        TenantModel = get_tenant_model()
        try:
            return TenantModel.objects.get(schema_name=schema_name)
        except TenantModel.DoesNotExist:
            self.stderr.write(f"Tenant with schema '{schema_name}' not found.")
            return None

    def _find_candidates(self):
        from django.db.models import Q
        from bims.models import Taxonomy

        return (
            Taxonomy.objects.filter(aphia_id__isnull=True)
            .filter(synonym__aphia_id__isnull=False)
            .filter(
                Q(fada_id__isnull=False, fada_id__gt="")
                | Q(taxongrouptaxonomy__is_validated=True)
            )
            .distinct()
        )

    def _fix(self, options):
        from preferences import preferences
        from bims.models import Taxonomy, TaxonGroup, TaxonGroupTaxonomy
        from bims.scripts.taxa_upload_worms import (
            WormsTaxaProcessor, WORMS_COLUMN_NAMES,
        )
        from bims.utils.worms import get_aphia_record, api_record_to_csv_row

        dry_run = options.get("dry_run", False)

        candidates = list(self._find_candidates())
        if not candidates:
            self.stdout.write(self.style.SUCCESS("No suspect stub taxa found."))
            return

        self.stdout.write(
            f"Found {len(candidates)} suspect stub taxa "
            f"(no aphia_id, referenced by a WoRMS synonym, validated "
            f"and/or has a FADA-ID):"
        )

        if dry_run:
            for t in candidates:
                groups = list(
                    TaxonGroup.objects.filter(taxonomies=t).values_list(
                        "name", flat=True
                    )
                )
                self.stdout.write(
                    f"  [dry-run] id={t.id} '{t.canonical_name}' "
                    f"rank={t.rank} fada_id={t.fada_id or '-'} "
                    f"groups={groups}"
                )
            return

        processor = WormsTaxaProcessor()
        auto_validate = preferences.SiteSetting.auto_validate_taxa_on_upload

        fixed, unverifiable, no_reference = 0, 0, 0

        for stub in candidates:
            synonym = stub.synonym.filter(aphia_id__isnull=False).first()
            if not synonym:
                no_reference += 1
                self.stdout.write(
                    self.style.WARNING(
                        f"  id={stub.id} '{stub.canonical_name}': no "
                        f"referencing WoRMS synonym found - skipped, "
                        f"needs manual review."
                    )
                )
                continue

            additional_data = synonym.additional_data or {}
            accepted_aphia_id = additional_data.get(
                WORMS_COLUMN_NAMES["aphia_id_acc"]
            )
            try:
                accepted_aphia_id = int(accepted_aphia_id)
            except (TypeError, ValueError):
                accepted_aphia_id = None

            if not accepted_aphia_id:
                no_reference += 1
                self.stdout.write(
                    self.style.WARNING(
                        f"  id={stub.id} '{stub.canonical_name}': "
                        f"referencing synonym id={synonym.id} has no "
                        f"AphiaID_accepted on record - skipped, needs "
                        f"manual review."
                    )
                )
                continue

            record = get_aphia_record(accepted_aphia_id)
            if not record:
                unverifiable += 1
                self.stdout.write(
                    self.style.WARNING(
                        f"  id={stub.id} '{stub.canonical_name}': WoRMS "
                        f"AphiaID={accepted_aphia_id} could not be "
                        f"fetched - left unchanged."
                    )
                )
                continue

            row = api_record_to_csv_row(record)
            groups = list(TaxonGroup.objects.filter(taxonomies=stub))

            with transaction.atomic():
                for group in groups:
                    processor.process_worms_data(
                        row, group, harvest_synonyms=False, fetch_col_id=False,
                    )
                    TaxonGroupTaxonomy.objects.filter(
                        taxonomy=stub, taxongroup=group,
                    ).update(is_validated=auto_validate)

                stub.refresh_from_db()
                if not auto_validate and stub.fada_id:
                    stub.fada_id = ""
                    stub.save(update_fields=["fada_id"])

            fixed += 1
            self.stdout.write(
                self.style.SUCCESS(
                    f"  id={stub.id} '{stub.canonical_name}' -> re-verified "
                    f"against WoRMS AphiaID={accepted_aphia_id} "
                    f"(status={record.get('status')}), validated="
                    f"{auto_validate}."
                )
            )

        self.stdout.write(
            self.style.SUCCESS(
                f"Done. Fixed: {fixed}, unverifiable (left unchanged): "
                f"{unverifiable}, needs manual review: {no_reference}."
            )
        )
