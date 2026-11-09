# coding=utf-8
"""
Export the data owners recorded in a tenant, with their organisation, the
biodiversity module (taxon group) and the source reference of their records.

One row per owner, organisation, module, source collection and source
reference, with the number of records and the first and last collection date.
This is meant for checking who is recorded as the owner of which data, for
example when owners or organisations look wrong after an import.

Nothing is written to the database.

    python manage.py export_data_owners -s fbis -o data_owners_fbis.csv

    # one file per tenant (data_owners_<schema>.csv, public is skipped)
    python manage.py export_data_owners --all-tenants

Use --exclude-gbif to leave out gbif and virtual museum records.
"""
import csv

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count, Max, Min
from django_tenants.utils import get_tenant_model, tenant_context

EXCLUDED_SOURCE_COLLECTIONS = ['gbif', 'virtual_museum']

CSV_FIELDS = [
    'owner', 'owner_name', 'owner_organisation',
    'module', 'source_collection',
    'source_reference_id', 'source_reference',
    'custodian', 'records', 'first_date', 'last_date',
]


class Command(BaseCommand):
    help = 'Export data owners, organisations, modules and source references.'

    def add_arguments(self, parser):
        parser.add_argument(
            '-s', '--schema-name', default=None,
            help='Tenant schema to export, e.g. fbis')
        parser.add_argument(
            '--all-tenants', action='store_true',
            help='Export every tenant to its own file (public is skipped)')
        parser.add_argument(
            '-o', '--output', default=None,
            help='Output file (default data_owners_<schema>.csv)')
        parser.add_argument(
            '--exclude-gbif', action='store_true',
            help='Leave out gbif and virtual museum records')

    def handle(self, *args, **options):
        tenant_model = get_tenant_model()
        if options['all_tenants']:
            if options['output']:
                raise CommandError(
                    '--output cannot be used with --all-tenants.')
            tenants = tenant_model.objects.exclude(schema_name='public')
        elif options['schema_name']:
            try:
                tenants = [tenant_model.objects.get(
                    schema_name=options['schema_name'])]
            except tenant_model.DoesNotExist:
                raise CommandError(
                    f'Tenant "{options["schema_name"]}" not found.')
        else:
            raise CommandError('Give a tenant with -s or use --all-tenants.')

        for tenant in tenants:
            output = (
                options['output'] or f'data_owners_{tenant.schema_name}.csv')
            with tenant_context(tenant):
                self.export(
                    tenant.schema_name, output, options['exclude_gbif'])

    def export(self, schema, output, exclude_gbif):
        from bims.models.biological_collection_record import (
            BiologicalCollectionRecord
        )
        from bims.models.source_reference import SourceReference

        records = BiologicalCollectionRecord.objects.all()
        if exclude_gbif:
            records = records.exclude(
                source_collection__in=EXCLUDED_SOURCE_COLLECTIONS)

        rows = list(records.values(
            'owner__username', 'owner__first_name', 'owner__last_name',
            'owner__organization', 'module_group__name', 'source_collection',
            'source_reference_id', 'institution_id'
        ).annotate(
            records=Count('id'),
            first_date=Min('collection_date'),
            last_date=Max('collection_date'),
        ).order_by(
            'owner__username', 'module_group__name', 'source_reference_id'))

        titles = self.reference_titles(
            SourceReference, {r['source_reference_id'] for r in rows})

        with open(output, 'w', newline='', encoding='utf-8-sig') as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
            writer.writeheader()
            for row in rows:
                name = ' '.join(filter(None, [
                    row['owner__first_name'], row['owner__last_name']]))
                writer.writerow({
                    'owner': row['owner__username'] or '',
                    'owner_name': name,
                    'owner_organisation': row['owner__organization'] or '',
                    'module': row['module_group__name'] or '',
                    'source_collection': row['source_collection'] or '',
                    'source_reference_id': row['source_reference_id'] or '',
                    'source_reference': titles.get(
                        row['source_reference_id'], ''),
                    'custodian': row['institution_id'] or '',
                    'records': row['records'],
                    'first_date': row['first_date'] or '',
                    'last_date': row['last_date'] or '',
                })

        owners = len({r['owner__username'] for r in rows})
        self.stdout.write(
            f'[{schema}] {len(rows)} rows, {owners} owners -> {output}')

    @staticmethod
    def reference_titles(model, reference_ids):
        """Title of the source references, by id."""
        titles = {}
        for reference in model.objects.filter(
            id__in={i for i in reference_ids if i}
        ):
            try:
                titles[reference.id] = (reference.title or '').strip()
            except Exception:  # noqa: a broken source of the reference
                titles[reference.id] = (reference.source_name or '').strip()
        return titles
