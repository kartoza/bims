# coding=utf-8
"""Re-link accepted/doubtful species that lost their parent (genus/subgenus).

Changing a taxon's status in the edit form could post an empty parent (the
parent select was cleared when the status passed through "Synonym"). Once the
proposal was approved the species ended up with no parent, its canonical name
reduced to the bare epithet and no suprageneric hierarchy.

For every non-synonym species/subspecies without a parent the original
placement is recovered from, in order:
  1. the most recent approved update proposal that still had a parent,
  2. the Genus / SubGenus columns stored in additional_data (upload row),
  3. an optional spreadsheet (--file) with "Genus" and "Species" columns.

The canonical/scientific names are re-prefixed with the genus when they were
reduced to the epithet, and the cached hierarchical data is rebuilt.

Examples:
    python manage.py restore_lost_parents --tenant fada --dry-run
    python manage.py restore_lost_parents --tenant fada \\
        --file Doubtful_species.xlsx --status DOUBTFUL
"""
import re

try:
    from django_tenants.utils import schema_context
except ImportError:  # pragma: no cover - tenant support optional
    schema_context = None

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Q

SPECIES_RANKS = ['SPECIES', 'SUBSPECIES', 'VARIETY', 'FORMA']
SUBGENUS_RE = re.compile(r'\(([^)]+)\)')


def _clean(value):
    return re.sub(r'\s+', ' ', str(value or '')).strip()


class Command(BaseCommand):
    help = (
        "Restore the parent (genus/subgenus) of accepted/doubtful species "
        "that lost it, using proposal history, upload data or a spreadsheet."
    )

    def add_arguments(self, parser):
        parser.add_argument("--tenant", dest="tenant", default=None)
        parser.add_argument(
            "--file", dest="file", default=None,
            help="Optional .xlsx/.csv with Genus and Species columns. When "
                 "given, only the listed species are processed.",
        )
        parser.add_argument(
            "--status", dest="status", default=None,
            help="Only process taxa with this status (e.g. DOUBTFUL). "
                 "Default: every non-synonym status.",
        )
        parser.add_argument(
            "--dry-run", dest="dry_run", action="store_true", default=False,
        )

    def handle(self, *args, **options):
        schema = options.get("tenant")
        if schema:
            if schema_context is None:
                raise CommandError("django-tenants is required for --tenant.")
            with schema_context(schema):
                self._run(options)
        else:
            self._run(options)

    # ------------------------------------------------------------------ input
    def _read_sheet(self, path):
        """Return {(genus_lower, epithet_lower): genus} from the spreadsheet."""
        rows = []
        if path.lower().endswith('.csv'):
            import csv
            with open(path, newline='', encoding='utf-8-sig') as f:
                rows = list(csv.DictReader(f))
        else:
            import openpyxl
            wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
            ws = wb.active
            it = ws.iter_rows(values_only=True)
            header = [_clean(h) for h in next(it)]
            rows = [dict(zip(header, r)) for r in it]
        wanted = {}
        for row in rows:
            lower = {k.lower(): v for k, v in row.items() if k}
            genus = _clean(lower.get('genus'))
            epithet = _clean(lower.get('species'))
            if genus and epithet:
                wanted[(genus.lower(), epithet.split(' ')[-1].lower())] = genus
        if not wanted:
            raise CommandError(f"No Genus/Species rows found in {path}")
        return wanted

    # -------------------------------------------------------------- resolving
    def _find_genus(self, name):
        from bims.models import Taxonomy
        qs = Taxonomy.objects.filter(
            canonical_name__iexact=name, rank__iexact='GENUS'
        ).exclude(taxonomic_status__icontains='SYNONYM')
        return qs.filter(parent__isnull=False).first() or qs.first()

    def _find_subgenus(self, genus, name):
        from bims.models import Taxonomy
        qs = Taxonomy.objects.filter(rank__iexact='SUBGENUS').filter(
            Q(canonical_name__iexact=name) |
            Q(canonical_name__iexact=f'{genus.canonical_name} ({name})')
        ).exclude(taxonomic_status__icontains='SYNONYM')
        return qs.filter(parent=genus).first() or qs.first()

    def _resolve(self, taxon, sheet_genus):
        """Return (parent, full_canonical_name or None, source)."""
        from bims.models import TaxonomyUpdateProposal

        proposal = (
            TaxonomyUpdateProposal.objects
            .filter(original_taxonomy=taxon, status='approved',
                    parent__isnull=False)
            .select_related('parent').order_by('-id').first()
        )
        if proposal:
            name = _clean(proposal.canonical_name)
            return proposal.parent, (name if ' ' in name else None), \
                f'proposal #{proposal.pk}'

        if taxon.rank.upper() != 'SPECIES':
            return None, None, ''

        ad = taxon.additional_data if isinstance(
            taxon.additional_data, dict) else {}
        genus_name = _clean(ad.get('Genus')) or sheet_genus
        source = 'additional_data' if _clean(ad.get('Genus')) else 'file'
        if not genus_name:
            return None, None, ''
        genus = self._find_genus(genus_name)
        if not genus:
            return None, None, f'genus "{genus_name}" not found'

        subgenus_name = _clean(ad.get('SubGenus') or ad.get('Subgenus'))
        if not subgenus_name:
            match = SUBGENUS_RE.search(taxon.canonical_name or '')
            subgenus_name = match.group(1) if match else ''
        if subgenus_name:
            subgenus = self._find_subgenus(genus, subgenus_name)
            if subgenus:
                return subgenus, None, source
        return genus, None, source

    # ------------------------------------------------------------------- main
    def _run(self, options):
        from bims.models import Taxonomy, TaxonomyUpdateProposal

        dry_run = options['dry_run']
        wanted = self._read_sheet(options['file']) if options['file'] else None

        qs = Taxonomy.objects.filter(
            parent__isnull=True, rank__in=SPECIES_RANKS
        ).exclude(taxonomic_status__icontains='SYNONYM')
        if options['status']:
            qs = qs.filter(taxonomic_status__iexact=options['status'])

        fixed, unresolved, matched_keys = 0, [], set()
        for taxon in qs.order_by('canonical_name'):
            name = _clean(taxon.canonical_name)
            epithet = name.split(' ')[-1].lower()
            sheet_genus = ''
            if wanted is not None:
                tokens = name.lower().split(' ')
                key = None
                if len(tokens) > 1 and (tokens[0], epithet) in wanted:
                    key = (tokens[0], epithet)
                elif len(tokens) == 1:
                    hits = [k for k in wanted if k[1] == epithet]
                    if len(hits) == 1:
                        key = hits[0]
                if not key:
                    continue
                matched_keys.add(key)
                sheet_genus = wanted[key]

            parent, full_name, source = self._resolve(taxon, sheet_genus)
            if not parent:
                unresolved.append(f'{name} (id={taxon.pk}) {source}'.strip())
                continue

            genus_name = parent.genus_name or parent.canonical_name
            if not full_name and ' ' not in name and genus_name:
                full_name = f'{genus_name} {name}'
            sci = _clean(taxon.scientific_name)
            new_sci = sci
            if full_name and full_name != name and sci.startswith(name):
                new_sci = f'{full_name}{sci[len(name):]}'

            self.stdout.write(
                f"{'[dry-run] ' if dry_run else ''}{name} (id={taxon.pk}, "
                f"{taxon.taxonomic_status}) -> parent {parent.canonical_name} "
                f"[{parent.rank}, id={parent.pk}] via {source}"
                + (f"; name -> {full_name}" if full_name and full_name != name else '')
            )
            fixed += 1
            if dry_run:
                continue

            with transaction.atomic():
                taxon.parent = parent
                if full_name:
                    taxon.canonical_name = full_name
                taxon.scientific_name = new_sci
                taxon.hierarchical_data = {}
                taxon.save()
                # Pending proposals created from the broken state would wipe
                # the parent again on approval.
                TaxonomyUpdateProposal.objects.filter(
                    original_taxonomy=taxon, status='pending',
                    parent__isnull=True,
                ).exclude(taxonomic_status__icontains='SYNONYM').update(
                    parent=parent)

        self.stdout.write(self.style.SUCCESS(
            f"{'Would fix' if dry_run else 'Fixed'} {fixed} taxa."))
        for item in unresolved:
            self.stdout.write(self.style.WARNING(f"Unresolved: {item}"))
        if wanted is not None:
            for key in sorted(set(wanted) - matched_keys):
                self.stdout.write(
                    f"Not a parentless taxon (already OK or not found): "
                    f"{wanted[key]} {key[1]}")
