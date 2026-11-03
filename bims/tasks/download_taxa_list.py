# coding=utf-8
import csv
import datetime
import json
import logging
import os
from typing import OrderedDict

from django.conf import settings
from django.contrib.auth import get_user_model
from celery import shared_task
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import Paragraph, Spacer, SimpleDocTemplate
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase.pdfmetrics import registerFontFamily

from bims.models.taxonomy import Taxonomy
from bims.scripts.species_keys import (
    ACCEPTED_TAXON, TAXON_RANK,
    COMMON_NAME, CLASS, SUBSPECIES,
    CITES_LISTING, FADA_ID, ON_GBIF, GBIF_LINK,
    SPECIES_GROUP, SUBGENUS, SUBTRIBE, SUBFAMILY,
    SUBORDER, SUBCLASS, SUBPHYLUM, SPECIES, GENUS,
    TRIBE, FAMILY, ORDER, PHYLUM, KINGDOM, AUTHORS
)
from bims.templatetags import is_fada_site
from bims.utils.domain import get_current_domain
from bims.utils.taxonomy import canonical_with_subgenus, get_addendum_display

logger = logging.getLogger(__name__)


SNAPSHOT_FIXED_HEADERS = [
    'taxon_rank', 'kingdom', 'phylum', 'class_name', 'order',
    'family', 'subfamily', 'tribe', 'subtribe',
    'genus', 'subgenus', 'species', 'subspecies', 'variety', 'species_group',
    'taxon', 'scientific_name_and_authority', 'author',
    'taxonomic_status', 'accepted_taxon',
    'common_name', 'origin', 'endemism', 'invasion',
    'conservation_status_global', 'conservation_status_national',
    'on_gbif', 'gbif_link', 'fada_id', 'cites_listing',
]

def format_taxa_csv_headers(
        _headers, tag_titles=(), additional_attributes_titles=()):
    """Turn serializer keys into the column labels used by taxa CSV exports."""
    from preferences import preferences
    from bims.fada.taxa_list import FADA_ADDITIONAL_KEYS

    header_map = {
        'class_name': CLASS,
        'taxon_rank': TAXON_RANK,
        'common_name': COMMON_NAME,
        'accepted_taxon': ACCEPTED_TAXON,
        'fada_id': FADA_ID,
        'species_group': SPECIES_GROUP,
        'subgenus': SUBGENUS,
        'subtribe': SUBTRIBE,
        'subfamily': SUBFAMILY,
        'suborder': SUBORDER,
        'subclass': SUBCLASS,
        'subphylum': SUBPHYLUM,
        'subspecies': SUBSPECIES,
        'species': SPECIES,
        'addendum': 'Addendum',
        'genus': GENUS,
        'tribe': TRIBE,
        'family': FAMILY,
        'order': ORDER,
        'phylum': PHYLUM,
        'kingdom': KINGDOM,
        'cites_listing': CITES_LISTING,
        'author': AUTHORS,
        'taxonomic_status': 'Taxonomic Status',
    }

    _updated_headers = []
    for header in _headers:
        if header in FADA_ADDITIONAL_KEYS:
            _updated_headers.append(header)
            continue
        original = header

        if header.lower() in header_map:
            header = header_map[header.lower()]
            _updated_headers.append(header)
            continue

        if header.lower().strip() in ['on_gbif', 'on gbif']:
            header = ON_GBIF
            _updated_headers.append(header)
            continue
        elif header.lower().strip() in ['gbif_link', 'gbif link']:
            header = GBIF_LINK
            _updated_headers.append(header)
            continue
        elif header.lower().strip() == 'invasion':
            header = preferences.SiteSetting.invasion_label or 'Invasion'
            _updated_headers.append(header)
            continue

        header = header.replace('_or_', '/')

        if (
            not header.istitle()
            and original not in tag_titles
            and original not in additional_attributes_titles
        ):
            header = header.replace('_', ' ').capitalize()

        _updated_headers.append(header)
    return _updated_headers


def process_download_csv_taxa_list(
        request,
        csv_file_path,
        filename,
        user_id,
        download_request_id='',
        taxa_ids=None,
        order_by=None
    ):
    import csv
    from django.contrib.auth import get_user_model
    from django.conf import settings

    from bims.api_views.taxon import TaxaList
    from bims.views.download_csv_taxa_list import (
        TaxaCSVSerializer,
    )
    from bims.tasks import send_csv_via_email
    from bims.models.download_request import DownloadRequest
    from bims.scripts.species_keys import BIOGRAPHIC_DISTRIBUTIONS
    from bims.models.taxon_group import TaxonGroup
    from bims.models import TaxonGroupCitation
    from bims.templatetags import is_fada_site
    from bims.fada.taxa_list import (
        FADA_ADDITIONAL_KEYS,
        reorder_headers_for_fada,
    )

    is_fada = is_fada_site()

    def _from_additional_data(instance, k):
        data = getattr(instance, 'additional_data', None)
        if not data:
            return ''
        if isinstance(data, dict):
            return data.get(k, '')
        try:
            parsed = json.loads(data)
            if isinstance(parsed, dict):
                return parsed.get(k, '')
        except Exception:
            pass
        return ''

    def _current_domain():
        try:
            from bims.utils.domain import get_current_domain
            return get_current_domain()
        except Exception:  # noqa
            pass
        try:
            from django.contrib.sites.models import Site
            return Site.objects.get_current().domain
        except Exception:  # noqa
            pass
        return getattr(settings, "SITE_DOMAIN", "")

    class RequestGet:
        def __init__(self, get_data, user=None):
            self.GET = get_data
            self.user = user

    User = get_user_model()
    user = None
    if user_id:
        try:
            user = User.objects.get(id=user_id)
        except User.DoesNotExist:
            pass

    request_get = RequestGet(request, user)

    taxon_group_name = "Taxa checklist"
    tg_id = request.get("taxonGroup") if isinstance(request, dict) else None
    if tg_id:
        try:
            tg = TaxonGroup.objects.get(id=tg_id)
            taxon_group_name = f"{tg.name} Checklist"
        except TaxonGroup.DoesNotExist:
            pass

    if taxa_ids:
        from bims.models import Taxonomy
        taxa_qs = Taxonomy.objects.filter(id__in=taxa_ids)
        if order_by:
            taxa_qs = taxa_qs.order_by(order_by)
        if is_fada:
            from django.db.models import Q
            taxa_qs = taxa_qs.exclude(
                (Q(fada_id__isnull=True) | Q(fada_id='')) & Q(aphia_id__isnull=True) & Q(taxonworks_id__isnull=True)
            )
    else:
        taxa_qs = TaxaList.get_taxa_by_parameters(request_get)
    try:
        taxa_qs = taxa_qs.prefetch_related('tags', 'biographic_distributions', 'vernacular_names')
    except Exception:
        pass

    total_taxa = taxa_qs.count()

    tag_titles = set()
    additional_attributes_titles = set()

    def update_headers(_headers):
        return format_taxa_csv_headers(
            _headers, tag_titles, additional_attributes_titles)

    raw_headers = []
    has_addendum = False
    for taxon in taxa_qs.iterator(chunk_size=2000):
        ser = TaxaCSVSerializer(taxon)
        row = ser.data

        for k in row.keys():
            if k not in raw_headers:
                raw_headers.append(k)

        if row.get('addendum'):
            has_addendum = True

        tag_titles.update(ser.context.get('tags', []))
        additional_attributes_titles.update(
            ser.context.get('additional_attributes_titles', [])
        )

    if not has_addendum and 'addendum' in raw_headers:
        raw_headers.remove('addendum')

    for key in BIOGRAPHIC_DISTRIBUTIONS:
        if key not in raw_headers:
            raw_headers.append(key)

    if is_fada:
        for k in FADA_ADDITIONAL_KEYS:
            if k not in raw_headers:
                raw_headers.append(k)
        raw_headers = reorder_headers_for_fada(raw_headers)

    if taxa_ids:
        raw_headers.append('admin_url')

    updated_headers = update_headers(raw_headers)

    progress = 1
    with open(csv_file_path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        writer.writerow(updated_headers)

        if taxa_ids:
            taxa_qs_write = Taxonomy.objects.filter(id__in=taxa_ids)
            if order_by:
                taxa_qs_write = taxa_qs_write.order_by(order_by)
            if is_fada:
                from django.db.models import Q
                taxa_qs_write = taxa_qs_write.exclude(
                    (Q(fada_id__isnull=True) | Q(fada_id='')) & Q(aphia_id__isnull=True) & Q(taxonworks_id__isnull=True)
                )
        else:
            taxa_qs_write = TaxaList.get_taxa_by_parameters(request_get)
        try:
            taxa_qs_write = taxa_qs_write.prefetch_related('tags', 'biographic_distributions', 'vernacular_names')
        except Exception:
            pass

        if taxa_ids:
            from django.urls import reverse
            current_domain = _current_domain()

        for taxon in taxa_qs_write.iterator(chunk_size=2000):
            ser = TaxaCSVSerializer(taxon)
            row = ser.data

            if is_fada:
                for k in FADA_ADDITIONAL_KEYS:
                    if k not in row:
                        row[k] = _from_additional_data(taxon, k)

            if taxa_ids:
                admin_path = reverse(
                    'admin:bims_taxonomy_change', args=[taxon.pk]
                )
                row['admin_url'] = f'{current_domain}{admin_path}'

            writer.writerow([row.get(k, '') for k in raw_headers])

            if progress % 10 == 0 and download_request_id:
                try:
                    download_request = DownloadRequest.objects.get(id=download_request_id)
                    download_request.progress = f'{progress}/{total_taxa}'
                    download_request.save(update_fields=['progress'])
                except DownloadRequest.DoesNotExist:
                    pass

            progress += 1

        writer.writerow([])

        # Metadata
        title_text = f"{taxon_group_name}"
        writer.writerow([title_text])
        current_site = _current_domain()
        generated_text = (
            f"(generated {datetime.datetime.now().strftime('%a %b %d %H:%M:%S %Y')} "
            f"from {current_site})"
        )
        writer.writerow([generated_text])

        subset_note = _build_subset_note(request if isinstance(request, dict) else {})
        if subset_note:
            writer.writerow([subset_note])

        # --- Citations ---
        try:
            if tg_id:
                tg = TaxonGroup.objects.get(id=tg_id)
                citations = (
                    TaxonGroupCitation.objects
                    .filter(taxon_group=tg)
                    .order_by('-year', '-access_date', '-updated_at', '-created_at')
                )
                if citations.exists():
                    writer.writerow([])  # spacer
                    writer.writerow(["To be cited as:"])
                    for c in citations:
                        writer.writerow([c.formatted_citation()])
        except Exception:
            pass


    UserModel = get_user_model()
    try:
        user = UserModel.objects.get(id=user_id)
        if download_request_id:
            DownloadRequest.objects.filter(id=download_request_id).update(
                progress=f'{total_taxa}/{total_taxa}'
            )
        send_csv_via_email(
            user_id=user.id,
            csv_file=csv_file_path,
            file_name=filename,
            approved=True,
            download_request_id=download_request_id
        )
    except UserModel.DoesNotExist:
        pass


class _Styles:
    pass


def _get_checklist_pdf_styles() -> _Styles:
    font_dir = os.path.join(settings.STATIC_ROOT, 'fonts', 'garamond')
    if not os.path.isdir(font_dir):
        font_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'static', 'fonts', 'garamond'
        )
    pdfmetrics.registerFont(TTFont('Garamond', os.path.join(font_dir, 'EBGaramond-Regular.ttf')))
    pdfmetrics.registerFont(TTFont('Garamond-Bold', os.path.join(font_dir, 'EBGaramond-Bold.ttf')))
    pdfmetrics.registerFont(TTFont('Garamond-Italic', os.path.join(font_dir, 'EBGaramond-Italic.ttf')))
    pdfmetrics.registerFont(TTFont('Garamond-BoldItalic', os.path.join(font_dir, 'EBGaramond-BoldItalic.ttf')))
    registerFontFamily(
        'Garamond',
        normal='Garamond',
        bold='Garamond-Bold',
        italic='Garamond-Italic',
        boldItalic='Garamond-BoldItalic',
    )

    base = getSampleStyleSheet()

    s = _Styles()
    s.title = ParagraphStyle('TitleStyle', parent=base['Normal'],
        fontName='Garamond-Bold', fontSize=22, leading=22, alignment=TA_LEFT)
    s.generated = ParagraphStyle('GeneratedStyle', parent=base['Normal'],
        fontName='Garamond-Italic', fontSize=10, leading=12, alignment=TA_LEFT)
    s.group = ParagraphStyle('GroupStyle', parent=base['Normal'],
        fontName='Garamond-Bold', fontSize=12, leading=14)
    s.species = ParagraphStyle('SpeciesStyle', parent=base['Normal'],
        fontName='Garamond', fontSize=11, leading=14)
    s.synonym = ParagraphStyle('SynonymStyle', parent=s.species, leftIndent=15)
    s.heading = ParagraphStyle('HeadingStyle', parent=base['Normal'],
        fontName='Garamond-Bold', fontSize=12, leading=14, alignment=TA_LEFT)
    s.citation = ParagraphStyle('CitationStyle', parent=base['Normal'],
        fontName='Garamond', fontSize=10, leading=12, alignment=TA_LEFT)
    return s


def _build_subset_note(request_dict):
    """Return a human-readable note when the export is filtered to a subset."""
    if not request_dict:
        return ''
    from bims.models.taxonomy import Taxonomy as _Taxonomy
    parts = []
    family = (request_dict.get('family') or '').strip()
    if family:
        parts.append(f'family {family}')
    genus = (request_dict.get('genus') or '').strip()
    if genus:
        parts.append(f'genus {genus}')
    taxon_name = (request_dict.get('taxon') or '').strip()
    if taxon_name:
        parts.append(f'taxon "{taxon_name}"')
    parent_ids = [p for p in (request_dict.get('parent') or '').split(',') if p.strip()]
    if parent_ids:
        names = list(
            _Taxonomy.objects.filter(id__in=parent_ids)
            .values_list('canonical_name', flat=True)
        )
        if names:
            parts.append(', '.join(names))
    if not parts:
        return ''
    return 'Note: This export contains only taxa filtered to ' + '; '.join(parts) + '.'


def _build_checklist_pdf_header(title, taxon_group, styles, subset_note='', doi=''):
    from bims.models import TaxonGroupCitation

    paragraphs = []
    paragraphs.append(Paragraph(title, styles.title))
    paragraphs.append(Spacer(1, 6))
    paragraphs.append(Paragraph(
        f'(generated {datetime.datetime.now().strftime("%a %b %d %H:%M:%S %Y")} '
        f'from {get_current_domain()})',
        styles.generated,
    ))
    paragraphs.append(Spacer(1, 6))

    if subset_note:
        paragraphs.append(Paragraph(subset_note, styles.citation))
        paragraphs.append(Spacer(1, 6))

    citations = (
        TaxonGroupCitation.objects
        .filter(taxon_group=taxon_group)
        .order_by('-year', '-access_date', '-updated_at', '-created_at')
    )
    if doi or citations.exists():
        paragraphs.append(Paragraph('To be cited as:', styles.heading))
        if citations.exists():
            for c in citations:
                paragraphs.append(
                    Paragraph(c.formatted_citation(doi=doi), styles.citation)
                )
        elif doi:
            paragraphs.append(Paragraph(doi, styles.citation))
        paragraphs.append(Spacer(1, 6))

    paragraphs.append(Paragraph(
        '<i>= denotes a synonym of the accepted taxon above it</i>', styles.citation
    ))
    paragraphs.append(Paragraph(
        '<i>s.l. = sensu lato</i>', styles.citation
    ))
    if is_fada_site():
        paragraphs.append(Spacer(1, 6))
        paragraphs.append(Paragraph(
            'Biogeographic distribution: ANT = Antarctic, AT = Afrotropical, AU = '
            'Australasian, NA = Nearctic, NT = Neotropical, ', styles.citation
        ))
        paragraphs.append(Paragraph(
            'OL = Oriental (Indomalaya), PA = Palaearctic, '
            'PAC = Pacific (Oceania)', styles.citation
        ))
    paragraphs.append(Spacer(1, 12))
    return paragraphs


def _render_checklist_groups(groups, styles):
    """Render normalized checklist groups into a list of PDF paragraphs.
    """
    paragraphs = []
    for info in groups.values():
        header = f"<i>{info['name']}</i>"
        author = info.get('author') or ''
        if author and author not in info['name']:
            header += f" {author}"
        paragraphs.append(Paragraph(header, styles.group))
        paragraphs.append(Spacer(1, 10))

        for sp in info['species']:
            sp_line = f"<i>{sp['canonical_name']}</i>"
            addendum = get_addendum_display(sp.get('addendum'), abbreviate=True)
            if addendum:
                sp_line += f" {addendum}"
            if sp.get('author'):
                sp_line += f" {sp['author']}"
            if sp.get('type_species'):
                sp_line += " (Type species for genus)"
            if sp.get('regions'):
                sp_line += f": {' '.join(sp['regions'])}"
            paragraphs.append(Paragraph(sp_line, styles.species))

            for syn in sp.get('synonyms', []):
                syn_line = f"= <i>{syn['canonical_name']}</i>"
                if syn.get('author'):
                    syn_line += f" {syn['author']}"
                paragraphs.append(Paragraph(syn_line, styles.synonym))

        paragraphs.append(Spacer(1, 10))
    return paragraphs


def _snapshot_display_name(row):
    """Canonical name to display for a checklist snapshot row."""
    taxon_name = canonical_with_subgenus(row.canonical_name, row.genus, row.subgenus)
    return taxon_name or row.scientific_name or ''


def process_download_pdf_taxa_list(
        request, pdf_file_path, filename, user_id, taxon_group_id, download_request_id, order_by=None):
    from bims.tasks import send_csv_via_email
    from bims.models.taxon_group import TaxonGroup
    from bims.api_views.taxon import TaxaList

    if not order_by:
        order_by = 'genus'

    class RequestGet:
        def __init__(self, get_data, user=None):
            self.GET = get_data
            self.user = user

    def get_checklist_paragraphs(taxon_group, taxonomies):
        st = _get_checklist_pdf_styles()

        subset_note = _build_subset_note(request if isinstance(request, dict) else {})
        paragraphs = _build_checklist_pdf_header(
            f'{taxon_group.name} Checklist', taxon_group, st, subset_note=subset_note
        )

        species_qs = taxonomies.filter(
            rank__in=['SPECIES', 'SUBSPECIES']
        ).exclude(
            taxonomic_status__icontains='synonym'
        ).order_by('canonical_name')

        groups = {}
        for s in species_qs:
            if order_by == 'family':
                key = s.family.canonical_name if s.family else 'No Family'
                key_author = s.family.author if s.family and s.family.author else ''
                key_id = s.family.id if s.family else 'no-family'
            else:
                key = s.genus.canonical_name if s.genus else ''
                key_author = s.genus.author if s.genus and s.genus.author else ''
                key_id = s.genus.id if s.genus else None
            if not key:
                continue
            if key_id not in groups:
                groups[key_id] = {
                    'name': key,
                    'author': key_author,
                    'species': []
                }
            groups[key_id]['species'].append({
                'canonical_name': s.canonical_name,
                'author': s.author or '',
                'addendum': s.addendum,
                'type_species': 'type species' in (s.additional_data or {}),
                'regions': sorted(
                    t.name for t in s.biographic_distributions.all()
                ),
                'synonyms': [
                    {'canonical_name': syn.canonical_name, 'author': syn.author or ''}
                    for syn in Taxonomy.objects.filter(accepted_taxonomy=s)
                ],
            })

        groups = OrderedDict(
            sorted(groups.items(), key=lambda item: item[1]['name']))

        paragraphs.extend(_render_checklist_groups(groups, st))
        return paragraphs

    taxon_group = TaxonGroup.objects.get(id=taxon_group_id)

    UserModel = get_user_model()
    user = None
    if user_id:
        try:
            user = UserModel.objects.get(id=user_id)
        except UserModel.DoesNotExist:
            pass

    request_get = RequestGet(request, user)
    taxonomies = TaxaList.get_taxa_by_parameters(request_get)

    doc = SimpleDocTemplate(
        pdf_file_path,
        pagesize=A4,
        rightMargin=50,
        leftMargin=50,
        topMargin=50,
        bottomMargin=50
    )

    story = []

    checklist_paragraphs = get_checklist_paragraphs(taxon_group, taxonomies)
    story.extend(checklist_paragraphs)
    doc.build(story)

    # Mark the download request as finished
    if download_request_id:
        from django.utils import timezone
        from bims.models.download_request import DownloadRequest
        total = taxonomies.filter(rank__in=['SPECIES', 'SUBSPECIES']).count()
        try:
            dr = DownloadRequest.objects.get(id=download_request_id)
            dr.progress = f'{total}/{total}'
            dr.processing = False
            dr.request_file = pdf_file_path
            dr.progress_updated_at = timezone.now()
            dr.save(update_fields=[
                'progress', 'processing', 'request_file', 'progress_updated_at'
            ])
        except DownloadRequest.DoesNotExist:
            pass

    UserModel = get_user_model()
    try:
        user = UserModel.objects.get(id=user_id)
        send_csv_via_email(
            user_id=user.id,
            csv_file=pdf_file_path,
            file_name=filename,
            approved=True,
            download_request_id=download_request_id
        )
    except UserModel.DoesNotExist:
        pass


@shared_task(name='bims.tasks.download_csv_taxa_list', queue='update')
def download_taxa_list(
        request, csv_file, filename, user_id, output = 'csv', download_request_id='',
        taxon_group_id = None, taxa_ids=None, order_by=None):
    from bims.utils.celery import memcache_lock
    lock_id = '{0}-lock-{1}'.format(
        filename,
        csv_file
    )
    oid = '{0}'.format(csv_file)
    with memcache_lock(lock_id, oid) as acquired:
        if acquired:
            if output == 'csv':
                return process_download_csv_taxa_list(
                    request, csv_file, filename, user_id, download_request_id,
                    taxa_ids=taxa_ids, order_by=order_by
                )
            else:
                return process_download_pdf_taxa_list(
                    request, csv_file, filename, user_id, taxon_group_id, download_request_id,
                    order_by=order_by
                )
    logger.info(
        'Csv %s is already being processed by another worker',
        csv_file)


def _snapshot_row_to_dict(row):
    # Snapshots only keep the GBIF key, so there is no col_id to read.
    col_id = getattr(row, 'col_id', '')
    on_gbif = 'Yes' if row.gbif_key or col_id else 'No'
    if col_id:
        gbif_link = f'https://www.gbif.org/taxon/{col_id}'
    elif row.gbif_key:
        gbif_link = f'https://www.gbif.org/species/{row.gbif_key}'
    else:
        gbif_link = '-'
    taxon = canonical_with_subgenus(row.canonical_name, row.genus, row.subgenus)
    scientific_name = row.scientific_name or ''
    if row.subgenus and taxon not in scientific_name:
        authorship = row.authorship or ''
        scientific_name = f'{taxon} {authorship}'.strip() if authorship else taxon
    return {
        'taxon_rank': row.rank.capitalize() if row.rank else '',
        'kingdom': row.kingdom,
        'phylum': row.phylum,
        'class_name': row.klass,
        'order': row.order,
        'family': row.family,
        'subfamily': row.subfamily,
        'tribe': row.tribe,
        'subtribe': row.subtribe,
        'genus': row.genus,
        'subgenus': row.subgenus,
        'species': row.species,
        'subspecies': row.subspecies,
        'variety': row.variety,
        'species_group': row.species_group,
        'taxon': taxon,
        'scientific_name_and_authority': scientific_name,
        'author': row.authorship,
        'taxonomic_status': row.taxonomic_status,
        'accepted_taxon': row.accepted_taxon,
        'common_name': row.common_name,
        'origin': row.origin,
        'endemism': row.endemism,
        'invasion': row.invasion,
        'conservation_status_global': row.conservation_status_global,
        'conservation_status_national': row.conservation_status_national,
        'on_gbif': on_gbif,
        'gbif_link': gbif_link,
        'fada_id': row.fada_id,
        'cites_listing': row.cites_listing,
    }


def write_snapshot_pdf(snapshots, version, output_file, order_by=None):
    checklist_style = _get_checklist_pdf_styles()

    story = _build_checklist_pdf_header(
        f'{version.taxon_group.name} Checklist v{version.version}',
        version.taxon_group,
        checklist_style,
        doi=version.doi or '',
    )

    synonym_map = {}
    for syn in snapshots.filter(taxonomic_status__icontains='synonym'):
        if syn.basionym_checklist_id:
            synonym_map.setdefault(syn.basionym_checklist_id, []).append(syn)

    accepted = snapshots.filter(
        rank__in=['SPECIES', 'SUBSPECIES'],
    ).exclude(taxonomic_status__icontains='synonym').order_by('scientific_name')

    group_key = 'family' if order_by == 'family' else 'genus'
    group_authors = {}
    for name, author in Taxonomy.objects.filter(
        rank=group_key.upper(),
        canonical_name__in=set(
            accepted.exclude(**{group_key: ''}).values_list(group_key, flat=True)
        ),
    ).exclude(author__isnull=True).exclude(author='').values_list(
        'canonical_name', 'author'
    ):
        group_authors.setdefault(name, author)
    groups = OrderedDict()
    for s in accepted:
        key_name = (s.family if group_key == 'family' else s.genus) or ''
        if not key_name:
            continue
        group = groups.setdefault(
            key_name, {
                'name': key_name,
                'author': group_authors.get(key_name, ''),
                'species': [],
            }
        )
        group['species'].append({
            'canonical_name': _snapshot_display_name(s),
            'author': s.authorship or '',
            'type_species': False,
            'regions': sorted(
                d['area'] for d in (s.distributions or []) if d.get('area')
            ),
            'synonyms': [
                {'canonical_name': _snapshot_display_name(syn), 'author': syn.authorship or ''}
                for syn in synonym_map.get(s.checklist_id, [])
            ],
        })

    groups = OrderedDict(sorted(groups.items()))
    story.extend(_render_checklist_groups(groups, checklist_style))

    SimpleDocTemplate(
        output_file, pagesize=A4,
        rightMargin=50, leftMargin=50, topMargin=50, bottomMargin=50,
    ).build(story)


def write_snapshot_csv(snapshots, output_file, order_by=None, version=None):
    if order_by == 'family':
        snapshots = snapshots.order_by('family', 'genus', 'canonical_name')
    else:
        snapshots = snapshots.order_by('genus', 'canonical_name')

    from bims.models import TaxonExtraAttribute
    from bims.scripts.species_keys import BIOGRAPHIC_DISTRIBUTIONS
    from bims.fada.taxa_list import (
        FADA_ADDITIONAL_KEYS,
        reorder_headers_for_fada,
    )

    is_fada = is_fada_site()

    extra_attr_keys = []
    if version is not None:
        extra_attr_keys = [
            name for name in TaxonExtraAttribute.objects.filter(
                taxon_group=version.taxon_group
            ).values_list('name', flat=True)
            if name.lower().strip() != 'cites listing'
        ]
    tag_keys = []
    for tags in snapshots.values_list('tags', flat=True).iterator(chunk_size=2000):
        for k in (tags or {}):
            if k not in tag_keys:
                tag_keys.append(k)
    for k in BIOGRAPHIC_DISTRIBUTIONS:
        if k not in tag_keys:
            tag_keys.append(k)

    all_keys = list(SNAPSHOT_FIXED_HEADERS)
    for k in extra_attr_keys + tag_keys:
        if k not in all_keys:
            all_keys.append(k)
    if is_fada:
        for k in FADA_ADDITIONAL_KEYS:
            if k not in all_keys:
                all_keys.append(k)
        all_keys = reorder_headers_for_fada(all_keys)
    headers = format_taxa_csv_headers(
        all_keys,
        tag_titles=set(tag_keys),
        additional_attributes_titles=set(extra_attr_keys),
    )

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)
        for row in snapshots.iterator(chunk_size=2000):
            d = _snapshot_row_to_dict(row)
            for k in all_keys:
                if k not in d:
                    d[k] = (row.additional_data or {}).get(k, '')
            for k in tag_keys:
                d[k] = (row.tags or {}).get(k, '')
            writer.writerow([d.get(k, '') for k in all_keys])

        if version is not None:
            from bims.models import TaxonGroupCitation
            writer.writerow([])
            title_text = f'{version.taxon_group.name} Checklist v{version.version}'
            writer.writerow([title_text])
            generated_text = (
                f"(generated {datetime.datetime.now().strftime('%a %b %d %H:%M:%S %Y')} "
                f"from {get_current_domain()})"
            )
            writer.writerow([generated_text])
            citations = (
                TaxonGroupCitation.objects
                .filter(taxon_group=version.taxon_group)
                .order_by('-year', '-access_date', '-updated_at', '-created_at')
            )
            if version.doi or citations.exists():
                writer.writerow([])
                writer.writerow(['To be cited as:'])
                if citations.exists():
                    for c in citations:
                        writer.writerow([c.formatted_citation(doi=version.doi or None)])
                elif version.doi:
                    writer.writerow([version.doi])


@shared_task(name='bims.tasks.download_checklist_snapshot', queue='update')
def download_checklist_snapshot_task(
        version_id, output_file, filename, user_id,
        output='csv', order_by=None, download_request_id=''):
    from bims.models.checklist_version import ChecklistSnapshot, ChecklistVersion
    from bims.models.download_request import DownloadRequest

    try:
        version = ChecklistVersion.objects.select_related('taxon_group').get(pk=version_id)
    except ChecklistVersion.DoesNotExist:
        logger.error('ChecklistVersion %s not found', version_id)
        return

    snapshots = (
        ChecklistSnapshot.objects
        .filter(checklist_version=version)
        .exclude(change_type=ChecklistSnapshot.CHANGE_DELETED)
        .order_by('scientific_name')
    )

    if output == 'csv':
        write_snapshot_csv(snapshots, output_file, order_by, version=version)
    else:
        write_snapshot_pdf(snapshots, version, output_file, order_by)

    if download_request_id:
        from django.utils import timezone
        try:
            dr = DownloadRequest.objects.get(id=download_request_id)
            total = snapshots.count()
            dr.progress = f'{total}/{total}'
            dr.processing = False
            dr.request_file = output_file
            dr.progress_updated_at = timezone.now()
            dr.save(update_fields=['progress', 'processing', 'request_file', 'progress_updated_at'])
        except DownloadRequest.DoesNotExist:
            pass

    from bims.tasks import send_csv_via_email
    UserModel = get_user_model()
    try:
        user = UserModel.objects.get(id=user_id)
        send_csv_via_email(
            user_id=user.id,
            csv_file=output_file,
            file_name=filename,
            approved=True,
            download_request_id=download_request_id,
        )
    except UserModel.DoesNotExist:
        pass
