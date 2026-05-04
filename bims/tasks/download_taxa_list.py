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
from bims.utils.domain import get_current_domain

logger = logging.getLogger(__name__)


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

    from preferences import preferences
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
            taxa_qs = taxa_qs.exclude(Q(fada_id__isnull=True) | Q(fada_id=''))
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

    raw_headers = []
    for taxon in taxa_qs.iterator(chunk_size=2000):
        ser = TaxaCSVSerializer(taxon)
        row = ser.data

        for k in row.keys():
            if k not in raw_headers:
                raw_headers.append(k)

        tag_titles.update(ser.context.get('tags', []))
        additional_attributes_titles.update(
            ser.context.get('additional_attributes_titles', [])
        )

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
                taxa_qs_write = taxa_qs_write.exclude(Q(fada_id__isnull=True) | Q(fada_id=''))
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
                        writer.writerow([c.formatted_citation])
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


def process_download_pdf_taxa_list(
        request, pdf_file_path, filename, user_id, taxon_group_id, download_request_id, order_by=None):
    from bims.tasks import send_csv_via_email
    from bims.models.taxon_group import TaxonGroup
    from bims.api_views.taxon import TaxaList
    from bims.models import TaxonGroupCitation

    if not order_by:
        order_by = 'genus'

    class RequestGet:
        def __init__(self, get_data, user=None):
            self.GET = get_data
            self.user = user

    def get_checklist_paragraphs(taxon_group, taxonomies):
        """
        Build a list of Paragraph objects for the textual "checklist" portion,
        using CormorantGaramond Serif fonts.
        """
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
            boldItalic='Garamond-BoldItalic'
        )

        # Get the default stylesheet
        styles = getSampleStyleSheet()

        title_style = ParagraphStyle(
            name="TitleStyle",
            parent=styles['Normal'],
            fontName='Garamond-Bold',
            fontSize=22,
            leading=22,
            alignment=TA_LEFT,
        )

        generated_style = ParagraphStyle(
            name="GeneratedStyle",
            parent=styles['Normal'],
            fontName='Garamond-Italic',
            fontSize=10,
            leading=12,
            alignment=TA_LEFT,
        )

        genus_style = ParagraphStyle(
            name="GenusStyle",
            parent=styles['Normal'],
            fontName='Garamond-Bold',
            fontSize=12,
            leading=14,
        )

        species_style = ParagraphStyle(
            name="SpeciesStyle",
            parent=styles['Normal'],
            fontName='Garamond',
            fontSize=11,
            leading=14,
        )

        syn_style = ParagraphStyle(
            'SynonymStyle',
            parent=species_style,
            leftIndent=15,
        )

        heading_style = ParagraphStyle(
            name="HeadingStyle", parent=styles['Normal'],
            fontName='Garamond-Bold', fontSize=12, leading=14, alignment=TA_LEFT,
        )
        citation_style = ParagraphStyle(
            name="CitationStyle", parent=styles['Normal'],
            fontName='Garamond', fontSize=10, leading=12, alignment=TA_LEFT,
        )

        paragraphs = []

        title_text = f"{taxon_group.name} Checklist"
        paragraphs.append(Paragraph(title_text, title_style))
        paragraphs.append(Spacer(1, 6))

        current_site = get_current_domain()
        generated_text = (
            f"(generated {datetime.datetime.now().strftime('%a %b %d %H:%M:%S %Y')} "
            f"from {current_site})"
        )
        paragraphs.append(Paragraph(generated_text, generated_style))
        paragraphs.append(Spacer(1, 6))

        # --- Citations ---
        citations = (
            TaxonGroupCitation.objects
            .filter(taxon_group=taxon_group)
            .order_by('-year', '-access_date', '-updated_at', '-created_at')
        )
        if citations.exists():
            paragraphs.append(Paragraph("To be cited as:", heading_style))
            for c in citations:
                paragraphs.append(Paragraph(c.formatted_citation, citation_style))
            paragraphs.append(Spacer(1, 6))

        paragraphs.append(Paragraph(
            "<i>= denotes a synonym of the accepted taxon above it.</i>",
            citation_style
        ))
        paragraphs.append(Spacer(1, 12))

        order_by_dict = {}
        species_qs = taxonomies.filter(
            rank__in=['SPECIES', 'SUBSPECIES']
        ).exclude(
            taxonomic_status__icontains='synonym'
        ).order_by('canonical_name')

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
            if key_id not in order_by_dict:
                order_by_dict[key_id] = {
                    'canonical_name': key,
                    'author': key_author,
                    'species': []
                }
            order_by_dict[key_id]['species'].append(s)

        # Sort by key canonical name
        order_by_dict = OrderedDict(
            sorted(order_by_dict.items(), key=lambda item: item[1]['canonical_name']))

        for key_id, info in order_by_dict.items():
            header_line = info['canonical_name']
            header_author = ''
            if info['author'] and info['author'] not in header_line:
                header_author += f" {info['author']}"

            paragraphs.append(Paragraph(f"<i>{header_line}</i>{header_author}", genus_style))
            paragraphs.append(Spacer(1, 10))

            for s_obj in info['species']:
                sp_line = f'<i>{s_obj.canonical_name}</i>'
                if s_obj.author:
                    sp_line += f" {s_obj.author}"
                if "type species" in (s_obj.additional_data or {}):
                    sp_line += " (Type species for genus)"

                paragraphs.append(Paragraph(sp_line, species_style))

                synonyms = Taxonomy.objects.filter(
                    accepted_taxonomy=s_obj
                )
                for syn in synonyms:
                    syn_lin = f'= <i>{syn.canonical_name}</i>'
                    if syn.author:
                        syn_lin += f" {syn.author}"
                    paragraphs.append(Paragraph(syn_lin, syn_style))

            paragraphs.append(Spacer(1, 10))

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
