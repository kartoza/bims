from collections import defaultdict
import csv
import os
import re
from django.conf import settings
from django.http import Http404
from preferences import preferences
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from django.utils import timezone
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, TableStyle, Table
from reportlab.platypus.para import Paragraph
from rest_framework.response import Response
from rest_framework.views import APIView

from django.contrib.auth import get_user_model

from bims.api_views.search import CollectionSearch
from bims.enums import TaxonomicRank
from bims.models import LocationContext
from bims.models.taxonomy import Taxonomy
from bims.scripts.collection_csv_keys import PARK_OR_MPA_NAME
from bims.utils.domain import get_current_domain
from bims.models.download_request import DownloadRequest
from bims.serializers.checklist_serializer import (
    ChecklistSerializer,
    ChecklistPDFSerializer
)
from bims.utils.site_code import SANPARK_PARK_KEY
from bims.utils.url import parse_url_to_filters
from bims.tasks.checklist import download_checklist
from bims.tasks.email_csv import send_csv_via_email


CSV_HEADER_TITLE = {
    'class_name': 'Class',
    'scientific_name': 'Accepted Scientific name and authority',
    'cites_listing': 'CITES listing',
    'tops': 'TOPS',
    'park_or_mpa_name': PARK_OR_MPA_NAME,
    'include or exclude': 'Include/Exclude',
    'gbif_coordinate_uncertainty_m': 'GBIF coordinate uncertainty (m)',
    'gbif_coordinate_precision': 'GBIF coordinate precision'
}

User = get_user_model()


def get_custom_header(fieldnames, header_title_dict):
    custom_header = []
    for field in fieldnames:
        if field in header_title_dict:
            custom_header.append(header_title_dict[field])
        else:
            custom_header.append(field.replace('_', ' ').capitalize())
    return custom_header


def get_serializer_keys(serializer_class):
    return list(serializer_class().get_fields().keys())


def checklist_collection_records(download_request: DownloadRequest):
    filters = parse_url_to_filters(
        download_request.dashboard_url
    )
    search = CollectionSearch(
        filters,
        bypass_data_type_checks=True)
    collection_records = search.process_search()
    return collection_records


def _record_ids_by_taxon(collection_records, taxonomy_ids):
    """Map taxonomy id -> filtered record ids with one query per batch."""
    record_ids = defaultdict(list)
    rows = collection_records.filter(
        taxonomy_id__in=taxonomy_ids
    ).order_by().values_list('taxonomy_id', 'id')
    for taxonomy_id, record_id in rows.iterator():
        record_ids[taxonomy_id].append(record_id)
    return record_ids


def generate_checklist(download_request_id):
    if not download_request_id:
        return False
    try:
        download_request = DownloadRequest.objects.get(
            id=download_request_id
        )
    except DownloadRequest.DoesNotExist:
        return False

    if download_request and download_request.rejected:
        download_request.processing = False
        download_request.save()
        return False

    if not download_request:
        return False

    batch_size = 1000
    collection_records = checklist_collection_records(
        download_request
    )
    module_name = ''

    module_names = list(
        collection_records.order_by().values_list(
            'module_group__name',
            flat=True
        ).distinct()[:2]
    )
    if len(module_names) == 1:
        module_name = module_names[0] or ''
    elif len(module_names) > 1:
        module_name = 'Multiple Taxon Groups'

    if (
        download_request.resource_type and
        download_request.resource_type.lower() == 'pdf'
    ):
        return generate_pdf_checklist(
            download_request, module_name, collection_records, batch_size)
    else:
        return generate_csv_checklist(
            download_request, module_name, collection_records, batch_size)


def _format_park_column_title(park_name):
    name = (park_name or '').strip()
    if not name:
        return ''
    if 'National Park' in name:
        name = name.replace('National Park', 'NP').strip()
    elif not name.endswith('NP'):
        name = f'{name} NP'
    return f'{name} invasion status'


def _slugify_park_fieldname(park_name):
    base = (park_name or '').strip().lower()
    base = base.replace('national park', 'np')
    base = re.sub(r'\W+', '_', base)
    base = base.strip('_')
    if not base:
        return ''
    return f'invasion_status_{base}'


def _get_distinct_parks_for_collection(collection_records):
    site_ids = (
        collection_records
        .exclude(site__isnull=True)
        .values_list('site_id', flat=True)
        .distinct()
    )
    if not site_ids:
        return []

    park_names = (
        LocationContext.objects
        .filter(
            site_id__in=site_ids,
            group__name__icontains=SANPARK_PARK_KEY
        )
        .values_list('value', flat=True)
        .distinct()
    )

    return [name.strip() for name in park_names if name and name.strip()]


def generate_csv_checklist(download_request, module_name, collection_records, batch_size):
    site_domain_name = get_current_domain()

    csv_file_path = os.path.join(
        settings.MEDIA_ROOT,
        'checklists',
        f'{site_domain_name}_checklist_{module_name}_{download_request.id}.csv')
    os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

    invasion_label = preferences.SiteSetting.invasion_label or 'Invasion'
    fieldnames = [
        invasion_label if key == 'invasion' else key
        for key in get_serializer_keys(ChecklistSerializer)
        if key != 'id'
    ]

    header_title_dict = dict(CSV_HEADER_TITLE)
    header_title_dict[invasion_label] = invasion_label
    park_field_map = {}

    if preferences.SiteSetting.project_name == 'sanparks':
        park_names = _get_distinct_parks_for_collection(collection_records)
        for park_name in park_names:
            field_key = _slugify_park_fieldname(park_name)
            if not field_key:
                continue
            park_field_map[park_name] = field_key
            fieldnames.append(field_key)
            header_title_dict[field_key] = _format_park_column_title(park_name)

    custom_header = get_custom_header(fieldnames, header_title_dict)

    taxonomy_collection_records = (
        collection_records.distinct(
            'taxonomy__scientific_name'
        ).order_by('taxonomy__scientific_name')
    )
    taxonomy_collection_records_count = taxonomy_collection_records.count()
    taxonomy_ids = list(taxonomy_collection_records.values_list('taxonomy_id', flat=True))

    written_taxa_ids = set()

    with open(csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not written_taxa_ids:
            writer.writerow(dict(zip(fieldnames, custom_header)))

        for start in range(0, taxonomy_collection_records_count, batch_size):
            batch = taxonomy_ids[start:start + batch_size]
            process_batch(
                batch,
                writer,
                written_taxa_ids,
                collection_records,
                park_field_map,
            )
            download_request.progress = (
                f'{start}/{taxonomy_collection_records_count}'
            )
            download_request.save()

    download_request.progress = (
        f'{taxonomy_collection_records_count}/{taxonomy_collection_records_count}'
    )
    download_request.processing = False
    download_request.request_file = csv_file_path
    download_request.request_category = f'{module_name} Checklist'
    download_request.save()

    return True


def generate_pdf_checklist(download_request, module_name, collection_records, batch_size):
    site_domain_name = get_current_domain()
    pdf_file_path = os.path.join(
        settings.MEDIA_ROOT,
        'checklists',
        f'{site_domain_name}_checklist_{module_name}_{download_request.id}.pdf')
    os.makedirs(os.path.dirname(pdf_file_path), exist_ok=True)

    if os.path.exists(pdf_file_path):
        os.remove(pdf_file_path)

    written_taxa_ids = set()
    all_taxa = []
    common_names_and_count = {}

    taxonomy_collection_records = (
        collection_records.distinct(
            'taxonomy__scientific_name'
        ).order_by('taxonomy__scientific_name')
    )
    taxonomy_collection_records_count = taxonomy_collection_records.count()
    taxonomy_ids = list(taxonomy_collection_records.values_list('taxonomy_id', flat=True))

    for start in range(0, taxonomy_collection_records_count, batch_size):
        record_taxonomy_ids = taxonomy_ids[start:start + batch_size]
        unique_taxonomy_ids = set(record_taxonomy_ids) - written_taxa_ids

        if unique_taxonomy_ids:
            taxa = Taxonomy.objects.filter(
                id__in=unique_taxonomy_ids
            ).order_by(
                'scientific_name'
            ).filter(
                rank__in=[
                    TaxonomicRank.SPECIES.name,
                    TaxonomicRank.SUBSPECIES.name]
            )
            taxon_serializer = ChecklistPDFSerializer(
                taxa,
                many=True,
                context={
                    'collection_records': collection_records,
                    'record_ids_by_taxon': _record_ids_by_taxon(
                        collection_records, unique_taxonomy_ids),
                }
            )
            for taxon_obj, taxon in zip(list(taxa), list(taxon_serializer.data)):
                written_taxa_ids.add(taxon_obj.id)
                common_name = taxon['common_name'].lower().strip()

                taxon_entry = [
                    Paragraph(taxon['scientific_name'], getSampleStyleSheet()['Normal']),
                    Paragraph(taxon['common_name'], getSampleStyleSheet()['Normal']),
                    Paragraph(taxon['threat_status'], getSampleStyleSheet()['Normal']),
                    Paragraph(taxon['sources'], getSampleStyleSheet()['Normal'])
                ]

                if common_name != '':
                    if common_name not in common_names_and_count:
                        common_names_and_count[common_name] = (taxon['occurrence_records'], len(all_taxa))
                        all_taxa.append(taxon_entry)
                    else:
                        existing_occurrence_records, index = common_names_and_count[common_name]
                        if taxon['occurrence_records'] > existing_occurrence_records:
                            common_names_and_count[common_name] = (taxon['occurrence_records'], index)
                            all_taxa[index] = taxon_entry
                        else:
                            all_taxa.append(taxon_entry)
                else:
                    all_taxa.append(taxon_entry)

                del taxon['id']
                del taxon['occurrence_records']

        download_request.progress = (
            f'{start}/{taxonomy_collection_records_count}'
        )
        download_request.save()

    # Generate the PDF
    doc = SimpleDocTemplate(
        pdf_file_path,
        pagesize=landscape(A4), topMargin=0.5 * cm, bottomMargin=0.35 * cm)
    elements = []

    # Define table style
    style_table = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), '#0D3511'),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 11),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 5),
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('LEFTPADDING', (0, 0), (-1, -1), 5),
        ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ])

    data = [
               ['Accepted Scientific name and authority',
                'Common Name',
                'Threat Status',
                'Sources']] + all_taxa
    table_width = doc.width
    col_widths = [table_width * 0.35, table_width * 0.30, table_width * 0.15, table_width * 0.30]

    table = Table(data, colWidths=col_widths)
    table.setStyle(style_table)

    elements.append(table)
    doc.build(elements)

    download_request.progress = (
        f'{taxonomy_collection_records_count}/{taxonomy_collection_records_count}'
    )
    download_request.processing = False
    download_request.request_file = pdf_file_path
    download_request.request_category = f'{module_name} Checklist'
    download_request.progress_updated_at = timezone.now()
    download_request.save()

    return True


def _get_invasion_status_for_taxon_parks(taxon_obj, park_field_map, collection_records):
    if not park_field_map:
        return {}

    results = {field_key: '' for field_key in park_field_map.values()}

    for park_name, field_key in park_field_map.items():
        is_key = f'IS_{park_name}'
        statuses = set()
        addl = getattr(taxon_obj, 'additional_data', None) or {}
        val = addl.get(is_key)
        if isinstance(val, str):
            val = val.strip()
        if val:
            statuses.add(val)
        if statuses:
            results[field_key] = ', '.join(sorted(statuses))

    return results


def process_batch(record_taxonomy_ids, writer, written_taxa_ids, collection_records, park_field_map=None):
    """
    Process a batch of collection records and write unique taxa to the CSV file.
    Args:
        record_taxonomy_ids (list): list of taxonomy ids
        writer (csv.DictWriter): CSV writer object.
        written_taxa_ids (set): Set of already written taxa IDs to avoid duplication.
        collection_records (QuerySet): Filtered collection records
        park_field_map (dict): {park_name: field_key} for dynamic invasion status columns
    """
    if park_field_map is None:
        park_field_map = {}

    unique_taxonomy_ids = set(record_taxonomy_ids) - written_taxa_ids

    if unique_taxonomy_ids:
        taxa = Taxonomy.objects.filter(
            id__in=unique_taxonomy_ids
        ).order_by('scientific_name')
        taxon_serializer = ChecklistSerializer(
            taxa,
            many=True,
            context={
                'collection_records': collection_records,
                'record_ids_by_taxon': _record_ids_by_taxon(
                    collection_records, unique_taxonomy_ids),
            }
        )

        taxa_list = list(taxa)
        serializer_data = list(taxon_serializer.data)

        for taxon_obj, taxon in zip(taxa_list, serializer_data):
            written_taxa_ids.add(taxon_obj.id)
            row = dict(taxon)
            del row['id']

            if park_field_map and preferences.SiteSetting.project_name == 'sanparks':
                park_statuses = _get_invasion_status_for_taxon_parks(
                    taxon_obj,
                    park_field_map,
                    collection_records,
                )
                row.update(park_statuses)

            writer.writerow(row)


class DownloadChecklistAPIView(APIView):

    def post(self, request, *args, **kwargs):
        auto_approved = not preferences.SiteSetting.enable_download_request_approval
        download_request_id = self.request.POST.get(
            'downloadRequestId', None)

        if not download_request_id:
            raise Http404('Missing download request id')

        download_request = DownloadRequest.objects.get(
            id=download_request_id
        )

        if download_request.request_file and download_request.approved:
            send_csv_via_email.delay(
                user_id=self.request.user.id,
                csv_file=download_request.request_file.path,
                file_name='Checklist',
                download_request_id=download_request_id
            )
        else:
            if auto_approved and not download_request.approved:
                download_request.approved = True
                download_request.save()

            download_checklist.delay(
                download_request_id,
                auto_approved,
                download_request.requester.id)

        return Response({
            'status': 'processing',
        })
