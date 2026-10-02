from datetime import date, datetime

from django.core.exceptions import ValidationError
from django.db.models import Count, Q
from django.http import (
    Http404, HttpResponseServerError, HttpResponseForbidden
)
from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from bims.models.source_reference import (
    SourceReference
)
from bims.models.biological_collection_record import (
    BiologicalCollectionRecord
)
from bims.models.chemical_record import (
    ChemicalRecord
)
from bims.models.decision_support_tool import DecisionSupportTool

UNSPECIFIED_DATA_TYPE = 'unspecified'


class DeleteRecordsByReferenceId(APIView):
    """
    API endpoint for deleting BiologicalCollectionRecord and ChemicalRecord
    instances associated with a given SourceReference ID.
    """

    def post(self, request, *args, **kwargs):
        if not request.user.is_superuser:
            return HttpResponseForbidden(
                'Only superusers are allowed to perform this action.'
            )

        source_reference_id = kwargs.get('source_reference_id')
        if not source_reference_id:
            raise Http404('Missing id')

        try:
            source_reference = get_object_or_404(
                SourceReference,
                pk=source_reference_id
            )
            messages = []
            bio_records = BiologicalCollectionRecord.objects.filter(source_reference_id=source_reference_id)
            if bio_records.exists():
                DecisionSupportTool.objects.filter(
                    biological_collection_record__id__in=list(bio_records.values_list('id', flat=True))
                ).delete()
                BiologicalCollectionRecord.objects.filter(source_reference_id=source_reference_id).delete()
                messages.append(
                    'BiologicalCollectionRecord successfully deleted'
                )
            else:
                messages.append("No BiologicalCollectionRecord found for the given reference ID.")

            if ChemicalRecord.objects.filter(source_reference_id=source_reference_id).exists():
                ChemicalRecord.objects.filter(source_reference_id=source_reference_id).delete()
                messages.append(
                    'ChemicalRecord successfully deleted'
                )
            else:
                messages.append("No ChemicalRecord found for the given reference ID.")

            return Response(
                        {'message': messages},
                        status=status.HTTP_200_OK)

        except Exception as e:
            # In case of any other error, return a 500 Internal Server Error
            return HttpResponseServerError(f'An error occurred: {e}')


def data_type_records(source_reference, params):
    """
    Return the BiologicalCollectionRecord queryset of a source reference.
    Raises ValidationError when the parameter is invalid.
    """
    current_data_type = params.get('current_data_type') or None
    valid_data_types = [
        choice[0] for choice in BiologicalCollectionRecord.DATA_TYPE_CHOICES
    ]
    records = BiologicalCollectionRecord.objects.filter(
        source_reference=source_reference
    )
    if current_data_type:
        if current_data_type == UNSPECIFIED_DATA_TYPE:
            records = records.filter(
                Q(data_type='') | Q(data_type__isnull=True)
            )
        elif current_data_type in valid_data_types:
            records = records.filter(data_type=current_data_type)
        else:
            raise ValidationError(
                f'Invalid current data type: {current_data_type}'
            )
    return records


def records_needing_data_type(records, new_data_type):
    """
    Exclude records that already have the new data type. Records without a
    data type are treated as public.
    """
    records = records.exclude(data_type=new_data_type)
    if new_data_type == 'public':
        records = records.exclude(
            Q(data_type='') | Q(data_type__isnull=True)
        )
    return records


def embargo_dates(params):
    """
    Parse the embargo_start_date and embargo_end_date parameters
    (DD/MM/YYYY). Returns a (start_date, end_date) tuple, each may be None.
    Raises ValidationError when the dates are invalid.
    """
    start_date = params.get('embargo_start_date') or None
    end_date = params.get('embargo_end_date') or None
    try:
        if start_date:
            start_date = datetime.strptime(start_date, '%d/%m/%Y').date()
        if end_date:
            end_date = datetime.strptime(end_date, '%d/%m/%Y').date()
    except ValueError:
        raise ValidationError('Embargo dates must use the DD/MM/YYYY format.')
    today = date.today()
    if start_date and not end_date:
        raise ValidationError(
            'An embargo end date is required when a start date is set.')
    if start_date and start_date < today:
        raise ValidationError(
            'Embargo start date must be today or in the future.')
    if end_date and end_date <= (start_date or today):
        raise ValidationError(
            'Embargo end date must be after the start date '
            '(or after today when no start date is set).')
    return start_date, end_date


def can_update_data_type(user):
    if user.is_anonymous:
        return False
    if user.is_superuser:
        return True
    return (
        user.has_perm('bims.change_sourcereference') and
        user.has_perm('bims.change_biologicalcollectionrecord')
    )


class DataTypeSummaryByReferenceId(APIView):
    """
    API endpoint returning the number of BiologicalCollectionRecord
    instances per data type, and the number currently under embargo, for a
    source reference filtered by the current_data_type parameter.
    """

    def get(self, request, *args, **kwargs):
        if not can_update_data_type(request.user):
            return Response(
                {'message': 'You do not have permission to perform this '
                            'action.'},
                status=status.HTTP_403_FORBIDDEN)

        source_reference = get_object_or_404(
            SourceReference, pk=kwargs.get('source_reference_id')
        )
        try:
            records = data_type_records(source_reference, request.GET)
        except ValidationError as e:
            return Response(
                {'message': e.message},
                status=status.HTTP_400_BAD_REQUEST)

        summary = {
            choice[0]: 0
            for choice in BiologicalCollectionRecord.DATA_TYPE_CHOICES
        }
        summary[UNSPECIFIED_DATA_TYPE] = 0
        for row in records.values('data_type').annotate(total=Count('id')):
            key = row['data_type'] or UNSPECIFIED_DATA_TYPE
            summary[key] = summary.get(key, 0) + row['total']
        today = date.today()
        under_embargo = records.filter(
            Q(end_embargo_date__gt=today) &
            (
                Q(start_embargo_date__isnull=True) |
                Q(start_embargo_date__lte=today)
            )
        ).count()
        return Response({
            'total': sum(summary.values()),
            'data_types': summary,
            'under_embargo': under_embargo
        })
