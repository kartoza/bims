"""Tests for batched occurrence download serialization."""
import datetime
from unittest import mock

from django.db import connection
from django.test.utils import CaptureQueriesContext
from django_tenants.test.cases import FastTenantTestCase

from bims.download.collection_record import collection_record_batches
from bims.enums.taxonomic_group_category import TaxonomicGroupCategory
from bims.models import (
    BiologicalCollectionRecord,
    DecisionSupportTool,
    DecisionSupportToolName,
    Survey,
    SurveyData,
    SurveyDataOption,
    SurveyDataValue,
)
from bims.models.location_context_filter import LocationContextFilter
from bims.models.location_context_filter_group_order import (
    LocationContextFilterGroupOrder
)
from bims.serializers.bio_collection_serializer import (
    BioCollectionOneRowSerializer,
    prefetch_batch,
)
from bims.tests.model_factories import (
    BiologicalCollectionRecordF,
    ChemF,
    ChemicalRecordF,
    LocationContextF,
    LocationContextGroupF,
    LocationSiteF,
    SourceReferenceF,
    SurveyF,
    TaxonGroupF,
    TaxonGroupTaxonomyF,
    TaxonomyF,
    UserF,
)

SERIALIZER_MODULE = 'bims.serializers.bio_collection_serializer'


@mock.patch('bims.models.location_site.update_location_site_context')
class TestCollectionRecordDownloadBatch(FastTenantTestCase):

    def setUp(self):
        self.date = datetime.date(2024, 5, 1)
        self.owner = UserF.create()
        self.taxonomy = TaxonomyF.create()
        self.source_reference = SourceReferenceF.create(note='Shared ref')
        self.sites = [LocationSiteF.create(), LocationSiteF.create()]
        self.surveys = [
            SurveyF.create(site=site, date=self.date) for site in self.sites
        ]

        groups = [
            LocationContextGroupF.create(name='Geomorphology', key='geo'),
            LocationContextGroupF.create(name='Freshwater', key='fw'),
        ]
        ctx_filter = LocationContextFilter.objects.create(
            title='Filter', display_order=1
        )
        for order, group in enumerate(groups):
            LocationContextFilterGroupOrder.objects.create(
                group=group, filter=ctx_filter, group_display_order=order
            )
        LocationContextF.create(
            site=self.sites[0], group=groups[0], value='GMZ1'
        )
        LocationContextF.create(
            site=self.sites[1], group=groups[1], value='FW2'
        )

        ChemicalRecordF.create(
            survey=self.surveys[0], chem=ChemF.create(chem_code='ph'),
            value=7.5
        )

        division = TaxonGroupF.create(
            name='Division A',
            category=TaxonomicGroupCategory.DIVISION_GROUP.name
        )
        TaxonGroupTaxonomyF.create(
            taxongroup=division, taxonomy=self.taxonomy
        )
        self.dst_name = DecisionSupportToolName.objects.create(name='DST')

    def create_records(self, count, **kwargs):
        records = []
        for i in range(count):
            site_index = i % 2
            record = BiologicalCollectionRecordF.create(
                site=self.sites[site_index],
                survey=self.surveys[site_index],
                collection_date=self.date,
                owner=self.owner,
                taxonomy=self.taxonomy,
                source_reference=self.source_reference,
                **kwargs
            )
            DecisionSupportTool.objects.create(
                biological_collection_record=record,
                dst_name=self.dst_name,
                name='DST'
            )
            records.append(record)
        return records

    @staticmethod
    def survey_data(name):
        # Migrations may already seed these, with different casing.
        return (
            SurveyData.objects.filter(name__iexact=name).order_by('pk').first()
            or SurveyData.objects.create(name=name)
        )

    def serialize_batch(self, records):
        batch = next(collection_record_batches(
            BiologicalCollectionRecord.objects.filter(
                id__in=[r.id for r in records]
            ),
            batch_size=len(records)
        ))
        context = {'header': []}
        prefetch_batch(batch, context)
        return BioCollectionOneRowSerializer(
            batch, many=True, context=context
        ).data

    def assert_batch_matches_single_records(self, records):
        batch_rows = self.serialize_batch(records)
        self.assertEqual(len(batch_rows), len(records))
        for record, batch_row in zip(
                sorted(records, key=lambda r: r.pk), batch_rows):
            single_row = BioCollectionOneRowSerializer(
                BiologicalCollectionRecord.objects.get(pk=record.pk),
                context={'header': []}
            ).data
            self.assertEqual(dict(batch_row), dict(single_row))
        return batch_rows

    def test_batch_matches_single_record_serialization(self, mock_ctx):
        records = self.create_records(4)
        no_survey = BiologicalCollectionRecordF.create(
            site=self.sites[0], survey=None, collection_date=self.date,
            owner=self.owner, taxonomy=self.taxonomy,
        )
        # save() always assigns a survey; legacy rows can still lack one.
        BiologicalCollectionRecord.objects.filter(
            pk=no_survey.pk
        ).update(survey=None)
        records.append(no_survey)

        rows = self.assert_batch_matches_single_records(records)

        first = rows[0]
        self.assertEqual(first['Geomorphology'], 'GMZ1')
        self.assertEqual(first['Freshwater'], '-')
        self.assertEqual(first['PH'], 7.5)
        self.assertEqual(first['Division'], 'Division A')
        self.assertEqual(first['decision_support_tool'], 'DST')
        self.assertEqual(first['title'], 'Shared ref')
        self.assertEqual(rows[1]['Freshwater'], 'FW2')
        self.assertNotIn('PH', rows[1])

    def test_export_does_not_create_surveys(self, mock_ctx):
        record = BiologicalCollectionRecordF.create(
            site=self.sites[0], survey=None, collection_date=self.date,
            owner=UserF.create(), taxonomy=self.taxonomy,
        )
        survey_id = record.survey_id
        BiologicalCollectionRecord.objects.filter(
            pk=record.pk
        ).update(survey=None)
        Survey.objects.filter(pk=survey_id).delete()
        record.refresh_from_db()
        survey_count = Survey.objects.count()

        self.serialize_batch([record])

        self.assertEqual(Survey.objects.count(), survey_count)

    @mock.patch(f'{SERIALIZER_MODULE}.is_fbis', return_value=True)
    def test_fbis_batch_matches_single_record_serialization(
            self, mock_fbis, mock_ctx):
        ChemicalRecordF.create(
            survey=self.surveys[0],
            chem=ChemF.create(chem_code='TEMP', chem_description='Temp'),
            value=21.0
        )
        level = self.survey_data('Water Level')
        turbidity = self.survey_data('Water Turbidity')
        SurveyDataValue.objects.create(
            survey=self.surveys[0], survey_data=level,
            survey_data_option=SurveyDataOption.objects.create(
                survey_data=level, option='High'
            )
        )
        SurveyDataValue.objects.create(
            survey=self.surveys[0], survey_data=turbidity,
            survey_data_option=SurveyDataOption.objects.create(
                survey_data=turbidity, option='Clear'
            )
        )

        rows = self.assert_batch_matches_single_records(
            self.create_records(4)
        )

        temp_column = next(k for k in rows[0] if k.startswith('Temp ('))
        self.assertEqual(rows[0][temp_column], 21.0)
        self.assertEqual(rows[1][temp_column], '-')
        self.assertEqual(rows[0]['Water Level'], 'High')
        self.assertEqual(rows[0]['Water Turbidity'], 'Clear')

    def test_query_count_does_not_grow_with_batch_size(self, mock_ctx):
        records = self.create_records(16)

        def count_queries(batch_records):
            with CaptureQueriesContext(connection) as queries:
                self.serialize_batch(batch_records)
            # django-tenants sets the schema before each query
            return len([
                q for q in queries.captured_queries
                if not q['sql'].startswith('SET search_path')
            ])

        # Warm process-wide caches (content types, current site).
        count_queries(records[:1])
        self.assertEqual(
            count_queries(records[:4]),
            count_queries(records)
        )
