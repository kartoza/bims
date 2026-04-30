import io
import os
import mock
import json
import datetime

from django.core.files.uploadedfile import SimpleUploadedFile
from django_tenants.test.cases import FastTenantTestCase
from preferences import preferences
from django.db.models import Q
from django.test import TestCase, override_settings
from django.core.files import File
from bims.scripts.collections_upload_source_reference import (
    process_source_reference
)
from bims.tests.model_factories import (
    DocumentF,
    UserF,
    UploadSessionF,
    TaxonomyF,
    TaxonGroupF, BiologicalCollectionRecordF, SiteF,
    LocationSiteF,
)
from bims.models import UploadSession, BiologicalCollectionRecord, Survey
from bims.models.sampling_effort_measure import SamplingEffortMeasure
from bims.scripts.occurrences_upload import (
    OccurrencesCSVUpload,
    OccurrenceProcessor,
)

from bims.models import SiteSetting

test_data_directory = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'data')


def mocked_location_context_data(url):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    response_file = 'geocontext_data.json'
    response_path = os.path.join(
        test_data_directory, response_file)
    if os.path.exists(response_path):
        response_data = open(response_path)
        json_data = response_data.read()
        response_data.close()
        return MockResponse(json.loads(json_data), 200)
    return ''


def mocked_doi_loader(
        ids = None, format = "bibtex", style = 'apa',
        locale = "en-US", url = None, **kwargs):
    response_file = 'doi_data.json'
    response_path = os.path.join(
        test_data_directory, response_file)
    if os.path.exists(response_path):
        response_data = open(response_path)
        json_data = response_data.read()
        response_data.close()
        return json_data
    return ''


class TestCollectionUpload(FastTenantTestCase):
    document_link = 'site/document/11'
    owner = None
    reference_title = 'title'

    def setUp(self):
        self.owner = UserF.create(
            first_name='dimas'
        )

    def test_reference_missing_author(self):
        # Missing author
        message, source_reference = process_source_reference(
            reference='name',
            document_link=self.document_link,
            reference_category='published report'
        )
        self.assertTrue('missing author' in message.lower())

    def test_reference_document_not_exist(self):
        message, source_reference = process_source_reference(
            document_author='dimas',
            reference='name',
            document_link=self.document_link,
            reference_category='published report'
        )
        self.assertTrue('does not exist' in message)

    def test_reference_published_report_created(self):
        # Published report source reference created
        DocumentF.create(
            id=11,
            owner=self.owner,
            title=self.reference_title
        )
        message, source_reference = process_source_reference(
            document_author='dimas',
            reference=self.reference_title,
            document_link=self.document_link,
            reference_category='published report'
        )
        self.assertIsNotNone(source_reference)
        self.assertEqual(
            source_reference.source_name,
            self.reference_title
        )
        self.assertEqual(
            source_reference.title,
            self.reference_title
        )
        self.assertIsNotNone(
            source_reference.active_sites.first()
        )

    def test_reference_bibliography_not_created(self):
        message, source_reference = process_source_reference(
            document_author=self.owner.first_name,
            reference=self.reference_title,
            doi='1123',
            reference_category='peer-reviewed'
        )
        self.assertIsNone(
            source_reference
        )

    @mock.patch('habanero.cn.content_negotiation', mock.Mock(
        side_effect=mocked_doi_loader))
    def test_reference_bibliography_created(self):
        doc = DocumentF.create(
            id=11,
            owner=self.owner,
            title=self.reference_title
        )
        message, source_reference = process_source_reference(
            document_link=self.document_link,
            document_author=self.owner.first_name,
            reference=self.reference_title,
            doi='10.1016/j.aaen.2007.05.002',
            reference_category='peer-reviewed'
        )
        self.assertIsNotNone(
            source_reference
        )
        self.assertEqual(
            source_reference.document,
            doc
        )

    def test_reference_database_created(self):
        message, source_reference = process_source_reference(
            document_author=self.owner.first_name,
            reference=self.reference_title,
            source_year='2012',
            reference_category='database'
        )
        self.assertIsNotNone(
            source_reference
        )

    @override_settings(GEOCONTEXT_URL="test.gecontext.com")
    @mock.patch('requests.get', mock.Mock(side_effect=mocked_location_context_data))
    @mock.patch('bims.scripts.data_upload.DataCSVUpload.finish')
    @mock.patch('bims.scripts.occurrences_upload.OccurrenceProcessor.update_location_site_context')
    @mock.patch('bims.scripts.occurrences_upload.get_feature_centroid')
    @mock.patch('bims.scripts.occurrences_upload.fetch_river_name')
    def test_csv_upload(self,
                        mock_fetch_river_name,
                        mock_get_feature_centroid,
                        mock_update_location_context,
                        mock_finish):

        site_setting = preferences.SiteSetting

        if not site_setting:
            site_setting = SiteSetting.objects.create()

        if site_setting:
            csv_content = b"Park_Name,x,y\nPark A,34.0522,-118.2437\nPark B,36.1699,-115.1398\n"
            csv_file = SimpleUploadedFile(
                "park_data.csv",
                csv_content,
                content_type="text/csv"
            )
            site_setting.default_data_source = 'fbis'
            site_setting.park_layer_csv = csv_file
            csv_content = b"section,x,y\nSection 1,31.0522,-111.2437\nSection 2,31.1699,-111.1398\n"
            csv_file = SimpleUploadedFile(
                "section.csv",
                csv_content,
                content_type="text/csv"
            )
            site_setting.section_layer_csv = csv_file

            site_setting.save()

        mock_finish.return_value = None

        mock_get_feature_centroid.return_value = (1, 1)
        mock_fetch_river_name.return_value = 'River Name'

        taxonomy_1 = TaxonomyF.create(
            canonical_name='Achnanthes eutrophila',
            rank='SPECIES',
            taxonomic_status='ACCEPTED'
        )

        taxon_group = TaxonGroupF.create(
            name='test',
            taxonomies=(taxonomy_1,)
        )

        BiologicalCollectionRecordF.create(
            taxonomy=taxonomy_1,
            module_group=taxon_group,
            uuid='5a08bfe1-0e9b-4e0e-bf30-5b50156d35a9'
        )
        self.assertTrue(BiologicalCollectionRecord.objects.filter(
            uuid='5a08bfe1-0e9b-4e0e-bf30-5b50156d35a9'
        ).exists())

        with open(os.path.join(
            test_data_directory, 'csv_upload_test.csv'
        ), 'rb') as file:
            upload_session = UploadSessionF.create(
                uploader=self.owner,
                process_file=File(file),
                module_group=taxon_group
            )

        saved_instance = UploadSession.objects.get(pk=upload_session.pk)

        self.assertTrue(saved_instance.process_file)

        data_upload = OccurrencesCSVUpload()
        data_upload.upload_session = saved_instance
        data_upload.start()

        bio = BiologicalCollectionRecord.objects.filter(
            Q(uuid='5a08bfe1-0e9b-4e0e-bf30-5b50156d35a9') |
            Q(uuid='5a08bfe10e9b4e0ebf305b50156d35a9')
        )
        self.assertTrue(bio.exists())
        self.assertEqual(bio.first().sampling_effort_link.name, 'Time(min)')
        self.assertEqual(BiologicalCollectionRecord.objects.filter(
            Q(uuid='5a08bfe1-0e9b-4e0e-bf30-5b50156d35a9') |
            Q(uuid='5a08bfe10e9b4e0ebf305b50156d35a9')
        ).count(), 1)

        with open(os.path.join(
                test_data_directory, 'csv_upload_test_2.csv'
        ), 'rb') as file:
            upload_session_2 = UploadSessionF.create(
                uploader=self.owner,
                process_file=File(file),
                module_group=taxon_group
            )

        saved_instance = UploadSession.objects.get(pk=upload_session_2.pk)

        self.assertTrue(saved_instance.process_file)

        data_upload = OccurrencesCSVUpload()
        data_upload.upload_session = saved_instance
        data_upload.start()

        bio = BiologicalCollectionRecord.objects.filter(
            Q(uuid='5a08bfe1-0e9b-4e0e-bf30-5b50156d35a9') |
            Q(uuid='5a08bfe10e9b4e0ebf305b50156d35a9')
        )
        self.assertEqual(bio.count(), 1)
        self.assertEqual(bio.first().site.legacy_river_name, 'User River Name 2')
        self.assertEqual(
            BiologicalCollectionRecord.objects.filter(
                site__name='Park A'
            ).count(), 2
        )
        self.assertEqual(
            BiologicalCollectionRecord.objects.filter(
                site__name='Section 2'
            ).count(), 1
        )
        self.assertEqual(
            BiologicalCollectionRecord.objects.filter(
                site__latitude=31.0522,
                site__longitude=-111.2437
            ).count(), 1
        )


class TestProcessSurveySamplingEffort(FastTenantTestCase):
    """Tests that process_survey separates surveys by sampling effort."""

    def setUp(self):
        self.collector = UserF.create()
        self.site = LocationSiteF.create()
        self.date = datetime.date(2024, 1, 15)
        self.time_measure, _ = SamplingEffortMeasure.objects.get_or_create(name='Time(min)')
        self.area_measure, _ = SamplingEffortMeasure.objects.get_or_create(name='Area(m2)')
        self.processor = OccurrenceProcessor()

    def _process(self, sampling_effort, sampling_effort_link):
        self.processor.process_survey(
            record={},
            location_site=self.site,
            sampling_date=self.date,
            collector=self.collector,
            sampling_effort=sampling_effort,
            sampling_effort_link=sampling_effort_link,
        )
        return self.processor.survey

    def _add_record(self, survey, sampling_effort, sampling_effort_link):
        taxonomy = TaxonomyF.create()
        BiologicalCollectionRecordF.create(
            site=self.site,
            survey=survey,
            owner=self.collector,
            sampling_effort=sampling_effort,
            sampling_effort_link=sampling_effort_link,
            taxonomy=taxonomy,
        )

    def test_different_effort_value_creates_separate_surveys(self):
        """Two rows with the same site/date/collector but different
        sampling_effort values must land in different surveys."""
        survey_a = self._process('10', self.time_measure)
        self._add_record(survey_a, '10', self.time_measure)

        survey_b = self._process('20', self.time_measure)

        self.assertNotEqual(survey_a.id, survey_b.id)
        self.assertEqual(Survey.objects.filter(
            site=self.site,
            date=self.date,
            collector_user=self.collector,
        ).count(), 2)

    def test_different_effort_measure_creates_separate_surveys(self):
        """Two rows with the same effort value but different measures must
        land in different surveys."""
        survey_a = self._process('10', self.time_measure)
        self._add_record(survey_a, '10', self.time_measure)

        survey_b = self._process('10', self.area_measure)

        self.assertNotEqual(survey_a.id, survey_b.id)

    def test_same_effort_reuses_survey(self):
        """A row with the same effort combination must reuse the existing survey."""
        survey_a = self._process('10', self.time_measure)
        self._add_record(survey_a, '10', self.time_measure)

        survey_same = self._process('10', self.time_measure)

        self.assertEqual(survey_a.id, survey_same.id)
        self.assertEqual(Survey.objects.filter(
            site=self.site,
            date=self.date,
            collector_user=self.collector,
        ).count(), 1)

    def test_no_existing_survey_creates_one(self):
        """When no survey exists yet, one is created."""
        self.assertEqual(Survey.objects.filter(
            site=self.site,
            date=self.date,
            collector_user=self.collector,
        ).count(), 0)

        survey = self._process('5', self.time_measure)

        self.assertIsNotNone(survey)
        self.assertEqual(Survey.objects.filter(
            site=self.site,
            date=self.date,
            collector_user=self.collector,
        ).count(), 1)

    def test_survey_with_no_records_is_reused(self):
        """A survey that exists but has no records yet should be reused
        regardless of the requested sampling effort."""
        survey_a = self._process('10', self.time_measure)
        # No record added — survey is empty

        survey_b = self._process('99', self.area_measure)

        self.assertEqual(survey_a.id, survey_b.id)