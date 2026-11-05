# coding=utf-8
import csv
import os
import tempfile
from io import StringIO

from django.core.management import call_command
from django.test import SimpleTestCase
from django_tenants.test.cases import FastTenantTestCase

from bims.management.commands.apply_legacy_custodian import chosen_custodian
from bims.management.commands.export_legacy_custodian_candidates import (
    base_url, dominant, is_generic, propose, record_path
)
from bims.models.biological_collection_record import (
    BiologicalCollectionRecord
)
from bims.tests.model_factories import BiologicalCollectionRecordF


class LegacyCustodianHelperTests(SimpleTestCase):

    def test_is_generic(self):
        for value in ('', 'bims', 'FBIS', 'Healthyrivers', ' healthyrivers '):
            self.assertTrue(is_generic(value), value)
        self.assertFalse(is_generic('Freshwater Research Centre'))

    def test_base_url(self):
        self.assertEqual(
            base_url('freshwaterbiodiversity.org'),
            'https://freshwaterbiodiversity.org')
        self.assertEqual(
            base_url('http://fbis.local:8000/'), 'http://fbis.local:8000')
        self.assertEqual(base_url(''), '')

    def test_record_path(self):
        self.assertEqual(record_path(True, 12, 5), '/sass/view/12/')
        self.assertEqual(record_path(False, None, 5), '/site-visit/detail/5/')
        self.assertEqual(record_path(True, None, 5), '/site-visit/detail/5/')
        self.assertEqual(record_path(False, None, None), '')

    def test_dominant_needs_records_and_share(self):
        counts = {'A': 9, 'B': 1}
        self.assertEqual(dominant(counts, 5, 0.8), ('A', 10, 0.9))
        self.assertIsNone(dominant(counts, 20, 0.8))
        self.assertIsNone(dominant({'A': 5, 'B': 5}, 5, 0.8))

    def test_proposal_order(self):
        row = {
            'survey_id': 1, 'source_reference_id': 2,
            'institution_code': 'CODE',
            'owner__organization': 'Owner Org', 'owner__username': 'user',
        }
        by_survey = {1: {'Survey Org': 3}}
        by_reference = {2: {'Ref Org': 50}}
        self.assertEqual(
            propose(row, by_survey, by_reference, 5, 0.8)[:2],
            ('Survey Org', 'same_site_visit'))
        # a survey with several custodians is not used
        by_survey = {1: {'A': 1, 'B': 1}}
        self.assertEqual(
            propose(row, by_survey, by_reference, 5, 0.8)[:2],
            ('CODE', 'additional_data'))
        row['institution_code'] = 'bims'
        self.assertEqual(
            propose(row, by_survey, by_reference, 5, 0.8)[:2],
            ('Ref Org', 'source_reference'))
        self.assertEqual(
            propose(row, by_survey, {}, 5, 0.8)[:2],
            ('Owner Org', 'owner_organisation'))
        row['owner__username'] = 'admin'
        self.assertEqual(
            propose(row, by_survey, {}, 5, 0.8)[:2], ('', 'none'))

    def test_chosen_custodian(self):
        self.assertEqual(
            chosen_custodian({
                'final_institution_id': 'Mine',
                'proposed_institution_id': 'Proposed', 'action': ''}),
            ('Mine', 'final'))
        self.assertEqual(
            chosen_custodian({
                'final_institution_id': '',
                'proposed_institution_id': 'Proposed',
                'action': ' Accept '}),
            ('Proposed', 'proposed'))
        # not approved: nothing is applied
        self.assertEqual(
            chosen_custodian({
                'final_institution_id': '',
                'proposed_institution_id': 'Proposed', 'action': ''}),
            ('', ''))


class LegacyCustodianApplyTests(FastTenantTestCase):

    def setUp(self):
        self.legacy = BiologicalCollectionRecordF.create(
            institution_id='Healthyrivers')
        self.real = BiologicalCollectionRecordF.create(
            institution_id='Existing Institute')
        self.unapproved = BiologicalCollectionRecordF.create(
            institution_id='bims')
        self.directory = tempfile.mkdtemp()

    def reviewed_file(self, rows):
        path = os.path.join(self.directory, 'reviewed.csv')
        fields = ['uuid', 'id', 'proposed_institution_id',
                  'proposal_source', 'action', 'final_institution_id']
        with open(path, 'w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def run_apply(self, path, **options):
        out = StringIO()
        call_command(
            'apply_legacy_custodian',
            schema_name=self.tenant.schema_name, input=path,
            stdout=out, **options)
        return out.getvalue()

    def rows(self):
        return [
            {'uuid': self.legacy.uuid, 'proposed_institution_id': 'New Org',
             'action': 'accept'},
            {'uuid': self.real.uuid, 'proposed_institution_id': 'New Org',
             'action': 'accept'},
            {'uuid': self.unapproved.uuid,
             'proposed_institution_id': 'New Org', 'action': ''},
        ]

    def test_dry_run_changes_nothing(self):
        output = self.run_apply(self.reviewed_file(self.rows()))
        self.assertIn('DRY RUN', output)
        self.legacy.refresh_from_db()
        self.assertEqual(self.legacy.institution_id, 'Healthyrivers')

    def test_apply_only_approved_and_not_overwriting(self):
        self.run_apply(self.reviewed_file(self.rows()), apply=True)
        for record in (self.legacy, self.real, self.unapproved):
            record.refresh_from_db()
        self.assertEqual(self.legacy.institution_id, 'New Org')
        self.assertEqual(self.real.institution_id, 'Existing Institute')
        self.assertEqual(self.unapproved.institution_id, 'bims')
