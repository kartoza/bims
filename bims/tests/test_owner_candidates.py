# coding=utf-8
from collections import defaultdict

from django.test import SimpleTestCase

from bims.management.commands.apply_owner_candidates import chosen_owner
from bims.management.commands.export_owner_candidates import (
    choose_owner, reason_for
)


def record(**kwargs):
    row = {
        'uuid': 'u1', 'owner_id': 1, 'owner__username': 'john',
        'collector_user_id': 2, 'collector': 'Jane Doe', 'survey_id': 10,
        'source_reference_id': 20,
    }
    row.update(kwargs)
    return row


def users():
    by_name = defaultdict(set)
    by_name['jane doe'].add(5)
    by_name['jane_doe'].add(5)
    by_name['john smith'].update({6, 7})
    return {
        'by_name': by_name,
        'username': {1: 'john', 2: 'uploader', 5: 'jane_doe'},
        'name': {},
    }


class SassSiteVisitOwnerTests(SimpleTestCase):

    def test_sass_site_visit_owner(self):
        row = record(is_sass=True, sass_site_visit_id=30, collector='')
        self.assertEqual(
            choose_owner(row, users(), {}, {}, {30: 5})[:2],
            (5, 'sass_site_visit'))
        # not for non SASS data, nor when it is the current owner
        row['is_sass'] = False
        self.assertEqual(
            choose_owner(row, users(), {}, {}, {30: 5})[1], 'none')
        row.update(is_sass=True, owner_id=5)
        self.assertEqual(
            choose_owner(row, users(), {}, {}, {30: 5})[1], 'none')


class OwnerReasonTests(SimpleTestCase):

    def test_reasons(self):
        self.assertEqual(
            reason_for(record(owner_id=None, owner__username=None), set()),
            'owner_blank')
        self.assertEqual(
            reason_for(record(owner__username='Kartoza_Admin'), set()),
            'owner_is_admin')
        # owner is the uploader
        row = record(owner_id=2, collector='')
        self.assertEqual(
            reason_for(row, set()), 'owner_is_uploader_collector_blank')
        self.assertEqual(
            reason_for(row, {'u1'}), 'session_csv_blank_owner')
        # an owner that is not the uploader, or a collector that is known
        self.assertEqual(reason_for(record(), set()), '')
        self.assertEqual(
            reason_for(record(owner_id=2, collector='Jane Doe'), set()), '')


class PossibleOwnerTests(SimpleTestCase):

    def test_collector_text_must_be_one_user(self):
        self.assertEqual(
            choose_owner(record(), users(), {}, {})[:2],
            (5, 'collector_text'))
        # two users with the same full name: not guessed
        row = record(collector='John Smith')
        self.assertEqual(
            choose_owner(row, users(), {}, {})[1], 'none')

    def test_same_survey_ignores_uploader_and_current_owner(self):
        row = record(collector='')
        by_survey = {10: {1, 2, 9}}
        self.assertEqual(
            choose_owner(row, users(), by_survey, {})[:2],
            (9, 'same_survey'))
        by_survey = {10: {9, 8}}
        self.assertEqual(
            choose_owner(row, users(), by_survey, {})[1], 'none')

    def test_source_reference_author(self):
        row = record(collector='')
        self.assertEqual(
            choose_owner(row, users(), {}, {20: {7}})[:2],
            (7, 'source_reference_author'))
        self.assertEqual(
            choose_owner(row, users(), {}, {20: {7, 8}})[1], 'none')

    def test_uploader_only_for_admin_owner(self):
        row = record(collector='', owner__username='kartoza_admin')
        self.assertEqual(
            choose_owner(row, users(), {}, {})[:2], (2, 'uploader'))
        self.assertEqual(
            choose_owner(record(collector=''), users(), {}, {})[1], 'none')


class ChosenOwnerTests(SimpleTestCase):

    def test_chosen_owner(self):
        self.assertEqual(
            chosen_owner({'final_owner': 'bob', 'possible_owner': 'jane',
                          'action': ''}),
            ('bob', 'final'))
        self.assertEqual(
            chosen_owner({'final_owner': '', 'possible_owner': 'jane',
                          'action': 'Accept'}),
            ('jane', 'possible'))
        self.assertEqual(
            chosen_owner({'final_owner': '', 'possible_owner': 'jane',
                          'action': ''}),
            ('', ''))
