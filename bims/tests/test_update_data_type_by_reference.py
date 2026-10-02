# coding=utf-8
import datetime
from unittest import mock

from allauth.utils import get_user_model
from django.contrib.auth.models import Permission
from django.contrib.messages import get_messages
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient

from bims.factories import EntryFactory
from bims.models import BiologicalCollectionRecord, Survey
from bims.utils.gbif_publish import gather_data_for_source_reference
from bims.tests.model_factories import (
    SourceReferenceBibliographyF,
    BiologicalCollectionRecordF
)


class TestUpdateDataTypeByReference(FastTenantTestCase):
    """ Tests updating data type of records by source reference """

    def setUp(self):
        # Clearing cached search results runs in a background thread
        clear_patcher = mock.patch(
            'bims.views.source_reference.'
            'clear_finished_search_in_background')
        self.mock_clear_search = clear_patcher.start()
        self.addCleanup(clear_patcher.stop)

        user = get_user_model().objects.create(
            is_staff=True,
            is_active=True,
            is_superuser=True,
            username='@.test'
        )
        user.set_password('psst')
        user.save()

        non_staff_user = get_user_model().objects.create(
            is_staff=False,
            is_active=True,
            is_superuser=False,
            username='@.test2'
        )
        non_staff_user.set_password('psst')
        non_staff_user.save()

        self.client = TenantClient(self.tenant)

        entry = EntryFactory.create(title='Test')
        self.source_reference = SourceReferenceBibliographyF.create(
            source=entry)
        other_entry = EntryFactory.create(title='Other')
        self.other_reference = SourceReferenceBibliographyF.create(
            source=other_entry)

        self.record_2019 = BiologicalCollectionRecordF.create(
            source_reference=self.source_reference,
            collection_date=datetime.date(2019, 6, 1),
            data_type='private'
        )
        self.record_2020 = BiologicalCollectionRecordF.create(
            source_reference=self.source_reference,
            collection_date=datetime.date(2020, 6, 1),
            data_type='private'
        )
        self.record_2021 = BiologicalCollectionRecordF.create(
            source_reference=self.source_reference,
            collection_date=datetime.date(2021, 6, 1),
            data_type='sensitive'
        )
        self.other_record = BiologicalCollectionRecordF.create(
            source_reference=self.other_reference,
            collection_date=datetime.date(2020, 6, 1),
            data_type='private'
        )
        self.start = datetime.date.today() + datetime.timedelta(days=10)
        self.end = datetime.date.today() + datetime.timedelta(days=100)
        self.edit_url = (
            f'/edit-source-reference/{self.source_reference.id}/'
        )
        self.summary_url = (
            '/api/data-type-summary-by-source-reference-id/'
            f'{self.source_reference.id}/'
        )

    def _data_type(self, record):
        return BiologicalCollectionRecord.objects.get(
            id=record.id).data_type

    def _post_edit(self, data):
        post_dict = {'title': 'Test', 'year': 2020}
        post_dict.update(data)
        return self.client.post(self.edit_url, post_dict)

    def test_summary_requires_permission(self):
        self.client.login(username='@.test2', password='psst')
        response = self.client.get(self.summary_url)
        self.assertEqual(response.status_code, 403)

    def test_summary_counts(self):
        self.client.login(username='@.test', password='psst')
        BiologicalCollectionRecord.objects.filter(
            id=self.record_2019.id
        ).update(end_embargo_date=datetime.date.today() + datetime.timedelta(
            days=30))
        response = self.client.get(self.summary_url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['total'], 3)
        self.assertEqual(response.data['data_types']['private'], 2)
        self.assertEqual(response.data['data_types']['sensitive'], 1)
        self.assertEqual(response.data['under_embargo'], 1)
        response = self.client.get(
            self.summary_url, {'current_data_type': 'sensitive'})
        self.assertEqual(response.data['total'], 1)

    def test_update_requires_record_permission(self):
        user = get_user_model().objects.create(
            is_active=True, username='@.editor')
        user.set_password('psst')
        user.save()
        user.user_permissions.add(
            Permission.objects.get(codename='change_sourcereference'))
        self.client.login(username='@.editor', password='psst')
        self._post_edit({'new_data_type': 'public'})
        self.assertEqual(self._data_type(self.record_2020), 'private')

    def test_update_data_type_with_embargo(self):
        self.client.login(username='@.test', password='psst')
        response = self._post_edit({
            'current_data_type': 'private',
            'new_data_type': 'public',
            'embargo_start_date': self.start.strftime('%d/%m/%Y'),
            'embargo_end_date': self.end.strftime('%d/%m/%Y')
        })
        self.assertEqual(response.status_code, 302)
        for record in (self.record_2019, self.record_2020):
            record.refresh_from_db()
            self.assertEqual(record.data_type, 'public')
            self.assertEqual(
                record.start_embargo_date, self.start)
            self.assertEqual(
                record.end_embargo_date, self.end)
        self.record_2021.refresh_from_db()
        self.assertEqual(self.record_2021.data_type, 'sensitive')
        self.assertIsNone(self.record_2021.end_embargo_date)
        self.assertEqual(self._data_type(self.other_record), 'private')

    def test_unspecified_is_not_updated_to_public(self):
        self.client.login(username='@.test', password='psst')
        BiologicalCollectionRecord.objects.filter(
            id=self.record_2020.id
        ).update(data_type='')
        response = self._post_edit({'new_data_type': 'public'})
        self.assertEqual(response.status_code, 302)
        # Unspecified is already treated as public, so it is left as is
        self.assertEqual(self._data_type(self.record_2020), '')
        self.assertEqual(self._data_type(self.record_2019), 'public')
        self.assertEqual(self._data_type(self.record_2021), 'public')

    def test_unspecified_is_updated_to_private(self):
        self.client.login(username='@.test', password='psst')
        BiologicalCollectionRecord.objects.filter(
            id=self.record_2020.id
        ).update(data_type='')
        self._post_edit({'new_data_type': 'private'})
        self.assertEqual(self._data_type(self.record_2020), 'private')

    def test_embargo_applies_to_records_already_of_new_type(self):
        self.client.login(username='@.test', password='psst')
        BiologicalCollectionRecord.objects.filter(
            id=self.record_2020.id
        ).update(data_type='')
        self._post_edit({
            'new_data_type': 'public',
            'embargo_end_date': self.end.strftime('%d/%m/%Y')
        })
        self.record_2020.refresh_from_db()
        self.assertEqual(self.record_2020.data_type, '')
        self.assertEqual(self.record_2020.end_embargo_date, self.end)

    def test_update_embargo_only(self):
        self.client.login(username='@.test', password='psst')
        self._post_edit({'embargo_end_date': self.end.strftime('%d/%m/%Y')})
        self.record_2021.refresh_from_db()
        self.assertEqual(self.record_2021.data_type, 'sensitive')
        self.assertIsNone(self.record_2021.start_embargo_date)
        self.assertEqual(
            self.record_2021.end_embargo_date, self.end)

    def test_remove_embargo(self):
        self.client.login(username='@.test', password='psst')
        BiologicalCollectionRecord.objects.filter(
            source_reference=self.source_reference
        ).update(
            start_embargo_date=datetime.date(2026, 1, 1),
            end_embargo_date=datetime.date(2027, 1, 1))
        self._post_edit({
            'remove_embargo': '1',
            'embargo_end_date': '01/01/2000'
        })
        self.record_2020.refresh_from_db()
        self.assertIsNone(self.record_2020.start_embargo_date)
        self.assertIsNone(self.record_2020.end_embargo_date)

    def test_no_changes_keeps_records(self):
        self.client.login(username='@.test', password='psst')
        self._post_edit({'current_data_type': 'private'})
        self.record_2020.refresh_from_db()
        self.assertEqual(self.record_2020.data_type, 'private')
        self.assertIsNone(self.record_2020.end_embargo_date)

    def test_invalid_input(self):
        self.client.login(username='@.test', password='psst')
        today = datetime.date.today()
        invalid_posts = [
            {'new_data_type': 'secret'},
            # End date before start date
            {
                'embargo_start_date': self.end.strftime('%d/%m/%Y'),
                'embargo_end_date': self.start.strftime('%d/%m/%Y'),
            },
            # End date equal to start date
            {
                'embargo_start_date': self.start.strftime('%d/%m/%Y'),
                'embargo_end_date': self.start.strftime('%d/%m/%Y'),
            },
            # Start date without end date
            {'embargo_start_date': self.start.strftime('%d/%m/%Y')},
            # Start date in the past
            {
                'embargo_start_date': (
                    today - datetime.timedelta(days=1)).strftime('%d/%m/%Y'),
                'embargo_end_date': self.end.strftime('%d/%m/%Y'),
            },
            # Wrong date format
            {'embargo_end_date': self.end.isoformat()},
            # End date not after today without start date
            {'embargo_end_date': today.strftime('%d/%m/%Y')},
        ]
        for data in invalid_posts:
            data.setdefault('new_data_type', 'public')
            response = self._post_edit(data)
            self.assertEqual(response.status_code, 302)
            self.assertEqual(response['Location'], self.edit_url)
            self.record_2020.refresh_from_db()
            self.assertEqual(self.record_2020.data_type, 'private', data)
            self.assertIsNone(self.record_2020.end_embargo_date, data)

    def test_embargo_starting_today(self):
        self.client.login(username='@.test', password='psst')
        today = datetime.date.today()
        self._post_edit({
            'embargo_start_date': today.strftime('%d/%m/%Y'),
            'embargo_end_date': (
                today + datetime.timedelta(days=1)).strftime('%d/%m/%Y'),
        })
        self.record_2020.refresh_from_db()
        self.assertEqual(self.record_2020.start_embargo_date, today)

    def test_edit_page_shows_data_type_fields(self):
        self.client.login(username='@.test', password='psst')
        response = self.client.get(self.edit_url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'name="new_data_type"')
        self.assertContains(response, 'name="embargo_end_date"')

    def test_gbif_publish_skips_records_under_embargo(self):
        today = datetime.date.today()
        BiologicalCollectionRecord.objects.filter(
            source_reference=self.source_reference
        ).update(data_type='public')
        Survey.objects.filter(
            biological_collection_record__source_reference=(
                self.source_reference)
        ).update(validated=True)
        # Embargo active
        BiologicalCollectionRecord.objects.filter(
            id=self.record_2019.id
        ).update(end_embargo_date=today + datetime.timedelta(days=30))
        # Embargo has not started yet
        BiologicalCollectionRecord.objects.filter(
            id=self.record_2020.id
        ).update(
            start_embargo_date=today + datetime.timedelta(days=10),
            end_embargo_date=today + datetime.timedelta(days=30))
        record_ids = {
            record.id for record in
            gather_data_for_source_reference(self.source_reference)
        }
        self.assertNotIn(self.record_2019.id, record_ids)
        self.assertIn(self.record_2020.id, record_ids)
        self.assertIn(self.record_2021.id, record_ids)

    def _messages(self, response):
        return [str(m) for m in get_messages(response.wsgi_request)]

    def _editor(self):
        """User allowed to edit source references but not records"""
        user = get_user_model().objects.create(
            is_active=True, username='@.editor')
        user.set_password('psst')
        user.save()
        user.user_permissions.add(
            Permission.objects.get(codename='change_sourcereference'))
        return user

    def test_update_clears_search_results(self):
        self.client.login(username='@.test', password='psst')
        self._post_edit({'current_data_type': 'private'})
        self.mock_clear_search.assert_not_called()
        self._post_edit({'new_data_type': 'public'})
        self.mock_clear_search.assert_called_once()

    def test_success_message(self):
        self.client.login(username='@.test', password='psst')
        response = self._post_edit({
            'current_data_type': 'private',
            'new_data_type': 'public',
            'embargo_start_date': self.start.strftime('%d/%m/%Y'),
            'embargo_end_date': self.end.strftime('%d/%m/%Y')
        })
        self.assertIn(
            'Records updated: 2 record(s) set to public; '
            'embargo set on 2 record(s) '
            f'from {self.start:%d/%m/%Y} until {self.end:%d/%m/%Y}.',
            self._messages(response))

    def test_success_message_embargo_without_start_date(self):
        self.client.login(username='@.test', password='psst')
        response = self._post_edit({
            'embargo_end_date': self.end.strftime('%d/%m/%Y')
        })
        self.assertIn(
            'Records updated: embargo set on 3 record(s) '
            f'until {self.end:%d/%m/%Y}.',
            self._messages(response))

    def test_success_message_remove_embargo(self):
        self.client.login(username='@.test', password='psst')
        response = self._post_edit({'remove_embargo': '1'})
        self.assertIn(
            'Records updated: embargo removed from 3 record(s).',
            self._messages(response))

    def test_success_message_counts_only_changed_records(self):
        self.client.login(username='@.test', password='psst')
        BiologicalCollectionRecord.objects.filter(
            id=self.record_2020.id
        ).update(data_type='')
        response = self._post_edit({'new_data_type': 'public'})
        # Unspecified is already public, only private and sensitive change
        self.assertIn(
            'Records updated: 2 record(s) set to public.',
            self._messages(response))

    def test_error_message_and_redirect_keep_next_url(self):
        self.client.login(username='@.test', password='psst')
        edit_url = self.edit_url + '?next=/source-references/'
        past_date = datetime.date.today() - datetime.timedelta(days=1)
        response = self.client.post(edit_url, {
            'title': 'Updated title',
            'year': 2020,
            'new_data_type': 'public',
            'embargo_start_date': past_date.strftime('%d/%m/%Y'),
            'embargo_end_date': self.end.strftime('%d/%m/%Y')
        })
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response['Location'], edit_url)
        self.assertIn(
            'Embargo start date must be today or in the future.',
            self._messages(response))
        # Nothing is saved, including the other form fields
        self.source_reference.refresh_from_db()
        self.assertNotEqual(self.source_reference.title, 'Updated title')
        self.mock_clear_search.assert_not_called()

    def test_edit_page_confirm_message_mentions_owner(self):
        self.client.login(username='@.test', password='psst')
        response = self.client.get(self.edit_url)
        self.assertContains(
            response, '(hidden from everyone but the owner)')

    def test_edit_page_hides_data_type_fields_without_permission(self):
        self._editor()
        self.client.login(username='@.editor', password='psst')
        response = self.client.get(self.edit_url)
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, 'name="new_data_type"')

    def test_edit_page_without_records_hides_data_type_fields(self):
        self.client.login(username='@.test', password='psst')
        BiologicalCollectionRecord.objects.filter(
            source_reference=self.source_reference).delete()
        response = self.client.get(self.edit_url)
        self.assertNotContains(response, 'name="new_data_type"')

    def test_summary_unspecified_filter(self):
        self.client.login(username='@.test', password='psst')
        BiologicalCollectionRecord.objects.filter(
            id=self.record_2020.id
        ).update(data_type='')
        response = self.client.get(
            self.summary_url, {'current_data_type': 'unspecified'})
        self.assertEqual(response.data['total'], 1)
        self.assertEqual(response.data['data_types']['unspecified'], 1)

    def test_summary_invalid_filter(self):
        self.client.login(username='@.test', password='psst')
        response = self.client.get(
            self.summary_url, {'current_data_type': 'secret'})
        self.assertEqual(response.status_code, 400)

    def test_summary_future_embargo_is_not_counted(self):
        self.client.login(username='@.test', password='psst')
        BiologicalCollectionRecord.objects.filter(
            id=self.record_2019.id
        ).update(
            start_embargo_date=self.start, end_embargo_date=self.end)
        response = self.client.get(self.summary_url)
        self.assertEqual(response.data['under_embargo'], 0)
