"""
Tests for DownloadRequest helpers:
  - params_from_dashboard_url
  - resume_stalled_downloads periodic task
"""
import os
from datetime import timedelta
from unittest import mock

from django.test import TestCase, override_settings
from django.utils import timezone

from bims.models.download_request import (
    DownloadRequest,
    normalize_download_params,
    params_from_dashboard_url,
)
from bims.tests.model_factories import UserF

FAKE_MEDIA = '/tmp/test_media'
FAKE_CSV_PATH = 'processed_csv'


# ---------------------------------------------------------------------------
# params_from_dashboard_url
# ---------------------------------------------------------------------------

@override_settings(MEDIA_ROOT=FAKE_MEDIA, PROCESSED_CSV_PATH=FAKE_CSV_PATH)
class TestParamsFromDashboardUrl(TestCase):

    def setUp(self):
        self.user = UserF.create()

    def _make_request(self, dashboard_url=''):
        return DownloadRequest.objects.create(
            requester=self.user,
            resource_name='Occurrence Data',
            resource_type=DownloadRequest.CSV,
            dashboard_url=dashboard_url,
        )

    def test_returns_none_when_no_dashboard_url(self):
        dr = self._make_request(dashboard_url='')
        path, params = params_from_dashboard_url(dr)
        self.assertIsNone(path)
        self.assertIsNone(params)

    def test_parses_params_from_query_string(self):
        dr = self._make_request(
            dashboard_url='https://example.com/map/?siteId=42&taxon=Foo'
        )
        with mock.patch('os.makedirs'):
            path, params = params_from_dashboard_url(dr)

        self.assertEqual(params['siteId'], '42')
        self.assertEqual(params['taxon'], 'Foo')
        self.assertEqual(params['downloadRequestId'], str(dr.pk))

    def test_parses_params_from_fragment(self):
        dr = self._make_request(
            dashboard_url='https://example.com/#site-detail/siteId=99&taxon=Bar'
        )
        with mock.patch('os.makedirs'):
            path, params = params_from_dashboard_url(dr)

        self.assertEqual(params['siteId'], '99')
        self.assertEqual(params['taxon'], 'Bar')
        self.assertEqual(params['downloadRequestId'], str(dr.pk))

    def test_fragment_without_prefix_segment(self):
        dr = self._make_request(
            dashboard_url='https://example.com/#siteId=7'
        )
        with mock.patch('os.makedirs'):
            path, params = params_from_dashboard_url(dr)

        self.assertEqual(params['siteId'], '7')

    def test_path_uses_username_and_settings(self):
        dr = self._make_request(
            dashboard_url='https://example.com/?siteId=1'
        )
        with mock.patch('os.makedirs'):
            path, params = params_from_dashboard_url(dr)

        self.assertIn(self.user.username, path)
        self.assertIn(FAKE_CSV_PATH, path)
        self.assertIn(FAKE_MEDIA, path)

    def test_path_is_deterministic_for_same_day(self):
        dr = self._make_request(
            dashboard_url='https://example.com/?siteId=1'
        )
        with mock.patch('os.makedirs'):
            path1, params1 = params_from_dashboard_url(dr)
            path2, params2 = params_from_dashboard_url(dr)

        self.assertEqual(path1, path2)
        self.assertEqual(params1, params2)

    def test_unknown_username_when_no_requester(self):
        dr = DownloadRequest.objects.create(
            requester=None,
            resource_name='Occurrence Data',
            resource_type=DownloadRequest.CSV,
            dashboard_url='https://example.com/?siteId=1',
        )
        with mock.patch('os.makedirs'):
            path, params = params_from_dashboard_url(dr)

        self.assertIn('unknown', path)
        self.assertIsNotNone(params)

    def test_duplicate_params_use_last_value(self):
        params = normalize_download_params({
            'modules': ['21', '22', '37'],
            'rank': ['', 'GENUS'],
            'taxon': [''],
        })

        self.assertEqual(params['modules'], '37')
        self.assertEqual(params['rank'], 'GENUS')
        self.assertEqual(params['taxon'], '')


# ---------------------------------------------------------------------------
# resume_stalled_downloads
# ---------------------------------------------------------------------------

class _FakeTenant:
    schema_name = 'test'


@override_settings(MEDIA_ROOT=FAKE_MEDIA, PROCESSED_CSV_PATH=FAKE_CSV_PATH)
class TestResumeStalledDownloads(TestCase):

    def setUp(self):
        self.user = UserF.create()

    def _stale_request(self, **kwargs):
        """Create an approved, stale DownloadRequest ready for resumption."""
        stale_time = timezone.now() - timedelta(minutes=30)
        defaults = dict(
            requester=self.user,
            resource_name='Occurrence Data',
            resource_type=DownloadRequest.CSV,
            approved=True,
            rejected=False,
            progress_updated_at=stale_time,
            download_path='/tmp/test_media/processed_csv/somefile',
            download_params={'siteId': '1', 'downloadRequestId': '1'},
        )
        defaults.update(kwargs)
        return DownloadRequest.objects.create(**defaults)

    def _run_task(self):
        """
        Run resume_stalled_downloads with tenant machinery mocked out so
        the loop body executes against the real test database.
        """
        from bims.tasks.collection_record import resume_stalled_downloads

        fake_tenant_qs = mock.MagicMock()
        fake_tenant_qs.__iter__ = mock.Mock(return_value=iter([_FakeTenant()]))
        fake_tenant_model = mock.MagicMock()
        fake_tenant_model.objects.all.return_value = fake_tenant_qs

        with mock.patch(
            'bims.tasks.collection_record.download_collection_record_task'
        ) as mock_task, mock.patch(
            'django_tenants.utils.get_tenant_model', return_value=fake_tenant_model
        ), mock.patch(
            'django_tenants.utils.tenant_context',
            return_value=mock.MagicMock(__enter__=mock.Mock(return_value=None),
                                        __exit__=mock.Mock(return_value=False)),
        ):
            resume_stalled_downloads()

        return mock_task

    def test_stalled_request_is_restarted(self):
        dr = self._stale_request()
        mock_task = self._run_task()

        mock_task.delay.assert_called_once_with(
            dr.download_path,
            dr.download_params,
            send_email=True,
            user_id=self.user.id,
        )

    def test_progress_cleared_on_resume(self):
        dr = self._stale_request(progress='50/200')
        self._run_task()

        dr.refresh_from_db()
        self.assertIsNone(dr.progress)
        self.assertIsNone(dr.progress_updated_at)

    def test_not_started_request_is_restarted(self):
        """progress=None but progress_updated_at is stale (dispatched, never picked up)."""
        self._stale_request(progress=None)
        mock_task = self._run_task()

        mock_task.delay.assert_called_once()

    def test_fresh_request_is_not_restarted(self):
        """progress_updated_at within threshold — should not be touched."""
        DownloadRequest.objects.create(
            requester=self.user,
            resource_name='Occurrence Data',
            resource_type=DownloadRequest.CSV,
            approved=True,
            rejected=False,
            progress_updated_at=timezone.now(),
            download_path='/tmp/test_media/processed_csv/fresh',
            download_params={'siteId': '1'},
        )
        mock_task = self._run_task()
        mock_task.delay.assert_not_called()

    def test_unapproved_request_is_ignored(self):
        self._stale_request(approved=False)
        mock_task = self._run_task()
        mock_task.delay.assert_not_called()

    def test_rejected_request_is_ignored(self):
        self._stale_request(rejected=True)
        mock_task = self._run_task()
        mock_task.delay.assert_not_called()

    def test_missing_params_falls_back_to_dashboard_url(self):
        """When download_params is missing, derive from dashboard_url."""
        stale_time = timezone.now() - timedelta(minutes=30)
        dr = DownloadRequest.objects.create(
            requester=self.user,
            resource_name='Occurrence Data',
            resource_type=DownloadRequest.CSV,
            approved=True,
            rejected=False,
            progress_updated_at=stale_time,
            download_path=None,
            download_params=None,
            dashboard_url='https://example.com/?siteId=5',
        )
        with mock.patch('os.makedirs'):
            mock_task = self._run_task()

        mock_task.delay.assert_called_once()
        dr.refresh_from_db()
        self.assertIsNotNone(dr.download_path)
        self.assertIsNotNone(dr.download_params)
        self.assertEqual(dr.download_params['siteId'], '5')

    def test_missing_params_and_no_dashboard_url_is_skipped(self):
        """No download_params and no dashboard_url — cannot restart, skip."""
        stale_time = timezone.now() - timedelta(minutes=30)
        DownloadRequest.objects.create(
            requester=self.user,
            resource_name='Occurrence Data',
            resource_type=DownloadRequest.CSV,
            approved=True,
            rejected=False,
            progress_updated_at=stale_time,
            download_path=None,
            download_params=None,
            dashboard_url='',
        )
        mock_task = self._run_task()
        mock_task.delay.assert_not_called()

    def test_partial_file_is_deleted_before_restart(self):
        dr = self._stale_request()
        with mock.patch('os.path.exists', return_value=True), \
             mock.patch('os.remove') as mock_remove:
            self._run_task()

        mock_remove.assert_called_once_with(dr.download_path)
