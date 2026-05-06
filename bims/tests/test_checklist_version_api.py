# coding=utf-8
"""Tests for the ChecklistVersion list/detail API endpoints."""
import shutil
import tempfile
import uuid
from unittest.mock import patch

from django.core.files.base import ContentFile
from django.urls import reverse
from django.test import override_settings
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient
from rest_framework import status

from bims.models.checklist_version import ChecklistVersion
from bims.models.download_request import DownloadRequest
from bims.models.licence import Licence
from bims.tests.model_factories import TaxonGroupF, UserF


def _licence():
    obj, _ = Licence.objects.get_or_create(
        identifier='CC-BY-4.0',
        defaults={
            'name': 'Creative Commons Attribution 4.0',
            'url': 'https://creativecommons.org/licenses/by/4.0/',
        },
    )
    return obj


def _make_version(taxon_group, version='1.0', status=ChecklistVersion.STATUS_PUBLISHED, **kwargs):
    return ChecklistVersion.objects.create(
        taxon_group=taxon_group,
        version=version,
        license=_licence(),
        status=status,
        **kwargs,
    )


class TestChecklistVersionListAPI(FastTenantTestCase):

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.user = UserF.create()
        self.expert = UserF.create()
        self.superuser = UserF.create(is_superuser=True, is_staff=True)
        self.group = TaxonGroupF.create(name='Fish')
        self.other_group = TaxonGroupF.create(name='Frogs')
        self.url = reverse('checklist-version-list')
        self.group.experts.add(self.expert)

        self.v1 = _make_version(self.group, version='1.0')
        self.v2 = _make_version(self.group, version='2.0')
        self.v_other = _make_version(self.other_group, version='1.0')
        self.v_draft = _make_version(
            self.group, version='3.0-draft',
            status=ChecklistVersion.STATUS_DRAFT,
        )

    def test_list_returns_200(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_list_returns_only_published_by_default(self):
        response = self.client.get(self.url)
        ids = [r['id'] for r in response.data['results']]
        self.assertIn(str(self.v1.pk), ids)
        self.assertIn(str(self.v2.pk), ids)
        self.assertNotIn(str(self.v_draft.pk), ids)

    def test_list_filter_by_taxon_group(self):
        response = self.client.get(self.url, {'taxon_group': self.group.pk})
        ids = [r['id'] for r in response.data['results']]
        self.assertIn(str(self.v1.pk), ids)
        self.assertNotIn(str(self.v_other.pk), ids)

    def test_list_response_fields(self):
        response = self.client.get(self.url)
        result = response.data['results'][0]
        for field in ('id', 'version', 'status', 'taxon_group', 'taxon_group_name',
                      'taxa_count', 'additions_count', 'updates_count',
                      'created_at', 'published_at'):
            self.assertIn(field, result)

    def test_list_pagination_keys(self):
        response = self.client.get(self.url)
        self.assertIn('count', response.data)
        self.assertIn('next', response.data)
        self.assertIn('previous', response.data)
        self.assertIn('results', response.data)

    def test_draft_hidden_from_anonymous(self):
        response = self.client.get(self.url, {'status': 'draft'})
        # non-superuser gets published results silently
        ids = [r['id'] for r in response.data['results']]
        self.assertNotIn(str(self.v_draft.pk), ids)

    def test_superuser_can_list_drafts(self):
        self.client.force_login(self.superuser)
        response = self.client.get(self.url, {'status': 'draft'})
        ids = [r['id'] for r in response.data['results']]
        self.assertIn(str(self.v_draft.pk), ids)

    def test_group_expert_can_list_drafts_for_managed_group(self):
        self.client.force_login(self.expert)
        response = self.client.get(self.url, {
            'status': 'draft',
            'taxon_group': self.group.pk,
        })
        ids = [r['id'] for r in response.data['results']]
        self.assertIn(str(self.v_draft.pk), ids)

    def test_group_expert_cannot_list_drafts_for_other_group(self):
        self.client.force_login(self.expert)
        response = self.client.get(self.url, {
            'status': 'draft',
            'taxon_group': self.other_group.pk,
        })
        ids = [r['id'] for r in response.data['results']]
        self.assertNotIn(str(self.v_draft.pk), ids)

    def test_page_size_respected(self):
        response = self.client.get(self.url, {'page_size': 1})
        self.assertEqual(len(response.data['results']), 1)
        self.assertIsNotNone(response.data['next'])

    def test_taxon_group_name_populated(self):
        response = self.client.get(self.url, {'taxon_group': self.group.pk})
        result = response.data['results'][0]
        self.assertEqual(result['taxon_group_name'], self.group.name)


class TestChecklistVersionDetailAPI(FastTenantTestCase):

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.publisher = UserF.create(first_name='Alice', last_name='Smith')
        self.group = TaxonGroupF.create(name='Birds')
        self.version = _make_version(self.group, version='1.0')
        self.version.published_by = self.publisher
        self.version.save(update_fields=['published_by'])

    def _url(self, pk):
        return reverse('checklist-version-detail', args=[pk])

    def test_detail_returns_200(self):
        response = self.client.get(self._url(self.version.pk))
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_detail_returns_correct_version(self):
        response = self.client.get(self._url(self.version.pk))
        self.assertEqual(response.data['id'], str(self.version.pk))
        self.assertEqual(response.data['version'], '1.0')

    def test_detail_published_by_name(self):
        response = self.client.get(self._url(self.version.pk))
        self.assertEqual(response.data['published_by_name'], 'Alice Smith')

    def test_detail_not_found_returns_404(self):
        missing = uuid.uuid4()
        response = self.client.get(self._url(missing))
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_detail_previous_version_link(self):
        v2 = _make_version(self.group, version='2.0', previous_version=self.version)
        response = self.client.get(self._url(v2.pk))
        self.assertEqual(str(response.data['previous_version']), str(self.version.pk))


class TestChecklistVersionExportAPI(FastTenantTestCase):

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.user = UserF.create()
        self.group = TaxonGroupF.create(name='Fish')
        self.version = _make_version(self.group, version='1.0')
        self.url = reverse('checklist-version-export', args=[self.version.pk])

    def test_export_uses_existing_download_request_and_queues_task(self):
        self.client.force_login(self.user)
        download_request = DownloadRequest.objects.create(
            requester=self.user,
            resource_type=DownloadRequest.ZIP,
            resource_name='Checklist ZIP',
            approved=True,
            processing=True,
        )

        with patch('bims.tasks.coldp_export.export_coldp_zip.delay') as delay_mock:
            response = self.client.post(
                self.url,
                {'download_request_id': download_request.id},
            )

        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        self.assertEqual(response.data['download_request_id'], download_request.id)
        self.assertIn(f'/api/download-request/{download_request.id}/progress/', response.data['status_url'])
        self.assertIn(f'/api/download-request/{download_request.id}/file/', response.data['download_url'])
        download_request.refresh_from_db()
        self.assertTrue(download_request.processing)
        self.assertEqual(download_request.resource_type, DownloadRequest.ZIP)
        self.assertEqual(
            download_request.request_category,
            f'{self.group.name} {self.version.version}'
        )
        delay_mock.assert_called_once_with(download_request.id, str(self.version.pk))


class TestDownloadRequestZipFileApi(FastTenantTestCase):

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.user = UserF.create()
        self.media_root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.media_root, ignore_errors=True)
        super().tearDown()

    def test_zip_request_file_is_streamed_directly(self):
        with override_settings(MEDIA_ROOT=self.media_root):
            self.client.force_login(self.user)
            download_request = DownloadRequest.objects.create(
                requester=self.user,
                resource_type=DownloadRequest.ZIP,
                resource_name='Checklist ZIP',
                approved=True,
                processing=False,
                request_category='checklist_export.zip',
            )
            download_request.request_file.save(
                'checklist_export.zip',
                ContentFile(b'PK\x03\x04dummy zip'),
                save=True,
            )

            response = self.client.get(
                reverse('download-request-file', args=[download_request.id])
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response['Content-Type'], 'application/zip')
        self.assertIn(
            'attachment; filename="checklist_export.zip"',
            response['Content-Disposition']
        )


class TestChecklistVersionPublishAPI(FastTenantTestCase):

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.expert = UserF.create()
        self.other_user = UserF.create()
        self.group = TaxonGroupF.create(name='Fish')
        self.group.experts.add(self.expert)
        self.version = _make_version(
            self.group,
            version='1.0-draft',
            status=ChecklistVersion.STATUS_DRAFT,
        )
        self.url = reverse('checklist-version-publish', args=[self.version.pk])

    def test_group_expert_can_publish_draft(self):
        self.client.force_login(self.expert)
        response = self.client.post(self.url)
        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        self.version.refresh_from_db()
        self.assertEqual(self.version.status, ChecklistVersion.STATUS_PUBLISHED)

    def test_non_expert_cannot_publish_draft(self):
        self.client.force_login(self.other_user)
        response = self.client.post(self.url)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)


class TestChecklistVersionDeleteAPI(FastTenantTestCase):

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.expert = UserF.create()
        self.other_user = UserF.create()
        self.group = TaxonGroupF.create(name='Fish')
        self.group.experts.add(self.expert)
        self.version = _make_version(
            self.group,
            version='1.0',
            status=ChecklistVersion.STATUS_PUBLISHED,
        )
        self.url = reverse('checklist-version-delete', args=[self.version.pk])

    def test_group_expert_can_queue_delete_for_published_version(self):
        self.client.force_login(self.expert)
        with patch('bims.tasks.checklist.delete_published_checklist_version_task.delay') as delay_mock:
            response = self.client.post(self.url)
        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        delay_mock.assert_called_once()

    def test_non_expert_cannot_queue_delete(self):
        self.client.force_login(self.other_user)
        response = self.client.post(self.url)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
