# coding=utf-8

from django.test import TestCase, Client
from allauth.utils import get_user_model
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient
from bims.signals.utils import disconnect_bims_signals, connect_bims_signals

from bims.factories import (
    EntryFactory,
    AuthorEntryRankFactory
)
from bims.models import (
    SourceReference,
    BiologicalCollectionRecord
)
from bims.tests.model_factories import (
    SourceReferenceBibliographyF,
    SourceReferenceF,
    BiologicalCollectionRecordF
)


class TestEditReference(FastTenantTestCase):
    """ Tests Edit reference view
    """

    def setUp(self):
        """
        Sets up before each test
        """
        disconnect_bims_signals()
        user = get_user_model().objects.create(
            is_staff=True,
            is_active=True,
            is_superuser=True,
            username='@.test')
        user.set_password('psst')
        user.save()
        non_staff_user = get_user_model().objects.create(
            is_staff=False,
            is_active=True,
            is_superuser=False,
            username='@.test2')
        non_staff_user.set_password('psst')
        non_staff_user.save()
        self.client = TenantClient(self.tenant)
        entry = EntryFactory.create(
            title='Test'
        )
        self.source_reference = SourceReferenceBibliographyF.create(
            source=entry
        )

    def tearDown(self):
        connect_bims_signals()

    def test_staff_open_edit_reference_page(self):
        """
        Test edit reference form get method
        """
        # Login
        resp = self.client.login(
            username='@.test',
            password='psst'
        )
        self.assertTrue(resp)
        response = self.client.get(
            '/edit-source-reference/{}/'.format(
                self.source_reference.id
            )
        )
        self.assertEqual(
            self.source_reference.source.title,
            response.context_data['object'].title
        )

    def test_nonstaff_open_edit_reference_page(self):
        resp = self.client.login(
            username='@.test2',
            password='psst'
        )
        self.assertTrue(resp)
        response = self.client.get(
            '/edit-source-reference/{}/'.format(
                self.source_reference.id
            )
        )
        self.assertEqual(
            response.status_code,
            403
        )

    def test_unpublished_data(self):
        self.client.login(
            username='@.test',
            password='psst'
        )
        source_reference_1 = SourceReferenceF.create(
            note='test',
        )
        BiologicalCollectionRecordF.create(
            source_reference=source_reference_1
        )
        BiologicalCollectionRecordF.create(
            source_reference=source_reference_1
        )
        source_reference_2 = SourceReferenceF.create(
            note='test2'
        )
        BiologicalCollectionRecordF.create(
            source_reference=source_reference_2
        )
        source_reference_3 = SourceReferenceF.create(
            note='test2'
        )
        BiologicalCollectionRecordF.create(
            source_reference=source_reference_3
        )
        # Update first source reference
        post_dict = {
            'title': 'updated test'
        }
        self.client.post(
            '/edit-source-reference/{}/'.format(
                source_reference_1.id
            ),
            post_dict
        )
        updated_reference = SourceReference.objects.get(
            id=source_reference_1.id
        )
        self.assertEqual(updated_reference.note, post_dict['title'])
        self.assertEqual(BiologicalCollectionRecord.objects.filter(
            source_reference=updated_reference
        ).count(), 2)

        # Merge source reference 1 with source reference 2
        post_dict['title'] = 'test2'
        self.client.post(
            '/edit-source-reference/{}/'.format(
                source_reference_1.id
            ),
            post_dict
        )
        updated_reference = SourceReference.objects.filter(
            note='test2'
        )[0]
        self.assertEqual(updated_reference.note, post_dict['title'])
        self.assertEqual(BiologicalCollectionRecord.objects.filter(
            source_reference=updated_reference
        ).count(), 4)


    def test_edit_bibliography(self):
        self.client.login(
            username='@.test',
            password='psst'
        )
        first_author = AuthorEntryRankFactory.create(
            entry=self.source_reference.source,
            rank=1
        )
        second_author = AuthorEntryRankFactory.create(
            entry=self.source_reference.source,
            rank=2
        )
        post_dict = {
            'title': 'updated bibliography',
            'year': 2000,
            # Switch authors order
            'author_id_1': (
                second_author.author.user.id
            ),
            'author_id_2': (
                first_author.author.user.id
            ),
            # change journal name
            'source': 'new journal name'
        }
        response = self.client.post(
            '/edit-source-reference/{}/'.format(
                self.source_reference.id
            ),
            post_dict
        )
        updated_reference = SourceReference.objects.get(
            id=self.source_reference.id
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(updated_reference.title, post_dict['title'])
        self.assertEqual(updated_reference.year, post_dict['year'])
        self.assertEqual(
            updated_reference.source.first_author, second_author.author)
        self.assertEqual(
            updated_reference.source.last_author, first_author.author)
        self.assertEqual(
            updated_reference.source.journal.name,
            post_dict['source']
        )

    def _post_gbif_csv(self, csv_text, **extra):
        from django.core.files.uploadedfile import SimpleUploadedFile
        self.client.login(username='@.test', password='psst')
        data = {
            'title': 'updated bibliography',
            'year': 2000,
            'source': 'new journal name',
            'gbif_metadata_csv': SimpleUploadedFile(
                'meta.csv', csv_text.encode('utf-8'), content_type='text/csv'),
        }
        data.update(extra)
        return self.client.post(
            '/edit-source-reference/{}/'.format(self.source_reference.id),
            data
        )

    def test_upload_gbif_metadata_csv(self):
        from bims.utils.gbif_publish import load_publish_metadata
        response = self._post_gbif_csv(
            'project_identifier,title,description,license\n'
            'BID-REG2025-094,GBIF title,GBIF description,CC BY 4.0\n')
        self.assertEqual(response.status_code, 302)
        source_reference = SourceReference.objects.get(
            id=self.source_reference.id)
        try:
            metadata = load_publish_metadata(source_reference)
            self.assertEqual(metadata['project_identifier'], 'BID-REG2025-094')
            self.assertEqual(metadata['title'], 'GBIF title')
        finally:
            source_reference.gbif_metadata_file.delete()

    def test_invalid_gbif_metadata_csv_changes_nothing(self):
        response = self._post_gbif_csv(
            'id,title,description\n999999,GBIF title,GBIF description\n')
        self.assertEqual(response.status_code, 200)
        self.assertIn(
            'GBIF metadata CSV not saved', response.content.decode())
        self.source_reference.refresh_from_db()
        self.assertFalse(self.source_reference.gbif_metadata_file)
        self.assertNotEqual(self.source_reference.title, 'updated bibliography')

    def test_remove_gbif_metadata(self):
        from django.core.files.base import ContentFile
        self.source_reference.gbif_metadata_file.save(
            'meta.csv', ContentFile(b'title,description\nT,D\n'))
        self.client.login(username='@.test', password='psst')
        response = self.client.post(
            '/edit-source-reference/{}/'.format(self.source_reference.id),
            {'title': 'updated bibliography', 'year': 2000,
             'source': 'new journal name', 'remove_gbif_metadata': '1'})
        self.assertEqual(response.status_code, 302)
        self.assertFalse(SourceReference.objects.get(
            id=self.source_reference.id).gbif_metadata_file)
