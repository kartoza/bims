# coding=utf-8
"""Tests for source reference."""
import logging

from django.test import TestCase
from django.urls import reverse
from django.db.models.signals import post_save
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient
from rest_framework import status
from rest_framework.test import APIClient

from bims.models import BiologicalCollectionRecord
from bims.models.bims_document import BimsDocument, BimsDocumentAuthorship
from bims.models.source_reference import (
    SourceReference,
    SourceReferenceAuthor,
    merge_source_references, SourceReferenceDatabase,
    SourceReferenceDocument, source_reference_post_save_handler,
    SourceReferenceBibliography
)
from bims.tests.model_factories import (
    SourceReferenceF,
    SourceReferenceBibliographyF,
    SourceReferenceDatabaseF,
    SourceReferenceDocumentF,
    DatabaseRecordF,
    BiologicalCollectionRecordF, UserF,
    ChemicalRecordF,
    LocationSite,
    DocumentF,
)
from bims.models.location_site import location_site_post_save_handler
from bims.factories import AuthorFactory
from geonode.documents.models import Document
from bims.models.chemical_record import ChemicalRecord
from td_biblio.models.bibliography import AuthorEntryRank
from td_biblio.tests.model_factories import (
    AuthorF,
    JournalF,
    EntryF
)

logger = logging.getLogger('bims')


class TestSourceReferences(FastTenantTestCase):
    """ Tests CURD Profile.
    """

    def setUp(self):
        """
        Sets up before each test
        """

        # setup bibliography
        self.journal_title = 'test title'
        self.entry = EntryF.create(
            pk=1,
            title=self.journal_title,
            journal=JournalF.create(
                pk=1,
                name='journal'
            )
        )

        # setup database record
        self.db_name = 'test db'
        self.db_record = DatabaseRecordF(name=self.db_name)

        # setup biological record
        self.record = BiologicalCollectionRecordF()
        self.client = TenantClient(self.tenant)

    def test_source_reference_create(self):
        """
        Tests Source references create
        """
        source = SourceReferenceF(note='test')
        self.assertIsNotNone(source.pk)
        self.assertIsNone(source.get_source_unicode())

        # assign into record
        self.record.source_reference = source
        self.record.save()

        self.assertIsNotNone(self.record.source_reference)
        self.assertIsNone(self.record.source_reference.get_source_unicode())

    def test_source_reference_bibilography(self):
        """
        Tests Source references bibliography create
        """
        source = SourceReferenceBibliographyF(note='test', source=self.entry)
        source_reference = SourceReference.objects.last()
        self.assertEqual(
            source_reference.__class__.__name__,
            source.__class__.__name__
        )
        self.assertEqual(
            source_reference.source.title,
            self.entry.title
        )

        # assign into record
        self.record.source_reference = source
        self.record.save()
        self.assertEqual(
            self.record.source_reference.__class__.__name__,
            source.__class__.__name__
        )
        self.assertEqual(
            self.record.source_reference.source.title,
            self.entry.title
        )

    def test_source_reference_database(self):
        """
        Tests Source references database create
        """
        source = SourceReferenceDatabaseF(
            note='test', source=self.db_record)
        source_reference = SourceReference.objects.last()
        self.assertEqual(
            source_reference.__class__.__name__,
            source.__class__.__name__
        )
        self.assertEqual(
            source_reference.source.name,
            self.db_name
        )

        # assign into record
        self.record.source_reference = source
        self.record.save()
        self.assertEqual(
            self.record.source_reference.__class__.__name__,
            source.__class__.__name__
        )
        self.assertEqual(
            self.record.source_reference.source.name,
            self.db_name
        )

    def test_source_reference_unpublished(self):
        """
        Tests Source reference unpublished data
        """

        user = UserF.create(
            id=1
        )
        self.client.login(
            username=user.username,
            password='password'
        )
        result = self.client.post(
            '/source-reference/unpublished/',
            {"note": "test", "source": "test"},
            follow=True
        )

        self.assertEqual(result.status_code, 200)
        self.assertIsNotNone(
            SourceReference.objects.filter(note='test', source_name='test'))

    def test_merge_source_reference(self):
        """
        Tests merge source reference
        """

        source_1 = SourceReferenceF(note='test')
        source_2 = SourceReferenceF(note='test')

        self.record.source_reference = source_1
        self.record.save()

        record = BiologicalCollectionRecordF()
        record.source_reference = source_2
        record.save()

        merge_source_references(
            primary_source_reference=source_1,
            source_reference_list=SourceReference.objects.filter(note='test'))

        self.assertTrue(
            BiologicalCollectionRecord.objects.filter(
                source_reference=source_1
            ).count(),
            2
        )

        biblio = SourceReferenceBibliographyF.create(
            note='test', source=self.entry)
        database = SourceReferenceDatabaseF.create()
        document = SourceReferenceDocumentF.create()
        unpublished = SourceReferenceF.create(note='unpublished')
        source_document_id = document.source.id

        BiologicalCollectionRecordF.create(
            source_reference=biblio
        )
        BiologicalCollectionRecordF.create(
            source_reference=database
        )
        BiologicalCollectionRecordF.create(
            source_reference=document
        )
        BiologicalCollectionRecordF.create(
            source_reference=unpublished
        )
        self.assertTrue(
            Document.objects.filter(id=source_document_id).exists()
        )
        self.assertEqual(
            BiologicalCollectionRecord.objects.filter(
                source_reference=document
            ).count(),
            1
        )
        source_references = SourceReference.objects.filter(
            id__in=[database.id, document.id, unpublished.id]
        )
        merge_source_references(
            primary_source_reference=biblio,
            source_reference_list=source_references
        )
        self.assertEqual(
            BiologicalCollectionRecord.objects.filter(
                source_reference=biblio
            ).count(),
            4
        )
        self.assertFalse(
            BiologicalCollectionRecord.objects.filter(
                source_reference=database
            ).exists()
        )
        self.assertFalse(
            Document.objects.filter(id=source_document_id).exists()
        )
        self.assertFalse(
            SourceReferenceDatabase.objects.filter(
                id=database.id
            ).exists()
        )
        self.assertFalse(
            SourceReferenceDocument.objects.filter(
                id=document.id
            ).exists()
        )
        self.assertFalse(
            SourceReference.objects.filter(
                id=unpublished.id
            ).exists()
        )


class TestRemoveRecordsBySourceReference(FastTenantTestCase):

    def setUp(self):
        post_save.disconnect(receiver=location_site_post_save_handler, sender=LocationSite)
        post_save.disconnect(receiver=source_reference_post_save_handler, sender=SourceReferenceBibliography)

        self.superuser = UserF.create(
            is_superuser=True,
            password='password'
        )
        self.user = UserF.create(password='password')

        self.client = TenantClient(self.tenant)

        self.source_reference = SourceReferenceBibliographyF.create(
            source_name="Test Reference")
        self.bio_record = BiologicalCollectionRecordF.create(
            source_reference=self.source_reference
        )
        BiologicalCollectionRecordF.create(
            source_reference=self.source_reference
        )
        BiologicalCollectionRecordF.create(
            source_reference=self.source_reference
        )
        BiologicalCollectionRecordF.create(
            source_reference=self.source_reference
        )
        BiologicalCollectionRecordF.create(
            source_reference=self.source_reference
        )
        self.chem_record = ChemicalRecordF.create(
            source_reference=self.source_reference
        )

        self.url = reverse(
            'delete-records-by-source-reference-id',
            kwargs={'source_reference_id': self.source_reference.pk})

    def tearDown(self):
        post_save.connect(receiver=location_site_post_save_handler, sender=LocationSite)
        post_save.connect(receiver=source_reference_post_save_handler, sender=SourceReferenceBibliography)

    def test_superuser_access(self):
        self.client.login(username=self.user.username, password='password')
        response = self.client.post(self.url)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

        self.client.login(username=self.superuser.username, password='password')
        response = self.client.post(self.url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_delete_functionality(self):
        self.client.login(username=self.superuser.username, password='password')
        response = self.client.post(self.url)
        self.assertFalse(BiologicalCollectionRecord.objects.filter(source_reference=self.source_reference).exists())
        self.assertFalse(ChemicalRecord.objects.filter(source_reference=self.source_reference).exists())

    def test_no_records_found(self):
        source = SourceReferenceBibliography.objects.create(source_name="Another Reference")
        another_url = reverse('delete-records-by-source-reference-id',
                              kwargs={'source_reference_id': source.id})
        self.client.login(username=self.superuser.username, password='password')
        response = self.client.post(another_url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('No BiologicalCollectionRecord found for the given reference ID.', response.data['message'])
        self.assertIn('No ChemicalRecord found for the given reference ID.', response.data['message'])

    def test_missing_or_invalid_id(self):
        # Test the response when source_reference_id is missing or invalid
        self.client.login(username=self.superuser.username, password='password')
        invalid_url = reverse('delete-records-by-source-reference-id', kwargs={'source_reference_id': 0})
        response = self.client.post(invalid_url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)


class TestSourceReferenceListCollectorsFilter(FastTenantTestCase):
    """Tests for the collectors filter on the source reference list view."""

    def setUp(self):
        from bims.models.source_reference import disconnect_source_reference_signals
        disconnect_source_reference_signals()
        self.client = TenantClient(self.tenant)
        self.user = UserF.create(password='password')
        self.other_user = UserF.create(password='password')
        self.url = '/source-references/'

    def tearDown(self):
        from bims.models.source_reference import reconnect_source_reference_signals
        reconnect_source_reference_signals()

    def _get_ids(self, response):
        return {obj.id for obj in response.context['object_list']}

    def _make_author_for_user(self, user):
        """Create an Author linked to user, bypassing _set_user() override."""
        from td_biblio.models.bibliography import Author
        author = AuthorF.create()
        # _set_user() in save() overrides the user field based on name lookup,
        # so use update() to bypass save() and force the correct user.
        Author.objects.filter(pk=author.pk).update(user=user)
        author.refresh_from_db()
        return author

    def test_collectors_filter_bibliography(self):
        """Bibliography reference whose Entry author matches user is returned."""
        author = self._make_author_for_user(self.user)
        entry = EntryF.create()
        AuthorEntryRank.objects.create(author=author, entry=entry, rank=0)
        ref = SourceReferenceBibliographyF.create(source=entry)

        # Unrelated bibliography (no matching author)
        SourceReferenceBibliographyF.create()

        response = self.client.get(self.url, {'collectors': self.user.id})
        self.assertEqual(response.status_code, 200)
        ids = self._get_ids(response)
        self.assertIn(ref.id, ids)

    def test_collectors_filter_document(self):
        """Document reference whose BimsDocument author matches user is returned."""
        # Give document a non-null owner so BimsDocument.save() doesn't fail
        document = DocumentF.create(owner=self.user)
        bims_doc = BimsDocument.objects.create(document=document)
        # BimsDocument.save() auto-adds document.owner (self.user) as author
        ref = SourceReferenceDocumentF.create(source=document)

        # Unrelated document reference
        other_doc = DocumentF.create(owner=self.other_user)
        BimsDocument.objects.create(document=other_doc)
        SourceReferenceDocumentF.create(source=other_doc)

        response = self.client.get(self.url, {'collectors': self.user.id})
        self.assertEqual(response.status_code, 200)
        ids = self._get_ids(response)
        self.assertIn(ref.id, ids)

    def test_collectors_filter_unpublished(self):
        """Unpublished reference with a SourceReferenceAuthor matching user is returned."""
        author = self._make_author_for_user(self.user)
        ref = SourceReferenceF.create(note='unpublished ref')
        SourceReferenceAuthor.objects.create(
            source_reference=ref, author=author, order=0
        )

        # Unrelated unpublished reference (no author)
        SourceReferenceF.create(note='other ref')

        response = self.client.get(self.url, {'collectors': self.user.id})
        self.assertEqual(response.status_code, 200)
        ids = self._get_ids(response)
        self.assertIn(ref.id, ids)

    def test_collectors_filter_database(self):
        """Database reference with a SourceReferenceAuthor matching user is returned."""
        author = self._make_author_for_user(self.user)
        ref = SourceReferenceDatabaseF.create()
        SourceReferenceAuthor.objects.create(
            source_reference=ref, author=author, order=0
        )

        # Unrelated database reference (no author)
        SourceReferenceDatabaseF.create()

        response = self.client.get(self.url, {'collectors': self.user.id})
        self.assertEqual(response.status_code, 200)
        ids = self._get_ids(response)
        self.assertIn(ref.id, ids)

    def test_collectors_filter_excludes_other_users(self):
        """References authored only by other_user are not returned for user."""
        author = self._make_author_for_user(self.other_user)
        ref = SourceReferenceF.create(note='other user ref')
        SourceReferenceAuthor.objects.create(
            source_reference=ref, author=author, order=0
        )

        response = self.client.get(self.url, {'collectors': self.user.id})
        self.assertEqual(response.status_code, 200)
        ids = self._get_ids(response)
        self.assertNotIn(ref.id, ids)

    def test_collectors_filter_multiple_users(self):
        """Passing multiple user IDs returns references for any of them."""
        author1 = self._make_author_for_user(self.user)
        author2 = self._make_author_for_user(self.other_user)

        ref1 = SourceReferenceF.create(note='ref user 1')
        SourceReferenceAuthor.objects.create(
            source_reference=ref1, author=author1, order=0
        )
        ref2 = SourceReferenceF.create(note='ref user 2')
        SourceReferenceAuthor.objects.create(
            source_reference=ref2, author=author2, order=0
        )

        response = self.client.get(
            self.url,
            {'collectors': f'{self.user.id},{self.other_user.id}'}
        )
        self.assertEqual(response.status_code, 200)
        ids = self._get_ids(response)
        self.assertIn(ref1.id, ids)
        self.assertIn(ref2.id, ids)

    def test_no_collectors_filter_returns_all(self):
        """Without collectors param, all source references are returned."""
        ref1 = SourceReferenceF.create(note='ref a')
        ref2 = SourceReferenceDatabaseF.create()

        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        ids = self._get_ids(response)
        self.assertIn(ref1.id, ids)
        self.assertIn(ref2.id, ids)

