import csv
import io
import json
import os
import shutil
import tempfile
import zipfile as zf
from unittest import mock

from django.db import connection
from django.test import SimpleTestCase, override_settings
from django.utils import timezone
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.utils import schema_context, get_public_schema_name

from django_celery_beat.models import PeriodicTask

from td_biblio.models.bibliography import Author

from bims.models.gbif_publish import (
    GbifPublish,
    GbifPublishConfig,
    GbifPublishContact,
    GbifPublishSession,
    PublishPeriod,
    PublishStatus,
    RoleType,
)
from bims.models.licence import Licence
from bims.models.source_reference import SourceReferenceAuthor
from bims.tasks.gbif_publish import run_scheduled_gbif_publish
from bims.tests.model_factories import (
    SourceReferenceF,
    SourceReferenceBibliographyF,
    SourceReferenceDatabaseF,
    SourceReferenceDocumentF,
    BiologicalCollectionRecordF,
    RecordTypeF,
    SurveyF,
    UserF,
)
from bims.utils.gbif_publish import (
    build_dwca,
    eml_citation,
    eml_author,
    eml_contact_from_model,
    intellectual_rights_text,
    register_dataset,
    publish_gbif_data_with_config,
    LICENSE_URL,
)


# ---------------------------------------------------------------------------
# Shared config helper
# ---------------------------------------------------------------------------

def _make_config(**kwargs):
    defaults = {
        "name": "Test",
        "gbif_api_url": "https://api.gbif-test.org/v1",
        "export_base_url": "https://example.org",
        "username": "user",
        "password": "pass",
        "publishing_org_key": "org-key",
        "installation_key": "inst-key",
        "is_active": True,
    }
    defaults.update(kwargs)
    return GbifPublishConfig.objects.create(**defaults)


def _make_contact(config, **kwargs):
    defaults = {
        "individual_name_given": "Alice",
        "individual_name_sur": "Researcher",
        "organization_name": "Test Org",
        "electronic_mail_address": "alice@example.com",
    }
    defaults.update(kwargs)
    return GbifPublishContact.objects.create(gbif_config=config, **defaults)


# ---------------------------------------------------------------------------
# Model signal tests
# ---------------------------------------------------------------------------

class GbifPublishModelSignalTests(FastTenantTestCase):

    def test_sync_gbif_publish_periodic_task_daily(self):
        source_reference = SourceReferenceF.create()
        config = _make_config()
        schedule = GbifPublish.objects.create(
            source_reference=source_reference,
            gbif_config=config,
            enabled=True,
            period=PublishPeriod.DAILY,
            run_at=timezone.datetime(2024, 1, 1, 2, 30).time(),
            timezone="UTC",
        )

        with schema_context(get_public_schema_name()):
            task = PeriodicTask.objects.get(
                name=f"GBIF publish: source_reference={source_reference.id} id={schedule.id}"
            )
            self.assertEqual(task.task, "bims.tasks.gbif_publish.run_scheduled_gbif_publish")
            self.assertEqual(json.loads(task.args), [self.tenant.schema_name, schedule.id])
            self.assertTrue(task.enabled)
            self.assertEqual(task.queue, "update")
            self.assertEqual(task.crontab.minute, "30")
            self.assertEqual(task.crontab.hour, "2")

    def test_sync_gbif_publish_periodic_task_custom_cron(self):
        source_reference = SourceReferenceF.create()
        config = _make_config()
        schedule = GbifPublish.objects.create(
            source_reference=source_reference,
            gbif_config=config,
            enabled=False,
            period=PublishPeriod.CUSTOM,
            cron_expression="15 */6 * * *",
            timezone="UTC",
        )

        with schema_context(get_public_schema_name()):
            task = PeriodicTask.objects.get(
                name=f"GBIF publish: source_reference={source_reference.id} id={schedule.id}"
            )
            self.assertFalse(task.enabled)
            self.assertEqual(task.crontab.minute, "15")
            self.assertEqual(task.crontab.hour, "*/6")
            self.assertEqual(task.crontab.day_of_month, "*")
            self.assertEqual(task.crontab.day_of_week, "*")

    def _make_schedule(self, source_reference=None, **kwargs):
        config = _make_config()
        defaults = dict(
            gbif_config=config,
            enabled=False,
            period=PublishPeriod.DAILY,
            run_at=timezone.datetime(2024, 1, 1, 2, 0).time(),
            timezone="UTC",
        )
        defaults.update(kwargs)
        if source_reference is None:
            source_reference = SourceReferenceF.create()
        return GbifPublish.objects.create(source_reference=source_reference, **defaults)

    # ------------------------------------------------------------------
    # seed_contacts_from_source_reference_authors
    # ------------------------------------------------------------------

    def test_authors_seeded_as_schedule_contacts_on_create(self):
        """Authors of the source reference become schedule-level contacts."""
        source_reference = SourceReferenceF.create()
        user = UserF.create(first_name="Alice", last_name="Smith", email="alice@example.com")
        author = Author.objects.create(first_name="Alice", last_name="Smith", user=user)
        SourceReferenceAuthor.objects.create(
            source_reference=source_reference, author=author, order=0
        )

        schedule = self._make_schedule(source_reference=source_reference)

        contacts = list(
            GbifPublishContact.objects.filter(gbif_publish=schedule).order_by("id")
        )
        self.assertEqual(len(contacts), 1)
        c = contacts[0]
        self.assertEqual(c.individual_name_given, "Alice")
        self.assertEqual(c.individual_name_sur, "Smith")
        self.assertEqual(c.user, user)
        self.assertIsNone(c.gbif_config)

    def test_multiple_authors_seeded_in_order(self):
        """Multiple authors are seeded in order and all linked to the schedule."""
        source_reference = SourceReferenceF.create()
        authors_data = [
            ("First", "Author"),
            ("Second", "Writer"),
            ("Third", "Person"),
        ]
        for i, (first, last) in enumerate(authors_data):
            author = Author.objects.create(first_name=first, last_name=last)
            SourceReferenceAuthor.objects.create(
                source_reference=source_reference, author=author, order=i
            )

        schedule = self._make_schedule(source_reference=source_reference)

        contacts = list(
            GbifPublishContact.objects.filter(gbif_publish=schedule).order_by("id")
        )
        self.assertEqual(len(contacts), 3)
        self.assertEqual(contacts[0].individual_name_given, "First")
        self.assertEqual(contacts[1].individual_name_given, "Second")
        self.assertEqual(contacts[2].individual_name_given, "Third")

    def test_author_without_user_seeded_with_name_only(self):
        """An Author with no linked user still produces a contact with name fields."""
        source_reference = SourceReferenceF.create()
        author = Author.objects.create(first_name="NoLink", last_name="Person")
        # Author._set_user() auto-creates a user on save; bypass it via direct update.
        Author.objects.filter(pk=author.pk).update(user=None)
        author.refresh_from_db()
        SourceReferenceAuthor.objects.create(
            source_reference=source_reference, author=author, order=0
        )

        schedule = self._make_schedule(source_reference=source_reference)

        contacts = list(GbifPublishContact.objects.filter(gbif_publish=schedule))
        self.assertEqual(len(contacts), 1)
        self.assertEqual(contacts[0].individual_name_given, "NoLink")
        self.assertEqual(contacts[0].individual_name_sur, "Person")
        self.assertIsNone(contacts[0].user)

    def test_no_contacts_seeded_when_no_source_reference(self):
        """Schedules without a source reference get no seeded contacts."""
        config = _make_config()
        schedule = GbifPublish.objects.create(
            source_reference=None,
            gbif_config=config,
            enabled=False,
            period=PublishPeriod.DAILY,
            timezone="UTC",
        )
        self.assertFalse(GbifPublishContact.objects.filter(gbif_publish=schedule).exists())

    def test_no_contacts_seeded_when_source_reference_has_no_authors(self):
        """A source reference with no authors produces no schedule contacts."""
        source_reference = SourceReferenceF.create()
        schedule = self._make_schedule(source_reference=source_reference)
        self.assertFalse(GbifPublishContact.objects.filter(gbif_publish=schedule).exists())

    def test_contacts_not_reseeded_on_subsequent_save(self):
        """Saving an existing schedule a second time must not duplicate contacts."""
        source_reference = SourceReferenceF.create()
        author = Author.objects.create(first_name="Once", last_name="Only")
        SourceReferenceAuthor.objects.create(
            source_reference=source_reference, author=author, order=0
        )

        schedule = self._make_schedule(source_reference=source_reference)
        self.assertEqual(
            GbifPublishContact.objects.filter(gbif_publish=schedule).count(), 1
        )

        # Save again — must not add a second copy
        schedule.enabled = True
        schedule.save()
        self.assertEqual(
            GbifPublishContact.objects.filter(gbif_publish=schedule).count(), 1
        )

    def test_gbif_publish_post_delete_removes_task(self):
        source_reference = SourceReferenceF.create()
        config = _make_config()
        schedule = GbifPublish.objects.create(
            source_reference=source_reference,
            gbif_config=config,
            enabled=True,
            period=PublishPeriod.DAILY,
            run_at=timezone.datetime(2024, 1, 1, 2, 0).time(),
            timezone="UTC",
        )

        with schema_context(get_public_schema_name()):
            self.assertTrue(
                PeriodicTask.objects.filter(
                    name=f"GBIF publish: source_reference={source_reference.id} id={schedule.id}"
                ).exists()
            )

        schedule.delete()

        with schema_context(get_public_schema_name()):
            self.assertFalse(
                PeriodicTask.objects.filter(
                    args=json.dumps([self.tenant.schema_name, schedule.id])
                ).exists()
            )


# ---------------------------------------------------------------------------
# Celery task tests
# ---------------------------------------------------------------------------

class GbifPublishTaskTests(FastTenantTestCase):

    def _create_schedule(self, **kwargs):
        source_reference = kwargs.pop("source_reference", SourceReferenceBibliographyF.create())
        config = kwargs.pop("gbif_config", _make_config())
        defaults = {
            "source_reference": source_reference,
            "gbif_config": config,
            "enabled": True,
            "period": PublishPeriod.DAILY,
            "run_at": timezone.datetime(2024, 1, 1, 2, 0).time(),
            "timezone": "UTC",
        }
        defaults.update(kwargs)
        return GbifPublish.objects.create(**defaults)

    def test_task_skips_when_locked(self):
        schedule = self._create_schedule()
        with mock.patch("bims.tasks.gbif_publish.pg_advisory_lock") as lock_mock:
            lock_mock.return_value.__enter__.return_value = False
            lock_mock.return_value.__exit__.return_value = None
            result = run_scheduled_gbif_publish(self.tenant.schema_name, schedule.id)

        self.assertEqual(result["status"], "skipped_locked")
        self.assertFalse(GbifPublishSession.objects.exists())

    def test_task_returns_disabled(self):
        schedule = self._create_schedule(enabled=False)
        with mock.patch("bims.tasks.gbif_publish.pg_advisory_lock") as lock_mock:
            lock_mock.return_value.__enter__.return_value = True
            lock_mock.return_value.__exit__.return_value = None
            result = run_scheduled_gbif_publish(self.tenant.schema_name, schedule.id)

        self.assertEqual(result["status"], "disabled")
        self.assertFalse(GbifPublishSession.objects.exists())

    def test_task_returns_excluded_when_publish_to_gbif_false(self):
        """Schedule is skipped when source_reference.publish_to_gbif is False."""
        source_reference = SourceReferenceBibliographyF.create()
        source_reference.publish_to_gbif = False
        source_reference.save()
        schedule = self._create_schedule(source_reference=source_reference)
        _make_contact(schedule.gbif_config)

        with mock.patch("bims.tasks.gbif_publish.pg_advisory_lock") as lock_mock:
            lock_mock.return_value.__enter__.return_value = True
            lock_mock.return_value.__exit__.return_value = None
            result = run_scheduled_gbif_publish(self.tenant.schema_name, schedule.id)

        self.assertEqual(result["status"], "excluded_from_gbif")
        self.assertFalse(GbifPublishSession.objects.exists())

    def test_task_returns_no_contacts_when_none_configured(self):
        schedule = self._create_schedule()
        # No contacts created for this config
        with mock.patch("bims.tasks.gbif_publish.pg_advisory_lock") as lock_mock:
            lock_mock.return_value.__enter__.return_value = True
            lock_mock.return_value.__exit__.return_value = None
            result = run_scheduled_gbif_publish(self.tenant.schema_name, schedule.id)

        self.assertEqual(result["status"], "no_contacts")
        self.assertFalse(GbifPublishSession.objects.exists())

    def test_task_success_updates_session_and_schedule(self):
        schedule = self._create_schedule()
        _make_contact(schedule.gbif_config)
        with mock.patch("bims.tasks.gbif_publish.pg_advisory_lock") as lock_mock:
            lock_mock.return_value.__enter__.return_value = True
            lock_mock.return_value.__exit__.return_value = None

            with mock.patch(
                "bims.tasks.gbif_publish.publish_gbif_data_with_config",
                return_value={
                    "dataset_key": "dataset-123",
                    "records_published": 3,
                    "archive_url": "https://example.org/media/dwca.zip",
                },
            ):
                result = run_scheduled_gbif_publish(self.tenant.schema_name, schedule.id)

        self.assertEqual(result["status"], "success")
        session = GbifPublishSession.objects.get(id=result["session_id"])
        self.assertEqual(session.status, PublishStatus.SUCCESS)
        self.assertEqual(session.dataset_key, "dataset-123")
        schedule.refresh_from_db()
        self.assertIsNotNone(schedule.last_publish)

    def test_task_no_records_marks_session(self):
        schedule = self._create_schedule()
        _make_contact(schedule.gbif_config)
        with mock.patch("bims.tasks.gbif_publish.pg_advisory_lock") as lock_mock:
            lock_mock.return_value.__enter__.return_value = True
            lock_mock.return_value.__exit__.return_value = None

            with mock.patch(
                "bims.tasks.gbif_publish.publish_gbif_data_with_config",
                side_effect=ValueError("No records to publish"),
            ):
                result = run_scheduled_gbif_publish(self.tenant.schema_name, schedule.id)

        self.assertEqual(result["status"], "no_records")
        session = GbifPublishSession.objects.get(id=result["session_id"])
        self.assertEqual(session.status, PublishStatus.NO_RECORDS)
        self.assertIn("No records to publish", session.error_message)

    def test_task_error_marks_session(self):
        schedule = self._create_schedule()
        _make_contact(schedule.gbif_config)
        with mock.patch("bims.tasks.gbif_publish.pg_advisory_lock") as lock_mock:
            lock_mock.return_value.__enter__.return_value = True
            lock_mock.return_value.__exit__.return_value = None

            with mock.patch(
                "bims.tasks.gbif_publish.publish_gbif_data_with_config",
                side_effect=RuntimeError("Boom"),
            ):
                result = run_scheduled_gbif_publish(self.tenant.schema_name, schedule.id)

        self.assertEqual(result["status"], "error")
        session = GbifPublishSession.objects.get(id=result["session_id"])
        self.assertEqual(session.status, PublishStatus.ERROR)
        self.assertIn("Boom", session.error_message)


# ---------------------------------------------------------------------------
# API / build_dwca tests
# ---------------------------------------------------------------------------

class GbifPublishApiTests(FastTenantTestCase):

    def setUp(self):
        self.config = _make_config(export_base_url="https://example.org")
        self.contact = _make_contact(self.config)

    def _make_record(self, source_reference, **kwargs):
        kwargs.setdefault("institution_id", "Test Institution")
        survey = SurveyF.create(validated=True)
        return BiologicalCollectionRecordF.create(
            survey=survey,
            source_reference=source_reference,
            data_type="public",
            **kwargs,
        )

    def _read_occurrence_rows(self, zip_path):
        with zf.ZipFile(zip_path) as z:
            content = z.read("occurrence.txt").decode("utf-8")
        reader = csv.DictReader(io.StringIO(content), delimiter="\t")
        return list(reader)

    def _read_eml(self, zip_path):
        with zf.ZipFile(zip_path) as z:
            return z.read("eml.xml").decode("utf-8")

    # -- register_dataset error handling --

    def test_gbif_config_password(self):
        config = GbifPublishConfig.objects.get(id=self.config.id)
        self.assertEqual(
            config.password,
            'pass'
        )

    def test_register_dataset_401(self):
        response = mock.Mock()
        response.status_code = 401
        response.raise_for_status = mock.Mock()
        response.json.return_value = ""

        with mock.patch("bims.utils.gbif_publish.requests.post", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "401 Unauthorized"):
                register_dataset(self.config, "title", "desc")

    def test_register_dataset_403(self):
        response = mock.Mock()
        response.status_code = 403
        response.raise_for_status = mock.Mock()
        response.json.return_value = ""

        with mock.patch("bims.utils.gbif_publish.requests.post", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "403 Forbidden"):
                register_dataset(self.config, "title", "desc")

    # -- build_dwca: archive URL --

    def test_build_dwca_uses_export_base_url(self):
        source_reference = SourceReferenceF.create()
        record = self._make_record(source_reference)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, archive_url, written_ids = build_dwca(
                self.config, [record], [self.contact], source_reference
            )

        self.assertTrue(os.path.exists(zip_path))
        self.assertIn(record.id, written_ids)
        self.assertTrue(archive_url.startswith("https://example.org/media/"))

    # -- EML title / description --

    def test_build_dwca_title_is_source_reference_title(self):
        source_reference = SourceReferenceF.create()
        source_reference.source_name = "My Reference Title"
        source_reference.save()
        record = self._make_record(source_reference)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(
                self.config, [record], [self.contact], source_reference
            )

        eml = self._read_eml(zip_path)
        self.assertIn("My Reference Title", eml)
        self.assertNotIn("UTC", eml.split("<title>")[1].split("</title>")[0])

    def test_build_dwca_description_format(self):
        """Description uses config.name as publisher, not SITE_NAME."""
        source_reference = SourceReferenceF.create()
        source_reference.source_name = "Fish Survey 2022"
        source_reference.save()
        record = self._make_record(source_reference)
        config = _make_config(name="Freshwater Research Centre")
        contact = _make_contact(config)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(
                config, [record], [contact], source_reference
            )

        eml = self._read_eml(zip_path)
        self.assertIn(
            "Occurrence dataset for Fish Survey 2022 uploaded to Freshwater Research Centre.",
            eml,
        )

    # -- occurrenceID tenant prefix --

    def test_occurrence_id_uses_tenant_name(self):
        source_reference = SourceReferenceF.create()
        record = self._make_record(source_reference)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, written_ids = build_dwca(
                self.config, [record], [self.contact], source_reference
            )

        rows = self._read_occurrence_rows(zip_path)
        self.assertTrue(len(rows) > 0)
        tenant = connection.schema_name
        for row in rows:
            self.assertTrue(
                row["occurrenceID"].startswith(f"{tenant}:"),
                f"Expected occurrenceID to start with '{tenant}:', got '{row['occurrenceID']}'",
            )

    def test_institution_id_takes_priority_over_user_organization(self):
        """institution_id on the record should override collector_user.organization."""
        source_reference = SourceReferenceF.create()
        user = UserF.create(first_name="Data", last_name="Owner")
        user.organization = "Freshwater Research Centre"
        user.save()
        record = self._make_record(
            source_reference,
            collector="",
            collector_user=user,
            owner=user,
            institution_id="South African Institute for Aquatic Biodiversity",
        )
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(
                self.config, [record], [self.contact], source_reference
            )

        rows = self._read_occurrence_rows(zip_path)
        self.assertEqual(
            rows[0]["institutionCode"],
            "South African Institute for Aquatic Biodiversity",
        )

    def test_institution_id_default_falls_back_to_user_organization(self):
        """When institution_id is the site default, fall back to collector_user.organization."""
        source_reference = SourceReferenceF.create()
        user = UserF.create(first_name="Data", last_name="Owner")
        user.organization = "Freshwater Research Centre"
        user.save()
        record = self._make_record(
            source_reference,
            collector="",
            collector_user=user,
            owner=user,
            institution_id="bims",  # INSTITUTION_ID_DEFAULT
        )
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(
            MEDIA_ROOT=temp_dir,
            MEDIA_URL="/media/",
            INSTITUTION_ID_DEFAULT="bims",
        ):
            zip_path, _, _ = build_dwca(
                self.config, [record], [self.contact], source_reference
            )

        rows = self._read_occurrence_rows(zip_path)
        self.assertEqual(rows[0]["institutionCode"], "Freshwater Research Centre")

    def test_owner_fallback_sets_recorded_by_and_institution_code(self):
        source_reference = SourceReferenceF.create()
        owner = UserF.create(first_name="River", last_name="Team")
        owner.organization = "Owner Org"
        owner.save()
        # institution_id is the default so owner.organization should be used
        record = self._make_record(
            source_reference,
            collector="",
            collector_user=None,
            owner=owner,
            institution_id="bims",
        )
        new_record = self._make_record(
            source_reference,
            collector="",
            collector_user=owner,
            owner=owner,
            institution_id="bims",
        )
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(
            MEDIA_ROOT=temp_dir,
            MEDIA_URL="/media/",
            INSTITUTION_ID_DEFAULT="bims",
        ):
            zip_path, _, _ = build_dwca(
                self.config, [record, new_record], [self.contact], source_reference
            )

        rows = self._read_occurrence_rows(zip_path)
        self.assertEqual(rows[0]["recordedBy"], "River Team")
        self.assertEqual(rows[0]["institutionCode"], "Owner Org")

        self.assertEqual(rows[1]["recordedBy"], "River Team")
        self.assertEqual(rows[1]["institutionCode"], "Owner Org")

    def test_owner_is_preferred_over_collector_user_for_recorded_by(self):
        source_reference = SourceReferenceF.create()
        owner = UserF.create(first_name="Owner", last_name="Person")
        collector_user = UserF.create(first_name="Collector", last_name="Person")
        owner.save()
        collector_user.save()
        record = self._make_record(
            source_reference,
            collector="",
            collector_user=collector_user,
            owner=owner,
            institution_id="LEGACY-INST-ID",
        )
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(
                self.config, [record], [self.contact], source_reference
            )

        rows = self._read_occurrence_rows(zip_path)
        self.assertEqual(rows[0]["recordedBy"], "Owner Person")

    def test_owner_is_preferred_over_collector_string_for_recorded_by(self):
        source_reference = SourceReferenceF.create()
        owner = UserF.create(first_name="Owner", last_name="Person")
        owner.save()
        record = self._make_record(
            source_reference,
            collector="Field Collector",
            collector_user=None,
            owner=owner,
            institution_id="LEGACY-INST-ID",
        )
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(
                self.config, [record], [self.contact], source_reference
            )

        rows = self._read_occurrence_rows(zip_path)
        self.assertEqual(rows[0]["recordedBy"], "Owner Person")

    def test_basis_of_record_mapping_for_requested_record_types(self):
        source_reference = SourceReferenceF.create()
        cases = [
            ("Acoustic record", "HUMAN_OBSERVATION"),
            ("DNA sample", "MATERIAL_SAMPLE"),
            ("Photographic record", "MACHINE_OBSERVATION"),
            ("Specimen collection", "MATERIAL_SAMPLE"),
            ("Visual observation", "HUMAN_OBSERVATION"),
        ]
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        for record_type_name, expected_basis in cases:
            record_type = RecordTypeF.create(name=record_type_name)
            record = self._make_record(source_reference, record_type=record_type)

            with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
                zip_path, _, _ = build_dwca(
                    self.config, [record], [self.contact], source_reference
                )

            rows = self._read_occurrence_rows(zip_path)
            self.assertEqual(
                rows[0]["basisOfRecord"],
                expected_basis,
                msg=f"Unexpected basisOfRecord for '{record_type_name}'",
            )

    # -- abundance type mapping --

    def _make_record_with_abundance(self, source_reference, abundance_name, abundance_number):
        from bims.models.abundance_type import AbundanceType
        abundance_type, _ = AbundanceType.objects.get_or_create(name=abundance_name)
        return self._make_record(
            source_reference,
            abundance_type=abundance_type,
            abundance_number=abundance_number,
        )

    def test_abundance_number_type(self):
        source_reference = SourceReferenceF.create()
        record = self._make_record_with_abundance(source_reference, "Number", 5)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(
                self.config, [record], [self.contact], source_reference
            )

        rows = self._read_occurrence_rows(zip_path)
        self.assertAlmostEqual(float(rows[0]["individualCount"]), 5.0)
        self.assertAlmostEqual(float(rows[0]["organismQuantity"]), 5.0)
        self.assertEqual(rows[0]["organismQuantityType"], "individuals")

    def test_abundance_percentage_type(self):
        source_reference = SourceReferenceF.create()
        record = self._make_record_with_abundance(source_reference, "Percentage", 30)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(
                self.config, [record], [self.contact], source_reference,
            )

        rows = self._read_occurrence_rows(zip_path)
        self.assertEqual(rows[0]["individualCount"], "")
        self.assertEqual(rows[0]["organismQuantityType"], "% cover")

    def test_abundance_density_m2_type(self):
        source_reference = SourceReferenceF.create()
        record = self._make_record_with_abundance(source_reference, "Density (m2)", 12)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(
                self.config, [record], [self.contact], source_reference,
            )

        rows = self._read_occurrence_rows(zip_path)
        self.assertEqual(rows[0]["individualCount"], "")
        self.assertEqual(rows[0]["organismQuantityType"], "individuals/m2")

    # -- intellectualRights --

    def test_intellectual_rights_default(self):
        text = intellectual_rights_text(None)
        self.assertIn("Creative Commons CCZero 1.0 License", text)
        self.assertIn(LICENSE_URL, text)
        self.assertTrue(text.startswith("This work is licensed under a "))

    def test_intellectual_rights_with_licence(self):
        licence = Licence(name="CC BY 4.0", url="https://creativecommons.org/licenses/by/4.0/legalcode")
        text = intellectual_rights_text(licence)
        self.assertEqual(
            text,
            "This work is licensed under a CC BY 4.0 https://creativecommons.org/licenses/by/4.0/legalcode.",
        )

    def test_eml_multiple_licences(self):
        source_reference = SourceReferenceF.create()
        lic1 = Licence.objects.create(name="CC BY 4.0", identifier="cc-by-4", url="https://creativecommons.org/licenses/by/4.0/legalcode")
        lic2 = Licence.objects.create(name="CC0 1.0", identifier="cc0-1", url="https://creativecommons.org/publicdomain/zero/1.0/legalcode")
        record1 = self._make_record(source_reference, licence=lic1)
        record2 = self._make_record(source_reference, licence=lic2)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(
                self.config, [record1, record2], [self.contact], source_reference
            )

        eml = self._read_eml(zip_path)
        self.assertIn("CC BY 4.0", eml)
        self.assertIn("CC0 1.0", eml)
        self.assertEqual(eml.count("<para>This work is licensed under"), 2)

    # -- eml_author: org and role --

    def test_eml_author_org_only_omits_individual_name(self):
        """User with no first/last name must not emit an empty <individualName> in <creator>."""
        user = UserF.create(first_name="", last_name="")
        user.organization = "Freshwater Research Centre"
        user.save()
        snippet = eml_author(user)
        self.assertNotIn("<individualName>", snippet)
        self.assertIn("<organizationName>Freshwater Research Centre</organizationName>", snippet)

    def test_eml_author_includes_organization_and_role(self):
        from bims.models.profile import Role, Profile as BimsProfile
        user = UserF.create(
            first_name="Jane",
            last_name="Smith",
            email="jane@example.com",
        )
        user.organization = "University of Cape Town"
        user.save()
        role, _ = Role.objects.get_or_create(
            name="researcher", defaults={"display_name": "Researcher"}
        )
        bims_profile, _ = BimsProfile.objects.get_or_create(user=user)
        bims_profile.role = role
        bims_profile.save()

        snippet = eml_author(user)
        self.assertIn("Jane", snippet)
        self.assertIn("Smith", snippet)
        self.assertIn("University of Cape Town", snippet)
        self.assertIn("<positionName>Researcher</positionName>", snippet)
        self.assertIn("jane@example.com", snippet)

    def test_eml_citation_bibliography(self):
        source_reference = SourceReferenceBibliographyF.create()
        source_reference.source.doi = "10.1038/ismej.2011.170"
        source_reference.source.save()

        result = eml_citation(source_reference)

        self.assertIn('identifier="10.1038/ismej.2011.170"', result)
        self.assertIn("doi:10.1038/ismej.2011.170", result)
        self.assertTrue(result.startswith("<citation"))
        self.assertTrue(result.endswith("</citation>"))

    def test_eml_citation_document(self):
        source_reference = SourceReferenceDocumentF.create()
        source_reference.source.doc_url = "https://example.org/doc.pdf"
        source_reference.source.save()

        result = eml_citation(source_reference)

        self.assertIn('identifier="https://example.org/doc.pdf"', result)
        self.assertTrue(result.startswith("<citation"))

    def test_eml_citation_plain_reference(self):
        source_reference = SourceReferenceF.create(source_name="Unpublished survey data")
        result = eml_citation(source_reference)
        self.assertTrue(result.startswith("<citation"))
        self.assertTrue(result.endswith("</citation>"))
        self.assertIn("Unpublished survey data", result)

    def test_eml_citation_database(self):
        from bims.models.source_reference import DatabaseRecord
        db_record = DatabaseRecord.objects.create(
            name="Freshwater Atlas", url="https://atlas.example.org"
        )
        source_reference = SourceReferenceDatabaseF.create(source=db_record)
        result = eml_citation(source_reference)
        self.assertTrue(result.startswith("<citation"))
        self.assertIn('identifier="https://atlas.example.org"', result)
        self.assertIn("Freshwater Atlas", result)

    def test_eml_citation_uses_originator_contacts_as_authors(self):
        source_reference = SourceReferenceF.create(source_name="Survey 2023")
        config = _make_config()
        originator = GbifPublishContact.objects.create(
            gbif_config=config,
            individual_name_given="Jane",
            individual_name_sur="Smith",
            role=RoleType.ORIGINATOR,
        )
        other = GbifPublishContact.objects.create(
            gbif_config=config,
            individual_name_given="Bob",
            individual_name_sur="Jones",
            role=RoleType.METADATA_PROVIDER,
        )
        result = eml_citation(source_reference, contacts=[originator, other])
        self.assertIn("Jane Smith", result)
        self.assertNotIn("Bob Jones", result)

    def test_eml_citation_falls_back_to_all_contacts_when_no_originators(self):
        source_reference = SourceReferenceF.create(source_name="Dataset")
        config = _make_config()
        c1 = GbifPublishContact.objects.create(
            gbif_config=config,
            individual_name_given="Alice",
            individual_name_sur="Brown",
            role=RoleType.METADATA_PROVIDER,
        )
        c2 = GbifPublishContact.objects.create(
            gbif_config=config,
            individual_name_given="Carol",
            individual_name_sur="White",
            role=RoleType.METADATA_PROVIDER,
        )
        result = eml_citation(source_reference, contacts=[c1, c2])
        self.assertIn("Alice Brown", result)
        self.assertIn("Carol White", result)

    def test_eml_citation_org_only_contact(self):
        source_reference = SourceReferenceF.create(source_name="Dataset")
        config = _make_config()
        contact = GbifPublishContact.objects.create(
            gbif_config=config,
            organization_name="Freshwater Research Centre",
            role=RoleType.ORIGINATOR,
        )
        result = eml_citation(source_reference, contacts=[contact])
        self.assertIn("Freshwater Research Centre", result)

    def test_eml_citation_org_preferred_over_individual_name(self):
        """When a contact has both org name and individual name, org name wins in the citation."""
        source_reference = SourceReferenceF.create(source_name="Dataset")
        config = _make_config()
        contact = GbifPublishContact.objects.create(
            gbif_config=config,
            individual_name_sur="Kartoza",
            organization_name="Kartoza Org",
            role=RoleType.ORIGINATOR,
        )
        result = eml_citation(source_reference, contacts=[contact])
        self.assertIn("Kartoza Org", result)
        self.assertNotIn(">Kartoza.<", result)

    def test_eml_citation_in_additional_metadata(self):
        source_reference = SourceReferenceBibliographyF.create()
        source_reference.source.doi = "10.9999/test"
        source_reference.source.save()
        record = self._make_record(source_reference)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(
                self.config, [record], [self.contact], source_reference
            )

        eml = self._read_eml(zip_path)
        self.assertIn("<additionalMetadata>", eml)
        self.assertIn("<citation", eml)
        self.assertIn("10.9999/test", eml)

    # -- publish_gbif_data_with_config --

    def test_publish_gbif_data_with_config_raises_without_contacts(self):
        # Use a bibliography reference so the date/author gate is not triggered.
        source_reference = SourceReferenceBibliographyF.create()
        self._make_record(source_reference)

        with self.assertRaisesRegex(ValueError, "No contacts configured"):
            publish_gbif_data_with_config(
                self.config, source_reference=source_reference, contacts=[]
            )

    def test_publish_gbif_data_with_config_updates_records(self):
        # Use a bibliography reference so the date/author gate is not triggered.
        source_reference = SourceReferenceBibliographyF.create()
        record = self._make_record(source_reference)

        with mock.patch(
            "bims.utils.gbif_publish.build_dwca",
            return_value=("/tmp/dwca.zip", "https://example.org/media/dwca.zip", [record.id]),
        ), mock.patch(
            "bims.utils.gbif_publish.register_dataset",
            return_value="dataset-xyz",
        ), mock.patch(
            "bims.utils.gbif_publish.add_endpoint",
            return_value=None,
        ), mock.patch(
            "bims.utils.gbif_publish.sync_dataset_contacts",
            return_value=None,
        ):
            result = publish_gbif_data_with_config(
                self.config, source_reference=source_reference, contacts=[self.contact]
            )

        record.refresh_from_db()
        self.assertEqual(record.dataset_key, "dataset-xyz")
        self.assertEqual(result["dataset_key"], "dataset-xyz")
        self.assertEqual(result["records_published"], 1)


# ---------------------------------------------------------------------------
# Crontab tests
# ---------------------------------------------------------------------------

class GbifPublishCrontabTests(FastTenantTestCase):

    def test_monthly_crontab_defaults_day_of_month(self):
        source_reference = SourceReferenceF.create()
        config = _make_config()
        schedule = GbifPublish.objects.create(
            source_reference=source_reference,
            gbif_config=config,
            enabled=True,
            period=PublishPeriod.MONTHLY,
            run_at=timezone.datetime(2024, 1, 1, 2, 0).time(),
            day_of_month=None,
            timezone="UTC",
        )

        with schema_context(get_public_schema_name()):
            task = PeriodicTask.objects.get(
                name=f"GBIF publish: source_reference={source_reference.id} id={schedule.id}"
            )
            self.assertEqual(task.crontab.day_of_month, "1")


# ---------------------------------------------------------------------------
# GbifPublishContact model & EML tests
# ---------------------------------------------------------------------------

class GbifPublishContactTests(FastTenantTestCase):

    def setUp(self):
        self.config = _make_config()

    # -- post_save auto-fill from user --

    def test_post_save_fills_blank_fields_from_user(self):
        from bims.models.profile import Role, Profile as BimsProfile
        user = UserF.create(first_name="Auto", last_name="Fill", email="auto@example.com")
        user.organization = "Auto Org"
        user.save()
        role, _ = Role.objects.get_or_create(name="auto_role", defaults={"display_name": "Auto Role"})
        profile, _ = BimsProfile.objects.get_or_create(user=user)
        profile.role = role
        profile.save()

        contact = GbifPublishContact.objects.create(gbif_config=self.config, user=user)
        contact.refresh_from_db()

        self.assertEqual(contact.individual_name_given, "Auto")
        self.assertEqual(contact.individual_name_sur, "Fill")
        self.assertEqual(contact.electronic_mail_address, "auto@example.com")
        self.assertEqual(contact.organization_name, "Auto Org")
        self.assertEqual(contact.position_name, "Auto Role")

    def test_post_save_does_not_overwrite_explicit_fields(self):
        user = UserF.create(first_name="Bob", last_name="Jones", email="bob@example.com")

        contact = GbifPublishContact.objects.create(
            gbif_config=self.config,
            user=user,
            individual_name_given="Custom Given",
            electronic_mail_address="custom@example.com",
        )
        contact.refresh_from_db()

        self.assertEqual(contact.individual_name_given, "Custom Given")
        self.assertEqual(contact.electronic_mail_address, "custom@example.com")
        # blank fields still get filled from user
        self.assertEqual(contact.individual_name_sur, "Jones")

    def test_post_save_no_user_leaves_fields_empty(self):
        contact = GbifPublishContact.objects.create(gbif_config=self.config)
        contact.refresh_from_db()

        self.assertEqual(contact.individual_name_given, "")
        self.assertEqual(contact.individual_name_sur, "")
        self.assertEqual(contact.electronic_mail_address, "")
        self.assertEqual(contact.organization_name, "")
        self.assertEqual(contact.position_name, "")

    def test_post_save_does_not_overwrite_on_update(self):
        """Manual field edits on an existing contact must not be overwritten on save."""
        user = UserF.create(first_name="Carol", last_name="White", email="carol@example.com")
        user.organization = "Original Org"
        user.save()

        contact = GbifPublishContact.objects.create(gbif_config=self.config, user=user)
        contact.refresh_from_db()
        self.assertEqual(contact.individual_name_given, "Carol")
        self.assertEqual(contact.organization_name, "Original Org")

        # User manually changes the name and organization on the contact.
        contact.individual_name_given = "Dr. Carol"
        contact.organization_name = "New Institute"
        contact.save()
        contact.refresh_from_db()

        self.assertEqual(contact.individual_name_given, "Dr. Carol")
        self.assertEqual(contact.organization_name, "New Institute")

    def test_post_save_clearing_field_is_preserved_on_update(self):
        """Clearing a field on an existing contact must not be re-populated from user profile."""
        user = UserF.create(first_name="Dave", last_name="Brown", email="dave@example.com")

        contact = GbifPublishContact.objects.create(gbif_config=self.config, user=user)
        contact.refresh_from_db()
        self.assertEqual(contact.individual_name_given, "Dave")

        # User explicitly clears the given name.
        contact.individual_name_given = ""
        contact.save()
        contact.refresh_from_db()

        self.assertEqual(contact.individual_name_given, "")

    # -- resolved_* fallback properties --

    def test_explicit_fields_take_precedence_over_user(self):
        from bims.models.profile import Role, Profile as BimsProfile
        user = UserF.create(first_name="Bob", last_name="Jones", email="bob@example.com")
        role, _ = Role.objects.get_or_create(name="manager", defaults={"display_name": "Manager"})
        profile, _ = BimsProfile.objects.get_or_create(user=user)
        profile.role = role
        profile.save()

        contact = GbifPublishContact(
            gbif_config=self.config,
            user=user,
            individual_name_given="Override Given",
            individual_name_sur="Override Sur",
            organization_name="Override Org",
            position_name="Override Position",
            electronic_mail_address="override@example.com",
        )
        self.assertEqual(contact.resolved_given_name, "Override Given")
        self.assertEqual(contact.resolved_sur_name, "Override Sur")
        self.assertEqual(contact.resolved_organization_name, "Override Org")
        self.assertEqual(contact.resolved_position_name, "Override Position")
        self.assertEqual(contact.resolved_email, "override@example.com")


    def test_no_user_returns_empty_strings(self):
        contact = GbifPublishContact(gbif_config=self.config)
        self.assertEqual(contact.resolved_given_name, "")
        self.assertEqual(contact.resolved_sur_name, "")
        self.assertEqual(contact.resolved_organization_name, "")
        self.assertEqual(contact.resolved_position_name, "")
        self.assertEqual(contact.resolved_email, "")

    def test_user_without_bims_profile_role_returns_empty_position(self):
        user = UserF.create()
        contact = GbifPublishContact(gbif_config=self.config, user=user)
        self.assertEqual(contact.resolved_position_name, "")

    # -- eml_contact_from_model --

    def test_eml_contact_from_model_basic_fields(self):
        contact = GbifPublishContact(
            gbif_config=self.config,
            individual_name_given="Alice",
            individual_name_sur="Wonder",
            organization_name="SANBI",
            position_name="Botanist",
            electronic_mail_address="alice@sanbi.org",
            phone="+27 21 999 8888",
            online_url="https://sanbi.org",
        )
        snippet = eml_contact_from_model(contact)
        self.assertIn("<givenName>Alice</givenName>", snippet)
        self.assertIn("<surName>Wonder</surName>", snippet)
        self.assertIn("<organizationName>SANBI</organizationName>", snippet)
        self.assertIn("<positionName>Botanist</positionName>", snippet)
        self.assertIn("<electronicMailAddress>alice@sanbi.org</electronicMailAddress>", snippet)
        self.assertIn("<phone>+27 21 999 8888</phone>", snippet)
        self.assertIn("<onlineUrl>https://sanbi.org</onlineUrl>", snippet)

    def test_eml_contact_from_model_address_block(self):
        contact = GbifPublishContact(
            gbif_config=self.config,
            individual_name_sur="Doe",
            delivery_point="1 Main Street",
            city="Cape Town",
            postal_code="8001",
            country="ZA",
        )
        snippet = eml_contact_from_model(contact)
        self.assertIn("<address>", snippet)
        self.assertIn("<deliveryPoint>1 Main Street</deliveryPoint>", snippet)
        self.assertIn("<city>Cape Town</city>", snippet)
        self.assertIn("<postalCode>8001</postalCode>", snippet)
        self.assertIn("<country>ZA</country>", snippet)

    def test_eml_contact_from_model_omits_empty_address(self):
        contact = GbifPublishContact(
            gbif_config=self.config,
            individual_name_given="No",
            individual_name_sur="Address",
        )
        snippet = eml_contact_from_model(contact)
        self.assertNotIn("<address>", snippet)

    def test_eml_contact_org_only_embedded_in_dwca(self):
        """An org-only config contact must appear correctly in the generated eml.xml."""
        source_reference = SourceReferenceF.create()
        record = self._make_record(source_reference)
        contact = _make_contact(
            self.config,
            individual_name_given="",
            individual_name_sur="",
            organization_name="Freshwater Research Centre",
            electronic_mail_address="fbis_gbif@frcsa.org.za",
        )
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(self.config, [record], [contact], source_reference)

        eml = self._read_eml(zip_path)
        self.assertIn("<organizationName>Freshwater Research Centre</organizationName>", eml)
        self.assertIn("<electronicMailAddress>fbis_gbif@frcsa.org.za</electronicMailAddress>", eml)
        # Must not contain an empty <individualName> block
        self.assertNotIn("<individualName>\n  </individualName>", eml)


    def test_eml_contact_default_role_is_point_of_contact(self):
        contact = GbifPublishContact(
            gbif_config=self.config,
            individual_name_sur="Default",
        )
        snippet = eml_contact_from_model(contact)
        self.assertIn("<role>originator</role>", snippet)

    def test_eml_contact_role_originator(self):
        contact = GbifPublishContact(
            gbif_config=self.config,
            individual_name_sur="Origin",
            role=RoleType.ORIGINATOR,
        )
        snippet = eml_contact_from_model(contact)
        self.assertIn("<role>originator</role>", snippet)

    def test_eml_contact_role_all_choices_rendered(self):
        """Every RoleType value should render a matching <role> element."""
        for role_value, _ in RoleType.choices:
            contact = GbifPublishContact(
                gbif_config=self.config,
                individual_name_sur="Test",
                role=role_value,
            )
            snippet = eml_contact_from_model(contact)
            self.assertIn(f"<role>{role_value}</role>", snippet, msg=f"Missing role: {role_value}")

    def test_eml_contact_role_embedded_in_dwca_eml(self):
        """Role value should appear inside <contact> in the generated eml.xml."""
        source_reference = SourceReferenceF.create()
        record = self._make_record(source_reference)
        contact = _make_contact(
            self.config,
            individual_name_given="Publisher",
            individual_name_sur="Person",
            role=RoleType.PUBLISHER,
        )
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(self.config, [record], [contact], source_reference)

        eml = self._read_eml(zip_path)
        self.assertIn("<role>publisher</role>", eml)

    # -- contacts in EML via build_dwca --

    def _make_record(self, source_reference, **kwargs):
        kwargs.setdefault("institution_id", "Test Institution")
        survey = SurveyF.create(validated=True)
        return BiologicalCollectionRecordF.create(
            survey=survey,
            source_reference=source_reference,
            data_type="public",
            **kwargs,
        )

    def _read_eml(self, zip_path):
        with zf.ZipFile(zip_path) as z:
            return z.read("eml.xml").decode("utf-8")

    def test_build_dwca_embeds_contacts_in_eml(self):
        source_reference = SourceReferenceF.create()
        record = self._make_record(source_reference)
        contact = _make_contact(
            self.config,
            individual_name_given="Lerato",
            individual_name_sur="Dlamini",
            organization_name="SAIAB",
            electronic_mail_address="lerato@saiab.ac.za",
            phone="+27 46 603 5800",
        )
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(self.config, [record], [contact], source_reference)

        eml = self._read_eml(zip_path)
        self.assertIn("<contact>", eml)
        self.assertIn("<givenName>Lerato</givenName>", eml)
        self.assertIn("<surName>Dlamini</surName>", eml)
        self.assertIn("<organizationName>SAIAB</organizationName>", eml)
        self.assertIn("<electronicMailAddress>lerato@saiab.ac.za</electronicMailAddress>", eml)
        self.assertIn("<phone>+27 46 603 5800</phone>", eml)

    def test_build_dwca_without_originator_uses_site_default_creator(self):
        source_reference = SourceReferenceF.create()
        record = self._make_record(source_reference)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))
        contact = _make_contact(
            self.config,
            individual_name_given="Lerato",
            individual_name_sur="Dlamini",
            organization_name="SAIAB",
            electronic_mail_address="lerato@saiab.ac.za",
            phone="+27 46 603 5800",
            role=RoleType.METADATA_PROVIDER,
        )

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/", SITE_NAME="TestSite"):
            zip_path, _, _ = build_dwca(self.config, [record], [contact], source_reference)

        eml = self._read_eml(zip_path)
        self.assertIn("<contact>", eml)
        self.assertIn("TestSite", eml)

    def test_originator_contacts_appear_as_creators(self):
        """Contacts with role=originator appear in both <creator> and <contact> blocks."""
        source_reference = SourceReferenceF.create()
        record = self._make_record(source_reference)
        contact = _make_contact(self.config, individual_name_given="ContactOnly", role=RoleType.ORIGINATOR)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(self.config, [record], [contact], source_reference)

        eml = self._read_eml(zip_path)
        self.assertIn("<contact>", eml)
        self.assertIn("<creator>", eml)
        self.assertEqual(eml.count("ContactOnly"), 2)

    def test_non_originator_contact_not_in_creator(self):
        """Contacts with a non-originator role appear only in <contact>, not <creator>."""
        source_reference = SourceReferenceF.create()
        record = self._make_record(source_reference)
        contact = _make_contact(self.config, individual_name_given="MetaOnly", role=RoleType.METADATA_PROVIDER)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(self.config, [record], [contact], source_reference)

        eml = self._read_eml(zip_path)
        self.assertIn("<contact>", eml)
        self.assertIn("MetaOnly", eml)
        # no originator - falls back to site-name default creator
        self.assertIn("<creator>", eml)
        self.assertNotIn("MetaOnly", eml.split("<creator>")[1].split("</creator>")[0])

    def test_pubdate_falls_back_to_today_without_source_reference(self):
        """<pubDate> should fall back to today's date when no source reference is given."""
        from datetime import datetime as dt
        today = dt.utcnow().date().isoformat()
        source_reference = SourceReferenceF.create()
        record = self._make_record(source_reference)
        contact = _make_contact(self.config)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(self.config, [record], [contact], None)

        eml = self._read_eml(zip_path)
        self.assertIn(f"<pubDate>{today}</pubDate>", eml)


# ---------------------------------------------------------------------------
# Unpublished / Database GBIF eligibility gate tests
# ---------------------------------------------------------------------------

class GbifPublishEligibilityGateTests(FastTenantTestCase):
    """publish_gbif_data_with_config must reject unpublished/database source
    references that are missing a date or authors."""

    def setUp(self):
        self.config = _make_config()
        self.contact = _make_contact(self.config)

    def _make_record(self, source_reference):
        survey = SurveyF.create(validated=True)
        return BiologicalCollectionRecordF.create(
            survey=survey,
            source_reference=source_reference,
            data_type="public",
            institution_id="Test Institution",
        )

    def _add_author(self, source_reference):
        user = UserF.create(first_name="Alice", last_name="Author")
        author = Author.objects.create(
            first_name="Alice", last_name="Author", user=user
        )
        SourceReferenceAuthor.objects.create(
            source_reference=source_reference, author=author, order=0
        )
        return author

    # -- unpublished (plain SourceReference) --

    def test_unpublished_without_date_raises(self):
        sr = SourceReferenceF.create()
        self._add_author(sr)
        self._make_record(sr)

        with self.assertRaisesRegex(ValueError, "no date"):
            publish_gbif_data_with_config(
                self.config, source_reference=sr, contacts=[self.contact]
            )

    def test_unpublished_without_authors_raises(self):
        import datetime
        sr = SourceReferenceF.create(source_date=datetime.date(2022, 1, 1))
        self._make_record(sr)

        with self.assertRaisesRegex(ValueError, "no authors"):
            publish_gbif_data_with_config(
                self.config, source_reference=sr, contacts=[self.contact]
            )

    def test_unpublished_with_date_and_authors_passes_gate(self):
        import datetime
        sr = SourceReferenceF.create(source_date=datetime.date(2022, 6, 1))
        self._add_author(sr)
        self._make_record(sr)

        with mock.patch(
            "bims.utils.gbif_publish.build_dwca",
            return_value=("/tmp/dwca.zip", "https://example.org/media/dwca.zip", [1]),
        ), mock.patch(
            "bims.utils.gbif_publish.register_dataset", return_value="ds-key"
        ), mock.patch(
            "bims.utils.gbif_publish.add_endpoint", return_value=None
        ), mock.patch(
            "bims.utils.gbif_publish.sync_dataset_contacts", return_value=None
        ):
            result = publish_gbif_data_with_config(
                self.config, source_reference=sr, contacts=[self.contact]
            )
        self.assertEqual(result["dataset_key"], "ds-key")

    # -- database (SourceReferenceDatabase) --

    def test_database_without_date_raises(self):
        sr = SourceReferenceDatabaseF.create()
        self._add_author(sr)
        self._make_record(sr)

        with self.assertRaisesRegex(ValueError, "no date"):
            publish_gbif_data_with_config(
                self.config, source_reference=sr, contacts=[self.contact]
            )

    def test_database_without_authors_raises(self):
        import datetime
        sr = SourceReferenceDatabaseF.create(source_date=datetime.date(2021, 3, 15))
        self._make_record(sr)

        with self.assertRaisesRegex(ValueError, "no authors"):
            publish_gbif_data_with_config(
                self.config, source_reference=sr, contacts=[self.contact]
            )

    def test_database_with_date_and_authors_passes_gate(self):
        import datetime
        sr = SourceReferenceDatabaseF.create(source_date=datetime.date(2021, 3, 15))
        self._add_author(sr)
        self._make_record(sr)

        with mock.patch(
            "bims.utils.gbif_publish.build_dwca",
            return_value=("/tmp/dwca.zip", "https://example.org/media/dwca.zip", [1]),
        ), mock.patch(
            "bims.utils.gbif_publish.register_dataset", return_value="ds-db-key"
        ), mock.patch(
            "bims.utils.gbif_publish.add_endpoint", return_value=None
        ), mock.patch(
            "bims.utils.gbif_publish.sync_dataset_contacts", return_value=None
        ):
            result = publish_gbif_data_with_config(
                self.config, source_reference=sr, contacts=[self.contact]
            )
        self.assertEqual(result["dataset_key"], "ds-db-key")

    # -- bibliography / document are exempt from the gate --

    def test_bibliography_without_date_and_authors_is_not_blocked(self):
        """SourceReferenceBibliography skips the date/author gate entirely."""
        sr = SourceReferenceBibliographyF.create()

        # Patch everything after the gate so no network call is made.
        with mock.patch(
            "bims.utils.gbif_publish.gather_data_for_source_reference",
            return_value=[],
        ):
            with self.assertRaisesRegex(ValueError, "No records to publish"):
                # Fails at "no records", NOT at the date/author gate.
                publish_gbif_data_with_config(
                    self.config, source_reference=sr, contacts=[self.contact]
                )


# ---------------------------------------------------------------------------
# EML creator blocks from originator contacts
# ---------------------------------------------------------------------------

class GbifPublishEmlCreatorTests(FastTenantTestCase):
    """build_dwca must use originator contacts as EML <creator> blocks."""

    def setUp(self):
        self.config = _make_config(export_base_url="https://example.org")
        self.contact = _make_contact(self.config)

    def _make_record(self, source_reference):
        survey = SurveyF.create(validated=True)
        return BiologicalCollectionRecordF.create(
            survey=survey,
            source_reference=source_reference,
            data_type="public",
            institution_id="Test Institution",
        )

    def _read_eml(self, zip_path):
        with zf.ZipFile(zip_path) as z:
            return z.read("eml.xml").decode("utf-8")

    def test_build_dwca_eml_creator_from_originator_contact(self):
        """An originator contact appears as an EML <creator> with name and email."""
        import datetime
        sr = SourceReferenceF.create(source_date=datetime.date(2023, 1, 1))
        contact = _make_contact(
            self.config,
            individual_name_given="Thabo",
            individual_name_sur="Nkosi",
            electronic_mail_address="thabo@example.com",
            role=RoleType.ORIGINATOR,
        )
        record = self._make_record(sr)

        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(self.config, [record], [contact], sr)

        eml = self._read_eml(zip_path)
        self.assertIn("<creator>", eml)
        self.assertIn("<givenName>Thabo</givenName>", eml)
        self.assertIn("<surName>Nkosi</surName>", eml)

    def test_build_dwca_eml_falls_back_to_site_name_when_no_originators(self):
        """When no originator contacts exist the site-name default creator is used."""
        import datetime
        sr = SourceReferenceF.create(source_date=datetime.date(2023, 1, 1))
        contact = _make_contact(self.config, role=RoleType.METADATA_PROVIDER)
        record = self._make_record(sr)

        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/", SITE_NAME="FallbackSite"):
            zip_path, _, _ = build_dwca(self.config, [record], [contact], sr)

        eml = self._read_eml(zip_path)
        self.assertIn("<creator>", eml)
        self.assertIn("FallbackSite", eml)

    def test_build_dwca_eml_creator_order_follows_contacts_order(self):
        """Multiple originator contacts appear as <creator> blocks in contacts list order."""
        import datetime
        sr = SourceReferenceF.create(source_date=datetime.date(2023, 1, 1))
        contact_a = _make_contact(self.config, individual_name_given="First", role=RoleType.ORIGINATOR)
        contact_b = _make_contact(self.config, individual_name_given="Second", role=RoleType.ORIGINATOR)
        record = self._make_record(sr)

        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(self.config, [record], [contact_a, contact_b], sr)

        eml = self._read_eml(zip_path)
        self.assertLess(eml.index("First"), eml.index("Second"),
                        "First originator contact should appear before Second in EML")


class GbifMetadataCsvParsingTests(SimpleTestCase):
    HEADER = (
        "id,project_identifier,title,description,license,taxonomic_coverage,"
        "geographic_description,temporal_start,temporal_end,"
        "sampling_description,purpose\n"
    )
    ROW_1 = (
        '1234,BID-REG2025-094,Fish surveys,"First paragraph.\n\nSecond.",CC BY 4.0,'
        "Fishes to species,Tana River Basin,2018-03-15,2022-11-30,Seine nets,"
        "To assess fish\n"
    )
    ROW_2 = "99,,Other,Desc,CC0 1.0,,,,,,\n"

    def _parse(self, text, *args, **kwargs):
        import io
        from bims.utils.gbif_metadata import parse_metadata_file
        return parse_metadata_file(
            io.BytesIO(text.encode("utf-8")), *args, **kwargs)

    def test_selects_row_by_source_reference_id(self):
        row = self._parse(self.HEADER + self.ROW_1 + self.ROW_2, 99)
        self.assertEqual(row["title"], "Other")
        row = self._parse(self.HEADER + self.ROW_1 + self.ROW_2, 1234)
        self.assertEqual(row["project_identifier"], "BID-REG2025-094")

    def test_missing_id_row_is_rejected(self):
        with self.assertRaises(ValueError):
            self._parse(self.HEADER + self.ROW_1 + self.ROW_2, 5)

    def test_single_row_without_id_is_accepted(self):
        row = self._parse(self.HEADER + ",,Only,Desc,,,,,,,\n", 5)
        self.assertEqual(row["title"], "Only")

    def test_id_column_is_optional(self):
        text = "title,description\nOnly,Desc\n"
        self.assertEqual(self._parse(text, 5)["title"], "Only")

    def test_single_row_with_other_id_is_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            self._parse(self.HEADER + self.ROW_1, 5)
        self.assertIn("does not match", str(ctx.exception))

    def test_validation_errors(self):
        bad = "7,,,Desc,GPL,,,2022-01-02,2021-01-01,,\n"
        with self.assertRaises(ValueError) as ctx:
            self._parse(self.HEADER + bad, 7)
        msg = str(ctx.exception)
        self.assertIn("title must not be empty", msg)
        self.assertIn("license", msg)
        self.assertIn("temporal_end", msg)

    def test_duplicate_ids_rejected(self):
        with self.assertRaises(ValueError):
            self._parse(self.HEADER + self.ROW_1 + self.ROW_1, 1234)

    def test_other_rows_only_validated_when_requested(self):
        bad_other = "55,,,,,,,,,,\n"
        text = self.HEADER + self.ROW_1 + bad_other
        self.assertEqual(self._parse(text, 1234)["id"], "1234")
        with self.assertRaises(ValueError):
            self._parse(text, 1234, validate_all=True)

    def test_semicolon_delimiter_bom_and_licence_case(self):
        text = "\ufeff" + (self.HEADER + "99,,Other,Desc,cc0 1.0,,,,,,\n").replace(",", ";")
        self.assertEqual(self._parse(text, 99)["license"], "CC0 1.0")

    def test_eml_contains_metadata(self):
        import os
        import tempfile
        import xml.etree.ElementTree as ET
        from bims.utils.gbif_publish import write_eml_xml

        row = self._parse(self.HEADER + self.ROW_1, 1234)
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "eml.xml")
            write_eml_xml(
                path, row["title"], "generic", [], metadata=row,
                bbox=(30.0, 31.0, -1.0, -2.0))
            dataset = ET.parse(path).getroot().find("dataset")
        self.assertEqual(
            [p.text for p in dataset.findall("abstract/para")],
            ["First paragraph.", "Second."])
        self.assertIn("CC BY 4.0", dataset.find("intellectualRights/para").text)
        self.assertEqual(
            dataset.find("coverage/geographicCoverage/geographicDescription").text,
            "Tana River Basin")
        self.assertEqual(
            dataset.find("coverage/temporalCoverage/rangeOfDates/endDate/calendarDate").text,
            "2022-11-30")
        self.assertEqual(
            dataset.find("coverage/taxonomicCoverage/generalTaxonomicCoverage").text,
            "Fishes to species")
        self.assertEqual(dataset.find("purpose/para").text, "To assess fish")
        self.assertEqual(
            dataset.find("methods/sampling/samplingDescription/para").text,
            "Seine nets")
        self.assertEqual(dataset.find("project").get("id"), "BID-REG2025-094")
        tags = [c.tag for c in dataset]
        self.assertLess(tags.index("abstract"), tags.index("intellectualRights"))
        self.assertLess(tags.index("intellectualRights"), tags.index("coverage"))
        self.assertLess(tags.index("coverage"), tags.index("purpose"))
        self.assertLess(tags.index("purpose"), tags.index("methods"))
        self.assertLess(tags.index("methods"), tags.index("project"))

    def test_eml_without_metadata_keeps_generic_abstract(self):
        import os
        import tempfile
        import xml.etree.ElementTree as ET
        from bims.utils.gbif_publish import write_eml_xml

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "eml.xml")
            write_eml_xml(path, "T", "generic abstract", [])
            dataset = ET.parse(path).getroot().find("dataset")
        self.assertEqual(dataset.find("abstract/para").text, "generic abstract")
        self.assertIsNone(dataset.find("project"))
        self.assertIsNone(dataset.find("coverage"))


class GbifMetadataLoadTests(FastTenantTestCase):
    HEADER = GbifMetadataCsvParsingTests.HEADER

    def test_load_publish_metadata(self):
        from django.core.files.base import ContentFile
        from bims.utils.gbif_publish import load_publish_metadata
        sr = SourceReferenceF.create()
        self.assertEqual(load_publish_metadata(sr), {})
        self.assertEqual(load_publish_metadata(None), {})
        sr.gbif_metadata_file.save("meta.csv", ContentFile(
            (self.HEADER + f"{sr.pk},BID-REG2025-094,T,D,CC BY 4.0,,,,,,\n").encode()))
        try:
            self.assertEqual(
                load_publish_metadata(sr)["project_identifier"], "BID-REG2025-094")
        finally:
            sr.gbif_metadata_file.delete()


class GbifMetadataAdminFormTests(FastTenantTestCase):
    HEADER = GbifMetadataCsvParsingTests.HEADER

    def _form(self, sr, csv_text, name="meta.csv"):
        from django.core.files.uploadedfile import SimpleUploadedFile
        from django.forms import modelform_factory
        from bims.custom_admin.source_reference import GbifMetadataCsvAdminForm
        from bims.models.source_reference import SourceReference
        form_class = modelform_factory(
            SourceReference, form=GbifMetadataCsvAdminForm,
            fields=("gbif_metadata_file",))
        return form_class(
            data={}, instance=sr,
            files={"gbif_metadata_file": SimpleUploadedFile(
                name, csv_text.encode("utf-8"), content_type="text/csv")})

    def test_valid_csv_accepted(self):
        sr = SourceReferenceF.create()
        form = self._form(sr, self.HEADER + f"{sr.pk},,T,D,CC BY 4.0,,,,,,\n")
        self.assertTrue(form.is_valid(), form.errors)

    def test_invalid_csv_rejected(self):
        sr = SourceReferenceF.create()
        form = self._form(sr, self.HEADER + f"{sr.pk},,,D,GPL,,,,,,\n")
        self.assertFalse(form.is_valid())
        self.assertIn("gbif_metadata_file", form.errors)

    def test_non_csv_rejected(self):
        sr = SourceReferenceF.create()
        form = self._form(sr, "title,description\nT,D\n", name="meta.pdf")
        self.assertFalse(form.is_valid())
        self.assertIn("gbif_metadata_file", form.errors)
