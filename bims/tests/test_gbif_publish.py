import csv
import io
import json
import os
import shutil
import tempfile
import zipfile as zf
from unittest import mock

from django.db import connection
from django.test import override_settings
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
        "license_url": "https://creativecommons.org/publicdomain/zero/1.0/legalcode",
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

    def test_eml_citation_plain_reference_returns_empty(self):
        source_reference = SourceReferenceF.create()
        self.assertEqual(eml_citation(source_reference), "")

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

    def test_blank_fields_fall_back_to_user_profile(self):
        from bims.models.profile import Role, Profile as BimsProfile
        user = UserF.create(first_name="Jane", last_name="Smith", email="jane@example.com")
        user.organization = "Cape Town University"
        user.save()
        role, _ = Role.objects.get_or_create(name="researcher", defaults={"display_name": "Researcher"})
        profile, _ = BimsProfile.objects.get_or_create(user=user)
        profile.role = role
        profile.save()

        contact = GbifPublishContact(gbif_config=self.config, user=user)
        self.assertEqual(contact.resolved_given_name, "Jane")
        self.assertEqual(contact.resolved_sur_name, "Smith")
        self.assertEqual(contact.resolved_organization_name, "Cape Town University")
        self.assertEqual(contact.resolved_position_name, "Researcher")
        self.assertEqual(contact.resolved_email, "jane@example.com")

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

    def test_eml_contact_org_only_omits_individual_name(self):
        """Org-only contact (no given/sur name) must not emit an empty <individualName>."""
        contact = GbifPublishContact(
            gbif_config=self.config,
            organization_name="Freshwater Research Centre",
            electronic_mail_address="fbis_gbif@frcsa.org.za",
        )
        snippet = eml_contact_from_model(contact)
        self.assertNotIn("<individualName>", snippet)
        self.assertIn("<organizationName>Freshwater Research Centre</organizationName>", snippet)
        self.assertIn("<electronicMailAddress>fbis_gbif@frcsa.org.za</electronicMailAddress>", snippet)

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

    def test_eml_contact_from_model_uses_resolved_fallback(self):
        user = UserF.create(first_name="Tom", last_name="Baker", email="tom@example.com")
        contact = GbifPublishContact(gbif_config=self.config, user=user)
        snippet = eml_contact_from_model(contact)
        self.assertIn("<givenName>Tom</givenName>", snippet)
        self.assertIn("<surName>Baker</surName>", snippet)
        self.assertIn("<electronicMailAddress>tom@example.com</electronicMailAddress>", snippet)

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

    def test_build_dwca_without_contacts_uses_site_default(self):
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
        )

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/", SITE_NAME="TestSite"):
            zip_path, _, _ = build_dwca(self.config, [record], [contact], source_reference)

        eml = self._read_eml(zip_path)
        self.assertIn("<contact>", eml)
        self.assertIn("TestSite", eml)

    def test_contacts_not_used_as_creators(self):
        """Contacts should appear in <contact> but NOT replace <creator> blocks."""
        source_reference = SourceReferenceF.create()
        record = self._make_record(source_reference)
        contact = _make_contact(self.config, individual_name_given="ContactOnly")
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(self.config, [record], [contact], source_reference)

        eml = self._read_eml(zip_path)
        # contact name appears inside <contact> block
        self.assertIn("<contact>", eml)
        self.assertIn("ContactOnly", eml)
        # but <creator> block still exists (site default since no authors on this ref)
        self.assertIn("<creator>", eml)

    def test_pubdate_uses_source_reference_year(self):
        """<pubDate> should reflect the source reference publication year."""
        import datetime
        source_reference = SourceReferenceBibliographyF.create()
        source_reference.source.publication_date = datetime.date(2019, 6, 15)
        source_reference.source.save()

        record = self._make_record(source_reference)
        contact = _make_contact(self.config)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(self.config, [record], [contact], source_reference)

        eml = self._read_eml(zip_path)
        self.assertIn("<pubDate>2019</pubDate>", eml)

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
# EML author resolution from SourceReferenceAuthor
# ---------------------------------------------------------------------------

class GbifPublishEmlAuthorResolutionTests(FastTenantTestCase):
    """build_dwca must resolve SourceReferenceAuthor → User/Author for EML."""

    def setUp(self):
        self.config = _make_config(export_base_url="https://example.org")
        self.contact = _make_contact(self.config)

    def _make_record(self, source_reference):
        survey = SurveyF.create(validated=True)
        return BiologicalCollectionRecordF.create(
            survey=survey,
            source_reference=source_reference,
            data_type="public",
        )

    def _read_eml(self, zip_path):
        with zf.ZipFile(zip_path) as z:
            return z.read("eml.xml").decode("utf-8")

    def test_build_dwca_eml_includes_author_from_through_model_with_user(self):
        """Author linked to a User: EML <creator> contains first/last name."""
        import datetime
        sr = SourceReferenceF.create(source_date=datetime.date(2023, 1, 1))
        user = UserF.create(first_name="Thabo", last_name="Nkosi", email="thabo@example.com")
        author = Author.objects.create(first_name="Thabo", last_name="Nkosi", user=user)
        SourceReferenceAuthor.objects.create(source_reference=sr, author=author, order=0)
        record = self._make_record(sr)

        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(self.config, [record], [self.contact], sr)

        eml = self._read_eml(zip_path)
        self.assertIn("<creator>", eml)
        self.assertIn("<givenName>Thabo</givenName>", eml)
        self.assertIn("<surName>Nkosi</surName>", eml)

    def test_build_dwca_eml_includes_author_without_user(self):
        """Author with no linked User: EML <creator> still contains first/last name."""
        import datetime
        sr = SourceReferenceF.create(source_date=datetime.date(2023, 1, 1))
        author = Author.objects.create(first_name="Maria", last_name="Santos")
        SourceReferenceAuthor.objects.create(source_reference=sr, author=author, order=0)
        record = self._make_record(sr)

        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(self.config, [record], [self.contact], sr)

        eml = self._read_eml(zip_path)
        self.assertIn("<creator>", eml)
        self.assertIn("<givenName>Maria</givenName>", eml)
        self.assertIn("<surName>Santos</surName>", eml)

    def test_build_dwca_eml_author_order_preserved(self):
        """Multiple authors appear in EML in their defined order."""
        import datetime
        sr = SourceReferenceF.create(source_date=datetime.date(2023, 1, 1))
        for i, (first, last) in enumerate([("First", "Author"), ("Second", "Writer")]):
            author = Author.objects.create(first_name=first, last_name=last)
            SourceReferenceAuthor.objects.create(source_reference=sr, author=author, order=i)
        record = self._make_record(sr)

        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))

        with override_settings(MEDIA_ROOT=temp_dir, MEDIA_URL="/media/"):
            zip_path, _, _ = build_dwca(self.config, [record], [self.contact], sr)

        eml = self._read_eml(zip_path)
        pos_first = eml.index("First")
        pos_second = eml.index("Second")
        self.assertLess(pos_first, pos_second, "First author should appear before Second in EML")
