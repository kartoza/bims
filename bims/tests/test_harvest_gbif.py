import os
import csv
import zipfile
from tempfile import NamedTemporaryFile
from pathlib import Path
from unittest import mock

import requests
from django.contrib.gis.geos import Point
from django_tenants.test.cases import FastTenantTestCase
from urllib3.exceptions import ProtocolError

from bims.models import Survey, BiologicalCollectionRecord, LocationSite, LocationType, SourceReference, \
    SourceReferenceDatabase
from bims.tests.model_factories import (
    BiologicalCollectionRecordF,
    TaxonomyF,
)
from bims.scripts.import_gbif_occurrences import import_gbif_occurrences

# -----------------------------------------------------------------------------
# Helpers & mocks
# -----------------------------------------------------------------------------


def _create_dummy_gbif_zip() -> str:
    """Create a Darwin‑Core zip archive with six occurrences.

    Two rows share identical coordinates so that only five *LocationSite*/Survey
    pairs are produced – mirroring the expectations of the legacy tests.
    """

    header = [
        "gbifID",
        "decimalLongitude",
        "decimalLatitude",
        "coordinateUncertaintyInMeters",
        "eventDate",
        "recordedBy",
        "institutionCode",
        "references",
        "verbatimLocality",
        "locality",
        "species",
        "datasetKey",
        "modified",
        "basisOfRecord",
        "projectId",
        "taxonKey"
    ]

    rows = [
        [
            "2563631087",
            "30",
            "-25",
            "",
            "2021-01-01",
            "Collector F",
            "INST",
            "http://example.org/2563631087",
            "Site F",
            "Site F",
            "Species F",
            "a8503dc0-7226-4ac5-999e-245b458e84b3",
            "2021-01-02",
            "OBSERVATION",
            "",
            "1",
        ],
        [
            "1",
            "30",  # identical coordinates
            "-25",
            "",
            "2021-01-01",
            "Collector A",
            "INST",
            "http://example.org/1",
            "Site A",
            "Site A",
            "Species A",
            "ead9ec8e-b346-4f4f-bad4-7cbaf0e0066f",
            "2021-01-02",
            "OBSERVATION",
            "",
            "1",
        ],
        [
            "2",
            "31",
            "-25",
            "",
            "2021-01-01",
            "Collector B",
            "INST",
            "http://example.org/2",
            "Site B",
            "Site B",
            "Species B",
            "ced0ecb9-19fe-4297-9aa5-2424a687b70c",
            "2021-01-02",
            "OBSERVATION",
            "",
            "1",
        ],
        [
            "3",
            "32",
            "-25",
            "",
            "2021-01-01",
            "Collector C",
            "INST",
            "http://example.org/3",
            "Site C",
            "Site C",
            "Species C",
            "075ae6ac-14c2-4bbf-ac1d-a9a2dd8093db",
            "2021-01-02",
            "OBSERVATION",
            "",
            "1",
        ],
        [
            "4",
            "33",
            "-25",
            "",
            "2021-01-01",
            "Collector D",
            "INST",
            "http://example.org/4",
            "Site D",
            "Site D",
            "Species D",
            "1c15ccaf-25f6-4566-90a8-a5f292e78779",
            "2021-01-02",
            "OBSERVATION",
            "",
            "1",
        ],
        [
            "5",
            "34",
            "-25",
            "",
            "2021-01-01",
            "Collector E",
            "INST",
            "http://example.org/5",
            "Site E",
            "Site E",
            "Species E",
            "087e4051-dcbc-4435-a99e-f6d82f672c82",
            "2021-01-02",
            "OBSERVATION",
            "",
            "1",
        ],
    ]

    with NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zf:
            content = "\t".join(header) + "\n" + "\n".join("\t".join(r) for r in rows)
            zf.writestr("occurrence.txt", content)
        return tmp.name


def _create_single_row_zip(gbif_id: str, lon: str, lat: str, taxon_key: str) -> str:
    """Create a Darwin-Core zip with a single occurrence row."""
    header = [
        "gbifID", "decimalLongitude", "decimalLatitude",
        "coordinateUncertaintyInMeters", "eventDate", "recordedBy",
        "institutionCode", "references", "verbatimLocality", "locality",
        "species", "datasetKey", "modified", "basisOfRecord", "projectId",
        "taxonKey",
    ]
    row = [
        gbif_id, lon, lat, "", "2021-06-15", "Collector X", "INST",
        f"http://example.org/{gbif_id}", "Test Site", "Test Site",
        "Test species", "ead9ec8e-b346-4f4f-bad4-7cbaf0e0066f",
        "2021-06-16", "OBSERVATION", "", taxon_key,
    ]
    with NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zf:
            content = "\t".join(header) + "\n" + "\t".join(row)
            zf.writestr("occurrence.txt", content)
        return tmp.name


# Dummy replacements for the GBIF helper functions --------------------------------

def _mock_submit_download(*_, **__):
    return "mock-key", 200

def _mock_download_information(*_, **__):
    return {
        "key": "XXXXXXXX",
        "doi": "10.11111/111111"
    }

def _mock_get_ready_download_url(*_, **__):
    return "http://example.org/download.zip"


def _mock_download_archive(*_, **__):
    return _create_dummy_gbif_zip()


def _mock_harvest_session_get(*_, **__):
    fake_session = mock.Mock()
    fake_session.id = 1
    fake_session.canceled = False
    return fake_session

# No‑op for dataset look‑ups so the tests stay offline
_noop = lambda *_, **__: None  # noqa: E731 – keep it tiny


# -----------------------------------------------------------------------------
# Test‑case
# -----------------------------------------------------------------------------

@mock.patch("bims.models.location_site.update_location_site_context")
class TestHarvestGbif(FastTenantTestCase):
    """Exercise the GBIF harvest pipeline with fully mocked network helpers."""

    def setUp(self):
        self.taxonomy = TaxonomyF.create(col_id="1")
        os.environ.setdefault("GBIF_USERNAME", "user")
        os.environ.setdefault("GBIF_PASSWORD", "pass")

    def tearDown(self):
        BiologicalCollectionRecord.objects.filter(taxonomy=self.taxonomy).delete()
        Survey.objects.filter(biological_collection_record__taxonomy=self.taxonomy).delete()
        self.taxonomy.delete()

    # ------------------------------------------------------------------
    # Success path – first run and idempotency on the second run
    # ------------------------------------------------------------------
    @mock.patch("bims.scripts.import_gbif_occurrences.get_download_information", _mock_download_information)
    @mock.patch("bims.scripts.import_gbif_occurrences.submit_download", _mock_submit_download)
    @mock.patch("bims.scripts.import_gbif_occurrences.get_ready_download_url", _mock_get_ready_download_url)
    @mock.patch("bims.scripts.import_gbif_occurrences.download_archive", _mock_download_archive)
    @mock.patch("bims.scripts.import_gbif_occurrences.is_canceled", lambda *_: False)
    @mock.patch("bims.scripts.import_gbif_occurrences.create_dataset_from_gbif", _noop)
    @mock.patch("bims.models.HarvestSession.objects.get", _mock_harvest_session_get)
    @mock.patch("bims.scripts.import_gbif_occurrences.preferences")
    def test_harvest_gbif(self, mock_preferences, mock_update_location_context):
        # Configure site preferences (no boundary filtering)
        mock_preferences.SiteSetting.base_country_code = "ZA"
        mock_preferences.SiteSetting.site_boundary = None

        # Sanity: no GBIF data yet
        self.assertEqual(
            BiologicalCollectionRecord.objects.filter(
                owner__username="GBIF",
                taxonomy=self.taxonomy,
                source_reference__source_name="Global Biodiversity Information Facility (GBIF)",
            ).count(),
            0,
        )

        status = import_gbif_occurrences([self.taxonomy.id], session_id=1)
        self.assertTrue(status)

        # Six records, five distinct surveys (two points share identical coords)
        source_references = SourceReferenceDatabase.objects.filter(
            source_name='Global Biodiversity Information Facility (GBIF)',
        )
        self.assertEqual(
            BiologicalCollectionRecord.objects.filter(
                owner__username="GBIF",
                taxonomy=self.taxonomy,
                source_reference_id__in=source_references,
            ).count(),
            6,
        )
        self.assertEqual(
            Survey.objects.filter(
                owner__username="GBIF",
                biological_collection_record__taxonomy=self.taxonomy,
                validated=True,
            )
            .distinct()
            .count(),
            5,
        )

        # Second import run must be idempotent
        import_gbif_occurrences([self.taxonomy.id], session_id=1)
        self.assertEqual(
            BiologicalCollectionRecord.objects.filter(
                owner__username="GBIF",
                taxonomy=self.taxonomy,
                source_reference__source_name="Global Biodiversity Information Facility (GBIF)",
            ).count(),
            6,
        )

        mock_update_location_context.assert_called()

    # ------------------------------------------------------------------
    # Incoming data references an upstream_id that already has duplicates
    # ------------------------------------------------------------------
    @mock.patch("bims.scripts.import_gbif_occurrences.submit_download", _mock_submit_download)
    @mock.patch("bims.scripts.import_gbif_occurrences.get_ready_download_url", _mock_get_ready_download_url)
    @mock.patch("bims.scripts.import_gbif_occurrences.download_archive", _mock_download_archive)
    @mock.patch("bims.scripts.import_gbif_occurrences.is_canceled", lambda *_: False)
    @mock.patch("bims.scripts.import_gbif_occurrences.create_dataset_from_gbif", _noop)
    @mock.patch("bims.models.HarvestSession.objects.get", _mock_harvest_session_get)
    @mock.patch("bims.scripts.import_gbif_occurrences.preferences")
    def test_harvest_gbif_multiple_objects_returned(self, mock_preferences, mock_update_location_context):
        mock_preferences.SiteSetting.base_country_code = "ZA"
        mock_preferences.SiteSetting.site_boundary = None

        BiologicalCollectionRecordF.create(upstream_id="2563631087", taxonomy=self.taxonomy)
        BiologicalCollectionRecordF.create(upstream_id="2563631087", taxonomy=self.taxonomy)

        status = import_gbif_occurrences([self.taxonomy.id], session_id=1)
        self.assertTrue(status)

        # Harvester should resolve duplicates – five GBIF rows remain
        self.assertEqual(
            BiologicalCollectionRecord.objects.filter(
                owner__username="GBIF",
                taxonomy=self.taxonomy,
                source_reference__source_name="Global Biodiversity Information Facility (GBIF)",
            ).count(),
            5,
        )
        mock_update_location_context.assert_called()

    # ------------------------------------------------------------------
    # Error handling: HTTPError from the GBIF helper (returns no key)
    # ------------------------------------------------------------------
    def _submit_download_http_error(*_, **__):  # pylint: disable=unused-argument
        return None, 404

    @mock.patch("bims.scripts.import_gbif_occurrences.submit_download", _submit_download_http_error)
    @mock.patch("bims.models.HarvestSession.objects.get", _mock_harvest_session_get)
    @mock.patch("bims.scripts.import_gbif_occurrences.preferences")
    def test_harvest_gbif_http_error(self, mock_preferences, mock_update_location_context):
        mock_preferences.SiteSetting.base_country_code = "ZA"
        mock_preferences.SiteSetting.site_boundary = None

        status = import_gbif_occurrences([self.taxonomy.id], session_id=1)
        self.assertEqual(status, "Download request failed")

    # ------------------------------------------------------------------
    # Error handling: low‑level connection failures (returns no key)
    # ------------------------------------------------------------------
    def _submit_download_protocol_error(*_, **__):  # pylint: disable=unused-argument
        return None, None

    @mock.patch("bims.scripts.import_gbif_occurrences.submit_download", _submit_download_protocol_error)
    @mock.patch("bims.models.HarvestSession.objects.get", _mock_harvest_session_get)
    @mock.patch("bims.scripts.import_gbif_occurrences.preferences")
    def test_harvest_gbif_protocol_error(self, mock_preferences, mock_update_location_context):
        mock_preferences.SiteSetting.base_country_code = "ZA"
        mock_preferences.SiteSetting.site_boundary = None

        status = import_gbif_occurrences([self.taxonomy.id], session_id=1)
        self.assertEqual(status, "Download request failed")

    # ------------------------------------------------------------------
    # Coordinate update – site used only by the re-harvested record
    # ------------------------------------------------------------------
    @mock.patch("bims.scripts.import_gbif_occurrences.submit_download", _mock_submit_download)
    @mock.patch("bims.scripts.import_gbif_occurrences.get_ready_download_url", _mock_get_ready_download_url)
    @mock.patch("bims.scripts.import_gbif_occurrences.is_canceled", lambda *_: False)
    @mock.patch("bims.scripts.import_gbif_occurrences.create_dataset_from_gbif", _noop)
    @mock.patch("bims.models.HarvestSession.objects.get", _mock_harvest_session_get)
    @mock.patch("bims.scripts.import_gbif_occurrences.preferences")
    def test_coordinate_update_sole_site(self, mock_preferences, mock_update_location_context):
        """Re-harvest with corrected coords updates the existing site in place
        when no other records share it."""
        mock_preferences.SiteSetting.base_country_code = "ZA"
        mock_preferences.SiteSetting.site_boundary = None

        old_lon, old_lat = 19.815377, -34.580192
        new_lon, new_lat = 18.487083, -33.846342

        location_type, _ = LocationType.objects.get_or_create(
            name="PointObservation", allowed_geometry="POINT"
        )
        old_site = LocationSite.objects.create(
            geometry_point=Point(old_lon, old_lat, srid=4326),
            name="Old Site",
            location_type=location_type,
            harvested_from_gbif=True,
        )
        existing_record = BiologicalCollectionRecordF.create(
            upstream_id="9999001",
            site=old_site,
            taxonomy=self.taxonomy,
        )
        old_site_id = old_site.id

        zip_path = _create_single_row_zip(
            gbif_id="9999001",
            lon=str(new_lon),
            lat=str(new_lat),
            taxon_key=str(self.taxonomy.col_id),
        )
        with mock.patch(
            "bims.scripts.import_gbif_occurrences.download_archive",
            return_value=zip_path,
        ):
            import_gbif_occurrences([self.taxonomy.id], session_id=1)

        existing_record.refresh_from_db()
        updated_site = existing_record.site

        # Same site object must have been updated in place
        self.assertEqual(updated_site.id, old_site_id)
        self.assertAlmostEqual(updated_site.geometry_point.x, new_lon, places=5)
        self.assertAlmostEqual(updated_site.geometry_point.y, new_lat, places=5)
        self.assertAlmostEqual(updated_site.longitude, new_lon, places=5)
        self.assertAlmostEqual(updated_site.latitude, new_lat, places=5)

    # ------------------------------------------------------------------
    # Coordinate update – site shared by another record → new site created
    # ------------------------------------------------------------------
    @mock.patch("bims.scripts.import_gbif_occurrences.submit_download", _mock_submit_download)
    @mock.patch("bims.scripts.import_gbif_occurrences.get_ready_download_url", _mock_get_ready_download_url)
    @mock.patch("bims.scripts.import_gbif_occurrences.is_canceled", lambda *_: False)
    @mock.patch("bims.scripts.import_gbif_occurrences.create_dataset_from_gbif", _noop)
    @mock.patch("bims.models.HarvestSession.objects.get", _mock_harvest_session_get)
    @mock.patch("bims.scripts.import_gbif_occurrences.preferences")
    def test_coordinate_update_shared_site(self, mock_preferences, mock_update_location_context):
        """Re-harvest with corrected coords creates a new site when the old
        site is referenced by another record, and reassigns the occurrence."""
        mock_preferences.SiteSetting.base_country_code = "ZA"
        mock_preferences.SiteSetting.site_boundary = None

        old_lon, old_lat = 20.015752, 34.8084
        new_lon, new_lat = 19.956449, -34.8095

        location_type, _ = LocationType.objects.get_or_create(
            name="PointObservation", allowed_geometry="POINT"
        )
        shared_site = LocationSite.objects.create(
            geometry_point=Point(old_lon, old_lat, srid=4326),
            name="Shared Site",
            location_type=location_type,
            harvested_from_gbif=True,
        )
        # The record we are about to re-harvest
        existing_record = BiologicalCollectionRecordF.create(
            upstream_id="9999002",
            site=shared_site,
            taxonomy=self.taxonomy,
        )
        # Another record that shares the same site
        other_record = BiologicalCollectionRecordF.create(
            site=shared_site,
            taxonomy=self.taxonomy,
        )
        shared_site_id = shared_site.id

        zip_path = _create_single_row_zip(
            gbif_id="9999002",
            lon=str(new_lon),
            lat=str(new_lat),
            taxon_key=str(self.taxonomy.col_id),
        )
        with mock.patch(
            "bims.scripts.import_gbif_occurrences.download_archive",
            return_value=zip_path,
        ):
            import_gbif_occurrences([self.taxonomy.id], session_id=1)

        existing_record.refresh_from_db()
        other_record.refresh_from_db()
        new_site = existing_record.site

        # A brand-new site must have been created
        self.assertNotEqual(new_site.id, shared_site_id)
        self.assertAlmostEqual(new_site.geometry_point.x, new_lon, places=5)
        self.assertAlmostEqual(new_site.geometry_point.y, new_lat, places=5)
        self.assertTrue(new_site.site_code)

        # The other record must still point to the original shared site
        self.assertEqual(other_record.site_id, shared_site_id)

        # The original shared site must be unchanged
        shared_site.refresh_from_db()
        self.assertAlmostEqual(shared_site.geometry_point.x, old_lon, places=5)
        self.assertAlmostEqual(shared_site.geometry_point.y, old_lat, places=5)
