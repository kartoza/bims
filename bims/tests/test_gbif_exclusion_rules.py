"""Tests for GBIF harvest exclusion rules.

Covers:
- check_exclusion_rules() helper (all conditions)
- process_gbif_row() respects exclusion_rules parameter
- SiteSetting.gbif_exclusion_rules_effective property and defaults
"""
import uuid
from unittest import mock

from django_tenants.test.cases import FastTenantTestCase

from bims.models import BiologicalCollectionRecord, LocationSite
from bims.scripts.import_gbif_occurrences import (
    check_exclusion_rules,
    process_gbif_row,
)
from bims.tests.model_factories import (
    TaxonomyF, UserF, SourceReferenceDatabaseF, TaxonGroupF,
    BiologicalCollectionRecordF,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_create_dataset(*args, **kwargs):
    pass


def _base_row(**overrides):
    """Return a minimal valid DwC row dict, with optional field overrides."""
    row = {
        "gbifID": "excl-test-1",
        "decimalLongitude": "25.0",
        "decimalLatitude": "-28.0",
        "coordinateUncertaintyInMeters": "",
        "coordinatePrecision": "",
        "informationWithheld": "",
        "eventDate": "2021-06-01",
        "recordedBy": "Tester",
        "institutionCode": "TEST",
        "references": "http://example.org/excl-test-1",
        "locality": "Exclusion Test Site",
        "species": "Test species",
        "datasetKey": str(uuid.uuid4()),
        "taxonKey": "99999",
        "basisOfRecord": "OBSERVATION",
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Pure unit tests for check_exclusion_rules (no DB required)
# ---------------------------------------------------------------------------

class TestCheckExclusionRules(FastTenantTestCase):
    """Unit tests for the check_exclusion_rules() helper."""

    # --- not_empty -----------------------------------------------------------

    def test_not_empty_matches_when_field_has_value(self):
        rules = [{"field": "informationWithheld", "condition": "not_empty"}]
        matched, reason = check_exclusion_rules(
            {"informationWithheld": "Coordinate uncertainty increased to 29039m"},
            rules,
        )
        self.assertTrue(matched)
        self.assertIn("informationWithheld", reason)

    def test_not_empty_no_match_when_field_is_empty_string(self):
        rules = [{"field": "informationWithheld", "condition": "not_empty"}]
        matched, _ = check_exclusion_rules({"informationWithheld": ""}, rules)
        self.assertFalse(matched)

    def test_not_empty_no_match_when_field_is_missing(self):
        rules = [{"field": "informationWithheld", "condition": "not_empty"}]
        matched, _ = check_exclusion_rules({}, rules)
        self.assertFalse(matched)

    def test_not_empty_no_match_when_field_is_none(self):
        rules = [{"field": "informationWithheld", "condition": "not_empty"}]
        matched, _ = check_exclusion_rules({"informationWithheld": None}, rules)
        self.assertFalse(matched)

    # --- equals --------------------------------------------------------------

    def test_equals_matches_exact_value(self):
        rules = [{"field": "basisOfRecord", "condition": "equals", "value": "FOSSIL_SPECIMEN"}]
        matched, _ = check_exclusion_rules({"basisOfRecord": "FOSSIL_SPECIMEN"}, rules)
        self.assertTrue(matched)

    def test_equals_no_match_on_different_value(self):
        rules = [{"field": "basisOfRecord", "condition": "equals", "value": "FOSSIL_SPECIMEN"}]
        matched, _ = check_exclusion_rules({"basisOfRecord": "OBSERVATION"}, rules)
        self.assertFalse(matched)

    # --- contains ------------------------------------------------------------

    def test_contains_matches_substring(self):
        rules = [{"field": "informationWithheld", "condition": "contains", "value": "observer"}]
        matched, _ = check_exclusion_rules(
            {"informationWithheld": "Coordinate uncertainty increased at the request of the observer"},
            rules,
        )
        self.assertTrue(matched)

    def test_contains_no_match_when_substring_absent(self):
        rules = [{"field": "informationWithheld", "condition": "contains", "value": "observer"}]
        matched, _ = check_exclusion_rules({"informationWithheld": "other text"}, rules)
        self.assertFalse(matched)

    # --- greater_than --------------------------------------------------------

    def test_greater_than_matches_when_value_exceeds_threshold(self):
        rules = [{"field": "coordinateUncertaintyInMeters", "condition": "greater_than", "value": 10000}]
        matched, reason = check_exclusion_rules(
            {"coordinateUncertaintyInMeters": "29039"}, rules
        )
        self.assertTrue(matched)
        self.assertIn("coordinateUncertaintyInMeters", reason)

    def test_greater_than_no_match_when_value_below_threshold(self):
        rules = [{"field": "coordinateUncertaintyInMeters", "condition": "greater_than", "value": 10000}]
        matched, _ = check_exclusion_rules(
            {"coordinateUncertaintyInMeters": "30"}, rules
        )
        self.assertFalse(matched)

    def test_greater_than_no_match_on_exact_threshold(self):
        rules = [{"field": "coordinateUncertaintyInMeters", "condition": "greater_than", "value": 10000}]
        matched, _ = check_exclusion_rules(
            {"coordinateUncertaintyInMeters": "10000"}, rules
        )
        self.assertFalse(matched)

    def test_greater_than_no_match_when_field_is_non_numeric(self):
        rules = [{"field": "coordinateUncertaintyInMeters", "condition": "greater_than", "value": 10000}]
        matched, _ = check_exclusion_rules(
            {"coordinateUncertaintyInMeters": "not-a-number"}, rules
        )
        self.assertFalse(matched)

    def test_greater_than_no_match_when_field_is_empty(self):
        rules = [{"field": "coordinateUncertaintyInMeters", "condition": "greater_than", "value": 10000}]
        matched, _ = check_exclusion_rules(
            {"coordinateUncertaintyInMeters": ""}, rules
        )
        self.assertFalse(matched)

    # --- less_than -----------------------------------------------------------

    def test_less_than_matches_when_value_below_threshold(self):
        rules = [{"field": "coordinatePrecision", "condition": "less_than", "value": 0.00001}]
        matched, _ = check_exclusion_rules({"coordinatePrecision": "0.000001"}, rules)
        self.assertTrue(matched)

    def test_less_than_no_match_when_value_above_threshold(self):
        rules = [{"field": "coordinatePrecision", "condition": "less_than", "value": 0.00001}]
        matched, _ = check_exclusion_rules({"coordinatePrecision": "1.0"}, rules)
        self.assertFalse(matched)

    # --- edge cases ----------------------------------------------------------

    def test_empty_rules_list_never_matches(self):
        matched, reason = check_exclusion_rules(
            {"informationWithheld": "something", "coordinateUncertaintyInMeters": "99999"},
            [],
        )
        self.assertFalse(matched)
        self.assertEqual(reason, "")

    def test_first_matching_rule_stops_evaluation(self):
        """Only the first matched rule's description appears in the reason."""
        rules = [
            {"field": "informationWithheld", "condition": "not_empty", "description": "withheld"},
            {"field": "coordinateUncertaintyInMeters", "condition": "greater_than", "value": 10000, "description": "high uncertainty"},
        ]
        matched, reason = check_exclusion_rules(
            {"informationWithheld": "yes", "coordinateUncertaintyInMeters": "99999"},
            rules,
        )
        self.assertTrue(matched)
        self.assertIn("withheld", reason)
        self.assertNotIn("high uncertainty", reason)

    def test_second_rule_matches_when_first_does_not(self):
        rules = [
            {"field": "informationWithheld", "condition": "not_empty", "description": "withheld"},
            {"field": "coordinateUncertaintyInMeters", "condition": "greater_than", "value": 10000, "description": "high uncertainty"},
        ]
        matched, reason = check_exclusion_rules(
            {"informationWithheld": "", "coordinateUncertaintyInMeters": "29039"},
            rules,
        )
        self.assertTrue(matched)
        self.assertIn("high uncertainty", reason)

    def test_reason_includes_field_value(self):
        rules = [{"field": "informationWithheld", "condition": "not_empty"}]
        _, reason = check_exclusion_rules(
            {"informationWithheld": "obscured by iNaturalist"},
            rules,
        )
        self.assertIn("obscured by iNaturalist", reason)


# ---------------------------------------------------------------------------
# Integration tests: process_gbif_row respects exclusion_rules
# ---------------------------------------------------------------------------

@mock.patch("bims.models.location_site.update_location_site_context")
class TestProcessGbifRowExclusionRules(FastTenantTestCase):
    """Verify that process_gbif_row skips rows that match exclusion rules."""

    def setUp(self):
        self.taxonomy = TaxonomyF.create(col_id="99999")
        self.owner = UserF.create(username="excl_test_user")
        self.source_reference = SourceReferenceDatabaseF.create(source_name="Exclusion Test")
        self.taxon_group = TaxonGroupF.create()
        self.log_messages = []
        self.log = lambda msg: self.log_messages.append(msg)

    def tearDown(self):
        BiologicalCollectionRecord.objects.all().delete()
        LocationSite.objects.all().delete()
        self.taxonomy.delete()
        self.owner.delete()
        self.source_reference.delete()
        self.taxon_group.delete()

    def _call(self, row, rules):
        return process_gbif_row(
            row=row,
            owner=self.owner,
            source_reference=self.source_reference,
            source_collection="gbif",
            harvest_session=None,
            taxon_group=self.taxon_group,
            log=self.log,
            exclusion_rules=rules,
        )

    # --- informationWithheld -------------------------------------------------

    @mock.patch("bims.scripts.import_gbif_occurrences.create_dataset_from_gbif", _mock_create_dataset)
    def test_row_with_information_withheld_is_skipped(self, _mock_ctx):
        row = _base_row(
            gbifID="skip-withheld-1",
            informationWithheld="Coordinate uncertainty increased to 29039m at the request of the observer",
        )
        rules = [{"field": "informationWithheld", "condition": "not_empty"}]
        record, processed = self._call(row, rules)

        self.assertIsNone(record)
        self.assertFalse(processed)
        self.assertFalse(
            BiologicalCollectionRecord.objects.filter(upstream_id="skip-withheld-1").exists()
        )
        self.assertTrue(any("skip-withheld-1" in m for m in self.log_messages))

    @mock.patch("bims.scripts.import_gbif_occurrences.create_dataset_from_gbif", _mock_create_dataset)
    def test_row_without_information_withheld_is_accepted(self, _mock_ctx):
        row = _base_row(gbifID="accept-no-withheld-1", informationWithheld="")
        rules = [{"field": "informationWithheld", "condition": "not_empty"}]
        record, processed = self._call(row, rules)

        self.assertTrue(processed)

    # --- coordinateUncertaintyInMeters ---------------------------------------

    @mock.patch("bims.scripts.import_gbif_occurrences.create_dataset_from_gbif", _mock_create_dataset)
    def test_row_with_high_uncertainty_is_skipped(self, _mock_ctx):
        row = _base_row(
            gbifID="skip-uncertainty-1",
            coordinateUncertaintyInMeters="29039",
        )
        rules = [{"field": "coordinateUncertaintyInMeters", "condition": "greater_than", "value": 10000}]
        record, processed = self._call(row, rules)

        self.assertIsNone(record)
        self.assertFalse(processed)
        self.assertFalse(
            BiologicalCollectionRecord.objects.filter(upstream_id="skip-uncertainty-1").exists()
        )

    @mock.patch("bims.scripts.import_gbif_occurrences.create_dataset_from_gbif", _mock_create_dataset)
    def test_row_with_acceptable_uncertainty_is_accepted(self, _mock_ctx):
        row = _base_row(
            gbifID="accept-uncertainty-1",
            coordinateUncertaintyInMeters="30",
        )
        rules = [{"field": "coordinateUncertaintyInMeters", "condition": "greater_than", "value": 10000}]
        record, processed = self._call(row, rules)

        self.assertTrue(processed)

    # --- default rules (both informationWithheld + coordinateUncertaintyInMeters) ---

    @mock.patch("bims.scripts.import_gbif_occurrences.create_dataset_from_gbif", _mock_create_dataset)
    def test_default_rules_skip_withheld(self, _mock_ctx):
        from bims.models.site_setting import SiteSetting

        row = _base_row(
            gbifID="default-withheld-1",
            informationWithheld="Coordinate uncertainty increased to 29039m",
        )
        record, processed = self._call(row, SiteSetting.GBIF_DEFAULT_EXCLUSION_RULES)

        self.assertIsNone(record)
        self.assertFalse(processed)


    @mock.patch("bims.scripts.import_gbif_occurrences.create_dataset_from_gbif", _mock_create_dataset)
    def test_default_rules_accept_clean_record(self, _mock_ctx):
        from bims.models.site_setting import SiteSetting

        row = _base_row(
            gbifID="default-clean-1",
            informationWithheld="",
            coordinateUncertaintyInMeters="30",
        )
        record, processed = self._call(row, SiteSetting.GBIF_DEFAULT_EXCLUSION_RULES)

        self.assertTrue(processed)

    # --- previously harvested record is removed when rule matches ------------

    @mock.patch("bims.scripts.import_gbif_occurrences.create_dataset_from_gbif", _mock_create_dataset)
    def test_existing_record_deleted_when_exclusion_rule_matches(self, _mock_ctx):
        """A record that was previously harvested must be removed when it now matches an exclusion rule."""
        upstream_id = "delete-on-exclude-1"
        existing = BiologicalCollectionRecordF.create(
            upstream_id=upstream_id,
            taxonomy=self.taxonomy,
            owner=self.owner,
        )
        self.assertTrue(
            BiologicalCollectionRecord.objects.filter(upstream_id=upstream_id).exists()
        )

        row = _base_row(
            gbifID=upstream_id,
            informationWithheld="Coordinate uncertainty increased to 29039m",
        )
        rules = [{"field": "informationWithheld", "condition": "not_empty"}]
        record, processed = self._call(row, rules)

        self.assertIsNone(record)
        self.assertFalse(processed)
        self.assertFalse(
            BiologicalCollectionRecord.objects.filter(upstream_id=upstream_id).exists()
        )
        self.assertTrue(
            any("delete-on-exclude-1" in str(m) and "Removed" in str(m) for m in self.log_messages)
        )

    @mock.patch("bims.scripts.import_gbif_occurrences.create_dataset_from_gbif", _mock_create_dataset)
    def test_no_existing_record_still_skipped_cleanly(self, _mock_ctx):
        """When no prior record exists and an exclusion rule matches, the skip is still clean."""
        upstream_id = "skip-no-prior-record-1"
        row = _base_row(
            gbifID=upstream_id,
            informationWithheld="Coordinate uncertainty increased to 29039m",
        )
        rules = [{"field": "informationWithheld", "condition": "not_empty"}]
        record, processed = self._call(row, rules)

        self.assertIsNone(record)
        self.assertFalse(processed)
        self.assertFalse(
            BiologicalCollectionRecord.objects.filter(upstream_id=upstream_id).exists()
        )

    # --- no rules applied when exclusion_rules is None or empty --------------

    @mock.patch("bims.scripts.import_gbif_occurrences.create_dataset_from_gbif", _mock_create_dataset)
    def test_none_rules_does_not_filter(self, _mock_ctx):
        row = _base_row(
            gbifID="no-rules-withheld-1",
            informationWithheld="Coordinate uncertainty increased to 29039m",
        )
        record, processed = self._call(row, None)
        self.assertTrue(processed)

    @mock.patch("bims.scripts.import_gbif_occurrences.create_dataset_from_gbif", _mock_create_dataset)
    def test_empty_rules_does_not_filter(self, _mock_ctx):
        row = _base_row(
            gbifID="empty-rules-withheld-1",
            informationWithheld="Coordinate uncertainty increased to 29039m",
        )
        record, processed = self._call(row, [])
        self.assertTrue(processed)


# ---------------------------------------------------------------------------
# SiteSetting.gbif_exclusion_rules_effective
# ---------------------------------------------------------------------------

@mock.patch("bims.models.location_site.update_location_site_context")
class TestSiteSettingExclusionRules(FastTenantTestCase):
    """Verify the gbif_exclusion_rules_effective property and default rules."""

    def _make_setting(self, rules_value):
        from bims.models.site_setting import SiteSetting
        s = SiteSetting()
        s.gbif_harvest_exclusion_rules = rules_value
        return s

    def test_default_rules_contain_information_withheld(self, _mock_ctx):
        from bims.models.site_setting import SiteSetting
        fields = [r["field"] for r in SiteSetting.GBIF_DEFAULT_EXCLUSION_RULES]
        self.assertIn("informationWithheld", fields)

    def test_effective_falls_back_to_defaults_when_null(self, _mock_ctx):
        from bims.models.site_setting import SiteSetting
        s = self._make_setting(None)
        self.assertEqual(s.gbif_exclusion_rules_effective, SiteSetting.GBIF_DEFAULT_EXCLUSION_RULES)

    def test_effective_falls_back_to_defaults_when_not_a_list(self, _mock_ctx):
        from bims.models.site_setting import SiteSetting
        s = self._make_setting({"field": "bad"})
        self.assertEqual(s.gbif_exclusion_rules_effective, SiteSetting.GBIF_DEFAULT_EXCLUSION_RULES)

    def test_effective_returns_configured_rules_when_set(self, _mock_ctx):
        custom = [{"field": "myField", "condition": "not_empty"}]
        s = self._make_setting(custom)
        self.assertEqual(s.gbif_exclusion_rules_effective, custom)

    def test_effective_returns_empty_list_when_explicitly_cleared(self, _mock_ctx):
        """An empty list means 'no filtering' — that's a deliberate admin choice."""
        s = self._make_setting([])
        self.assertEqual(s.gbif_exclusion_rules_effective, [])
