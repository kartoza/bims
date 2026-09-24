from django_tenants.test.cases import FastTenantTestCase

from bims.scripts.species_keys import (
    TAXON, TAXON_RANK, TAXONOMIC_STATUS, AUTHORS, ACCEPTED_TAXON,
    PHYLUM, SUBPHYLUM, CLASS, SUBCLASS, ON_GBIF, GBIF_LINK, SUBGENUS,
    FADA_ID
)
from bims.scripts.taxa_validation import TaxaValidator
from bims.tests.model_factories import UploadSessionF


class TestTaxaValidatorHomonymy(FastTenantTestCase):
    """Tests for TaxaValidator's homonymy-vs-accepted/synonym detection."""

    def setUp(self):
        self.upload_session = UploadSessionF.create()
        self.validator = TaxaValidator(self.upload_session)

    def _make_row(self, name, rank, status, author, accepted_taxon=''):
        return {
            TAXON: name,
            TAXON_RANK: rank,
            TAXONOMIC_STATUS: status,
            AUTHORS: author,
            ACCEPTED_TAXON: accepted_taxon,
        }

    def test_accepted_and_synonym_same_name_no_warning(self):
        """An accepted taxon and its synonym sharing a name/rank but
        different authors should not trigger the homonymy warning."""
        rows = [
            self._make_row(
                'Gordius lineatus', 'Species', 'Accepted', 'Leidy, 1851'),
            self._make_row(
                'Gordius lineatus', 'Species', 'Synonym', 'Villot, 1886',
                accepted_taxon='Gordius lineatus'),
        ]

        self.validator._first_pass_collect_keys(rows)

        messages_row1 = self.validator._validate_row(rows[0], row_number=2)
        messages_row2 = self.validator._validate_row(rows[1], row_number=3)

        self.assertFalse(
            any('Homonymy' in m for m in messages_row1 + messages_row2)
        )

    def test_two_accepted_same_name_different_author_warns(self):
        """Two accepted rows sharing a name/rank with different authors
        is a genuine ambiguity and should still warn."""
        rows = [
            self._make_row(
                'Gordius lineatus', 'Species', 'Accepted', 'Leidy, 1851'),
            self._make_row(
                'Gordius lineatus', 'Species', 'Accepted', 'Villot, 1886'),
        ]

        self.validator._first_pass_collect_keys(rows)

        messages_row1 = self.validator._validate_row(rows[0], row_number=2)
        messages_row2 = self.validator._validate_row(rows[1], row_number=3)

        self.assertTrue(
            any('Homonymy' in m for m in messages_row1)
        )
        self.assertTrue(
            any('Homonymy' in m for m in messages_row2)
        )

    def test_two_synonyms_no_accepted_same_name_warns(self):
        """Two synonym rows sharing a name/rank with no accepted taxon
        among them is ambiguous and should still warn."""
        rows = [
            self._make_row(
                'Gordius lineatus', 'Species', 'Synonym', 'Leidy, 1851',
                accepted_taxon='Some Other Name'),
            self._make_row(
                'Gordius lineatus', 'Species', 'Synonym', 'Villot, 1886',
                accepted_taxon='Some Other Name'),
        ]

        self.validator._first_pass_collect_keys(rows)

        messages_row1 = self.validator._validate_row(rows[0], row_number=2)

        self.assertTrue(
            any('Homonymy' in m for m in messages_row1)
        )

    def test_accepted_with_two_synonyms_no_warning(self):
        """One accepted taxon with multiple synonyms of it sharing the
        same name/rank should not warn either."""
        rows = [
            self._make_row(
                'Gordius lineatus', 'Species', 'Accepted', 'Leidy, 1851'),
            self._make_row(
                'Gordius lineatus', 'Species', 'Synonym', 'Villot, 1886',
                accepted_taxon='Gordius lineatus'),
            self._make_row(
                'Gordius lineatus', 'Species', 'Synonym', 'Smith, 1900',
                accepted_taxon='Gordius lineatus'),
        ]

        self.validator._first_pass_collect_keys(rows)

        messages = []
        for i, row in enumerate(rows):
            messages.extend(self.validator._validate_row(row, row_number=i + 2))

        self.assertFalse(any('Homonymy' in m for m in messages))

    def test_synonyms_differing_only_by_subgenus_describe_subgenus(self):
        """Rows with the same name, rank and author that differ only in
        subgenus are reported as a subgenus difference, not an author one."""
        rows = [
            self._make_row(
                'Candonopsis williami', 'Species', 'Synonym',
                'Karanovic & Marmonier, 2002',
                accepted_taxon='Abcandonopsis williami'),
            dict(self._make_row(
                'Candonopsis williami', 'Species', 'Synonym',
                'Karanovic & Marmonier, 2002',
                accepted_taxon='Abcandonopsis williami'),
                **{SUBGENUS: 'Abcandonopsis'}),
        ]

        self.validator._first_pass_collect_keys(rows)

        messages = self.validator._validate_row(rows[0], row_number=2)

        homonymy = [m for m in messages if 'Homonymy' in m]
        self.assertEqual(len(homonymy), 1)
        self.assertIn('a different subgenus', homonymy[0])
        self.assertNotIn('author', homonymy[0])
        self.assertIn('uploaded as separate taxa', homonymy[0])
        self.assertFalse(any('Duplicate taxon name' in m for m in messages))

    def test_other_rows_reported_with_spreadsheet_row_numbers(self):
        """Row references point at the spreadsheet row (header is row 1)."""
        rows = [
            self._make_row(
                'Gordius lineatus', 'Species', 'Accepted', 'Leidy, 1851'),
            self._make_row(
                'Gordius lineatus', 'Species', 'Accepted', 'Villot, 1886'),
        ]

        self.validator._first_pass_collect_keys(rows)

        messages_row2 = self.validator._validate_row(rows[0], row_number=2)
        messages_row3 = self.validator._validate_row(rows[1], row_number=3)

        self.assertTrue(any('also in row(s) 3)' in m for m in messages_row2))
        self.assertTrue(any('also in row(s) 2)' in m for m in messages_row3))

    def test_author_and_subgenus_both_differ(self):
        rows = [
            self._make_row(
                'Candonopsis williami', 'Species', 'Synonym', 'Smith, 1900',
                accepted_taxon='Abcandonopsis williami'),
            dict(self._make_row(
                'Candonopsis williami', 'Species', 'Synonym',
                'Karanovic & Marmonier, 2002',
                accepted_taxon='Abcandonopsis williami'),
                **{SUBGENUS: 'Abcandonopsis'}),
        ]

        self.validator._first_pass_collect_keys(rows)

        messages = self.validator._validate_row(rows[1], row_number=3)

        self.assertTrue(any(
            'different author(s) and subgenus' in m and
            'Verify that one is the accepted name' in m
            for m in messages
        ))

    def test_author_only_difference_keeps_accepted_synonym_advice(self):
        rows = [
            self._make_row(
                'Gordius lineatus', 'Species', 'Accepted', 'Leidy, 1851'),
            self._make_row(
                'Gordius lineatus', 'Species', 'Accepted', 'Villot, 1886'),
        ]

        self.validator._first_pass_collect_keys(rows)

        messages = self.validator._validate_row(rows[0], row_number=2)

        homonymy = [m for m in messages if 'Homonymy' in m]
        self.assertEqual(len(homonymy), 1)
        self.assertIn('appears with different author(s) (', homonymy[0])
        self.assertNotIn('subgenus', homonymy[0])
        self.assertIn('Verify that one is the accepted name', homonymy[0])

    def test_duplicate_ids_reported_with_spreadsheet_row_numbers(self):
        rows = [
            dict(self._make_row(
                'Alicenula furcabdominis', 'Species', 'Accepted', 'Keyser, 1976'),
                **{GBIF_LINK: 'https://www.gbif.org/taxon/BS37', FADA_ID: '500176'}),
            dict(self._make_row(
                'Darwinula furcabdominis', 'Species', 'Synonym', 'Keyser, 1976',
                accepted_taxon='Alicenula furcabdominis'),
                **{GBIF_LINK: 'https://www.gbif.org/taxon/BS37', FADA_ID: '500176'}),
        ]
        self.validator._validate_gbif_key = lambda row, key: []

        self.validator._first_pass_collect_keys(rows)

        messages = self.validator._validate_row(rows[0], row_number=2)

        self.assertIn(
            'ERROR: Duplicate GBIF col_id BS37 (also in row(s) 3)', messages)
        self.assertIn('ERROR: Duplicate FADA ID 500176 (also in row(s) 3)', messages)

    def test_same_name_rank_author_still_flagged_as_duplicate(self):
        """Exact duplicates (same name, rank, and author) must still be
        flagged as an ERROR, not silently suppressed."""
        rows = [
            self._make_row(
                'Gordius lineatus', 'Species', 'Accepted', 'Leidy, 1851'),
            self._make_row(
                'Gordius lineatus', 'Species', 'Accepted', 'Leidy, 1851'),
        ]

        self.validator._first_pass_collect_keys(rows)

        messages_row1 = self.validator._validate_row(rows[0], row_number=2)

        self.assertTrue(
            any('Duplicate taxon name' in m for m in messages_row1)
        )


class TestTaxaValidatorParentNameConflict(FastTenantTestCase):
    """Tests for TaxaValidator's classification-chain name conflict check."""

    def setUp(self):
        self.upload_session = UploadSessionF.create()
        self.validator = TaxaValidator(self.upload_session)

    def test_phylum_same_name_as_class_errors(self):
        row = {
            PHYLUM: 'Gastrotricha',
            CLASS: 'Gastrotricha',
            TAXON: 'Gastrotricha',
            TAXON_RANK: 'Class',
        }

        messages = self.validator._check_parent_name_conflict(row)

        self.assertTrue(
            any(
                "Parent 'Gastrotricha' (PHYLUM) cannot have the same "
                "name as 'Gastrotricha' (CLASS)" in m
                for m in messages
            )
        )

    def test_subclass_same_name_as_class_is_allowed(self):
        row = {
            CLASS: 'Insecta',
            SUBCLASS: 'Insecta',
        }

        messages = self.validator._check_parent_name_conflict(row)

        self.assertFalse(messages)

    def test_non_adjacent_ranks_with_same_name_via_gap_still_checked(self):
        """When an intermediate rank column is blank, the check should
        still compare against the nearest filled ancestor."""
        row = {
            PHYLUM: 'Gastrotricha',
            SUBPHYLUM: '',
            CLASS: 'Gastrotricha',
        }

        messages = self.validator._check_parent_name_conflict(row)

        self.assertTrue(
            any('PHYLUM' in m and 'CLASS' in m for m in messages)
        )

    def test_different_names_no_conflict(self):
        row = {
            PHYLUM: 'Gastrotricha',
            CLASS: 'Chaetonotida',
        }

        messages = self.validator._check_parent_name_conflict(row)

        self.assertFalse(messages)


class TestTaxaValidatorOnGbifWithoutLink(FastTenantTestCase):
    """Tests for the 'On GBIF' marked without a GBIF link warning."""

    def setUp(self):
        self.upload_session = UploadSessionF.create()
        self.validator = TaxaValidator(self.upload_session)

    def test_on_gbif_yes_without_link_warns(self):
        row = {ON_GBIF: 'Yes', GBIF_LINK: ''}

        messages = self.validator._check_on_gbif_without_link(row, gbif_key=None)

        self.assertTrue(any('taxon name' in m for m in messages))

    def test_on_gbif_yes_with_link_no_warning(self):
        row = {ON_GBIF: 'Yes', GBIF_LINK: 'https://www.gbif.org/species/12345'}

        messages = self.validator._check_on_gbif_without_link(row, gbif_key='12345')

        self.assertFalse(messages)

    def test_on_gbif_no_without_link_no_warning(self):
        row = {ON_GBIF: 'No', GBIF_LINK: ''}

        messages = self.validator._check_on_gbif_without_link(row, gbif_key=None)

        self.assertFalse(messages)


class TestTaxaValidatorGbifLinkFormat(FastTenantTestCase):
    """Tests for validating GBIF links point at a Catalogue of Life taxon."""

    def setUp(self):
        self.upload_session = UploadSessionF.create()
        self.validator = TaxaValidator(self.upload_session)

    def test_no_link_no_error(self):
        row = {GBIF_LINK: ''}

        messages = self.validator._check_gbif_link_format(row)

        self.assertFalse(messages)

    def test_valid_col_taxon_link_no_error(self):
        row = {GBIF_LINK: 'https://www.gbif.org/taxon/ABC123'}

        messages = self.validator._check_gbif_link_format(row)

        self.assertFalse(messages)

    def test_legacy_species_link_rejected(self):
        row = {GBIF_LINK: 'https://www.gbif.org/species/99999'}

        messages = self.validator._check_gbif_link_format(row)

        self.assertTrue(any('99999' in m and 'not accepted' in m for m in messages))

    def test_numeric_only_taxon_key_rejected(self):
        """Even under /taxon/, a purely numeric key is suspicious -
        Catalogue of Life taxon keys are not legacy numeric GBIF keys."""
        row = {GBIF_LINK: 'https://www.gbif.org/taxon/99999'}

        messages = self.validator._check_gbif_link_format(row)

        self.assertTrue(any('legacy GBIF taxon key' in m for m in messages))

    def test_unrecognized_gbif_link_format_rejected(self):
        row = {GBIF_LINK: 'https://www.gbif.org/somethingelse/ABC123'}

        messages = self.validator._check_gbif_link_format(row)

        self.assertTrue(any('not recognized' in m for m in messages))
