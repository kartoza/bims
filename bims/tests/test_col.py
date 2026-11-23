# coding: utf-8
"""Tests for the COL (Catalogue of Life) resolver utility."""
from unittest.mock import MagicMock, patch

from django.test import TestCase

from bims.utils.col import COL_CHECKLIST_KEY, resolve_col_id


def _make_response(status_code=200, json_data=None, raises=None):
    mock = MagicMock()
    mock.status_code = status_code
    mock.ok = status_code == 200
    if raises:
        mock.json.side_effect = raises
    else:
        mock.json.return_value = json_data or {}
    return mock


def _match_payload(usage_key='Q2M4', match_type='EXACT', canonical_name=None,
                    rank=None, **extra):
    payload = {'usage': {'key': usage_key}, 'matchType': match_type, **extra}
    if canonical_name is not None:
        payload['usage']['canonicalName'] = canonical_name
    if rank is not None:
        payload['usage']['rank'] = rank
    return payload


class ResolveColIdTest(TestCase):

    # --- happy path: gbif_key ---

    @patch('bims.utils.col.gbif_get')
    def test_returns_col_id_from_usage_key(self, mock_get):
        """usage.key from the v2 match response is returned as the COL id."""
        mock_get.return_value = _make_response(json_data=_match_payload('Q2M4'))
        col_id, payload = resolve_col_id(1427067)
        self.assertEqual(col_id, 'Q2M4')
        self.assertIsNotNone(payload)

    @patch('bims.utils.col.gbif_get')
    def test_col_id_is_always_str(self, mock_get):
        """col_id is always returned as a string."""
        mock_get.return_value = _make_response(json_data=_match_payload('CRLT8'))
        col_id, _ = resolve_col_id(12345)
        self.assertIsInstance(col_id, str)

    @patch('bims.utils.col.gbif_get')
    def test_correct_params_for_gbif_key(self, mock_get):
        """Request uses the COL checklist key and gbif: prefix on the taxon key."""
        mock_get.return_value = _make_response(json_data=_match_payload('Q2M4'))
        resolve_col_id(1427067)
        _, kwargs = mock_get.call_args
        params = kwargs.get('params', {})
        self.assertEqual(params['checklistKey'], COL_CHECKLIST_KEY)
        self.assertEqual(params['scientificNameID'], 'gbif:1427067')
        self.assertNotIn('scientificName', params)

    @patch('bims.utils.col.gbif_get')
    def test_accepts_string_gbif_key(self, mock_get):
        """A string gbif_key is accepted; the gbif: prefix is still correct."""
        mock_get.return_value = _make_response(json_data=_match_payload('Q2M4'))
        col_id, _ = resolve_col_id('1427067')
        self.assertEqual(col_id, 'Q2M4')
        _, kwargs = mock_get.call_args
        self.assertEqual(kwargs['params']['scientificNameID'], 'gbif:1427067')

    # --- happy path: canonical_name fallback ---

    @patch('bims.utils.col.gbif_get')
    def test_resolves_by_canonical_name_when_no_gbif_key(self, mock_get):
        """When gbif_key is absent, scientificName param is set to canonical_name."""
        mock_get.return_value = _make_response(json_data=_match_payload('CRLT8'))
        col_id, _ = resolve_col_id(None, canonical_name='Felidae')
        self.assertEqual(col_id, 'CRLT8')
        _, kwargs = mock_get.call_args
        params = kwargs.get('params', {})
        self.assertEqual(params['scientificName'], 'Felidae')
        self.assertNotIn('scientificNameID', params)

    # --- canonical name validation ---

    @patch('bims.utils.col.gbif_get')
    def test_canonical_name_match_returns_gbif_key_result(self, mock_get):
        """When canonical_name matches the API response, gbif_key col_id is returned."""
        mock_get.return_value = _make_response(
            json_data=_match_payload('Q2M4', canonical_name='Felidae')
        )
        col_id, _ = resolve_col_id(1427067, canonical_name='Felidae')
        self.assertEqual(col_id, 'Q2M4')
        mock_get.assert_called_once()

    @patch('bims.utils.col.gbif_get')
    def test_canonical_name_match_is_case_insensitive(self, mock_get):
        """Canonical name comparison ignores case."""
        mock_get.return_value = _make_response(
            json_data=_match_payload('Q2M4', canonical_name='felidae')
        )
        col_id, _ = resolve_col_id(1427067, canonical_name='Felidae')
        self.assertEqual(col_id, 'Q2M4')

    @patch('bims.utils.col.gbif_get')
    def test_canonical_name_mismatch_falls_back_to_canonical_name_lookup(self, mock_get):
        """When canonical_name doesn't match the API response, falls back to scientificName lookup."""
        gbif_response = _make_response(
            json_data=_match_payload('Q2M4', canonical_name='Canidae')
        )
        name_response = _make_response(
            json_data=_match_payload('CRLT8', canonical_name='Felidae')
        )
        mock_get.side_effect = [gbif_response, name_response]

        col_id, _ = resolve_col_id(1427067, canonical_name='Felidae')
        self.assertEqual(col_id, 'CRLT8')
        self.assertEqual(mock_get.call_count, 2)
        first_params = mock_get.call_args_list[0][1]['params']
        second_params = mock_get.call_args_list[1][1]['params']
        self.assertIn('scientificNameID', first_params)
        self.assertEqual(second_params['scientificName'], 'Felidae')

    @patch('bims.utils.col.gbif_get')
    def test_canonical_name_mismatch_no_fallback_returns_none(self, mock_get):
        """Canonical mismatch with no canonical_name to fall back on returns None."""
        mock_get.return_value = _make_response(
            json_data=_match_payload('Q2M4', canonical_name='Canidae')
        )
        # canonical_name empty so mismatch check is skipped - result returned as-is
        col_id, _ = resolve_col_id(1427067)
        self.assertEqual(col_id, 'Q2M4')
        mock_get.assert_called_once()

    @patch('bims.utils.col.gbif_get')
    def test_no_canonical_name_skips_validation(self, mock_get):
        """When canonical_name is empty, gbif_key result is returned without validation."""
        mock_get.return_value = _make_response(
            json_data=_match_payload('Q2M4', canonical_name='Canidae')
        )
        col_id, _ = resolve_col_id(1427067)
        self.assertEqual(col_id, 'Q2M4')
        mock_get.assert_called_once()

    # --- rank validation (canonical name fallback) ---

    @patch('bims.utils.col.gbif_get')
    def test_rank_match_returns_name_lookup_result(self, mock_get):
        """When rank matches the API response, the name-lookup col_id is returned."""
        mock_get.return_value = _make_response(
            json_data=_match_payload('CRLT8', canonical_name='Felidae', rank='FAMILY')
        )
        col_id, _ = resolve_col_id(None, canonical_name='Felidae', rank='FAMILY')
        self.assertEqual(col_id, 'CRLT8')

    @patch('bims.utils.col.gbif_get')
    def test_rank_match_is_case_insensitive(self, mock_get):
        """Rank comparison ignores case."""
        mock_get.return_value = _make_response(
            json_data=_match_payload('CRLT8', canonical_name='Felidae', rank='family')
        )
        col_id, _ = resolve_col_id(None, canonical_name='Felidae', rank='FAMILY')
        self.assertEqual(col_id, 'CRLT8')

    @patch('bims.utils.col.gbif_get')
    def test_rank_mismatch_discards_name_lookup_result(self, mock_get):
        """When rank doesn't match the API response, the name-lookup match is discarded."""
        mock_get.return_value = _make_response(
            json_data=_match_payload('CRLT8', canonical_name='Felidae', rank='GENUS')
        )
        col_id, _ = resolve_col_id(None, canonical_name='Felidae', rank='SPECIES')
        self.assertIsNone(col_id)

    @patch('bims.utils.col.gbif_get')
    def test_no_rank_skips_rank_validation(self, mock_get):
        """When rank is not provided, the name-lookup result is returned without validation."""
        mock_get.return_value = _make_response(
            json_data=_match_payload('CRLT8', canonical_name='Felidae', rank='GENUS')
        )
        col_id, _ = resolve_col_id(None, canonical_name='Felidae')
        self.assertEqual(col_id, 'CRLT8')

    @patch('bims.utils.col.gbif_get')
    def test_rank_mismatch_after_gbif_key_fallback(self, mock_get):
        """Rank validation also applies to the name lookup reached via gbif_key fallback."""
        gbif_response = _make_response(
            json_data=_match_payload('Q2M4', canonical_name='Canidae')
        )
        name_response = _make_response(
            json_data=_match_payload('CRLT8', canonical_name='Felidae', rank='GENUS')
        )
        mock_get.side_effect = [gbif_response, name_response]

        col_id, _ = resolve_col_id(1427067, canonical_name='Felidae', rank='SPECIES')
        self.assertIsNone(col_id)
        self.assertEqual(mock_get.call_count, 2)

    # --- no match ---

    @patch('bims.utils.col.gbif_get')
    def test_returns_none_for_match_type_none(self, mock_get):
        """matchType NONE means no COL record found; returns (None, payload)."""
        mock_get.return_value = _make_response(
            json_data={'matchType': 'NONE', 'scientificName': 'Unknown'}
        )
        col_id, payload = resolve_col_id(99999)
        self.assertIsNone(col_id)
        self.assertIsNotNone(payload)

    @patch('bims.utils.col.gbif_get')
    def test_returns_none_when_usage_key_missing(self, mock_get):
        """A 200 response without usage.key returns (None, payload)."""
        mock_get.return_value = _make_response(
            json_data={'matchType': 'FUZZY', 'scientificName': 'Felidae'}
        )
        col_id, payload = resolve_col_id(12345)
        self.assertIsNone(col_id)
        self.assertIsNotNone(payload)

    # --- error responses ---

    @patch('bims.utils.col.gbif_get')
    def test_returns_none_on_server_error(self, mock_get):
        """5xx responses return (None, None)."""
        mock_get.return_value = _make_response(status_code=500)
        col_id, payload = resolve_col_id(12345)
        self.assertIsNone(col_id)
        self.assertIsNone(payload)

    @patch('bims.utils.col.gbif_get')
    def test_returns_none_on_invalid_json(self, mock_get):
        """Malformed JSON response returns (None, None) without raising."""
        import simplejson
        mock_get.return_value = _make_response(
            raises=simplejson.errors.JSONDecodeError('', '', 0)
        )
        col_id, payload = resolve_col_id(12345)
        self.assertIsNone(col_id)
        self.assertIsNone(payload)

    @patch('bims.utils.col.gbif_get')
    def test_returns_none_on_connection_error(self, mock_get):
        """Network failures return (None, None) without raising."""
        import requests as req
        mock_get.side_effect = req.exceptions.ConnectionError('timeout')
        col_id, payload = resolve_col_id(12345)
        self.assertIsNone(col_id)
        self.assertIsNone(payload)

    # --- edge cases ---

    @patch('bims.utils.col.gbif_get')
    def test_returns_none_when_both_inputs_absent(self, mock_get):
        """No gbif_key and no canonical_name short-circuits without any HTTP call."""
        col_id, payload = resolve_col_id(None)
        self.assertIsNone(col_id)
        self.assertIsNone(payload)
        mock_get.assert_not_called()

    @patch('bims.utils.col.gbif_get')
    def test_returns_none_for_zero_gbif_key_and_no_name(self, mock_get):
        """0 gbif_key with no canonical_name short-circuits without any HTTP call."""
        col_id, payload = resolve_col_id(0)
        self.assertIsNone(col_id)
        self.assertIsNone(payload)
        mock_get.assert_not_called()
