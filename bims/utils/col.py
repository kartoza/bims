# coding: utf-8
import logging
import requests
import simplejson
from django.conf import settings

from bims.utils.gbif_http import gbif_get

logger = logging.getLogger(__name__)

GBIF_API_V2 = getattr(settings, 'GBIF_API_V2_BASE_URL', 'https://api.gbif.org/v2')

# The COL XR checklist key on GBIF ChecklistBank.
COL_CHECKLIST_KEY = '7ddf754f-d193-4cc9-b351-99906754a03b'

_MATCH_URL = f'{GBIF_API_V2}/species/match'


def _call_match_api(params, label=''):
    """Make one request to the GBIF v2 species/match endpoint. Returns (col_id, payload)."""
    try:
        response = gbif_get(_MATCH_URL, params=params)
    except requests.exceptions.RequestException as exc:
        logger.warning('COL resolver: connection error for %s: %s', label, exc)
        return None, None

    if not response.ok:
        logger.warning(
            'COL resolver: unexpected status %s for %s',
            response.status_code, label,
        )
        return None, None

    try:
        payload = response.json()
    except (simplejson.errors.JSONDecodeError, ValueError) as exc:
        logger.warning('COL resolver: invalid JSON for %s: %s', label, exc)
        return None, None

    if payload.get('matchType') == 'NONE':
        logger.info('COL resolver: no match found for %s', label)
        return None, payload

    col_id = payload.get('usage', {}).get('key')
    if not col_id:
        logger.warning('COL resolver: usageKey missing in response for %s', label)
        return None, payload

    return str(col_id), payload


def resolve_col_id(gbif_key, canonical_name='', rank=None):
    """
    Resolve a taxon to its COL XR identifier.

    Strategy:
    1. If gbif_key is provided, query by scientificNameID=gbif:{gbif_key}.
       If canonical_name is also provided, verify that payload["usage"]["canonicalName"]
       matches it (case-insensitive). A mismatch discards the result and falls through.
    2. If no col_id yet and canonical_name is provided, query by scientificName=canonical_name.
       Because a bare name search can match a different rank (e.g. a genus name
       colliding with a species epithet), if *rank* is also provided the result is
       verified against payload["usage"]["rank"] (case-insensitive). A mismatch
       discards the result.

    Returns (col_id, payload) where col_id is a str and payload is the full API
    response dict from whichever lookup succeeded. Returns (None, None) on no match,
    network errors, or bad responses.

    :param gbif_key: int or str or None - legacy GBIF backbone taxon key
    :param canonical_name: str - canonical name (without author) used for validation
                           and as scientificName fallback
    :param rank: str or None - expected taxon rank (e.g. "SPECIES"), used to
                validate name-based matches
    :return: (col_id: str | None, payload: dict | None)
    """
    if not gbif_key and not canonical_name:
        return None, None

    col_id = None
    payload = None

    if gbif_key:
        params = {
            'checklistKey': COL_CHECKLIST_KEY,
            'scientificNameID': f'gbif:{gbif_key}',
        }
        col_id, payload = _call_match_api(params, label=f'key {gbif_key}')
        if col_id and canonical_name:
            canonical_from_api = payload.get('usage', {}).get('canonicalName') or ''
            if canonical_from_api.strip().lower() != canonical_name.strip().lower():
                logger.info(
                    'COL resolver: canonical name mismatch for gbif_key %s '
                    '(api=%r, expected=%r), falling back to canonical name lookup',
                    gbif_key, canonical_from_api, canonical_name,
                )
                col_id = None

    if not col_id and canonical_name:
        params = {
            'checklistKey': COL_CHECKLIST_KEY,
            'scientificName': canonical_name,
        }
        col_id, payload = _call_match_api(params, label=f'name {canonical_name!r}')
        if col_id and rank:
            rank_from_api = payload.get('usage', {}).get('rank') or ''
            if rank_from_api.strip().lower() != rank.strip().lower():
                logger.info(
                    'COL resolver: rank mismatch for name %r '
                    '(api=%r, expected=%r), discarding match',
                    canonical_name, rank_from_api, rank,
                )
                col_id = None

    return col_id, payload
