# coding=utf-8
"""Utilities for fetching data from a remote BIMS instance."""
import logging
import time

import requests

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
PAGE_SIZE = 100


def normalize_bims_base_url(base_url: str) -> str:
    """Strip trailing slash from a BIMS base URL."""
    return (base_url or '').rstrip('/')


def _get_with_retry(url: str, params: dict = None) -> dict | list | None:
    """GET a URL with retry logic. Returns parsed JSON or None on failure."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            logger.warning(
                "_get_with_retry attempt %d/%d failed for %s: %s",
                attempt, RETRY_ATTEMPTS, url, exc,
            )
            if attempt < RETRY_ATTEMPTS:
                time.sleep(2 ** attempt)
    return None


def get_taxon_groups(base_url: str) -> list[dict]:
    """
    Fetch the list of species-module taxon groups from a remote BIMS instance.
    Returns a list of dicts with keys: id, name, logo.
    """
    url = f"{normalize_bims_base_url(base_url)}/api/module-list/"
    result = _get_with_retry(url)
    return result if isinstance(result, list) else []


def get_taxon_by_id(base_url: str, taxon_id: int) -> dict | None:
    """
    Fetch a single taxon by its ID from the remote BIMS instance.
    Returns the taxon dict or None if not found / request failed.
    """
    url = f"{normalize_bims_base_url(base_url)}/api/taxon/{taxon_id}/"
    return _get_with_retry(url)


def get_taxa_page(base_url: str, taxon_group_id: int, page: int = 1) -> dict:
    """
    Fetch one page of taxa from a remote BIMS instance for a given taxon group.
    Returns the raw paginated JSON response with keys: count, next, previous, results.
    """
    url = f"{normalize_bims_base_url(base_url)}/api/taxa-list/"
    result = _get_with_retry(url, params={
        'taxonGroup': taxon_group_id,
        'page': page,
        'page_size': PAGE_SIZE,
        'validated': 'True',
    })
    return result if isinstance(result, dict) else {}


def get_all_taxa(base_url: str, taxon_group_id: int):
    """
    Generator that yields individual taxon dicts from a remote BIMS instance,
    iterating over all pages.
    """
    page = 1
    while True:
        data = get_taxa_page(base_url, taxon_group_id, page)
        if not data:
            break
        results = data.get('results') or []
        for taxon in results:
            yield taxon
        if not data.get('next'):
            break
        page += 1
