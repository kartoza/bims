# coding=utf-8
from __future__ import annotations

import html
import logging
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger(__name__)

TAXONWORKS_SOURCE_NAME = "TaxonWorks"
DEFAULT_PER_PAGE = 1000
REQUEST_DELAY = 0.2


def _build_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update({"Accept": "application/json"})
    return sess


_http = _build_session()


def normalize_taxonworks_base_url(base_url: str) -> str:
    base_url = (base_url or "").strip().rstrip("/")
    if not base_url:
        return ""
    if base_url.endswith("/api/v1"):
        return base_url[:-7]
    return base_url


def taxonworks_api_base_url(base_url: str) -> str:
    normalized = normalize_taxonworks_base_url(base_url)
    return f"{normalized}/api/v1"


def get_taxon_name(base_url: str, project_token: str, taxon_name_id: int) -> Optional[dict]:
    url = f"{taxonworks_api_base_url(base_url)}/taxon_names/{taxon_name_id}"
    params = {"project_token": project_token}
    try:
        time.sleep(REQUEST_DELAY)
        resp = _http.get(url, params=params, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning("TaxonWorks get_taxon_name(%s) failed: %s", taxon_name_id, exc)
        return None


class TaxonWorksAPIError(Exception):
    """Raised when a TaxonWorks API request fails (as opposed to returning
    an empty result), so callers don't mistake a failure for end-of-data."""


def _parse_int_header(resp, name: str) -> Optional[int]:
    headers = getattr(resp, "headers", None) or {}
    try:
        value = headers.get(name)
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def get_taxon_names_page_with_meta(
        base_url: str, project_token: str,
        page: int = 1, per_page: int = DEFAULT_PER_PAGE,
        taxon_name_id: Optional[int] = None,
) -> tuple[list[dict], Optional[int]]:
    """
    Fetch one page of taxon names.

    If *taxon_name_id* is given, only that name's descendants are returned
    (the name itself is not included by the TaxonWorks API).

    Returns ``(records, total_pages)``; ``total_pages`` comes from the
    ``pagination-total-pages`` response header and is ``None`` if absent.
    Raises :class:`TaxonWorksAPIError` if the request fails.
    """
    url = f"{taxonworks_api_base_url(base_url)}/taxon_names"
    params = [
        ("project_token", project_token),
        ("page", page),
        ("per", per_page),
    ]
    if taxon_name_id:
        params.append(("taxon_name_id[]", int(taxon_name_id)))
        params.append(("descendants", "true"))
    try:
        time.sleep(REQUEST_DELAY)
        resp = _http.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        raise TaxonWorksAPIError(
            f"get_taxon_names_page(page={page}) failed: {exc}"
        ) from exc
    if not isinstance(data, list):
        raise TaxonWorksAPIError(
            f"get_taxon_names_page(page={page}) returned unexpected "
            f"payload type {type(data).__name__}"
        )
    return data, _parse_int_header(resp, "pagination-total-pages")


def get_taxon_names_page(base_url: str, project_token: str,
                         page: int = 1, per_page: int = DEFAULT_PER_PAGE,
                         taxon_name_id: Optional[int] = None) -> list[dict]:
    """Fetch one page of taxon names. Raises TaxonWorksAPIError on failure."""
    records, _ = get_taxon_names_page_with_meta(
        base_url, project_token, page=page, per_page=per_page,
        taxon_name_id=taxon_name_id,
    )
    return records


def get_all_taxon_names(base_url: str, project_token: str,
                        per_page: int = DEFAULT_PER_PAGE,
                        taxon_name_id: Optional[int] = None) -> list[dict]:
    results = []
    page = 1
    while True:
        items, total_pages = get_taxon_names_page_with_meta(
            base_url=base_url,
            project_token=project_token,
            page=page,
            per_page=per_page,
            taxon_name_id=taxon_name_id,
        )
        results.extend(items)
        if total_pages is not None:
            if page >= total_pages:
                break
        elif len(items) < per_page:
            break
        page += 1
    return results


def taxonworks_record_is_extinct(record: dict) -> bool:
    cached_html = html.unescape(record.get("cached_html") or "")
    return "†" in cached_html


def taxonworks_record_to_additional_data(record: dict, base_url: str) -> dict:
    extras = dict(record)
    extras.update({
        "_taxonworks_source": TAXONWORKS_SOURCE_NAME,
        "_taxonworks_base_url": normalize_taxonworks_base_url(base_url),
        "_taxonworks_project_id": record.get("project_id"),
        "_taxonworks_updated_at": record.get("updated_at"),
        "_taxonworks_created_at": record.get("created_at"),
        "_taxonworks_taxon_name_id": record.get("id"),
        "_taxonworks_valid_taxon_name_id": record.get("cached_valid_taxon_name_id"),
        "_taxonworks_rank_string": record.get("rank_string"),
        "_taxonworks_is_extinct": taxonworks_record_is_extinct(record),
    })
    return extras


def find_taxon_name_by_name(base_url: str, project_token: str,
                            name: str) -> list[dict]:
    """
    Search taxon names by name string.
    Returns a list of matching records, each containing at minimum ``id``,
    ``cached`` (display name), and ``rank``.
    """
    url = f"{taxonworks_api_base_url(base_url)}/taxon_names"
    params = {
        "project_token": project_token,
        "name": name,
    }
    try:
        time.sleep(REQUEST_DELAY)
        resp = _http.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as exc:
        logger.warning("TaxonWorks find_taxon_name_by_name(%r) failed: %s", name, exc)
        return []


def taxon_name_url(base_url: str, taxon_name_id: int) -> str:
    return f"{normalize_taxonworks_base_url(base_url)}/taxon_names/{taxon_name_id}"


def get_otus_for_taxon_name_ids(base_url: str, project_token: str,
                                taxon_name_ids: list[int],
                                per_page: int = 500) -> list[dict]:
    """Fetch OTUs for a specific batch of taxon_name_ids (max ~10 at a time)."""
    url = f"{taxonworks_api_base_url(base_url)}/otus"
    params = [
        ("project_token", project_token),
        ("page", 1),
        ("per", per_page),
    ]
    for tid in taxon_name_ids:
        params.append(("taxon_name_id[]", tid))
    try:
        time.sleep(REQUEST_DELAY)
        resp = _http.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as exc:
        logger.warning(
            "TaxonWorks get_otus_for_taxon_name_ids(%s) failed: %s",
            taxon_name_ids, exc
        )
        return []


def fetch_otus_for_ids(base_url: str, project_token: str,
                       all_taxon_name_ids: list[int],
                       batch_size: int = 10) -> dict[int, int]:
    """
    Fetch OTUs for all given taxon_name_ids in batches of batch_size.
    Returns a mapping of taxon_name_id -> otu_id.
    """
    mapping: dict[int, int] = {}
    ids = list(all_taxon_name_ids)
    for i in range(0, len(ids), batch_size):
        batch = ids[i:i + batch_size]
        otus = get_otus_for_taxon_name_ids(base_url, project_token, batch)
        for otu in otus:
            taxon_name_id = otu.get("taxon_name_id")
            otu_id = otu.get("id")
            if taxon_name_id and otu_id:
                mapping[int(taxon_name_id)] = int(otu_id)
    return mapping
