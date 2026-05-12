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


def get_taxon_names_page(base_url: str, project_token: str,
                         page: int = 1, per_page: int = DEFAULT_PER_PAGE) -> list[dict]:
    url = f"{taxonworks_api_base_url(base_url)}/taxon_names"
    params = {
        "project_token": project_token,
        "page": page,
        "per": per_page,
    }
    try:
        time.sleep(REQUEST_DELAY)
        resp = _http.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as exc:
        logger.warning("TaxonWorks get_taxon_names_page(page=%s) failed: %s", page, exc)
        return []


def get_all_taxon_names(base_url: str, project_token: str,
                        per_page: int = DEFAULT_PER_PAGE) -> list[dict]:
    results = []
    page = 1
    while True:
        items = get_taxon_names_page(
            base_url=base_url,
            project_token=project_token,
            page=page,
            per_page=per_page,
        )
        if not items:
            break
        results.extend(items)
        if len(items) < per_page:
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
