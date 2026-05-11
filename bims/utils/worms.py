# coding=utf-8
"""
WoRMS (World Register of Marine Species) REST API utilities.
API base: https://www.marinespecies.org/rest/

Key endpoints used:
  GET /AphiaRecordByAphiaID/{id}
  GET /AphiaChildrenByAphiaID/{id}?offset=1&count=50&marine_only=false
"""
import logging
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger(__name__)

WORMS_API_BASE = "https://www.marinespecies.org/rest"
WORMS_SOURCE_NAME = "WoRMS"
WORMS_BASE_URL = "https://www.marinespecies.org"

PAGE_SIZE = 50
REQUEST_DELAY = 0.3


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


def get_aphia_record(aphia_id: int) -> Optional[dict]:
    """Fetch a single AphiaRecord from WoRMS by AphiaID."""
    url = f"{WORMS_API_BASE}/AphiaRecordByAphiaID/{aphia_id}"
    try:
        resp = _http.get(url, timeout=30)
        if resp.status_code == 204:
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning("WoRMS get_aphia_record(%s) failed: %s", aphia_id, exc)
        return None


def get_aphia_children(aphia_id: int, marine_only: bool = False) -> list:
    """
    Fetch all direct children of an AphiaID, auto-paging.
    Returns a flat list of AphiaRecord dicts.
    """
    results = []
    offset = 1
    while True:
        url = f"{WORMS_API_BASE}/AphiaChildrenByAphiaID/{aphia_id}"
        params = {
            "offset": offset,
            "count": PAGE_SIZE,
            "marine_only": "true" if marine_only else "false",
        }
        try:
            time.sleep(REQUEST_DELAY)
            resp = _http.get(url, params=params, timeout=30)
            if resp.status_code == 204:
                break
            resp.raise_for_status()
            page = resp.json()
            if not page:
                break
            results.extend(page)
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        except Exception as exc:
            logger.warning(
                "WoRMS get_aphia_children(%s, offset=%s) failed: %s",
                aphia_id, offset, exc,
            )
            break
    return results


def api_record_to_csv_row(record: dict) -> dict:
    """
    Convert a WoRMS REST API AphiaRecord dict into the column format
    expected by WormsTaxaProcessor (keys from WORMS_COLUMN_NAMES).

    Also injects source/version metadata under private '_worms_*' keys
    that are written into Taxonomy.additional_data.
    """
    return {
        # --- identity ---
        "AphiaID": record.get("AphiaID"),
        "ScientificName": record.get("scientificname", ""),
        "Authority": record.get("authority", ""),
        # --- accepted name (for synonyms) ---
        "AphiaID_accepted": record.get("valid_AphiaID"),
        "ScientificName_accepted": record.get("valid_name", ""),
        "Authority_accepted": record.get("valid_authority", ""),
        # --- classification ---
        "Kingdom": record.get("kingdom", ""),
        "Phylum": record.get("phylum", ""),
        "Class": record.get("class", ""),
        "Order": record.get("order", ""),
        "Family": record.get("family", ""),
        "Genus": record.get("genus", ""),
        "Subgenus": record.get("subgenus", ""),
        "Species": _extract_species_epithet(record),
        "Subspecies": "",
        # --- rank / status ---
        "taxonRank": record.get("rank", ""),
        "taxonomicStatus": record.get("status", ""),
        "Qualitystatus": record.get("qualitystatus", ""),
        "Unacceptreason": record.get("unacceptreason", ""),
        # --- timestamps ---
        "DateLastModified": record.get("modified", ""),
        # --- identifiers ---
        "LSID": record.get("lsid", ""),
        "Parent AphiaID": record.get("parentNameUsageID", ""),
        "Storedpath": record.get("url", ""),
        "Citation": record.get("citation", ""),
        # --- habitat flags (WoRMS returns int 0/1/None) ---
        "Marine": record.get("isMarine", 0),
        "Brackish": record.get("isBrackish", 0),
        "Fresh": record.get("isFreshwater", 0),
        "Terrestrial": record.get("isTerrestrial", 0),
        # --- source / version metadata stored in additional_data ---
        "_worms_source": WORMS_SOURCE_NAME,
        "_worms_base_url": WORMS_BASE_URL,
        "_worms_modified": record.get("modified", ""),
        "_worms_aphia_id": record.get("AphiaID"),
    }


def _extract_species_epithet(record: dict) -> str:
    """
    Derive the species epithet from the full scientific name and genus.
    WoRMS REST API does not always provide a standalone epithet field.
    """
    sci = record.get("scientificname", "")
    genus = record.get("genus", "")
    rank = (record.get("rank") or "").lower()
    if rank in ("species", "subspecies") and genus and sci.startswith(genus):
        parts = sci[len(genus):].strip().split()
        if parts:
            return parts[0]
    return ""
