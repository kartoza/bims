# utils/gbif_publish.py
import csv
import os
import zipfile
import uuid
from types import SimpleNamespace
from datetime import datetime
from typing import Iterable, Tuple, List
from xml.sax.saxutils import escape as _xe

import requests
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils.timezone import now
from requests.auth import HTTPBasicAuth

from bims.models.biological_collection_record import BiologicalCollectionRecord
from bims.utils.gbif_metadata import METADATA_LICENCES, parse_metadata_file

LICENSE_URL = "https://creativecommons.org/licenses/by/4.0/legalcode"


class DwcaArchiveNotPublishedError(ValueError):
    """Raised when a DwC-A archive is created but cannot be published."""

    def __init__(self, message: str, archive_url: str, records_published: int = 0):
        super().__init__(message)
        self.archive_url = archive_url
        self.records_published = records_published


def _media_url() -> str:
    return getattr(settings, "MEDIA_URL", "/media/").rstrip("/")

def _site_name() -> str:
    return getattr(settings, "SITE_NAME", "BIMS")


def dwca_dir() -> str:
    ts = now().strftime("%Y%m%d-%H%M%S")
    rel = os.path.join("exports", "gbif", ts + "-" + uuid.uuid4().hex[:8])
    abs_dir = os.path.join(settings.MEDIA_ROOT, rel)
    os.makedirs(abs_dir, exist_ok=True)
    return abs_dir


def get_publisher_name(config) -> str:
    """Fetch the full publisher name from GBIF using publishing_org_key."""
    if not config.publishing_org_key:
        return ""
    api_url = config.gbif_api_url.rstrip("/")
    try:
        r = requests.get(
            f"{api_url}/organization/{config.publishing_org_key}",
            timeout=10,
        )
        r.raise_for_status()
        return r.json().get("title") or ""
    except Exception:
        return ""


_ABUNDANCE_TYPE_MAP = {
    "number":                        ("individuals",     True),
    "percentage":                    ("% cover",         False),
    "density (m2)":                  ("individuals/m2",  False),
    "density (cells/m2)":            ("cells/m2",        False),
    "density (cells/ml)":            ("cells/mL",        False),
    "species valve/frustule count":  ("valves/frustules", True),
}

_RECORD_TYPE_BASIS_MAP = {
    "dna sample": "MATERIAL_SAMPLE",
    "specimen collection": "MATERIAL_SAMPLE",
    "photographic record": "MACHINE_OBSERVATION",
    "visual observation": "HUMAN_OBSERVATION",
    # For now treat acoustic records as human observations.
    # This can be split into HUMAN/MACHINE once FBIS supports that distinction.
    "acoustic record": "HUMAN_OBSERVATION",
}


def write_occurrence_txt(
    path: str,
    records: Iterable[BiologicalCollectionRecord],
    dataset_name: str = "",
) -> Tuple[List[int], int]:
    from django.db import connection
    tenant_name = connection.schema_name

    header = [
        "occurrenceID", "basisOfRecord", "scientificName", "eventDate",
        "decimalLatitude", "decimalLongitude", "locality", "recordedBy",
        "datasetName", "institutionCode", "collectionCode", "catalogNumber",
        "individualCount", "organismQuantity", "organismQuantityType",
        "dataGeneralizations", "license"
    ]
    written_ids: List[int] = []
    missing_inst_code: int = 0
    with open(path, "w", newline="\n", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(header)
        for r in records:
            sci_name = (
                    getattr(r.taxonomy, "canonical_name", None) or
                    r.original_species_name or ""
            ).strip()
            if not sci_name:
                continue

            lat = getattr(r.site, "latitude", None)
            lon = getattr(r.site, "longitude", None)

            if not lat or not lon:
                continue

            lat, lon = str(lat), str(lon)

            if r.site.river:
                locality = r.site.river.name
            else:
                locality = r.site.site_description

            basis = "HUMAN_OBSERVATION"
            try:
                rt = (r.record_type.name or "").lower()
                if rt in _RECORD_TYPE_BASIS_MAP:
                    basis = _RECORD_TYPE_BASIS_MAP[rt]
                elif "sample" in rt or "specimen" in rt:
                    basis = "MATERIAL_SAMPLE"
                elif "photo" in rt or "machine observation" in rt:
                    basis = "MACHINE_OBSERVATION"
                elif "observation" in rt or "visual" in rt:
                    basis = "HUMAN_OBSERVATION"
            except Exception:
                pass

            occurrence_id = str(r.uuid or r.pk)
            collection_code = (
                getattr(getattr(r, "module_group", None), "name", "") or "BIMS"
            )
            catalog_number = str(r.pk)

            recorded_by = ""

            if not recorded_by and r.owner:
                candidate = (r.owner.get_full_name() or r.owner.username).strip()
                if 'admin' not in candidate.lower():
                    recorded_by = candidate

            if not recorded_by and r.collector_user:
                recorded_by = (
                    r.collector_user.get_full_name() or
                    r.collector_user.username
                ).strip()
                if 'admin' in recorded_by.lower():
                    recorded_by = ''

            if not recorded_by:
                recorded_by = (r.collector or "").strip()

            row_dataset_name = dataset_name or _site_name()

            inst_code = (r.institution_id or "").strip()
            if (
                inst_code == settings.INSTITUTION_ID_DEFAULT or inst_code.lower() in ['healthyrivers', 'bims', 'fbis']
            ):
                inst_code = ""
                for user in (r.collector_user, r.owner):
                    if user and getattr(user, "organization", None):
                        inst_code = user.organization.strip()
                        if inst_code:
                            break
            if not inst_code:
                missing_inst_code += 1

            dg = ""

            event_date = ""
            if r.collection_date:
                event_date = r.collection_date.isoformat()

            record_license = (
                (r.licence.url if r.licence and r.licence.url else None)
                or LICENSE_URL
            )

            abundance_name = ""
            try:
                abundance_name = (r.abundance_type.name or "").lower().strip()
            except Exception:
                pass

            oqt, use_individual_count = _ABUNDANCE_TYPE_MAP.get(
                abundance_name, ("individuals", True)
            )
            qty = r.abundance_number if r.abundance_number is not None else 1
            individual_count = qty if use_individual_count else ""
            organism_quantity = qty
            organism_quantity_type = oqt

            row = [
                f"{tenant_name}:{occurrence_id}", basis, sci_name, event_date,
                lat, lon, locality, recorded_by,
                row_dataset_name, inst_code, collection_code, catalog_number,
                individual_count, organism_quantity, organism_quantity_type,
                dg, record_license
            ]
            w.writerow(row)
            written_ids.append(r.id)
    return written_ids, missing_inst_code

def write_meta_xml(path: str):
    meta = f"""<archive xmlns="http://rs.tdwg.org/dwc/text/" metadata="eml.xml">
      <core encoding="UTF-8" fieldsTerminatedBy="\\t" linesTerminatedBy="\\n" fieldsEnclosedBy="" ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/Occurrence">
        <files><location>occurrence.txt</location></files>
        <id index="0"/>
        <field index="0"  term="http://rs.tdwg.org/dwc/terms/occurrenceID"/>
        <field index="1"  term="http://rs.tdwg.org/dwc/terms/basisOfRecord"/>
        <field index="2"  term="http://rs.tdwg.org/dwc/terms/scientificName"/>
        <field index="3"  term="http://rs.tdwg.org/dwc/terms/eventDate"/>
        <field index="4"  term="http://rs.tdwg.org/dwc/terms/decimalLatitude"/>
        <field index="5"  term="http://rs.tdwg.org/dwc/terms/decimalLongitude"/>
        <field index="6"  term="http://rs.tdwg.org/dwc/terms/locality"/>
        <field index="7"  term="http://rs.tdwg.org/dwc/terms/recordedBy"/>
        <field index="8"  term="http://rs.tdwg.org/dwc/terms/datasetName"/>
        <field index="9"  term="http://rs.tdwg.org/dwc/terms/institutionCode"/>
        <field index="10" term="http://rs.tdwg.org/dwc/terms/collectionCode"/>
        <field index="11" term="http://rs.tdwg.org/dwc/terms/catalogNumber"/>
        <field index="12" term="http://rs.tdwg.org/dwc/terms/dataGeneralizations"/>
        <field index="13" term="http://purl.org/dc/terms/license"/>
      </core>
    </archive>
    """
    with open(path, "w", encoding="utf-8") as f:
        f.write(meta)


def eml_author(author) -> str:
    """Return EML XML snippet for one author"""
    user = author
    try:
        bims_profile = user.bims_profile
        role_name = bims_profile.role.display_name if getattr(bims_profile, 'role', None) else None
    except Exception:
        role_name = None

    given = _xe(getattr(user, 'first_name', '').strip())
    sur = _xe(getattr(user, 'last_name', '').strip())
    email = _xe(getattr(user, 'email', '').strip())
    org = _xe((getattr(user, 'organization', '') or '').strip())

    parts = []
    if given or sur:
        parts.append('<individualName>')
        if given:
            parts.append(f'    <givenName>{given}</givenName>')
        if sur:
            parts.append(f'    <surName>{sur}</surName>')
        parts.append('  </individualName>')
    if org:
        parts.append(f'  <organizationName>{org}</organizationName>')
    if role_name:
        parts.append(f'  <positionName>{_xe(role_name)}</positionName>')
    if email:
        parts.append(f'  <electronicMailAddress>{email}</electronicMailAddress>')
    return '\n  '.join(parts)


def eml_contact_from_model(contact) -> str:
    """Return EML XML body snippet for a GbifPublishContact model instance.

    Applies the resolved_* properties so blank fields fall back to the linked
    user's profile data.
    """
    given = _xe(contact.resolved_given_name or "")
    sur = _xe(contact.resolved_sur_name or "")
    org = _xe(contact.resolved_organization_name or "")
    position = _xe(contact.resolved_position_name or "")
    email = _xe(contact.resolved_email or "")
    phone = _xe(contact.phone.strip() if contact.phone else "")
    url = _xe(contact.online_url.strip() if contact.online_url else "")

    # address sub-fields
    dp = _xe((contact.delivery_point or "").strip())
    city = _xe((contact.city or "").strip())
    postal = _xe((contact.postal_code or "").strip())
    country = _xe((contact.country or "").strip())

    parts = []
    if given or sur:
        parts.append("<individualName>")
        if given:
            parts.append(f"    <givenName>{given}</givenName>")
        if sur:
            parts.append(f"    <surName>{sur}</surName>")
        parts.append("  </individualName>")

    if org:
        parts.append(f"  <organizationName>{org}</organizationName>")
    if position:
        parts.append(f"  <positionName>{position}</positionName>")

    if dp or city or postal or country:
        parts.append("  <address>")
        if dp:
            parts.append(f"    <deliveryPoint>{dp}</deliveryPoint>")
        if city:
            parts.append(f"    <city>{city}</city>")
        if postal:
            parts.append(f"    <postalCode>{postal}</postalCode>")
        if country:
            parts.append(f"    <country>{country}</country>")
        parts.append("  </address>")

    if phone:
        parts.append(f"  <phone>{phone}</phone>")
    if email:
        parts.append(f"  <electronicMailAddress>{email}</electronicMailAddress>")
    if url:
        parts.append(f"  <onlineUrl>{url}</onlineUrl>")

    role = _xe((getattr(contact, "role", "") or "").strip())
    if role:
        parts.append(f"  <role>{role}</role>")

    return "\n  ".join(parts)


def _authors_from_contacts(contacts) -> str:
    """Build an author string from GBIF contacts, preferring originators."""
    if not contacts:
        return ""
    originators = [c for c in contacts if (getattr(c, "role", "") or "") == "originator"]
    pool = originators or contacts
    names = []
    for c in pool:
        given = (c.resolved_given_name or "").strip()
        sur = (c.resolved_sur_name or "").strip()
        org = (c.resolved_organization_name or "").strip()
        if org:
            names.append(org)
        elif given or sur:
            names.append(f"{given} {sur}".strip())
    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    return ", ".join(names[:-1]) + " & " + names[-1]


def eml_citation(source_reference, contacts=None) -> str:
    """Return an EML <citation> string for any source reference type.

    When contacts are provided, originator contacts are used as authors;
    this falls back to all contacts, then to the reference's own author list.
    """
    from bims.models.source_reference import (
        SourceReferenceBibliography,
        SourceReferenceDatabase,
        SourceReferenceDocument,
    )
    if source_reference is None:
        return ""

    if contacts:
        authors = _authors_from_contacts(contacts)
    else:
        authors = source_reference.authors
        authors = "" if authors == "-" else authors

    year = source_reference.year
    year = "" if year == "-" else str(year)
    title = source_reference.title or ""

    identifier = ""
    journal_str = ""
    doi_str = ""

    if isinstance(source_reference, SourceReferenceBibliography):
        doi = (getattr(source_reference.source, "doi", "") or "").strip()
        identifier = doi
        if doi:
            doi_str = f" doi:{doi}"
        if source_reference.source.journal:
            abbr = (getattr(source_reference.source.journal, "abbreviation", "") or "").strip()
            name = (getattr(source_reference.source.journal, "name", "") or "").strip()
            journal_str = abbr or name
    elif isinstance(source_reference, SourceReferenceDocument):
        identifier = (getattr(source_reference.source, "doc_url", "") or "").strip()
    elif isinstance(source_reference, SourceReferenceDatabase):
        identifier = (getattr(source_reference.source, "url", "") or "").strip()

    parts = []
    if authors:
        parts.append(_xe(authors))
    if year:
        parts.append(f"({_xe(year)})")
    if title:
        parts.append(_xe(title))
    if journal_str:
        parts.append(_xe(journal_str))
    text = " ".join(parts)
    if doi_str:
        text = f"{text}.{_xe(doi_str)}."
    elif text:
        text = f"{text}."

    id_attr = f' identifier="{_xe(identifier)}"' if identifier else ""
    return f"<citation{id_attr}>{text}</citation>"


def intellectual_rights_text(licence=None) -> str:
    """Format a single Licence object into an intellectualRights string."""
    name = (licence.name if licence and licence.name else "Creative Commons CCZero 1.0 License")
    url = (licence.url if licence and licence.url else LICENSE_URL)
    return f"This work is licensed under a {_xe(name)} {_xe(url)}."


def _paras(text: str) -> str:
    """Render free text as EML <para> elements, one per paragraph."""
    parts = [p.strip() for p in (text or "").replace("\r\n", "\n").split("\n\n")]
    return "".join(f"<para>{_xe(p)}</para>" for p in parts if p)


def _eml_coverage(metadata: dict, bbox=None) -> str:
    geographic = ""
    if metadata.get("geographic_description") and bbox:
        west, east, north, south = bbox
        geographic = (
            "<geographicCoverage>"
            f"<geographicDescription>{_xe(metadata['geographic_description'])}</geographicDescription>"
            "<boundingCoordinates>"
            f"<westBoundingCoordinate>{west}</westBoundingCoordinate>"
            f"<eastBoundingCoordinate>{east}</eastBoundingCoordinate>"
            f"<northBoundingCoordinate>{north}</northBoundingCoordinate>"
            f"<southBoundingCoordinate>{south}</southBoundingCoordinate>"
            "</boundingCoordinates></geographicCoverage>"
        )
    start, end = metadata.get("temporal_start"), metadata.get("temporal_end")
    temporal = ""
    if start and end:
        temporal = (
            "<temporalCoverage><rangeOfDates>"
            f"<beginDate><calendarDate>{start}</calendarDate></beginDate>"
            f"<endDate><calendarDate>{end}</calendarDate></endDate>"
            "</rangeOfDates></temporalCoverage>"
        )
    elif start or end:
        temporal = (
            "<temporalCoverage><singleDateTime>"
            f"<calendarDate>{start or end}</calendarDate>"
            "</singleDateTime></temporalCoverage>"
        )
    taxonomic = ""
    if metadata.get("taxonomic_coverage"):
        taxonomic = (
            "<taxonomicCoverage><generalTaxonomicCoverage>"
            f"{_xe(metadata['taxonomic_coverage'])}"
            "</generalTaxonomicCoverage></taxonomicCoverage>"
        )
    inner = geographic + temporal + taxonomic
    return f"<coverage>{inner}</coverage>" if inner else ""


def _eml_methods(metadata: dict) -> str:
    sampling = metadata.get("sampling_description")
    if not sampling:
        return ""
    extent = metadata.get("geographic_description") or "See geographic coverage."
    return (
        "<methods>"
        f"<methodStep><description>{_paras(sampling)}</description></methodStep>"
        "<sampling>"
        f"<studyExtent><description><para>{_xe(extent)}</para></description></studyExtent>"
        f"<samplingDescription>{_paras(sampling)}</samplingDescription>"
        "</sampling></methods>"
    )


def _eml_project(project_identifier: str) -> str:
    pid = (project_identifier or "").strip()
    if not pid:
        return ""
    return f'<project id="{_xe(pid, {chr(34): "&quot;"})}"><title>{_xe(pid)}</title></project>'


def write_eml_xml(
        path: str,
        title: str,
        abstract: str,
        contacts: list,
        licences: list = None,
        citation: str = "",
        pub_date: str = "",
        metadata: dict = None,
        bbox=None):
    metadata = metadata or {}
    abstract_xml = (
        f"<abstract>{_paras(metadata['description'])}</abstract>"
        if metadata.get("description")
        else f"<abstract><para>{_xe(abstract)}</para></abstract>"
    )
    coverage_xml = _eml_coverage(metadata, bbox)
    purpose_xml = (
        f"<purpose>{_paras(metadata['purpose'])}</purpose>"
        if metadata.get("purpose") else "")
    methods_xml = _eml_methods(metadata)
    project_xml = _eml_project(metadata.get("project_identifier"))
    if metadata.get("license"):
        name, url = METADATA_LICENCES[metadata["license"].lower()]
        licences = [SimpleNamespace(name=name, url=url)]
    today = datetime.utcnow().date().isoformat()
    pub_date = pub_date or today
    site = _site_name()

    originators = [c for c in contacts if (getattr(c, 'role', '') or '') == 'originator']
    if originators:
        creator_blocks = "\n".join(
            f"  <creator>\n  {eml_contact_from_model(c)}\n  </creator>"
            for c in originators
        )
    else:
        creator_blocks = (
            f"  <creator>\n"
            f"    <individualName><surName>{_xe(site)}</surName></individualName>\n"
            f"    <organizationName>{_xe(site)}</organizationName>\n"
            f"  </creator>"
        )

    contact_blocks = "\n".join(
        f"  <contact>\n  {eml_contact_from_model(c)}\n  </contact>"
        for c in contacts
    )

    if licences:
        rights_paras = "\n".join(
            f"              <para>{intellectual_rights_text(lic)}</para>"
            for lic in licences
        )
    else:
        rights_paras = f"              <para>{intellectual_rights_text()}</para>"

    additional_metadata = ""
    if citation:
        additional_metadata = f"""
        <additionalMetadata>
          <metadata>
            <gbif>
              {citation}
            </gbif>
          </metadata>
        </additionalMetadata>"""

    eml = f"""<?xml version="1.0" encoding="UTF-8"?>
        <eml:eml packageId="{uuid.uuid4()}" system="GBIF" xmlns:eml="eml://ecoinformatics.org/eml-2.1.1"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="eml://ecoinformatics.org/eml-2.1.1 http://rs.gbif.org/schema/eml-gbif-profile/1.1/eml.xsd">
          <dataset>
            <title>{_xe(title)}</title>
            {creator_blocks}
            <pubDate>{pub_date}</pubDate>
            {abstract_xml}
            <intellectualRights>
{rights_paras}
            </intellectualRights>
            {coverage_xml}
            {purpose_xml}
            {contact_blocks}
            {methods_xml}
            {project_xml}
          </dataset>{additional_metadata}
        </eml:eml>
        """
    with open(path, "w", encoding="utf-8") as f:
        f.write(eml)


def zip_dwca(folder: str) -> str:
    zip_path = os.path.join(folder, "dwca.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for fname in ["occurrence.txt", "meta.xml", "eml.xml"]:
            z.write(os.path.join(folder, fname), arcname=fname)
    return zip_path


def gather_data_for_source_reference(source_reference) -> Iterable[BiologicalCollectionRecord]:
    """Gather publishable records for a specific source reference."""
    return (
        BiologicalCollectionRecord.objects
        .filter(
            Q(data_type="public") | Q(data_type__isnull=True) | Q(data_type=""),
            survey__validated=True,
            source_reference=source_reference,
        )
        .exclude(source_collection__iexact="gbif")
        .select_related("taxonomy", "site", "record_type", "survey", "licence")
        .distinct()
    )


def register_dataset(
    config,
    title: str,
    description: str,
    project_identifier: str = "",
    license_url: str = "",
) -> str:
    """Register a dataset on GBIF using config credentials."""
    if not config.publishing_org_key or not config.installation_key:
        raise RuntimeError("Missing publishing_org_key or installation_key in config.")

    payload = {
        "publishingOrganizationKey": config.publishing_org_key,
        "installationKey": config.installation_key,
        "type": "OCCURRENCE",
        "title": title,
        "description": description,
        "language": "eng",
        "license": license_url or (
            config.license.url if config.license_id and config.license.url else LICENSE_URL),
    }

    if project_identifier:
        payload["project"] = {"identifier": project_identifier}

    auth = HTTPBasicAuth(config.username, config.password)
    api_url = config.gbif_api_url.rstrip("/")

    r = requests.post(
        f"{api_url}/dataset",
        json=payload,
        auth=auth,
        timeout=30,
        headers={"Content-Type": "application/json"},
    )

    if r.status_code == 403:
        raise RuntimeError(
            f"GBIF API returned 403 Forbidden. Please verify: "
            f"1) Username/password are correct for {api_url}, "
            f"2) User has permission to publish for org {config.publishing_org_key}, "
            f"3) Installation key {config.installation_key} is valid for this environment."
        )
    if r.status_code == 401:
        raise RuntimeError(
            f"GBIF API returned 401 Unauthorized. Invalid username or password for {api_url}."
        )

    r.raise_for_status()
    dataset_key = r.json()
    if not isinstance(dataset_key, str) or len(dataset_key) < 32:
        raise RuntimeError(f"Unexpected dataset key response: {dataset_key}")
    return dataset_key


def add_endpoint(config, dataset_key: str, archive_url: str):
    """Add a DWC_ARCHIVE endpoint to a dataset using config credentials."""
    payload = {
        "type": "DWC_ARCHIVE",
        "url": archive_url,
    }

    auth = HTTPBasicAuth(config.username, config.password)
    api_url = config.gbif_api_url.rstrip("/")

    r = requests.post(
        f"{api_url}/dataset/{dataset_key}/endpoint",
        json=payload,
        auth=auth,
        timeout=30,
        headers={"Content-Type": "application/json"},
    )
    r.raise_for_status()


def archive_url_dir(archive_url: str) -> str:
    """Return the filesystem directory that corresponds to a stored archive_url.
    """
    from urllib.parse import urlparse
    url_path = urlparse(archive_url).path
    media_prefix = _media_url()
    if url_path.startswith(media_prefix):
        rel = url_path[len(media_prefix):].lstrip("/")
    else:
        rel = url_path.lstrip("/")
    return os.path.dirname(os.path.join(settings.MEDIA_ROOT, rel))


def load_publish_metadata(source_reference) -> dict:
    """Return the row of the source reference's GBIF metadata CSV, or {}.

    Raises ValueError when the uploaded CSV is not usable.
    """
    metadata_file = getattr(source_reference, "gbif_metadata_file", None)
    if not metadata_file:
        return {}
    metadata_file.open("rb")
    try:
        return parse_metadata_file(metadata_file, source_reference.pk)
    finally:
        metadata_file.close()


def _records_bbox(records):
    """Return (west, east, north, south) of the records' sites, or None."""
    lats, lons = [], []
    for r in records:
        try:
            lat, lon = float(r.site.latitude), float(r.site.longitude)
        except (AttributeError, TypeError, ValueError):
            continue
        lats.append(lat)
        lons.append(lon)
    if not lats:
        return None
    return min(lons), max(lons), max(lats), min(lats)


def build_dwca(
    config,
    records: Iterable[BiologicalCollectionRecord],
    contacts: list,
    source_reference=None,
    out_dir: str = None,
    metadata: dict = None,
) -> Tuple[str, str, List[int]]:
    """Build DwC-A using config for base URL.
    """
    from bims.utils.mail import get_domain_name

    if out_dir is None:
        out_dir = dwca_dir()
    occ_path = os.path.join(out_dir, "occurrence.txt")
    meta_path = os.path.join(out_dir, "meta.xml")
    eml_path = os.path.join(out_dir, "eml.xml")

    ref_title = source_reference.title if source_reference else _site_name()

    written_ids, missing_inst_code = write_occurrence_txt(occ_path, records, dataset_name=ref_title)
    if not written_ids:
        raise ValueError("No eligible records to export.")

    metadata = metadata or {}
    title = metadata.get("title") or ref_title
    publisher_name = get_publisher_name(config) or getattr(config, 'name', None) or _site_name()
    abstract = (
        f"Occurrence dataset for {ref_title} uploaded to {publisher_name}."
    )
    seen = set()
    licences = []
    for r in records:
        lic = getattr(r, 'licence', None)
        if lic and lic.pk not in seen:
            seen.add(lic.pk)
            licences.append(lic)
    licences = licences or None

    citation = eml_citation(source_reference, contacts=contacts) if source_reference else ""

    pub_date = ""
    if source_reference:
        try:
            if getattr(source_reference, "source_date", None):
                pub_date = source_reference.source_date.isoformat()
            else:
                year = source_reference.year
                if year and year != "-":
                    pub_date = f"{year}-01-01"
        except Exception:
            pass

    write_meta_xml(meta_path)
    write_eml_xml(
        eml_path,
        title,
        abstract,
        contacts=contacts,
        licences=licences,
        citation=citation,
        pub_date=pub_date,
        metadata=metadata,
        bbox=_records_bbox(records))
    zip_path = zip_dwca(out_dir)

    domain_name = get_domain_name()
    base_url = config.export_base_url.rstrip("/") if config.export_base_url else f'https://{domain_name}/'

    rel_media = os.path.relpath(zip_path, settings.MEDIA_ROOT)
    archive_url = f"{base_url}{_media_url()}/{rel_media}".replace(
        "//", "/").replace(":/", "://")

    if missing_inst_code:
        raise DwcaArchiveNotPublishedError(
            f"The DwC-A archive was created at {archive_url} but was NOT published to GBIF: "
            f"{missing_inst_code} occurrence(s) are missing a Custodian (institution code). "
            f"Please update the Custodian field for all occurrences and re-publish, "
            f"or use the 'Update Custodian from GBIF Archive' tool in the admin to backfill "
            f"from the generated archive.",
            archive_url=archive_url,
            records_published=len(written_ids),
        )

    return zip_path, archive_url, written_ids


def _contact_to_gbif_payload(contact) -> dict:
    """Convert a GbifPublishContact instance to a GBIF registry API contact payload."""
    import re
    role = (getattr(contact, "role", "") or "").strip()
    # Convert camelCase role to UPPER_SNAKE for GBIF API e.g. "pointOfContact" -> "POINT_OF_CONTACT"
    role_upper = re.sub(r'(?<!^)(?=[A-Z])', '_', role).upper() if role else "POINT_OF_CONTACT"

    payload = {"type": role_upper, "primary": True}

    given = contact.resolved_given_name
    sur = contact.resolved_sur_name
    org = contact.resolved_organization_name
    if given:
        payload["firstName"] = given
    if sur:
        payload["lastName"] = sur
    if org:
        payload["organization"] = org
    position = contact.resolved_position_name
    if position:
        payload["position"] = [position]
    email = contact.resolved_email
    if email:
        payload["email"] = [email]
    phone = (contact.phone or "").strip()
    if phone:
        payload["phone"] = [phone]
    dp = (contact.delivery_point or "").strip()
    if dp:
        payload["address"] = [dp]
    city = (contact.city or "").strip()
    if city:
        payload["city"] = city
    postal = (contact.postal_code or "").strip()
    if postal:
        payload["postalCode"] = postal
    country = (contact.country or "").strip()
    if country:
        payload["country"] = country
    url = (contact.online_url or "").strip()
    if url:
        payload["homepage"] = [url]
    return payload


def sync_dataset_contacts(config, dataset_key: str, contacts: list) -> None:
    """Replace all contacts on a GBIF dataset with the provided list via the registry API."""
    auth = HTTPBasicAuth(config.username, config.password)
    api_url = config.gbif_api_url.rstrip("/")
    base = f"{api_url}/dataset/{dataset_key}/contact"

    r = requests.get(base, auth=auth, timeout=30)
    r.raise_for_status()
    for existing in r.json():
        contact_key = existing.get("key")
        if contact_key:
            requests.delete(f"{base}/{contact_key}", auth=auth, timeout=30).raise_for_status()

    for contact in contacts:
        payload = _contact_to_gbif_payload(contact)
        r = requests.post(
            base,
            json=payload,
            auth=auth,
            timeout=30,
            headers={"Content-Type": "application/json"},
        )
        r.raise_for_status()


def trigger_crawl_with_config(config, dataset_key: str) -> None:
    """Ask GBIF to re-crawl an existing dataset."""
    auth = HTTPBasicAuth(config.username, config.password)
    api_url = config.gbif_api_url.rstrip("/")
    r = requests.post(
        f"{api_url}/dataset/{dataset_key}/crawl",
        auth=auth,
        timeout=30,
    )
    r.raise_for_status()


def create_new_installation(config, title = "", description = "") -> str:
    """Creates a new installation"""
    if not title:
        title = f"{config.name} Installation"
    payload = {
        "organizationKey": config.publishing_org_key,
        "type": "HTTP_INSTALLATION",
        "title": title,
        "description": description,
        "disabled": False
    }
    auth = HTTPBasicAuth(config.username, config.password)
    api_url = config.gbif_api_url.rstrip("/")
    r = requests.post(
        f"{api_url}/installation",
        json=payload,
        auth=auth,
        timeout=30,
        headers={"Content-Type": "application/json"},
    )
    r.raise_for_status()
    installation_key = r.json()
    if not isinstance(installation_key, str) or len(installation_key) < 32:
        raise RuntimeError(f"Unexpected installation key response: {installation_key}")
    return installation_key


@transaction.atomic
def publish_gbif_data_with_config(
    config,
    source_reference=None,
    existing_dataset_key: str = "",
    existing_archive_url: str = "",
    contacts: list = None,
) -> dict:
    """
    Build a DwC-A for public, validated records and publish it to GBIF.

    Records are filtered by *source_reference* when provided.  The dataset
    title is the source reference title (no date appended), and the description
    follows the pattern:
        "Occurrence dataset for <title> uploaded to <site_name>."

    When *existing_dataset_key* and *existing_archive_url* are supplied the
    archive is overwritten in-place and a re-crawl is triggered so the GBIF
    endpoint URL stays unchanged.

    Args:
        config: GbifPublishConfig instance with API credentials and settings
        source_reference: Optional SourceReference to filter records by
        existing_dataset_key: GBIF dataset UUID from a previous successful
            publish for this schedule.  Empty string means "first publish".
        existing_archive_url: Public URL of the archive from the previous
            successful publish.  Used to derive the directory to overwrite.
        contacts: List of GbifPublishContact instances to embed as EML
            <contact> elements.  Required — raises ValueError if empty.

    Returns:
        dict with dataset_key, records_published, and archive_url
    """
    records = None

    if source_reference:
        from bims.models.source_reference import (
            SourceReferenceBibliography,
            SourceReferenceDocument,
        )
        is_typed = isinstance(
            source_reference, (SourceReferenceBibliography, SourceReferenceDocument)
        )
        if not is_typed:
            if not source_reference.source_date:
                raise ValueError(
                    "Cannot publish to GBIF: this source reference has no date. "
                    "Add a date before publishing."
                )
            if not source_reference.source_authors.exists():
                raise ValueError(
                    "Cannot publish to GBIF: this source reference has no authors. "
                    "Add at least one author before publishing."
                )
        records = list(gather_data_for_source_reference(source_reference))

    if not contacts:
        raise ValueError(
            "No contacts configured for this GBIF config. "
            "Add at least one contact via the GBIF Config admin before publishing."
        )

    if not records:
        raise ValueError("No records to publish (need public+validated).")

    metadata = load_publish_metadata(source_reference)

    out_dir = archive_url_dir(existing_archive_url) if existing_archive_url else None

    zip_path, archive_url, written_ids = build_dwca(
        config, records, contacts, source_reference, out_dir=out_dir,
        metadata=metadata,
    )

    ref_title = source_reference.title if source_reference else _site_name()

    if existing_dataset_key:
        dataset_key = existing_dataset_key
        sync_dataset_contacts(config, dataset_key, contacts)
        trigger_crawl_with_config(config, dataset_key)
    else:
        title = ref_title
        publisher_name = get_publisher_name(config) or getattr(config, 'name', None) or _site_name()
        description = (
            f"Occurrence dataset for {ref_title} uploaded to {publisher_name}."
        )
        title = metadata.get("title") or title
        description = metadata.get("description") or description
        licence = METADATA_LICENCES.get((metadata.get("license") or "").lower())
        dataset_key = register_dataset(
            config, title, description,
            project_identifier=metadata.get("project_identifier", ""),
            license_url=licence[1] if licence else "")
        add_endpoint(config, dataset_key, archive_url)
        sync_dataset_contacts(config, dataset_key, contacts)

    BiologicalCollectionRecord.objects.filter(
        id__in=written_ids
    ).update(dataset_key=dataset_key)

    return {
        "dataset_key": dataset_key,
        "records_published": len(written_ids),
        "archive_url": archive_url,
    }
