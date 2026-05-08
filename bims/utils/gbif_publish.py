# utils/gbif_publish.py
import csv
import os
import zipfile
import uuid
from datetime import datetime
from typing import Iterable, Tuple, List

import requests
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils.timezone import now
from requests.auth import HTTPBasicAuth

from bims.models.biological_collection_record import BiologicalCollectionRecord

LICENSE_URL = getattr(
    settings, "GBIF_DATASET_LICENSE",
    "https://creativecommons.org/publicdomain/zero/1.0/legalcode"
)

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
) -> List[int]:
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
            collection_code = (getattr(getattr(r, "module_group", None), "name", "") or "BIMS")
            catalog_number = str(r.pk)

            recorded_by = (r.collector or "").strip()
            collector_user = None

            if not recorded_by and r.collector_user:
                collector_user = r.collector_user
                recorded_by = (
                        r.collector_user.get_full_name() or
                        r.collector_user.username).strip()
            if not recorded_by and r.owner:
                recorded_by = (
                        r.owner.get_full_name() or
                        r.owner.username).strip()
                if 'admin' in recorded_by.lower():
                    recorded_by = ''
                else:
                    collector_user = r.owner

            row_dataset_name = dataset_name or _site_name()
            inst_code = (r.institution_id or "").strip()

            if collector_user:
                inst_code = collector_user.organization

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
    return written_ids

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
    """Return EML XML snippet for one author (GeoNode Profile/User or BIMS Profile object)."""
    user = author
    try:
        bims_profile = user.bims_profile
        role_name = bims_profile.role.display_name if getattr(bims_profile, 'role', None) else None
    except Exception:
        role_name = None

    given = getattr(user, 'first_name', '').strip()
    sur = getattr(user, 'last_name', '').strip()
    email = getattr(user, 'email', '').strip()
    org = (getattr(user, 'organization', '') or '').strip()

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
        parts.append(f'  <positionName>{role_name}</positionName>')
    if email:
        parts.append(f'  <electronicMailAddress>{email}</electronicMailAddress>')
    return '\n  '.join(parts)


def eml_contact_from_model(contact) -> str:
    """Return EML XML body snippet for a GbifPublishContact model instance.

    Applies the resolved_* properties so blank fields fall back to the linked
    user's profile data.
    """
    given = contact.resolved_given_name
    sur = contact.resolved_sur_name
    org = contact.resolved_organization_name
    position = contact.resolved_position_name
    email = contact.resolved_email
    phone = contact.phone.strip() if contact.phone else ""
    url = contact.online_url.strip() if contact.online_url else ""

    # address sub-fields
    dp = (contact.delivery_point or "").strip()
    city = (contact.city or "").strip()
    postal = (contact.postal_code or "").strip()
    country = (contact.country or "").strip()

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

    role = (getattr(contact, "role", "") or "").strip()
    if role:
        parts.append(f"  <role>{role}</role>")

    return "\n  ".join(parts)


def eml_citation(source_reference) -> str:
    """Return an EML <citation> string for SourceReferenceBibliography or SourceReferenceDocument.
    Returns empty string for any other type.
    """
    from bims.models.source_reference import (
        SourceReferenceBibliography,
        SourceReferenceDocument,
    )
    if not isinstance(source_reference, (SourceReferenceBibliography, SourceReferenceDocument)):
        return ""

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

    parts = []
    if authors:
        parts.append(authors)
    if year:
        parts.append(f"({year})")
    if title:
        parts.append(title)
    if journal_str:
        parts.append(journal_str)
    text = " ".join(parts)
    if doi_str:
        text = f"{text}.{doi_str}."
    elif text:
        text = f"{text}."

    id_attr = f' identifier="{identifier}"' if identifier else ""
    return f"<citation{id_attr}>{text}</citation>"


def intellectual_rights_text(licence=None) -> str:
    """Format a single Licence object into an intellectualRights string."""
    name = (licence.name if licence and licence.name else "Creative Commons CCZero 1.0 License")
    url = (licence.url if licence and licence.url else LICENSE_URL)
    return f"This work is licensed under a {name} {url}."


def write_eml_xml(
        path: str,
        title: str,
        abstract: str,
        contacts: list,
        authors: list = None,
        licences: list = None,
        citation: str = "",
        pub_date: str = ""):
    today = datetime.utcnow().date().isoformat()
    pub_date = pub_date or today
    site = _site_name()

    if authors:
        creator_blocks = "\n".join(
            f"  <creator>\n  {eml_author(a)}\n  </creator>"
            for a in authors
        )
    else:
        creator_blocks = (
            f"  <creator>\n"
            f"    <individualName><surName>{site}</surName></individualName>\n"
            f"    <organizationName>{site}</organizationName>\n"
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
            <title>{title}</title>
            {creator_blocks}
            <pubDate>{pub_date}</pubDate>
            <abstract><para>{abstract}</para></abstract>
            <intellectualRights>
{rights_paras}
            </intellectualRights>
            {contact_blocks}
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
        "license": config.license_url,
    }

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


def build_dwca(
    config,
    records: Iterable[BiologicalCollectionRecord],
    contacts: list,
    source_reference=None,
    out_dir: str = None,
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

    written_ids = write_occurrence_txt(occ_path, records, dataset_name=ref_title)
    if not written_ids:
        raise ValueError("No eligible records to export.")

    title = ref_title
    publisher_name = getattr(config, 'name', None) or _site_name()
    abstract = (
        f"Occurrence dataset for {ref_title} uploaded to {publisher_name}."
    )
    raw_sra = list(source_reference.author_list or []) if source_reference else []
    authors = []
    for sra in raw_sra:
        author_obj = getattr(sra, 'author', sra)
        authors.append(author_obj.user if getattr(author_obj, 'user_id', None) else author_obj)

    seen = set()
    licences = []
    for r in records:
        lic = getattr(r, 'licence', None)
        if lic and lic.pk not in seen:
            seen.add(lic.pk)
            licences.append(lic)
    licences = licences or None

    citation = eml_citation(source_reference) if source_reference else ""

    pub_date = ""
    if source_reference:
        try:
            year = source_reference.year
            if year:
                pub_date = str(year)
        except Exception:
            pass

    write_meta_xml(meta_path)
    write_eml_xml(
        eml_path,
        title,
        abstract,
        contacts=contacts,
        authors=authors,
        licences=licences,
        citation=citation,
        pub_date=pub_date)
    zip_path = zip_dwca(out_dir)

    domain_name = get_domain_name()
    base_url = config.export_base_url.rstrip("/") if config.export_base_url else f'https://{domain_name}/'

    rel_media = os.path.relpath(zip_path, settings.MEDIA_ROOT)
    archive_url = f"{base_url}{_media_url()}/{rel_media}".replace(
        "//", "/").replace(":/", "://")

    return zip_path, archive_url, written_ids


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

    out_dir = archive_url_dir(existing_archive_url) if existing_archive_url else None

    zip_path, archive_url, written_ids = build_dwca(
        config, records, contacts, source_reference, out_dir=out_dir,
    )

    ref_title = source_reference.title if source_reference else _site_name()

    if existing_dataset_key:
        dataset_key = existing_dataset_key
        trigger_crawl_with_config(config, dataset_key)
    else:
        title = ref_title
        publisher_name = getattr(config, 'name', None) or _site_name()
        description = (
            f"Occurrence dataset for {ref_title} uploaded to {publisher_name}."
        )
        dataset_key = register_dataset(config, title, description)
        add_endpoint(config, dataset_key, archive_url)

    BiologicalCollectionRecord.objects.filter(
        id__in=written_ids
    ).update(dataset_key=dataset_key)

    return {
        "dataset_key": dataset_key,
        "records_published": len(written_ids),
        "archive_url": archive_url,
    }
