# coding=utf-8
"""Celery task: harvest species from a remote BIMS instance."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from urllib.parse import urljoin, urlparse

from celery import shared_task
import requests
from django.core.files.base import ContentFile
from django.db import IntegrityError
from django.utils import timezone
from django.utils.text import slugify
from django_tenants.utils import schema_context

logger = logging.getLogger(__name__)

_TAG_COLOUR_RE = re.compile(r'\s*\(#([0-9A-Fa-f]{3,6})\)\s*$')
_REMOTE_LOGO_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg'}


def _parse_tag_list(tag_list_str: str) -> list[tuple[str, str | None]]:
    """
    Parse a tag_list string from BIMS into (tag_name, colour_or_None) pairs.

    Examples
    --------
    "test"                                 → [("test", None)]
    "aquatic (#51FF3E), freshwater (#FF5733)"
                                           → [("aquatic", "#51FF3E"),
                                              ("freshwater", "#FF5733")]
    """
    results = []
    if not tag_list_str:
        return results
    for part in tag_list_str.split(','):
        part = part.strip()
        if not part:
            continue
        m = _TAG_COLOUR_RE.search(part)
        if m:
            colour = f'#{m.group(1).upper()}'
            name = part[:m.start()].strip()
        else:
            colour = None
            name = part
        if name:
            results.append((name, colour))
    return results


def _apply_tags(taxonomy, tag_list_str: str) -> None:
    """
    Parse tag_list_str and add tags to taxonomy.
    If a tag carries a colour, find or create a TagGroup with that colour
    and associate the tag with it.
    """
    from taggit.models import Tag
    from bims.models.tag_group import TagGroup

    parsed = _parse_tag_list(tag_list_str)
    for tag_name, colour in parsed:
        tag, _ = Tag.objects.get_or_create(name=tag_name)
        taxonomy.tags.add(tag_name)

        if colour:
            tag_group = TagGroup.objects.filter(
                colour__iexact=colour,
                tags=tag,
            ).first()
            if not tag_group:
                # Try to find any existing group with this colour to reuse it.
                tag_group = TagGroup.objects.filter(colour__iexact=colour).first()
            if not tag_group:
                tag_group = TagGroup.objects.create(name=tag_name, colour=colour)
            if not tag_group.tags.filter(id=tag.id).exists():
                tag_group.tags.add(tag)


def _compute_taxon_checksum(taxon_data: dict) -> str:
    """
    Return a stable SHA-256 hex digest for the fields of *taxon_data* that
    drive conflict resolution.  Only the keys that the harvest task actually
    reads are included so that irrelevant upstream changes (e.g. metadata
    added to the API payload) don't invalidate a match unnecessarily.

    The digest is over a canonical (sorted-keys, separators-free) JSON
    encoding of a normalised subset of the payload.
    """
    subset = {
        'canonical_name': (taxon_data.get('canonical_name') or '').strip(),
        'rank': (taxon_data.get('rank') or '').strip().upper(),
        'author': (taxon_data.get('author') or '').strip(),
        'taxonomic_status': (taxon_data.get('taxonomic_status') or '').strip().upper(),
        'gbif_key': taxon_data.get('gbif_key'),
        'parent': taxon_data.get('parent'),
        'accepted_taxonomy': taxon_data.get('accepted_taxonomy'),
        'iucn_status_name': (taxon_data.get('iucn_status_name') or '').strip().upper(),
        'iucn_redlist_id': taxon_data.get('iucn_redlist_id'),
        'aphia_id': taxon_data.get('aphia_id'),
        'col_id': (taxon_data.get('col_id') or '').strip(),
        'fada_id': (taxon_data.get('fada_id') or '').strip(),
        'taxonworks_id': taxon_data.get('taxonworks_id'),
        'taxonworks_otu_id': taxon_data.get('taxonworks_otu_id'),
        'additional_data': taxon_data.get('additional_data') or {},
        'tag_list': (taxon_data.get('tag_list') or '').strip(),
    }
    raw = json.dumps(subset, sort_keys=True, separators=(',', ':'), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def _fada_id_taken_by_other(fada_id: str, exclude_pk=None) -> bool:
    """True if some other Taxonomy row already owns this fada_id (unique field)."""
    from bims.models.taxonomy import Taxonomy

    qs = Taxonomy.objects.filter(fada_id=fada_id)
    if exclude_pk is not None:
        qs = qs.exclude(pk=exclude_pk)
    return qs.exists()


def _resolve_remote_logo_url(base_url: str, remote_group_logo: str) -> str:
    """Resolve a remote taxon-group logo to an absolute URL."""
    remote_group_logo = (remote_group_logo or '').strip()
    if not remote_group_logo:
        return ''
    parsed = urlparse(remote_group_logo)
    if parsed.scheme and parsed.netloc:
        return remote_group_logo
    return urljoin(f'{base_url.rstrip("/")}/', remote_group_logo.lstrip('/'))


def _download_group_logo(target_group, base_url: str, remote_group_logo: str, _log=None) -> bool:
    """Download and attach a remote logo to a local taxon group."""
    if not remote_group_logo or target_group.logo:
        return False

    logo_url = _resolve_remote_logo_url(base_url, remote_group_logo)
    try:
        response = requests.get(logo_url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        if _log:
            _log(f"Failed to download remote taxon group logo from {logo_url}: {exc}")
        return False

    ext = os.path.splitext(urlparse(logo_url).path)[1].lower()
    if ext not in _REMOTE_LOGO_EXTENSIONS:
        ext = '.png'

    target_group.logo.save(
        f'{slugify(target_group.name) or f"taxon-group-{target_group.id}"}{ext}',
        ContentFile(response.content),
        save=True,
    )
    if _log:
        _log(f"Saved remote taxon group logo from {logo_url}")
    return True


def _find_or_create_taxonomy(taxon_data: dict, base_url: str,
                              remote_cache: dict,
                              target_group=None,
                              is_readonly: bool = False,
                              _log=None,
                              _visiting: set[int] | None = None) -> object | None:
    """
    Ensure a local Taxonomy record exists for *taxon_data* fetched from a
    remote BIMS instance.  Parents are resolved recursively.

    remote_cache maps remote_id → local Taxonomy (avoids redundant API calls).
    _visiting tracks the active remote-id stack and prevents parent /
    accepted-taxonomy cycles from recursing indefinitely.

    Matching priority
    -----------------
    1. upstream_taxon_id in TaxonGroupTaxonomy  (if target_group provided)
    2. gbif_key  (if present)
    3. canonical_name + rank

    Conflict resolution
    -------------------
    Non-readonly group (default):
      - additional_data : local wins on conflict
      - canonical_name, author, taxonomic_status : never updated
      - parent, accepted_taxonomy, iucn_status, iucn_redlist_id : filled only when missing locally
      - tags : additive only

    Read-only group (is_readonly=True):
      - additional_data : remote wins; overridden local keys are logged
      - canonical_name, author, taxonomic_status : updated when remote differs; logged
      - parent, accepted_taxonomy, iucn_status, iucn_redlist_id : always updated to match upstream
      - tags : additive only (safe default)
    """
    from bims.models.taxonomy import Taxonomy
    from bims.models.taxon_group_taxonomy import TaxonGroupTaxonomy
    from bims.utils.bims_instance import get_taxon_by_id

    def _emit(msg: str):
        if _log:
            _log(msg)
        else:
            logger.debug(msg)

    if _visiting is None:
        _visiting = set()

    remote_id = taxon_data.get('id')
    if remote_id is not None:
        remote_id = int(remote_id)
        cached = remote_cache.get(remote_id)
        if cached is not None:
            return cached
        if remote_id in _visiting:
            _emit(
                f"[CYCLE] Remote taxon id={remote_id} is already being "
                f"resolved; skipping recursive relation"
            )
            return None

    canonical_name = (taxon_data.get('canonical_name') or '').strip()
    rank = (taxon_data.get('rank') or '').strip().upper()
    scientific_name = (taxon_data.get('scientific_name') or canonical_name).strip()
    author = (taxon_data.get('author') or '').strip()
    taxonomic_status = (taxon_data.get('taxonomic_status') or 'ACCEPTED').strip().upper()
    gbif_key = taxon_data.get('gbif_key')
    if gbif_key:
        try:
            gbif_key = int(gbif_key)
        except (TypeError, ValueError):
            gbif_key = None
    aphia_id = taxon_data.get('aphia_id')
    if aphia_id:
        try:
            aphia_id = int(aphia_id)
        except (TypeError, ValueError):
            aphia_id = None
    col_id = (taxon_data.get('col_id') or '').strip() or None
    fada_id = (taxon_data.get('fada_id') or '').strip() or None
    taxonworks_id = taxon_data.get('taxonworks_id')
    if taxonworks_id:
        try:
            taxonworks_id = int(taxonworks_id)
        except (TypeError, ValueError):
            taxonworks_id = None
    taxonworks_otu_id = taxon_data.get('taxonworks_otu_id')
    if taxonworks_otu_id:
        try:
            taxonworks_otu_id = int(taxonworks_otu_id)
        except (TypeError, ValueError):
            taxonworks_otu_id = None

    if not canonical_name:
        return None

    if remote_id is not None:
        _visiting.add(remote_id)

    def _is_remote_synonym(remote_taxon: dict) -> bool:
        return 'SYNONYM' in (
            remote_taxon.get('taxonomic_status') or 'ACCEPTED'
        ).strip().upper()

    def _remote_int(value):
        if value in (None, ''):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # Resolve parent first (depth-first, recursive)
    # ------------------------------------------------------------------
    parent_taxonomy = None
    _is_synonym = 'SYNONYM' in taxonomic_status
    remote_parent_id = taxon_data.get('parent')
    if _is_synonym:
        remote_parent_id = None
    if remote_parent_id:
        remote_parent_id = _remote_int(remote_parent_id)
    if remote_parent_id:
        if remote_parent_id in remote_cache:
            parent_taxonomy = remote_cache[remote_parent_id]
            if getattr(parent_taxonomy, 'is_synonym', False):
                accepted_parent = getattr(parent_taxonomy, 'accepted_taxonomy', None)
                if accepted_parent and accepted_parent.pk != getattr(parent_taxonomy, 'pk', None):
                    parent_taxonomy = accepted_parent
                else:
                    _emit(
                        f"[CYCLE] Skipping cached synonym parent "
                        f"{parent_taxonomy.canonical_name!r} "
                        f"for {canonical_name!r}"
                    )
                    parent_taxonomy = None
        else:
            parent_data = get_taxon_by_id(base_url, remote_parent_id)
            if parent_data:
                if _is_remote_synonym(parent_data):
                    accepted_parent_id = _remote_int(
                        parent_data.get('accepted_taxonomy')
                    )
                    if accepted_parent_id == remote_id:
                        _emit(
                            f"[CYCLE] Skipping synonym parent "
                            f"{parent_data.get('canonical_name')!r} "
                            f"for {canonical_name!r}; its accepted taxonomy "
                            f"is the current taxon"
                        )
                        parent_data = None
                    elif accepted_parent_id in remote_cache:
                        parent_taxonomy = remote_cache[accepted_parent_id]
                        parent_data = None
                    elif accepted_parent_id:
                        parent_data = get_taxon_by_id(
                            base_url, accepted_parent_id
                        )
                    else:
                        _emit(
                            f"[CYCLE] Skipping synonym parent "
                            f"{parent_data.get('canonical_name')!r} "
                            f"for {canonical_name!r}; no accepted taxonomy "
                            f"was provided"
                        )
                        parent_data = None

            if parent_data:
                parent_taxonomy = _find_or_create_taxonomy(
                    parent_data, base_url, remote_cache,
                    target_group=target_group,
                    is_readonly=is_readonly,
                    _log=_log,
                    _visiting=_visiting,
                )

    # ------------------------------------------------------------------
    # Resolve accepted taxonomy for synonyms (depth-first, recursive)
    # ------------------------------------------------------------------
    accepted_taxonomy = None
    remote_accepted_id = taxon_data.get('accepted_taxonomy')
    if _is_synonym and remote_accepted_id:
        remote_accepted_id = _remote_int(remote_accepted_id)
    if _is_synonym and remote_accepted_id:
        if remote_accepted_id in remote_cache:
            accepted_taxonomy = remote_cache[remote_accepted_id]
        else:
            accepted_data = get_taxon_by_id(base_url, remote_accepted_id)
            if accepted_data:
                accepted_taxonomy = _find_or_create_taxonomy(
                    accepted_data, base_url, remote_cache,
                    target_group=target_group,
                    is_readonly=is_readonly,
                    _log=_log,
                    _visiting=_visiting,
                )

    # ------------------------------------------------------------------
    # Find existing local record
    # ------------------------------------------------------------------
    taxonomy = None

    if target_group is not None and remote_id is not None:
        membership = TaxonGroupTaxonomy.objects.filter(
            taxongroup=target_group,
            upstream_taxon_id=str(remote_id),
        ).select_related('taxonomy').first()
        if membership:
            taxonomy = membership.taxonomy

    if taxonomy is None and gbif_key:
        taxonomy = Taxonomy.objects.filter(gbif_key=gbif_key).first()

    if taxonomy is None and rank:
        taxonomy = Taxonomy.objects.filter(
            canonical_name__iexact=canonical_name,
            rank=rank,
        ).first()

    if (
        taxonomy is not None
        and parent_taxonomy is not None
        and parent_taxonomy.pk == taxonomy.pk
    ):
        _emit(
            f"[CYCLE] Skipping self parent for {canonical_name!r} "
            f"(id={taxonomy.pk})"
        )
        parent_taxonomy = None

    # ------------------------------------------------------------------
    # Resolve IUCN status from the remote category string
    # ------------------------------------------------------------------
    iucn_status_name = (taxon_data.get('iucn_status_name') or '').strip().upper()
    iucn_redlist_id = taxon_data.get('iucn_redlist_id')
    if iucn_redlist_id:
        try:
            iucn_redlist_id = int(iucn_redlist_id)
        except (TypeError, ValueError):
            iucn_redlist_id = None

    local_iucn_status = None
    if iucn_status_name:
        from bims.models.iucn_status import IUCNStatus
        # This value populates the global iucn_status field, so only match
        # global (national=False) statuses. National-only categories have no
        # global equivalent and must not leak into the global field.
        local_iucn_status = IUCNStatus.objects.filter(
            category__iexact=iucn_status_name,
            national=False
        ).first()

    # ------------------------------------------------------------------
    # Create if not found
    # ------------------------------------------------------------------
    if taxonomy is None:
        create_kwargs = {
            'canonical_name': canonical_name,
            'scientific_name': scientific_name or canonical_name,
            'rank': rank,
            'author': author,
            'taxonomic_status': taxonomic_status,
        }
        if gbif_key:
            create_kwargs['gbif_key'] = gbif_key
        if aphia_id:
            create_kwargs['aphia_id'] = aphia_id
        if col_id:
            create_kwargs['col_id'] = col_id
        if fada_id:
            if _fada_id_taken_by_other(fada_id):
                _emit(
                    f"[CONFLICT] fada_id {fada_id!r} for {canonical_name!r} is "
                    f"already used by another taxon — not assigning"
                )
            else:
                create_kwargs['fada_id'] = fada_id
        if taxonworks_id:
            create_kwargs['taxonworks_id'] = taxonworks_id
        if taxonworks_otu_id:
            create_kwargs['taxonworks_otu_id'] = taxonworks_otu_id
        if parent_taxonomy:
            create_kwargs['parent'] = parent_taxonomy
        if accepted_taxonomy:
            create_kwargs['accepted_taxonomy'] = accepted_taxonomy
        if local_iucn_status:
            create_kwargs['iucn_status'] = local_iucn_status
        if iucn_redlist_id:
            create_kwargs['iucn_redlist_id'] = iucn_redlist_id
        taxonomy = Taxonomy.objects.create(**create_kwargs)

    # ------------------------------------------------------------------
    # Update existing record - policy differs by group type
    # ------------------------------------------------------------------
    else:
        update_fields = []

        if is_readonly:
            # --- Read-only: upstream is source of truth ---
            if taxonomy.canonical_name != canonical_name:
                _emit(
                    f"[DIVERGENCE] {taxonomy.canonical_name} (id={taxonomy.pk}): "
                    f"canonical_name local={taxonomy.canonical_name!r} "
                    f"remote={canonical_name!r} — updating"
                )
                taxonomy.canonical_name = canonical_name
                taxonomy.scientific_name = scientific_name or canonical_name
                update_fields += ['canonical_name', 'scientific_name']

            if author and taxonomy.author != author:
                _emit(
                    f"[DIVERGENCE] {taxonomy.canonical_name} (id={taxonomy.pk}): "
                    f"author local={taxonomy.author!r} remote={author!r} — updating"
                )
                taxonomy.author = author
                update_fields.append('author')

            if taxonomic_status and (taxonomy.taxonomic_status or '').upper() != taxonomic_status:
                _emit(
                    f"[DIVERGENCE] {taxonomy.canonical_name} (id={taxonomy.pk}): "
                    f"taxonomic_status local={taxonomy.taxonomic_status!r} "
                    f"remote={taxonomic_status!r} - updating"
                )
                taxonomy.taxonomic_status = taxonomic_status
                update_fields.append('taxonomic_status')

            if parent_taxonomy and taxonomy.parent_id != parent_taxonomy.pk:
                _emit(
                    f"[DIVERGENCE] {taxonomy.canonical_name} (id={taxonomy.pk}): "
                    f"parent local={taxonomy.parent_id!r} "
                    f"remote={parent_taxonomy.pk!r} — updating"
                )
                taxonomy.parent = parent_taxonomy
                update_fields.append('parent')

            if accepted_taxonomy and taxonomy.accepted_taxonomy_id != accepted_taxonomy.pk:
                _emit(
                    f"[DIVERGENCE] {taxonomy.canonical_name} (id={taxonomy.pk}): "
                    f"accepted_taxonomy local={taxonomy.accepted_taxonomy_id!r} "
                    f"remote={accepted_taxonomy.pk!r} — updating"
                )
                taxonomy.accepted_taxonomy = accepted_taxonomy
                update_fields.append('accepted_taxonomy')

            if _is_synonym and taxonomy.parent_id:
                _emit(
                    f"[DIVERGENCE] {taxonomy.canonical_name} (id={taxonomy.pk}): "
                    f"parent local={taxonomy.parent_id!r} remote=None — clearing "
                    f"because synonyms use accepted_taxonomy"
                )
                taxonomy.parent = None
                update_fields.append('parent')

            if local_iucn_status and taxonomy.iucn_status_id != local_iucn_status.pk:
                taxonomy.iucn_status = local_iucn_status
                update_fields.append('iucn_status')

            if iucn_redlist_id and taxonomy.iucn_redlist_id != iucn_redlist_id:
                taxonomy.iucn_redlist_id = iucn_redlist_id
                update_fields.append('iucn_redlist_id')

            if aphia_id and taxonomy.aphia_id != aphia_id:
                taxonomy.aphia_id = aphia_id
                update_fields.append('aphia_id')

            if col_id and taxonomy.col_id != col_id:
                taxonomy.col_id = col_id
                update_fields.append('col_id')

            if fada_id and taxonomy.fada_id != fada_id:
                if _fada_id_taken_by_other(fada_id, exclude_pk=taxonomy.pk):
                    _emit(
                        f"[CONFLICT] {taxonomy.canonical_name} (id={taxonomy.pk}): "
                        f"fada_id {fada_id!r} is already used by another taxon "
                        f"— not assigning"
                    )
                else:
                    taxonomy.fada_id = fada_id
                    update_fields.append('fada_id')

            if taxonworks_id and taxonomy.taxonworks_id != taxonworks_id:
                taxonomy.taxonworks_id = taxonworks_id
                update_fields.append('taxonworks_id')

            if taxonworks_otu_id and taxonomy.taxonworks_otu_id != taxonworks_otu_id:
                taxonomy.taxonworks_otu_id = taxonworks_otu_id
                update_fields.append('taxonworks_otu_id')

        else:
            if parent_taxonomy and not taxonomy.parent:
                taxonomy.parent = parent_taxonomy
                update_fields.append('parent')

            if accepted_taxonomy and not taxonomy.accepted_taxonomy_id:
                taxonomy.accepted_taxonomy = accepted_taxonomy
                update_fields.append('accepted_taxonomy')

            if local_iucn_status and not taxonomy.iucn_status_id:
                taxonomy.iucn_status = local_iucn_status
                update_fields.append('iucn_status')

            if iucn_redlist_id and not taxonomy.iucn_redlist_id:
                taxonomy.iucn_redlist_id = iucn_redlist_id
                update_fields.append('iucn_redlist_id')

            if aphia_id and not taxonomy.aphia_id:
                taxonomy.aphia_id = aphia_id
                update_fields.append('aphia_id')

            if col_id and not taxonomy.col_id:
                taxonomy.col_id = col_id
                update_fields.append('col_id')

            if fada_id and not taxonomy.fada_id:
                if _fada_id_taken_by_other(fada_id, exclude_pk=taxonomy.pk):
                    _emit(
                        f"[CONFLICT] {taxonomy.canonical_name} (id={taxonomy.pk}): "
                        f"fada_id {fada_id!r} is already used by another taxon "
                        f"— not assigning"
                    )
                else:
                    taxonomy.fada_id = fada_id
                    update_fields.append('fada_id')

            if taxonworks_id and not taxonomy.taxonworks_id:
                taxonomy.taxonworks_id = taxonworks_id
                update_fields.append('taxonworks_id')

            if taxonworks_otu_id and not taxonomy.taxonworks_otu_id:
                taxonomy.taxonworks_otu_id = taxonworks_otu_id
                update_fields.append('taxonworks_otu_id')

        if update_fields:
            taxonomy.save(update_fields=update_fields)

    # ------------------------------------------------------------------
    # Merge additional_data
    # ------------------------------------------------------------------
    remote_additional = taxon_data.get('additional_data')
    if isinstance(remote_additional, dict) and remote_additional:
        existing = taxonomy.additional_data or {}

        if is_readonly:
            overridden = {
                k: existing[k]
                for k in remote_additional
                if k in existing and existing[k] != remote_additional[k]
            }
            if overridden:
                _emit(
                    f"[DIVERGENCE] {taxonomy.canonical_name} (id={taxonomy.pk}): "
                    f"additional_data keys overridden by upstream: "
                    + ", ".join(
                        f"{k}={existing[k]!r}→{remote_additional[k]!r}"
                        for k in overridden
                    )
                )
            merged = {**existing, **remote_additional}
        else:
            merged = {**remote_additional, **existing}

        if merged != taxonomy.additional_data:
            taxonomy.additional_data = merged
            taxonomy.save(update_fields=['additional_data'])

    # ------------------------------------------------------------------
    # Apply tags (additive for both group types)
    # ------------------------------------------------------------------
    tag_list_str = taxon_data.get('tag_list') or ''
    if tag_list_str:
        _apply_tags(taxonomy, tag_list_str)

    # Cache and return
    if remote_id is not None:
        remote_cache[remote_id] = taxonomy
        _visiting.discard(remote_id)
    return taxonomy


@shared_task(name='bims.tasks.harvest_bims_species', queue='update', ignore_result=True)
def harvest_bims_species(session_id: int, schema_name: str):
    from bims.models import HarvestSession
    from bims.models.taxon_group import TaxonGroup
    from bims.enums.taxonomic_group_category import TaxonomicGroupCategory
    from bims.signals.utils import connect_bims_signals, disconnect_bims_signals
    from bims.utils.bims_instance import (
        get_all_taxa,
        get_taxon_groups,
        normalize_bims_base_url,
    )

    with schema_context(schema_name):
        try:
            session = HarvestSession.objects.get(id=session_id)
        except HarvestSession.DoesNotExist:
            logger.error("harvest_bims_species: session %s not found", session_id)
            return

        def _log(msg: str):
            ts = timezone.now().isoformat(timespec="seconds")
            line = f"[{ts}] {msg}\n"
            logger.info("BIMS harvest session=%s: %s", session_id, msg)
            if session.log_file:
                try:
                    with open(session.log_file.path, "a") as fh:
                        fh.write(line)
                except Exception:
                    pass

        disconnect_bims_signals()

        if not session.status or session.status == "queued":
            session.status = "Processing"
            session.save(update_fields=["status"])

        additional = session.additional_data or {}
        base_url = normalize_bims_base_url((additional.get("base_url") or "").strip())
        remote_group_id = additional.get("remote_group_id")
        remote_group_name = (additional.get("remote_group_name") or "").strip()
        remote_group_logo = (additional.get("remote_group_logo") or "").strip()
        import_mode = additional.get("import_mode", "existing")

        if not base_url or not remote_group_id:
            _log("Missing base_url or remote_group_id — aborting")
            HarvestSession.objects.filter(id=session_id).update(
                status="Failed: incomplete config",
                finished=True,
            )
            connect_bims_signals()
            return

        remote_group_id = int(remote_group_id)

        # ------------------------------------------------------------------
        # Resolve target taxon group
        # ------------------------------------------------------------------
        if import_mode == "new":
            group_name = remote_group_name or f"Imported from {base_url} (group {remote_group_id})"
            if not remote_group_name or not remote_group_logo:
                for rg in get_taxon_groups(base_url):
                    if int(rg.get('id', 0)) == remote_group_id:
                        group_name = rg.get('name', group_name)
                        remote_group_logo = remote_group_logo or (rg.get('logo') or '').strip()
                        break

            from django.contrib.sites.models import Site
            mark_readonly = additional.get("mark_readonly", False)

            try:
                target_group, created = TaxonGroup.objects.get_or_create(
                    name=group_name,
                    defaults={
                        'category': TaxonomicGroupCategory.SPECIES_MODULE.name,
                        'site': Site.objects.get_current(),
                        'is_readonly': mark_readonly,
                        'upstream_url': base_url if mark_readonly else '',
                        'upstream_id': str(remote_group_id) if mark_readonly else '',
                    },
                )
            except IntegrityError:
                # Recover from concurrent insert / sequence desync by fetching
                # the logical group by name.
                target_group = TaxonGroup.objects.get(name=group_name)
                created = False
            if not created and mark_readonly:
                # Ensure upstream metadata is kept up to date on re-harvest
                update_fields = []
                if not target_group.upstream_url:
                    target_group.upstream_url = base_url
                    update_fields.append('upstream_url')
                if not target_group.upstream_id:
                    target_group.upstream_id = str(remote_group_id)
                    update_fields.append('upstream_id')
                if not target_group.is_readonly:
                    target_group.is_readonly = True
                    update_fields.append('is_readonly')
                if update_fields:
                    target_group.save(update_fields=update_fields)
            _download_group_logo(target_group, base_url, remote_group_logo, _log=_log)
            action_word = "Created" if created else "Using existing"
            _log(f"{action_word} taxon group: '{group_name}' (id={target_group.id})"
                 + (" [read-only]" if target_group.is_readonly else ""))
            session.module_group = target_group
            session.save(update_fields=["module_group"])
        else:
            target_group = session.module_group
            if not target_group:
                _log("No local taxon group selected — aborting")
                HarvestSession.objects.filter(id=session_id).update(
                    status="Failed: no local taxon group",
                    finished=True,
                )
                connect_bims_signals()
                return

            # For read-only groups, verify the harvest source matches
            if target_group.is_readonly and target_group.upstream_url and target_group.upstream_id:
                if (
                    target_group.upstream_url.rstrip('/') != base_url.rstrip('/')
                    or str(target_group.upstream_id) != str(remote_group_id)
                ):
                    _log(
                        f"Read-only group '{target_group.name}' is bound to "
                        f"{target_group.upstream_url} group {target_group.upstream_id}. "
                        f"Cannot harvest from {base_url} group {remote_group_id} — aborting."
                    )
                    HarvestSession.objects.filter(id=session_id).update(
                        status="Failed: upstream source mismatch for read-only group",
                        finished=True,
                    )
                    connect_bims_signals()
                    return

            if not target_group.logo:
                _logo_path = ''
                for rg in get_taxon_groups(base_url):
                    if int(rg.get('id', 0)) == remote_group_id:
                        _logo_path = (rg.get('logo') or '').strip()
                        break
                if _logo_path:
                    _download_group_logo(target_group, base_url, _logo_path, _log=_log)

        _log(
            f"Starting BIMS harvest from {base_url} "
            f"(remote group id={remote_group_id}, name='{remote_group_name}') "
            f"→ local group '{target_group.name}'"
        )

        from preferences import preferences
        auto_validate = preferences.SiteSetting.auto_validate_taxa_on_upload

        # remote_id → local Taxonomy cache, shared across all taxa in this run
        remote_cache: dict = {}

        total_processed = 0
        total_skipped = 0
        save_interval = 50
        since_last_save = 0

        from bims.models.taxon_group_taxonomy import TaxonGroupTaxonomy

        harvest_start_time = timezone.now()

        for taxon_data in get_all_taxa(base_url, remote_group_id):
            if HarvestSession.objects.filter(id=session_id, canceled=True).exists():
                _log("Harvest canceled by user")
                break

            canonical_name = (taxon_data.get('canonical_name') or '').strip()
            rank = (taxon_data.get('rank') or 'unknown').strip()

            if not canonical_name:
                total_skipped += 1
                continue

            try:
                remote_taxon_id = taxon_data.get('id')
                remote_taxon_id_str = str(remote_taxon_id) if remote_taxon_id is not None else ''

                # ----------------------------------------------------------
                # Checksum-based skip: if the upstream payload is unchanged
                # since the last harvest we can skip conflict resolution and
                # taxonomy updates entirely.
                # ----------------------------------------------------------
                checksum = _compute_taxon_checksum(taxon_data)
                existing_membership = None
                if remote_taxon_id_str:
                    existing_membership = TaxonGroupTaxonomy.objects.filter(
                        taxongroup=target_group,
                        upstream_taxon_id=remote_taxon_id_str,
                    ).first()

                if (
                    existing_membership is not None
                    and existing_membership.upstream_checksum == checksum
                ):
                    _log(
                        f"[SKIP] {canonical_name} ({rank}) — upstream unchanged "
                        f"(checksum={checksum[:8]}…)"
                    )
                    # Still refresh last_synced_at so stale-detection does not
                    # flag this taxon as deleted upstream on the next cleanup pass.
                    TaxonGroupTaxonomy.objects.filter(
                        id=existing_membership.id
                    ).update(last_synced_at=timezone.now())
                    total_skipped += 1
                    continue

                taxonomy = _find_or_create_taxonomy(
                    taxon_data, base_url, remote_cache,
                    target_group=target_group,
                    is_readonly=target_group.is_readonly,
                    _log=_log,
                )
                if taxonomy is None:
                    total_skipped += 1
                    continue

                now = timezone.now()

                # Add to target group (no-op if already a member)
                target_group.taxonomies.add(
                    taxonomy,
                    through_defaults={
                        'is_validated': auto_validate,
                        'upstream_taxon_id': remote_taxon_id_str,
                        'upstream_checksum': checksum,
                        'last_synced_at': now,
                    },
                )
                # Update membership fields that through_defaults only sets on INSERT
                update_fields = {
                    'upstream_checksum': checksum,
                    'last_synced_at': now,
                }
                if not auto_validate:
                    update_fields['is_validated'] = False
                if remote_taxon_id_str:
                    update_fields['upstream_taxon_id'] = remote_taxon_id_str
                target_group.taxonomies.through.objects.filter(
                    taxongroup=target_group,
                    taxonomy=taxonomy,
                ).update(**update_fields)

                total_processed += 1
                since_last_save += 1
                _log(f"[{total_processed}] Processed: {canonical_name} ({rank})")

            except Exception as exc:
                _log(f"Error processing '{canonical_name}': {exc}")
                total_skipped += 1
                continue

            if since_last_save >= save_interval:
                session.status = f"Processing ({total_processed} taxa)"
                session.save(update_fields=["status"])
                since_last_save = 0

        finished_at = timezone.now().isoformat(timespec="seconds")
        final_additional_data = {
            **additional,
            "finished_at": finished_at,
            "total_processed": total_processed,
            "total_skipped": total_skipped,
        }

        if not HarvestSession.objects.filter(id=session_id, canceled=True).exists():
            _log(
                f"Harvest complete — {total_processed} taxa processed, "
                f"{total_skipped} skipped"
            )

            if target_group.is_readonly:
                _remove_stale_taxa(target_group, harvest_start_time, _log)
                _ensure_bims_monthly_schedule(target_group, base_url, remote_group_id, remote_group_name, _log)

            HarvestSession.objects.filter(id=session_id).update(
                status=f"Finished ({total_processed} taxa)",
                finished=True,
                additional_data=final_additional_data,
            )
        else:
            HarvestSession.objects.filter(id=session_id).update(
                status=f"Canceled ({total_processed} taxa before cancel)",
                additional_data=final_additional_data,
            )

        connect_bims_signals()


def _remove_stale_taxa(target_group, harvest_start_time, _log=None):
    """
    Remove taxa from *target_group* that were not touched during the harvest
    run that started at *harvest_start_time*.  A taxon is considered deleted
    upstream when its TaxonGroupTaxonomy.last_synced_at predates the run start
    and the record was originally created by a harvest (last_synced_at is not
    null - null means manually added, which we leave alone).

    After unlinking from the group the underlying Taxonomy record is deleted
    only when it has no other group memberships AND no occurrence records.
    """
    from bims.models.taxon_group_taxonomy import TaxonGroupTaxonomy
    from bims.models.taxonomy import Taxonomy
    from bims.models import BiologicalCollectionRecord, TaxonomyUpdateProposal

    def _emit(msg):
        if _log:
            _log(msg)
        else:
            logger.info(msg)

    stale_memberships = TaxonGroupTaxonomy.objects.filter(
        taxongroup=target_group,
        last_synced_at__isnull=False,
        last_synced_at__lt=harvest_start_time,
    ).select_related('taxonomy')

    if not stale_memberships.exists():
        _emit("[STALE] No stale taxa detected.")
        return

    total_unlinked = 0
    total_deleted = 0
    total_skipped = 0

    for membership in stale_memberships:
        taxonomy = membership.taxonomy

        # Skip if there is a pending proposal - leave it for manual review.
        if TaxonomyUpdateProposal.objects.filter(
            original_taxonomy=taxonomy,
            status='pending',
        ).exists():
            _emit(
                f"[STALE] Skipping {taxonomy.canonical_name} (id={taxonomy.pk}) "
                f"— has a pending update proposal"
            )
            total_skipped += 1
            continue

        _emit(
            f"[STALE] Removing {taxonomy.canonical_name} (id={taxonomy.pk}) "
            f"from group '{target_group.name}' — not seen in upstream harvest"
        )
        target_group.taxonomies.remove(taxonomy)
        total_unlinked += 1

        still_in_other_groups = TaxonGroupTaxonomy.objects.filter(
            taxonomy=taxonomy
        ).exists()
        has_occurrences = BiologicalCollectionRecord.objects.filter(
            taxonomy=taxonomy
        ).exists()

        if not still_in_other_groups and not has_occurrences:
            _emit(
                f"[STALE] Deleting {taxonomy.canonical_name} (id={taxonomy.pk}) "
                f"— no remaining group memberships and no occurrences"
            )
            taxonomy.delete()
            total_deleted += 1

    _emit(
        f"[STALE] Cleanup complete — {total_unlinked} unlinked, "
        f"{total_deleted} deleted, {total_skipped} skipped (pending proposals)"
    )


def _ensure_bims_monthly_schedule(target_group, base_url, remote_group_id, remote_group_name, _log=None):
    """Create or update a monthly BIMS HarvestSchedule for a read-only taxon group."""
    from bims.models.harvest_schedule import HarvestSchedule, HarvestPeriod, HarvestScheduleCategory

    schedule, created = HarvestSchedule.objects.get_or_create(
        module_group=target_group,
        category=HarvestScheduleCategory.BIMS,
        defaults={
            'period': HarvestPeriod.MONTHLY,
            'day_of_month': 1,
            'enabled': True,
            'is_fetching_species': True,
            'bims_config': {
                'base_url': base_url,
                'remote_group_id': remote_group_id,
                'remote_group_name': remote_group_name,
            },
        },
    )
    if not created:
        update_fields = []
        if not schedule.enabled:
            schedule.enabled = True
            update_fields.append('enabled')
        config = schedule.bims_config or {}
        new_config = {
            'base_url': base_url,
            'remote_group_id': remote_group_id,
            'remote_group_name': remote_group_name,
        }
        if config != new_config:
            schedule.bims_config = new_config
            update_fields.append('bims_config')
        if update_fields:
            schedule.save(update_fields=update_fields)

    if _log:
        action = "Created" if created else "Updated"
        _log(
            f"{action} monthly BIMS harvest schedule (id={schedule.id}) "
            f"for group '{target_group.name}'"
        )
