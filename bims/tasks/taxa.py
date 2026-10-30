# coding=utf-8
import time
from typing import Iterable, Set

from celery import shared_task
from celery.utils.log import get_task_logger
from django.contrib.auth import get_user_model
from django.core.exceptions import FieldError

from django.db import transaction, connection
from django.conf import settings
from django.db.models import Count

from bims.utils.mail import mail_superusers, get_domain_name
from bims.utils.iucn import (
    get_iucn_status, get_iucn_assessments, normalize_iucn_category_code,
    fetch_iucn_data, parse_latest_global_status, IUCNRateLimitError,
)
from tenants.models import Domain

logger = get_task_logger(__name__)


@shared_task(name='bims.tasks.fetch_vernacular_names', queue='update')
def fetch_vernacular_names(taxa_ids: []):
    from bims.models.taxonomy import Taxonomy
    from bims.utils.fetch_gbif import fetch_gbif_vernacular_names

    taxa = Taxonomy.objects.filter(
        id__in=[str(taxa_id) for taxa_id in taxa_ids]
    )
    for taxon in taxa:
        fetch_gbif_vernacular_names(taxon)


@shared_task(name="bims.tasks.fetch_iucn_status", queue="update")
def fetch_iucn_status(
    taxa_ids: list[int] | None = None,
    *,
    batch_size: int = 1000,
    session_id: int | None = None,
) -> str:
    """
    Harvest (or update) IUCN Red-List information for the requested taxa.

    Creates/updates a HarvestSession (category='iucn') so callers can poll
    progress via the harvest-status API endpoint.
    """
    from bims.models import Taxonomy, IUCNAssessment, IUCNStatus, HarvestSession

    STATUS_INTERVAL = 10  # update session status and check cancel every N taxa

    def _has_meaningful_iucn_data(taxon_obj: Taxonomy | None) -> bool:
        if not taxon_obj:
            return False
        status = getattr(taxon_obj, "iucn_status", None)
        if status and getattr(status, "category", None) and status.category != "NE":
            return True
        data = getattr(taxon_obj, "iucn_data", None) or {}
        if isinstance(data, dict):
            return bool(data.get("url"))
        return bool(data)

    def _queue_for_update(obj: Taxonomy, bucket: list[Taxonomy], seen: set[int]):
        if obj.id not in seen:
            bucket.append(obj)
            seen.add(obj.id)

    log_file = None

    def _log(msg: str):
        logger.info(msg)
        if log_file:
            log_file.write(msg + '\n')
            log_file.flush()

    def _set_session_status(session, msg: str):
        if session is None:
            return
        session.status = msg
        session.save(update_fields=["status"])

    session = None
    if session_id:
        try:
            session = HarvestSession.objects.get(id=session_id)
        except HarvestSession.DoesNotExist:
            pass

    if session is None:
        session = HarvestSession.objects.create(
            category="iucn",
            status="queued",
        )

    if session.log_file and session.log_file.name:
        try:
            log_file = open(session.log_file.path, 'a')
        except (OSError, ValueError):
            pass

    t0 = time.perf_counter()

    try:
        qs = Taxonomy.objects.all()

        if taxa_ids:
            qs = qs.filter(id__in=taxa_ids)

        qs = qs.filter(rank__in=["SPECIES", "SUBSPECIES"]).select_related(
            "accepted_taxonomy",
            "iucn_status",
            "accepted_taxonomy__iucn_status",
            "origin",
        )

        total = qs.count()
        _log(f"Starting IUCN sync for {total} taxon record(s).")
        _set_session_status(session, f"Processing (0 / {total})")

        to_update: list[Taxonomy] = []
        queued_ids: set[int] = set()
        valid_categories = {c[0] for c in IUCNStatus.CATEGORY_CHOICES}
        assessments_created = 0
        assessments_updated = 0
        processed = 0

        for taxon in qs:
            if HarvestSession.objects.filter(id=session.id, canceled=True).exists():
                _log(f"Harvest canceled after {processed} taxa.")
                session.status = f"Canceled ({processed} of {total} processed)"
                session.finished = True
                session.canceled = True
                session.save(update_fields=["status", "finished", "canceled"])
                break

            if taxon.origin and getattr(taxon.origin, 'origin_key', '').startswith('alien'):
                processed += 1
                continue

            iucn_payload = fetch_iucn_data(taxon)

            if iucn_payload is None:
                _log(
                    f"Taxon {taxon.id} ({taxon.canonical_name}): "
                    f"not found in IUCN or API error, skipping."
                )

            status_obj, sis_id, url = parse_latest_global_status(iucn_payload)

            changed = False

            if status_obj is not None:
                if taxon.iucn_status_id != status_obj.id:
                    taxon.iucn_status = status_obj
                    changed = True

                if sis_id and taxon.iucn_redlist_id != sis_id:
                    taxon.iucn_redlist_id = sis_id
                    changed = True

                if url:
                    if not taxon.iucn_data or "url" not in taxon.iucn_data:
                        changed = True
                    taxon.iucn_data = {"url": url}

                if changed:
                    _log(
                        f"Taxon {taxon.id} ({taxon.canonical_name}): "
                        f"status -> {status_obj.category}, sis_id -> {sis_id or '-'}"
                    )
                    _queue_for_update(taxon, to_update, queued_ids)

                accepted = taxon.accepted_taxonomy
                is_synonym = "SYNONYM" in (taxon.taxonomic_status or "").upper()
                accepted_changed = False

                if (
                    is_synonym
                    and accepted
                    and status_obj.category != "NE"
                    and not _has_meaningful_iucn_data(accepted)
                ):
                    if accepted.iucn_status_id != status_obj.id:
                        accepted.iucn_status = status_obj
                        accepted_changed = True

                    if sis_id and accepted.iucn_redlist_id != sis_id:
                        accepted.iucn_redlist_id = sis_id
                        accepted_changed = True

                    if url:
                        accepted_data = accepted.iucn_data if isinstance(accepted.iucn_data, dict) else {}
                        if accepted_data.get("url") != url:
                            accepted.iucn_data = {"url": url}
                            accepted_changed = True

                    if accepted_changed:
                        logger.debug(
                            "Propagated IUCN data from synonym %s -> accepted %s",
                            taxon.id,
                            accepted.id,
                        )
                        _queue_for_update(accepted, to_update, queued_ids)

            assessments, assessment_sis_id = (
                get_iucn_assessments(taxon=taxon, json_result=iucn_payload)
                if iucn_payload is not None else ([], None)
            )
            if assessments:
                data_by_id = {}
                for assessment in assessments:
                    assessment_id = assessment.get("assessment_id")
                    if not assessment_id:
                        continue

                    scope = (assessment.get("scopes") or [{}])[0] or {}
                    scope_desc = scope.get("description") or {}
                    scope_label = scope_desc.get("en", "")

                    year = assessment.get("year_published")
                    try:
                        year_published = int(year) if year else None
                    except (TypeError, ValueError):
                        year_published = None

                    normalized_code = normalize_iucn_category_code(
                        assessment.get("red_list_category_code")
                    )
                    status_assessment = None
                    if normalized_code and normalized_code in valid_categories:
                        try:
                            status_assessment, _ = IUCNStatus.objects.get_or_create(
                                category=normalized_code,
                                national=False
                            )
                        except IUCNStatus.MultipleObjectsReturned:
                            status_assessment = IUCNStatus.objects.filter(
                                category=normalized_code,
                                national=False
                            ).first()

                    data_by_id[assessment_id] = {
                        "assessment_id": assessment_id,
                        "sis_taxon_id": assessment.get("sis_taxon_id") or assessment_sis_id,
                        "year_published": year_published,
                        "latest": bool(assessment.get("latest")),
                        "possibly_extinct": bool(assessment.get("possibly_extinct")),
                        "possibly_extinct_in_the_wild": bool(
                            assessment.get("possibly_extinct_in_the_wild")
                        ),
                        "red_list_category_code": assessment.get("red_list_category_code") or "",
                        "normalized_status": status_assessment,
                        "url": assessment.get("url") or "",
                        "scope_code": scope.get("code") or "",
                        "scope_label": scope_label,
                        "raw_data": assessment,
                    }

                assessment_ids = list(data_by_id.keys())
                if assessment_ids:
                    existing = IUCNAssessment.objects.filter(
                        taxonomy=taxon,
                        assessment_id__in=assessment_ids
                    )
                    existing_by_id = {a.assessment_id: a for a in existing}

                    to_create = [
                        IUCNAssessment(taxonomy=taxon, **data_by_id[assessment_id])
                        for assessment_id in assessment_ids
                        if assessment_id not in existing_by_id
                    ]
                    if to_create:
                        IUCNAssessment.objects.bulk_create(
                            to_create,
                            batch_size=batch_size,
                            ignore_conflicts=True,
                        )

                    all_objects = IUCNAssessment.objects.filter(
                        taxonomy=taxon,
                        assessment_id__in=assessment_ids
                    )
                    assessments_created += max(
                        0, all_objects.count() - len(existing_by_id)
                    )

                    to_update_assessments = []
                    for obj in all_objects:
                        data = data_by_id.get(obj.assessment_id)
                        if not data:
                            continue
                        for field, value in data.items():
                            setattr(obj, field, value)
                        to_update_assessments.append(obj)

                    if to_update_assessments:
                        IUCNAssessment.objects.bulk_update(
                            to_update_assessments,
                            [
                                "assessment_id",
                                "sis_taxon_id",
                                "year_published",
                                "latest",
                                "possibly_extinct",
                                "possibly_extinct_in_the_wild",
                                "red_list_category_code",
                                "normalized_status",
                                "url",
                                "scope_code",
                                "scope_label",
                                "raw_data",
                            ],
                            batch_size,
                        )
                        assessments_updated += len(to_update_assessments)

            processed += 1
            if processed % STATUS_INTERVAL == 0:
                _set_session_status(session, f"Processing ({processed} / {total})")
                _log(f"Progress: {processed} / {total} taxa processed.")

        if to_update:
            with transaction.atomic():
                Taxonomy.objects.bulk_update(
                    to_update, [
                        "iucn_status", "iucn_redlist_id", "iucn_data"
                    ], batch_size
                )

        dt = time.perf_counter() - t0

        session.refresh_from_db(fields=["canceled"])
        if session.canceled:
            msg = f"Canceled ({processed} of {total} processed)"
            _log(msg)
            session.status = msg
            session.finished = True
            session.save(update_fields=["status", "finished"])
            return msg

        summary = (
            f"Updated {len(to_update)} taxonomy record(s). "
            f"Assessments: {assessments_created} created, {assessments_updated} updated."
        )
        _log(
            f"IUCN sync finished: {len(to_update)}/{total} taxa updated, "
            f"{assessments_created} assessments created, "
            f"{assessments_updated} assessments updated in {dt:.2f}s."
        )

        session.status = f"Finished - {summary}"
        session.finished = True
        session.additional_data = {
            "taxa_updated": len(to_update),
            "total": total,
            "assessments_created": assessments_created,
            "assessments_updated": assessments_updated,
            "duration_seconds": round(dt, 2),
        }
        session.save(update_fields=["status", "finished", "additional_data"])
        if log_file:
            log_file.close()
        return summary

    except Exception as exc:
        error_msg = f"Failed: {exc}"
        _log(error_msg)
        logger.exception("IUCN sync failed: %s", exc)
        if session:
            session.status = error_msg
            session.finished = True
            session.save(update_fields=["status", "finished"])
        if log_file:
            log_file.close()
        raise


def _iter_queryset(qs, *, chunk_size: int = 1000) -> Iterable:
    """
    Memory-safe iterator over a queryset.
    """
    return qs.iterator(chunk_size=chunk_size)


@shared_task(name="bims.tasks.approve_unvalidated_taxa_by_group", queue="update", bind=True)
def approve_unvalidated_taxa_by_group(
    self,
    taxon_group_id: int,
    initiated_by_user_id: int,
    include_children: bool = True,
    chunk_size: int = 500,
) -> str:
    """
    Approve all taxonomy update proposals for a given taxon group.

    Args:
        taxon_group_id: The TaxonGroup to act on.
        initiated_by_user_id: The user performing the approvals (used for auditing).
        include_children: If True, also approve proposals under descendant groups.
        only_status: Limit to proposals in these statuses. Defaults to ["pending"].
        chunk_size: Iterator chunk size for DB reads.

    Returns:
        A short summary string.
    """
    t0 = time.perf_counter()

    from bims.models import TaxonGroup, TaxonGroupTaxonomy, TaxonomyUpdateProposal, Taxonomy
    from bims.api_views.taxon_update import create_taxon_proposal
    from bims.tasks.send_notification import send_mail_notification
    from bims.utils.domain import get_current_domain

    User = get_user_model()

    try:
        actor = User.objects.get(pk=initiated_by_user_id)
    except User.DoesNotExist:
        msg = f"Actor user {initiated_by_user_id} does not exist."
        logger.error(msg)
        return msg

    try:
        root_group = TaxonGroup.objects.get(pk=taxon_group_id)
    except TaxonGroup.DoesNotExist:
        msg = f"TaxonGroup {taxon_group_id} does not exist."
        logger.error(msg)
        return msg

    group_ids = [root_group.id]
    if include_children:
        try:
            group_ids.extend([g.id for g in root_group.get_all_children()])
        except Exception as exc:
            logger.exception("Failed to collect descendant groups for %s: %s", root_group.id, exc)

    tgt_qs = (
        TaxonGroupTaxonomy.objects
        .filter(taxongroup_id__in=group_ids,
                is_validated=False,
                is_rejected=False)
        # Unaccepted taxa stay unpublished, visible to experts only
        .exclude(taxonomy__taxonomic_status="UNACCEPTED")
        .select_related("taxonomy", "taxongroup")
        .order_by("id")
    )

    total = tgt_qs.count()
    if total == 0:
        msg = f"No unvalidated taxa found for {root_group.name}."
        logger.info(msg)
        return msg

    logger.info(
        "Approving %s proposal(s) for %s (include_children=%s).",
        total, root_group.name, include_children
    )

    created = 0
    approved = 0
    failed = 0
    failed_ids: list[int] = []
    approved_taxa_names: list[str] = []

    for link in _iter_queryset(tgt_qs, chunk_size=chunk_size):
        taxon: Taxonomy = link.taxonomy
        group: TaxonGroup = link.taxongroup

        proposal = (
            TaxonomyUpdateProposal.objects
            .filter(
                original_taxonomy_id=taxon.id,
                taxon_group_id__in=group_ids,
                status__in=["pending"]
            )
            .order_by("id")
            .first()
        )
        try:
            with transaction.atomic():
                if proposal is None:
                    proposal = create_taxon_proposal(
                        taxon=taxon,
                        taxon_group=group,
                        data={})
                    created += 1

                proposal.approve(actor, suppress_emails=True)

                proposal.refresh_from_db()
                if proposal.status in (None, "", "pending", "created"):
                    proposal.status = "approved"
                    proposal.save(update_fields=["status"])

                if not link.is_validated:
                    link.is_validated = True
                    link.save(update_fields=["is_validated"])

                approved += 1
                approved_taxa_names.append(
                    taxon.canonical_name or taxon.scientific_name or f"Taxon {taxon.id}"
                )

        except Exception as exc:
            failed += 1
            failed_ids.append(link.id)
            logger.exception(
                "Failed on TaxonGroupTaxonomy id=%s (taxon=%s, group=%s): %s",
                link.id, taxon.id, group.id, exc
            )

        if approved and (approved + failed) % 500 == 0:
            self.update_state(
                state="PROGRESS",
                meta={"processed": approved + failed, "approved": approved, "failed": failed, "total": total},
            )

    try:
        if approved > 0:
            current_site = get_current_domain()
            from_email = settings.DEFAULT_FROM_EMAIL

            recipients = []
            for expert in root_group.get_all_experts():
                if expert.email:
                    recipients.append(expert.email)
            superusers = list(
                User.objects.filter(is_superuser=True).values_list('email', flat=True)
            )
            recipients = list({e for e in (recipients + superusers) if e})

            subject = f'[{current_site}] Bulk approval complete: {approved} taxa in "{root_group.name}"'
            preview = ", ".join(approved_taxa_names[:20])
            more = f"\n… and {max(0, approved - 20)} more." if approved > 20 else ""
            message = (
                f"Bulk approval summary for {current_site}\n\n"
                f"Taxon Group: {root_group.name}\n"
                f"Include children: {include_children}\n\n"
                f"Approved: {approved}\n"
                f"Created proposals: {created}\n"
                f"Failed: {failed}\n\n"
                f"Approved taxa (first 20): {preview}{more}\n"
            )

            if recipients:
                send_mail_notification.delay(subject, message, from_email, recipients)
    except Exception as exc:
        logger.exception("Failed to send bulk approval summary email: %s", exc)

    dt = time.perf_counter() - t0
    summary = (
        f"Unvalidated taxa processed for {root_group.name}: "
        f"approved={approved}, failed={failed}, total={total} in {dt:.2f}s."
    )
    if failed_ids:
        logger.warning(
            "Proposals failed to approve: %s", failed_ids[:50] + (["…"] if len(failed_ids) > 50 else []))

    logger.info(summary)
    return summary


@shared_task(name="bims.tasks.clear_taxa_not_associated_in_taxon_group", queue="update")
def clear_taxa_not_associated_in_taxon_group(
    dry_run: bool = True,
    keep_referenced_by_occurrences: bool = True,
) -> dict:
    """
    Remove Taxonomy rows that are NOT associated with any taxon group,
    while keeping:
      • all taxa that are linked to a group,
      • all their ancestors (parent chain),
      • (optionally) any taxa referenced by occurrences,
      • (optionally) accepted-name pointers of kept taxa.

    In dry_run, no deletion is performed; we return hypothetical counts/samples.
    """
    from bims.models.taxonomy import Taxonomy
    from bims.models.biological_collection_record import BiologicalCollectionRecord as BCR

    def _ids(qs, field="id") -> Set[int]:
        return set(qs.values_list(field, flat=True))

    def _collect_ancestors_ids(seed_ids: Iterable[int]) -> Set[int]:
        """
        Walk parent chain upward to include all ancestors of the seed set.
        """
        keep = set(seed_ids)
        frontier = set(seed_ids)
        while frontier:
            parent_ids = _ids(
                Taxonomy.objects.filter(
                    id__in=frontier).exclude(parent_id=None), "parent_id")
            parent_ids -= keep
            if not parent_ids:
                break
            keep |= parent_ids
            frontier = parent_ids
        return keep

    def _with_taxon_group_qs():
        """
        Return a QS of taxa that are associated to at least one taxon group.
        """
        qs = Taxonomy.objects.filter(taxongroup__isnull=False).distinct()
        return qs

    def _accepted_pointer_expand(base_ids: Set[int]) -> Set[int]:
        """
        If the model has an 'accepted_taxonomy' pointer FK, include those accepted taxa too.
        """
        expanded = set(base_ids)
        acc_ids = _ids(Taxonomy.objects.filter(
            id__in=base_ids
        ).exclude(accepted_taxonomy_id=None), "accepted_taxonomy_id")
        expanded |= acc_ids
        return expanded

    domain_name = get_domain_name()

    taxa_with_group_qs = _with_taxon_group_qs()
    keep_ids = _ids(taxa_with_group_qs)

    kept_ancestors = _collect_ancestors_ids(keep_ids)
    kept_ancestors_len = len(kept_ancestors) - len(keep_ids)

    keep_ids |= kept_ancestors

    keep_ids = _accepted_pointer_expand(keep_ids)

    occ_ref_ids = set()
    if keep_referenced_by_occurrences:
        occ_ref_ids = set(
            BCR.objects.exclude(
                taxonomy__isnull=True
            ).values_list("taxonomy_id", flat=True)
        )
        keep_ids |= occ_ref_ids
        keep_ids |= _collect_ancestors_ids(occ_ref_ids)

    candidates_qs = Taxonomy.objects.exclude(id__in=keep_ids)

    total_taxa = Taxonomy.objects.count()
    kept_with_group = len(_ids(taxa_with_group_qs))
    kept_ancestors = kept_ancestors_len
    referenced_by_occ = len(occ_ref_ids)
    to_delete = candidates_qs.count()

    by_rank = []
    try:
        by_rank = list(
            candidates_qs.values("rank").annotate(n=Count("id")).order_by("-n")
        )
    except FieldError:
        pass

    sample_vals = list(
        candidates_qs.values("id", "canonical_name", "scientific_name", "rank")[:25]
    )
    sample_deleted = []
    for row in sample_vals:
        nm = row.get("canonical_name") or row.get("scientific_name") or "-"
        sample_deleted.append(f"{row['id']}: {nm} ({row.get('rank') or '-'})")

    deleted = 0
    details = {}

    if not dry_run:
        total_rows, detail_map = candidates_qs.delete()
        deleted = detail_map.get(f"{Taxonomy._meta.app_label}.{Taxonomy._meta.object_name}", 0)
        details = detail_map

    result = {
        "ok": True,
        "dry_run": dry_run,
        "domain": domain_name,
        "total_taxa": total_taxa,
        "kept_with_group": kept_with_group,
        "kept_ancestors_added": max(0, kept_ancestors),
        "kept_referenced_by_occurrences": referenced_by_occ,
        "to_delete": to_delete,
        "deleted": deleted,
        "delete_breakdown_by_rank": by_rank,
        "sample_taxa_to_delete": sample_deleted,
    }

    subject = f"[{domain_name}] Taxonomy cleanup (not in any taxon group){' (DRY RUN)' if dry_run else ''}"
    message = (
        f"Taxonomy cleanup completed{' (DRY RUN – no changes made)' if dry_run else ''}.\n\n"
        f"• Total taxa : {total_taxa}\n"
        f"• Kept (linked to group) : {kept_with_group}\n"
        f"• Kept (ancestors of kept) : {max(0, kept_ancestors)}\n"
        f"• Kept (referenced by occurrences) : {referenced_by_occ}\n"
        f"• Candidates to delete : {to_delete}\n"
        f"• Deleted : {deleted}\n"
        f"• Sample (up to 25) : {', '.join(sample_deleted) or '-'}\n"
    )
    mail_superusers(subject=subject, body=message)
    logger.info(message.replace("\n", " "))

    return result
