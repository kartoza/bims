# coding=utf-8
"""
Shared helpers for detecting and auto-resuming harvest sessions that died
mid-run (e.g. the worker was restarted by a deployment).

A session's log file doubles as its heartbeat: harvest tasks append a line
for every record they handle, so a live harvest never leaves its log
untouched for long. A session whose log hasn't been written for
``STALL_SECONDS`` is considered dead and is re-dispatched; the harvest
tasks persist their progress in ``additional_data`` so the new run
continues where the old one stopped.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Callable

from django.utils import timezone

logger = logging.getLogger(__name__)

STALL_SECONDS = 30 * 60
# Stop auto-resuming a session that keeps dying without making progress.
MAX_AUTO_RESUMES_WITHOUT_PROGRESS = 5


def session_seconds_since_activity(session) -> float | None:
    """Seconds since the session's log file was last written, or None."""
    try:
        if session.log_file:
            return time.time() - os.path.getmtime(session.log_file.path)
    except (OSError, ValueError):
        pass
    return None


def session_is_stalled(session, stall_seconds: int | None = None) -> bool:
    threshold = STALL_SECONDS if stall_seconds is None else stall_seconds
    idle = session_seconds_since_activity(session)
    return idle is None or idle >= threshold


def append_session_log(session, msg: str):
    ts = timezone.now().isoformat(timespec="seconds")
    try:
        with open(session.log_file.path, "a") as fh:
            fh.write(f"[{ts}] {msg}\n")
    except Exception:
        pass


def clear_auto_resume_state(additional: dict) -> dict:
    """Drop auto-resume bookkeeping, e.g. when a user resumes manually."""
    additional = dict(additional or {})
    for key in ("auto_resume_count", "auto_resume_page", "auto_resume_progress"):
        additional.pop(key, None)
    return additional


def auto_resume_stalled_sessions(
        schema_name: str,
        category: str,
        dispatch: Callable[[int, str], None],
        progress_marker: Callable[[dict], object],
        marker_key: str = "auto_resume_progress",
        label: str = "harvest",
        max_resumes: int = MAX_AUTO_RESUMES_WITHOUT_PROGRESS,
        stall_seconds: int | None = None,
        describe_position: Callable[[dict], str] | None = None,
):
    """
    Re-dispatch unfinished sessions of *category* whose log has gone quiet.

    *progress_marker* returns a value describing how far the session got
    (e.g. its current page); the resume counter resets whenever it changes,
    so a long harvest interrupted by several deployments keeps going, but a
    session that keeps dying at the same point is eventually marked failed.
    *describe_position* optionally renders that point for log and status
    messages, e.g. ``"at page 88"``.

    Must be called inside the tenant's schema context.
    """
    from bims.models import HarvestSession

    sessions = HarvestSession.objects.filter(
        category=category,
        finished=False,
        canceled=False,
    ).exclude(log_file='').exclude(log_file__isnull=True)

    for session in sessions:
        if not session_is_stalled(session, stall_seconds):
            continue

        additional = dict(session.additional_data or {})
        marker = progress_marker(additional)
        if additional.get(marker_key) != marker:
            additional[marker_key] = marker
            additional["auto_resume_count"] = 0
        count = additional.get("auto_resume_count", 0)
        position = describe_position(additional) if describe_position else ""
        position_suffix = f" {position}" if position else ""

        if count >= max_resumes:
            msg = (
                f"Auto-resume gave up: {label} stopped {count} times"
                f"{position_suffix} without making progress"
            )
            append_session_log(session, msg)
            HarvestSession.objects.filter(id=session.id).update(
                status=f"Failed{position_suffix}: {msg}",
                finished=True,
                additional_data=additional,
            )
            logger.warning("%s session=%s: %s", label, session.id, msg)
            continue

        additional["auto_resume_count"] = count + 1
        HarvestSession.objects.filter(id=session.id).update(
            additional_data=additional,
        )
        # Writing to the log also refreshes its mtime, so the next check
        # doesn't dispatch the session a second time.
        append_session_log(
            session,
            f"Harvest appears to have stopped - auto-resuming"
            f"{f' from {position}' if position else ''} "
            f"(attempt {count + 1}/{max_resumes})",
        )
        logger.info(
            "Auto-resuming %s session=%s schema=%s",
            label, session.id, schema_name,
        )
        dispatch(session.id, schema_name)


def for_each_tenant_schema(fn: Callable[[str], None], label: str):
    """Call fn(schema_name) for every non-public tenant, isolating errors."""
    from django_tenants.utils import get_public_schema_name, get_tenant_model

    public = get_public_schema_name()
    for tenant in get_tenant_model().objects.exclude(schema_name=public):
        try:
            fn(tenant.schema_name)
        except Exception as exc:
            logger.error("%s failed for %s: %s", label, tenant.schema_name, exc)
