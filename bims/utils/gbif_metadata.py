# coding=utf-8
"""Parse and validate the GBIF dataset metadata CSV.

The CSV has one row per dataset. Rows are keyed by ``id`` (the source
reference id); on a source reference's own page the id is optional because the
file is already tied to that record.
"""
import csv
import io
from datetime import datetime
from typing import List, Tuple

# Licences accepted in the metadata CSV: label -> (name, GBIF registry URL).
METADATA_LICENCES = {
    "cc0 1.0": (
        "CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
        "http://creativecommons.org/publicdomain/zero/1.0/legalcode"),
    "cc by 4.0": (
        "Creative Commons Attribution 4.0 International (CC BY 4.0)",
        "http://creativecommons.org/licenses/by/4.0/legalcode"),
    "cc by-nc 4.0": (
        "Creative Commons Attribution-NonCommercial 4.0 International (CC BY-NC 4.0)",
        "http://creativecommons.org/licenses/by-nc/4.0/legalcode"),
}
LICENCE_CHOICES = [
    ("CC0 1.0", "CC0 1.0"),
    ("CC BY 4.0", "CC BY 4.0"),
    ("CC BY-NC 4.0", "CC BY-NC 4.0"),
]

METADATA_COLUMNS = (
    "id", "project_identifier", "title", "description", "license",
    "taxonomic_coverage", "geographic_description", "temporal_start",
    "temporal_end", "sampling_description", "purpose",
)
_REQUIRED_COLUMNS = ("title", "description")


def _read_rows(f) -> list:
    """Read the CSV into a list of (line_number, raw row dict)."""
    data = f.read()
    if hasattr(f, "seek"):
        f.seek(0)
    if isinstance(data, bytes):
        try:
            text = data.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = data.decode("cp1252")
    else:
        text = data.lstrip("﻿")
    if not text.strip():
        raise ValueError("Metadata file is empty.")

    header_line = text.splitlines()[0]
    delimiter = max(",;\t", key=header_line.count)
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    header = [h.strip().lower() for h in next(reader)]
    missing = [c for c in _REQUIRED_COLUMNS if c not in header]
    if missing:
        raise ValueError(
            "Metadata file is missing required column(s): " + ", ".join(missing))

    rows = []
    for line_no, values in enumerate(reader, start=2):
        if not any((v or "").strip() for v in values):
            continue
        rows.append((line_no, {
            h: (values[i].strip() if i < len(values) else "")
            for i, h in enumerate(header)
        }))
    return rows


def _validate_row(raw: dict) -> Tuple[dict, List[str]]:
    errors = []
    row = {c: (raw.get(c) or "").strip() for c in METADATA_COLUMNS}

    if row["id"] and not row["id"].isdigit():
        errors.append("id must be an integer")
    for col in ("title", "description"):
        if not row[col]:
            errors.append(f"{col} must not be empty")

    if row["license"] and row["license"].lower() not in METADATA_LICENCES:
        errors.append(
            f"license '{row['license']}' is not one of: "
            "CC0 1.0, CC BY 4.0, CC BY-NC 4.0")

    dates = {}
    for col in ("temporal_start", "temporal_end"):
        if row[col]:
            try:
                dates[col] = datetime.strptime(row[col], "%Y-%m-%d").date()
            except ValueError:
                errors.append(f"{col} must be a valid date in YYYY-MM-DD format")
    if len(dates) == 2 and dates["temporal_end"] < dates["temporal_start"]:
        errors.append("temporal_end must not be before temporal_start")

    if row["license"]:
        # normalise to the canonical label, e.g. "cc by 4.0" -> "CC BY 4.0"
        for label, _ in LICENCE_CHOICES:
            if label.lower() == row["license"].lower():
                row["license"] = label
    return row, errors


def parse_metadata_file(f, source_reference_id=None, validate_all=False) -> dict:
    """Return the validated metadata row that belongs to a source reference.

    A row matches when its ``id`` equals *source_reference_id*, or when the
    file holds a single row with no id (a file uploaded on the source
    reference's own page). With *validate_all* every row is validated (used on
    bulk upload); otherwise only the selected row is, so a bad row for another
    dataset never blocks this one. Raises ValueError with a readable message.
    """
    rows = _read_rows(f)
    if not rows:
        raise ValueError("Metadata file has no data rows.")

    problems = []
    seen = {}
    for line_no, raw in rows:
        rid = (raw.get("id") or "").strip()
        if rid:
            if rid in seen:
                problems.append(
                    f"Row {line_no}: duplicate id {rid} (also row {seen[rid]})")
            seen.setdefault(rid, line_no)
        if validate_all:
            _, errs = _validate_row(raw)
            problems.extend(f"Row {line_no}: {e}" for e in errs)
    if problems:
        raise ValueError("; ".join(problems))

    if source_reference_id is None:
        if len(rows) != 1:
            raise ValueError(
                "Metadata file has several rows; the source reference id is "
                "needed to select the matching row.")
        match = rows[0]
    else:
        wanted = str(source_reference_id)
        matches = [r for r in rows if r[1].get("id") == wanted]
        if matches:
            match = matches[0]
        elif len(rows) == 1 and not rows[0][1].get("id"):
            match = rows[0]
        elif len(rows) == 1:
            raise ValueError(
                f"The id in the metadata file ({rows[0][1]['id']}) does not match "
                f"this source reference ({wanted}).")
        else:
            raise ValueError(
                f"Metadata file has no row with id {wanted} "
                "(the source reference id).")

    line_no, raw = match
    row, errors = _validate_row(raw)
    if errors:
        raise ValueError("; ".join(f"Row {line_no}: {e}" for e in errors))
    return row
