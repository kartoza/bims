# Checklist Version Workflow

**Related standard:** [Catalogue of Life Data Package (ColDP)](https://github.com/CatalogueOfLife/coldp)

---

## Overview

BIMS tracks taxonomy as a living, editable dataset. The **Checklist Version** system
layers versioned, citable releases on top of that dataset — one release per module
(TaxonGroup) — so that:

- External consumers (COL, ChecklistBank, data citations) always refer to a stable,
  immutable snapshot.
- Change tracking (additions vs updates vs unchanged) is derived automatically by
  diffing the current snapshot against the previous version — no manual tagging needed.
- Each published version carries a UUID, an optional DOI, and a link to the previous
  version, forming an auditable chain.

---

## Key Concepts

| Concept | Model | Description |
|---------|-------|-------------|
| Module | `TaxonGroup` | A biological module (e.g. Freshwater Invertebrates). Each module has its own independent version history. Enable ColDP publishing with `checklist_enabled = True`. |
| Checklist dataset | `TaxonomyChecklist` | Dataset-level metadata (title, license, contact) shared across all versions of a module's checklist. |
| Checklist version | `ChecklistVersion` | One immutable release of a module's checklist. Has a UUID, version string, status, and DOI. |
| Snapshot row | `ChecklistSnapshot` | One pre-rendered ColDP NameUsage row per taxon per published version. Written once at publish time, never modified. |

---

## Lifecycle of a Checklist Version

```
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 1 — Create a new draft version                                │
│                                                                      │
│  ChecklistVersion.objects.create(                                   │
│      taxon_group      = <module>,                                   │
│      version          = "2025.1",                                   │
│      previous_version = <last published version>,                   │
│  )                                                                  │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 2 — Assign a DOI (optional, before publishing)               │
│                                                                      │
│  version.doi = "https://doi.org/10.XXXX/YYYY"                      │
│  version.save()                                                     │
│                                                                      │
│  DOI can be reserved via DataCite or Zenodo before the release      │
│  date, then activated at publish time.                              │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 3 — Publish                                                   │
│                                                                      │
│  version.publish(published_by=request.user)                        │
│                                                                      │
│  Internally this:                                                   │
│  a) Collects all taxa from the module and its child groups          │
│  b) For each taxon, diffs against the previous version snapshot:    │
│       • Not in previous snapshot  → change_type = "added"          │
│       • Any field changed         → change_type = "updated"        │
│       • No changes                → change_type = "unchanged"      │
│  c) Bulk-creates all ChecklistSnapshot rows (one INSERT)           │
│  d) Stamps each affected Taxonomy row:                             │
│       checklist_version_uuid      ← set on first appearance        │
│       last_checklist_published_uuid ← updated on every change      │
│  e) Records taxa_count, additions_count, updates_count             │
│  f) Sets status → "published", records published_at                │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 4 — Generate ColDP package (async Celery task)               │
│                                                                      │
│  Produces a ZIP file containing:                                    │
│    metadata.yaml       ← title, version, doi, license, issued      │
│    NameUsage.tsv       ← full taxa list (from ChecklistSnapshot)    │
│    VernacularName.tsv  ← common names                              │
│    Distribution.tsv    ← biographic distributions                  │
│    Reference.tsv       ← source references                         │
│                                                                      │
│  ZIP is served at: /api/coldp/taxon/<uuid>/                        │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 5 — Share to ChecklistBank / COL (optional)                  │
│                                                                      │
│  The ColDP ZIP is submitted to ChecklistBank via their API.        │
│  The returned dataset_key is stored on the version.                │
│  Subsequent releases update the same dataset key.                  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Version Chain

Each `ChecklistVersion` points to its predecessor via `previous_version`, forming
a linked chain per module:

```
v2023.1 ──► v2024.1 ──► v2024.2 ──► v2025.1 (draft)
  │              │            │
  │              │            └── 3 additions, 1 update
  │              └── 5 additions, 2 updates
  └── initial release (no previous_version → all taxa are "added")
```

The chain can be traversed in the API using `previous_version` UUID references.

---

## Change Type Determination

Change types are determined automatically at publish time by diffing each taxon's
current data against its row in the `previous_version` snapshot:

| Condition | `change_type` |
|-----------|---------------|
| `previous_version` is null (first release) | `added` |
| Taxon `checklist_id` absent from previous snapshot | `added` |
| Any of the tracked fields differ from the previous row | `updated` |
| All tracked fields identical | `unchanged` |

**Tracked fields:** `scientific_name`, `rank`, `authorship`, `taxonomic_status`,
`parent_checklist_id`, `basionym_checklist_id`, `kingdom`, `phylum`, `class`,
`order`, `family`, `genus`, `vernacular_names`, `distributions`, `reference_id`.

---

## UUID Fields on Taxonomy

After publishing, each `Taxonomy` row carries two version UUIDs:

| Field | Set when |
|-------|---------|
| `checklist_version_uuid` | First publication in which this taxon appeared (`change_type = added`) |
| `last_checklist_published_uuid` | Every publication that included a change to this taxon |

These appear in the taxonomy REST API response and in generated checklist PDF headers.

---

## Per-Module ColDP Visibility (`checklist_enabled`)

Each `TaxonGroup` has a `checklist_enabled` boolean (default `False`).
Only modules with `checklist_enabled = True` are:
- Listed in the public `/api/checklist-version/` endpoint
- Included in ColDP exports served externally

This allows internal or draft modules to be excluded from public COL feeds while
still using the same version machinery internally.

---

## PDF Generation

When a checklist PDF is generated for a module, the header block includes:

```
Module:               Freshwater Invertebrates
Version:              2025.1
Version UUID:         a1b2c3d4-0000-0000-0000-000000000000
Previous version:     d4e5f6e7-0000-0000-0000-000000000000
DOI:                  https://doi.org/10.XXXX/YYYY
Changes this ver:     5 additions, 2 updates
Generated:            2025-04-01
```

If no version has been published yet, the UUID fields display `"Not yet published"`.

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/checklist-version/` | List published versions; filter by `taxon_group`, `status` |
| `GET` | `/api/checklist-version/<uuid>/` | Version detail |
| `GET` | `/api/coldp/metadata/` | ColDP dataset-level metadata |
| `GET` | `/api/coldp/taxon/` | Paginated ColDP NameUsage from live Taxonomy records |
| `GET` | `/api/coldp/taxon/<uuid>/` | Paginated NameUsage from a published ChecklistSnapshot |

### Snapshot endpoint filters (`/api/coldp/taxon/<uuid>/`)

| Parameter | Description |
|-----------|-------------|
| `rank` | Filter by rank (e.g. `SPECIES`, `GENUS`) |
| `change_type` | `added`, `updated`, or `unchanged` |
| `q` | Case-insensitive substring search on `scientific_name` |
| `page_size` | Results per page (default 100, max 1000) |

---

## Data Model Summary

```
TaxonGroup
  │  checklist_enabled (bool)
  │
  ├──► TaxonomyChecklist          (dataset-level metadata, optional)
  │
  └──► ChecklistVersion (1..*)    (one per release)
           │  id (UUID) ◄───────── version UUID in PDFs & API
           │  version (str)
           │  status (draft | published)
           │  doi
           │  dataset_key
           │  previous_version (FK → self)
           │  taxa_count / additions_count / updates_count
           │
           └──► ChecklistSnapshot (1 row per taxon per version)
                    │  checklist_id          ← str(Taxonomy.pk)
                    │  scientific_name
                    │  rank / authorship / taxonomic_status
                    │  kingdom / phylum / class / order / family / genus
                    │  vernacular_names (JSON)
                    │  distributions (JSON)
                    │  change_type (added | updated | unchanged)
                    │
                    └── Taxonomy (via checklist_id)
                               checklist_version_uuid       ← first appearance
                               last_checklist_published_uuid ← latest change
```
