# BIMS Taxonomy → Catalogue of Life Data Package (ColDP) Mapping Specification

**Version:** 1.0  
**Date:** 2026-04-27  
**Standard:** [Catalogue of Life Data Package (ColDP)](https://github.com/CatalogueOfLife/coldp)

---

## Overview

This document specifies how BIMS taxonomy fields map to the Catalogue of Life Data Package (ColDP) format. ColDP is the exchange format used by the Catalogue of Life (COL) and supports the following core entities:

| ColDP Entity | BIMS Source Model(s) |
|---|---|
| `NameUsage` | `Taxonomy` (flat export combining Name + Taxon) |
| `Name` | `Taxonomy` (name-related fields only) |
| `VernacularName` | `VernacularName` |
| `Reference` | `SourceReference` / `SourceReferenceBibliography` |
| `Distribution` | `Taxonomy.biographic_distributions` (tags) |
| `Media` | `TaxonImage` |
| `NameRelation` | `Taxonomy.accepted_taxonomy` (synonymy) |

---

## 1. NameUsage Entity

The flat `NameUsage` entity is the most commonly used ColDP format. It combines name, taxon, and synonym records into a single table.

### Mapping Table

Priority levels follow the ColDP Publishing Guidelines: **Minimal** (required for a valid checklist), **Highly Recommended**, **Desired**, **Optional**.

| ColDP Field | Type | Priority | BIMS Field | Model | Notes |
|---|---|---|---|---|---|
| `ID` | TEXT | Minimal | `id` | `Taxonomy` | Primary key; use as string |
| `alternativeID` | TEXT | Optional | *(not stored — skip for now)* | — | Comma-separated alternative identifiers for the name usage. Candidate sources: `gbif_key`, `iucn_redlist_id`, `fada_id`. Not implemented yet. |
| `nameAlternativeID` | TEXT | Optional | *(not stored — skip for now)* | — | Comma-separated alternative identifiers for the name itself. Not implemented yet. |
| `sourceID` | TEXT | Optional | *(not stored)* | — | Identifier of the source dataset within a management system. Not applicable for direct BIMS exports. |
| `parentID` | TEXT | Minimal | `parent_id` | `Taxonomy` | FK to parent `Taxonomy.id`. For synonyms use `accepted_taxonomy_id` instead. For accepted taxa use hierarchical `parent_id`. |
| `ordinal` | INT | Optional | *(not stored)* | — | Sort order within siblings. Not captured in BIMS. |
| `branchLength` | NUMERIC | Optional | *(not stored)* | — | Phylogenetic branch length. Not captured in BIMS. |
| `basionymID` | TEXT | Highly Recommended | *(not stored directly)* | `Taxonomy` | No dedicated field. GBIF returns `basionymKey` which may be present in `gbif_data['basionymKey']`, but BIMS does not extract or index it. To derive: read `gbif_data['basionymKey']`, look up `Taxonomy.objects.filter(gbif_key=basionymKey).first()`, use its `id`. Only possible for GBIF-sourced records where the basionym also exists in BIMS. Leave blank otherwise. |
| `status` | ENUM | Minimal | `taxonomic_status` | `Taxonomy` | See **Status Mapping** below |
| `scientificName` | TEXT | Minimal | `canonical_name` | `Taxonomy` | Name **without** authorship (e.g., `Homo sapiens`). Fall back to `scientific_name` only if `canonical_name` is blank, stripping any trailing authorship first. Never include the author in this field. |
| `authorship` | TEXT | Minimal | `author` | `Taxonomy` | Full authorship string (e.g., `Linnaeus, 1758` or `(Pocock, 1935)`). Used when parsed authorship fields below are not populated. |
| `rank` | ENUM | Minimal | `rank` | `Taxonomy` | See **Rank Mapping** below |
| `uninomial` | TEXT | Desired | `canonical_name` | `Taxonomy` | For GENUS and above only. Leave blank for SPECIES and below. |
| `genericName` | TEXT | Desired | *(derived from `canonical_name`)* | `Taxonomy` | First word of `canonical_name` for SPECIES and below only. See note below. |
| `infragenericEpithet` | TEXT | Desired | `sub_genus_name` *(property)* | `Taxonomy` | Subgenus epithet without parentheses. |
| `specificEpithet` | TEXT | Desired | `specific_epithet` *(property)* | `Taxonomy` | Second word of species name. |
| `infraspecificEpithet` | TEXT | Desired | *(derived)* | `Taxonomy` | Third word of name when `rank` = SUBSPECIES, VARIETY, or FORMA. |
| `cultivarEpithet` | TEXT | Optional | *(not stored)* | — | Not applicable; leave blank. |
| `notho` | ENUM | Optional | *(not stored)* | — | BIMS has no hybrid concept. Always blank. |
| `originalSpelling` | BOOL | Optional | *(not stored)* | — | BIMS does not track spelling emendations (`[sic]` / `corrig.`). Leave blank. |
| `combinationAuthorship` | TEXT | Optional | *(not stored)* | — | BIMS stores only the combined `author` string; combination vs. basionym authorship is not parsed out. Leave blank. |
| `combinationAuthorshipID` | TEXT | Optional | *(not stored)* | — | Requires Author entity with IPNI/ORCID IDs. Not captured in BIMS. Leave blank. |
| `combinationExAuthorship` | TEXT | Optional | *(not stored)* | — | Ex-authors of combination not parsed out in BIMS. Leave blank. |
| `combinationExAuthorshipID` | TEXT | Optional | *(not stored)* | — | Not captured in BIMS. Leave blank. |
| `combinationAuthorshipYear` | TEXT | Optional | *(not stored)* | — | Year portion of combination authorship not parsed separately. See `namePublishedInYear` for the resolved publication year. |
| `basionymAuthorship` | TEXT | Optional | *(not stored)* | — | Parenthetical authorship (original combination authors) not parsed out from `author`. Leave blank. |
| `basionymAuthorshipID` | TEXT | Optional | *(not stored)* | — | Not captured in BIMS. Leave blank. |
| `basionymExAuthorship` | TEXT | Optional | *(not stored)* | — | Not captured in BIMS. Leave blank. |
| `basionymExAuthorshipID` | TEXT | Optional | *(not stored)* | — | Not captured in BIMS. Leave blank. |
| `basionymAuthorshipYear` | TEXT | Optional | *(not stored)* | — | Year of original combination not parsed separately from `author`. Leave blank. |
| `namePhrase` | TEXT | Optional | *(not stored)* | — | Not captured; leave blank. |
| `nameReferenceID` | TEXT | Desired | `source_reference_id` | `Taxonomy` | FK to `SourceReference.id` |
| `namePublishedInYear` | INTEGER | Desired | `source_reference.source_date.year` with fallback | `Taxonomy` → `SourceReference` | 4-digit integer. Resolution: (1) `source_reference.source_date.year`; (2) first 4-digit number from `author` (e.g., `"(Linnaeus, 1758)"` → `1758`); (3) blank. |
| `namePublishedInPage` | TEXT | Optional | *(not stored)* | — | Not captured. |
| `namePublishedInPageLink` | TEXT | Optional | *(not stored)* | — | Not captured. |
| `gender` | ENUM | Optional | *(not stored)* | — | Grammatical gender of the genus name. Not captured in BIMS. |
| `genderAgreement` | BOOL | Optional | *(not stored)* | — | Not captured in BIMS. |
| `code` | ENUM | Desired | `tags` | `Taxonomy` | Look for a tag matching: `botanical`, `zoological`, `bacterial`, `virus`, `cultivar`, `phytosociological`, or `bio`. First match wins; case-insensitive. |
| `etymology` | TEXT | Optional | *(not stored)* | — | Not captured in BIMS. |
| `nameStatus` | ENUM | Desired | *(derived)* | — | `established` for accepted names; `not established` for doubtful. |
| `accordingToID` | TEXT | Optional | `source_reference_id` | `Taxonomy` | Reference asserting this name treatment. |
| `accordingToPage` | INTEGER | Optional | *(not stored)* | — | Not captured. |
| `accordingToPageLink` | TEXT | Optional | *(not stored)* | — | Not captured. |
| `scrutinizer` | TEXT | Optional | `last_modified_by` | `Taxonomy` | Username of last modifier. |
| `scrutinizerID` | TEXT | Optional | *(not stored)* | — | Not captured. |
| `scrutinizerDate` | DATE | Optional | `last_modified` *(property)* | `Taxonomy` | ISO 8601 date of last audit event. |
| `referenceID` | TEXT | Optional | `source_reference_id` | `Taxonomy` | Same as `nameReferenceID`. |
| `extinct` | BOOLEAN | Highly Recommended | *(derived from IUCN status)* | `Taxonomy` | `true` if `iucn_status.category` = EX or EW. |
| `temporalRangeStart` | ENUM | Optional | *(not stored)* | — | Not captured. |
| `temporalRangeEnd` | ENUM | Optional | *(not stored)* | — | Not captured. |
| `environment` | ENUM | Highly Recommended | `tags` | `Taxonomy` | Single value from: `brackish`, `freshwater`, `marine`, `terrestrial`. Use the first tag that matches one of the four allowed enum values. Leave blank if no matching tag exists. |
| `species` | TEXT | Desired | `species_name` *(property)* | `Taxonomy` | Full species binomial from hierarchy. |
| `section` | TEXT | Optional | *(not stored)* | — | Not captured. |
| `subgenus` | TEXT | Desired | `sub_genus_name` *(property)* | `Taxonomy` | Subgenus name. |
| `genus` | TEXT | Desired | `genus_name` *(property)* | `Taxonomy` | Current accepted genus from hierarchy traversal. |
| `subtribe` | TEXT | Desired | `subtribe_name` *(property, alias of `sub_tribe_name`)* | `Taxonomy` | Subtribe name from hierarchy. |
| `tribe` | TEXT | Desired | `tribe_name` *(property)* | `Taxonomy` | Tribe name from hierarchy. |
| `subfamily` | TEXT | Desired | `subfamily_name` *(property, alias of `sub_family_name`)* | `Taxonomy` | Subfamily name from hierarchy. |
| `family` | TEXT | Minimal *(denormalised alt.)* | `family_name` *(property)* | `Taxonomy` | Alternative to `parentID` for denormalised flat export. |
| `superfamily` | TEXT | Optional | `superfamily_name` *(property)* | `Taxonomy` | Traverse hierarchy for SUPERFAMILY rank. |
| `suborder` | TEXT | Optional | `suborder_name` *(property)* | `Taxonomy` | Traverse hierarchy for SUBORDER rank. |
| `order` | TEXT | Minimal *(denormalised alt.)* | `order_name` *(property)* | `Taxonomy` | Alternative to `parentID` for denormalised flat export. |
| `subclass` | TEXT | Optional | `subclass_name` *(property)* | `Taxonomy` | Traverse hierarchy for SUBCLASS rank. |
| `class` | TEXT | Minimal *(denormalised alt.)* | `class_name` *(property)* | `Taxonomy` | Alternative to `parentID` for denormalised flat export. |
| `subphylum` | TEXT | Optional | `subphylum_name` *(property)* | `Taxonomy` | Traverse hierarchy for SUBPHYLUM rank. |
| `phylum` | TEXT | Minimal *(denormalised alt.)* | `phylum_name` *(property)* | `Taxonomy` | Alternative to `parentID` for denormalised flat export. |
| `kingdom` | TEXT | Minimal *(denormalised alt.)* | `kingdom_name` *(property)* | `Taxonomy` | Alternative to `parentID` for denormalised flat export. |
| `link` | URI | Highly Recommended | *(derived)* | `Taxonomy` | Construct from `gbif_key`: `https://www.gbif.org/species/{gbif_key}`. Blank if no `gbif_key`. |
| `nameRemarks` | TEXT | Optional | *(not stored)* | — | Not captured. |
| `remarks` | TEXT | Optional | `additional_data` | `Taxonomy` | Serialise relevant JSON fields as a note string. |
| `modified` | TEXT | Optional | `created_at` / `last_modified` *(property)* | `Taxonomy` | ISO 8601 datetime of last record modification. Use audit event datetime if available, fall back to `created_at`. |
| `modifiedBy` | TEXT | Optional | `last_modified_by` | `Taxonomy` | Username of the user who last modified the record. |

---

## 2. Status Mapping

| BIMS `taxonomic_status` | ColDP `status` |
|---|---|
| `ACCEPTED` | `accepted` |
| `SYNONYM` | `synonym` |
| `HETEROTYPIC_SYNONYM` | `heterotypic synonym` |
| `HOMOTYPIC_SYNONYM` | `homotypic synonym` |
| `PROPARTE_SYNONYM` | `pro parte synonym` |
| `MISAPPLIED` | `misapplied` |
| `DOUBTFUL` | `ambiguous synonym` |

> **Note:** When `status` is any synonym type, the `parentID` field should be populated with the `Taxonomy.accepted_taxonomy_id` (the accepted name's ID), not the taxonomic parent.

---

## 3. Rank Mapping

| BIMS `TaxonomicRank` | ColDP `rank` |
|---|---|
| `DOMAIN` | `domain` |
| `KINGDOM` | `kingdom` |
| `PHYLUM` | `phylum` |
| `SUBPHYLUM` | `subphylum` |
| `SUPERCLASS` | `superclass` |
| `CLASS` | `class` |
| `SUBCLASS` | `subclass` |
| `SUPERORDER` | `superorder` |
| `ORDER` | `order` |
| `SUBORDER` | `suborder` |
| `INFRAORDER` | `infraorder` |
| `SUPERFAMILY` | `superfamily` |
| `FAMILY` | `family` |
| `SUBFAMILY` | `subfamily` |
| `TRIBE` | `tribe` |
| `SUBTRIBE` | `subtribe` |
| `GENUS` | `genus` |
| `SUBGENUS` | `subgenus` |
| `SPECIES` | `species` |
| `SUBSPECIES` | `subspecies` |
| `VARIETY` | `variety` |
| `FORMA` / `FORM` | `form` |

---

## 4. VernacularName Entity

Maps from `VernacularName` model.

| ColDP Field | BIMS Field | Model | Notes |
|---|---|---|---|
| `taxonID` | `taxonomy` (reverse M2M) | `VernacularName` | ID of the linked `Taxonomy` record |
| `name` | `name` | `VernacularName` | The common name string |
| `transliteration` | *(not stored)* | — | Not captured |
| `language` | `language` | `VernacularName` | ISO 639-1/2 language code (e.g., `eng`, `afr`) |
| `country` | *(not stored)* | — | Not captured |
| `area` | *(not stored)* | — | Not captured |
| `sex` | *(not stored)* | — | Not captured |
| `referenceID` | `source` | `VernacularName` | Use as reference note; no FK currently |
| `remarks` | *(not stored)* | — | Not captured |

---

## 5. Reference Entity

Maps from `SourceReference` / `SourceReferenceBibliography`.

| ColDP Field | BIMS Field | Model | Notes |
|---|---|---|---|
| `ID` | `id` | `SourceReference` | Primary key |
| `citation` | `note` | `SourceReference` | Full citation string if no structured data |
| `type` | *(derived)* | — | `article` for journal refs, `book` for documents |
| `title` | `note` | `SourceReference` | Title of reference |
| `author` | `source_authors` (M2M) | `SourceReferenceAuthor` | Concatenate author names |
| `issued` | `source_date` | `SourceReference` | Publication date (year or full date) |
| `containerTitle` | *(from bibtex/document)* | `SourceReferenceBibliography` | Journal/book title |
| `volume` | *(from bibtex)* | — | Volume number |
| `issue` | *(from bibtex)* | — | Issue number |
| `page` | *(from bibtex)* | — | Page range |
| `publisher` | *(from bibtex)* | — | Publisher name |
| `publisherPlace` | *(not stored)* | — | Not captured |
| `link` | `source_name` | `SourceReference` | URL if source_name is a URL |
| `remarks` | *(not stored)* | — | Not captured |

---

## 6. Distribution Entity

Maps from `Taxonomy.biographic_distributions` (taggit tags via `CustomTaggedTaxonomy`).

| ColDP Field | BIMS Field | Model | Notes |
|---|---|---|---|
| `taxonID` | `taxonomy.id` | `Taxonomy` | ID of the linked `Taxonomy` record |
| `area` | `biographic_distributions` tag name | `CustomTaggedTaxonomy` / `Tag` | Each tag becomes a distribution area string |
| `gazetteer` | *(not stored)* | — | Not captured; default to `text` |
| `status` | *(not stored)* | — | Not captured |
| `referenceID` | *(not stored)* | — | Not captured |
| `remarks` | *(not stored)* | — | Not captured |

---

## 7. Media Entity

Maps from `TaxonImage` model.

| ColDP Field | BIMS Field | Model | Notes |
|---|---|---|---|
| `taxonID` | `taxonomy_id` | `TaxonImage` | ID of the linked `Taxonomy` record |
| `url` | `taxon_image.url` | `TaxonImage` | Absolute URL to image file |
| `type` | *(fixed)* | — | Always `image` |
| `format` | *(derived)* | — | Derive MIME type from file extension |
| `title` | *(not stored)* | — | Not captured |
| `created` | `date` | `TaxonImage` | Image date |
| `creator` | `owner` / `uploader` | `TaxonImage` | Owner or uploader user name |
| `license` | *(not stored)* | — | Not captured; set to dataset default license |
| `link` | `source` | `TaxonImage` | Original source URL if available |
| `remarks` | *(not stored)* | — | Not captured |

---

## 8. NameRelation Entity

Maps synonym relationships explicitly (complement to `parentID` approach in NameUsage).

| ColDP Field | BIMS Field | Model | Notes |
|---|---|---|---|
| `nameID` | `id` | `Taxonomy` | Synonym taxon ID |
| `relatedNameID` | `accepted_taxonomy_id` | `Taxonomy` | Accepted taxon ID |
| `sourceID` | `source_reference_id` | `Taxonomy` | Relation source reference |
| `type` | *(derived from `taxonomic_status`)* | — | See NameRelation Type Mapping below |
| `referenceID` | `source_reference_id` | `Taxonomy` | Same as sourceID |
| `remarks` | *(not stored)* | — | Not captured |

### NameRelation Type Mapping

| BIMS `taxonomic_status` | ColDP `type` |
|---|---|
| `SYNONYM` | `synonym` |
| `HETEROTYPIC_SYNONYM` | `heterotypic synonym` |
| `HOMOTYPIC_SYNONYM` | `homotypic synonym` |
| `PROPARTE_SYNONYM` | `pro parte synonym` |
| `MISAPPLIED` | `misapplied` |

---

## 9. Fields Without ColDP Equivalents

The following BIMS-specific fields have no direct ColDP equivalent and should be handled as noted:

| BIMS Field | Model | Recommended Handling |
|---|---|---|
| `iucn_status` | `Taxonomy` | Export as separate metadata file or include in `remarks` |
| `national_conservation_status` | `Taxonomy` | Export as separate metadata file |
| `endemism` | `Taxonomy` | Include in `remarks` or custom extension |
| `origin` (native/alien/invasive) | `Taxonomy` | Include in `remarks` or `Distribution.status` |
| `gbif_key` | `Taxonomy` | Store as `link` referencing GBIF species page |
| `iucn_redlist_id` | `Taxonomy` | Append to `link` field or include in `remarks` |
| `fada_id` | `Taxonomy` | Include in `remarks` |
| `species_group` | `Taxonomy` | Include in `remarks` or map to informal group |
| `invasion` | `Taxonomy` | Include in `remarks` or `Distribution.status` |
| `tags` | `Taxonomy` | Include in `remarks` (comma-separated) |
| `additional_data` (Cites, Variety) | `Taxonomy` | Selectively include relevant keys in `remarks` |
| `cites_listing` | `Taxonomy.additional_data` | Include in `remarks` |
| `verified` | `Taxonomy` | Omit; internal workflow flag |
| `import_date` | `Taxonomy` | Omit; internal audit field |

---

## 10. Export Notes and Implementation Guidance

### File Format
ColDP packages are ZIP archives containing TSV files with a `metadata.yaml` descriptor. Required files:

```
coldp.zip
├── metadata.yaml        # Dataset-level metadata
├── NameUsage.tsv        # Core taxon/name records
├── VernacularName.tsv   # Common names
├── Reference.tsv        # Bibliography
├── Distribution.tsv     # Geographic distributions
├── Media.tsv            # Images
└── NameRelation.tsv     # Explicit synonym relations (optional)
```

### Hierarchy Export Strategy
- Export all `Taxonomy` records where `taxonomic_status = ACCEPTED` as NameUsage rows.
- For synonyms (`taxonomic_status` != ACCEPTED), set `parentID` = `accepted_taxonomy_id`.
- For accepted taxa, set `parentID` = `parent_id`.
- Only include `TaxonGroupTaxonomy.is_validated = True` records unless exporting for internal review.

### Scientific Name Construction

ColDP `scientificName` must **not** contain authorship — it is a pure name string only.

| ColDP field | BIMS field | Example value |
|---|---|---|
| `scientificName` | `canonical_name` | `Homo sapiens` |
| `authorship` | `author` | `Linnaeus, 1758` |

Use `canonical_name` directly. If it is blank, fall back to `scientific_name` but strip any trailing authorship (everything after the last lowercase epithet word). The `author` field maps 1-to-1 to `authorship` with no modification needed.

### Ranks Above Species
For ranks above GENUS, set `uninomial` = `canonical_name` and leave `genus`, `specificEpithet`, and `infraspecificEpithet` empty.

### Null Handling
ColDP TSV files use empty strings (not `\N` or `NULL`) for missing values.

### genericName — Genus Part of the Name String

`genericName` in ColDP is the genus epithet as it appears in the scientific name itself. BIMS has no dedicated field for this; derive it by splitting `canonical_name`:

```python
# Example derivation in exporter
SPECIES_AND_BELOW = {"SPECIES", "SUBSPECIES", "VARIETY", "FORMA", "FORM"}

generic_name = ""
if taxonomy.rank in SPECIES_AND_BELOW and taxonomy.canonical_name:
    generic_name = taxonomy.canonical_name.split()[0]
```

Rules:
- Only emit for ranks at SPECIES and below (binomials and trinomials).
- Leave blank for GENUS and above — `genericName` is redundant with the name itself at those ranks.
- Do **not** read from `gbif_data['genericName']`; GBIF does not reliably expose this field in the stored JSON, and it is not explicitly extracted by BIMS on import.

### GBIF Key as External Identifier
ColDP does not have a dedicated GBIF key field. Represent it as:
- `link` = `https://www.gbif.org/species/{gbif_key}` (when `gbif_key` is present)

### IUCN Status
ColDP has no built-in field for conservation status. Options:
1. Append to `remarks` field: `IUCN: {category}`
2. Include in a custom ColDP extension file `SpeciesEstimate.tsv`

---

## 11. metadata.yaml Template

```yaml
title: <Dataset title>
description: <Dataset description>
issued: <YYYY-MM-DD>
version: <version string>
license: <license URL>
creator:
  - name: <Author or Organisation>
    email: <email>
    url: <url>
contact:
  name: <Contact person>
  email: <email>
source:
  - id: <source ID>
    title: <Source title>
    url: <source URL>
```

---

## 12. Validation Checklist

Before submitting a ColDP package, verify:

- [ ] All `ID` values are unique within their entity file
- [ ] All `parentID` values reference existing `ID` values in `NameUsage.tsv`
- [ ] `status` values use only ColDP-accepted vocabulary
- [ ] `rank` values use only ColDP-accepted vocabulary
- [ ] `scientificName` does not contain authorship
- [ ] `authorship` is populated for species-rank and below
- [ ] `VernacularName.taxonID` values all exist in `NameUsage.tsv`
- [ ] `Distribution.taxonID` values all exist in `NameUsage.tsv`
- [ ] `Media.taxonID` values all exist in `NameUsage.tsv`
- [ ] `NameRelation.nameID` and `relatedNameID` both exist in `NameUsage.tsv`
- [ ] `language` codes in `VernacularName` are valid ISO 639-2/3 codes
- [ ] `metadata.yaml` is present and valid YAML
