<div align="center">

# Biodiversity Information Management System

**BIMS is an open-source platform for managing, analysing, visualising, and sharing biodiversity data from field collection through to decision support.**

[![Tests](https://github.com/kartoza/django-bims/actions/workflows/test.yml/badge.svg)](https://github.com/kartoza/django-bims/actions/workflows/test.yml)
[![Docker image](https://github.com/kartoza/django-bims/actions/workflows/dockerimage.yml/badge.svg)](https://github.com/kartoza/django-bims/actions/workflows/dockerimage.yml)

[Website](https://bims.kartoza.com/) |
[Documentation](https://kartoza.github.io/bims-website/) |
[Developer guide](README-dev.md) |
[Source code](https://github.com/kartoza/django-bims)

</div>

## Overview

The Biodiversity Information Management System (BIMS) helps teams turn species and biodiversity records into usable evidence for conservation, ecosystem management, research, reporting, and policy work.

BIMS is built for biodiversity planners, conservation agencies, protected-area teams, researchers, environmental consultants, species specialists, and data publishers who need a shared place to collect, manage, explore, map, and publish biodiversity data.

## What BIMS Supports

- Species occurrence and abundance records
- Taxon checklists, survey records, monitoring data, and abundance observations
- Biodiversity datasets across taxonomic groups, habitats, and ecosystems
- Habitat, site condition, ecological, and abiotic parameters
- Physico-chemical measurements
- Environmental and time-series measurements
- Photos, notes, field observations, and supporting metadata
- Map-based exploration, filtering, visualisation, and download workflows
- Open data publication and decision-support portals

## Running Portals

BIMS powers biodiversity information systems across multiple regions and use cases:

| Portal | Focus |
| --- | --- |
| [FBIS South Africa](https://freshwaterbiodiversity.org/) | Species biodiversity and biomonitoring in South Africa |
| [SANParks BIMS](https://bims.sanparks.org/) | Internal biodiversity data management for South African National Parks and marine protected areas |
| [RBIS Rwanda](https://rbis.ur.ac.rw/) | National biodiversity data and reporting for Rwanda |
| [Kafue Zambia](https://kafue.kartoza.com/) | Wetlands information for the Kafue Flats in Zambia |
| [FBIS Africa](https://fbisafrica.org/) | Species biodiversity data and decision-support tools for Africa |
| [FADA](https://fada.kartoza.com/) | Global animal diversity checklists and taxonomic backbone data |
| [FIPbio](https://fip-bio.igb-berlin.de/) | Federated biodiversity datasets for transboundary basin-scale decision-making |
| ORBIS Botswana / Okavango | Okavango biodiversity repository aligned with basin management. Currently offline |

## Mobile Data Capture

The FBIS mobile app lets field teams capture species observations, site conditions, photos, and notes in the field, then sync them when connectivity is available.

- [Get it on Google Play](https://play.google.com/store/apps/details?gl=US&hl=en&id=com.fbis)
- [Get it on the App Store](https://apps.apple.com/mu/app/fbis/id6473277389)

## Technology

This repository contains the Django implementation of BIMS. The application is Docker-first for local development and deployment, and includes support for GIS-backed biodiversity workflows, search indexing, background workers, static asset collection, and portal configuration.

Project metadata:

- Python 3.12+
- Django 6
- PostgreSQL/PostGIS
- Docker Compose
- AGPL-3.0 license

## Quick Start

Install Docker and Docker Compose, then build and start the application:

```bash
git clone https://github.com/kartoza/django-bims.git
cd django-bims

make build
make permissions
make web
```

Wait a few seconds for the database container to start, then initialise Django:

```bash
make migrate
make collectstatic
make rebuildindex
```

Create an administrator account:

```bash
make superuser
```

Useful commands:

```bash
make dev          # Run the development stack
make logs         # Follow uwsgi logs
make dblogs       # Follow database logs
make updateindex  # Update search indexes
make down         # Stop the Docker Compose stack
```

See the [developer guide](README-dev.md) for development environment notes, IDE setup, and additional Docker workflows.

## Optional Configuration

Some map layers require API keys. Add these values to `core/settings/secret.py` when needed:

| Setting | Purpose | Provider |
| --- | --- | --- |
| `MAP_TILER_KEY` | Enables MapTiler terrain and style layers | [MapTiler Cloud](https://www.maptiler.com/cloud/) |

## Documentation

- [BIMS website](https://bims.kartoza.com/)
- [Project documentation](https://kartoza.github.io/bims-website/)
- [Developer guide](README-dev.md)
- [GitHub issues](https://github.com/kartoza/django-bims/issues)

## Open Source

BIMS is developed in the open and maintained collaboratively. Contributions that improve biodiversity data management, field collection, portal deployment, documentation, testing, and data interoperability are welcome.

This project is a [Freshwater Research Centre](https://www.frcsa.org.za/) initiative, supported by [Kartoza](https://kartoza.com) as implementing partner.

## Contributors

Thank you to the people who have helped build BIMS:

- Dr. Helen Dallas, implementation lead
- Dr. Jeremy Shelton, biodiversity expert
- Tim Sutton, lead developer
- Dimas Ciputra, lead developer
- Irwan Fathurrahman
- Fanevanjanahary
