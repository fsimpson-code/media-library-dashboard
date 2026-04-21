# Media Library Dashboard

[![Version](https://img.shields.io/github/v/release/fsimpson-code/media-library-dashboard?style=flat-square)](https://github.com/fsimpson-code/media-library-dashboard/releases)
[![License](https://img.shields.io/github/license/fsimpson-code/media-library-dashboard?style=flat-square)](LICENSE)
[![Last Commit](https://img.shields.io/github/last-commit/fsimpson-code/media-library-dashboard?style=flat-square)](https://github.com/fsimpson-code/media-library-dashboard/commits/main)
[![Issues](https://img.shields.io/github/issues/fsimpson-code/media-library-dashboard?style=flat-square)](https://github.com/fsimpson-code/media-library-dashboard/issues)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue?style=flat-square)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED?style=flat-square&logo=docker&logoColor=white)](docker-compose.yml.example)

> A self-hosted media library dashboard for Radarr, Sonarr, and Jellyseer.

![Dashboard Screenshot](docs/screenshot.png)

## Features

- **Movies + TV stats** — KPIs, codec breakdown, resolution, HDR, size
- **Library DNA** — Multi-dimensional scoring and grading system across your entire collection
- **Bloat analysis** — Identify oversized encodes with one-click Radarr replace
- **Franchise tracker** — Collection completion with missing title drill-down
- **Talent deep dive** — Everywheremen, Unsung Heroes, BTWF, Deep Wounds analysis
- **Trend history** — Sparklines and history table across all pipeline runs
- **Historical snapshots** — Browse any past run via the dropdown
- **Optional: Family Fingerprint** — Per-member taste profiles, genre overlap, and recommendations
- **Optional: Hated titles** — Track and surface titles tagged for removal

## Requirements

- Radarr v3+
- Sonarr v3+
- Jellyseer or Overseerr
- Docker + Docker Compose
- Python 3.10+

## Database Backends

### SQLite (default)

Zero config — works out of the box. Recommended for most installs. Set `DB_PATH` in `.env` to choose the file location; if unset, a default path is used.

### MariaDB/MySQL and PostgreSQL

Supported for dashboard data only. Configure via `.env`:

```
DB_TYPE=mysql     # or: postgres
DB_HOST=your-db-host
DB_PORT=3306      # or 5432 for PostgreSQL
DB_USER=youruser
DB_PASS=yourpassword
DB_NAME=yourdbname
```

The user roster, groups, and settings layer will use your external DB. **Note:** The data pipeline (`library_runner.py`) requires SQLite and will exit with an error if `DB_TYPE` is not `sqlite`. A future release will address this.

### Third-party application databases

> **Note:** Third-party application databases (Plex, Radarr, Sonarr) are always accessed as SQLite files directly on disk regardless of `DB_TYPE`. This is a read-only side-channel and cannot be changed — these databases are owned and managed by those applications.

Features that depend on this:
- **Container Hitlist** — reads the Radarr SQLite DB directly
- **Request Audit watch history** — reads the Plex SQLite DB directly

`PLEX_DB_PATH` and `RADARR_DB_PATH` in `.env` must point to the local filesystem path of those files. These features require the dashboard container to have filesystem access to the Plex and Radarr data directories.

## Quick Start

```bash
git clone https://github.com/fsimpson-code/media-library-dashboard
cd media-library-dashboard
python3 setup.py
docker compose up -d
```

## Updating

```bash
git pull
python3 setup.py
```

`setup.py` detects your existing `config.py`, validates all API connections, prompts only for new config keys, and restarts containers if anything changed.

## Reconfigure

```bash
python3 setup.py --reconfigure
```

## Validate Connections Only

```bash
python3 setup.py --validate
```

## Optional Features

### Family Fingerprint

Enable during setup. Requires member tags in Radarr and Sonarr (e.g. `alice`, `bob`). Setup will offer to create missing tags automatically.

Provides:
- Per-member genre taste profiles
- Pairwise similarity (Common Threads)
- Personalized recommendations (from library + IMDb)

### Hated Titles

Requires Family Fingerprint enabled. Uses `[member]-hate` tags in Radarr/Sonarr. Surfaces a shared "expulsion list" of D/F-graded hated titles.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
