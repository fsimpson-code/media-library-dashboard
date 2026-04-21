# Changelog

## [1.3.3] - 2026-04-21

### Security
- Removed all hardcoded personal data from `app.py`: Jellyseerr URL and API key, Plex DB path, Radarr DB path, watch history DB path, and Plex account ID display name mappings
- Removed hardcoded family usernames from the default user seed list in `_ensure_roster_tables()`
- All values now read from environment variables (see `config.example.py` and `docker-compose.yml.example`)

### Bug Fixes
- Fixed dead SQLAlchemy layer — all user/group CRUD endpoints and roster functions now correctly route through `get_engine()` + `sql_text()` instead of bypassing it via raw `sqlite3.connect()`
- `_db()` now raises `NotImplementedError` for non-SQLite backends instead of silently failing
- `library_runner.py` now exits with a clear warning if `DB_TYPE` is not `sqlite`
- Fixed bloat table persisting on requests tab due to wrong `querySelector`

### Configuration
- Added env vars: `JELLYSEERR_INTERNAL_URL`, `JELLYSEERR_API_KEY`, `PLEX_DB_PATH`, `RADARR_DB_PATH`, `HISTORY_DB_PATH`
- Added `DB_TYPE` env var (`sqlite` default; `postgres` and `mysql` groundwork in place)
- Updated `config.example.py` and `docker-compose.yml.example` with all new vars and inline documentation

---

## [1.3.2] - 2026-04-17

### Features
- **User Roster** — DB-backed user management with display names and group assignments (Admin, Family, Extended, Friend)
- **Dynamic group filter** for Request Audit — filter unwatched requests by requester group
- Full CRUD API for users (`/api/settings/users`) and groups (`/api/settings/groups`)

### Fixes
- Run buttons moved under nav bar next to data dropdown
- Added 1400px breakpoint to compact nav on smaller screens

---

## [1.3.1] - 2026-04-16

### Features
- **Friend Request Audit** sub-tab under Bloat — shows all Jellyseerr-requested content that has been downloaded but never watched, with days-since-request and requester info
- Request Audit cross-references Plex watch history and Radarr/Sonarr for size data

---

## [1.3.0] - 2026-04-15

### Features
- **Container Hitlist** sub-tab under Bloat — lists downloaded movies not in MKV container, sourced directly from Radarr DB
- **Lisa Frank theme** — rainbow pastel palette with animated label map and scan effect
- **Version badge** in topbar upper right
- Behavior-based D2 Intentionality scoring using Radarr/Jellyseerr/Plex live data
- `watch_resolved` table and resolution pass for watch history tracking

### Fixes
- `.env` `DASHBOARD_NAME` now correctly overrides `config.py` on restart
- Removed D7 Audience/Critic Divergence from DNA scoring; rebalanced D1–D6 to 100%
- Version number corrected (was stuck at 1.2.0)

---

## [1.2.0] - 2026-04-14

### Features

- **Knight Rider theme** — red-on-black color palette with deep glow effects across all UI surfaces
- **KITT scanner animation** — animated red sweep across the topbar when Knight Rider theme is active
- `data-theme` attribute stamped on `<body>` on every theme switch, enabling theme-scoped CSS selectors

### Fixes

- Library name (DASHBOARD_NAME) now persists across page reloads without requiring a container restart

---

## [1.0.0] - 2026-04-09

### Features

- Initial release
- Movies + TV stats with charts and KPIs
- Library DNA scoring and grading system
- Encode quality / bloat analysis with one-click replace
- Franchise tracker with completion percentages
- Talent deep dive (Everywheremen, Unsung, BTWF, Deep Wounds)
- Trend history across pipeline runs
- Historical snapshot viewer via run dropdown
- Optional: Family Fingerprint (per-member taste profiles and recommendations)
- Optional: Hated titles tracking
- SQLite-only data store — no xlsx dependency
- Interactive setup script (`setup.py`) with API validation
- GitHub Actions: semantic release, lint, config validation
