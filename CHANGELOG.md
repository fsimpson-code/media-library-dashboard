# Changelog

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
