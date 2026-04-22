# Dashboard identity
DASHBOARD_NAME = "My Media Library"

# Radarr
RADARR_URL     = "http://your-radarr-url:7878"
RADARR_API_KEY = ""

# Sonarr
SONARR_URL     = "http://your-sonarr-url:8989"
SONARR_API_KEY = ""

# Jellyseer / Overseerr (URL only — used for search links in the dashboard)
SEERR_URL      = "http://your-seerr-url:5055"

# Database
DB_PATH     = "./data/library_history.db"
HISTORY_DIR = "./data/history/"

# Runner
RUNNER_URL      = "http://localhost:5757"
DASHBOARD_PORT  = 8686

# Optional Features
FINGERPRINT_ENABLED = False
FINGERPRINT_MEMBERS = []   # e.g. ["alice", "bob", "carol"]
HATED_ENABLED       = False

# Plex integration (optional — required for Request Audit watch history)
PLEX_DB_PATH = ""  # e.g. "/path/to/com.plexapp.plugins.library.db"

# Jellyseerr internal URL (used server-side for Request Audit API calls)
JELLYSEERR_INTERNAL_URL = "http://localhost:5055"
JELLYSEERR_API_KEY      = ""

# Radarr internal DB path (optional — only needed for Container Hitlist feature)
RADARR_DB_PATH = ""  # e.g. "/path/to/radarr/config/radarr.db"

# Watch history DB (optional — for Request Audit cross-reference)
HISTORY_DB_PATH = ""  # defaults to data/library_history_watch.db if blank

# Database backend (sqlite is default; postgres and mysql also supported)
DB_TYPE = "sqlite"
# For postgres or mysql, also set:
# DB_HOST = ""
# DB_PORT = ""   # postgres default: 5432, mysql default: 3306
# DB_NAME = ""
# DB_USER = ""
# DB_PASS = ""
