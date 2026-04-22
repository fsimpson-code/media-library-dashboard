#!/usr/bin/env python3
"""
library_runner.py
Fetches data from Radarr and Sonarr APIs + IMDb datasets and writes to the
configured database backend (SQLite by default; Postgres or MySQL/MariaDB via
DB_TYPE env var).

Run: python3 /scripts/library_runner.py
"""

import os, sys, gzip, csv, json, requests, shutil, sqlite3, math
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from pathlib import Path
from sqlalchemy import create_engine, text as sql_text, MetaData, Table, Column, Integer, String, Float, Text, inspect as sa_inspect

# ── Configuration ─────────────────────────────────────────────────────────────
try:
    from config import (
        RADARR_URL, RADARR_API_KEY,
        SONARR_URL, SONARR_API_KEY,
        DB_PATH as _CFG_DB,
        DASHBOARD_NAME,
        FINGERPRINT_MEMBERS,
    )
    RADARR_KEY = RADARR_API_KEY
    SONARR_KEY = SONARR_API_KEY
    DB_PATH    = Path(_CFG_DB)
except ImportError:
    RADARR_URL  = os.environ.get("RADARR_URL",  "http://localhost:7878")
    RADARR_KEY  = os.environ.get("RADARR_API_KEY", "")
    SONARR_URL  = os.environ.get("SONARR_URL",  "http://localhost:8989")
    SONARR_KEY  = os.environ.get("SONARR_API_KEY", "")
    DB_PATH     = Path(os.environ.get("DB_PATH", "/data/library_history.db"))
    DASHBOARD_NAME     = os.environ.get("DASHBOARD_NAME", "Media Library")
    FINGERPRINT_MEMBERS = [m.strip() for m in os.environ.get("FINGERPRINT_MEMBERS", "").split(",") if m.strip()]

IMDB_DIR          = Path(os.environ.get("IMDB_DIR", str(DB_PATH.parent / "imdb_data")))
IMDB_MAX_AGE_DAYS = 30

IMDB_URLS = {
    "title_basics":     "https://datasets.imdbws.com/title.basics.tsv.gz",
    "title_principals": "https://datasets.imdbws.com/title.principals.tsv.gz",
    "title_ratings":    "https://datasets.imdbws.com/title.ratings.tsv.gz",
    "name_basics":      "https://datasets.imdbws.com/name.basics.tsv.gz",
}

# ── Multi-backend DB engine ────────────────────────────────────────────────────
_engine = None
_engine_lock = __import__("threading").Lock()

def get_engine():
    global _engine
    with _engine_lock:
        if _engine is None:
            db_type = os.getenv("DB_TYPE", "sqlite")
            if db_type == "postgres":
                _engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(
                    os.getenv("DB_USER", ""), os.getenv("DB_PASS", ""),
                    os.getenv("DB_HOST", ""), os.getenv("DB_PORT", "5432"),
                    os.getenv("DB_NAME", "")))
            elif db_type == "mysql":
                _engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(
                    os.getenv("DB_USER", ""), os.getenv("DB_PASS", ""),
                    os.getenv("DB_HOST", ""), os.getenv("DB_PORT", "3306"),
                    os.getenv("DB_NAME", "")))
            else:
                _engine = create_engine("sqlite:///{}".format(
                    os.getenv("DB_PATH", str(DB_PATH))))
    return _engine

def _insert_returning_id(conn, stmt, params):
    """Execute an INSERT and return the new row's primary key, cross-backend."""
    db_type = os.getenv("DB_TYPE", "sqlite")
    if db_type == "postgres":
        row = conn.execute(sql_text(stmt + " RETURNING id"), params).fetchone()
        return row[0]
    result = conn.execute(sql_text(stmt), params)
    return result.lastrowid

# ── API helpers ────────────────────────────────────────────────────────────────
def radarr(endpoint):
    r = requests.get(f"{RADARR_URL}/api/v3/{endpoint}",
                     headers={"X-Api-Key": RADARR_KEY}, timeout=60)
    r.raise_for_status()
    return r.json()

def sonarr(endpoint):
    r = requests.get(f"{SONARR_URL}/api/v3/{endpoint}",
                     headers={"X-Api-Key": SONARR_KEY}, timeout=60)
    r.raise_for_status()
    return r.json()

# ── IMDb dataset helpers ───────────────────────────────────────────────────────
def imdb_needs_refresh():
    marker = IMDB_DIR / ".last_update"
    if not marker.exists():
        return True
    age = datetime.now() - datetime.fromtimestamp(marker.stat().st_mtime)
    return age.days >= IMDB_MAX_AGE_DAYS

def download_imdb():
    IMDB_DIR.mkdir(parents=True, exist_ok=True)
    for name, url in IMDB_URLS.items():
        gz_path  = IMDB_DIR / f"{name}.tsv.gz"
        tsv_path = IMDB_DIR / f"{name}.tsv"
        print(f"  Downloading {name}...")
        with requests.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(gz_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)
        with gzip.open(gz_path, "rb") as gz, open(tsv_path, "wb") as out:
            shutil.copyfileobj(gz, out)
        gz_path.unlink()
    (IMDB_DIR / ".last_update").touch()

def load_imdb_talent(imdb_ids):
    """Return {imdb_id: [{'name': str, 'role': str, 'order': int}]} for cast/crew."""
    principals_path = IMDB_DIR / "title_principals.tsv"
    names_path      = IMDB_DIR / "name_basics.tsv"
    if not principals_path.exists() or not names_path.exists():
        return {}, {}

    print("  Loading IMDb name data...")
    name_lookup = {}
    with open(names_path, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            name_lookup[row["nconst"]] = row["primaryName"]

    print("  Loading IMDb principals data...")
    id_set = set(imdb_ids)
    talent = defaultdict(list)
    with open(principals_path, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            tconst = row["tconst"]
            if tconst not in id_set:
                continue
            category = row["category"]
            if category not in ("actor", "actress", "director"):
                continue
            nconst = row["nconst"]
            name   = name_lookup.get(nconst, "Unknown")
            talent[tconst].append({
                "name":     name,
                "role":     category,
                "order":    int(row["ordering"]) if row["ordering"].isdigit() else 99,
                "nconst":   nconst,
            })
    return talent, name_lookup

# ── Data fetching ──────────────────────────────────────────────────────────────
def fetch_movies():
    print("Fetching Radarr movies...")
    raw = radarr("movie")
    radarr_tags = {t["id"]: t["label"] for t in radarr("tag")}
    movies = []
    for m in raw:
        mf  = m.get("movieFile", {}) or {}
        mi  = mf.get("mediaInfo", {}) or {}
        qq  = mf.get("quality", {}).get("quality", {}) or {}
        rat = m.get("ratings", {}) or {}
        col = m.get("collection") or {}
        movies.append({
            "radarr_id":       m.get("id"),
            "title_slug":      m.get("titleSlug", ""),
            "title":           m.get("title", ""),
            "original_title":  m.get("originalTitle", ""),
            "year":            m.get("year"),
            "status":          m.get("status", ""),
            "studio":          m.get("studio", ""),
            "certification":   m.get("certification", ""),
            "runtime":         m.get("runtime"),
            "genres":          ", ".join(m.get("genres", [])),
            "keywords":        ", ".join(m.get("keywords", [])[:10]),
            "imdb_id":         m.get("imdbId", ""),
            "tmdb_id":         m.get("tmdbId"),
            "has_file":        m.get("hasFile", False),
            "monitored":       m.get("monitored", False),
            "added":           m.get("added", "")[:10] if m.get("added") else "",
            "in_cinemas":      m.get("inCinemas", "")[:10] if m.get("inCinemas") else "",
            "popularity":      round(m.get("popularity", 0), 2),
            "collection":      col.get("title", ""),
            "collection_id":   col.get("tmdbId", ""),
            "quality_name":    qq.get("name", ""),
            "source":          qq.get("source", ""),
            "resolution":      qq.get("resolution", ""),
            "is_repack":       mf.get("quality", {}).get("revision", {}).get("isRepack", False),
            "release_group":   mf.get("releaseGroup", "") or "",
            "edition":         mf.get("edition", "") or "",
            "file_size_gb":    round(mf.get("size", 0) / 1_073_741_824, 2) if mf.get("size") else 0,
            "cutoff_not_met":  mf.get("qualityCutoffNotMet", False),
            "video_codec":     mi.get("videoCodec", ""),
            "video_bitrate":   mi.get("videoBitrate", 0),
            "bit_depth":       mi.get("videoBitDepth", ""),
            "hdr_type":        mi.get("videoDynamicRangeType", "") or "SDR",
            "audio_codec":     mi.get("audioCodec", ""),
            "audio_channels":  mi.get("audioChannels", ""),
            "imdb_rating":     rat.get("imdb", {}).get("value", ""),
            "imdb_votes":      rat.get("imdb", {}).get("votes", ""),
            "tmdb_rating":     rat.get("tmdb", {}).get("value", ""),
            "metacritic":      rat.get("metacritic", {}).get("value", ""),
            "rotten_tomatoes": rat.get("rottenTomatoes", {}).get("value", ""),
            "trakt_rating":    rat.get("trakt", {}).get("value", ""),
            "trakt_votes":     rat.get("trakt", {}).get("votes", ""),
            "tags":            ", ".join(radarr_tags.get(tid, str(tid)) for tid in m.get("tags", [])),
        })
    return movies

def fetch_series():
    print("Fetching Sonarr series...")
    raw = sonarr("series")
    sonarr_tags = {t["id"]: t["label"] for t in sonarr("tag")}
    series = []
    for s in raw:
        stats   = s.get("statistics", {}) or {}
        rat     = s.get("ratings", {}) or {}
        all_seasons = s.get("seasons", [])

        main_seasons     = [sn for sn in all_seasons if sn["seasonNumber"] > 0]
        specials_seasons = [sn for sn in all_seasons if sn["seasonNumber"] == 0]

        def ep_counts(season_list):
            have  = sum(sn.get("statistics", {}).get("episodeFileCount", 0) for sn in season_list)
            total = sum(sn.get("statistics", {}).get("totalEpisodeCount", 0) for sn in season_list)
            return have, total

        main_have,     main_total     = ep_counts(main_seasons)
        specials_have, specials_total = ep_counts(specials_seasons)

        main_completion = round((main_have / main_total) * 100, 1) if main_total > 0 else 0.0
        overall_completion = round(stats.get("percentOfEpisodes", 0), 1)

        series.append({
            "sonarr_id":            s.get("id"),
            "title":                s.get("title", ""),
            "year":                 s.get("year"),
            "status":               s.get("status", ""),
            "ended":                s.get("ended", False),
            "network":              s.get("network", ""),
            "certification":        s.get("certification", ""),
            "runtime":              s.get("runtime"),
            "genres":               ", ".join(s.get("genres", [])),
            "imdb_id":              s.get("imdbId", ""),
            "tvdb_id":              s.get("tvdbId"),
            "monitored":            s.get("monitored", False),
            "first_aired":          s.get("firstAired", "")[:10] if s.get("firstAired") else "",
            "last_aired":           s.get("lastAired", "")[:10] if s.get("lastAired") else "",
            "added":                s.get("added", "")[:10] if s.get("added") else "",
            "season_count":         len(main_seasons),
            "episodes_have":        main_have,
            "episodes_total":       main_total,
            "completion_pct":       main_completion,
            "specials_have":        specials_have,
            "specials_total":       specials_total,
            "has_specials":         specials_total > 0,
            "episodes_monitored":   stats.get("episodeCount", 0),
            "completion_overall":   overall_completion,
            "tags":                 ", ".join(sonarr_tags.get(tid, str(tid)) for tid in s.get("tags", [])),
            "size_gb":              round(stats.get("sizeOnDisk", 0) / 1_073_741_824, 2),
            "rating":               rat.get("value", ""),
            "rating_votes":         rat.get("votes", ""),
        })
    return series

# ── Library DNA Score ──────────────────────────────────────────────────────────
def compute_dna_scores(movies, talent_data, run_id, conn):
    """Compute Library DNA Score for each film and write to dna_scores table."""
    print("\nComputing Library DNA Scores...")

    has_file = [m for m in movies if m.get("has_file")]
    if not has_file:
        print("  No films with files — skipping DNA scoring.")
        return

    # ── Pre-compute library-wide signals ──────────────────────────────────────

    # Genre distribution for D6 (Genre Coherence)
    genre_counter = Counter()
    for m in has_file:
        for g in (m.get("genres") or "").split(", "):
            g = g.strip()
            if g:
                genre_counter[g] += 1
    top_genres = set(g for g, _ in genre_counter.most_common(10))

    # Collection film counts for D4 (Franchise Context)
    collection_counts = Counter()
    for m in has_file:
        col = (m.get("collection") or "").strip()
        if col:
            collection_counts[col] += 1

    # nconst → number of library films for D3 (Talent Crossover)
    nconst_film_count = Counter()
    for imdb_id, people in talent_data.items():
        for p in people:
            nconst_film_count[p["nconst"]] += 1

    # Era-expected vote baselines for D5 (Vote Density by Era)
    era_expected = {
        1920: 5_000,  1930: 10_000, 1940: 15_000, 1950: 20_000,
        1960: 30_000, 1970: 50_000, 1980: 80_000, 1990: 150_000,
        2000: 250_000, 2010: 400_000, 2020: 300_000,
    }
    def get_era_expected(year):
        if not year:
            return 100_000
        decade = (int(year) // 10) * 10
        return era_expected.get(decade, 200_000)

    # ── Grading helper ────────────────────────────────────────────────────────
    def grade(score):
        if score >= 90: return "A"
        if score >= 80: return "B"
        if score >= 70: return "C"
        if score >= 60: return "D"
        return "F"

    # ── Teacher note builder ──────────────────────────────────────────────────
    def build_teacher_note(scores, final):
        dim_labels = {
            "d1": "score authenticity",
            "d2": "intentionality",
            "d3": "talent crossover",
            "d4": "franchise context",
            "d5": "vote density",
            "d6": "genre fit",
            "d7": "audience vs critics",
        }
        ranked = sorted(scores.items(), key=lambda x: x[1])
        worst_key, worst_val = ranked[0]
        best_key,  best_val  = ranked[-1]

        g = grade(final)
        openers = {
            "A": "A model library citizen.",
            "B": "A solid, deliberate addition.",
            "C": "Passes, but without distinction.",
            "D": "Struggles to justify its place.",
            "F": "Failed to earn its seat.",
        }
        note = [openers[g]]

        # Best signal — d7 handled separately to distinguish consensus vs divergence
        best_notes = {
            "d2": "Deliberately chosen by the family.",
            "d1": "Strong score backed by real vote weight.",
            "d4": "Earns its franchise seat.",
            "d3": "Woven into the library's talent web.",
            "d6": "Fits the fabric of the collection.",
            "d5": "Remarkably well-known for its era.",
        }
        # Only fire a best-signal note if the film is actually passing (>=60)
        # and the best dimension is genuinely strong — avoids "Earns its franchise
        # seat" on a film that's failing everything else.
        if final >= 60 and best_val >= 70 and best_key in best_notes:
            note.append(best_notes[best_key])
        elif final >= 60 and best_key == "d7" and best_val >= 70:
            if scores["d7"] >= 95:
                note.append("A consensus hit — critics and audiences agree.")
            elif scores["d7"] >= 80:
                note.append("Audiences connected with it even if critics didn't.")

        # Worst signal
        worst_notes = {
            "d2": "Nobody tagged it — may be an accidental grab.",
            "d1": "Score lacks the vote weight to be credible.",
            "d7": "Both critics and audiences passed on this one.",
            "d4": "A franchise orphan — the original isn't even here.",
            "d3": "No talent crossover with the rest of the collection.",
            "d6": "A genre orphan — nothing else here like it.",
            "d5": "Barely registered for its era.",
        }
        if worst_val <= 40 and worst_key in worst_notes:
            note.append(worst_notes[worst_key])

        return " ".join(note)

    # ── Score each film ───────────────────────────────────────────────────────
    rows = []
    for m in has_file:
        imdb_id = m.get("imdb_id") or ""

        # ── D1: Score Authenticity (20%) ──────────────────────────────────────
        try:
            rating = float(m.get("imdb_rating") or 0)
            votes  = int(m.get("imdb_votes")  or 0)
        except (ValueError, TypeError):
            rating, votes = 0, 0
        if rating > 0 and votes > 0:
            d1 = min((rating * math.log10(max(votes, 1))) / 60.0, 1.0) * 100
        else:
            d1 = 20.0

        # ── D2: Intentionality Signal (20%) ───────────────────────────────────
        tags_raw = str(m.get("tags") or "").strip()
        tag_list = [t.strip() for t in tags_raw.split(",")
                    if t.strip() and t.strip().lower() != "nan"]
        d2 = {0: 20.0, 1: 60.0, 2: 80.0}.get(len(tag_list), 100.0)

        # ── D3: Talent Crossover (10%) ────────────────────────────────────────
        people = talent_data.get(imdb_id, [])
        crossover = sum(
            max(0, nconst_film_count.get(p["nconst"], 0) - 1)
            for p in people
        )
        if   crossover >= 10: d3 = 100.0
        elif crossover >= 6:  d3 = 80.0
        elif crossover >= 3:  d3 = 60.0
        elif crossover >= 1:  d3 = 30.0
        else:                 d3 = 0.0

        # ── D4: Franchise Context (15%) ───────────────────────────────────────
        collection = (m.get("collection") or "").strip()
        if not collection:
            d4 = 75.0  # standalone — neutral-positive
        else:
            count_in_lib = collection_counts.get(collection, 0)
            if   count_in_lib >= 3: d4 = 90.0
            elif count_in_lib >= 2: d4 = 70.0
            else:                   d4 = 30.0  # franchise orphan

        # ── D5: Vote Density by Era (10%) ─────────────────────────────────────
        expected = get_era_expected(m.get("year"))
        if votes > 0 and expected > 0:
            d5 = min((votes / expected) * 50, 100.0)
        else:
            d5 = 20.0

        # ── D6: Genre Coherence (10%) ─────────────────────────────────────────
        film_genres = set(
            g.strip() for g in (m.get("genres") or "").split(",") if g.strip()
        )
        overlap = len(film_genres & top_genres)
        if   overlap >= 3: d6 = 100.0
        elif overlap == 2: d6 = 80.0
        elif overlap == 1: d6 = 50.0
        else:              d6 = 10.0

        # ── D7: Audience/Critic Divergence (15%) ──────────────────────────────
        try:
            rt   = float(m.get("rotten_tomatoes") or 0)
        except (ValueError, TypeError):
            rt = 0
        try:
            meta = float(m.get("metacritic") or 0)
        except (ValueError, TypeError):
            meta = 0

        imdb_norm = rating * 10  # scale 0-10 → 0-100
        critic_scores = [s for s in [rt, meta] if s > 0]

        if critic_scores and imdb_norm > 0:
            critic_avg   = sum(critic_scores) / len(critic_scores)
            divergence   = imdb_norm - critic_avg
            consensus    = (imdb_norm + critic_avg) / 2
            if consensus >= 70 and abs(divergence) <= 15:
                d7 = 100.0   # consensus hit
            elif divergence >= 20:
                d7 = 85.0    # audiences love it, critics don't — cult potential
            elif divergence <= -20:
                d7 = 55.0    # critics love it, audiences didn't — prestige pick
            elif consensus < 40:
                d7 = 5.0     # both hate it — hollow schlock
            else:
                d7 = max(consensus, 30.0)
        elif imdb_norm > 0:
            d7 = max(imdb_norm * 0.7, 20.0)
        else:
            d7 = 20.0

        # ── Weighted final score ───────────────────────────────────────────────
        final = round(
            d1 * 0.20 + d2 * 0.20 + d3 * 0.10 +
            d4 * 0.15 + d5 * 0.10 + d6 * 0.10 + d7 * 0.15,
            1
        )

        scores = {
            "d1": round(d1, 1), "d2": round(d2, 1), "d3": round(d3, 1),
            "d4": round(d4, 1), "d5": round(d5, 1), "d6": round(d6, 1),
            "d7": round(d7, 1),
        }
        rows.append({
            "run_id": run_id, "imdb_id": imdb_id, "title": m["title"],
            "d1_score": scores["d1"], "d2_score": scores["d2"],
            "d3_score": scores["d3"], "d4_score": scores["d4"],
            "d5_score": scores["d5"], "d6_score": scores["d6"],
            "d7_score": scores["d7"],
            "final_score": final, "grade": grade(final),
            "teacher_note": build_teacher_note(scores, final),
        })

    if rows:
        conn.execute(
            sql_text("INSERT INTO dna_scores "
                     "(run_id, imdb_id, title, d1_score, d2_score, d3_score, d4_score, "
                     "d5_score, d6_score, d7_score, final_score, grade, teacher_note) "
                     "VALUES (:run_id,:imdb_id,:title,:d1_score,:d2_score,:d3_score,:d4_score,"
                     ":d5_score,:d6_score,:d7_score,:final_score,:grade,:teacher_note)"),
            rows)
    print(f"  DNA scores computed — {len(rows)} films scored.")


# ── Main ───────────────────────────────────────────────────────────────────────
# ── SQLite history ─────────────────────────────────────────────────────────────
def init_db():
    """Create all tables if needed; return the SQLAlchemy engine."""
    engine = get_engine()
    meta = MetaData()

    Table("runs", meta,
        Column("id",          Integer, primary_key=True, autoincrement=True),
        Column("run_date",    String(20),  nullable=False),
        Column("run_ts",      String(25),  nullable=False),
        Column("movie_count", Integer),
        Column("tv_count",    Integer),
        Column("movie_gb",    Float),
        Column("tv_gb",       Float),
        Column("x264_count",  Integer),
        Column("x265_count",  Integer),
    )
    Table("dna_scores", meta,
        Column("id",           Integer, primary_key=True, autoincrement=True),
        Column("run_id",       Integer, nullable=False),
        Column("imdb_id",      String(20)),
        Column("title",        String(512), nullable=False),
        Column("d1_score",     Float), Column("d2_score", Float),
        Column("d3_score",     Float), Column("d4_score", Float),
        Column("d5_score",     Float), Column("d6_score", Float),
        Column("d7_score",     Float),
        Column("final_score",  Float),
        Column("grade",        String(2)),
        Column("teacher_note", Text),
    )
    Table("actor_career", meta,
        Column("nconst",         String(20),  primary_key=True),
        Column("name",           String(255)),
        Column("horror_credits", Text),
        Column("true_breakout",  Text),
        Column("btwf_pre_fame",  Text),
        Column("updated_at",     String(25)),
        Column("cached_date",    String(25), nullable=False),
    )
    Table("talent_cache", meta,
        Column("id",          Integer, primary_key=True, autoincrement=True),
        Column("imdb_id",     String(20),  nullable=False),
        Column("nconst",      String(20),  nullable=False),
        Column("name",        String(255), nullable=False),
        Column("role",        String(50),  nullable=False),
        Column("ordering",    Integer),
        Column("cached_date", String(25),  nullable=False),
    )
    Table("movie_snapshots", meta,
        Column("id",             Integer, primary_key=True, autoincrement=True),
        Column("run_id",         Integer, nullable=False),
        Column("run_date",       String(20), nullable=False),
        Column("radarr_id",      Integer),
        Column("title_slug",     String(512)),
        Column("title",          String(512)),
        Column("original_title", String(512)),
        Column("year",           Integer),
        Column("status",         String(50)),
        Column("studio",         String(255)),
        Column("certification",  String(20)),
        Column("runtime",        Integer),
        Column("genres",         Text),
        Column("keywords",       Text),
        Column("imdb_id",        String(20)),
        Column("tmdb_id",        Integer),
        Column("has_file",       Integer),
        Column("monitored",      Integer),
        Column("added",          String(30)),
        Column("in_cinemas",     String(30)),
        Column("popularity",     Float),
        Column("collection",     String(512)),
        Column("collection_id",  String(50)),
        Column("quality_name",   String(100)),
        Column("source",         String(50)),
        Column("resolution",     Integer),
        Column("is_repack",      Integer),
        Column("release_group",  String(100)),
        Column("edition",        String(100)),
        Column("file_size_gb",   Float),
        Column("cutoff_not_met", Integer),
        Column("video_codec",    String(50)),
        Column("video_bitrate",  Integer),
        Column("bit_depth",      String(10)),
        Column("hdr_type",       String(50)),
        Column("audio_codec",    String(50)),
        Column("audio_channels", String(20)),
        Column("imdb_rating",    Float),
        Column("imdb_votes",     Integer),
        Column("tmdb_rating",    Float),
        Column("metacritic",     Float),
        Column("rotten_tomatoes",Float),
        Column("trakt_rating",   Float),
        Column("trakt_votes",    Integer),
        Column("tags",           Text),
    )
    Table("tv_snapshots", meta,
        Column("id",             Integer, primary_key=True, autoincrement=True),
        Column("run_id",         Integer, nullable=False),
        Column("run_date",       String(20), nullable=False),
        Column("sonarr_id",      Integer),
        Column("title",          String(512)),
        Column("year",           Integer),
        Column("status",         String(50)),
        Column("ended",          Integer),
        Column("network",        String(255)),
        Column("certification",  String(20)),
        Column("runtime",        Integer),
        Column("genres",         Text),
        Column("imdb_id",        String(20)),
        Column("tvdb_id",        Integer),
        Column("monitored",      Integer),
        Column("first_aired",    String(30)),
        Column("last_aired",     String(30)),
        Column("added",          String(30)),
        Column("season_count",   Integer),
        Column("episodes_have",  Integer),
        Column("episodes_total", Integer),
        Column("completion_pct", Float),
        Column("specials_have",  Integer),
        Column("specials_total", Integer),
        Column("has_specials",   Integer),
        Column("size_gb",        Float),
        Column("rating",         Float),
        Column("rating_votes",   Integer),
        Column("tags",           Text),
    )
    Table("franchise_snapshots", meta,
        Column("id",             Integer, primary_key=True, autoincrement=True),
        Column("run_id",         Integer, nullable=False),
        Column("franchise_name", String(512), nullable=False),
        Column("have",           Integer),
        Column("total",          Integer),
        Column("missing_count",  Integer),
        Column("pct",            Float),
        Column("status",         String(100)),
        Column("missing_titles", Text),
    )
    Table("top_talent_snapshots", meta,
        Column("id",              Integer, primary_key=True, autoincrement=True),
        Column("run_id",          Integer, nullable=False),
        Column("name",            String(255), nullable=False),
        Column("role",            String(50),  nullable=False),
        Column("film_count",      Integer),
        Column("avg_rating",      Float),
        Column("top_genre",       String(100)),
        Column("top_genre_count", Integer),
    )

    meta.create_all(engine, checkfirst=True)

    # Schema migrations — add columns that may be missing in older DBs
    insp = sa_inspect(engine)
    with engine.begin() as conn:
        # movie_snapshots: ensure radarr_id column exists
        ms_cols = {c["name"] for c in insp.get_columns("movie_snapshots")}
        if "radarr_id" not in ms_cols:
            conn.execute(sql_text("ALTER TABLE movie_snapshots ADD COLUMN radarr_id INTEGER"))
        # actor_career: ensure btwf_pre_fame + updated_at columns exist
        ac_cols = {c["name"] for c in insp.get_columns("actor_career")}
        if "btwf_pre_fame" not in ac_cols:
            conn.execute(sql_text("ALTER TABLE actor_career ADD COLUMN btwf_pre_fame TEXT"))
        if "updated_at" not in ac_cols:
            conn.execute(sql_text("ALTER TABLE actor_career ADD COLUMN updated_at TEXT"))

    return engine

def imdb_cache_valid():
    """Check if talent cache exists and is not stale."""
    try:
        with get_engine().connect() as conn:
            count = conn.execute(sql_text("SELECT COUNT(*) FROM talent_cache")).fetchone()[0]
            if count == 0:
                return False
            row = conn.execute(sql_text("SELECT cached_date FROM talent_cache LIMIT 1")).fetchone()
            if not row:
                return False
            age = (datetime.now() - datetime.fromisoformat(row[0])).days
            return age < IMDB_MAX_AGE_DAYS
    except Exception:
        return False

def load_imdb_talent_cached(imdb_ids):
    """Load talent from cache, falling back to TSV scan if needed."""
    if imdb_cache_valid():
        print("  Loading IMDb talent from cache...")
        id_set = set(imdb_ids)
        talent = defaultdict(list)
        name_lookup = {}
        with get_engine().connect() as conn:
            for row in conn.execute(sql_text(
                "SELECT imdb_id, nconst, name, role, ordering FROM talent_cache"
            )).fetchall():
                imdb_id, nconst, name, role, ordering = row
                if imdb_id in id_set:
                    talent[imdb_id].append({
                        "name": name, "role": role,
                        "order": ordering or 99, "nconst": nconst
                    })
                name_lookup[nconst] = name
        print(f"  Cache hit — {len(talent)} movies loaded instantly.")
        return talent, name_lookup

    print("  Cache miss — scanning IMDb TSV files...")
    talent, name_lookup = load_imdb_talent(imdb_ids)

    print("  Writing talent cache to DB...")
    cached_date = datetime.now().isoformat()
    rows = []
    for imdb_id, people in talent.items():
        for p in people:
            rows.append({
                "imdb_id": imdb_id, "nconst": p["nconst"],
                "name": p["name"], "role": p["role"],
                "ordering": p["order"], "cached_date": cached_date,
            })
    with get_engine().begin() as conn:
        conn.execute(sql_text("DELETE FROM talent_cache"))
        if rows:
            conn.execute(
                sql_text("INSERT INTO talent_cache (imdb_id,nconst,name,role,ordering,cached_date) "
                         "VALUES (:imdb_id,:nconst,:name,:role,:ordering,:cached_date)"),
                rows)
    print(f"  Talent cache written — {len(rows)} entries.")
    return talent, name_lookup



HORROR_GENRES = {"Horror", "Thriller", "Mystery", "Crime"}

def career_cache_valid():
    try:
        with get_engine().connect() as conn:
            count = conn.execute(sql_text("SELECT COUNT(*) FROM actor_career")).fetchone()[0]
            if count == 0:
                return False
            row = conn.execute(sql_text("SELECT cached_date FROM actor_career LIMIT 1")).fetchone()
            if not row:
                return False
            age = (datetime.now() - datetime.fromisoformat(row[0])).days
            return age < IMDB_MAX_AGE_DAYS
    except Exception:
        return False


def load_actor_career(library_nconsts, library_imdb_ids):
    basics_path     = IMDB_DIR / "title_basics.tsv"
    principals_path = IMDB_DIR / "title_principals.tsv"
    ratings_path    = IMDB_DIR / "title_ratings.tsv"

    if not all(p.exists() for p in [basics_path, principals_path, ratings_path]):
        print("  Career scan skipped — IMDb files missing.")
        return {}

    nconst_set  = set(library_nconsts)
    library_set = set(library_imdb_ids)

    print("  Loading IMDb ratings for career scan...")
    ratings = {}
    with open(ratings_path, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            try:
                ratings[row["tconst"]] = {
                    "rating": float(row["averageRating"]),
                    "votes":  int(row["numVotes"]),
                }
            except (ValueError, KeyError):
                pass

    print("  Scanning principals for career appearances...")
    career_apps = defaultdict(list)
    with open(principals_path, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if row["nconst"] not in nconst_set:
                continue
            if row["category"] not in ("actor", "actress", "director"):
                continue
            try:
                ordering = int(row["ordering"])
            except (ValueError, KeyError):
                ordering = 99
            career_apps[row["nconst"]].append({
                "tconst":   row["tconst"],
                "ordering": ordering,
                "category": row["category"],
            })

    needed = set()
    for apps in career_apps.values():
        for a in apps:
            needed.add(a["tconst"])

    print(f"  Loading basics for {len(needed):,} career titles...")
    title_info = {}
    with open(basics_path, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if row["tconst"] not in needed:
                continue
            if row.get("titleType") not in ("movie", "tvMovie", "video", "tvSeries", "tvMiniSeries"):
                continue
            genres_raw = row.get("genres", "") or ""
            genres = set(g for g in genres_raw.split(",") if g and g != r"\N")
            try:
                year = int(row["startYear"])
            except (ValueError, KeyError):
                year = None
            title_info[row["tconst"]] = {
                "title":  row.get("primaryTitle", ""),
                "year":   year,
                "genres": genres,
            }

    ANIM_GENRES = {"Animation", "Anime"}

    career_data = {}
    for nconst, apps in career_apps.items():
        horror_credits    = []
        all_film_cands    = {}   # tconst -> best entry (lowest ordering) for fame-jump
        lead_candidates   = []   # fallback: billing ≤2, votes ≥100k

        for a in apps:
            tconst = a["tconst"]
            info   = title_info.get(tconst)
            if not info or not info["year"]:
                continue
            r      = ratings.get(tconst, {})
            votes  = r.get("votes", 0)
            rating = r.get("rating", None)

            if info["genres"] & HORROR_GENRES and votes >= 5000:
                horror_credits.append({
                    "tconst":     tconst,
                    "title":      info["title"],
                    "year":       info["year"],
                    "genres":     ", ".join(sorted(g for g in info["genres"] if g)),
                    "rating":     rating,
                    "votes":      votes,
                    "in_library": tconst in library_set,
                    "ordering":   a["ordering"],
                })

            # Fame-jump pool: non-animation, any billing, must have votes
            if votes > 0 and not (info["genres"] & ANIM_GENRES):
                existing = all_film_cands.get(tconst)
                if existing is None or a["ordering"] < existing["ordering"]:
                    all_film_cands[tconst] = {
                        "tconst":   tconst,
                        "title":    info["title"],
                        "year":     info["year"],
                        "votes":    votes,
                        "rating":   rating,
                        "ordering": a["ordering"],
                    }

            # Fallback pool: billing ≤2, votes ≥100k, non-animation
            if a["ordering"] <= 2 and votes >= 100_000 and not (info["genres"] & ANIM_GENRES):
                lead_candidates.append({
                    "tconst":   tconst,
                    "title":    info["title"],
                    "year":     info["year"],
                    "votes":    votes,
                    "rating":   rating,
                    "ordering": a["ordering"],
                })

        horror_credits.sort(key=lambda x: x["year"])

        # ── Fame Jump Algorithm ───────────────────────────────────────────────
        # Sort deduplicated non-animation filmography by (year, votes) ascending
        timeline = sorted(all_film_cands.values(), key=lambda x: (x["year"], x["votes"]))

        breakout   = None
        best_jump  = 0
        best_idx   = None
        for i in range(len(timeline) - 1):
            prev = timeline[i]
            curr = timeline[i + 1]
            if prev["votes"] == 0:
                continue
            jump = curr["votes"] - prev["votes"]
            if curr["votes"] >= 50_000 and curr["votes"] >= 2 * prev["votes"] and jump > best_jump:
                best_jump = jump
                best_idx  = i + 1

        if best_idx is not None:
            breakout = timeline[best_idx]
        elif lead_candidates:
            # Fallback: earliest billing ≤2 film with ≥100k votes
            breakout = sorted(lead_candidates, key=lambda x: x["year"])[0]

        # BTWF: pre-fame supporting appearances (billing ≥3) before breakout year
        btwf_pre_fame = []
        if breakout:
            breakout_year = breakout["year"]
            for a in apps:
                info = title_info.get(a["tconst"])
                if not info or not info["year"]:
                    continue
                if a["ordering"] <= 2:
                    continue
                if info["year"] >= breakout_year:
                    continue
                if info["genres"] & ANIM_GENRES:
                    continue
                r = ratings.get(a["tconst"], {})
                btwf_pre_fame.append({
                    "tconst":     a["tconst"],
                    "title":      info["title"],
                    "year":       info["year"],
                    "ordering":   a["ordering"],
                    "rating":     r.get("rating"),
                    "votes":      r.get("votes", 0),
                    "genres":     ", ".join(sorted(g for g in info["genres"] if g)),
                    "in_library": a["tconst"] in library_set,
                })
            btwf_pre_fame.sort(key=lambda x: x["year"])

        if horror_credits or breakout:
            career_data[nconst] = {
                "horror_credits": horror_credits,
                "true_breakout":  breakout,
                "btwf_pre_fame":  btwf_pre_fame,
            }

    print(f"  Career data built — {len(career_data)} actors with horror/breakout data.")
    return career_data


def load_actor_career_cached(talent_data, name_lookup, library_imdb_ids):
    all_nconsts = set()
    for people in talent_data.values():
        for p in people:
            all_nconsts.add(p["nconst"])

    if career_cache_valid():
        print("  Loading actor career data from cache...")
        with get_engine().connect() as conn:
            rows = conn.execute(sql_text(
                "SELECT nconst, name, horror_credits, true_breakout, btwf_pre_fame FROM actor_career"
            )).fetchall()
        result = {}
        for nconst, name, hc_json, tb_json, btwf_json in rows:
            result[nconst] = {
                "name":           name,
                "horror_credits": json.loads(hc_json) if hc_json else [],
                "true_breakout":  json.loads(tb_json) if tb_json else None,
                "btwf_pre_fame":  json.loads(btwf_json) if btwf_json else [],
            }
        print(f"  Career cache hit — {len(result)} actors loaded.")
        return result

    print("  Career cache miss — scanning IMDb TSVs (takes a few minutes)...")
    career_data = load_actor_career(list(all_nconsts), library_imdb_ids)

    cached_date = datetime.now().isoformat()
    insert_rows = []
    for nconst, data in career_data.items():
        name = name_lookup.get(nconst, "Unknown")
        insert_rows.append({
            "nconst":          nconst,
            "name":            name,
            "horror_credits":  json.dumps(data["horror_credits"]),
            "true_breakout":   json.dumps(data["true_breakout"]) if data["true_breakout"] else None,
            "btwf_pre_fame":   json.dumps(data["btwf_pre_fame"]) if data["btwf_pre_fame"] else None,
            "cached_date":     cached_date,
        })
    with get_engine().begin() as conn:
        conn.execute(sql_text("DELETE FROM actor_career"))
        if insert_rows:
            conn.execute(
                sql_text("INSERT INTO actor_career "
                         "(nconst,name,horror_credits,true_breakout,btwf_pre_fame,cached_date) "
                         "VALUES (:nconst,:name,:horror_credits,:true_breakout,:btwf_pre_fame,:cached_date)"),
                insert_rows)
    print(f"  Actor career cache written — {len(insert_rows)} actors.")
    result = {}
    for nconst, data in career_data.items():
        result[nconst] = {
            "name":           name_lookup.get(nconst, "Unknown"),
            "horror_credits": data["horror_credits"],
            "true_breakout":  data["true_breakout"],
            "btwf_pre_fame":  data["btwf_pre_fame"],
        }
    return result



def write_history(conn, movies, series, run_ts):
    run_date   = run_ts[:10]
    has_file   = [m for m in movies if m["has_file"]]
    movie_gb   = sum(m["file_size_gb"] for m in has_file)
    tv_gb      = sum(s["size_gb"] for s in series)
    x264_count = sum(1 for m in has_file if m["video_codec"] in ("x264", "h264"))
    x265_count = sum(1 for m in has_file if m["video_codec"] in ("x265", "h265", "HEVC"))

    run_id = _insert_returning_id(conn,
        "INSERT INTO runs (run_date, run_ts, movie_count, tv_count, movie_gb, tv_gb, x264_count, x265_count) "
        "VALUES (:run_date, :run_ts, :movie_count, :tv_count, :movie_gb, :tv_gb, :x264_count, :x265_count)",
        {
            "run_date": run_date, "run_ts": run_ts,
            "movie_count": len(movies), "tv_count": len(series),
            "movie_gb": round(movie_gb, 2), "tv_gb": round(tv_gb, 2),
            "x264_count": x264_count, "x265_count": x265_count,
        }
    )

    def _num(v):
        return v if isinstance(v, (int, float)) else None

    conn.execute(
        sql_text("""
            INSERT INTO movie_snapshots
                (run_id, run_date, radarr_id, title_slug, title, original_title, year, status,
                 studio, certification, runtime, genres, keywords, imdb_id, tmdb_id,
                 has_file, monitored, added, in_cinemas, popularity, collection, collection_id,
                 quality_name, source, resolution, is_repack, release_group, edition,
                 file_size_gb, cutoff_not_met, video_codec, video_bitrate, bit_depth, hdr_type,
                 audio_codec, audio_channels, imdb_rating, imdb_votes,
                 tmdb_rating, metacritic, rotten_tomatoes, trakt_rating, trakt_votes, tags)
            VALUES (:run_id,:run_date,:radarr_id,:title_slug,:title,:original_title,:year,:status,
                    :studio,:certification,:runtime,:genres,:keywords,:imdb_id,:tmdb_id,
                    :has_file,:monitored,:added,:in_cinemas,:popularity,:collection,:collection_id,
                    :quality_name,:source,:resolution,:is_repack,:release_group,:edition,
                    :file_size_gb,:cutoff_not_met,:video_codec,:video_bitrate,:bit_depth,:hdr_type,
                    :audio_codec,:audio_channels,:imdb_rating,:imdb_votes,
                    :tmdb_rating,:metacritic,:rotten_tomatoes,:trakt_rating,:trakt_votes,:tags)
        """),
        [{
            "run_id": run_id, "run_date": run_date,
            "radarr_id": m["radarr_id"], "title_slug": m["title_slug"],
            "title": m["title"], "original_title": m["original_title"],
            "year": m["year"], "status": m["status"], "studio": m["studio"],
            "certification": m["certification"], "runtime": m["runtime"],
            "genres": m["genres"], "keywords": m["keywords"],
            "imdb_id": m["imdb_id"], "tmdb_id": m["tmdb_id"],
            "has_file": 1 if m["has_file"] else 0,
            "monitored": 1 if m["monitored"] else 0,
            "added": m["added"], "in_cinemas": m["in_cinemas"],
            "popularity": m["popularity"],
            "collection": m["collection"], "collection_id": m["collection_id"],
            "quality_name": m["quality_name"], "source": m["source"],
            "resolution": m["resolution"],
            "is_repack": 1 if m["is_repack"] else 0,
            "release_group": m["release_group"], "edition": m["edition"],
            "file_size_gb": m["file_size_gb"],
            "cutoff_not_met": 1 if m["cutoff_not_met"] else 0,
            "video_codec": m["video_codec"], "video_bitrate": _num(m["video_bitrate"]),
            "bit_depth": m["bit_depth"], "hdr_type": m["hdr_type"],
            "audio_codec": m["audio_codec"], "audio_channels": m["audio_channels"],
            "imdb_rating": _num(m["imdb_rating"]), "imdb_votes": _num(m["imdb_votes"]),
            "tmdb_rating": _num(m["tmdb_rating"]), "metacritic": _num(m["metacritic"]),
            "rotten_tomatoes": _num(m["rotten_tomatoes"]),
            "trakt_rating": _num(m["trakt_rating"]), "trakt_votes": _num(m["trakt_votes"]),
            "tags": m["tags"],
        } for m in movies]
    )

    conn.execute(
        sql_text("""
            INSERT INTO tv_snapshots
                (run_id, run_date, sonarr_id, title, year, status, ended, network,
                 certification, runtime, genres, imdb_id, tvdb_id, monitored,
                 first_aired, last_aired, added, season_count, episodes_have,
                 episodes_total, completion_pct, specials_have, specials_total,
                 has_specials, size_gb, rating, rating_votes, tags)
            VALUES (:run_id,:run_date,:sonarr_id,:title,:year,:status,:ended,:network,
                    :certification,:runtime,:genres,:imdb_id,:tvdb_id,:monitored,
                    :first_aired,:last_aired,:added,:season_count,:episodes_have,
                    :episodes_total,:completion_pct,:specials_have,:specials_total,
                    :has_specials,:size_gb,:rating,:rating_votes,:tags)
        """),
        [{
            "run_id": run_id, "run_date": run_date,
            "sonarr_id": s["sonarr_id"], "title": s["title"],
            "year": s["year"], "status": s["status"],
            "ended": 1 if s["ended"] else 0, "network": s["network"],
            "certification": s["certification"], "runtime": s["runtime"],
            "genres": s["genres"], "imdb_id": s["imdb_id"], "tvdb_id": s["tvdb_id"],
            "monitored": 1 if s["monitored"] else 0,
            "first_aired": s["first_aired"], "last_aired": s["last_aired"],
            "added": s["added"], "season_count": s["season_count"],
            "episodes_have": s["episodes_have"], "episodes_total": s["episodes_total"],
            "completion_pct": s["completion_pct"],
            "specials_have": s["specials_have"], "specials_total": s["specials_total"],
            "has_specials": 1 if s["has_specials"] else 0,
            "size_gb": s["size_gb"],
            "rating": _num(s["rating"]), "rating_votes": _num(s["rating_votes"]),
            "tags": s["tags"],
        } for s in series]
    )

    print(f"  History written — run_id {run_id} ({run_date})")
    return run_id


def compute_franchise_snapshot(movies, run_id, conn):
    """Compute and store franchise completion data for this run."""
    franchises = defaultdict(list)
    for m in movies:
        if m["collection"]:
            franchises[m["collection"]].append(m)

    rows = []
    for fname, films in franchises.items():
        have    = sum(1 for f in films if f["has_file"])
        total   = len(films)
        missing = [f["title"] for f in films if not f["has_file"]]
        pct     = round(have / total * 100, 1) if total else 0
        status  = "Complete" if have == total else f"{len(missing)} missing"
        rows.append({
            "run_id": run_id, "franchise_name": fname,
            "have": have, "total": total, "missing_count": len(missing),
            "pct": pct, "status": status, "missing_titles": " | ".join(missing),
        })

    if rows:
        conn.execute(
            sql_text("INSERT INTO franchise_snapshots "
                     "(run_id, franchise_name, have, total, missing_count, pct, status, missing_titles) "
                     "VALUES (:run_id,:franchise_name,:have,:total,:missing_count,:pct,:status,:missing_titles)"),
            rows)
    print(f"  Franchise snapshot written — {len(rows)} franchises.")


def compute_top_talent_snapshot(movies, talent_data, run_id, conn):
    """Compute and store top directors/actors for this run."""
    has_file = [m for m in movies if m["has_file"]]
    imdb_to_movie = {m["imdb_id"]: m for m in has_file if m["imdb_id"]}
    EXCLUDE_GENRES = {"Animation", "Anime"}

    directors = Counter()
    actors    = Counter()
    dir_ratings  = defaultdict(list)
    act_ratings  = defaultdict(list)
    dir_genres   = defaultdict(Counter)
    act_genres   = defaultdict(Counter)

    for imdb_id, people in talent_data.items():
        movie = imdb_to_movie.get(imdb_id)
        if not movie:
            continue
        genres = [g.strip() for g in (movie["genres"] or "").split(",") if g.strip()]
        if EXCLUDE_GENRES & set(genres):
            continue
        rating = movie["imdb_rating"] if isinstance(movie["imdb_rating"], (int, float)) else None
        for p in people:
            name = p["name"]
            if p["role"] == "director":
                directors[name] += 1
                if rating:
                    dir_ratings[name].append(rating)
                for g in genres:
                    dir_genres[name][g] += 1
            elif p["role"] in ("actor", "actress") and p["order"] <= 5:
                actors[name] += 1
                if rating:
                    act_ratings[name].append(rating)
                for g in genres:
                    act_genres[name][g] += 1

    rows = []
    for name, count in directors.most_common(50):
        avg_r = round(sum(dir_ratings[name]) / len(dir_ratings[name]), 2) if dir_ratings[name] else None
        tg = dir_genres[name].most_common(1)
        rows.append({"run_id": run_id, "name": name, "role": "director", "film_count": count,
                     "avg_rating": avg_r, "top_genre": tg[0][0] if tg else None,
                     "top_genre_count": tg[0][1] if tg else None})
    for name, count in actors.most_common(75):
        avg_r = round(sum(act_ratings[name]) / len(act_ratings[name]), 2) if act_ratings[name] else None
        tg = act_genres[name].most_common(1)
        rows.append({"run_id": run_id, "name": name, "role": "actor", "film_count": count,
                     "avg_rating": avg_r, "top_genre": tg[0][0] if tg else None,
                     "top_genre_count": tg[0][1] if tg else None})

    if rows:
        conn.execute(
            sql_text("INSERT INTO top_talent_snapshots "
                     "(run_id, name, role, film_count, avg_rating, top_genre, top_genre_count) "
                     "VALUES (:run_id,:name,:role,:film_count,:avg_rating,:top_genre,:top_genre_count)"),
            rows)
    print(f"  Top talent snapshot written — {len(rows)} entries.")


def main():
    print("=" * 60)
    print(f"{DASHBOARD_NAME} Pipeline")
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Started: {run_ts}")
    print("=" * 60)

    movies = fetch_movies()
    series = fetch_series()
    print(f"  Radarr: {len(movies)} movies ({sum(1 for m in movies if m['has_file'])} with files)")
    print(f"  Sonarr: {len(series)} series")

    talent_data = {}
    name_lookup = {}
    if imdb_needs_refresh():
        print("IMDb data missing or stale — downloading...")
        try:
            download_imdb()
            print("  IMDb download complete.")
        except Exception as e:
            print(f"  IMDb download failed: {e}  (talent data will be skipped)")

    imdb_ids = [m["imdb_id"] for m in movies if m["imdb_id"]]
    if (IMDB_DIR / "title_principals.tsv").exists():
        talent_data, name_lookup = load_imdb_talent_cached(imdb_ids)
        print(f"  IMDb talent loaded: {len(talent_data)} movies matched")

    career_data = {}
    if talent_data and (IMDB_DIR / "title_ratings.tsv").exists():
        career_data = load_actor_career_cached(talent_data, name_lookup, imdb_ids)

    print("\nWriting to database...")
    engine = init_db()
    with engine.begin() as conn:
        run_id = write_history(conn, movies, series, run_ts)
        compute_franchise_snapshot(movies, run_id, conn)
        compute_top_talent_snapshot(movies, talent_data, run_id, conn)
        compute_dna_scores(movies, talent_data, run_id, conn)

    print(f"\n  Finished: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 60)

if __name__ == "__main__":
    main()
