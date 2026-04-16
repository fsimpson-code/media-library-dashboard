#!/usr/bin/env python3
"""
library_runner.py
Fetches data from Radarr and Sonarr APIs + IMDb datasets and writes to SQLite.

Run: python3 /scripts/library_runner.py
"""

import os, sys, gzip, csv, json, requests, shutil, sqlite3, math
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from pathlib import Path

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
    DB_PATH     = Path(os.environ.get("DB_PATH", "/scripts/library_history.db"))
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
def compute_dna_scores(movies, talent_data, run_id, con):
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

        # Worst signal
        worst_notes = {
            "d2": "Nobody tagged it — may be an accidental grab.",
            "d1": "Score lacks the vote weight to be credible.",
            "d4": "A franchise orphan — the original isn't even here.",
            "d3": "No talent crossover with the rest of the collection.",
            "d6": "A genre orphan — nothing else here like it.",
            "d5": "Barely registered for its era.",
        }
        if worst_val <= 40 and worst_key in worst_notes:
            note.append(worst_notes[worst_key])

        return " ".join(note)

    # ── External DB lookups for D2 (built once before per-film loop) ─────────
    import urllib.parse

    _RADARR_DB   = "/your/appdata/app_mounts/radarr/config/radarr.db"
    _JELLYSEER_DB = "/your/nas/path/Plex-Config/jellyseerr/db/db.sqlite3"
    _PLEX_DB     = ("/your/nas/path/Plex-Config/Library/Application Support/"
                    "Plex Media Server/Plug-in Support/Databases/"
                    "com.plexapp.plugins.library.db")
    _PLEX_FAMILY   = {1, 29089754, 29091010, 77898712}
    _SEER_FAMILY   = {1, 2, 3, 5}

    def _ro(path):
        """Open a SQLite DB read-only, handling spaces in path."""
        uri = "file:" + urllib.parse.quote(path, safe="/:") + "?mode=ro"
        return sqlite3.connect(uri, uri=True)

    # A. Radarr: imdb_id → releaseSource (most recent grab) + added date + tmdb bridge
    radarr_source = {}
    radarr_added  = {}
    tmdb_map      = {}
    try:
        _rc = _ro(_RADARR_DB)
        for imdb_id, src in _rc.execute("""
            SELECT mm.ImdbId, json_extract(h.Data, '$.releaseSource')
            FROM History h
            JOIN Movies mov ON mov.Id = h.MovieId
            JOIN MovieMetadata mm ON mm.Id = mov.MovieMetadataId
            WHERE h.EventType = 1
              AND mm.ImdbId IS NOT NULL AND mm.ImdbId != ''
            ORDER BY h.Date ASC
        """).fetchall():
            if imdb_id:
                radarr_source[imdb_id] = src   # ASC order → last row = most recent
        for imdb_id, added_str in _rc.execute("""
            SELECT mm.ImdbId, mov.Added
            FROM Movies mov
            JOIN MovieMetadata mm ON mm.Id = mov.MovieMetadataId
            WHERE mm.ImdbId IS NOT NULL AND mm.ImdbId != ''
        """).fetchall():
            if imdb_id and added_str:
                try:
                    radarr_added[imdb_id] = datetime.fromisoformat(added_str[:19])
                except ValueError:
                    pass
        for imdb_id, tmdb_id in _rc.execute("""
            SELECT ImdbId, TmdbId FROM MovieMetadata
            WHERE ImdbId IS NOT NULL AND ImdbId != '' AND TmdbId IS NOT NULL
        """).fetchall():
            if imdb_id and tmdb_id:
                tmdb_map[imdb_id] = int(tmdb_id)
        _rc.close()
        print(f"  D2: Radarr — {len(radarr_source)} grab sources, {len(tmdb_map)} tmdb bridges")
    except Exception as _e:
        print(f"  D2 WARNING: Radarr DB unavailable ({_e}) — base score defaults to 10")

    # B. Jellyseer: tmdb_id (int) → "family" | "other"
    jellyseer_requests = {}
    try:
        _jc = _ro(_JELLYSEER_DB)
        for tmdb_id, req_by in _jc.execute("""
            SELECT m.tmdbId, r.requestedById
            FROM media_request r
            JOIN media m ON m.id = r.mediaId
            WHERE r.type = 'movie' AND r.status = 5
        """).fetchall():
            if tmdb_id is None:
                continue
            label = "family" if req_by in _SEER_FAMILY else "other"
            if jellyseer_requests.get(int(tmdb_id)) != "family":
                jellyseer_requests[int(tmdb_id)] = label
        _jc.close()
        print(f"  D2: Jellyseer — {len(jellyseer_requests)} movie requests indexed")
    except Exception as _e:
        print(f"  D2 WARNING: Jellyseer DB unavailable ({_e}) — request bonus skipped")

    # C. Plex: title_lower → {family_viewers, any_rewatch, has_any_play}
    #    Primary join: metadata_item_views → metadata_items via guid
    #    Fallback: unmatched guids use miv.title directly (recovers films only
    #              on other Plex servers like Predator, Dances with Wolves, Taken)
    plex_plays = {}
    try:
        _pc = _ro(_PLEX_DB)
        _raw = {}   # title_lower → {account_id: view_count}

        # Primary: guid join gives authoritative titles
        for title, acct_id, view_count in _pc.execute("""
            SELECT mi.title, miv.account_id, COUNT(*) as vc
            FROM metadata_item_views miv
            JOIN metadata_items mi ON mi.guid = miv.guid
            WHERE miv.metadata_type = 1
            GROUP BY mi.title, miv.account_id
        """).fetchall():
            key = (title or "").lower().strip()
            _raw.setdefault(key, {})[acct_id] = view_count

        # Fallback: guids not present in local metadata_items — use miv.title
        for title, acct_id, view_count in _pc.execute("""
            SELECT miv.title, miv.account_id, COUNT(*) as vc
            FROM metadata_item_views miv
            WHERE miv.metadata_type = 1
              AND miv.title IS NOT NULL AND miv.title != ''
              AND NOT EXISTS (
                  SELECT 1 FROM metadata_items mi WHERE mi.guid = miv.guid
              )
            GROUP BY miv.title, miv.account_id
        """).fetchall():
            key = (title or "").lower().strip()
            if key:
                _raw.setdefault(key, {})[acct_id] = view_count

        _pc.close()
        for key, acct_counts in _raw.items():
            plex_plays[key] = {
                "family_viewers": sum(1 for a in acct_counts if a in _PLEX_FAMILY),
                "any_rewatch":    any(v > 1 for a, v in acct_counts.items()
                                      if a in _PLEX_FAMILY),
                "has_any_play":   True,
            }
        print(f"  D2: Plex — {len(plex_plays)} films with play history")
    except Exception as _e:
        print(f"  D2 WARNING: Plex DB unavailable ({_e}) — play bonus and neglect penalty skipped")

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

        # ── D2: Intentionality Signal (25%) ───────────────────────────────────
        tags_raw = str(m.get("tags") or "").strip()
        tag_list = [t.strip() for t in tags_raw.split(",")
                    if t.strip() and t.strip().lower() != "nan"]

        # Base: was this grabbed intentionally?
        _src = radarr_source.get(imdb_id)
        d2 = 60.0 if _src in ("UserInvokedSearch", "InteractiveSearch") else 10.0

        # Jellyseer request bonus (someone explicitly asked for it)
        _tmdb = m.get("tmdb_id") or tmdb_map.get(imdb_id)
        if _tmdb:
            _req = jellyseer_requests.get(int(_tmdb))
            if _req == "family":
                d2 += 40.0
            elif _req == "other":
                d2 += 30.0

        # Plex play bonus (family actually watched it)
        _pkey = (m.get("title") or "").lower().strip()
        _plays = plex_plays.get(_pkey, {})
        _fv = _plays.get("family_viewers", 0)
        if   _fv >= 3: d2 += 35.0
        elif _fv == 2: d2 += 25.0
        elif _fv == 1: d2 += 15.0
        if _plays.get("any_rewatch"):
            d2 += 5.0

        # Tags minor signal
        if tag_list:
            d2 += 5.0

        # Neglect penalty: 365+ days in library, never played by anyone
        _added = radarr_added.get(imdb_id)
        if _added and not _plays.get("has_any_play", False):
            if (datetime.now() - _added).days >= 365:
                d2 -= 10.0

        d2 = max(0.0, min(100.0, d2))

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

        d7 = 0.0  # D7 retired — stub preserved for DB schema compatibility

        # ── Weighted final score ───────────────────────────────────────────────
        final = round(
            d1 * 0.25 + d2 * 0.25 + d3 * 0.10 +
            d4 * 0.20 + d5 * 0.10 + d6 * 0.10,
            1
        )

        scores = {
            "d1": round(d1, 1), "d2": round(d2, 1), "d3": round(d3, 1),
            "d4": round(d4, 1), "d5": round(d5, 1), "d6": round(d6, 1),
            "d7": round(d7, 1),
        }
        rows.append((
            run_id, imdb_id, m["title"],
            scores["d1"], scores["d2"], scores["d3"], scores["d4"],
            scores["d5"], scores["d6"], scores["d7"],
            final, grade(final), build_teacher_note(scores, final),
        ))

    con.executemany("""
        INSERT INTO dna_scores
            (run_id, imdb_id, title,
             d1_score, d2_score, d3_score, d4_score,
             d5_score, d6_score, d7_score,
             final_score, grade, teacher_note)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    con.commit()
    print(f"  DNA scores computed — {len(rows)} films scored.")


# ── Main ───────────────────────────────────────────────────────────────────────
# ── SQLite history ─────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # runs — historical, append-only
    cur.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date    TEXT NOT NULL,
            run_ts      TEXT NOT NULL,
            movie_count INTEGER,
            tv_count    INTEGER,
            movie_gb    REAL,
            tv_gb       REAL,
            x264_count  INTEGER,
            x265_count  INTEGER
        )
    """)

    # dna_scores — derived, run-keyed
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dna_scores (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id       INTEGER NOT NULL,
            imdb_id      TEXT,
            title        TEXT NOT NULL,
            d1_score     REAL,
            d2_score     REAL,
            d3_score     REAL,
            d4_score     REAL,
            d5_score     REAL,
            d6_score     REAL,
            d7_score     REAL,
            final_score  REAL,
            grade        TEXT,
            teacher_note TEXT,
            FOREIGN KEY (run_id) REFERENCES runs(id)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dna_run ON dna_scores(run_id)")

    # actor_career — cached, not run-keyed
    cur.execute("""
        CREATE TABLE IF NOT EXISTS actor_career (
            nconst          TEXT PRIMARY KEY,
            name            TEXT,
            horror_credits  TEXT,
            true_breakout   TEXT,
            btwf_pre_fame   TEXT,
            cached_date     TEXT NOT NULL
        )
    """)

    # talent_cache — cached, not run-keyed
    cur.execute("""
        CREATE TABLE IF NOT EXISTS talent_cache (
            imdb_id     TEXT NOT NULL,
            nconst      TEXT NOT NULL,
            name        TEXT NOT NULL,
            role        TEXT NOT NULL,
            ordering    INTEGER,
            cached_date TEXT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_talent_imdb ON talent_cache(imdb_id)")

    con.commit()

    # Drop and recreate run-keyed snapshot tables with full schema
    existing = {r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    needs_rebuild = "movie_snapshots" not in existing
    if not needs_rebuild:
        cols = {r[1] for r in con.execute("PRAGMA table_info(movie_snapshots)").fetchall()}
        if "radarr_id" not in cols:
            needs_rebuild = True
    if needs_rebuild:
        con.execute("DROP TABLE IF EXISTS movie_snapshots")
        con.execute("DROP TABLE IF EXISTS tv_snapshots")
        con.execute("DROP TABLE IF EXISTS franchise_snapshots")
        con.execute("DROP TABLE IF EXISTS top_talent_snapshots")

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS movie_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            run_date        TEXT NOT NULL,
            radarr_id       INTEGER,
            title_slug      TEXT,
            title           TEXT,
            original_title  TEXT,
            year            INTEGER,
            status          TEXT,
            studio          TEXT,
            certification   TEXT,
            runtime         INTEGER,
            genres          TEXT,
            keywords        TEXT,
            imdb_id         TEXT,
            tmdb_id         INTEGER,
            has_file        INTEGER,
            monitored       INTEGER,
            added           TEXT,
            in_cinemas      TEXT,
            popularity      REAL,
            collection      TEXT,
            collection_id   TEXT,
            quality_name    TEXT,
            source          TEXT,
            resolution      INTEGER,
            is_repack       INTEGER,
            release_group   TEXT,
            edition         TEXT,
            file_size_gb    REAL,
            cutoff_not_met  INTEGER,
            video_codec     TEXT,
            video_bitrate   INTEGER,
            bit_depth       TEXT,
            hdr_type        TEXT,
            audio_codec     TEXT,
            audio_channels  TEXT,
            imdb_rating     REAL,
            imdb_votes      INTEGER,
            tmdb_rating     REAL,
            metacritic      REAL,
            rotten_tomatoes REAL,
            trakt_rating    REAL,
            trakt_votes     INTEGER,
            tags            TEXT,
            FOREIGN KEY (run_id) REFERENCES runs(id)
        );
        CREATE TABLE IF NOT EXISTS tv_snapshots (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id              INTEGER NOT NULL,
            run_date            TEXT NOT NULL,
            sonarr_id           INTEGER,
            title               TEXT,
            year                INTEGER,
            status              TEXT,
            ended               INTEGER,
            network             TEXT,
            certification       TEXT,
            runtime             INTEGER,
            genres              TEXT,
            imdb_id             TEXT,
            tvdb_id             INTEGER,
            monitored           INTEGER,
            first_aired         TEXT,
            last_aired          TEXT,
            added               TEXT,
            season_count        INTEGER,
            episodes_have       INTEGER,
            episodes_total      INTEGER,
            completion_pct      REAL,
            specials_have       INTEGER,
            specials_total      INTEGER,
            has_specials        INTEGER,
            size_gb             REAL,
            rating              REAL,
            rating_votes        INTEGER,
            tags                TEXT,
            FOREIGN KEY (run_id) REFERENCES runs(id)
        );
        CREATE TABLE IF NOT EXISTS franchise_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            franchise_name  TEXT NOT NULL,
            have            INTEGER,
            total           INTEGER,
            missing_count   INTEGER,
            pct             REAL,
            status          TEXT,
            missing_titles  TEXT,
            FOREIGN KEY (run_id) REFERENCES runs(id)
        );
        CREATE TABLE IF NOT EXISTS top_talent_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            name            TEXT NOT NULL,
            role            TEXT NOT NULL,
            film_count      INTEGER,
            avg_rating      REAL,
            top_genre       TEXT,
            top_genre_count INTEGER,
            FOREIGN KEY (run_id) REFERENCES runs(id)
        );
    """)
    con.commit()

    con.execute("""
        CREATE TABLE IF NOT EXISTS watch_resolved (
            account_id      INTEGER NOT NULL,
            guid            TEXT NOT NULL,
            watch_type      TEXT NOT NULL,
            corroborated_by TEXT,
            updated_at      INTEGER,
            PRIMARY KEY (account_id, guid)
        )
    """)
    con.commit()

    # Migrations for cached tables
    try:
        con.execute("ALTER TABLE actor_career ADD COLUMN btwf_pre_fame TEXT")
        con.commit()
    except Exception:
        pass
    return con

def imdb_cache_valid():
    """Check if talent cache exists and is not stale."""
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM talent_cache")
        count = cur.fetchone()[0]
        if count == 0:
            con.close()
            return False
        cur.execute("SELECT cached_date FROM talent_cache LIMIT 1")
        row = cur.fetchone()
        con.close()
        if not row:
            return False
        from datetime import datetime
        age = (datetime.now() - datetime.fromisoformat(row[0])).days
        return age < IMDB_MAX_AGE_DAYS
    except Exception:
        return False

def load_imdb_talent_cached(imdb_ids):
    """Load talent from SQLite cache, falling back to TSV scan if needed."""
    # Ensure cache table exists
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS talent_cache (
            imdb_id     TEXT NOT NULL,
            nconst      TEXT NOT NULL,
            name        TEXT NOT NULL,
            role        TEXT NOT NULL,
            ordering    INTEGER,
            cached_date TEXT NOT NULL
        )""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_talent_imdb ON talent_cache(imdb_id)")
    con.commit()
    
    if imdb_cache_valid():
        print("  Loading IMDb talent from cache...")
        id_set = set(imdb_ids)
        talent = defaultdict(list)
        name_lookup = {}
        cur = con.execute(
            "SELECT imdb_id, nconst, name, role, ordering FROM talent_cache")
        for imdb_id, nconst, name, role, ordering in cur.fetchall():
            if imdb_id in id_set:
                talent[imdb_id].append({
                    "name": name, "role": role,
                    "order": ordering or 99, "nconst": nconst
                })
            name_lookup[nconst] = name
        con.close()
        print(f"  Cache hit — {len(talent)} movies loaded instantly.")
        return talent, name_lookup
    
    # Cache miss — do full TSV scan
    con.close()
    print("  Cache miss — scanning IMDb TSV files...")
    talent, name_lookup = load_imdb_talent(imdb_ids)
    
    # Write to cache
    print("  Writing talent cache to SQLite...")
    cached_date = datetime.now().isoformat()
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM talent_cache")
    rows = []
    for imdb_id, people in talent.items():
        for p in people:
            rows.append((imdb_id, p["nconst"], p["name"],
                         p["role"], p["order"], cached_date))
    con.executemany(
        "INSERT INTO talent_cache (imdb_id,nconst,name,role,ordering,cached_date) VALUES (?,?,?,?,?,?)",
        rows)
    con.commit()
    con.close()
    print(f"  Talent cache written — {len(rows)} entries.")
    return talent, name_lookup



HORROR_GENRES = {"Horror", "Thriller", "Mystery", "Crime"}

def career_cache_valid():
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM actor_career")
        count = cur.fetchone()[0]
        if count == 0:
            con.close()
            return False
        cur.execute("SELECT cached_date FROM actor_career LIMIT 1")
        row = cur.fetchone()
        con.close()
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
        con = sqlite3.connect(DB_PATH)
        # Migrate: add btwf_pre_fame column if missing, then force rebuild so it gets populated
        col_names = [r[1] for r in con.execute("PRAGMA table_info(actor_career)").fetchall()]
        if "btwf_pre_fame" not in col_names:
            con.execute("ALTER TABLE actor_career ADD COLUMN btwf_pre_fame TEXT")
            con.commit()
            con.close()
            print("  btwf_pre_fame column added — forcing career rebuild...")
        else:
            rows = con.execute(
                "SELECT nconst, name, horror_credits, true_breakout, btwf_pre_fame FROM actor_career"
            ).fetchall()
            con.close()
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
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM actor_career")
    insert_rows = []
    for nconst, data in career_data.items():
        name = name_lookup.get(nconst, "Unknown")
        insert_rows.append((
            nconst, name,
            json.dumps(data["horror_credits"]),
            json.dumps(data["true_breakout"]) if data["true_breakout"] else None,
            json.dumps(data["btwf_pre_fame"]) if data["btwf_pre_fame"] else None,
            cached_date,
        ))
    con.executemany(
        "INSERT OR REPLACE INTO actor_career (nconst,name,horror_credits,true_breakout,btwf_pre_fame,cached_date) VALUES (?,?,?,?,?,?)",
        insert_rows
    )
    con.commit()
    con.close()
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



def write_history(con, movies, series, run_ts):
    run_date   = run_ts[:10]
    has_file   = [m for m in movies if m["has_file"]]
    movie_gb   = sum(m["file_size_gb"] for m in has_file)
    tv_gb      = sum(s["size_gb"] for s in series)
    x264_count = sum(1 for m in has_file if m["video_codec"] in ("x264", "h264"))
    x265_count = sum(1 for m in has_file if m["video_codec"] in ("x265", "h265", "HEVC"))

    cur = con.cursor()
    cur.execute("""
        INSERT INTO runs
            (run_date, run_ts, movie_count, tv_count, movie_gb, tv_gb,
             x264_count, x265_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (run_date, run_ts, len(movies), len(series),
          round(movie_gb, 2), round(tv_gb, 2),
          x264_count, x265_count))
    run_id = cur.lastrowid

    def _num(v):
        return v if isinstance(v, (int, float)) else None

    cur.executemany("""
        INSERT INTO movie_snapshots
            (run_id, run_date, radarr_id, title_slug, title, original_title, year, status,
             studio, certification, runtime, genres, keywords, imdb_id, tmdb_id,
             has_file, monitored, added, in_cinemas, popularity, collection, collection_id,
             quality_name, source, resolution, is_repack, release_group, edition,
             file_size_gb, cutoff_not_met, video_codec, video_bitrate, bit_depth, hdr_type,
             audio_codec, audio_channels, imdb_rating, imdb_votes,
             tmdb_rating, metacritic, rotten_tomatoes, trakt_rating, trakt_votes, tags)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [(
        run_id, run_date,
        m["radarr_id"], m["title_slug"], m["title"], m["original_title"],
        m["year"], m["status"], m["studio"], m["certification"], m["runtime"],
        m["genres"], m["keywords"], m["imdb_id"], m["tmdb_id"],
        1 if m["has_file"] else 0, 1 if m["monitored"] else 0,
        m["added"], m["in_cinemas"], m["popularity"],
        m["collection"], m["collection_id"],
        m["quality_name"], m["source"], m["resolution"],
        1 if m["is_repack"] else 0, m["release_group"], m["edition"],
        m["file_size_gb"], 1 if m["cutoff_not_met"] else 0,
        m["video_codec"], _num(m["video_bitrate"]), m["bit_depth"], m["hdr_type"],
        m["audio_codec"], m["audio_channels"],
        _num(m["imdb_rating"]), _num(m["imdb_votes"]),
        _num(m["tmdb_rating"]), _num(m["metacritic"]),
        _num(m["rotten_tomatoes"]), _num(m["trakt_rating"]), _num(m["trakt_votes"]),
        m["tags"],
    ) for m in movies])

    cur.executemany("""
        INSERT INTO tv_snapshots
            (run_id, run_date, sonarr_id, title, year, status, ended, network,
             certification, runtime, genres, imdb_id, tvdb_id, monitored,
             first_aired, last_aired, added, season_count, episodes_have,
             episodes_total, completion_pct, specials_have, specials_total,
             has_specials, size_gb, rating, rating_votes, tags)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [(
        run_id, run_date,
        s["sonarr_id"], s["title"], s["year"], s["status"],
        1 if s["ended"] else 0, s["network"], s["certification"], s["runtime"],
        s["genres"], s["imdb_id"], s["tvdb_id"],
        1 if s["monitored"] else 0,
        s["first_aired"], s["last_aired"], s["added"],
        s["season_count"], s["episodes_have"], s["episodes_total"],
        s["completion_pct"], s["specials_have"], s["specials_total"],
        1 if s["has_specials"] else 0, s["size_gb"],
        _num(s["rating"]), _num(s["rating_votes"]), s["tags"],
    ) for s in series])

    con.commit()
    print(f"  History written — run_id {run_id} ({run_date})")
    return run_id


def compute_franchise_snapshot(movies, run_id, con):
    """Compute and store franchise completion data for this run."""
    from collections import defaultdict
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
        rows.append((run_id, fname, have, total, len(missing), pct, status,
                     " | ".join(missing)))

    con.executemany("""
        INSERT INTO franchise_snapshots
            (run_id, franchise_name, have, total, missing_count, pct, status, missing_titles)
        VALUES (?,?,?,?,?,?,?,?)
    """, rows)
    con.commit()
    print(f"  Franchise snapshot written — {len(rows)} franchises.")


def compute_top_talent_snapshot(movies, talent_data, run_id, con):
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
        rows.append((run_id, name, "director", count, avg_r,
                     tg[0][0] if tg else None, tg[0][1] if tg else None))
    for name, count in actors.most_common(75):
        avg_r = round(sum(act_ratings[name]) / len(act_ratings[name]), 2) if act_ratings[name] else None
        tg = act_genres[name].most_common(1)
        rows.append((run_id, name, "actor", count, avg_r,
                     tg[0][0] if tg else None, tg[0][1] if tg else None))

    con.executemany("""
        INSERT INTO top_talent_snapshots
            (run_id, name, role, film_count, avg_rating, top_genre, top_genre_count)
        VALUES (?,?,?,?,?,?,?)
    """, rows)
    con.commit()
    print(f"  Top talent snapshot written — {len(rows)} entries.")



def resolve_watch_history(con):
    """Classify every (account_id, guid) into real_stream / corroborated_mark / unverified_mark."""
    import urllib.parse, time

    PLEX_DB = (
        "/your/nas/path/Plex-Config/Library/Application Support/"
        "Plex Media Server/Plug-in Support/Databases/"
        "com.plexapp.plugins.library.db"
    )
    FAMILY   = {1, 29089754, 29091010, 77898712, 222944852}
    EXTENDED = {3670375}
    FRIENDS  = {137417867, 210861484, 615117225}
    SCOPED   = FAMILY | EXTENDED

    print("\nResolving watch history...")

    def _ro(path):
        uri = "file:" + urllib.parse.quote(path, safe="/:") + "?mode=ro"
        return sqlite3.connect(uri, uri=True)

    try:
        pc = _ro(PLEX_DB)
    except Exception as e:
        print(f"  watch_resolved: Plex DB unavailable ({e}) — skipping")
        return

    ph     = ",".join("?" * len(SCOPED))
    params = list(SCOPED)

    real_streams = set(pc.execute(
        f"SELECT DISTINCT account_id, guid FROM metadata_item_views WHERE account_id IN ({ph})",
        params
    ).fetchall())

    all_watched = pc.execute(
        f"SELECT DISTINCT account_id, guid FROM metadata_item_settings "
        f"WHERE view_count > 0 AND account_id IN ({ph})",
        params
    ).fetchall()
    pc.close()

    # Build family real-stream index for corroboration lookups
    family_streams_by_guid = defaultdict(set)
    for acct_id, guid in real_streams:
        if acct_id in FAMILY:
            family_streams_by_guid[guid].add(acct_id)

    now  = int(time.time())
    rows = []

    # Classify settings records
    watched_pairs = set()
    for acct_id, guid in all_watched:
        if acct_id in FRIENDS:
            continue
        watched_pairs.add((acct_id, guid))
        if (acct_id, guid) in real_streams:
            rows.append((acct_id, guid, "real_stream", None, now))
        elif acct_id in FAMILY:
            corroborators = family_streams_by_guid[guid] - {acct_id}
            if corroborators:
                rows.append((acct_id, guid, "corroborated_mark",
                              ",".join(str(a) for a in sorted(corroborators)), now))
            else:
                rows.append((acct_id, guid, "unverified_mark", None, now))
        # Extended (3670375): marks-only → skip

    # Add real_stream records that exist in views but not in settings
    for acct_id, guid in real_streams:
        if acct_id in FRIENDS:
            continue
        if (acct_id, guid) not in watched_pairs:
            rows.append((acct_id, guid, "real_stream", None, now))

    con.executemany(
        "INSERT OR REPLACE INTO watch_resolved "
        "(account_id, guid, watch_type, corroborated_by, updated_at) VALUES (?,?,?,?,?)",
        rows
    )
    con.commit()

    counts = {}
    for r in rows:
        counts[r[2]] = counts.get(r[2], 0) + 1
    print(f"  watch_resolved: {len(rows)} records written.")
    for wt, cnt in sorted(counts.items()):
        print(f"    {wt}: {cnt}")


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
    con = init_db()
    run_id = write_history(con, movies, series, run_ts)
    compute_franchise_snapshot(movies, run_id, con)
    compute_top_talent_snapshot(movies, talent_data, run_id, con)
    compute_dna_scores(movies, talent_data, run_id, con)
    resolve_watch_history(con)
    con.close()

    print(f"\n  Finished: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 60)

if __name__ == "__main__":
    main()
