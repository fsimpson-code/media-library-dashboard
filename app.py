#!/usr/bin/env python3
"""
Media Library Dashboard — Flask backend
All data served from SQLite. No xlsx dependency.
"""

import os, re, json, sqlite3
from pathlib import Path
from collections import Counter, defaultdict
from flask import Flask, jsonify, send_file, abort, Response, stream_with_context, request
import requests as req
import subprocess
import threading
from dotenv import load_dotenv
from sqlalchemy import create_engine, text as sql_text
load_dotenv()

app = Flask(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
try:
    from config import (
        RADARR_URL, RADARR_API_KEY,
        SEERR_URL,
        DASHBOARD_NAME, DB_PATH as _CFG_DB, RUNNER_URL as _CFG_RUNNER,
        FINGERPRINT_ENABLED, FINGERPRINT_MEMBERS, HATED_ENABLED,
        DASHBOARD_PORT,
    )
except ImportError:
    RADARR_URL          = os.environ.get("RADARR_URL", "")
    RADARR_API_KEY      = os.environ.get("RADARR_API_KEY", "")
    SEERR_URL           = os.environ.get("SEERR_URL", "")
    DASHBOARD_NAME      = os.environ.get("DASHBOARD_NAME", "Media Library")
    _CFG_DB             = os.environ.get("DB_PATH", "/data/library_history.db")
    _CFG_RUNNER         = os.environ.get("RUNNER_URL", "http://localhost:5757")
    FINGERPRINT_ENABLED = os.environ.get("FINGERPRINT_ENABLED", "false").lower() == "true"
    FINGERPRINT_MEMBERS = [m.strip() for m in os.environ.get("FINGERPRINT_MEMBERS", "").split(",") if m.strip()]
    HATED_ENABLED       = os.environ.get("HATED_ENABLED", "false").lower() == "true"
    DASHBOARD_PORT      = int(os.environ.get("DASHBOARD_PORT", 8686))

RUNNER_URL = os.environ.get("RUNNER_URL", _CFG_RUNNER)
DB_PATH    = Path(os.environ.get("DB_PATH", str(_CFG_DB)))

# Palette cycles for dynamically generated member colors
_PALETTE = ["#00e5ff", "#e040fb", "#00ff9f", "#ffcc00", "#b060ff",
            "#ff6b6b", "#4fc3f7", "#aed581", "#ffb74d", "#f48fb1"]

def _member_colors(members):
    return {m: _PALETTE[i % len(_PALETTE)] for i, m in enumerate(members)}


# ── Multi-backend DB engine ───────────────────────────────────────────────────
ENV_PATH = Path("/app/.env")
_engine = None
_engine_lock = threading.Lock()
_migrate_proc = None
_migrate_lines = []

def get_engine():
    """Return SQLAlchemy engine based on DB_TYPE env var."""
    global _engine
    with _engine_lock:
        if _engine is None:
            db_type = os.getenv("DB_TYPE", "sqlite")
            if db_type == "postgres":
                _engine = create_engine(
                    "postgresql://{}:{}@{}:{}/{}".format(
                        os.getenv("DB_USER", ""), os.getenv("DB_PASS", ""),
                        os.getenv("DB_HOST", ""), os.getenv("DB_PORT", "5432"),
                        os.getenv("DB_NAME", "")
                    )
                )
            elif db_type == "mysql":
                _engine = create_engine(
                    "mysql+pymysql://{}:{}@{}:{}/{}".format(
                        os.getenv("DB_USER", ""), os.getenv("DB_PASS", ""),
                        os.getenv("DB_HOST", ""), os.getenv("DB_PORT", "3306"),
                        os.getenv("DB_NAME", "")
                    )
                )
            else:
                db_path = os.getenv("DB_PATH", str(DB_PATH))
                _engine = create_engine("sqlite:///{}".format(db_path))
    return _engine

def _db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def _latest_run_id(con):
    cur = con.execute("SELECT id FROM runs ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    return row[0] if row else None

def _has_data():
    if not DB_PATH.exists():
        return False
    try:
        con = _db()
        run_id = _latest_run_id(con)
        con.close()
        return run_id is not None
    except Exception:
        return False

def top_n(counter, n=10):
    return [{"label": k, "value": v} for k, v in counter.most_common(n)]


@app.route("/api/config")
def api_config():
    from version import __version__
    members = FINGERPRINT_MEMBERS if FINGERPRINT_ENABLED else []
    colors  = _member_colors(members)
    return jsonify({
        "name":                 DASHBOARD_NAME,
        "fingerprint_enabled":  FINGERPRINT_ENABLED,
        "hated_enabled":        HATED_ENABLED and FINGERPRINT_ENABLED,
        "members":              [{"name": m, "color": colors[m]} for m in members],
        "seerr_url":            SEERR_URL,
        "version":              __version__,
        "default_tab":          os.getenv("DEFAULT_TAB", "movies"),
        "theme":                os.getenv("THEME", "neon-noir"),
        "theme_custom_bg":      os.getenv("THEME_CUSTOM_BG", ""),
        "theme_custom_surface": os.getenv("THEME_CUSTOM_SURFACE", ""),
        "theme_custom_accent1": os.getenv("THEME_CUSTOM_ACCENT1", ""),
        "theme_custom_accent2": os.getenv("THEME_CUSTOM_ACCENT2", ""),
        "theme_custom_text":    os.getenv("THEME_CUSTOM_TEXT", ""),
        "theme_custom_muted":   os.getenv("THEME_CUSTOM_MUTED", ""),
        "theme_custom_positive":os.getenv("THEME_CUSTOM_POSITIVE", ""),
        "theme_custom_warning": os.getenv("THEME_CUSTOM_WARNING", ""),
        "theme_custom_danger":  os.getenv("THEME_CUSTOM_DANGER", ""),
    })

def parse_movies():
    con = _db()
    run_id = _latest_run_id(con)
    if run_id is None:
        con.close()
        raise FileNotFoundError("No runs found — run an update first.")

    rows = [dict(r) for r in con.execute("""
        SELECT title, year, has_file, monitored, video_codec, resolution, hdr_type,
               file_size_gb, release_group, cutoff_not_met, imdb_rating, quality_name,
               source, bit_depth, certification, genres, studio, runtime, tags,
               imdb_votes, metacritic, rotten_tomatoes
        FROM movie_snapshots WHERE run_id = ?
    """, (run_id,)).fetchall()]
    con.close()

    has_file = [m for m in rows if m["has_file"]]
    total       = len(rows)
    downloaded  = len(has_file)
    wishlist    = total - downloaded
    upgrade_q   = sum(1 for m in rows if m["cutoff_not_met"])
    total_gb    = sum(m["file_size_gb"] or 0 for m in has_file)
    avg_gb      = round(total_gb / downloaded, 1) if downloaded else 0

    with_rt = [m for m in has_file if m["runtime"] and m["runtime"] > 0]
    if with_rt:
        avg_gb_per_hr = round(
            sum(m["file_size_gb"] or 0 for m in with_rt) / (sum(m["runtime"] for m in with_rt) / 60), 2
        )
    else:
        avg_gb_per_hr = 0

    res_map = {2160: "4K (2160p)", 1080: "1080p", 720: "720p", 480: "480p"}
    res_c = Counter()
    for m in has_file:
        try:
            res_c[res_map.get(int(m["resolution"]), str(int(m["resolution"])) + "p")] += 1
        except (ValueError, TypeError):
            res_c["Unknown"] += 1

    hdr_c   = Counter(m["hdr_type"] or "SDR" for m in has_file)
    codec_c = Counter(m["video_codec"] for m in has_file if m["video_codec"])

    source_labels = {"webdl": "WEB-DL", "bluray": "Blu-ray", "webrip": "WEBRip",
                     "tv": "TV Capture", "dvd": "DVD", "unknown": "Unknown"}
    src_c = Counter()
    for m in has_file:
        s = (m["source"] or "").strip().lower()
        src_c[source_labels.get(s, s or "Unknown")] += 1

    cert_c   = Counter(m["certification"] or "Not Rated" for m in has_file)
    studio_c = Counter(m["studio"] or "Unknown" for m in has_file)

    genre_c  = Counter()
    for m in has_file:
        for g in (m["genres"] or "").split(","):
            g = g.strip()
            if g:
                genre_c[g] += 1

    decade_c = Counter()
    for m in has_file:
        if m["year"]:
            decade_c[f"{int(m['year'])//10*10}s"] += 1

    rating_buckets = Counter()
    for m in has_file:
        v = m["imdb_rating"]
        if v and v > 0:
            rating_buckets[f"{int(v)}.0–{int(v)+1}.0"] += 1

    top_movies = sorted(
        [m for m in has_file if m["imdb_rating"]],
        key=lambda x: x["imdb_rating"], reverse=True
    )[:20]
    top_movies_out = [{"Title": m["title"], "Year": m["year"],
                       "IMDb Rating": m["imdb_rating"], "Quality": m["quality_name"],
                       "Genres": m["genres"]} for m in top_movies]

    return {
        "kpi": {
            "total": total, "downloaded": downloaded, "wishlist": wishlist,
            "upgrade_queue": upgrade_q,
            "total_tb": round(total_gb / 1024, 2), "avg_gb": avg_gb,
            "avg_gb_per_hr": avg_gb_per_hr,
            "generated": "",
        },
        "resolution":    top_n(res_c, 10),
        "hdr":           top_n(hdr_c, 10),
        "codec":         top_n(codec_c, 10),
        "source":        top_n(src_c, 10),
        "certification": top_n(cert_c, 15),
        "genres":        top_n(genre_c, 15),
        "studios":       top_n(studio_c, 15),
        "decades":       sorted([{"label": k, "value": v} for k, v in decade_c.items()], key=lambda x: str(x["label"])),
        "ratings":       sorted([{"label": k, "value": v} for k, v in rating_buckets.items()], key=lambda x: x["label"], reverse=True),
        "top_movies":    top_movies_out,
    }

def parse_tv():
    con = _db()
    run_id = _latest_run_id(con)
    if run_id is None:
        con.close()
        raise FileNotFoundError("No runs found — run an update first.")

    rows = [dict(r) for r in con.execute("""
        SELECT title, year, status, ended, network, certification, genres, tags,
               season_count, episodes_have, episodes_total, completion_pct,
               specials_have, specials_total, has_specials, size_gb, rating
        FROM tv_snapshots WHERE run_id = ?
    """, (run_id,)).fetchall()]
    con.close()

    total        = len(rows)
    complete     = sum(1 for s in rows if (s["completion_pct"] or 0) >= 100)
    partial      = sum(1 for s in rows if 0 < (s["completion_pct"] or 0) < 100)
    empty        = sum(1 for s in rows if not s["episodes_have"])
    airing       = sum(1 for s in rows if not s["ended"])
    total_eps    = sum(s["episodes_have"] or 0 for s in rows)
    total_specs  = sum(s["specials_have"] or 0 for s in rows)
    has_specials = sum(1 for s in rows if (s["specials_have"] or 0) > 0)
    total_gb     = sum(s["size_gb"] or 0 for s in rows)

    net_c   = Counter(s["network"] or "Unknown" for s in rows if s["network"])
    cert_c  = Counter(s["certification"] or "Not Rated" for s in rows)
    genre_c = Counter()
    for s in rows:
        for g in (s["genres"] or "").split(","):
            g = g.strip()
            if g:
                genre_c[g] += 1
    decade_c = Counter()
    for s in rows:
        if s["year"]:
            decade_c[f"{int(s['year'])//10*10}s"] += 1

    biggest = sorted(rows, key=lambda x: x["episodes_have"] or 0, reverse=True)[:15]
    biggest_out = [{"Title": s["title"], "Seasons": s["season_count"],
                    "Episodes (Have)": s["episodes_have"], "Episodes (Total)": s["episodes_total"],
                    "Specials (Have)": s["specials_have"], "Specials (Total)": s["specials_total"],
                    "Size (GB)": s["size_gb"]} for s in biggest]

    incomplete = sorted(
        [s for s in rows if 0 < (s["completion_pct"] or 0) < 100],
        key=lambda x: x["completion_pct"] or 0
    )
    incomplete_out = [{"Title": s["title"], "Have": s["episodes_have"],
                       "Total": s["episodes_total"], "Main Completion %": s["completion_pct"],
                       "Specials": s["specials_have"], "ended": bool(s["ended"])}
                      for s in incomplete]

    return {
        "kpi": {
            "total": total, "complete": complete, "partial": partial,
            "empty": empty, "airing": airing,
            "total_episodes": total_eps, "total_specials": total_specs,
            "has_specials": has_specials,
            "total_tb": round(total_gb / 1024, 2),
            "completion_pct": round(complete / total * 100, 1) if total else 0,
        },
        "networks":           top_n(net_c, 15),
        "genres":             top_n(genre_c, 15),
        "certification":      top_n(cert_c, 10),
        "decades":            sorted([{"label": k, "value": v} for k, v in decade_c.items()], key=lambda x: str(x["label"])),
        "completion_status":  [{"label": "Complete", "value": complete},
                               {"label": "Partial",  "value": partial},
                               {"label": "Wanted",   "value": empty}],
        "biggest":            biggest_out,
        "incomplete":         incomplete_out,
    }

def parse_talent():
    con = _db()
    run_id = _latest_run_id(con)
    if run_id is None:
        con.close()
        raise FileNotFoundError("No runs found — run an update first.")
    rows = [dict(r) for r in con.execute("""
        SELECT name, role, film_count, avg_rating, top_genre
        FROM top_talent_snapshots WHERE run_id = ?
        ORDER BY film_count DESC
    """, (run_id,)).fetchall()]
    con.close()

    dirs = [{"name": r["name"], "films": r["film_count"], "avg_rating": r["avg_rating"],
              "top_genre": r["top_genre"] or ""} for r in rows if r["role"] == "director"]
    acts = [{"name": r["name"], "films": r["film_count"], "avg_rating": r["avg_rating"],
              "top_genre": r["top_genre"] or ""} for r in rows if r["role"] == "actor"]
    return {"directors": dirs[:25], "actors": acts[:25]}

def parse_franchises():
    con = _db()
    run_id = _latest_run_id(con)
    if run_id is None:
        con.close()
        raise FileNotFoundError("No runs found — run an update first.")
    rows = [dict(r) for r in con.execute("""
        SELECT franchise_name, have, total, missing_count, pct, status
        FROM franchise_snapshots WHERE run_id = ?
        ORDER BY missing_count DESC, total DESC
    """, (run_id,)).fetchall()]
    con.close()
    return [{"franchise": r["franchise_name"], "have": r["have"] or 0,
             "total": r["total"] or 0, "missing": r["missing_count"] or 0,
             "pct": r["pct"] or 0, "status": r["status"] or ""} for r in rows]


def parse_constellation():
    con = _db()
    run_id = _latest_run_id(con)
    if run_id is None:
        con.close()
        raise FileNotFoundError("No runs found — run an update first.")
    rows = [dict(r) for r in con.execute("""
        SELECT title, year, imdb_rating, file_size_gb, quality_name, genres
        FROM movie_snapshots
        WHERE run_id = ? AND has_file = 1 AND imdb_rating IS NOT NULL
              AND year IS NOT NULL AND imdb_rating > 0
    """, (run_id,)).fetchall()]
    con.close()
    return [{"title": r["title"], "year": r["year"],
             "rating": round(r["imdb_rating"], 1),
             "size_gb": round(r["file_size_gb"] or 0, 2),
             "quality": r["quality_name"] or "",
             "genres": r["genres"] or "",
             "decade": f"{r['year'] // 10 * 10}s"} for r in rows]

# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/api/stats")
def api_stats():
    if not _has_data():
        return jsonify({"error": "No library data found — run an update first."}), 404
    try:
        data = {
            "movies":     parse_movies(),
            "tv":         parse_tv(),
            "talent":     parse_talent(),
            "franchises": parse_franchises(),
        }
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/constellation")
def api_constellation():
    if not _has_data():
        return jsonify({"error": "No library data found"}), 404
    try:
        return jsonify(parse_constellation())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def parse_bloat():
    con = _db()
    run_id = _latest_run_id(con)
    if run_id is None:
        con.close()
        raise FileNotFoundError("No runs found — run an update first.")
    rows = [dict(r) for r in con.execute("""
        SELECT title, year, quality_name, video_codec, release_group,
               file_size_gb, runtime, video_bitrate, radarr_id, title_slug
        FROM movie_snapshots WHERE run_id = ? AND has_file = 1
    """, (run_id,)).fetchall()]
    con.close()

    movies = []
    for r in rows:
        gb = r["file_size_gb"] or 0
        rt = r["runtime"]
        gb_hr = round(gb / (rt / 60), 2) if rt and rt > 0 else None
        movies.append({
            "title":      r["title"],
            "year":       r["year"],
            "quality":    r["quality_name"] or "",
            "codec":      r["video_codec"] or "",
            "group":      r["release_group"] or "",
            "size_gb":    round(gb, 2),
            "runtime":    rt,
            "gb_hr":      gb_hr,
            "bitrate":    r["video_bitrate"],
            "radarr_id":  r["radarr_id"],
            "title_slug": r["title_slug"] or "",
        })

    with_gh = [m for m in movies if m["gb_hr"] is not None and m["size_gb"] >= 1]
    worst     = sorted(with_gh, key=lambda x: x["gb_hr"], reverse=True)[:40]
    efficient = sorted(with_gh, key=lambda x: x["gb_hr"])[:40]
    x264      = sorted(
        [m for m in movies if re.search(r"x264|h264|avc", (m["codec"] or "").lower())],
        key=lambda x: x["size_gb"], reverse=True
    )

    total_gb        = sum(m["size_gb"] for m in movies)
    x264_gb         = sum(m["size_gb"] for m in x264)
    top10_gb        = sum(m["size_gb"] for m in sorted(movies, key=lambda x: x["size_gb"], reverse=True)[:10])
    avg_gb_hr       = round(sum(m["gb_hr"] for m in with_gh) / len(with_gh), 2) if with_gh else 0
    bloat           = [m for m in with_gh if m["gb_hr"] > 10]
    avg_bloat_gb_hr = round(sum(m["gb_hr"] for m in bloat) / len(bloat), 2) if bloat else 0
    recoverable_gb  = round(sum(m["size_gb"] for m in with_gh if m["gb_hr"] > 8), 1)

    return {
        "kpi": {
            "total_gb":    round(total_gb, 2),
            "x264_gb":     round(x264_gb, 2),
            "x264_count":  len(x264),
            "top10_gb":    round(top10_gb, 2),
            "avg_gb_hr":   avg_gb_hr,
            "bloat_count": len(bloat),
            "avg_bloat_gb_hr": avg_bloat_gb_hr,
            "recoverable_gb": recoverable_gb,
        },
        "worst":    worst,
        "efficient": efficient,
        "x264":     x264,
    }

@app.route("/api/bloat")
def api_bloat():
    if not _has_data():
        return jsonify({"error": "No library data found"}), 404
    try:
        return jsonify(parse_bloat())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Runner proxy routes ────────────────────────────────────────────────────────

@app.route("/api/run", methods=["POST"])
def api_run():
    try:
        r = req.post(f"{RUNNER_URL}/run", timeout=5)
        return jsonify(r.json()), r.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 503

@app.route("/api/run/stream")
def api_stream():
    def generate():
        try:
            with req.get(f"{RUNNER_URL}/stream", stream=True, timeout=3600) as r:
                for chunk in r.iter_content(chunk_size=None):
                    if chunk:
                        yield chunk
        except Exception as e:
            yield f"data: ✗ Stream error: {e}\n\n"
    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":               "no-cache",
            "X-Accel-Buffering":           "no",
            "Access-Control-Allow-Origin": "*",
        },
    )

@app.route("/api/run/status")
def api_run_status():
    try:
        r = req.get(f"{RUNNER_URL}/status", timeout=5)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 503

@app.route("/api/history")
def api_history():
    try:
        r = req.get(f"{RUNNER_URL}/history", timeout=5)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 503

@app.route("/api/history/trends")
def api_trends():
    try:
        r = req.get(f"{RUNNER_URL}/history/trends", timeout=5)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 503
@app.route("/")
def index():
    return send_file("/app/dashboard.html", mimetype="text/html")

@app.route("/api/history/<int:run_id>", methods=["DELETE"])
def api_delete_run(run_id):
    try:
        r = req.delete(f"{RUNNER_URL}/history/{run_id}", timeout=5)
        return jsonify(r.json()), r.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 503

@app.route("/api/history/<int:run_id>/snapshot")
def api_snapshot(run_id):
    try:
        r = req.get(f"{RUNNER_URL}/history/{run_id}/snapshot", timeout=10)
        return jsonify(r.json()), r.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 503
def parse_talent_deep():
    import sqlite3 as _sq
    from collections import defaultdict, Counter

    if not DB_PATH.exists():
        return {"error": "Database not found"}

    # ── Load movies from SQLite ────────────────────────────────────────────────
    con_m = _sq.connect(DB_PATH)
    con_m.row_factory = _sq.Row
    run_id = _latest_run_id(con_m)
    if run_id is None:
        con_m.close()
        return {"error": "No runs found — run an update first."}

    imdb_to_movie = {}
    for r in con_m.execute("""
        SELECT imdb_id, title, year, imdb_rating, genres
        FROM movie_snapshots WHERE run_id = ? AND has_file = 1
    """, (run_id,)).fetchall():
        if r["imdb_id"]:
            imdb_to_movie[r["imdb_id"]] = {
                "title":  r["title"],
                "year":   r["year"],
                "rating": r["imdb_rating"],
                "genres": r["genres"] or "",
            }
    con_m.close()

    # ── Load talent cache ──────────────────────────────────────────────────────
    if not DB_PATH.exists():
        return {"error": "Database not found"}

    con = _sq.connect(DB_PATH)
    con.row_factory = _sq.Row
    rows = con.execute(
        "SELECT imdb_id, nconst, name, role, ordering FROM talent_cache"
    ).fetchall()
    con.close()

    # Filter to films we actually have
    talent = defaultdict(list)   # imdb_id -> [{name, nconst, role, ordering}]
    for r in rows:
        if r["imdb_id"] in imdb_to_movie:
            talent[r["imdb_id"]].append({
                "name": r["name"], "nconst": r["nconst"],
                "role": r["role"], "ordering": r["ordering"] or 99,
            })

    # ── Per-person aggregates ──────────────────────────────────────────────────
    # nconst -> list of {imdb_id, ordering, year, title, rating, genres}
    person_appearances = defaultdict(list)
    person_names = {}
    person_roles = {}

    for imdb_id, people in talent.items():
        movie = imdb_to_movie.get(imdb_id)
        if not movie:
            continue
        for p in people:
            nc = p["nconst"]
            person_names[nc] = p["name"]
            person_roles[nc] = p["role"]
            person_appearances[nc].append({
                "imdb_id":  imdb_id,
                "title":    movie["title"],
                "year":     movie["year"],
                "rating":   movie["rating"],
                "genres":   movie["genres"],
                "ordering": p["ordering"],
                "role":     p["role"],
            })

    # ── 1. The Everywheremen ──────────────────────────────────────────────────
    # "Hey it's that guy!" — character actors woven deep into the library.
    # Scored by: appearances × billing_depth × fame_suppression × breadth_bonus
    # Fame ceiling via actor_career true_breakout votes — if you were never
    # top-2 billed in a 100k+ vote film, you're character actor territory.

    # Load true_breakout votes from actor_career for fame suppression
    breakout_votes = {}
    try:
        con_ac = _sq.connect(DB_PATH)
        for ac_row in con_ac.execute("SELECT nconst, true_breakout FROM actor_career").fetchall():
            tb = json.loads(ac_row[1]) if ac_row[1] else None
            breakout_votes[ac_row[0]] = tb["votes"] if tb else 0
        con_ac.close()
    except Exception:
        pass

    def fame_factor(votes):
        """1.0 for unknown character actors, down to 0.05 for mega-stars."""
        if not votes or votes < 50_000:
            return 1.0
        if votes < 150_000:
            return 0.75
        if votes < 300_000:
            return 0.45
        if votes < 500_000:
            return 0.2
        return 0.05

    EVERYWHEREMEN_EXCLUDE_GENRES = {"Animation", "Anime"}

    everywheremen = []
    for nc, apps in person_appearances.items():
        if person_roles.get(nc) not in ("actor", "actress"):
            continue
        if len(apps) < 4:
            continue

        # Deduplicate by imdb_id (can appear multiple times per film)
        seen_ids = set()
        deduped = []
        for a in apps:
            if a["imdb_id"] not in seen_ids:
                seen_ids.add(a["imdb_id"])
                # Exclude Animation-genre films — voice actors skew all metrics
                film_genres = {g.strip() for g in a["genres"].split(",")}
                if film_genres & EVERYWHEREMEN_EXCLUDE_GENRES:
                    continue
                deduped.append(a)

        total = len(deduped)
        if total < 3:  # not enough non-animation appearances
            continue
        orderings  = [a["ordering"] for a in deduped]
        avg_order  = sum(orderings) / len(orderings)   # higher = deeper in cast
        lead_count = sum(1 for o in orderings if o <= 2)
        deep_count = sum(1 for o in orderings if o >= 4)  # billing 4+ = supporting

        # Need at least 3 deep supporting appearances
        if deep_count < 3:
            continue

        # Billing depth weight: avg ordering mapped to 0-1 (billing 10+ = 1.0)
        billing_weight = min(avg_order / 10.0, 1.0)

        # Lead suppression: heavy penalty if lots of lead appearances
        lead_ratio = lead_count / total
        lead_penalty = max(0.1, 1.0 - (lead_ratio * 2.5))

        # Fame suppression
        peak_votes = breakout_votes.get(nc, 0)
        ff = fame_factor(peak_votes)

        # Breadth bonus
        genres = set()
        decades = set()
        for a in deduped:
            for g in a["genres"].split(","):
                g = g.strip()
                if g:
                    genres.add(g)
            if a["year"]:
                decades.add((a["year"] // 10) * 10)
        breadth = (len(genres) * len(decades)) ** 0.4

        score = total * billing_weight * lead_penalty * ff * breadth

        ratings = [a["rating"] for a in deduped if a["rating"]]
        avg_r   = round(sum(ratings) / len(ratings), 2) if ratings else None

        everywheremen.append({
            "name":         person_names[nc],
            "film_count":   total,
            "deep_count":   deep_count,
            "lead_count":   lead_count,
            "avg_billing":  round(avg_order, 1),
            "genre_count":  len(genres),
            "decade_count": len(decades),
            "avg_rating":   avg_r,
            "peak_votes":   peak_votes,
            "score":        round(score, 2),
            "films":        sorted(deduped, key=lambda x: x["year"] or 0, reverse=True)[:8],
        })

    everywheremen.sort(key=lambda x: x["score"], reverse=True)
    everywheremen = everywheremen[:30]
    for e in everywheremen:
        e["films"] = [{"title": f["title"], "year": f["year"], "ordering": f["ordering"]} for f in e["films"]]

    # ── 2. The Unsung ─────────────────────────────────────────────────────────
    # Actors consistently appearing with billing 5+ — rarely leads, always present
    unsung = []
    for nc, apps in person_appearances.items():
        if person_roles.get(nc) not in ("actor", "actress"):
            continue
        # Exclude Animation-genre films from Unsung
        non_anim = [a for a in apps if not ({"Animation","Anime"} & {g.strip() for g in a["genres"].split(",")})]
        if len(non_anim) < 2:
            continue  # skip actors whose library presence is mostly animation
        supporting = [a for a in non_anim if a["ordering"] >= 4]
        lead_count  = sum(1 for a in non_anim if a["ordering"] <= 2)
        if len(supporting) < 4 or lead_count > 2:
            continue
        ratings = [a["rating"] for a in supporting if a["rating"]]
        avg_r = round(sum(ratings) / len(ratings), 2) if ratings else None
        genres = Counter()
        for a in supporting:
            for g in a["genres"].split(","):
                g = g.strip()
                if g:
                    genres[g] += 1
        unsung.append({
            "name":         person_names[nc],
            "support_count":len(supporting),
            "lead_count":   lead_count,
            "avg_rating":   avg_r,
            "top_genre":    genres.most_common(1)[0][0] if genres else "",
            "films":        sorted(supporting, key=lambda x: x["rating"] or 0, reverse=True)[:6],
        })
    unsung.sort(key=lambda x: x["support_count"], reverse=True)
    unsung = unsung[:30]
    for u in unsung:
        u["films"] = [{"title": f["title"], "year": f["year"], "ordering": f["ordering"], "rating": f["rating"]} for f in u["films"]]

    # ── 3. Before They Were Famous ────────────────────────────────────────────
    # Uses full IMDb filmography from actor_career to find pre-fame appearances
    # NOT in the library, with Jellyseer links to request them.
    ANIM_GENRES = {"Animation", "Anime"}

    # Load actor_career BTWF data (pre-computed from IMDb TSV scan)
    career_btwf = {}
    try:
        con_btwf = _sq.connect(DB_PATH)
        con_btwf.row_factory = _sq.Row
        for ac_row in con_btwf.execute(
            "SELECT nconst, true_breakout, btwf_pre_fame FROM actor_career"
        ).fetchall():
            tb   = json.loads(ac_row["true_breakout"])  if ac_row["true_breakout"]  else None
            pre  = json.loads(ac_row["btwf_pre_fame"])   if ac_row["btwf_pre_fame"]   else []
            if tb:
                career_btwf[ac_row["nconst"]] = {"true_breakout": tb, "btwf_pre_fame": pre}
        con_btwf.close()
    except Exception:
        pass

    btwf = []
    for nc, apps in person_appearances.items():
        if person_roles.get(nc) not in ("actor", "actress"):
            continue

        non_anim = [a for a in apps if not (ANIM_GENRES & {g.strip() for g in a["genres"].split(",")})]

        career = career_btwf.get(nc)
        if career:
            # ── IMDb-backed path ──────────────────────────────────────────────
            tb            = career["true_breakout"]
            breakout_year = tb["year"]
            breakout_film = tb["title"]
            breakout_votes = tb.get("votes", 0)
            pre_fame      = career["btwf_pre_fame"]
            early_in_lib  = [f for f in pre_fame if     f.get("in_library")]
            early_missing = [f for f in pre_fame if not f.get("in_library")]
            if not early_in_lib and not early_missing:
                continue
            btwf.append({
                "name":           person_names[nc],
                "breakout_film":  breakout_film,
                "breakout_year":  breakout_year,
                "breakout_votes": breakout_votes,
                "missing_count":  len(early_missing),
                "early_count":    len(pre_fame),
                "early_in_lib":   sorted(early_in_lib,  key=lambda x: x["year"])[:5],
                "early_missing":  sorted(early_missing, key=lambda x: x["year"])[:10],
                "total_films":    len(non_anim),
                "imdb_source":    True,
            })
        else:
            # ── Library-only fallback (no career cache entry) ─────────────────
            if len(non_anim) < 3:
                continue
            leads = sorted(
                [a for a in non_anim if a["ordering"] <= 2 and a["year"]],
                key=lambda x: x["year"]
            )
            if not leads:
                continue
            breakout_year = leads[0]["year"]
            breakout_film = leads[0]["title"]
            early = [
                a for a in non_anim
                if a["ordering"] >= 5 and a["year"] and a["year"] < breakout_year
            ]
            if not early:
                continue
            btwf.append({
                "name":           person_names[nc],
                "breakout_film":  breakout_film,
                "breakout_year":  breakout_year,
                "breakout_votes": 0,
                "missing_count":  0,
                "early_count":    len(early),
                "early_in_lib":   [{"title": f["title"], "year": f["year"], "ordering": f["ordering"]}
                                   for f in sorted(early, key=lambda x: x["year"])[:5]],
                "early_missing":  [],
                "total_films":    len(non_anim),
                "imdb_source":    False,
            })

    # Prioritise actors with the most missing pre-fame discoveries
    btwf.sort(key=lambda x: (x["missing_count"], x["early_count"]), reverse=True)
    btwf = btwf[:30]

    # ── 4. Director Loyalty Index ──────────────────────────────────────────────
    # Directors ranked by film count, avg rating, genre range
    loyalty = []
    for nc, apps in person_appearances.items():
        if person_roles.get(nc) != "director":
            continue
        # Exclude animation films from director loyalty
        apps = [a for a in apps if not ({"Animation","Anime"} & {g.strip() for g in a["genres"].split(",")})]
        if len(apps) < 2:
            continue
        ratings = [a["rating"] for a in apps if a["rating"]]
        avg_r = round(sum(ratings) / len(ratings), 2) if ratings else None
        genres = Counter()
        decades = set()
        for a in apps:
            for g in a["genres"].split(","):
                g = g.strip()
                if g:
                    genres[g] += 1
            if a["year"]:
                decades.add((a["year"] // 10) * 10)
        # Loyalty score: film count weighted by avg rating and consistency
        loyalty_score = round(
            len(apps) * (avg_r / 10 if avg_r else 0.5) * (1 + len(decades) * 0.1), 2
        )
        loyalty.append({
            "name":          person_names[nc],
            "film_count":    len(apps),
            "avg_rating":    avg_r,
            "top_genre":     genres.most_common(1)[0][0] if genres else "",
            "genre_count":   len(genres),
            "decade_count":  len(decades),
            "loyalty_score": loyalty_score,
            "films":         sorted(apps, key=lambda x: x["year"] or 0, reverse=True)[:6],
        })
    loyalty.sort(key=lambda x: x["loyalty_score"], reverse=True)
    loyalty = loyalty[:30]
    for d in loyalty:
        d["films"] = [{"title": f["title"], "year": f["year"], "rating": f["rating"]} for f in d["films"]]

    return {
        "everywheremen": everywheremen,
        "unsung":        unsung,
        "btwf":          btwf,
        "loyalty":       loyalty,
    }


@app.route("/api/talent/deep")
def api_talent_deep():
    try:
        data = parse_talent_deep()
        if "error" in data:
            return jsonify(data), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def parse_fingerprint():
    MEMBERS = FINGERPRINT_MEMBERS + ["family"]
    COLORS  = _member_colors(FINGERPRINT_MEMBERS)
    COLORS["family"] = "#ff6b6b"
    md = _fingerprint_member_data()
    result = {}
    for member in MEMBERS:
        m_data = md[member]
        if not m_data["rows"]:
            result[member] = None
            continue
        genre_counter  = m_data["genres"]
        decade_counter = Counter()
        ratings, movie_count, tv_count = [], 0, 0
        for row in m_data["rows"]:
            if row.get("source") == "movie":
                movie_count += 1
            else:
                tv_count += 1
            try:
                y = int(float(str(row.get("year") or 0)))
                if y > 0:
                    decade_counter[f"{(y//10)*10}s"] += 1
            except (ValueError, TypeError):
                pass
            try:
                r = float(str(row.get("rating") or 0))
                if r > 0:
                    ratings.append(r)
            except (ValueError, TypeError):
                pass
        result[member] = {
            "color":       COLORS.get(member, "#8888cc"),
            "film_count":  len(m_data["rows"]),
            "movie_count": movie_count,
            "tv_count":    tv_count,
            "avg_rating":  round(sum(ratings)/len(ratings), 2) if ratings else None,
            "top_genres":  [{"genre": g, "count": c} for g, c in genre_counter.most_common(10)],
            "decades":     sorted([{"decade": d, "count": c} for d, c in decade_counter.items()], key=lambda x: x["decade"]),
        }
    return result


@app.route("/api/fingerprint")
def api_fingerprint():
    if not FINGERPRINT_ENABLED:
        return jsonify({"enabled": False})
    try:
        return jsonify(parse_fingerprint())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def parse_deep_wounds():
    import sqlite3 as _sq
    from collections import defaultdict

    if not DB_PATH.exists():
        return {"error": "Database not found"}

    con = _sq.connect(DB_PATH)
    con.row_factory = _sq.Row

    try:
        rows = con.execute(
            "SELECT nconst, name, horror_credits, true_breakout FROM actor_career"
        ).fetchall()
    except Exception:
        con.close()
        return {"error": "Career data not yet computed — run an update first."}
    con.close()

    # Also load talent_cache to know which actors are in the library
    con2 = _sq.connect(DB_PATH)
    lib_nconsts = set(
        r[0] for r in con2.execute(
            "SELECT DISTINCT nconst FROM talent_cache"
        ).fetchall()
    )
    con2.close()

    actors = []
    for row in rows:
        if row["nconst"] not in lib_nconsts:
            continue
        hc = json.loads(row["horror_credits"]) if row["horror_credits"] else []
        tb = json.loads(row["true_breakout"])  if row["true_breakout"]  else None
        if not hc:
            continue

        have    = [f for f in hc if f["in_library"]]
        missing = [f for f in hc if not f["in_library"] and f.get("votes", 0) >= 20_000]
        missing.sort(key=lambda x: x.get("votes", 0), reverse=True)

        actors.append({
            "name":          row["name"],
            "nconst":        row["nconst"],
            "true_breakout": tb,
            "horror_have":   have,
            "horror_missing": missing[:10],
        })

    # Sort by total horror credits descending
    actors.sort(key=lambda x: len(x["horror_have"]) + len(x["horror_missing"]), reverse=True)
    return {"actors": actors[:50]}


@app.route("/api/deep-wounds")
def api_deep_wounds():
    try:
        data = parse_deep_wounds()
        if "error" in data:
            return jsonify(data), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def parse_dna():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DB_PATH}")

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    # Latest run that has DNA scores
    cur = con.execute("""
        SELECT run_id, COUNT(*) as count, AVG(final_score) as avg_score
        FROM dna_scores
        GROUP BY run_id
        ORDER BY run_id DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        con.close()
        return {"error": "No DNA scores computed yet — run an update first."}

    run_id    = row["run_id"]
    avg_score = round(row["avg_score"], 1)

    def grade(s):
        if s >= 90: return "A"
        if s >= 80: return "B"
        if s >= 70: return "C"
        if s >= 60: return "D"
        return "F"

    # Grade distribution
    cur = con.execute("""
        SELECT grade, COUNT(*) as count
        FROM dna_scores WHERE run_id = ?
        GROUP BY grade ORDER BY grade
    """, (run_id,))
    grade_dist = {r["grade"]: r["count"] for r in cur.fetchall()}

    # Best films (top 20)
    cur = con.execute("""
        SELECT title, imdb_id, final_score, grade, teacher_note,
               d1_score, d2_score, d3_score, d4_score, d5_score, d6_score, d7_score
        FROM dna_scores WHERE run_id = ?
        ORDER BY final_score DESC LIMIT 20
    """, (run_id,))
    best = [dict(r) for r in cur.fetchall()]

    # Worst films (bottom 20)
    cur = con.execute("""
        SELECT title, imdb_id, final_score, grade, teacher_note,
               d1_score, d2_score, d3_score, d4_score, d5_score, d6_score, d7_score
        FROM dna_scores WHERE run_id = ?
        ORDER BY final_score ASC LIMIT 20
    """, (run_id,))
    worst = [dict(r) for r in cur.fetchall()]

    # Enrich worst with title_slug from Radarr API (match by title)
    try:
        import requests as _req
        _r = _req.get(
            f'{RADARR_URL.rstrip("/")}/api/v3/movie',
            params={'apikey': RADARR_API_KEY},
            timeout=5
        )
        if _r.ok:
            _slug_map = {
                m.get('title', '').strip().lower(): m.get('titleSlug', '')
                for m in _r.json()
            }
            for w in worst:
                _key = (w.get('title') or '').strip().lower()
                w['title_slug'] = _slug_map.get(_key, '')
    except Exception:
        pass  # non-fatal — links just won't appear

    # F-grade count
    cur = con.execute("""
        SELECT COUNT(*) as count FROM dna_scores WHERE run_id = ? AND grade = 'F'
    """, (run_id,))
    f_count = cur.fetchone()["count"]

    # Historical trend — avg DNA score per run
    cur = con.execute("""
        SELECT d.run_id, r.run_date, AVG(d.final_score) as avg_score,
               COUNT(*) as film_count
        FROM dna_scores d
        JOIN runs r ON r.id = d.run_id
        GROUP BY d.run_id
        ORDER BY d.run_id
    """)
    trend = [
        {
            "run_id":     r["run_id"],
            "date":       r["run_date"],
            "avg_score":  round(r["avg_score"], 1),
            "film_count": r["film_count"],
        }
        for r in cur.fetchall()
    ]

    # Library-wide dimension averages
    cur = con.execute("""
        SELECT AVG(d1_score) as d1, AVG(d2_score) as d2, AVG(d3_score) as d3,
               AVG(d4_score) as d4, AVG(d5_score) as d5, AVG(d6_score) as d6,
               AVG(d7_score) as d7
        FROM dna_scores WHERE run_id = ?
    """, (run_id,))
    dim_row = cur.fetchone()
    dimensions = [
        {"key": "d1", "label": "Score Authenticity",   "weight": 20, "score": round(dim_row["d1"] or 0, 1)},
        {"key": "d2", "label": "Intentionality",        "weight": 20, "score": round(dim_row["d2"] or 0, 1)},
        {"key": "d3", "label": "Talent Crossover",      "weight": 10, "score": round(dim_row["d3"] or 0, 1)},
        {"key": "d4", "label": "Franchise Context",     "weight": 15, "score": round(dim_row["d4"] or 0, 1)},
        {"key": "d5", "label": "Vote Density by Era",   "weight": 10, "score": round(dim_row["d5"] or 0, 1)},
        {"key": "d6", "label": "Genre Coherence",       "weight": 10, "score": round(dim_row["d6"] or 0, 1)},
        {"key": "d7", "label": "Audience vs Critics",   "weight": 15, "score": round(dim_row["d7"] or 0, 1)},
    ]

    # Expulsion list — D/F films that carry at least one *-hate tag
    cur = con.execute("""
        SELECT title, imdb_id, final_score, grade, teacher_note
        FROM dna_scores WHERE run_id = ? AND grade IN ('D', 'F')
        ORDER BY final_score ASC
    """, (run_id,))
    expulsion_candidates = [dict(r) for r in cur.fetchall()]
    con.close()

    # Build imdb_id → tags map from movie_snapshots (latest run)
    tag_map = {}
    try:
        con2 = sqlite3.connect(DB_PATH)
        for r in con2.execute("""
            SELECT imdb_id, tags FROM movie_snapshots
            WHERE run_id = ? AND imdb_id IS NOT NULL AND imdb_id != ''
        """, (run_id,)).fetchall():
            tag_map[r[0]] = (r[1] or "").lower()
        con2.close()
    except Exception:
        pass

    expulsion = []
    for film in expulsion_candidates:
        iid = (film.get("imdb_id") or "").strip()
        tags = [t.strip() for t in tag_map.get(iid, "").split(",")
                if t.strip() and t.strip() != "nan"]
        haters = [m for m in FINGERPRINT_MEMBERS if f"{m}-hate" in tags]
        if haters:
            expulsion.append({
                "title":        film["title"],
                "imdb_id":      iid,
                "final_score":  film["final_score"],
                "grade":        film["grade"],
                "teacher_note": film.get("teacher_note", ""),
                "haters":       haters,
                "tier":         "expulsion" if film["grade"] == "F" else "detention",
            })
    # F first, then most haters, then lowest score
    expulsion.sort(key=lambda x: (0 if x["tier"] == "expulsion" else 1,
                                  -len(x["haters"]), x["final_score"]))

    return {
        "library_score":     avg_score,
        "library_grade":     grade(avg_score),
        "grade_distribution": grade_dist,
        "dimensions":        dimensions,
        "best":              best,
        "worst":             worst,
        "f_count":           f_count,
        "trend":             trend,
        "expulsion":         expulsion,
    }




@app.route("/api/radarr/replace/<int:radarr_id>", methods=["POST"])
def radarr_replace(radarr_id):
    """Delete existing file and trigger automatic search for replacement."""
    import requests as _req
    RADARR = RADARR_URL.rstrip('/')
    KEY    = RADARR_API_KEY
    try:
        # Step 1: get movie to confirm fileId
        movie_r = _req.get(f'{RADARR}/api/v3/movie/{radarr_id}',
                           params={'apikey': KEY}, timeout=60)
        if not movie_r.ok:
            return jsonify({"error": f"Radarr returned {movie_r.status_code}"}), 502
        movie = movie_r.json()
        title = movie.get('title', 'Unknown')
        file_id = movie.get('movieFileId')

        # Step 2: delete the file
        if file_id:
            del_r = _req.delete(f'{RADARR}/api/v3/moviefile/{file_id}',
                                params={'apikey': KEY}, timeout=60)
            if not del_r.ok:
                return jsonify({"error": f"File delete failed: {del_r.status_code}"}), 502

        # Step 3: trigger automatic search
        search_r = _req.post(f'{RADARR}/api/v3/command',
                             params={'apikey': KEY},
                             json={'name': 'MoviesSearch', 'movieIds': [radarr_id]},
                             timeout=60)
        if not search_r.ok:
            return jsonify({"error": f"Search trigger failed: {search_r.status_code}"}), 502

        return jsonify({"ok": True, "title": title, "file_id": file_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/dna")
def api_dna():
    try:
        data = parse_dna()
        if "error" in data:
            return jsonify(data), 404
        return jsonify(data)
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500



import math as _math

def _cosine(v1, v2):
    keys = set(v1) | set(v2)
    dot  = sum(v1.get(k, 0) * v2.get(k, 0) for k in keys)
    m1   = _math.sqrt(sum(x**2 for x in v1.values()))
    m2   = _math.sqrt(sum(x**2 for x in v2.values()))
    if m1 == 0 or m2 == 0:
        return 0.0
    return round(dot / (m1 * m2), 4)

def _fingerprint_member_data():
    MEMBERS     = FINGERPRINT_MEMBERS + ["family"]
    INDIVIDUALS = FINGERPRINT_MEMBERS
    empty = {m: {"genres": Counter(), "titles": set(), "imdb_ids": set(), "rows": []} for m in MEMBERS}

    if not DB_PATH.exists():
        return empty

    try:
        con = _db()
        run_id = _latest_run_id(con)
        if run_id is None:
            con.close()
            return empty

        all_rows = []
        for r in con.execute("""
            SELECT title, imdb_id, year, genres, tags, imdb_rating AS rating, 'movie' AS source
            FROM movie_snapshots WHERE run_id = ? AND has_file = 1
        """, (run_id,)).fetchall():
            all_rows.append(dict(r))
        for r in con.execute("""
            SELECT title, imdb_id, year, genres, tags, rating, 'tv' AS source
            FROM tv_snapshots WHERE run_id = ? AND episodes_have > 0
        """, (run_id,)).fetchall():
            all_rows.append(dict(r))
        con.close()
    except Exception:
        return empty

    md = {m: {"genres": Counter(), "titles": set(), "imdb_ids": set(), "rows": []} for m in MEMBERS}
    for row in all_rows:
        tags = [t.strip() for t in (row.get("tags") or "").lower().split(",")
                if t.strip() and t.strip() != "nan"]
        if all(ind in tags for ind in INDIVIDUALS) and "family" not in tags:
            tags.append("family")
        for member in MEMBERS:
            if member not in tags:
                continue
            md[member]["titles"].add(row["title"])
            iid = (row.get("imdb_id") or "").strip()
            if iid:
                md[member]["imdb_ids"].add(iid)
            md[member]["rows"].append(row)
            for g in str(row.get("genres") or "").split(","):
                g = g.strip()
                if g and g != "nan":
                    md[member]["genres"][g] += 1
    return md

_imdb_rec_cache: dict = {}

def _load_imdb_for_recs():
    """Load IMDb basics+ratings for recommendation engine using csv reader (no pandas)."""
    import csv as _csv
    global _imdb_rec_cache
    if "basics" in _imdb_rec_cache:
        return _imdb_rec_cache["basics"], True

    b_path = Path("/data/imdb/title_basics.tsv")
    r_path = Path("/data/imdb/title_ratings.tsv")
    if not b_path.exists() or not r_path.exists():
        return None, False

    try:
        ratings = {}
        with open(r_path, encoding="utf-8") as f:
            for row in _csv.DictReader(f, delimiter="\t"):
                try:
                    votes = int(row["numVotes"])
                    if votes >= 1000:
                        ratings[row["tconst"]] = {
                            "averageRating": float(row["averageRating"]),
                            "numVotes": votes,
                        }
                except (ValueError, KeyError):
                    pass

        VALID_TYPES = {"movie", "tvSeries", "tvMiniSeries"}
        basics = []
        with open(b_path, encoding="utf-8") as f:
            for row in _csv.DictReader(f, delimiter="\t"):
                if row.get("titleType") not in VALID_TYPES:
                    continue
                sy = row.get("startYear", r"\N")
                if sy in (r"\N", "", None):
                    continue
                tconst = row["tconst"]
                r_data = ratings.get(tconst)
                if not r_data or r_data["numVotes"] < 5000:
                    continue
                try:
                    year = int(sy)
                except (ValueError, TypeError):
                    continue
                genres_raw = row.get("genres") or ""
                if genres_raw == r"\N":
                    genres_raw = ""
                basics.append({
                    "tconst":        tconst,
                    "primaryTitle":  row.get("primaryTitle", ""),
                    "startYear":     year,
                    "genres":        genres_raw.replace(",", ", "),
                    "averageRating": r_data["averageRating"],
                    "numVotes":      r_data["numVotes"],
                    "score":         r_data["averageRating"] * (r_data["numVotes"] / (r_data["numVotes"] + 25000)),
                })
        _imdb_rec_cache["basics"] = basics
        return basics, True
    except Exception:
        return None, False

def parse_fingerprint_threads():
    MEMBERS = FINGERPRINT_MEMBERS
    md = _fingerprint_member_data()

    # Family-wide shared: titles that appear in md["family"] (auto-tagged when all 5 present)
    fam = md.get("family", {})
    fam_titles = sorted(fam.get("titles", set()))
    fam_genres = fam.get("genres", Counter())
    family_shared = {
        "titles":      fam_titles,
        "count":       len(fam_titles),
        "top_genres":  [{"genre": g, "count": c} for g, c in fam_genres.most_common(8)],
    }

    pairs = []
    for i, m1 in enumerate(MEMBERS):
        for m2 in MEMBERS[i + 1:]:
            d1, d2 = md[m1], md[m2]
            sim = _cosine(d1["genres"], d2["genres"])
            shared_titles = sorted(d1["titles"] & d2["titles"])
            shared_genres = Counter()
            for g in d1["genres"]:
                if g in d2["genres"]:
                    shared_genres[g] = min(d1["genres"][g], d2["genres"][g])
            pairs.append({
                "pair":              [m1, m2],
                "similarity":        sim,
                "shared_count":      len(shared_titles),
                "shared_titles":     shared_titles,  # all titles, no cap
                "top_shared_genres": [{"genre": g, "count": c} for g, c in shared_genres.most_common(8)],
                "only_left":         len(d1["titles"] - d2["titles"]),
                "only_right":        len(d2["titles"] - d1["titles"]),
            })
    pairs.sort(key=lambda x: x["similarity"], reverse=True)
    return {"family_shared": family_shared, "pairs": pairs}

@app.route("/api/fingerprint/threads")
def api_fingerprint_threads():
    if not FINGERPRINT_ENABLED:
        return jsonify({"enabled": False})
    try:
        return jsonify(parse_fingerprint_threads())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def parse_fingerprint_recs():
    MEMBERS = FINGERPRINT_MEMBERS
    COLORS  = _member_colors(MEMBERS)
    md = _fingerprint_member_data()

    all_library_ids: set = set()
    for m in MEMBERS:
        all_library_ids |= md[m]["imdb_ids"]

    imdb_basics, imdb_available = _load_imdb_for_recs()

    result = {}
    for member in MEMBERS:
        d = md[member]
        best_twin, best_sim = None, -1.0
        for other in MEMBERS:
            if other == member:
                continue
            sim = _cosine(d["genres"], md[other]["genres"])
            if sim > best_sim:
                best_sim, best_twin = sim, other

        twin_titles  = md[best_twin]["titles"] if best_twin else set()
        in_lib_gaps  = twin_titles - d["titles"]
        twin_row_map = {r["title"]: r for r in (md[best_twin]["rows"] if best_twin else [])}

        in_lib_recs = []
        for title in in_lib_gaps:
            row = twin_row_map.get(title, {})
            try:
                rating = float(row.get("rating") or 0) or None
            except (ValueError, TypeError):
                rating = None
            in_lib_recs.append({
                "title": title, "year": row.get("year"), "genres": row.get("genres", ""),
                "rating": rating, "imdb_id": row.get("imdb_id", ""),
                "in_library": True, "source": row.get("source", "movie"),
            })
        in_lib_recs.sort(key=lambda x: x["rating"] or 0, reverse=True)
        in_lib_recs = in_lib_recs[:15]

        imdb_recs = []
        if imdb_available and d["genres"] and imdb_basics:
            top_genres = {g for g, _ in d["genres"].most_common(6)}
            candidates = sorted(
                [b for b in imdb_basics
                 if b["tconst"] not in all_library_ids
                 and any(g.strip() in top_genres for g in b["genres"].split(","))],
                key=lambda x: x["score"], reverse=True
            )[:20]
            for b in candidates:
                imdb_recs.append({
                    "title": b["primaryTitle"], "year": str(b["startYear"]),
                    "genres": b["genres"], "rating": round(b["averageRating"], 1),
                    "votes": b["numVotes"], "imdb_id": b["tconst"], "in_library": False,
                })

        result[member] = {
            "color": COLORS.get(member, "#8888cc"),
            "taste_twin": best_twin,
            "twin_similarity": round(best_sim, 3) if best_sim >= 0 else None,
            "in_library_recs": in_lib_recs,
            "imdb_recs": imdb_recs,
            "imdb_available": imdb_available,
        }
    return result

@app.route("/api/fingerprint/recommendations")
def api_fingerprint_recs():
    if not FINGERPRINT_ENABLED:
        return jsonify({"enabled": False})
    try:
        return jsonify(parse_fingerprint_recs())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def parse_fingerprint_hated():
    MEMBERS = FINGERPRINT_MEMBERS + ["family"]
    COLORS  = _member_colors(FINGERPRINT_MEMBERS)
    COLORS["family"] = "#ff6b6b"

    # Query ALL library films directly — do not filter by member base tag.
    # A film tagged only with [member]-hate (no base tag) must still appear.
    deduped = []
    if DB_PATH.exists():
        try:
            con = _db()
            run_id = _latest_run_id(con)
            if run_id is not None:
                for r in con.execute("""
                    SELECT title, imdb_id, year, genres, tags, imdb_rating AS rating, 'movie' AS source
                    FROM movie_snapshots WHERE run_id = ? AND has_file = 1
                """, (run_id,)).fetchall():
                    deduped.append(dict(r))
                for r in con.execute("""
                    SELECT title, imdb_id, year, genres, tags, rating, 'tv' AS source
                    FROM tv_snapshots WHERE run_id = ? AND episodes_have > 0
                """, (run_id,)).fetchall():
                    deduped.append(dict(r))
            con.close()
        except Exception:
            pass

    hate = {m: [] for m in MEMBERS}
    for row in deduped:
        tags = [t.strip() for t in (row.get("tags") or "").lower().split(",")
                if t.strip() and t.strip() != "nan"]
        for member in MEMBERS:
            if f"{member}-hate" in tags:
                try:
                    rating = float(row.get("rating") or 0) or None
                except (ValueError, TypeError):
                    rating = None
                hate[member].append({
                    "title":   row.get("title", ""),
                    "year":    row.get("year"),
                    "genres":  row.get("genres", ""),
                    "rating":  rating,
                    "source":  row.get("source", "movie"),
                    "imdb_id": row.get("imdb_id", ""),
                })

    hate_counts = defaultdict(list)
    for member in MEMBERS:
        for item in hate[member]:
            hate_counts[item["title"]].append(member)
    consensus = [{"title": t, "haters": h, "count": len(h)}
                 for t, h in hate_counts.items() if len(h) >= 2]
    consensus.sort(key=lambda x: x["count"], reverse=True)

    result = {}
    for member in MEMBERS:
        result[member] = {
            "color": COLORS.get(member, "#8888cc"),
            "hated": sorted(hate[member], key=lambda x: x.get("rating") or 0, reverse=True),
            "count": len(hate[member]),
        }
    return {"members": result, "consensus": consensus}


@app.route("/api/fingerprint/hated")
def api_fingerprint_hated():
    if not FINGERPRINT_ENABLED or not HATED_ENABLED:
        return jsonify({"enabled": False})
    try:
        return jsonify(parse_fingerprint_hated())
    except Exception as e:
        return jsonify({"error": str(e)}), 500



# ── Settings & multi-backend endpoints (v1.1.0) ──────────────────────────────

def _read_env_file():
    """Read .env file into dict."""
    result = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                result[k.strip()] = v.strip()
    return result

_SECRET_KEYS = {"RADARR_API_KEY", "SONARR_API_KEY", "JELLYSEERR_API_KEY", "DB_PASS"}

def _mask_val(val, key):
    if key in _SECRET_KEYS and val:
        return "..." + val[-6:] if len(val) > 6 else "****"
    return val


@app.route("/api/settings/config")
def api_settings_config():
    env = _read_env_file()
    fields = [
        "DB_TYPE", "DB_PATH", "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASS",
        "RADARR_URL", "RADARR_API_KEY", "SONARR_URL", "SONARR_API_KEY",
        "JELLYSEERR_URL", "JELLYSEERR_API_KEY",
        "IMDB_BASICS_PATH", "IMDB_RATINGS_PATH",
        "DEFAULT_TAB", "ROWS_PER_TABLE",
        "THEME", "THEME_CUSTOM_BG", "THEME_CUSTOM_SURFACE", "THEME_CUSTOM_ACCENT1",
        "THEME_CUSTOM_ACCENT2", "THEME_CUSTOM_TEXT", "THEME_CUSTOM_MUTED",
        "THEME_CUSTOM_POSITIVE", "THEME_CUSTOM_WARNING", "THEME_CUSTOM_DANGER",
    ]
    out = {f.lower(): _mask_val(env.get(f, ""), f) for f in fields}
    sqlite_src = env.get("DB_PATH", "")
    out["sqlite_path_exists"] = Path(sqlite_src).exists() if sqlite_src else False
    try:
        con = _db()
        ts = con.execute("SELECT MAX(updated_at) FROM actor_career").fetchone()[0]
        con.close()
        out["imdb_last_built"] = ts
    except Exception:
        out["imdb_last_built"] = None
    return jsonify(out)


@app.route("/api/settings/save", methods=["POST"])
def api_settings_save():
    global _engine
    data = request.get_json()
    if not data:
        return jsonify({"ok": False, "error": "No data"}), 400
    KEY_MAP = {
        "db_type": "DB_TYPE", "db_path": "DB_PATH", "db_host": "DB_HOST",
        "db_port": "DB_PORT", "db_name": "DB_NAME", "db_user": "DB_USER",
        "db_pass": "DB_PASS", "radarr_url": "RADARR_URL",
        "radarr_api_key": "RADARR_API_KEY", "sonarr_url": "SONARR_URL",
        "sonarr_api_key": "SONARR_API_KEY", "jellyseerr_url": "JELLYSEERR_URL",
        "jellyseerr_api_key": "JELLYSEERR_API_KEY",
        "imdb_basics_path": "IMDB_BASICS_PATH", "imdb_ratings_path": "IMDB_RATINGS_PATH",
        "default_tab": "DEFAULT_TAB", "rows_per_table": "ROWS_PER_TABLE",
        "theme": "THEME", "theme_custom_bg": "THEME_CUSTOM_BG",
        "theme_custom_surface": "THEME_CUSTOM_SURFACE", "theme_custom_accent1": "THEME_CUSTOM_ACCENT1",
        "theme_custom_accent2": "THEME_CUSTOM_ACCENT2", "theme_custom_text": "THEME_CUSTOM_TEXT",
        "theme_custom_muted": "THEME_CUSTOM_MUTED", "theme_custom_positive": "THEME_CUSTOM_POSITIVE",
        "theme_custom_warning": "THEME_CUSTOM_WARNING", "theme_custom_danger": "THEME_CUSTOM_DANGER",
    }
    current = _read_env_file()
    for js_key, env_key in KEY_MAP.items():
        if js_key in data:
            val = str(data[js_key])
            if val.startswith("...") and len(val) <= 9:
                continue  # skip masked placeholder
            current[env_key] = val
    try:
        ENV_PATH.write_text("\n".join("{}={}".format(k, v) for k, v in current.items()) + "\n")
        for k, v in current.items():
            os.environ[k] = v
        with _engine_lock:
            _engine = None
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/db/test", methods=["POST"])
def api_db_test():
    data = request.get_json() or {}
    db_type = data.get("db_type", "sqlite")
    try:
        if db_type == "postgres":
            engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(
                data.get("db_user", ""), data.get("db_pass", ""),
                data.get("db_host", ""), data.get("db_port", "5432"),
                data.get("db_name", "")))
        elif db_type == "mysql":
            engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(
                data.get("db_user", ""), data.get("db_pass", ""),
                data.get("db_host", ""), data.get("db_port", "3306"),
                data.get("db_name", "")))
        else:
            engine = create_engine("sqlite:///{}".format(
                data.get("db_path", str(DB_PATH))))
        with engine.connect() as conn:
            conn.execute(sql_text("SELECT 1"))
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/service/test", methods=["POST"])
def api_service_test():
    data = request.get_json() or {}
    service = data.get("service", "")
    url = (data.get("url") or "").rstrip("/")
    api_key = data.get("api_key", "")
    try:
        if service in ("radarr", "sonarr"):
            r = req.get("{}/api/v3/system/status".format(url),
                        params={"apikey": api_key}, timeout=8)
            r.raise_for_status()
            return jsonify({"ok": True, "name": r.json().get("instanceName", service.title())})
        elif service == "jellyseerr":
            r = req.get("{}/api/v1/auth/me".format(url),
                        headers={"X-Api-Key": api_key}, timeout=8)
            r.raise_for_status()
            return jsonify({"ok": True, "name": r.json().get("displayName", "Jellyseerr")})
        else:
            return jsonify({"ok": False, "error": "Unknown service"}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/db/migrate", methods=["POST"])
def api_db_migrate():
    global _migrate_proc, _migrate_lines
    data = request.get_json() or {}
    env = _read_env_file()
    dest_type = env.get("DB_TYPE", "sqlite")
    if dest_type == "sqlite":
        return jsonify({"ok": False,
                        "error": "Destination is SQLite — set DB_TYPE to postgres or mysql first."}), 400
    cmd = [
        "python3", "/scripts/migrate_sqlite_to_sql.py",
        "--src", env.get("DB_PATH", str(DB_PATH)),
        "--dest-type", dest_type,
        "--dest-host", env.get("DB_HOST", ""),
        "--dest-port", env.get("DB_PORT", "5432" if dest_type == "postgres" else "3306"),
        "--dest-name", env.get("DB_NAME", ""),
        "--dest-user", env.get("DB_USER", ""),
        "--dest-pass", env.get("DB_PASS", ""),
    ]
    if data.get("wipe_dest"):
        cmd.append("--wipe-dest")
    _migrate_lines = []
    try:
        _migrate_proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1)
        def _drain():
            for line in _migrate_proc.stdout:
                _migrate_lines.append(line.rstrip())
            _migrate_proc.wait()
        threading.Thread(target=_drain, daemon=True).start()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/db/migrate/status")
def api_db_migrate_status():
    import time as _time
    def generate():
        sent = 0
        while True:
            while sent < len(_migrate_lines):
                yield "data: {}\n\n".format(_migrate_lines[sent])
                sent += 1
            if _migrate_proc and _migrate_proc.poll() is not None:
                while sent < len(_migrate_lines):
                    yield "data: {}\n\n".format(_migrate_lines[sent])
                    sent += 1
                break
            _time.sleep(0.2)
    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/imdb/refresh", methods=["POST"])
def api_imdb_refresh():
    try:
        con = _db()
        con.execute("DELETE FROM actor_career")
        con.commit()
        con.close()
    except Exception as e:
        return jsonify({"ok": False, "error": "DB clear failed: {}".format(str(e))}), 500
    try:
        req.post("{}/run".format(RUNNER_URL), timeout=5)
    except Exception:
        pass
    return jsonify({"ok": True, "message": "Cache cleared. Rebuild started."})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=DASHBOARD_PORT)
