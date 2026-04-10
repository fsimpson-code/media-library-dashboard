#!/usr/bin/env python3
"""
runner.py — Media Library runner service
Exposes HTTP endpoints to trigger library_runner.py, stream its output
via SSE, and return history data.

Endpoints:
  POST /run            — start a library update run
  GET  /stream         — SSE stream of live stdout/stderr
  GET  /status         — current run status (idle/running/done/error)
  GET  /history        — JSON array of past runs from SQLite
  GET  /history/trends — JSON trend data for charts
"""

import os, subprocess, threading, sqlite3, json, glob
from datetime import datetime
from pathlib import Path
from flask import Flask, Response, jsonify, send_file, abort

app = Flask(__name__)

SCRIPT_PATH  = "/scripts/library_runner.py"
HISTORY_DIR  = Path("/scripts/history")

try:
    from config import DB_PATH as _CFG_DB
    DB_PATH = Path(_CFG_DB)
except ImportError:
    DB_PATH = Path(os.environ.get("DB_PATH", "/data/library_history.db"))

# ── Run state ─────────────────────────────────────────────────────────────────
_lock        = threading.Lock()
_running     = False
_output_buf  = []          # list of (timestamp, line) tuples
_status      = "idle"      # idle | running | done | error
_clients     = []          # SSE response queues


def _broadcast(line):
    """Send a line to all connected SSE clients."""
    ts   = datetime.now().strftime("%H:%M:%S")
    msg  = f"[{ts}] {line}"
    _output_buf.append(msg)
    dead = []
    for q in _clients:
        try:
            q.append(msg)
        except Exception:
            dead.append(q)
    for q in dead:
        _clients.remove(q)


def _run_script():
    global _running, _status
    _broadcast("▶ Starting Simpson Library update...")
    _broadcast("=" * 52)
    try:
        proc = subprocess.Popen(
            ["python3", SCRIPT_PATH],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        for line in proc.stdout:
            _broadcast(line.rstrip())
        proc.wait()
        if proc.returncode == 0:
            _status = "done"
            _broadcast("=" * 52)
            _broadcast("✓ Run completed successfully.")
        else:
            _status = "error"
            _broadcast("=" * 52)
            _broadcast(f"✗ Run failed — exit code {proc.returncode}")
    except Exception as e:
        _status = "error"
        _broadcast(f"✗ Exception: {e}")
    finally:
        _running = False


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/run", methods=["POST"])
def run():
    global _running, _status, _output_buf
    with _lock:
        if _running:
            return jsonify({"status": "already_running"}), 409
        _running    = True
        _status     = "running"
        _output_buf = []
    t = threading.Thread(target=_run_script, daemon=True)
    t.start()
    return jsonify({"status": "started"})


@app.route("/stream")
def stream():
    """SSE endpoint — streams output lines as they arrive."""
    client_buf = []
    _clients.append(client_buf)
    # Replay buffer for late joiners
    replay = list(_output_buf)

    def generate():
        # Send anything already buffered
        for line in replay:
            yield f"data: {line}\n\n"
        # Then stream live
        last_idx = len(replay)
        while True:
            current = client_buf[last_idx:]
            for line in current:
                yield f"data: {line}\n\n"
            last_idx += len(current)
            if not _running and last_idx >= len(client_buf):
                # Send final status and close
                yield f"data: __STATUS__{_status}\n\n"
                break
            import time; time.sleep(0.25)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":               "no-cache",
            "X-Accel-Buffering":           "no",
            "Access-Control-Allow-Origin": "*",
        },
    )


@app.route("/status")
def status():
    return jsonify({
        "status":  _status,
        "running": _running,
        "lines":   len(_output_buf),
    })



@app.route("/history")
def history():
    """Return all past runs as JSON."""
    if not DB_PATH.exists():
        return jsonify([])
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("""
        SELECT id, run_date, run_ts, movie_count, tv_count,
               movie_gb, tv_gb, x264_count, x265_count
        FROM runs
        ORDER BY run_ts DESC
        LIMIT 90
    """).fetchall()
    con.close()
    return jsonify([dict(r) for r in rows])


@app.route("/history/trends")
def trends():
    """Return trend data shaped for chart consumption."""
    if not DB_PATH.exists():
        return jsonify({"runs": []})
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("""
        SELECT id, run_date, run_ts,
               movie_count, tv_count,
               round(movie_gb + tv_gb, 2) AS total_gb,
               round(movie_gb, 2)         AS movie_gb,
               round(tv_gb, 2)            AS tv_gb,
               x264_count, x265_count
        FROM runs
        ORDER BY run_ts ASC
        LIMIT 90
    """).fetchall()
    con.close()

    data = [dict(r) for r in rows]

    # Calculate space delta between consecutive runs
    for i, row in enumerate(data):
        if i == 0:
            row["gb_delta"] = 0
        else:
            row["gb_delta"] = round(row["total_gb"] - data[i-1]["total_gb"], 2)

    return jsonify({"runs": data})


@app.route("/history/<int:run_id>", methods=["DELETE"])
def delete_run(run_id):
    if not DB_PATH.exists():
        return jsonify({"error": "No database found"}), 404
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM movie_snapshots WHERE run_id = ?", (run_id,))
    cur.execute("DELETE FROM tv_snapshots WHERE run_id = ?", (run_id,))
    cur.execute("DELETE FROM runs WHERE id = ?", (run_id,))
    con.commit()
    deleted = cur.rowcount
    con.close()
    if deleted == 0:
        return jsonify({"error": "Run not found"}), 404
    return jsonify({"deleted": run_id})

@app.route("/history/<int:run_id>/snapshot")
def run_snapshot(run_id):
    """Return aggregated stats for a specific historical run."""
    if not DB_PATH.exists():
        return jsonify({"error": "No database found"}), 404
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    
    # Verify run exists
    run = con.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    if not run:
        con.close()
        return jsonify({"error": "Run not found"}), 404
    run = dict(run)
    
    # Movie aggregations
    movies = con.execute(
        "SELECT * FROM movie_snapshots WHERE run_id = ?", (run_id,)).fetchall()
    movies = [dict(m) for m in movies]
    
    has_file = [m for m in movies if m["has_file"]]
    total_gb = sum(m["file_size_gb"] or 0 for m in has_file)
    
    from collections import Counter
    codec_c  = Counter(m["video_codec"] or "Unknown" for m in has_file)
    res_map  = {2160:"4K (2160p)",1080:"1080p",720:"720p",480:"480p"}
    res_c    = Counter(res_map.get(m["resolution"], "Unknown") for m in has_file)
    hdr_c    = Counter(m["hdr_type"] or "SDR" for m in has_file)
    
    # TV aggregations
    series = con.execute(
        "SELECT * FROM tv_snapshots WHERE run_id = ?", (run_id,)).fetchall()
    series = [dict(s) for s in series]
    tv_gb  = sum(s["size_gb"] or 0 for s in series)
    total_eps = sum(s["episodes_have"] or 0 for s in series)
    complete  = sum(1 for s in series if s["completion_pct"] == 100)
    
    con.close()
    
    return jsonify({
        "run": run,
        "movies": {
            "total":        len(movies),
            "downloaded":   len(has_file),
            "wishlist":     len(movies) - len(has_file),
            "upgrade_queue": sum(1 for m in has_file if m["cutoff_not_met"]),
            "total_gb":     round(total_gb, 2),
            "x264_count":   sum(1 for m in has_file if m["video_codec"] in ("x264","h264")),
            "x265_count":   sum(1 for m in has_file if m["video_codec"] in ("x265","h265","HEVC")),
            "codec":        [{"label":k,"value":v} for k,v in codec_c.most_common()],
            "resolution":   [{"label":k,"value":v} for k,v in res_c.most_common()],
            "hdr":          [{"label":k,"value":v} for k,v in hdr_c.most_common()],
        },
        "tv": {
            "total":        len(series),
            "complete":     complete,
            "total_gb":     round(tv_gb, 2),
            "total_episodes": total_eps,
        },
    })



@app.route("/reset", methods=["POST"])
def reset_runner():
    global _running, _status
    with _lock:
        _running = False
        _status  = "idle"
    return jsonify({"ok": True, "message": "Runner state reset."})


@app.route("/reset", methods=["POST"])
def reset_runner():
    global _running, _status
    with _lock:
        _running = False
        _status  = "idle"
    return jsonify({"ok": True, "message": "Runner state reset."})

if __name__ == "__main__":
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    print("Simpson Library Runner — listening on :5757")
    app.run(host="0.0.0.0", port=5757, threaded=True)
