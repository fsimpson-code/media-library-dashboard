#!/usr/bin/env python3
"""
Media Library Dashboard — Setup & Configuration
Usage:
  python3 setup.py              # first-time setup or update check
  python3 setup.py --reconfigure  # force full prompt flow
  python3 setup.py --validate     # test connections only
"""
import sys
import os
import json
import importlib.util
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' is required. Run: pip install requests")
    sys.exit(1)

CONFIG_PATH = Path("config.py")
EXAMPLE_PATH = Path("config.example.py")

REQUIRED_KEYS = [
    "DASHBOARD_NAME",
    "RADARR_URL", "RADARR_API_KEY",
    "SONARR_URL", "SONARR_API_KEY",
    "SEERR_URL", "SEERR_API_KEY",
    "DB_PATH", "HISTORY_DIR",
    "RUNNER_URL", "DASHBOARD_PORT",
    "FINGERPRINT_ENABLED", "FINGERPRINT_MEMBERS",
    "HATED_ENABLED",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_config(path: Path) -> dict:
    spec = importlib.util.spec_from_file_location("config", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return {k: getattr(mod, k) for k in dir(mod) if not k.startswith("_")}


def prompt(label: str, default=None, secret=False) -> str:
    suffix = f" [{default}]" if default is not None else ""
    try:
        if secret:
            import getpass
            val = getpass.getpass(f"  {label}{suffix}: ")
        else:
            val = input(f"  {label}{suffix}: ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(0)
    return val if val else (str(default) if default is not None else "")


def confirm(label: str, default=True) -> bool:
    hint  = "Y/n" if default else "y/N"
    reply = prompt(f"{label} ({hint})", default="").lower()
    if not reply:
        return default
    return reply in ("y", "yes")


def check_connection(url: str, timeout=5) -> bool:
    try:
        r = requests.get(url, timeout=timeout)
        return r.status_code < 500
    except Exception:
        return False


def validate_radarr(url: str, key: str) -> tuple[bool, int]:
    try:
        r = requests.get(f"{url.rstrip('/')}/api/v3/movie",
                         params={"apikey": key}, timeout=8)
        if r.status_code == 200:
            return True, len(r.json())
        return False, 0
    except Exception:
        return False, 0


def validate_sonarr(url: str, key: str) -> tuple[bool, int]:
    try:
        r = requests.get(f"{url.rstrip('/')}/api/v3/series",
                         params={"apikey": key}, timeout=8)
        if r.status_code == 200:
            return True, len(r.json())
        return False, 0
    except Exception:
        return False, 0


def validate_seerr(url: str, key: str) -> tuple[bool, str]:
    try:
        r = requests.get(f"{url.rstrip('/')}/api/v1/settings/main",
                         headers={"X-Api-Key": key}, timeout=8)
        if r.status_code == 200:
            name = r.json().get("applicationTitle", "Jellyseer")
            return True, name
        return False, ""
    except Exception:
        return False, ""


def get_radarr_tags(url: str, key: str) -> list[str]:
    try:
        r = requests.get(f"{url.rstrip('/')}/api/v3/tag",
                         params={"apikey": key}, timeout=8)
        if r.status_code == 200:
            return [t["label"].lower() for t in r.json()]
        return []
    except Exception:
        return []


def create_radarr_tag(url: str, key: str, label: str) -> bool:
    try:
        r = requests.post(f"{url.rstrip('/')}/api/v3/tag",
                          params={"apikey": key},
                          json={"label": label}, timeout=8)
        return r.status_code in (200, 201)
    except Exception:
        return False


def write_config(cfg: dict):
    members_repr = repr(cfg["FINGERPRINT_MEMBERS"])
    lines = [
        f'DASHBOARD_NAME      = {repr(cfg["DASHBOARD_NAME"])}',
        "",
        f'RADARR_URL          = {repr(cfg["RADARR_URL"])}',
        f'RADARR_API_KEY      = {repr(cfg["RADARR_API_KEY"])}',
        "",
        f'SONARR_URL          = {repr(cfg["SONARR_URL"])}',
        f'SONARR_API_KEY      = {repr(cfg["SONARR_API_KEY"])}',
        "",
        f'SEERR_URL           = {repr(cfg["SEERR_URL"])}',
        f'SEERR_API_KEY       = {repr(cfg["SEERR_API_KEY"])}',
        "",
        f'DB_PATH             = {repr(cfg["DB_PATH"])}',
        f'HISTORY_DIR         = {repr(cfg["HISTORY_DIR"])}',
        "",
        f'RUNNER_URL          = {repr(cfg["RUNNER_URL"])}',
        f'DASHBOARD_PORT      = {cfg["DASHBOARD_PORT"]}',
        "",
        f'FINGERPRINT_ENABLED = {cfg["FINGERPRINT_ENABLED"]}',
        f'FINGERPRINT_MEMBERS = {members_repr}',
        f'HATED_ENABLED       = {cfg["HATED_ENABLED"]}',
    ]
    CONFIG_PATH.write_text("\n".join(lines) + "\n")
    print(f"\n  ✓ config.py written")


# ── Validation only ───────────────────────────────────────────────────────────

def run_validate(cfg: dict):
    print("\n── Validating connections ───────────────────────────────")
    ok, count = validate_radarr(cfg["RADARR_URL"], cfg["RADARR_API_KEY"])
    print(f"  {'✓' if ok else '✗'} Radarr — {count} movies" if ok else "  ✗ Radarr — connection failed")

    ok, count = validate_sonarr(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
    print(f"  {'✓' if ok else '✗'} Sonarr — {count} series" if ok else "  ✗ Sonarr — connection failed")

    ok, name = validate_seerr(cfg["SEERR_URL"], cfg["SEERR_API_KEY"])
    print(f"  {'✓' if ok else '✗'} Jellyseer ({name})" if ok else "  ✗ Jellyseer — connection failed")
    print()


# ── Update check (existing config) ───────────────────────────────────────────

def run_update_check(existing: dict, reconfigure: bool):
    if reconfigure:
        return run_full_setup(existing)

    # Find keys in REQUIRED_KEYS not present in existing config
    missing = [k for k in REQUIRED_KEYS if k not in existing]
    if missing:
        print(f"\n  New config keys detected: {', '.join(missing)}")
        print("  Loading defaults from config.example.py...\n")
        example = load_config(EXAMPLE_PATH) if EXAMPLE_PATH.exists() else {}
        for key in missing:
            default = example.get(key, "")
            val = prompt(f"{key}", default=default)
            if isinstance(default, bool):
                existing[key] = val.lower() in ("true", "1", "yes")
            elif isinstance(default, int):
                try:
                    existing[key] = int(val)
                except ValueError:
                    existing[key] = default
            elif isinstance(default, list):
                existing[key] = [v.strip() for v in val.split(",") if v.strip()] if val else []
            else:
                existing[key] = val
        write_config(existing)

    run_validate(existing)
    print("  Config is up to date.")


# ── Full setup flow ───────────────────────────────────────────────────────────

def run_full_setup(defaults: dict = None):
    d = defaults or {}
    cfg = {}

    print("\n── Dashboard Identity ───────────────────────────────────")
    cfg["DASHBOARD_NAME"] = prompt("Dashboard name", d.get("DASHBOARD_NAME"))
    while not cfg["DASHBOARD_NAME"]:
        print("  Dashboard name is required.")
        cfg["DASHBOARD_NAME"] = prompt("Dashboard name")

    # Radarr
    print("\n── Radarr ───────────────────────────────────────────────")
    while True:
        cfg["RADARR_URL"]     = prompt("Radarr URL", d.get("RADARR_URL", "http://localhost:7878"))
        cfg["RADARR_API_KEY"] = prompt("Radarr API key", d.get("RADARR_API_KEY", ""), secret=True)
        ok, count = validate_radarr(cfg["RADARR_URL"], cfg["RADARR_API_KEY"])
        if ok:
            print(f"  ✓ Radarr connected ({count} movies)")
            break
        print("  ✗ Could not connect. Check URL and API key.")
        if not confirm("Retry?"):
            break

    # Sonarr
    print("\n── Sonarr ───────────────────────────────────────────────")
    while True:
        cfg["SONARR_URL"]     = prompt("Sonarr URL", d.get("SONARR_URL", "http://localhost:8989"))
        cfg["SONARR_API_KEY"] = prompt("Sonarr API key", d.get("SONARR_API_KEY", ""), secret=True)
        ok, count = validate_sonarr(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
        if ok:
            print(f"  ✓ Sonarr connected ({count} series)")
            break
        print("  ✗ Could not connect. Check URL and API key.")
        if not confirm("Retry?"):
            break

    # Jellyseer
    print("\n── Jellyseer / Overseerr ────────────────────────────────")
    while True:
        cfg["SEERR_URL"]     = prompt("Jellyseer/Overseerr URL", d.get("SEERR_URL", "http://localhost:5055"))
        cfg["SEERR_API_KEY"] = prompt("API key", d.get("SEERR_API_KEY", ""), secret=True)
        ok, name = validate_seerr(cfg["SEERR_URL"], cfg["SEERR_API_KEY"])
        if ok:
            print(f"  ✓ {name} connected")
            break
        print("  ✗ Could not connect. Check URL and API key.")
        if not confirm("Retry?"):
            break

    # Paths
    print("\n── Paths ────────────────────────────────────────────────")
    cfg["DB_PATH"]        = prompt("Database path", d.get("DB_PATH", "./data/simpson_history.db"))
    cfg["HISTORY_DIR"]    = prompt("History directory", d.get("HISTORY_DIR", "./data/history/"))
    cfg["RUNNER_URL"]     = prompt("Runner URL", d.get("RUNNER_URL", "http://localhost:5757"))
    port_str              = prompt("Dashboard port", d.get("DASHBOARD_PORT", 8686))
    cfg["DASHBOARD_PORT"] = int(port_str) if str(port_str).isdigit() else 8686

    # Family Fingerprint
    print("\n── Optional Features ────────────────────────────────────")
    cfg["FINGERPRINT_ENABLED"] = confirm("Enable Family Fingerprint?", d.get("FINGERPRINT_ENABLED", False))
    cfg["FINGERPRINT_MEMBERS"] = []
    cfg["HATED_ENABLED"]       = False

    if cfg["FINGERPRINT_ENABLED"]:
        members_default = ", ".join(d.get("FINGERPRINT_MEMBERS", []))
        members_str = prompt("Member names (comma separated)", members_default or None)
        members = [m.strip().lower() for m in members_str.split(",") if m.strip()]
        cfg["FINGERPRINT_MEMBERS"] = members

        if members:
            print("  Checking Radarr tags...")
            existing_tags = get_radarr_tags(cfg["RADARR_URL"], cfg["RADARR_API_KEY"])
            for member in members:
                tag = member
                hate_tag = f"{member}-hate"
                for t in [tag, hate_tag]:
                    if t not in existing_tags:
                        print(f"  Tag '{t}' not found in Radarr.", end=" ")
                        if confirm("Create it?"):
                            ok = create_radarr_tag(cfg["RADARR_URL"], cfg["RADARR_API_KEY"], t)
                            print(f"  {'✓ Created' if ok else '✗ Failed to create'} tag '{t}'")
                    else:
                        print(f"  ✓ Tag '{t}' exists")

        cfg["HATED_ENABLED"] = confirm("Enable Hated tracking?", d.get("HATED_ENABLED", False))

    write_config(cfg)

    # docker-compose
    compose_example = Path("docker-compose.yml.example")
    compose_target  = Path("docker-compose.yml")
    if compose_example.exists() and not compose_target.exists():
        import shutil
        shutil.copy(compose_example, compose_target)
        print("  ✓ docker-compose.yml created from example")

    print("\n  ✓ Setup complete. Run: docker compose up -d\n")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    reconfigure = "--reconfigure" in args
    validate    = "--validate"    in args

    if validate:
        if not CONFIG_PATH.exists():
            print("ERROR: config.py not found. Run setup.py first.")
            sys.exit(1)
        run_validate(load_config(CONFIG_PATH))
        return

    if CONFIG_PATH.exists():
        existing = load_config(CONFIG_PATH)
        run_update_check(existing, reconfigure)
    else:
        print("No config.py found — running first-time setup.")
        run_full_setup()


if __name__ == "__main__":
    main()
