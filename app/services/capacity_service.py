"""Shared FC capacity service.

Single source of truth for FC congestion data across all three tool modes:
- FC Capacity Dashboard  (served via get_all_fc_statuses + is_capacity_cached)
- Escalation Lookup      (get_fc_status for verdict enrichment)
- PO Flip Request        (get_fc_status for source/target congestion warnings)

Two separate caches with different TTLs:
  fc_capacity_cache.json  — 10 min   (real-time yard data, changes hourly)
  uph_cache.json          —  6 hr    (weekly UPH data, barely changes)

Safety:
  - threading.Lock prevents concurrent BQ double-fire on cache miss
  - Atomic os.replace() writes prevent JSON corruption under concurrent load
  - All public functions return None / [] on error — never crash a submission
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any

log = logging.getLogger(__name__)

# ── File paths ───────────────────────────────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CAPACITY_CACHE_FILE = os.path.join(_ROOT, "fc_capacity_cache.json")
UPH_CACHE_FILE      = os.path.join(_ROOT, "uph_cache.json")

# ── TTLs ─────────────────────────────────────────────────────────────────────
CAPACITY_TTL = 600    # 10 min — real-time yard data
UPH_TTL      = 21600  # 6 hr  — weekly UPH, doesn’t change intra-day

# ── Locks — one per cache, prevents concurrent BQ double-fire ─────────────────
_capacity_lock = threading.Lock()
_uph_lock      = threading.Lock()


# ── Low-level cache helpers ──────────────────────────────────────────────────

def _read_json(path: str) -> dict:
    """Read a JSON cache file. Returns empty cache on any error."""
    try:
        if os.path.exists(path):
            with open(path, "r") as f:
                return json.load(f)
    except Exception as e:
        log.warning("Cache read failed (%s): %s", path, e)
    return {"time": 0, "data": []}


def _write_json(path: str, data: list) -> None:
    """Atomically write data to a JSON cache file (safe under concurrent load)."""
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump({"time": time.time(), "data": data}, f)
        os.replace(tmp, path)  # atomic on POSIX — readers never see a partial file
    except Exception as e:
        log.error("Cache write failed (%s): %s", path, e)


def _is_fresh(path: str, ttl: int) -> bool:
    cache = _read_json(path)
    return bool(cache["data"] and (time.time() - cache["time"] < ttl))


# ── FC Capacity (real-time yard data) ────────────────────────────────────────

def refresh_capacity_cache() -> None:
    """Fetch fresh FC capacity data from BQ and write to cache.
    Called proactively by APScheduler every 10 min and on startup.
    """
    from app.services.bigquery import get_fc_capacity_raw  # lazy — avoids circular import at module load
    try:
        data = get_fc_capacity_raw()
        _write_json(CAPACITY_CACHE_FILE, data)
        log.info("FC capacity cache refreshed — %d FCs", len(data))
    except Exception as e:
        log.error("FC capacity cache refresh failed: %s", e)


def get_all_fc_statuses() -> list[dict[str, Any]]:
    """Return all FC capacity rows. Triggers BQ refresh if cache is cold."""
    if not _is_fresh(CAPACITY_CACHE_FILE, CAPACITY_TTL):
        with _capacity_lock:
            if not _is_fresh(CAPACITY_CACHE_FILE, CAPACITY_TTL):  # double-check inside lock
                refresh_capacity_cache()
    return _read_json(CAPACITY_CACHE_FILE)["data"]


def is_capacity_cached() -> bool:
    """True if the FC capacity cache exists and is younger than CAPACITY_TTL.
    Used by the FC Capacity page to decide whether to SSR data or defer to HTMX.
    """
    return _is_fresh(CAPACITY_CACHE_FILE, CAPACITY_TTL)


def get_fc_status(fc_name: str) -> dict[str, Any] | None:
    """Return capacity row for a specific FC by name (case-insensitive).
    Returns None gracefully if not found or cache unavailable — never crashes a submission.
    """
    if not fc_name:
        return None
    try:
        name_upper = fc_name.strip().upper()
        return next(
            (r for r in get_all_fc_statuses() if (r.get("fc_name") or "").upper() == name_upper),
            None,
        )
    except Exception as e:
        log.warning("get_fc_status(%s) failed: %s", fc_name, e)
        return None


def is_fc_congested(fc_name: str) -> bool:
    """Quick bool — True if FC status is High."""
    status = get_fc_status(fc_name)
    return bool(status and status.get("status") == "High")


# ── IB UPH (weekly, slow-moving) ─────────────────────────────────────────────

def refresh_uph_cache() -> None:
    """Fetch fresh IB UPH data from BQ and write to cache.
    Called proactively by APScheduler every 6 hr and on startup.
    """
    from app.services.bigquery import get_fc_uph_raw  # lazy import
    try:
        data = get_fc_uph_raw()
        _write_json(UPH_CACHE_FILE, data)
        log.info("UPH cache refreshed — %d FCs", len(data))
    except Exception as e:
        log.error("UPH cache refresh failed: %s", e)


def get_all_fc_uph() -> list[dict[str, Any]]:
    """Return all UPH rows. Triggers BQ refresh if cache is cold."""
    if not _is_fresh(UPH_CACHE_FILE, UPH_TTL):
        with _uph_lock:
            if not _is_fresh(UPH_CACHE_FILE, UPH_TTL):  # double-check inside lock
                refresh_uph_cache()
    return _read_json(UPH_CACHE_FILE)["data"]


def get_fc_uph(fc_name: str) -> dict[str, Any] | None:
    """Return UPH row for a specific FC (case-insensitive match on fc_name_raw).
    UPH data from BQ uses lowercase FC names — handled here transparently.
    """
    if not fc_name:
        return None
    try:
        name_lower = fc_name.strip().lower()
        return next(
            (r for r in get_all_fc_uph() if (r.get("fc_name_raw") or "").lower() == name_lower),
            None,
        )
    except Exception as e:
        log.warning("get_fc_uph(%s) failed: %s", fc_name, e)
        return None
