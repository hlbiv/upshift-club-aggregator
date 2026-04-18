"""
One-shot HTTP audit for the 6 Google MyMaps state-association seeds.

For every entry in ``scraper/extractors/state_assoc.py:_STATE_CONFIG`` whose
``type == "google_maps"``, fetch the KML feed
(``https://www.google.com/maps/d/kml?mid=<map_id>&forcekml=1``) and report
its status + Placemark count. Exits non-zero if any seed comes back broken
so the script can be wired into CI later if desired.

Usage:
    cd scraper && python3 audits/audit_state_maps.py

This is a read-only script — no DB writes, no config edits. If a seed comes
back broken, prefer adding ``"disabled": true`` to its
``state_assoc_config.json`` entry over deletion (preserves ``_note`` audit
history). The matching guard lives in ``state_assoc._scrape_state``.
"""

from __future__ import annotations

import os
import re
import sys
from typing import List, Tuple

# Make ``extractors`` and ``utils`` importable when running as
# ``python3 audits/audit_state_maps.py`` from the ``scraper/`` cwd.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests

from extractors.state_assoc import _STATE_CONFIG  # noqa: E402
from utils.retry import retry_with_backoff  # noqa: E402


HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0)"}
KML_URL = "https://www.google.com/maps/d/kml?mid={mid}&forcekml=1"
TIMEOUT_SECONDS = 20

# Reused from extractors/state_assoc.py:116
_PLACEMARK_NAME_RE = re.compile(r"<name>([^<]+)</name>")


def _collect_google_maps_seeds() -> List[Tuple[str, str, str]]:
    """Return ``(state, source_url, map_id)`` triples for every google_maps seed."""
    seeds: List[Tuple[str, str, str]] = []
    for source_url, cfg in _STATE_CONFIG.items():
        if cfg.get("type") != "google_maps":
            continue
        state = cfg.get("state", "?")
        for mid in cfg.get("map_ids", []):
            seeds.append((state, source_url, mid))
    return seeds


def _fetch_kml(map_id: str) -> requests.Response:
    url = KML_URL.format(mid=map_id)
    return retry_with_backoff(
        lambda: requests.get(url, headers=HEADERS, timeout=TIMEOUT_SECONDS),
        max_retries=3,
        base_delay=2.0,
        retryable_check=lambda e: isinstance(e, requests.RequestException),
        label=f"audit-state-maps:{map_id}",
    )


def _classify(map_id: str) -> Tuple[str, int, str]:
    """Return ``(status, placemark_count, error_message)`` for one map_id."""
    try:
        r = _fetch_kml(map_id)
    except Exception as exc:  # noqa: BLE001 — final classification, not a swallow
        return ("ERROR", 0, f"{type(exc).__name__}: {exc}")

    if r.status_code != 200:
        return (f"HTTP_{r.status_code}", 0, "")

    placemarks = _PLACEMARK_NAME_RE.findall(r.text)
    count = len(placemarks)
    if count == 0:
        return ("EMPTY", 0, "")
    return ("OK", count, "")


def main() -> int:
    seeds = _collect_google_maps_seeds()
    if not seeds:
        print("No google_maps seeds found in _STATE_CONFIG — nothing to audit.")
        return 1

    rows: List[Tuple[str, str, str, int, str]] = []
    any_broken = False
    for state, _source_url, map_id in seeds:
        status, count, err = _classify(map_id)
        if status != "OK":
            any_broken = True
        rows.append((state, map_id, status, count, err))

    # Compute column widths from the rows themselves.
    state_w = max(len("STATE"), max(len(r[0]) for r in rows))
    mid_w = max(len("MAP_ID"), max(len(r[1]) for r in rows))
    status_w = max(len("STATUS"), max(len(r[2]) for r in rows))
    pm_w = max(len("PLACEMARKS"), max(len(str(r[3])) for r in rows))

    header = (
        f"{'STATE':<{state_w}} | "
        f"{'MAP_ID':<{mid_w}} | "
        f"{'STATUS':<{status_w}} | "
        f"{'PLACEMARKS':>{pm_w}} | "
        f"ERROR"
    )
    print(header)
    print("-" * len(header))
    for state, map_id, status, count, err in rows:
        print(
            f"{state:<{state_w}} | "
            f"{map_id:<{mid_w}} | "
            f"{status:<{status_w}} | "
            f"{count:>{pm_w}} | "
            f"{err}"
        )

    if any_broken:
        broken = [(s, m, st) for (s, m, st, _c, _e) in rows if st != "OK"]
        print(
            f"\n{len(broken)} broken seed(s): "
            + ", ".join(f"{s} ({st})" for s, _m, st in broken)
        )
        print(
            "Recommendation: add `\"disabled\": true` to each broken seed in "
            "scraper/data/state_assoc_config.json (preserves the `_note` audit "
            "history). The guard in state_assoc._scrape_state will short-circuit "
            "on the flag."
        )
        return 1

    print(f"\nAll {len(rows)} seed(s) OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
