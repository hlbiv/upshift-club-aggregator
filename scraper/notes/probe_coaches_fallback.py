"""Live measurement of the PR-9 coaches-page fallback (Task #38).

Re-fetches the three D1 schools that the Task #34 inline-coach probe
identified as fully JS-rendered (Pepperdine, George Mason, Virginia
Tech) plus a small SIDEARM control set, and records the before/after
head-coach hit rate. Captures the diff requested by Task #38's "record
the diff in scraper/notes/" acceptance criterion without touching the
production DB.

Run from repo root:
    python3 scraper/notes/probe_coaches_fallback.py

Outputs a markdown table to stdout.
"""
from __future__ import annotations

import os
import sys
import time
from typing import List, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ncaa_soccer_rosters import (  # noqa: E402
    _get_session,
    fetch_with_retry,
    extract_head_coach_from_html,
    probe_coaches_pages,
    compose_coaches_urls,
)


# (school_name, current-season roster URL).
# Pepperdine / George Mason / Virginia Tech were the 3-school residual
# from Task #34 — JS-rendered roster pages with zero inline staff
# markup at any selector. Anything we recover from these is a clean
# win attributable to PR-9.
SAMPLE: List[Tuple[str, str]] = [
    ("Pepperdine (men)", "https://pepperdinewaves.com/sports/mens-soccer/roster"),
    ("George Mason (men)", "https://gomason.com/sports/mens-soccer/roster"),
    ("Virginia Tech (men)", "https://hokiesports.com/sports/mens-soccer/roster"),
]


def main() -> int:
    session = _get_session()
    cache: dict = {}

    rows = []
    print("# PR-9 coaches-page fallback — live probe results\n")
    print(f"Sample size: {len(SAMPLE)} schools (Task #34 JS-rendered residual)\n")
    print("| School | Inline (Task #34) | Fallback (PR-9) | Source URL |")
    print("| --- | --- | --- | --- |")

    inline_hits = 0
    fallback_hits = 0

    for name, roster_url in SAMPLE:
        candidates = compose_coaches_urls(roster_url)
        roster_html = fetch_with_retry(session, roster_url) or ""
        inline = extract_head_coach_from_html(roster_html) if roster_html else None
        if inline:
            inline_hits += 1
            inline_str = f"{inline['name']} ({inline['title']})"
            fallback_str = "n/a (inline hit)"
            src = roster_url
        else:
            inline_str = "MISS"
            fallback = probe_coaches_pages(session, roster_url, cache=cache)
            if fallback:
                fallback_hits += 1
                fallback_str = f"{fallback['name']} ({fallback['title']})"
                src = fallback.get("_source_url", "?")
            else:
                fallback_str = "MISS"
                src = f"probed: {', '.join(candidates)}"
        rows.append((name, inline_str, fallback_str, src))
        print(f"| {name} | {inline_str} | {fallback_str} | {src} |")
        time.sleep(1.0)

    total = len(SAMPLE)
    combined = inline_hits + fallback_hits
    print()
    print("## Summary")
    print(f"- Inline-only baseline: **{inline_hits}/{total}** "
          f"({100.0 * inline_hits / total:.0f}%)")
    print(f"- With PR-9 fallback:   **{combined}/{total}** "
          f"({100.0 * combined / total:.0f}%)")
    print(f"- Net additional captures from PR-9: **+{fallback_hits}**")
    return 0


if __name__ == "__main__":
    sys.exit(main())
