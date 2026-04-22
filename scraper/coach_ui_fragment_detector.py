"""Detect UI-fragment pollution in `coach_discoveries` and upsert one
`coach_quality_flags` row of type `ui_fragment_as_name` per offending
discovery.

Second-wave complement to `coach_pollution_detector.py`. The first-wave
`looks_like_name_reject` flag catches role-label / format-shape rejects
(`Head Coach`, `OPEN TRAINING & TRYOUTS`, `Saturday, April 11`, …). It
does NOT catch strings whose *shape* looks like a plausible name —
Title-Case, two tokens, alphabetic — but whose *content* is a UI label,
pricing tier, section heading, or marketing tile.

Example survivors from the April 2026 post-purge Q2 audit on production
`upshift-data`:

    "Where We Are"                 — nav link
    "Get In Touch"                 — nav link
    "Camp Dates"                   — pricing/schedule label
    "One Week"                     — pricing tier
    "Technical Ball Mastery"       — service-tile heading
    "Privacy Preference Center"    — cookie-banner heading
    "Fashion Magazine"             — third-party widget
    "Championship For The Community" — marketing tile

None of these reject through `looks_like_name` — two-token, Title-Case,
alpha-start, no blocklist hit. They still aren't coach names.

Approach: **strictly-exact case-folded gazetteer match.** No regex, no
heuristics on length or token count. The trade-off is precision over
recall — new classes require an operator to add the string to the
gazetteer below and re-run. That's deliberate: false-positives in the
coach read model are more expensive than false-negatives (a false-flag
deletes a real coach; a missed flag just means one more round of audit
later).

CLI: python3 run.py --source coach-ui-fragment-detect \\
        [--commit] [--limit N] [--window-days N]

Dry-run by default, same safety inversion as `coach-pollution-detect`.

Idempotent via the (discovery_id, flag_type) unique constraint in
`coach_quality_flags_discovery_type_uq`.

Note: requires PR adding `ui_fragment_as_name` to the CHECK constraint
in `lib/db/src/schema/coach-quality-flags.ts` + a Replit
`pnpm --filter @workspace/db run push` before `--commit` mode will
succeed. Dry-run works today.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import Json  # type: ignore
except ImportError:  # pragma: no cover — tested envs have psycopg2
    psycopg2 = None  # type: ignore
    Json = None  # type: ignore


log = logging.getLogger("coach_ui_fragment_detector")

FLAG_TYPE = "ui_fragment_as_name"


# ---------------------------------------------------------------------------
# Gazetteer
# ---------------------------------------------------------------------------
#
# Each entry is stored exactly as it was observed in the
# `coach_discoveries.name` column on production (upshift-data, April
# 2026). Matching is case-folded + whitespace-trimmed. The categories
# below are documentary — the matcher returns a single `ui_fragment`
# reason code on any hit, and the matched raw string goes into the flag
# metadata for triage. We do NOT branch on category at match time
# because a given string could plausibly belong to more than one
# (e.g. "Important Dates" is both a section heading and a pricing/date
# label).
#
# Extending: append new strings in alphabetical order within their
# category. Case is preserved for documentation only; matching is
# case-insensitive.

_NAV_LABELS: Tuple[str, ...] = (
    "Contact Us",
    "Corporate Info",
    "Corporate Sponsors",
    "Get In Touch",
    "Important Links",
    "Nearby Communities",
    "National Edition",
    "Privacy Preference Center",
    "Privacy Preferences",
    "State Edition",
    "Where We Are",
)

_MARKETING_TILES: Tuple[str, ...] = (
    "Add commentCancel reply",
    "Balance, Quickness and Agility",
    "Book a Trial",
    "Championship For The Community",
    "City skyline",
    "Fashion Magazine",
    "Get real feedback",
    "SKV Sporting Stripes",
    "Slide title",
    "Storm for Nike",
    "Technical Ball Mastery",
    "Video of the Day",
    "Watch them Grow",
)

_PRICING_AND_DATES: Tuple[str, ...] = (
    "Camp Dates",
    "Four Week",
    "Important Dates",
    "One Week",
    "Six Week",
    "Three Weeks",
    "Two Weeks",
)

_SECTION_HEADINGS: Tuple[str, ...] = (
    "DME Sarasota",
    "Equipment Needed",
    "For First Time Coaches",
    "More Than a Mom",
    "NM Rapids SC Overview",
    "Oasis for the Senses",
    "Premier ID Sessions",
    "Premier Tryouts",
)


def _build_gazetteer() -> FrozenSet[str]:
    """Return a case-folded, whitespace-trimmed frozenset for O(1)
    match lookups. Built once at import time.
    """
    combined = (
        _NAV_LABELS
        + _MARKETING_TILES
        + _PRICING_AND_DATES
        + _SECTION_HEADINGS
    )
    return frozenset(s.strip().casefold() for s in combined)


GAZETTEER: FrozenSet[str] = _build_gazetteer()


# Kept in this order for CLI output stability; additions append.
GAZETTEER_CATEGORIES: Tuple[str, ...] = (
    "nav_label",
    "marketing_tile",
    "pricing_or_date",
    "section_heading",
)


def _category_of(folded: str) -> str:
    """Return the documentary category for a folded string. Used only
    for CLI sample output — the flag metadata records the raw matched
    string, not the category, so category drift doesn't break the audit
    trail.
    """
    if folded in frozenset(s.strip().casefold() for s in _NAV_LABELS):
        return "nav_label"
    if folded in frozenset(s.strip().casefold() for s in _MARKETING_TILES):
        return "marketing_tile"
    if folded in frozenset(s.strip().casefold() for s in _PRICING_AND_DATES):
        return "pricing_or_date"
    if folded in frozenset(s.strip().casefold() for s in _SECTION_HEADINGS):
        return "section_heading"
    return "unknown"


# ---------------------------------------------------------------------------
# Matcher
# ---------------------------------------------------------------------------

def classify_ui_fragment(raw_name: Any) -> Optional[str]:
    """Return the matched category code (`nav_label`, `marketing_tile`,
    `pricing_or_date`, `section_heading`) if `raw_name` is a gazetteer
    hit, or None if it isn't.

    Tolerates non-string inputs (None / int / etc.) — they always return
    None since they can't match a gazetteer string.
    """
    if not isinstance(raw_name, str):
        return None
    folded = raw_name.strip().casefold()
    if not folded or folded not in GAZETTEER:
        return None
    return _category_of(folded)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@dataclass
class DetectorStats:
    """Per-run counters + samples. Surface shape mirrors
    `coach_pollution_detector.DetectorStats` one-for-one so shared
    dashboards can switch on flag_type without branching on schema.
    """

    discoveries_scanned: int = 0
    discoveries_flagged: int = 0
    flags_inserted: int = 0
    flags_skipped_existing: int = 0  # ON CONFLICT DO NOTHING hits
    category_counts: Dict[str, int] = field(default_factory=dict)
    # (discovery_id, raw_name, matched_category)
    sample_flags: List[Tuple[int, str, str]] = field(default_factory=list)

    def record_category(self, category: str) -> None:
        self.category_counts[category] = self.category_counts.get(category, 0) + 1

    def to_details(self) -> dict:
        return {
            "discoveries_scanned": self.discoveries_scanned,
            "discoveries_flagged": self.discoveries_flagged,
            "flags_inserted": self.flags_inserted,
            "flags_skipped_existing": self.flags_skipped_existing,
            "category_counts": dict(self.category_counts),
            "sample_flags": [
                {
                    "discovery_id": s[0],
                    "raw_name": s[1],
                    "matched_category": s[2],
                }
                for s in self.sample_flags[:10]
            ],
        }


# ---------------------------------------------------------------------------
# DB layer — shared shape with coach_pollution_detector; DELIBERATELY
# not factored into a common module. The second-wave detector is
# narrower (gazetteer-based) and we want to be able to deprecate it
# independently once first-wave + better scraper-side guards subsume
# the residual pollution. One module, one flag type, one lifecycle.
# ---------------------------------------------------------------------------

PAGE_SIZE = 1000


def _table_exists(cur, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
    row = cur.fetchone()
    return row is not None and row[0] is not None


def _iter_discoveries(
    cur,
    window_days: Optional[int],
    limit: Optional[int],
):
    """Yield (id, name, email) tuples from `coach_discoveries` in id
    order. See `coach_pollution_detector._iter_discoveries` for the full
    rationale — short version: pages with `fetchmany(PAGE_SIZE)`, and
    the caller MUST use a separate cursor for any writes or psycopg2's
    default client-side cursor will wipe the SELECT's buffered rows.
    """
    if window_days is not None:
        where = "WHERE first_seen_at >= NOW() - (%s || ' days')::interval"
        params: Tuple[Any, ...] = (str(int(window_days)),)
    else:
        where = ""
        params = ()

    limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""

    sql = (
        "SELECT id, name, email FROM coach_discoveries "
        f"{where} ORDER BY id{limit_sql}"
    )
    cur.execute(sql, params if params else None)

    while True:
        batch = cur.fetchmany(PAGE_SIZE)
        if not batch:
            break
        for row in batch:
            yield row


def _upsert_flag(
    cur,
    discovery_id: int,
    matched_raw: str,
    matched_category: str,
    raw_email: Optional[str],
) -> bool:
    """Insert a `coach_quality_flags` row of type `ui_fragment_as_name`.
    Returns True on fresh insert, False on ON CONFLICT DO NOTHING hit.

    Metadata contract:

        {
          matched_raw:      string,  // the raw display_name that matched
          matched_category: string,  // documentary: nav_label / marketing_tile / ...
          raw_email:        string|null
        }
    """
    metadata = {
        "matched_raw": matched_raw,
        "matched_category": matched_category,
        "raw_email": raw_email,
    }
    cur.execute(
        """
        INSERT INTO coach_quality_flags
            (discovery_id, flag_type, metadata, flagged_at)
        VALUES (%s, 'ui_fragment_as_name', %s::jsonb, NOW())
        ON CONFLICT ON CONSTRAINT coach_quality_flags_discovery_type_uq
        DO NOTHING
        RETURNING id
        """,
        (discovery_id, Json(metadata) if Json is not None else metadata),
    )
    row = cur.fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def detect_all(
    conn,
    commit: bool = False,
    limit: Optional[int] = None,
    window_days: Optional[int] = None,
) -> DetectorStats:
    """Scan `coach_discoveries`, match each row's `name` against the
    UI-fragment gazetteer, and (when `commit=True`) upsert a
    `coach_quality_flags` row for every hit.

    Safety model is identical to `coach_pollution_detector.detect_all`:
    separate read/write cursors so the paging SELECT isn't clobbered;
    no-op on missing `coach_quality_flags` table; rollback on dry-run.
    """
    stats = DetectorStats()

    with conn.cursor() as read_cur:
        if not _table_exists(read_cur, "coach_quality_flags"):
            log.warning(
                "coach_quality_flags table does not exist — "
                "run `pnpm --filter @workspace/db run push` on Replit. "
                "Skipping detector."
            )
            return stats

        # Separate write cursor — see module docstring + read_cur note.
        with conn.cursor() as write_cur:
            for discovery_id, name, email in _iter_discoveries(
                read_cur, window_days=window_days, limit=limit
            ):
                stats.discoveries_scanned += 1

                category = classify_ui_fragment(name)
                if category is None:
                    continue

                stats.discoveries_flagged += 1
                stats.record_category(category)

                raw_name = (
                    name if isinstance(name, str)
                    else ("" if name is None else str(name))
                )
                raw_email = (
                    email if (isinstance(email, str) or email is None)
                    else str(email)
                )

                if len(stats.sample_flags) < 10:
                    stats.sample_flags.append(
                        (discovery_id, raw_name, category)
                    )

                if not commit:
                    continue

                inserted = _upsert_flag(
                    write_cur,
                    discovery_id=discovery_id,
                    matched_raw=raw_name,
                    matched_category=category,
                    raw_email=raw_email,
                )
                if inserted:
                    stats.flags_inserted += 1
                else:
                    stats.flags_skipped_existing += 1

        if commit:
            conn.commit()
        else:
            conn.rollback()

    return stats


# ---------------------------------------------------------------------------
# CLI entry point — mirrors coach_pollution_detector.run_cli.
# ---------------------------------------------------------------------------

def run_cli(
    commit: bool = False,
    limit: Optional[int] = None,
    window_days: Optional[int] = None,
) -> int:
    """Entry point for `python3 run.py --source coach-ui-fragment-detect`.
    Returns the process exit code (0 on success, 1 on DB unavailable).
    """
    # Lazy-import scraper-local deps so module import doesn't error at
    # test time.
    from scrape_run_logger import ScrapeRunLogger, FailureKind
    from alerts import alert_scraper_failure

    if psycopg2 is None:
        log.error("psycopg2 not installed")
        return 1

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log.error(
            "DATABASE_URL is not set — cannot run coach-ui-fragment detector"
        )
        return 1

    run_log: Optional[ScrapeRunLogger] = None
    if commit:
        # Only write a scrape_run_logs row on real (committing) runs.
        run_log = ScrapeRunLogger(
            scraper_key="coach-ui-fragment-detect",
            league_name="coach-ui-fragment-detection",
        )
        run_log.start(source_url="derived:coach_quality_flags")

    try:
        conn = psycopg2.connect(db_url)
        try:
            stats = detect_all(
                conn,
                commit=commit,
                limit=limit,
                window_days=window_days,
            )
        finally:
            conn.close()
    except Exception as exc:
        log.error("coach-ui-fragment detector failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(FailureKind.UNKNOWN, str(exc))
        alert_scraper_failure(
            scraper_key="coach-ui-fragment-detect",
            failure_kind=FailureKind.UNKNOWN.value,
            error_message=str(exc),
            source_url="derived:coach_quality_flags",
            league_name="coach-ui-fragment-detection",
        )
        return 1

    _print_summary(stats, commit=commit)

    if run_log is not None:
        import json
        details = stats.to_details()
        details_json = json.dumps(details)[:3900]
        run_log.finish_ok(
            records_created=stats.flags_inserted,
            records_updated=0,
        )
        log.info("coach-ui-fragment-details: %s", details_json)

    return 0


def _print_summary(stats: DetectorStats, commit: bool) -> None:
    mode = "commit" if commit else "dry-run"
    print(f"coach-ui-fragment-detect ({mode}):")
    print(f"  gazetteer size      : {len(GAZETTEER)}")
    print(f"  discoveries scanned : {stats.discoveries_scanned}")
    print(f"  discoveries flagged : {stats.discoveries_flagged}")
    if commit:
        print(f"  flags inserted      : {stats.flags_inserted}")
        print(f"  flags skipped (dup) : {stats.flags_skipped_existing}")

    if stats.category_counts:
        print("  category breakdown:")
        # Surface in canonical order for stable output.
        for category in GAZETTEER_CATEGORIES:
            count = stats.category_counts.get(category, 0)
            if count:
                print(f"    {category:18} {count}")
        for category, count in stats.category_counts.items():
            if category not in GAZETTEER_CATEGORIES and count:
                print(f"    {category:18} {count}  (unknown category)")

    if stats.sample_flags:
        print("  sample flagged rows (up to 10):")
        for discovery_id, raw_name, category in stats.sample_flags:
            truncated = raw_name if len(raw_name) <= 60 else raw_name[:57] + "..."
            print(f"    id={discovery_id} category={category} name={truncated!r}")


# ---------------------------------------------------------------------------
# argparse entry (standalone: `python3 coach_ui_fragment_detector.py`)
# ---------------------------------------------------------------------------

def _main() -> int:  # pragma: no cover — exercised via run.py
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Scan coach_discoveries for UI-fragment pollution (nav "
            "labels, pricing tiers, section headings mistaken for "
            "coach names). Writes coach_quality_flags rows of type "
            "ui_fragment_as_name. Dry-run by default — pass --commit "
            "to actually write."
        )
    )
    parser.add_argument(
        "--commit", action="store_true",
        help="Actually write flag rows. Default is dry-run.",
    )
    parser.add_argument(
        "--limit", type=int, metavar="N", default=None,
        help="Cap the number of discoveries scanned (for testing).",
    )
    parser.add_argument(
        "--window-days", type=int, metavar="N", dest="window_days",
        default=None,
        help=(
            "Only scan discoveries with first_seen_at >= NOW() - N days. "
            "Omit to scan every row (default)."
        ),
    )
    args = parser.parse_args()

    return run_cli(
        commit=args.commit,
        limit=args.limit,
        window_days=args.window_days,
    )


if __name__ == "__main__":  # pragma: no cover
    import sys
    sys.exit(_main())
