"""Detect `player_name` values that consist entirely of digits,
separators, and whitespace leaking into `club_roster_snapshots` and
upsert one `roster_quality_flags` row of type `numeric_only_name` per
offending snapshot-group: (club_name_raw, season, age_group, gender).

Common sources of these rows:
  - Scraper misparsed the jersey-number column as the name column
    (e.g. player_name = "14")
  - A date cell landed where a name should be
    (e.g. player_name = "2024-05-15" or "5/15")
  - Extractor wrote an empty / whitespace-only cell

Idempotent via the (snapshot_id, flag_type) unique constraint.

Incremental scan window:
    By default the detector restricts the scan to
    `club_roster_snapshots` rows whose `scraped_at` is within the last
    7 days. This keeps nightly runs bounded as the snapshots table
    grows past the 10M-row scale. 7 days covers the slowest weekly
    scraper cadence AND tolerates up to ~6 consecutive missed nightly
    runs (Replit hiccup, transient DB unavailability). Idempotency of
    the upsert (ON CONFLICT + WHERE metadata IS DISTINCT FROM) makes
    re-scanning the same snapshot across consecutive windows a cheap
    no-op, so overlap is free.

    Use `--full-scan` to ignore the window — intended for one-time
    re-scans after a heuristic change or historical-bug investigation,
    not routine operation.

CLI: python3 run.py --source numeric-only-names-detect \\
        [--dry-run] [--limit N] [--full-scan]
"""

from __future__ import annotations

import logging
import os
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import Json  # type: ignore
except ImportError:  # pragma: no cover — tested envs have psycopg2
    psycopg2 = None  # type: ignore
    Json = None  # type: ignore

log = logging.getLogger("numeric_only_name_detector")


# ---------------------------------------------------------------------------
# Matcher — full-string match on digits + separators + whitespace.
#
# After `.strip()`, the entire `player_name` must consist ONLY of digits,
# forward slashes, hyphens, dots, and whitespace for it to be flagged.
# This covers:
#   - bare jersey numbers ("14", "007")
#   - ISO dates ("2024-05-15")
#   - US-format dates ("5/15/2024", "5/15")
#   - numeric ranges / ids ("12.5", "1 2 3")
#   - empty / whitespace-only strings (scraper wrote a blank cell)
#
# Deliberately does NOT flag strings with `#`, letters, or other
# punctuation — "Tom 14" / "Jane Smith" / "# 14" don't match.
# ---------------------------------------------------------------------------

_NUMERIC_ONLY_RE = re.compile(r"^[\d/\-.\s]+$")


def is_numeric_only_name(value: str) -> bool:
    """True iff value, after `.strip()`, is empty OR consists entirely of
    digits, '/', '-', '.', and whitespace characters."""
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    if stripped == "":
        # Empty / all-whitespace input — scraper wrote a blank cell.
        # Still a bug worth flagging; match defensively even though
        # `player_name` is NOT NULL in the schema (empty strings can
        # still slip in).
        return True
    return _NUMERIC_ONLY_RE.match(stripped) is not None


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@dataclass
class DetectorStats:
    snapshot_groups_scanned: int = 0
    snapshot_groups_flagged: int = 0
    flags_inserted: int = 0
    flags_updated: int = 0
    rows_scanned: int = 0
    numeric_strings_seen: int = 0
    sample_flags: List[Tuple[str, str, str, str, List[str]]] = field(
        default_factory=list
    )  # (club_name_raw, season, age_group, gender, numeric_strings)

    def to_details(self) -> dict:
        return {
            "snapshot_groups_scanned": self.snapshot_groups_scanned,
            "snapshot_groups_flagged": self.snapshot_groups_flagged,
            "flags_inserted": self.flags_inserted,
            "flags_updated": self.flags_updated,
            "rows_scanned": self.rows_scanned,
            "numeric_strings_seen": self.numeric_strings_seen,
            "sample_flags": [
                {
                    "club_name_raw": s[0],
                    "season": s[1],
                    "age_group": s[2],
                    "gender": s[3],
                    "numeric_strings": s[4],
                }
                for s in self.sample_flags[:10]
            ],
        }


# ---------------------------------------------------------------------------
# DB layer
# ---------------------------------------------------------------------------

# Group key: (club_name_raw, season, age_group, gender). All four are NOT NULL
# on `club_roster_snapshots` so no COALESCE / sentinel handling is needed.
GroupKey = Tuple[str, str, str, str]


# Default incremental scan window, in days. Covers the slowest
# weekly-cadence scraper plus headroom for multiple consecutive missed
# nightly runs. See module docstring for rationale.
DEFAULT_WINDOW_DAYS = 7


def _fetch_snapshot_rows(
    cur,
    limit: Optional[int],
    full_scan: bool = False,
    window_days: int = DEFAULT_WINDOW_DAYS,
) -> List[Tuple[int, str, str, str, str, str]]:
    """
    Return (id, club_name_raw, season, age_group, gender, player_name)
    for snapshot rows, ordered by id.

    By default only rows with `scraped_at >= NOW() - window_days` are
    returned — see module docstring for the window rationale. Pass
    `full_scan=True` to skip the window filter (operator escape hatch
    for one-off historical re-scans).

    `limit` caps the row count for smoke tests; production nightly
    runs leave it None.
    """
    if full_scan:
        sql = (
            "SELECT id, club_name_raw, season, age_group, gender, player_name "
            "FROM club_roster_snapshots "
            "ORDER BY id"
        )
        params: Tuple[Any, ...] = ()
    else:
        sql = (
            "SELECT id, club_name_raw, season, age_group, gender, player_name "
            "FROM club_roster_snapshots "
            "WHERE scraped_at >= NOW() - (%s || ' days')::interval "
            "ORDER BY id"
        )
        params = (str(int(window_days)),)
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql, params if params else None)
    return list(cur.fetchall())


def _upsert_flag(
    cur,
    snapshot_id: int,
    numeric_strings: List[str],
    snapshot_roster_size: int,
) -> str:
    """
    Insert or refresh a `roster_quality_flags` row of type
    `numeric_only_name` for the given snapshot. Returns 'inserted',
    'updated', or 'noop' for stats accounting.

    Idempotency: ON CONFLICT on the (snapshot_id, flag_type) unique
    constraint refreshes ONLY the `metadata` column when the new payload
    differs from the stored one. `resolved_at` and `resolved_by` are
    intentionally NOT touched — an operator's prior triage stays
    stamped even if a future detector pass observes a slightly
    different offending set on the same snapshot. The `WHERE … IS
    DISTINCT FROM` predicate further suppresses no-op writes on
    metadata-equal re-runs.
    """
    metadata = {
        "numeric_strings": numeric_strings,
        "snapshot_roster_size": snapshot_roster_size,
    }
    cur.execute(
        """
        INSERT INTO roster_quality_flags
            (snapshot_id, flag_type, metadata)
        VALUES (%s, 'numeric_only_name', %s)
        ON CONFLICT ON CONSTRAINT roster_quality_flags_snapshot_type_uq
        DO UPDATE SET metadata = EXCLUDED.metadata
        WHERE roster_quality_flags.metadata IS DISTINCT FROM EXCLUDED.metadata
        RETURNING (xmax = 0) AS inserted
        """,
        (snapshot_id, Json(metadata) if Json is not None else metadata),
    )
    row = cur.fetchone()
    if row is None:
        # Conflict + WHERE filtered out (metadata unchanged) — neither
        # insert nor update fired. Count as a no-op (= update for
        # idempotency-tracking purposes).
        return "noop"
    inserted = bool(row[0])
    return "inserted" if inserted else "updated"


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def detect_all(
    conn,
    dry_run: bool = False,
    limit: Optional[int] = None,
    full_scan: bool = False,
    window_days: int = DEFAULT_WINDOW_DAYS,
) -> DetectorStats:
    """
    Main entry point. Scans `club_roster_snapshots`, groups by
    (club_name_raw, season, age_group, gender), and upserts one
    `roster_quality_flags` row per group whose player_name column
    contains numeric-only leaks.

    Args:
        conn: open psycopg2 connection.
        dry_run: don't write — populate stats only.
        limit: cap on snapshot rows fetched (smoke testing).
        full_scan: skip the default `scraped_at` window and scan every
            snapshot row. Intended for one-off re-scans; routine
            nightly runs leave this False.
        window_days: size of the incremental window when `full_scan`
            is False. Defaults to `DEFAULT_WINDOW_DAYS` (7).
    """
    stats = DetectorStats()

    with conn.cursor() as cur:
        rows = _fetch_snapshot_rows(
            cur, limit, full_scan=full_scan, window_days=window_days
        )
        stats.rows_scanned = len(rows)

        # Group rows by (club_name_raw, season, age_group, gender).
        groups: Dict[GroupKey, List[Tuple[int, str]]] = defaultdict(list)
        for row_id, club_name_raw, season, age_group, gender, player_name in rows:
            key: GroupKey = (club_name_raw, season, age_group, gender)
            groups[key].append((row_id, player_name))

        stats.snapshot_groups_scanned = len(groups)

        for key, members in groups.items():
            offending_set: Dict[str, None] = {}
            for _row_id, player_name in members:
                if is_numeric_only_name(player_name):
                    # Preserve original (stripped) form of the offending
                    # player_name for operator review.
                    offending_set.setdefault(
                        player_name.strip() if isinstance(player_name, str) else "",
                        None,
                    )

            if not offending_set:
                continue

            stats.snapshot_groups_flagged += 1
            numeric_strings = list(offending_set.keys())
            stats.numeric_strings_seen += len(numeric_strings)
            roster_size = len(members)

            # Reference the smallest snapshot_id in the group as the
            # canonical row the flag points at — stable across runs and
            # the natural choice when the panel needs "one snapshot to
            # show" for the flag.
            representative_snapshot_id = min(r[0] for r in members)

            if len(stats.sample_flags) < 10:
                stats.sample_flags.append(
                    (key[0], key[1], key[2], key[3], numeric_strings)
                )

            if dry_run:
                continue

            outcome = _upsert_flag(
                cur,
                representative_snapshot_id,
                numeric_strings,
                roster_size,
            )
            if outcome == "inserted":
                stats.flags_inserted += 1
            elif outcome == "updated":
                stats.flags_updated += 1
            # 'noop' (metadata unchanged) — no counter bump.

        if dry_run:
            conn.rollback()
        else:
            conn.commit()

    return stats


# ---------------------------------------------------------------------------
# CLI entry point — mirrors nav_leaked_names_detector.run_cli shape.
# ---------------------------------------------------------------------------

def run_cli(
    dry_run: bool = False,
    limit: Optional[int] = None,
    full_scan: bool = False,
) -> int:
    """
    Entry point for `python run.py --source numeric-only-names-detect`.
    Returns the process exit code (0 on success, 1 on DB unavailable).

    `full_scan` maps to the CLI `--full-scan` flag; see module
    docstring for when to use it.
    """
    from scrape_run_logger import ScrapeRunLogger, FailureKind
    from alerts import alert_scraper_failure

    if psycopg2 is None:
        log.error("psycopg2 not installed")
        return 1

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log.error(
            "DATABASE_URL is not set — cannot run numeric-only-name detector"
        )
        return 1

    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key="numeric-only-names-detect",
            league_name="numeric-only-names-detection",
        )
        run_log.start(source_url="derived:roster_quality_flags")

    try:
        conn = psycopg2.connect(db_url)
        try:
            stats = detect_all(
                conn,
                dry_run=dry_run,
                limit=limit,
                full_scan=full_scan,
            )
        finally:
            conn.close()
    except Exception as exc:
        log.error("numeric-only-name detector failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(FailureKind.UNKNOWN, str(exc))
        alert_scraper_failure(
            scraper_key="numeric-only-names-detect",
            failure_kind=FailureKind.UNKNOWN.value,
            error_message=str(exc),
            source_url="derived:roster_quality_flags",
            league_name="numeric-only-names-detection",
        )
        return 1

    print(
        f"numeric-only-names: scanned {stats.rows_scanned} rows in "
        f"{stats.snapshot_groups_scanned} snapshot groups; "
        f"flagged {stats.snapshot_groups_flagged} groups "
        f"({stats.flags_inserted} inserted, {stats.flags_updated} updated)."
    )

    if run_log is not None:
        import json
        details = stats.to_details()
        details_json = json.dumps(details)[:3900]
        run_log.finish_ok(
            records_created=stats.flags_inserted,
            records_updated=stats.flags_updated,
        )
        log.info("numeric-only-names-details: %s", details_json)

    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys
    sys.exit(run_cli())
