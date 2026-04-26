"""Detect nav-menu strings (e.g. "Home", "Contact") leaking into
`club_roster_snapshots.player_name` and upsert one
`roster_quality_flags` row of type `nav_leaked_name` per offending
snapshot-group: (club_name_raw, season, age_group, gender).

Match is case-insensitive on the FULL normalized player_name (not
substring). Idempotent via the (snapshot_id, flag_type) unique
constraint.

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
    re-scans after a nav-word-list expansion or historical-bug
    investigation, not routine operation.

CLI: python3 run.py --source nav-leaked-names-detect \\
        [--dry-run] [--limit N] [--full-scan]
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Tuple

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import Json  # type: ignore
except ImportError:  # pragma: no cover — tested envs have psycopg2
    psycopg2 = None  # type: ignore
    Json = None  # type: ignore

log = logging.getLogger("nav_leaked_names_detector")


# ---------------------------------------------------------------------------
# Nav-word list — 39 strings. Case-insensitive exact match on full
# normalized player_name. Sourced from the Phase 2 task brief (nav
# tokens observed in the wild across roster scrapes — site main nav
# menus, footers, social-media labels, calls-to-action).
# ---------------------------------------------------------------------------

NAV_WORDS: Tuple[str, ...] = (
    "Home",
    "About",
    "About Us",
    "Contact",
    "Contact Us",
    "News",
    "Events",
    "Calendar",
    "Teams",
    "Coaches",
    "Staff",
    "Roster",
    "Rosters",
    "Schedule",
    "Schedules",
    "Standings",
    "Results",
    "Tryouts",
    "Register",
    "Registration",
    "Login",
    "Sign In",
    "Sign Up",
    "Subscribe",
    "Donate",
    "Shop",
    "Store",
    "Sponsors",
    "Partners",
    "Gallery",
    "Photos",
    "Videos",
    "Media",
    "Facilities",
    "Programs",
    "Camps",
    "Clinics",
    "FAQ",
    "Sitemap",
)

# Lowercase set for O(1) case-insensitive membership tests.
_NAV_WORDS_CASEFOLD = frozenset(w.casefold() for w in NAV_WORDS)


def is_nav_word(value: str) -> bool:
    """True iff value (full string, case-folded) matches the nav-word list."""
    if not isinstance(value, str):
        return False
    return value.strip().casefold() in _NAV_WORDS_CASEFOLD


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
    leaked_strings_seen: int = 0
    sample_flags: List[Tuple[str, str, str, str, List[str]]] = field(
        default_factory=list
    )  # (club_name_raw, season, age_group, gender, leaked_strings)

    def to_details(self) -> dict:
        return {
            "snapshot_groups_scanned": self.snapshot_groups_scanned,
            "snapshot_groups_flagged": self.snapshot_groups_flagged,
            "flags_inserted": self.flags_inserted,
            "flags_updated": self.flags_updated,
            "rows_scanned": self.rows_scanned,
            "leaked_strings_seen": self.leaked_strings_seen,
            "sample_flags": [
                {
                    "club_name_raw": s[0],
                    "season": s[1],
                    "age_group": s[2],
                    "gender": s[3],
                    "leaked_strings": s[4],
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

# Keyset pagination batch size. The detector streams
# `club_roster_snapshots` in fixed-size pages so memory stays bounded
# regardless of corpus size — see module docstring. 50k rows × ~6
# small string columns ≈ tens of MB peak, well within the
# scheduled-job container's headroom.
DEFAULT_BATCH_SIZE = 50_000


def _iter_snapshot_rows(
    cur,
    limit: Optional[int],
    full_scan: bool = False,
    window_days: int = DEFAULT_WINDOW_DAYS,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Iterator[Tuple[int, str, str, str, str, str]]:
    """
    Yield (id, club_name_raw, season, age_group, gender, player_name)
    for snapshot rows, ordered by id, using keyset pagination on `id`.

    By default only rows with `scraped_at >= NOW() - window_days` are
    returned — see module docstring for the window rationale. Pass
    `full_scan=True` to skip the window filter (operator escape hatch
    for one-off historical re-scans).

    `limit` caps the total row count for smoke tests; production
    nightly runs leave it None. Pages are fetched lazily so the
    detector never materializes the whole corpus in memory.
    """
    if full_scan:
        base_sql = (
            "SELECT id, club_name_raw, season, age_group, gender, player_name "
            "FROM club_roster_snapshots "
            "WHERE id > %s "
            "ORDER BY id "
            "LIMIT %s"
        )
        base_params: Tuple[Any, ...] = ()
    else:
        base_sql = (
            "SELECT id, club_name_raw, season, age_group, gender, player_name "
            "FROM club_roster_snapshots "
            "WHERE scraped_at >= NOW() - (%s || ' days')::interval "
            "AND id > %s "
            "ORDER BY id "
            "LIMIT %s"
        )
        base_params = (str(int(window_days)),)

    last_id = 0
    yielded = 0
    while True:
        # Shrink the page on the final batch when a global `limit` is
        # set, so we never overshoot.
        page_size = batch_size
        if limit is not None:
            remaining = limit - yielded
            if remaining <= 0:
                return
            page_size = min(batch_size, remaining)

        cur.execute(base_sql, base_params + (last_id, page_size))
        page = cur.fetchall()
        if not page:
            return
        for row in page:
            yield row
            yielded += 1
        last_id = page[-1][0]
        if len(page) < page_size:
            return


def _upsert_flag(
    cur,
    snapshot_id: int,
    leaked_strings: List[str],
    snapshot_roster_size: int,
) -> str:
    """
    Insert or refresh a `roster_quality_flags` row of type
    `nav_leaked_name` for the given snapshot. Returns 'inserted' or
    'updated' for stats accounting.

    Idempotency: ON CONFLICT on the (snapshot_id, flag_type) unique
    constraint refreshes ONLY the `metadata` column when the new payload
    differs from the stored one. `resolved_at` and `resolved_by` are
    intentionally NOT touched — an operator's prior triage stays
    stamped even if a future detector pass observes a slightly
    different leak set on the same snapshot. The `WHERE … IS DISTINCT
    FROM` predicate further suppresses no-op writes on metadata-equal
    re-runs.
    """
    metadata = {
        "leaked_strings": leaked_strings,
        "snapshot_roster_size": snapshot_roster_size,
    }
    cur.execute(
        """
        INSERT INTO roster_quality_flags
            (snapshot_id, flag_type, metadata)
        VALUES (%s, 'nav_leaked_name', %s)
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
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> DetectorStats:
    """
    Main entry point. Streams `club_roster_snapshots` in keyset-paged
    batches (memory bounded by group count, not row count), groups by
    (club_name_raw, season, age_group, gender), and upserts one
    `roster_quality_flags` row per group whose player_name column
    contains nav-word leaks.

    Group accumulators that span batch boundaries are handled
    correctly: the iterator yields rows in `id` order across
    arbitrarily many pages, and each group's per-group accumulator
    (leaked-set, roster size, max snapshot_id) is updated incrementally
    as each row arrives.

    Args:
        conn: open psycopg2 connection.
        dry_run: don't write — populate stats only.
        limit: cap on snapshot rows fetched (smoke testing).
        full_scan: skip the default `scraped_at` window and scan every
            snapshot row. Intended for one-off re-scans after the
            nav-word list expands; routine nightly runs leave this
            False.
        window_days: size of the incremental window when `full_scan`
            is False. Defaults to `DEFAULT_WINDOW_DAYS` (7).
        batch_size: keyset-pagination page size. Defaults to
            `DEFAULT_BATCH_SIZE` (50k); tests override to small values
            to force batch-boundary scenarios.
    """
    stats = DetectorStats()

    # Per-group accumulators. Bounded in memory by the number of
    # distinct (club, season, age_group, gender) groups in the scan
    # window — orders of magnitude smaller than the row count.
    #
    # leaked_set is a dict-as-ordered-set keyed by the case-folded
    # player_name so "Home" / "HOME" / "home" collapse to one entry;
    # the value preserves the first-seen original casing for display.
    accumulators: Dict[
        GroupKey, Dict[str, Any]
    ] = {}

    with conn.cursor() as cur:
        for row in _iter_snapshot_rows(
            cur,
            limit,
            full_scan=full_scan,
            window_days=window_days,
            batch_size=batch_size,
        ):
            row_id, club_name_raw, season, age_group, gender, player_name = row
            stats.rows_scanned += 1

            key: GroupKey = (club_name_raw, season, age_group, gender)
            acc = accumulators.get(key)
            if acc is None:
                acc = {
                    "max_id": row_id,
                    "roster_size": 0,
                    "leaked_set": {},  # dict-as-ordered-set, case-folded keys
                }
                accumulators[key] = acc
            else:
                # Track the NEWEST snapshot in the group so the flag attaches
                # to the most-recent leaked roster, not the oldest — operators
                # browsing recent flagged snapshots in the admin UI need to see
                # the latest occurrence.
                if row_id > acc["max_id"]:
                    acc["max_id"] = row_id
            acc["roster_size"] += 1

            if is_nav_word(player_name):
                # Case-fold the dedup key so "Home"/"HOME"/"home" collapse;
                # preserve the first-seen original casing as the value for
                # human-readable audit output.
                stripped = player_name.strip()
                acc["leaked_set"].setdefault(stripped.casefold(), stripped)

        stats.snapshot_groups_scanned = len(accumulators)

        for key, acc in accumulators.items():
            leaked_set: Dict[str, str] = acc["leaked_set"]
            if not leaked_set:
                continue

            stats.snapshot_groups_flagged += 1
            leaked_strings = list(leaked_set.values())
            stats.leaked_strings_seen += len(leaked_strings)
            roster_size = acc["roster_size"]
            representative_snapshot_id = acc["max_id"]

            if len(stats.sample_flags) < 10:
                stats.sample_flags.append(
                    (key[0], key[1], key[2], key[3], leaked_strings)
                )

            if dry_run:
                continue

            outcome = _upsert_flag(
                cur,
                representative_snapshot_id,
                leaked_strings,
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
# CLI entry point — mirrors canonical_club_linker.run_cli shape.
# ---------------------------------------------------------------------------

def run_cli(
    dry_run: bool = False,
    limit: Optional[int] = None,
    full_scan: bool = False,
) -> int:
    """
    Entry point for `python run.py --source nav-leaked-names-detect`.
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
        log.error("DATABASE_URL is not set — cannot run nav-leaked detector")
        return 1

    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key="nav-leaked-names-detect",
            league_name="nav-leaked-names-detection",
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
        log.error("nav-leaked-names detector failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(FailureKind.UNKNOWN, str(exc))
        alert_scraper_failure(
            scraper_key="nav-leaked-names-detect",
            failure_kind=FailureKind.UNKNOWN.value,
            error_message=str(exc),
            source_url="derived:roster_quality_flags",
            league_name="nav-leaked-names-detection",
        )
        return 1

    print(
        f"nav-leaked-names: scanned {stats.rows_scanned} rows in "
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
        log.info("nav-leaked-names-details: %s", details_json)

    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys
    sys.exit(run_cli())
