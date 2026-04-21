"""Detect coach-name pollution in `coach_discoveries` and upsert one
`coach_quality_flags` row of type `looks_like_name_reject` per offending
discovery.

PR 2 of the 3-PR coach-pollution remediation sequence:

  PR 1 (#191, merged): extracted `looks_like_name` into
    `scraper/extractors/_coach_name_guard.py` so every scraper writing
    into `coach_discoveries` can validate names consistently.
  PR 3 (#188, merged): added the `coach_quality_flags` table + admin
    read/resolve API as the audit trail for this remediation.
  PR 2 (this module): a one-off historical flagger that walks every row
    already present in `coach_discoveries`, runs each `name` through the
    shared guard, and records the rejects as flags.

Scope — FLAG ONLY, NO DELETION. `coach_quality_flags.discovery_id` has
`ON DELETE CASCADE`, so a one-pass "flag-then-delete" would lose the
audit trail (deleting the discovery would cascade-delete its flag). The
actual purge is a separate follow-up PR that snapshots the offending
discoveries to a JSONL dump before deleting.

Idempotent via the (discovery_id, flag_type) unique constraint.

CLI: python3 run.py --source coach-pollution-detect \\
        [--commit] [--limit N] [--window-days N]

Note the default here is DRY-RUN: pass `--commit` to actually write the
flags. This inverts the `--dry-run` convention of `nav-leaked-names-detect`
and `numeric-only-names-detect` on purpose — a historical-scan-of-every-row
job is a higher-blast-radius operation and the safe default is "just show
me what you would do".
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import Json  # type: ignore
except ImportError:  # pragma: no cover — tested envs have psycopg2
    psycopg2 = None  # type: ignore
    Json = None  # type: ignore

# Guard module MUST exist — PR #191 shipped it. If the import fails we
# intentionally let the ImportError propagate out of the module so a
# broken environment is loud, not silently-no-op.
from extractors._coach_name_guard import (  # noqa: E402
    REJECT_REASONS,
    RejectCounter,
    looks_like_name,
)

log = logging.getLogger("coach_pollution_detector")


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@dataclass
class DetectorStats:
    """Per-run counters + samples.

    Summaries drive the CLI's stdout output AND feed `scrape_run_logs`
    via `to_details()` when this detector runs via the in-tree CLI.
    """

    discoveries_scanned: int = 0
    discoveries_flagged: int = 0
    flags_inserted: int = 0
    flags_skipped_existing: int = 0  # ON CONFLICT DO NOTHING hits
    reject_reason_counts: Dict[str, int] = field(default_factory=dict)
    # (discovery_id, raw_name, reject_reason)
    sample_flags: List[Tuple[int, str, str]] = field(default_factory=list)

    def record_reject(self, reason: str) -> None:
        self.reject_reason_counts[reason] = (
            self.reject_reason_counts.get(reason, 0) + 1
        )

    def to_details(self) -> dict:
        return {
            "discoveries_scanned": self.discoveries_scanned,
            "discoveries_flagged": self.discoveries_flagged,
            "flags_inserted": self.flags_inserted,
            "flags_skipped_existing": self.flags_skipped_existing,
            "reject_reason_counts": dict(self.reject_reason_counts),
            "sample_flags": [
                {
                    "discovery_id": s[0],
                    "raw_name": s[1],
                    "reject_reason": s[2],
                }
                for s in self.sample_flags[:10]
            ],
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _classify_reject(raw_name: Any) -> Optional[str]:
    """Run the guard against `raw_name` and return the rejection reason
    code if it fails, or None if the name passes.

    `looks_like_name` is side-effect-only on the counter — the counter
    increments at most one reason per call (first-match-wins). We use a
    fresh counter per row so the reason we read back is unambiguous.

    `raw_name` is tolerated as any type: NULL / non-string inputs are
    coerced to the empty string and will reject as `too_short`.
    """
    if raw_name is None:
        text = ""
    elif not isinstance(raw_name, str):
        text = str(raw_name)
    else:
        text = raw_name

    counter = RejectCounter()
    if looks_like_name(text, counter):
        return None

    # Exactly one reason was recorded (first failing check short-circuits).
    summary = counter.summary()
    if not summary:
        # Shouldn't happen — `looks_like_name` always records a reason
        # on reject when given a counter. Fall back to a sentinel rather
        # than KeyError so the detector doesn't crash on an unexpected
        # guard change.
        return "unknown"
    # Pick the reason with count > 0 (there's only ever one per call).
    for reason, count in summary.items():
        if count > 0:
            return reason
    return "unknown"


def _table_exists(cur, table: str) -> bool:
    """True iff `table` is a regclass-addressable object in the current
    search_path. Used for the no-op-on-missing-table guard at startup so
    this PR can merge before Replit runs `pnpm --filter @workspace/db
    run push` to materialize PR #188's schema.
    """
    cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
    row = cur.fetchone()
    return row is not None and row[0] is not None


# ---------------------------------------------------------------------------
# DB layer
# ---------------------------------------------------------------------------

# Page size for `coach_discoveries` scan. ~10 bytes/row for the three
# selected columns; a 1000-row page is ~10KB of network, comfortably
# below psycopg2's default fetch-many buffer.
PAGE_SIZE = 1000


def _iter_discoveries(
    cur,
    window_days: Optional[int],
    limit: Optional[int],
):
    """Yield (id, name, email) tuples from `coach_discoveries` in id
    order. Optionally restricted to rows whose `first_seen_at` is within
    the last `window_days` days, and/or capped at `limit` rows total.

    Uses a server-side cursor so we don't materialize the entire table
    in memory — `coach_discoveries` is ~several-thousand rows today but
    the detector should scale to whatever the table grows into.
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
    reject_reason: str,
    raw_name: str,
    raw_email: Optional[str],
) -> bool:
    """Insert a `coach_quality_flags` row for `discovery_id` with type
    `looks_like_name_reject`. Returns True on fresh insert, False when
    the (discovery_id, flag_type) pair already exists and the ON
    CONFLICT DO NOTHING branch fired.

    Metadata shape is the contract documented in
    `lib/db/src/schema/coach-quality-flags.ts`, extended with `raw_email`
    for triage-UI convenience (jsonb accepts additional keys):

        { reject_reason: <code>, raw_name: <str>, raw_email: <str|null> }
    """
    metadata = {
        "reject_reason": reject_reason,
        "raw_name": raw_name,
        "raw_email": raw_email,
    }
    cur.execute(
        """
        INSERT INTO coach_quality_flags
            (discovery_id, flag_type, metadata, flagged_at)
        VALUES (%s, 'looks_like_name_reject', %s::jsonb, NOW())
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
    """Scan `coach_discoveries`, run each row's `name` through the
    shared `looks_like_name` guard, and (when `commit=True`) upsert a
    `coach_quality_flags` row for every reject.

    Args:
        conn: open psycopg2 connection.
        commit: when False (default) no writes happen; the transaction
            is rolled back before returning so the caller can re-run
            against the same DB without side effects.
        limit: cap on discoveries scanned (smoke testing).
        window_days: if set, only consider discoveries whose
            `first_seen_at` is within the last N days. None = all rows.

    Returns a `DetectorStats` with per-reason counts + a sample slice.
    """
    stats = DetectorStats()

    with conn.cursor() as cur:
        # No-op on missing table. This lets the PR merge before Replit
        # runs `pnpm --filter @workspace/db run push` to apply PR #188's
        # schema; the detector simply reports "table missing" and exits
        # cleanly so a scheduled invocation doesn't error-page.
        if not _table_exists(cur, "coach_quality_flags"):
            log.warning(
                "coach_quality_flags table does not exist — "
                "run `pnpm --filter @workspace/db run push` on Replit. "
                "Skipping detector."
            )
            return stats

        for discovery_id, name, email in _iter_discoveries(
            cur, window_days=window_days, limit=limit
        ):
            stats.discoveries_scanned += 1

            reason = _classify_reject(name)
            if reason is None:
                continue

            stats.discoveries_flagged += 1
            stats.record_reject(reason)

            raw_name = name if isinstance(name, str) else ("" if name is None else str(name))
            raw_email = email if (isinstance(email, str) or email is None) else str(email)

            if len(stats.sample_flags) < 10:
                stats.sample_flags.append((discovery_id, raw_name, reason))

            if not commit:
                continue

            inserted = _upsert_flag(
                cur,
                discovery_id=discovery_id,
                reject_reason=reason,
                raw_name=raw_name,
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
# CLI entry point — mirrors nav_leaked_names_detector.run_cli shape.
# ---------------------------------------------------------------------------

def run_cli(
    commit: bool = False,
    limit: Optional[int] = None,
    window_days: Optional[int] = None,
) -> int:
    """Entry point for `python3 run.py --source coach-pollution-detect`.
    Returns the process exit code (0 on success, 1 on DB unavailable).
    """
    # Import inside the function so a missing scraper-local dependency
    # doesn't break module import at test time.
    from scrape_run_logger import ScrapeRunLogger, FailureKind
    from alerts import alert_scraper_failure

    if psycopg2 is None:
        log.error("psycopg2 not installed")
        return 1

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log.error(
            "DATABASE_URL is not set — cannot run coach-pollution detector"
        )
        return 1

    run_log: Optional[ScrapeRunLogger] = None
    if commit:
        # Only write a scrape_run_logs row on real (committing) runs.
        # Dry-run invocations are a read-only smoke test and shouldn't
        # pollute the health-dashboard timeline.
        run_log = ScrapeRunLogger(
            scraper_key="coach-pollution-detect",
            league_name="coach-pollution-detection",
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
        log.error("coach-pollution detector failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(FailureKind.UNKNOWN, str(exc))
        alert_scraper_failure(
            scraper_key="coach-pollution-detect",
            failure_kind=FailureKind.UNKNOWN.value,
            error_message=str(exc),
            source_url="derived:coach_quality_flags",
            league_name="coach-pollution-detection",
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
        log.info("coach-pollution-details: %s", details_json)

    return 0


def _print_summary(stats: DetectorStats, commit: bool) -> None:
    """Print a human-readable summary to stdout. Both dry-run and
    commit modes print the same shape — operators eyeball the
    reject-reason breakdown + sample before flipping `--commit`.
    """
    mode = "commit" if commit else "dry-run"
    print(f"coach-pollution-detect ({mode}):")
    print(f"  discoveries scanned : {stats.discoveries_scanned}")
    print(f"  discoveries flagged : {stats.discoveries_flagged}")
    if commit:
        print(f"  flags inserted      : {stats.flags_inserted}")
        print(f"  flags skipped (dup) : {stats.flags_skipped_existing}")

    if stats.reject_reason_counts:
        print("  reject-reason breakdown:")
        # REJECT_REASONS is the canonical order; surface in that order
        # so the output is stable across runs.
        for reason in REJECT_REASONS:
            count = stats.reject_reason_counts.get(reason, 0)
            if count:
                print(f"    {reason:22} {count}")
        # Any reason not in REJECT_REASONS (defensive — shouldn't happen).
        for reason, count in stats.reject_reason_counts.items():
            if reason not in REJECT_REASONS and count:
                print(f"    {reason:22} {count}  (unknown reason code)")

    if stats.sample_flags:
        print("  sample flagged rows (up to 10):")
        for discovery_id, raw_name, reason in stats.sample_flags:
            truncated = raw_name if len(raw_name) <= 60 else raw_name[:57] + "..."
            print(f"    id={discovery_id} reason={reason} name={truncated!r}")


# ---------------------------------------------------------------------------
# argparse entry (standalone invocation: `python3 coach_pollution_detector.py`)
# ---------------------------------------------------------------------------

def _main() -> int:  # pragma: no cover — exercised via run.py
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Scan coach_discoveries and flag rows whose `name` fails the "
            "shared looks_like_name guard. Writes coach_quality_flags rows "
            "of type looks_like_name_reject. Dry-run by default — pass "
            "--commit to actually write."
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
