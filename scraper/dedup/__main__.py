"""
dedup/__main__.py — orchestrate the tiered dedup resolver.

Loads canonical_clubs from the live DB, runs the fuzzy detector
(``club_dedup``), classifies pairs into tiers (``canonical_club_merger``),
auto-merges the high-confidence tier, and writes the review tier to a
CSV the operator can hand-resolve.

DEFAULTS TO DRY-RUN. ``--no-dry-run`` is required to commit auto-merges.

CLI (the canonical entry point is ``run.py --source club-dedup-resolve``;
this module is wired through there but can also be invoked as
``python3 -m dedup`` for local testing):

    python3 -m dedup [--threshold 0.85] [--state GA] [--no-dry-run]
                     [--review-csv path]
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from typing import List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    import psycopg2  # type: ignore
except ImportError:
    psycopg2 = None  # type: ignore

from dedup.club_dedup import (  # noqa: E402
    DedupPair,
    _fetch_all_clubs,
    find_duplicate_pairs,
)
from dedup.canonical_club_merger import (  # noqa: E402
    AUTO_MERGE_SIMILARITY,
    REVIEW_MIN_SIMILARITY,
    MergeResult,
    TieredPair,
    fetch_club_meta,
    merge_canonical_clubs,
    tier_pairs,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dedup_resolve")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _connect():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def _default_review_csv_path() -> str:
    """``scraper/dedup/output/dedup-review-<YYYY-MM-DD>.csv``"""
    out_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(out_dir, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return os.path.join(out_dir, f"dedup-review-{today}.csv")


def write_review_csv(
    review_pairs: List[TieredPair],
    meta_by_id: dict,
    path: str,
) -> str:
    """
    Write the review-tier pairs to CSV. Columns match the spec in the
    BACKLOG #2 task description so an operator can sort/filter in a
    spreadsheet.
    """
    fieldnames = [
        "id_a", "name_a", "state_a",
        "id_b", "name_b", "state_b",
        "similarity", "recommendation", "reasoning",
    ]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for tp in review_pairs:
            p = tp.pair
            a = meta_by_id.get(p.club_a_id)
            b = meta_by_id.get(p.club_b_id)
            w.writerow({
                "id_a": p.club_a_id,
                "name_a": p.club_a_name,
                "state_a": (a.state if a else "") or "",
                "id_b": p.club_b_id,
                "name_b": p.club_b_name,
                "state_b": (b.state if b else "") or "",
                "similarity": f"{p.similarity:.4f}",
                "recommendation": tp.tier,
                "reasoning": tp.reasoning,
            })
    return path


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_resolve(
    *,
    threshold: float = REVIEW_MIN_SIMILARITY,
    state: Optional[str] = None,
    dry_run: bool = True,
    review_csv: Optional[str] = None,
) -> dict:
    """
    Scan canonical_clubs, tier the pairs, auto-merge the safe tier, and
    write the review CSV. Returns a summary dict.

    Designed to be call-safe from `run.py` — never raises on routine
    conditions; logs and degrades.
    """
    summary: dict = {
        "tiers": {"auto_merge": 0, "review": 0, "skip": 0},
        "merges_attempted": 0,
        "merges_committed": 0,
        "merges_skipped": 0,
        "merges_failed": 0,
        "review_csv": None,
        "dry_run": dry_run,
    }

    conn = _connect()
    try:
        clubs = _fetch_all_clubs(conn, state_filter=state)
        log.info("Loaded %d canonical_clubs", len(clubs))

        pairs: List[DedupPair] = find_duplicate_pairs(clubs, threshold=threshold)
        log.info("Detected %d candidate pairs at threshold %.2f", len(pairs), threshold)

        # Pull metadata for every club referenced by any pair, in one round trip.
        ids_in_pairs: set = set()
        for p in pairs:
            ids_in_pairs.add(p.club_a_id)
            ids_in_pairs.add(p.club_b_id)
        meta = fetch_club_meta(conn, list(ids_in_pairs))

        tiered = tier_pairs(pairs, meta)
        for t in tiered:
            summary["tiers"][t.tier] += 1

        log.info(
            "Tier breakdown — auto_merge=%d review=%d skip=%d (auto >= %.2f, review >= %.2f)",
            summary["tiers"]["auto_merge"],
            summary["tiers"]["review"],
            summary["tiers"]["skip"],
            AUTO_MERGE_SIMILARITY,
            REVIEW_MIN_SIMILARITY,
        )

        # Auto-merge tier-1 pairs.
        auto_pairs = [t for t in tiered if t.tier == "auto_merge"]
        for tp in auto_pairs:
            summary["merges_attempted"] += 1
            assert tp.recommended_winner_id is not None
            winner_id = tp.recommended_winner_id
            loser_id = (
                tp.pair.club_b_id
                if winner_id == tp.pair.club_a_id
                else tp.pair.club_a_id
            )
            res: MergeResult = merge_canonical_clubs(
                loser_id=loser_id,
                winner_id=winner_id,
                conn=conn,
                dry_run=dry_run,
            )
            if res.skipped:
                summary["merges_skipped"] += 1
                log.warning(
                    "[merge skipped] loser=%s winner=%s reason=%s",
                    loser_id, winner_id, res.skip_reason,
                )
            elif res.error:
                summary["merges_failed"] += 1
                log.error(
                    "[merge failed] loser=%s winner=%s error=%s",
                    loser_id, winner_id, res.error,
                )
            elif res.committed:
                summary["merges_committed"] += 1
                log.info(
                    "[merge committed] loser=%s -> winner=%s redirects=%s deletes=%s",
                    loser_id, winner_id,
                    sum(res.rows_redirected.values()),
                    sum(res.rows_deleted_from_loser.values()),
                )
            else:
                # dry_run path
                log.info(
                    "[merge dry-run] would merge loser=%s -> winner=%s "
                    "redirects=%s deletes=%s",
                    loser_id, winner_id,
                    sum(res.rows_redirected.values()),
                    sum(res.rows_deleted_from_loser.values()),
                )

        # Write review CSV.
        review_pairs = [t for t in tiered if t.tier == "review"]
        if review_pairs:
            path = review_csv or _default_review_csv_path()
            written = write_review_csv(review_pairs, meta, path)
            summary["review_csv"] = written
            log.info("Review tier (%d pairs) written to %s", len(review_pairs), written)
        else:
            log.info("No pairs landed in review tier — CSV skipped.")

        return summary
    finally:
        try:
            conn.close()
        except Exception:
            pass


def print_summary(summary: dict) -> None:
    print("\n" + "=" * 70)
    print("  Canonical-clubs dedup resolver")
    print("=" * 70)
    print(f"  Mode               : {'dry-run' if summary['dry_run'] else 'COMMIT'}")
    print(f"  Tier auto_merge    : {summary['tiers']['auto_merge']}")
    print(f"  Tier review        : {summary['tiers']['review']}")
    print(f"  Tier skip          : {summary['tiers']['skip']}")
    print(f"  Merges attempted   : {summary['merges_attempted']}")
    print(f"  Merges committed   : {summary['merges_committed']}")
    print(f"  Merges skipped     : {summary['merges_skipped']}")
    print(f"  Merges failed      : {summary['merges_failed']}")
    if summary.get("review_csv"):
        print(f"  Review CSV         : {summary['review_csv']}")
    print("=" * 70)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Tiered canonical_clubs dedup resolver. Defaults to dry-run.",
    )
    parser.add_argument(
        "--threshold", type=float, default=REVIEW_MIN_SIMILARITY,
        help=f"Detection threshold (0-1, default {REVIEW_MIN_SIMILARITY})",
    )
    parser.add_argument(
        "--state", metavar="ST",
        help="Restrict to a single state (e.g. GA, CA)",
    )
    parser.add_argument(
        "--no-dry-run", action="store_true",
        help=(
            "Actually commit auto-merge tier merges. Without this flag the "
            "resolver runs in dry-run mode (rolls back every merge). "
            "REQUIRED for any DB mutation."
        ),
    )
    parser.add_argument(
        "--review-csv", metavar="PATH",
        help="Override the review-tier CSV path.",
    )
    args = parser.parse_args()

    try:
        summary = run_resolve(
            threshold=args.threshold,
            state=args.state,
            dry_run=not args.no_dry_run,
            review_csv=args.review_csv,
        )
    except RuntimeError as exc:
        # Local-dev path — DATABASE_URL is unset. Fail loud but with a
        # readable message; the spec calls this acceptable.
        log.error("Cannot connect to DB: %s", exc)
        return 1
    except Exception as exc:  # pragma: no cover
        log.error("Resolver crashed: %s", exc)
        return 2

    print_summary(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
