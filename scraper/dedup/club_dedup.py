"""
club_dedup.py — Fuzzy dedup utility for canonical_clubs.

Groups clubs by state, then compares names within each state using
Levenshtein distance and token-set similarity. Pairs above a
configurable threshold are flagged as potential duplicates.

Default behaviour is REPORT-ONLY — no auto-merge, stdout only. Pass
``--persist`` to also queue pending pairs into ``club_duplicates`` for the
admin review UI. Persistence is idempotent: the table enforces ordered-pair
uniqueness via ``LEAST(left_club_id, right_club_id),
GREATEST(left_club_id, right_club_id)``, and the writer uses
``ON CONFLICT DO NOTHING`` so re-running the sweep (including with flipped
pair ordering) will never double-queue a pair.

Usage:
    python -m dedup.club_dedup [--threshold 0.85] [--dry-run] [--state GA]
                               [--persist]

Run the sweep on Replit cron with ``--persist`` to feed the admin dedup
review queue; operators doing local dry-runs should omit the flag so the
queue isn't polluted with noise.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    import psycopg2  # type: ignore
except ImportError:
    psycopg2 = None  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("club_dedup")


@dataclass
class DedupPair:
    club_a_id: int
    club_a_name: str
    club_b_id: int
    club_b_name: str
    state: str
    similarity: float
    match_reason: str


# ---------------------------------------------------------------------------
# String similarity utilities
# ---------------------------------------------------------------------------

def _levenshtein_distance(s1: str, s2: str) -> int:
    """Compute Levenshtein edit distance between two strings."""
    if len(s1) < len(s2):
        return _levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)

    prev_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = prev_row[j + 1] + 1
            deletions = curr_row[j] + 1
            substitutions = prev_row[j] + (c1 != c2)
            curr_row.append(min(insertions, deletions, substitutions))
        prev_row = curr_row

    return prev_row[-1]


def levenshtein_similarity(s1: str, s2: str) -> float:
    """Compute normalized Levenshtein similarity (0-1)."""
    if not s1 and not s2:
        return 1.0
    max_len = max(len(s1), len(s2))
    if max_len == 0:
        return 1.0
    dist = _levenshtein_distance(s1, s2)
    return 1.0 - (dist / max_len)


# Common abbreviation expansions for soccer club names
_ABBREVIATIONS = {
    "fc": "football club",
    "sc": "soccer club",
    "utd": "united",
    "cty": "city",
    "ac": "athletic club",
    "cf": "club de futbol",
    "sa": "soccer academy",
    "ya": "youth academy",
}

# Noise words to strip for token comparison
_NOISE_WORDS = frozenset({
    "soccer", "club", "fc", "sc", "youth", "academy", "the",
    "of", "and", "&", "inc", "llc",
})


def _canonicalize(name: str) -> str:
    """Lowercase, expand abbreviations, strip noise."""
    tokens = name.lower().split()
    # Step 1: expand abbreviations
    expanded = []
    for t in tokens:
        t_clean = t.strip(".,()-")
        if not t_clean:
            continue
        if t_clean in _ABBREVIATIONS:
            expanded.extend(_ABBREVIATIONS[t_clean].split())
        else:
            expanded.append(t_clean)
    # Step 2: strip noise words
    result = [t for t in expanded if t not in _NOISE_WORDS]
    return " ".join(result)


def token_set_similarity(s1: str, s2: str) -> float:
    """Jaccard similarity on token sets after canonicalization."""
    t1 = set(_canonicalize(s1).split())
    t2 = set(_canonicalize(s2).split())
    if not t1 and not t2:
        return 1.0
    if not t1 or not t2:
        return 0.0
    intersection = t1 & t2
    union = t1 | t2
    return len(intersection) / len(union)


def combined_similarity(s1: str, s2: str) -> float:
    """Combine Levenshtein and token-set similarity (weighted average)."""
    lev = levenshtein_similarity(s1.lower(), s2.lower())
    tok = token_set_similarity(s1, s2)
    # Weight token-set higher because club name variations tend to be
    # reorderings/abbreviations rather than single-char edits.
    return 0.4 * lev + 0.6 * tok


# ---------------------------------------------------------------------------
# DB access
# ---------------------------------------------------------------------------

def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def _fetch_all_clubs(conn, *, state_filter: Optional[str] = None) -> List[Dict]:
    """Fetch all canonical_clubs (id, name, state)."""
    query = "SELECT id, club_name_canonical, state FROM canonical_clubs ORDER BY state, id"
    params: list = []
    if state_filter:
        query = (
            "SELECT id, club_name_canonical, state FROM canonical_clubs "
            "WHERE UPPER(state) = UPPER(%s) ORDER BY id"
        )
        params = [state_filter]

    with conn.cursor() as cur:
        cur.execute(query, params)
        return [
            {"id": row[0], "name": row[1], "state": row[2] or ""}
            for row in cur.fetchall()
        ]


# ---------------------------------------------------------------------------
# Dedup logic
# ---------------------------------------------------------------------------

def find_duplicate_pairs(
    clubs: List[Dict],
    threshold: float = 0.85,
) -> List[DedupPair]:
    """Compare clubs within the same state and flag potential duplicates."""
    # Group by state
    by_state: Dict[str, List[Dict]] = defaultdict(list)
    for c in clubs:
        by_state[c["state"].upper() if c["state"] else "UNKNOWN"].append(c)

    pairs: List[DedupPair] = []

    for state, state_clubs in by_state.items():
        n = len(state_clubs)
        if n < 2:
            continue

        logger.debug("Comparing %d clubs in state=%s", n, state)
        for i in range(n):
            for j in range(i + 1, n):
                a = state_clubs[i]
                b = state_clubs[j]
                sim = combined_similarity(a["name"], b["name"])
                if sim >= threshold:
                    # Order by id for stable pair representation
                    if a["id"] > b["id"]:
                        a, b = b, a
                    pairs.append(DedupPair(
                        club_a_id=a["id"],
                        club_a_name=a["name"],
                        club_b_id=b["id"],
                        club_b_name=b["name"],
                        state=state,
                        similarity=sim,
                        match_reason=f"fuzzy_name (lev={levenshtein_similarity(a['name'].lower(), b['name'].lower()):.2f}, tok={token_set_similarity(a['name'], b['name']):.2f})",
                    ))

    # Sort by similarity descending
    pairs.sort(key=lambda p: p.similarity, reverse=True)
    return pairs


# ---------------------------------------------------------------------------
# Persistence — club_duplicates review queue
# ---------------------------------------------------------------------------

# Method label written to club_duplicates.method. Kept as a constant so the
# admin UI can filter by it and the test suite can assert on it.
DEFAULT_METHOD = "name_fuzzy_88"


def upsert_pending_duplicate(
    cur,
    left_id: int,
    right_id: int,
    score: float,
    method: str,
    left_snapshot: Dict[str, Any],
    right_snapshot: Dict[str, Any],
) -> None:
    """Queue a pending duplicate pair.

    Idempotent — the table's ordered-pair unique index on
    ``(LEAST(left_club_id, right_club_id), GREATEST(left_club_id, right_club_id))``
    collapses flipped pairs, and ``ON CONFLICT DO NOTHING`` means re-running
    the sweep never double-queues a row.
    """
    cur.execute(
        """
        INSERT INTO club_duplicates (
            left_club_id, right_club_id, score, method, status,
            left_snapshot, right_snapshot
        )
        VALUES (%s, %s, %s, %s, 'pending', %s::jsonb, %s::jsonb)
        ON CONFLICT (
            (LEAST(left_club_id, right_club_id)),
            (GREATEST(left_club_id, right_club_id))
        ) DO NOTHING
        """,
        (
            left_id,
            right_id,
            score,
            method,
            json.dumps(left_snapshot),
            json.dumps(right_snapshot),
        ),
    )


def _snapshot_from_pair(pair: DedupPair, side: str) -> Dict[str, Any]:
    """Build a denormalized snapshot for one side of a pair."""
    if side == "left":
        return {
            "id": pair.club_a_id,
            "name": pair.club_a_name,
            "state": pair.state,
        }
    return {
        "id": pair.club_b_id,
        "name": pair.club_b_name,
        "state": pair.state,
    }


def persist_pending_duplicates(
    conn,
    pairs: List[DedupPair],
    *,
    method: str = DEFAULT_METHOD,
) -> int:
    """Persist all pairs to ``club_duplicates``; return rows attempted.

    Actual inserts may be fewer due to ON CONFLICT DO NOTHING on re-runs.
    """
    attempted = 0
    with conn.cursor() as cur:
        for p in pairs:
            upsert_pending_duplicate(
                cur,
                left_id=p.club_a_id,
                right_id=p.club_b_id,
                score=p.similarity,
                method=method,
                left_snapshot=_snapshot_from_pair(p, "left"),
                right_snapshot=_snapshot_from_pair(p, "right"),
            )
            attempted += 1
    conn.commit()
    return attempted


def print_report(pairs: List[DedupPair]) -> None:
    """Print a formatted dedup report to stdout."""
    print("\n" + "=" * 80)
    print("  Club Fuzzy Dedup Report")
    print("=" * 80)
    print(f"  Potential duplicate pairs found: {len(pairs)}")
    print()

    if not pairs:
        print("  No duplicates found above threshold.")
        print("=" * 80)
        return

    for i, p in enumerate(pairs, 1):
        print(f"  {i:>3}. [{p.state}] similarity={p.similarity:.3f}")
        print(f"       A: id={p.club_a_id:<6}  {p.club_a_name}")
        print(f"       B: id={p.club_b_id:<6}  {p.club_b_name}")
        print(f"       reason: {p.match_reason}")
        print()

    print("=" * 80)


def run_club_dedup(
    *,
    threshold: float = 0.85,
    dry_run: bool = False,
    state: Optional[str] = None,
    persist: bool = False,
    method: str = DEFAULT_METHOD,
) -> List[DedupPair]:
    """Run the fuzzy dedup analysis.

    When ``persist`` is True, candidate pairs are written to
    ``club_duplicates`` with ``status='pending'`` for admin review. The
    queue is idempotent — repeat runs hit ``ON CONFLICT DO NOTHING``.
    """
    conn = _get_connection()
    try:
        clubs = _fetch_all_clubs(conn, state_filter=state)

        logger.info("Loaded %d clubs for dedup analysis", len(clubs))

        if dry_run:
            logger.info(
                "[dry-run] Would compare clubs with threshold=%.2f", threshold
            )
            return []

        pairs = find_duplicate_pairs(clubs, threshold=threshold)

        if persist and pairs:
            attempted = persist_pending_duplicates(conn, pairs, method=method)
            logger.info(
                "Queued %d candidate pair(s) into club_duplicates "
                "(duplicates from prior sweeps skipped via ON CONFLICT)",
                attempted,
            )
        elif persist:
            logger.info("No pairs to persist.")

        return pairs
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fuzzy dedup utility for canonical_clubs",
    )
    parser.add_argument(
        "--threshold", type=float, default=0.85,
        help="Similarity threshold (0-1, default 0.85)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be compared, no analysis",
    )
    parser.add_argument(
        "--state", metavar="ST",
        help="Only compare clubs in this state (e.g. GA, CA)",
    )
    parser.add_argument(
        "--persist", action="store_true",
        help=(
            "Queue candidate pairs into club_duplicates as status='pending'. "
            "Idempotent via ordered-pair unique index + ON CONFLICT DO "
            "NOTHING. Default off so dry-runs don't pollute the review queue."
        ),
    )
    args = parser.parse_args()

    pairs = run_club_dedup(
        threshold=args.threshold,
        dry_run=args.dry_run,
        state=args.state,
        persist=args.persist,
    )
    print_report(pairs)


if __name__ == "__main__":
    main()
