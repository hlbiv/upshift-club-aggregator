"""
club_dedup.py — Fuzzy dedup utility for canonical_clubs.

Groups clubs by state, then compares names within each state using
Levenshtein distance and token-set similarity. Pairs above a
configurable threshold are flagged as potential duplicates.

Output is REPORT-ONLY — no auto-merge. Writes to stdout.

Usage:
    python -m dedup.club_dedup [--threshold 0.85] [--dry-run] [--state GA]
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

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
) -> List[DedupPair]:
    """Run the fuzzy dedup analysis."""
    conn = _get_connection()
    try:
        clubs = _fetch_all_clubs(conn, state_filter=state)
    finally:
        conn.close()

    logger.info("Loaded %d clubs for dedup analysis", len(clubs))

    if dry_run:
        logger.info("[dry-run] Would compare clubs with threshold=%.2f", threshold)
        return []

    pairs = find_duplicate_pairs(clubs, threshold=threshold)
    return pairs


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
    args = parser.parse_args()

    pairs = run_club_dedup(
        threshold=args.threshold,
        dry_run=args.dry_run,
        state=args.state,
    )
    print_report(pairs)


if __name__ == "__main__":
    main()
