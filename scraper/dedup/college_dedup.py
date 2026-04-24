"""
college_dedup.py — Fuzzy dedup utility for colleges.

Groups colleges by (division, gender_program), then compares names within
each group using Levenshtein distance and token-set similarity. Pairs above
a configurable threshold are flagged as potential duplicates.

Primary use case: D1 womens programs were seeded from multiple scrapers with
slightly different name spellings (~468 rows → ~205 actual programs).

Default behaviour is REPORT-ONLY — no auto-merge, stdout only. Pass
``--persist`` to also queue pending pairs into ``college_duplicates`` for the
admin review UI. Persistence is idempotent: the table enforces ordered-pair
uniqueness via ``LEAST(left_college_id, right_college_id),
GREATEST(left_college_id, right_college_id)``, and the writer uses
``ON CONFLICT DO NOTHING`` so re-running the sweep (including with flipped
pair ordering) will never double-queue a pair.

Usage:
    python -m dedup.college_dedup [--threshold 0.92] [--dry-run]
                                  [--division D1] [--gender womens]
                                  [--persist]

Run the sweep on Replit with ``--persist`` to feed the admin dedup review
queue; operators doing local dry-runs should omit the flag so the queue
isn't polluted with noise.
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
logger = logging.getLogger("college_dedup")


@dataclass
class DedupPair:
    college_a_id: int
    college_a_name: str
    college_b_id: int
    college_b_name: str
    division: str
    gender_program: str
    similarity: float
    match_reason: str


# ---------------------------------------------------------------------------
# String similarity utilities (mirrored from club_dedup.py)
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


# Noise words to strip for token comparison (college-specific)
_NOISE_WORDS = frozenset({
    "university", "college", "state", "the", "of", "and", "&",
    "at", "in", "a", "an",
})


def _canonicalize(name: str) -> str:
    """Lowercase and strip noise words for token comparison."""
    tokens = name.lower().split()
    cleaned = []
    for t in tokens:
        t_clean = t.strip(".,()-")
        if not t_clean:
            continue
        if t_clean not in _NOISE_WORDS:
            cleaned.append(t_clean)
    return " ".join(cleaned)


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


def _is_prefix_extension(a: str, b: str) -> bool:
    """Return True if one name is just the other with extra words appended.

    Catches: "South Carolina" vs "South Carolina State"
             "North Dakota" vs "North Dakota State"
             "Jacksonville" vs "Jacksonville State"
    """
    a_tokens = a.lower().split()
    b_tokens = b.lower().split()
    shorter, longer = (a_tokens, b_tokens) if len(a_tokens) <= len(b_tokens) else (b_tokens, a_tokens)
    return longer[:len(shorter)] == shorter


def combined_similarity(s1: str, s2: str) -> float:
    """Combine Levenshtein and token-set similarity (weighted average).

    Weight token-set higher because college name variations (e.g. "UNC" vs
    "University of North Carolina", "NC State" vs "North Carolina State")
    are typically reorderings/abbreviations rather than single-char edits.
    """
    lev = levenshtein_similarity(s1.lower(), s2.lower())
    tok = token_set_similarity(s1, s2)
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


def _fetch_all_colleges(
    conn,
    *,
    division_filter: Optional[str] = None,
    gender_filter: Optional[str] = None,
) -> List[Dict]:
    """Fetch all colleges (id, name, division, gender_program, state, city)."""
    query = (
        "SELECT id, name, division, gender_program, state, city "
        "FROM colleges "
        "ORDER BY division, gender_program, name"
    )
    params: list = []

    conditions = []
    if division_filter:
        conditions.append("UPPER(division) = UPPER(%s)")
        params.append(division_filter)
    if gender_filter:
        conditions.append("LOWER(gender_program) = LOWER(%s)")
        params.append(gender_filter)

    if conditions:
        query = (
            "SELECT id, name, division, gender_program, state, city "
            "FROM colleges "
            "WHERE " + " AND ".join(conditions) + " "
            "ORDER BY division, gender_program, name"
        )

    with conn.cursor() as cur:
        cur.execute(query, params)
        return [
            {
                "id": row[0],
                "name": row[1],
                "division": row[2] or "",
                "gender_program": row[3] or "",
                "state": row[4] or "",
                "city": row[5] or "",
            }
            for row in cur.fetchall()
        ]


# ---------------------------------------------------------------------------
# Dedup logic
# ---------------------------------------------------------------------------

def find_duplicate_pairs(
    colleges: List[Dict],
    threshold: float = 0.92,
) -> List[DedupPair]:
    """Compare colleges within the same (division, gender_program) group."""
    # Group by (division, gender_program) — only meaningful to compare
    # D1 womens against D1 womens, etc.
    by_group: Dict[Tuple[str, str], List[Dict]] = defaultdict(list)
    for c in colleges:
        group_key = (
            c["division"].upper() if c["division"] else "UNKNOWN",
            c["gender_program"].lower() if c["gender_program"] else "unknown",
        )
        by_group[group_key].append(c)

    pairs: List[DedupPair] = []

    for (division, gender), group_colleges in by_group.items():
        n = len(group_colleges)
        if n < 2:
            continue

        logger.debug("Comparing %d colleges in division=%s gender=%s", n, division, gender)
        for i in range(n):
            for j in range(i + 1, n):
                a = group_colleges[i]
                b = group_colleges[j]
                if _is_prefix_extension(a["name"], b["name"]):
                    continue
                sim = combined_similarity(a["name"], b["name"])
                if sim >= threshold:
                    # Order by id for stable pair representation
                    if a["id"] > b["id"]:
                        a, b = b, a
                    lev_score = levenshtein_similarity(
                        a["name"].lower(), b["name"].lower()
                    )
                    tok_score = token_set_similarity(a["name"], b["name"])
                    pairs.append(DedupPair(
                        college_a_id=a["id"],
                        college_a_name=a["name"],
                        college_b_id=b["id"],
                        college_b_name=b["name"],
                        division=division,
                        gender_program=gender,
                        similarity=sim,
                        match_reason=(
                            f"fuzzy_name (lev={lev_score:.2f}, tok={tok_score:.2f})"
                        ),
                    ))

    # Sort by similarity descending
    pairs.sort(key=lambda p: p.similarity, reverse=True)
    return pairs


# ---------------------------------------------------------------------------
# Persistence — college_duplicates review queue
# ---------------------------------------------------------------------------

# Method label written to college_duplicates.method.
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
    ``(LEAST(left_college_id, right_college_id), GREATEST(left_college_id, right_college_id))``
    collapses flipped pairs, and ``ON CONFLICT DO NOTHING`` means re-running
    the sweep never double-queues a row.
    """
    cur.execute(
        """
        INSERT INTO college_duplicates (
            left_college_id, right_college_id, score, method, status,
            left_snapshot, right_snapshot
        )
        VALUES (%s, %s, %s, %s, 'pending', %s::jsonb, %s::jsonb)
        ON CONFLICT (
            (LEAST(left_college_id, right_college_id)),
            (GREATEST(left_college_id, right_college_id))
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
            "id": pair.college_a_id,
            "name": pair.college_a_name,
            "division": pair.division,
            "genderProgram": pair.gender_program,
        }
    return {
        "id": pair.college_b_id,
        "name": pair.college_b_name,
        "division": pair.division,
        "genderProgram": pair.gender_program,
    }


def persist_pending_duplicates(
    conn,
    pairs: List[DedupPair],
    *,
    method: str = DEFAULT_METHOD,
) -> int:
    """Persist all pairs to ``college_duplicates``; return rows attempted.

    Actual inserts may be fewer due to ON CONFLICT DO NOTHING on re-runs.
    """
    attempted = 0
    with conn.cursor() as cur:
        for p in pairs:
            upsert_pending_duplicate(
                cur,
                left_id=p.college_a_id,
                right_id=p.college_b_id,
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
    print("  College Fuzzy Dedup Report")
    print("=" * 80)
    print(f"  Potential duplicate pairs found: {len(pairs)}")
    print()

    if not pairs:
        print("  No duplicates found above threshold.")
        print("=" * 80)
        return

    for i, p in enumerate(pairs, 1):
        print(
            f"  {i:>3}. [{p.division} / {p.gender_program}] "
            f"similarity={p.similarity:.3f}"
        )
        print(f"       A: id={p.college_a_id:<6}  {p.college_a_name}")
        print(f"       B: id={p.college_b_id:<6}  {p.college_b_name}")
        print(f"       reason: {p.match_reason}")
        print()

    print("=" * 80)


def run_college_dedup(
    *,
    threshold: float = 0.92,
    dry_run: bool = False,
    division: Optional[str] = None,
    gender: Optional[str] = None,
    persist: bool = False,
    method: str = DEFAULT_METHOD,
) -> List[DedupPair]:
    """Run the fuzzy dedup analysis.

    When ``persist`` is True, candidate pairs are written to
    ``college_duplicates`` with ``status='pending'`` for admin review. The
    queue is idempotent — repeat runs hit ``ON CONFLICT DO NOTHING``.
    """
    conn = _get_connection()
    try:
        colleges_data = _fetch_all_colleges(
            conn, division_filter=division, gender_filter=gender
        )

        logger.info("Loaded %d colleges for dedup analysis", len(colleges_data))

        if dry_run:
            logger.info(
                "[dry-run] Would compare colleges with threshold=%.2f", threshold
            )
            return []

        pairs = find_duplicate_pairs(colleges_data, threshold=threshold)

        if persist and pairs:
            attempted = persist_pending_duplicates(conn, pairs, method=method)
            logger.info(
                "Queued %d candidate pair(s) into college_duplicates "
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
        description="Fuzzy dedup utility for colleges",
    )
    parser.add_argument(
        "--threshold", type=float, default=0.92,
        help="Similarity threshold (0-1, default 0.92)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be compared, no analysis",
    )
    parser.add_argument(
        "--division", metavar="DIV",
        help="Only compare colleges in this division (e.g. D1, D2, D3, NAIA, NJCAA)",
    )
    parser.add_argument(
        "--gender", metavar="GENDER",
        help="Only compare colleges with this gender_program (e.g. mens, womens, both)",
    )
    parser.add_argument(
        "--persist", action="store_true",
        help=(
            "Queue candidate pairs into college_duplicates as status='pending'. "
            "Idempotent via ordered-pair unique index + ON CONFLICT DO "
            "NOTHING. Default off so dry-runs don't pollute the review queue."
        ),
    )
    args = parser.parse_args()

    pairs = run_college_dedup(
        threshold=args.threshold,
        dry_run=args.dry_run,
        division=args.division,
        gender=args.gender,
        persist=args.persist,
    )
    print_report(pairs)


if __name__ == "__main__":
    main()
