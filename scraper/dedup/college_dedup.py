"""
Colleges dedup — find and merge short-name/full-name duplicate pairs.

Common pattern: "Alabama" vs "University of Alabama" in same division/gender/sport.

Usage:
  python3 run.py --rollup college-dedup [--dry-run]

Emits JSONL audit to exports/college_dedup_audit_<timestamp>.jsonl
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from rapidfuzz.distance import JaroWinkler

SIMILARITY_THRESHOLD = 0.90
EXPORTS_DIR = Path(__file__).parent.parent.parent / "exports"

# FK sites to repoint before deleting loser
_FK_SINGLE = [
    ("college_coaches", "college_id"),
    ("college_coach_tenures", "college_id"),
    ("college_roster_history", "college_id"),
    ("college_roster_quality_flags", "college_id"),
    ("commitments", "college_id"),
    ("coach_misses", "college_id"),
]
_FK_MULTI = [
    ("transfer_portal_entries", "from_college_id"),
    ("transfer_portal_entries", "to_college_id"),
]


def _sim(a: str, b: str) -> float:
    return JaroWinkler.similarity(a.lower(), b.lower())


def find_college_duplicate_candidates(conn, threshold: float = SIMILARITY_THRESHOLD) -> list[dict]:
    """Return candidate duplicate pairs within same (division, gender_program, sport)."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, name, division, gender_program, sport FROM colleges ORDER BY id")
        rows = cur.fetchall()

    groups: dict = defaultdict(list)
    for row in rows:
        col_id, name, division, gender, sport = row[0], row[1], row[2], row[3], row[4]
        groups[(division, gender, sport)].append({"id": col_id, "name": name})

    candidates = []
    for (division, gender, sport), colleges in groups.items():
        for i, a in enumerate(colleges):
            for b in colleges[i + 1:]:
                score = _sim(a["name"], b["name"])
                if score >= threshold:
                    winner, loser = (a, b) if a["id"] < b["id"] else (b, a)
                    candidates.append({
                        "winner_id": winner["id"], "winner_name": winner["name"],
                        "loser_id": loser["id"],  "loser_name": loser["name"],
                        "division": division, "gender_program": gender, "sport": sport,
                        "similarity": round(score, 4),
                    })
    return candidates


def merge_college(conn, loser_id: int, winner_id: int) -> dict:
    """Repoint all FK references from loser to winner, then delete loser."""
    stats: dict[str, int] = {}
    with conn.cursor() as cur:
        for table, col in _FK_SINGLE + _FK_MULTI:
            cur.execute(
                f"UPDATE {table} SET {col} = %s WHERE {col} = %s",  # noqa: S608
                (winner_id, loser_id),
            )
            stats[f"{table}.{col}"] = cur.rowcount
        cur.execute("DELETE FROM colleges WHERE id = %s", (loser_id,))
        stats["colleges_deleted"] = cur.rowcount
    conn.commit()
    return stats


def run_college_dedup(conn, dry_run: bool = False, audit_path: Optional[str] = None) -> dict:
    """Find candidates, merge (unless dry_run), write JSONL audit."""
    candidates = find_college_duplicate_candidates(conn)
    print(f"[college-dedup] Found {len(candidates)} candidate pair(s) at threshold={SIMILARITY_THRESHOLD}")

    audit_lines = []
    merged = 0

    for pair in candidates:
        action = "dry_run" if dry_run else "merged"
        entry = {**pair, "action": action, "ts": datetime.now(timezone.utc).isoformat()}
        if dry_run:
            print(f"[dry-run] '{pair['loser_name']}' ({pair['loser_id']}) → '{pair['winner_name']}' ({pair['winner_id']})  sim={pair['similarity']}")
        else:
            merge_stats = merge_college(conn, pair["loser_id"], pair["winner_id"])
            entry["merge_stats"] = merge_stats
            merged += 1
            print(f"[college-dedup] Merged '{pair['loser_name']}' ({pair['loser_id']}) → '{pair['winner_name']}' ({pair['winner_id']})")
        audit_lines.append(entry)

    EXPORTS_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = audit_path or str(EXPORTS_DIR / f"college_dedup_audit_{ts}.jsonl")
    with open(out_path, "w") as f:
        for line in audit_lines:
            f.write(json.dumps(line) + "\n")
    print(f"[college-dedup] Audit written to {out_path}")

    return {"candidates": len(candidates), "merged": merged, "dry_run": dry_run, "audit_path": out_path}
