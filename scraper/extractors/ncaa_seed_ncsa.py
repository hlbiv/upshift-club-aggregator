"""
NCAA URL seed loader — reads curated CSVs and fills colleges.soccer_program_url gaps.

Usage:
  python3 run.py --source ncaa-seed-ncsa [--division D1|D2|D3|NAIA] [--dry-run]

CSV format (scraper/seeds/ncaa_urls_*.csv):
  name,division,gender_program,soccer_program_url,source

Only fills rows where soccer_program_url IS NULL.
Matching: Jaro-Winkler >= 0.88 on name within same (division, gender_program).
"""
from __future__ import annotations

import csv
import glob
from pathlib import Path
from typing import Optional

from rapidfuzz.distance import JaroWinkler

SEEDS_DIR = Path(__file__).parent.parent / "seeds"
SIMILARITY_THRESHOLD = 0.88


def find_all_seed_csvs() -> list[str]:
    return sorted(glob.glob(str(SEEDS_DIR / "ncaa_urls_*.csv")))


def load_seed_csv(path: str) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _name_similarity(a: str, b: str) -> float:
    return JaroWinkler.similarity(a.lower(), b.lower())


def run_ncaa_seed_ncsa(
    conn,
    division: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """
    Load seed CSVs, fuzzy-match to colleges rows with NULL soccer_program_url, fill URLs.
    Returns stats dict.
    """
    seed_files = find_all_seed_csvs()
    if not seed_files:
        print(f"[ncaa-seed-ncsa] No seed CSVs found in {SEEDS_DIR}")
        return {"filled": 0, "matched": 0, "no_match": 0}

    all_seeds: list[dict] = []
    for path in seed_files:
        rows = load_seed_csv(path)
        # Skip header-only CSVs (rows with no soccer_program_url value)
        all_seeds.extend(r for r in rows if r.get("soccer_program_url", "").strip())
    print(f"[ncaa-seed-ncsa] Loaded {len(all_seeds)} seed rows from {len(seed_files)} file(s)")

    # Query colleges with NULL soccer_program_url
    query = "SELECT id, name, division, gender_program FROM colleges WHERE soccer_program_url IS NULL"
    params: list = []
    if division:
        query += " AND division = %s"
        params.append(division)

    with conn.cursor() as cur:
        cur.execute(query, params)
        null_url_colleges = cur.fetchall()

    print(f"[ncaa-seed-ncsa] {len(null_url_colleges)} colleges with NULL soccer_program_url")

    stats = {"filled": 0, "matched": 0, "no_match": 0}

    for row in null_url_colleges:
        col_id, col_name, col_div, col_gender = row[0], row[1], row[2], row[3]

        candidates = [
            s for s in all_seeds
            if s.get("division") == col_div and s.get("gender_program") == col_gender
        ]
        if not candidates:
            stats["no_match"] += 1
            continue

        best: Optional[dict] = None
        best_score = 0.0
        for seed in candidates:
            score = _name_similarity(col_name, seed["name"])
            if score > best_score:
                best_score = score
                best = seed

        if best is None or best_score < SIMILARITY_THRESHOLD:
            stats["no_match"] += 1
            continue

        url = best["soccer_program_url"].strip()
        if not url:
            stats["no_match"] += 1
            continue

        stats["matched"] += 1
        if dry_run:
            print(
                f"[dry-run] {col_name} ({col_div} {col_gender}) → {url}  sim={best_score:.3f}"
            )
        else:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE colleges SET soccer_program_url = %s WHERE id = %s",
                    (url, col_id),
                )
            conn.commit()
            print(f"[ncaa-seed-ncsa] Filled {col_name} ({col_div} {col_gender}) → {url}")
        stats["filled"] += 1

    print(f"[ncaa-seed-ncsa] Done: {stats}")
    return stats
