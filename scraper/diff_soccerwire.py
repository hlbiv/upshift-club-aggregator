"""
SoccerWire net-new diff report.

Compares clubs returned by the SoccerWire extractor against the canonical
clubs already in master.csv for the 8 states where SoccerWire is the source:
  HI, LA, MA, MS, NE, RI, SC, WI

Each SoccerWire club is classified using the same fuzzy-match logic the
normalizer uses (rapidfuzz token_sort_ratio, FUZZY_THRESHOLD=88):

  NET_NEW     — best score < 75   → clearly a new club, safe to append
  NEAR_MATCH  — best score 75–87  → borderline; needs human review
  DUPLICATE   — best score ≥ 88   → already covered by another source

Output written to:
  scraper/output/soccerwire_diff/YYYY-MM-DD/
    summary.md        — Markdown table: one row per state
    HI.csv            — per-state detail
    LA.csv            — …
    …

Usage:
  python3 diff_soccerwire.py                 # all 8 states
  python3 diff_soccerwire.py --state HI      # single state only
  python3 diff_soccerwire.py --output-dir /tmp/my-diff  # custom output dir
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from datetime import date
from typing import Dict, List, Optional, Tuple

from rapidfuzz import fuzz

# ---------------------------------------------------------------------------
# Bootstrap path so we can import from scraper/ when called from any CWD
# ---------------------------------------------------------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

from config import FUZZY_THRESHOLD, MASTER_CSV
from normalizer import _canonical
from extractors.soccerwire import scrape_soccerwire_state

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("diff_soccerwire")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# The 8 states sourced from SoccerWire (Task #22, April 2026)
SOCCERWIRE_STATES: Dict[str, str] = {
    "HI": "Hawaii Soccer Association",
    "LA": "Louisiana Soccer Association",
    "MA": "Massachusetts Youth Soccer Association",
    "MS": "Mississippi Soccer Association",
    "NE": "Nebraska State Soccer Association",
    "RI": "Soccer Rhode Island",
    "SC": "South Carolina Youth Soccer Association",
    "WI": "Wisconsin Youth Soccer Association",
}

# Full state name → abbreviation (for master.csv `state` field matching)
_STATE_FULL_NAMES: Dict[str, str] = {
    "HI": "Hawaii",
    "LA": "Louisiana",
    "MA": "Massachusetts",
    "MS": "Mississippi",
    "NE": "Nebraska",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "WI": "Wisconsin",
}

# Classification thresholds
_NEAR_MATCH_LOW = 75   # below this → NET_NEW
_DUPLICATE_LOW  = FUZZY_THRESHOLD  # at or above this → DUPLICATE

# CSV columns in the per-state detail files
_DETAIL_COLS = [
    "soccerwire_name",
    "canonical_name",
    "best_match_in_master",
    "best_match_score",
    "classification",
    "city",
    "soccerwire_url",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_master_by_state(master_csv: str) -> Dict[str, List[Dict]]:
    """
    Read master.csv and return a dict mapping state abbr → list of row dicts.

    The `state` column in master.csv can hold full names ("Hawaii") or
    abbreviations ("HI") or regional labels ("Cal North"). We index by
    the 2-letter abbreviation for the 8 SoccerWire states only.
    """
    abbr_from_full = {v: k for k, v in _STATE_FULL_NAMES.items()}
    by_state: Dict[str, List[Dict]] = {abbr: [] for abbr in SOCCERWIRE_STATES}

    if not os.path.exists(master_csv):
        logger.warning("master.csv not found at %s — state buckets will be empty", master_csv)
        return by_state

    with open(master_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            state_val = (row.get("state") or "").strip()
            # Try direct abbreviation match first
            if state_val in by_state:
                by_state[state_val].append(row)
            # Try full-name match
            elif state_val in abbr_from_full:
                by_state[abbr_from_full[state_val]].append(row)

    for abbr, rows in by_state.items():
        logger.info("master.csv: %s — %d existing clubs", abbr, len(rows))

    return by_state


def _best_fuzzy_match(
    canonical: str,
    existing_canonicals: List[str],
) -> Tuple[str, int]:
    """
    Return (best_match_name, score) for the highest token_sort_ratio hit.
    Returns ("", 0) if existing_canonicals is empty.
    """
    if not existing_canonicals:
        return ("", 0)
    best_name = ""
    best_score = 0
    for ec in existing_canonicals:
        score = fuzz.token_sort_ratio(canonical, ec)
        if score > best_score:
            best_score = score
            best_name = ec
    return (best_name, best_score)


def _classify(score: int) -> str:
    if score >= _DUPLICATE_LOW:
        return "DUPLICATE"
    if score >= _NEAR_MATCH_LOW:
        return "NEAR_MATCH"
    return "NET_NEW"


# ---------------------------------------------------------------------------
# Core diff logic per state
# ---------------------------------------------------------------------------

def diff_state(
    state_abbr: str,
    league_name: str,
    master_rows: List[Dict],
) -> List[Dict]:
    """
    Fetch SoccerWire clubs for one state, compare against master rows,
    and return a list of result dicts (one per SoccerWire club).
    """
    logger.info("--- Diffing state %s ---", state_abbr)

    # Fetch live SoccerWire clubs for this state
    sw_clubs = scrape_soccerwire_state(state_abbr, league_name)
    if not sw_clubs:
        logger.warning("%s: SoccerWire returned 0 clubs", state_abbr)

    # Build list of canonical names from master for this state
    existing_canonicals = [
        _canonical(r.get("canonical_name") or r.get("club_name", ""))
        for r in master_rows
        if r.get("canonical_name") or r.get("club_name")
    ]
    existing_canonicals = [c for c in existing_canonicals if c]

    results: List[Dict] = []
    for club in sw_clubs:
        sw_name = club.get("club_name", "")
        sw_canonical = _canonical(sw_name)
        best_match, score = _best_fuzzy_match(sw_canonical, existing_canonicals)
        classification = _classify(score)

        results.append({
            "soccerwire_name":   sw_name,
            "canonical_name":    sw_canonical,
            "best_match_in_master": best_match,
            "best_match_score":  score,
            "classification":    classification,
            "city":              club.get("city", ""),
            "soccerwire_url":    club.get("source_url", ""),
        })

    net_new  = sum(1 for r in results if r["classification"] == "NET_NEW")
    near     = sum(1 for r in results if r["classification"] == "NEAR_MATCH")
    dupes    = sum(1 for r in results if r["classification"] == "DUPLICATE")
    logger.info(
        "%s: %d SoccerWire clubs → NET_NEW=%d  NEAR_MATCH=%d  DUPLICATE=%d",
        state_abbr, len(results), net_new, near, dupes,
    )
    return results


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def _write_state_csv(path: str, rows: List[Dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_DETAIL_COLS)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Written %s (%d rows)", path, len(rows))


def _write_summary_md(path: str, state_results: Dict[str, List[Dict]], run_date: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    lines: List[str] = [
        f"# SoccerWire Net-New Diff Report — {run_date}",
        "",
        "Comparison of SoccerWire-sourced clubs against canonical clubs in `master.csv`.",
        f"Fuzzy-match threshold: DUPLICATE ≥ {_DUPLICATE_LOW}, "
        f"NEAR_MATCH {_NEAR_MATCH_LOW}–{_DUPLICATE_LOW - 1}, NET_NEW < {_NEAR_MATCH_LOW}.",
        "",
        "| State | SoccerWire Total | NET_NEW | NEAR_MATCH | DUPLICATE | NEAR_MATCH clubs (review) | DUPLICATE clubs (best canonical match) |",
        "|-------|-----------------|---------|------------|-----------|--------------------------|----------------------------------------|",
    ]

    totals = {"total": 0, "NET_NEW": 0, "NEAR_MATCH": 0, "DUPLICATE": 0}

    for abbr in sorted(state_results.keys()):
        rows = state_results[abbr]
        total   = len(rows)
        net_new = sum(1 for r in rows if r["classification"] == "NET_NEW")
        near    = sum(1 for r in rows if r["classification"] == "NEAR_MATCH")
        dupes   = sum(1 for r in rows if r["classification"] == "DUPLICATE")

        # List NEAR_MATCH clubs with their best master match for human review
        near_clubs = "; ".join(
            f"{r['soccerwire_name']} ≈ '{r['best_match_in_master']}' ({r['best_match_score']:.0f})"
            for r in rows if r["classification"] == "NEAR_MATCH"
        ) or "—"

        # List DUPLICATE clubs with their matching canonical name in master
        dupe_clubs = "; ".join(
            f"{r['soccerwire_name']} → '{r['best_match_in_master']}' ({r['best_match_score']:.0f})"
            for r in rows if r["classification"] == "DUPLICATE"
        ) or "—"

        lines.append(
            f"| {abbr} | {total} | {net_new} | {near} | {dupes} | {near_clubs} | {dupe_clubs} |"
        )
        totals["total"]      += total
        totals["NET_NEW"]    += net_new
        totals["NEAR_MATCH"] += near
        totals["DUPLICATE"]  += dupes

    lines += [
        f"| **TOTAL** | **{totals['total']}** | **{totals['NET_NEW']}** "
        f"| **{totals['NEAR_MATCH']}** | **{totals['DUPLICATE']}** | | |",
        "",
        "## NET_NEW clubs by state",
        "",
    ]

    for abbr in sorted(state_results.keys()):
        net_new_rows = [r for r in state_results[abbr] if r["classification"] == "NET_NEW"]
        if not net_new_rows:
            continue
        lines.append(f"### {abbr}")
        lines.append("")
        for r in net_new_rows:
            city_str = f" ({r['city']})" if r["city"] else ""
            lines.append(f"- **{r['soccerwire_name']}**{city_str} — {r['soccerwire_url']}")
        lines.append("")

    lines += [
        "## NEAR_MATCH clubs by state (needs human review)",
        "",
    ]

    for abbr in sorted(state_results.keys()):
        near_rows = [r for r in state_results[abbr] if r["classification"] == "NEAR_MATCH"]
        if not near_rows:
            continue
        lines.append(f"### {abbr}")
        lines.append("")
        for r in near_rows:
            city_str = f" ({r['city']})" if r["city"] else ""
            lines.append(
                f"- **{r['soccerwire_name']}**{city_str} ≈ `{r['best_match_in_master']}` "
                f"(score {r['best_match_score']:.0f}) — {r['soccerwire_url']}"
            )
        lines.append("")

    lines += [
        "## DUPLICATE clubs by state (already in master.csv)",
        "",
    ]

    for abbr in sorted(state_results.keys()):
        dupe_rows = [r for r in state_results[abbr] if r["classification"] == "DUPLICATE"]
        if not dupe_rows:
            continue
        lines.append(f"### {abbr}")
        lines.append("")
        for r in dupe_rows:
            city_str = f" ({r['city']})" if r["city"] else ""
            lines.append(
                f"- **{r['soccerwire_name']}**{city_str} → `{r['best_match_in_master']}` "
                f"(score {r['best_match_score']:.0f}) — {r['soccerwire_url']}"
            )
        lines.append("")

    lines += [
        "## Notes",
        "",
        f"- Generated: {run_date}",
        f"- Source: SoccerWire WP REST API + individual club pages",
        f"- Compared against: `output/master.csv` (canonical clubs, all sources)",
        f"- Classifications: NET_NEW=safe to append; NEAR_MATCH=needs human review; "
        f"DUPLICATE=already covered",
        "",
    ]

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logger.info("Written %s", path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare SoccerWire clubs against master.csv for the 8 SoccerWire states.",
    )
    parser.add_argument(
        "--state",
        metavar="XX",
        help="Run for a single 2-letter state code only (e.g. HI). Default: all 8 states.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Override output directory. Default: output/soccerwire_diff/YYYY-MM-DD/",
    )
    parser.add_argument(
        "--master-csv",
        default=None,
        help=f"Path to master.csv. Default: {MASTER_CSV}",
    )
    args = parser.parse_args()

    run_date = date.today().isoformat()

    # Resolve output directory
    if args.output_dir:
        out_dir = args.output_dir
    else:
        out_dir = os.path.join("output", "soccerwire_diff", run_date)

    # Resolve master.csv path
    master_csv = args.master_csv or os.path.join(_SCRIPT_DIR, MASTER_CSV)
    if not os.path.isabs(master_csv):
        master_csv = os.path.join(_SCRIPT_DIR, master_csv)

    # Determine which states to run
    if args.state:
        state_abbr = args.state.upper()
        if state_abbr not in SOCCERWIRE_STATES:
            parser.error(
                f"Unknown state '{state_abbr}'. Valid options: "
                + ", ".join(sorted(SOCCERWIRE_STATES))
            )
        states_to_run = {state_abbr: SOCCERWIRE_STATES[state_abbr]}
    else:
        states_to_run = SOCCERWIRE_STATES

    logger.info("Run date: %s | Output: %s | States: %s", run_date, out_dir, sorted(states_to_run))

    # Load master.csv once
    master_by_state = _load_master_by_state(master_csv)

    # Run diff per state
    state_results: Dict[str, List[Dict]] = {}
    for abbr, league_name in states_to_run.items():
        result = diff_state(abbr, league_name, master_by_state.get(abbr, []))
        state_results[abbr] = result
        # Write per-state CSV
        csv_path = os.path.join(out_dir, f"{abbr}.csv")
        _write_state_csv(csv_path, result)

    # Write summary
    summary_path = os.path.join(out_dir, "summary.md")
    _write_summary_md(summary_path, state_results, run_date)

    # Print console summary
    print()
    print(f"{'State':<8} {'SW Total':>10} {'NET_NEW':>9} {'NEAR_MATCH':>11} {'DUPLICATE':>10}")
    print("-" * 52)
    grand = {"total": 0, "NET_NEW": 0, "NEAR_MATCH": 0, "DUPLICATE": 0}
    for abbr in sorted(state_results):
        rows = state_results[abbr]
        t = len(rows)
        n = sum(1 for r in rows if r["classification"] == "NET_NEW")
        m = sum(1 for r in rows if r["classification"] == "NEAR_MATCH")
        d = sum(1 for r in rows if r["classification"] == "DUPLICATE")
        print(f"{abbr:<8} {t:>10} {n:>9} {m:>11} {d:>10}")
        grand["total"] += t
        grand["NET_NEW"] += n
        grand["NEAR_MATCH"] += m
        grand["DUPLICATE"] += d
    print("-" * 52)
    print(f"{'TOTAL':<8} {grand['total']:>10} {grand['NET_NEW']:>9} {grand['NEAR_MATCH']:>11} {grand['DUPLICATE']:>10}")
    print()
    print(f"Reports written to: {os.path.abspath(out_dir)}/")


if __name__ == "__main__":
    main()
