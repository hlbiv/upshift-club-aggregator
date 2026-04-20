"""
canonical_school_linker.py — Resolve raw high-school-name + state strings
written by the MaxPreps HS-roster scraper to `canonical_schools.id`,
populating the nullable FK column the scraper deliberately leaves blank.

Populates:
  - hs_rosters.school_id           (from hs_rosters.school_name_raw +
                                    hs_rosters.school_state)

Resolution strategy (4 passes, each optimistic, short-circuits on first
hit). CRITICAL invariant: every pass is scoped to `school_state` —
"Lincoln High" in NE and "Lincoln High" in CA must NEVER cross-match.

  1. Exact alias match          SELECT school_id FROM school_aliases
                                WHERE alias_name = ? AND school_state = ?
  2. Exact canonical name match SELECT id FROM canonical_schools
                                WHERE school_name_canonical = ? AND school_state = ?
  3. Fuzzy match (RapidFuzz token_set_ratio >= FUZZY_THRESHOLD) against
     canonical_schools + aliases scoped to the same state; on hit,
     insert a new school_aliases row so future runs hit pass #1.
  4. No match — leave FK NULL, record (raw_name, state) in the
     unmatched bucket.

Idempotency: only updates hs_rosters rows where school_id IS NULL.

CLI:
    python3 run.py --source link-canonical-schools [--dry-run] [--limit N]
"""

from __future__ import annotations

import logging
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover — tested envs have psycopg2
    psycopg2 = None  # type: ignore

try:
    from rapidfuzz import fuzz, process as rf_process  # type: ignore
except ImportError:  # pragma: no cover
    fuzz = None  # type: ignore
    rf_process = None  # type: ignore

# Stdlib fallback — mirrors canonical_club_linker.py so the HS linker
# survives a rapidfuzz-less environment too.
import difflib  # noqa: E402

from config import FUZZY_THRESHOLD  # noqa: E402

log = logging.getLogger("canonical_school_linker")

_RAPIDFUZZ_AVAILABLE = fuzz is not None and rf_process is not None


# ---------------------------------------------------------------------------
# Raw-name normalization
# ---------------------------------------------------------------------------

_WHITESPACE = re.compile(r"\s+")
_PUNCTUATION = re.compile(r"[^\w\s-]")


def normalize_school_name(raw: str) -> str:
    """Normalize a raw HS name for matching.

    Conservative — school names are typically already tidy ("Lincoln High
    School", "Mater Dei High School"). We only strip punctuation and
    collapse whitespace. Unlike the club linker we do NOT strip tokens
    like "High" / "Academy" — those are often load-bearing parts of the
    canonical school name.
    """
    if not isinstance(raw, str):
        return ""
    s = raw.strip()
    if not s:
        return ""
    s = _PUNCTUATION.sub(" ", s)
    s = _WHITESPACE.sub(" ", s).strip()
    return s


def _normalize_state(raw: Optional[str]) -> str:
    """Uppercase a 2-letter state code. Empty string if falsy."""
    if not isinstance(raw, str):
        return ""
    return raw.strip().upper()


# ---------------------------------------------------------------------------
# Fuzzy index — loaded once per run, grouped by state
# ---------------------------------------------------------------------------

@dataclass
class SchoolIndex:
    """In-memory state-scoped index of canonical_schools + school_aliases."""
    # {state: {lowercased_name: school_id}}
    alias_exact: Dict[str, Dict[str, int]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    canonical_exact: Dict[str, Dict[str, int]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    # {state: (choices_list, school_ids_list)} for rapidfuzz.
    fuzzy_by_state: Dict[str, Tuple[List[str], List[int]]] = field(
        default_factory=dict,
    )

    def total_canonical(self) -> int:
        return sum(len(v) for v in self.canonical_exact.values())

    def total_aliases(self) -> int:
        return sum(len(v) for v in self.alias_exact.values())


def load_school_index(cur) -> SchoolIndex:
    """Build a SchoolIndex from canonical_schools + school_aliases."""
    idx = SchoolIndex()
    # state -> list of (choice, school_id)
    fuzzy_pairs: Dict[str, List[Tuple[str, int]]] = defaultdict(list)

    cur.execute(
        "SELECT id, school_name_canonical, school_state FROM canonical_schools"
    )
    for school_id, name, state in cur.fetchall():
        if not name or not state:
            continue
        state_key = _normalize_state(state)
        name_key = name.strip().lower()
        idx.canonical_exact[state_key][name_key] = school_id
        fuzzy_pairs[state_key].append((name_key, school_id))

    cur.execute(
        "SELECT school_id, alias_name, school_state FROM school_aliases"
    )
    for school_id, alias, state in cur.fetchall():
        if not alias or not state:
            continue
        state_key = _normalize_state(state)
        alias_key = alias.strip().lower()
        # Alias-exact takes priority over canonical-exact for pass #1.
        idx.alias_exact[state_key][alias_key] = school_id
        fuzzy_pairs[state_key].append((alias_key, school_id))

    for state_key, pairs in fuzzy_pairs.items():
        choices = [p[0] for p in pairs]
        ids = [p[1] for p in pairs]
        idx.fuzzy_by_state[state_key] = (choices, ids)

    return idx


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------

@dataclass
class ResolveResult:
    school_id: Optional[int]
    pass_number: int  # 1=alias, 2=canonical, 3=fuzzy, 4=no-match
    score: Optional[int] = None
    matched_choice: Optional[str] = None


def resolve_raw_school_name(
    raw: str,
    state: str,
    idx: SchoolIndex,
    threshold: int = FUZZY_THRESHOLD,
) -> ResolveResult:
    """Run the 4-pass, state-scoped resolver.

    Pure function — new aliases from pass-3 hits are handled by the
    caller so writes stay in one place.
    """
    state_key = _normalize_state(state)
    if not state_key:
        # State is mandatory. A row without a state can never match.
        return ResolveResult(None, 4)
    if not raw or not isinstance(raw, str):
        return ResolveResult(None, 4)

    raw_key = raw.strip().lower()
    normalized = normalize_school_name(raw)
    normalized_key = normalized.lower() if normalized else ""

    alias_map = idx.alias_exact.get(state_key, {})
    canonical_map = idx.canonical_exact.get(state_key, {})

    # Pass 1 — exact alias (state-scoped)
    for k in (raw_key, normalized_key):
        if k and k in alias_map:
            return ResolveResult(alias_map[k], 1, matched_choice=k)

    # Pass 2 — exact canonical (state-scoped)
    for k in (raw_key, normalized_key):
        if k and k in canonical_map:
            return ResolveResult(canonical_map[k], 2, matched_choice=k)

    # Pass 3 — fuzzy (state-scoped)
    choices_ids = idx.fuzzy_by_state.get(state_key)
    if not choices_ids:
        return ResolveResult(None, 4)
    choices, ids = choices_ids
    if not choices:
        return ResolveResult(None, 4)

    query = normalized_key or raw_key
    if not query:
        return ResolveResult(None, 4)

    if _RAPIDFUZZ_AVAILABLE:
        match = rf_process.extractOne(
            query,
            choices,
            scorer=fuzz.token_set_ratio,
            score_cutoff=threshold,
        )
        if match is None:
            return ResolveResult(None, 4)
        matched_choice, score, match_idx = match
        return ResolveResult(
            ids[match_idx], 3, score=int(score), matched_choice=matched_choice,
        )

    # Stdlib fallback
    cutoff = threshold / 100.0
    best_score = 0.0
    best_idx: Optional[int] = None
    best_choice: Optional[str] = None
    for i, choice in enumerate(choices):
        r = _difflib_token_set_ratio(query, choice)
        if r >= cutoff and r > best_score:
            best_score = r
            best_idx = i
            best_choice = choice
    if best_idx is None:
        return ResolveResult(None, 4)
    return ResolveResult(
        ids[best_idx], 3,
        score=int(round(best_score * 100)), matched_choice=best_choice,
    )


def _difflib_token_set_ratio(a: str, b: str) -> float:
    """Approximate rapidfuzz.fuzz.token_set_ratio using stdlib difflib.

    See canonical_club_linker._difflib_token_set_ratio for the full
    derivation — same algorithm, reused here verbatim.
    """
    ta = set(a.split())
    tb = set(b.split())
    if not ta or not tb:
        return 0.0
    inter = ta & tb
    diff_a = ta - tb
    diff_b = tb - ta
    s_inter = " ".join(sorted(inter))
    s_a = (s_inter + " " + " ".join(sorted(diff_a))).strip()
    s_b = (s_inter + " " + " ".join(sorted(diff_b))).strip()
    sm = difflib.SequenceMatcher(None, autojunk=False)
    best = 0.0
    for x, y in ((s_inter, s_a), (s_inter, s_b), (s_a, s_b)):
        if not x or not y:
            continue
        sm.set_seqs(x, y)
        r = sm.ratio()
        if r > best:
            best = r
    return best


# ---------------------------------------------------------------------------
# DB row iterators + updaters
# ---------------------------------------------------------------------------

def _fetch_null_hs_rosters(
    cur, limit: Optional[int]
) -> List[Tuple[int, str, str]]:
    """Return (id, school_name_raw, school_state) for every hs_rosters row
    where school_id IS NULL and both raw + state are present."""
    sql = (
        "SELECT id, school_name_raw, school_state FROM hs_rosters "
        "WHERE school_id IS NULL "
        "AND school_name_raw IS NOT NULL AND school_name_raw <> '' "
        "AND school_state IS NOT NULL AND school_state <> '' "
        "ORDER BY id"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    return list(cur.fetchall())


def _update_hs_roster(cur, row_id: int, school_id: int) -> None:
    cur.execute(
        "UPDATE hs_rosters SET school_id = %s "
        "WHERE id = %s AND school_id IS NULL",
        (school_id, row_id),
    )


def _insert_alias(
    cur, school_id: int, alias_name: str, school_state: str,
) -> None:
    """Cache a fuzzy-hit alias so future runs short-circuit at pass #1.

    Unique on (alias_name, school_state) — ON CONFLICT DO NOTHING.
    """
    cur.execute(
        "INSERT INTO school_aliases (school_id, alias_name, school_state) "
        "VALUES (%s, %s, %s) "
        "ON CONFLICT ON CONSTRAINT school_aliases_alias_state_uq DO NOTHING",
        (school_id, alias_name, school_state),
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

@dataclass
class LinkerStats:
    hs_rosters_linked: int = 0
    unmatched_names: Counter = field(default_factory=Counter)
    pass_hits: Counter = field(default_factory=Counter)
    aliases_written: int = 0

    def total_linked(self) -> int:
        return self.hs_rosters_linked

    def unmatched_sample(self, n: int = 20) -> List[str]:
        # Counter keys are "(name, state)" strings — keep them human-readable.
        return [key for key, _count in self.unmatched_names.most_common(n)]

    def to_details(self) -> dict:
        return {
            "hs_rosters_linked": self.hs_rosters_linked,
            "pass_1_alias_hits": self.pass_hits.get(1, 0),
            "pass_2_canonical_hits": self.pass_hits.get(2, 0),
            "pass_3_fuzzy_hits": self.pass_hits.get(3, 0),
            "no_match_count": self.pass_hits.get(4, 0),
            "aliases_written": self.aliases_written,
            "unmatched_unique_count": len(self.unmatched_names),
            "unmatched_sample": self.unmatched_sample(20),
        }


def link_all(
    conn,
    dry_run: bool = False,
    limit: Optional[int] = None,
) -> LinkerStats:
    """Main entry point. Runs the state-scoped 4-pass resolver over every
    NULL-FK row in hs_rosters.
    """
    stats = LinkerStats()

    with conn.cursor() as cur:
        log.info("Loading canonical-school index…")
        idx = load_school_index(cur)
        log.info(
            "Loaded %d canonical schools across %d states, %d aliases",
            idx.total_canonical(),
            len(idx.canonical_exact),
            idx.total_aliases(),
        )
        if _RAPIDFUZZ_AVAILABLE:
            log.info(
                "Fuzzy backend: rapidfuzz (token_set_ratio, threshold=%d)",
                FUZZY_THRESHOLD,
            )
        else:
            log.warning(
                "rapidfuzz is NOT installed — falling back to stdlib "
                "difflib (threshold=%d). Install rapidfuzz for better "
                "fuzzy matching.",
                FUZZY_THRESHOLD,
            )

        hs_rows = _fetch_null_hs_rosters(cur, limit)
        log.info("Candidates: %d hs_rosters", len(hs_rows))

        for row_id, raw, state in hs_rows:
            res = resolve_raw_school_name(raw, state, idx)
            stats.pass_hits[res.pass_number] += 1
            if res.school_id is None:
                bucket_name = normalize_school_name(raw) or (raw or "").strip()
                state_key = _normalize_state(state)
                stats.unmatched_names[f"{bucket_name} ({state_key})"] += 1
                continue

            if res.pass_number == 3 and not dry_run:
                alias_to_write = (
                    normalize_school_name(raw) or (raw or "").strip()
                )
                state_key = _normalize_state(state)
                if alias_to_write and state_key:
                    _insert_alias(
                        cur, res.school_id, alias_to_write, state_key,
                    )
                    # Warm the in-memory index so later rows in this same
                    # run short-circuit at pass #1 (rosters typically have
                    # dozens of players per school).
                    idx.alias_exact[state_key][alias_to_write.lower()] = (
                        res.school_id
                    )
                    stats.aliases_written += 1

            stats.hs_rosters_linked += 1
            if not dry_run:
                _update_hs_roster(cur, row_id, res.school_id)

        if dry_run:
            conn.rollback()
        else:
            conn.commit()

    return stats


def run_cli(dry_run: bool = False, limit: Optional[int] = None) -> int:
    """Entry point for `python run.py --source link-canonical-schools`.
    Opens a DB connection, runs the linker, writes a scrape_run_logs row.
    """
    from scrape_run_logger import ScrapeRunLogger, FailureKind
    from alerts import alert_scraper_failure

    if psycopg2 is None:
        log.error("psycopg2 not installed")
        return 1

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL is not set — cannot link")
        return 1

    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key="link-canonical-schools",
            league_name="canonical-school-resolution",
        )
        run_log.start(source_url="derived:canonical_schools")

    try:
        conn = psycopg2.connect(db_url)
        try:
            stats = link_all(conn, dry_run=dry_run, limit=limit)
        finally:
            conn.close()
    except Exception as exc:
        log.error("Linker failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(FailureKind.UNKNOWN, str(exc))
        alert_scraper_failure(
            scraper_key="link-canonical-schools",
            failure_kind=FailureKind.UNKNOWN.value,
            error_message=str(exc),
            source_url="derived:canonical_schools",
            league_name="canonical-school-resolution",
        )
        return 1

    print(
        f"Linked {stats.hs_rosters_linked} hs_rosters rows, "
        f"{len(stats.unmatched_names)} unmatched unique (name, state) pairs."
    )
    if stats.unmatched_names:
        print("Top 10 unmatched (name, state) pairs:")
        for name, count in stats.unmatched_names.most_common(10):
            print(f"  {count:>4}  {name}")

    if run_log is not None:
        import json
        details = stats.to_details()
        details_json = json.dumps(details)[:3900]
        run_log.finish_ok(
            records_created=0,
            records_updated=stats.total_linked(),
        )
        log.info("linker-details: %s", details_json)

    # Post-run reconcile mirrors the club linker's soft-failure pattern.
    if not dry_run:
        try:
            from reconcilers import end_of_run_reconcile
            end_of_run_reconcile()
        except Exception as exc:  # pragma: no cover — defensive
            log.warning("end_of_run_reconcile skipped: %s", exc)

    return 0
