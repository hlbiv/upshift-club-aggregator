"""
canonical_club_linker.py — Resolve raw team-name strings written by the
event + match scrapers to `canonical_clubs.id`, populating the nullable
FK columns those scrapers deliberately leave blank.

Populates:
  - event_teams.canonical_club_id       (from event_teams.team_name_raw)
  - matches.home_club_id                (from matches.home_team_name)
  - matches.away_club_id                (from matches.away_team_name)
  - club_roster_snapshots.club_id       (from club_roster_snapshots.club_name_raw)
  - roster_diffs.club_id                (from roster_diffs.club_name_raw)
  - tryouts.club_id                     (from tryouts.club_name_raw)

Resolution strategy (4 passes, each optimistic, short-circuits on first hit):
  1. Exact alias match          SELECT club_id FROM club_aliases WHERE alias_name = ?
  2. Exact canonical name match SELECT id FROM canonical_clubs WHERE club_name_canonical = ?
  3. Fuzzy match (RapidFuzz token_set_ratio >= FUZZY_THRESHOLD) against ALL
     canonical_clubs + aliases; on hit, insert a new club_alias row so
     future runs hit pass #1.
  4. No match — leave FK NULL, record the raw name in the unmatched bucket.

Idempotency: only updates rows where the FK column is currently NULL.

Downstream consumers (do not work without this job running):
  - /api/events/search?club_id=N   (PR #11)
  - matches → club_results rollup  (PR #10)

CLI:
    python3 run.py --source link-canonical-clubs [--dry-run] [--limit N]
"""

from __future__ import annotations

import logging
import os
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import execute_batch  # type: ignore  # noqa: F401
except ImportError:  # pragma: no cover — tested envs have psycopg2
    psycopg2 = None  # type: ignore

try:
    from rapidfuzz import fuzz, process as rf_process  # type: ignore
except ImportError:  # pragma: no cover
    fuzz = None  # type: ignore
    rf_process = None  # type: ignore

from config import FUZZY_THRESHOLD

log = logging.getLogger("canonical_club_linker")


# ---------------------------------------------------------------------------
# Raw-name stripping
# ---------------------------------------------------------------------------

# Match U12 / U-12 / U 12 style age tokens.
_AGE_PATTERN = re.compile(r"\bU-?\s*\d{1,2}\b", flags=re.IGNORECASE)
# Four-digit birth-year tokens typical of youth soccer: 2004-2016 window.
_BIRTH_YEAR_PATTERN = re.compile(r"\b(?:19[89]\d|200\d|201[0-9]|202\d)\b")
# Gender / program / division / generic tokens to strip. Conservative: we
# KEEP "FC", "SC", "AC", "CF" because those are canonical club-name parts
# (Concorde FC, Hurricanes SC). `normalizer._canonical` strips them
# downstream inside the canonical column — we want pass #1 (exact alias
# hit) and pass #2 (exact canonical hit) to work on the raw column first.
_STOPWORDS: frozenset = frozenset({
    "boys", "girls", "men", "women", "male", "female",
    "m", "f", "b", "g",
    "academy", "elite", "premier", "select", "classic",
    "gold", "silver", "bronze", "white", "black", "red", "blue", "green",
    "ecnl", "enpl", "npl", "mls", "usl", "nal", "eal", "edp",
    "rl", "national", "regional",
    "youth",
})

_WHITESPACE = re.compile(r"\s+")
_PUNCTUATION = re.compile(r"[^\w\s-]")


def strip_team_descriptors(raw: str) -> str:
    """
    Strip age / gender / program tokens from a raw team name to get a
    club-name guess for matching. Conservative — keeps "FC", "SC", etc.

    Example:
        "Concorde Fire Premier 2011 Boys" -> "Concorde Fire"
        "NTH Tophat U15 Boys Gold"        -> "NTH Tophat"
    """
    if not isinstance(raw, str):
        return ""
    s = raw.strip()
    if not s:
        return ""
    # Strip age patterns + birth years first (they're unambiguous).
    s = _AGE_PATTERN.sub(" ", s)
    s = _BIRTH_YEAR_PATTERN.sub(" ", s)
    # Drop punctuation to avoid "2011." residue keeping a token alive.
    s = _PUNCTUATION.sub(" ", s)
    # Token-level stopword filter.
    tokens = [t for t in s.split() if t.lower() not in _STOPWORDS]
    s = " ".join(tokens)
    s = _WHITESPACE.sub(" ", s).strip()
    return s


# ---------------------------------------------------------------------------
# Fuzzy index — loaded once per run
# ---------------------------------------------------------------------------

@dataclass
class ClubIndex:
    """In-memory index of canonical clubs + aliases for fuzzy matching."""
    # Exact-match maps (lowercased key -> club_id).
    alias_exact: Dict[str, int] = field(default_factory=dict)
    canonical_exact: Dict[str, int] = field(default_factory=dict)
    # Fuzzy choice list: lowercased display string -> club_id.
    # Kept as parallel arrays so rapidfuzz.process.extractOne can read
    # the choices list directly.
    fuzzy_choices: List[str] = field(default_factory=list)
    fuzzy_club_ids: List[int] = field(default_factory=list)

    def size(self) -> int:
        return len(self.fuzzy_choices)


def load_club_index(cur) -> ClubIndex:
    """
    Build a ClubIndex from all canonical_clubs + club_aliases. One round
    trip per table.
    """
    idx = ClubIndex()

    cur.execute("SELECT id, club_name_canonical FROM canonical_clubs")
    for club_id, name in cur.fetchall():
        if not name:
            continue
        key = name.strip().lower()
        idx.canonical_exact[key] = club_id
        idx.fuzzy_choices.append(key)
        idx.fuzzy_club_ids.append(club_id)

    cur.execute(
        "SELECT club_id, alias_name FROM club_aliases WHERE club_id IS NOT NULL"
    )
    for club_id, alias in cur.fetchall():
        if not alias:
            continue
        key = alias.strip().lower()
        if not key:
            continue
        # Alias-exact takes priority over canonical-exact for pass #1.
        idx.alias_exact[key] = club_id
        idx.fuzzy_choices.append(key)
        idx.fuzzy_club_ids.append(club_id)

    return idx


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------

@dataclass
class ResolveResult:
    club_id: Optional[int]
    pass_number: int          # 1=alias, 2=canonical, 3=fuzzy, 4=no-match
    score: Optional[int] = None  # fuzzy score when pass_number == 3
    matched_choice: Optional[str] = None


def resolve_raw_team_name(
    raw: str,
    idx: ClubIndex,
    threshold: int = FUZZY_THRESHOLD,
) -> ResolveResult:
    """
    Run the 4-pass resolver against the in-memory ClubIndex. Pure
    function — does not write. New aliases from pass-3 hits are handled
    by the caller so writes stay in one place.
    """
    if not raw or not isinstance(raw, str):
        return ResolveResult(None, 4)

    # Build two candidate keys: the raw name itself (lowered) and the
    # stripped guess. Pass #1 + #2 try both — some sources store the
    # canonical form as the raw string already (linker is idempotent).
    raw_key = raw.strip().lower()
    stripped = strip_team_descriptors(raw)
    stripped_key = stripped.lower() if stripped else ""

    # Pass 1 — exact alias
    for k in (raw_key, stripped_key):
        if k and k in idx.alias_exact:
            return ResolveResult(idx.alias_exact[k], 1, matched_choice=k)

    # Pass 2 — exact canonical
    for k in (raw_key, stripped_key):
        if k and k in idx.canonical_exact:
            return ResolveResult(idx.canonical_exact[k], 2, matched_choice=k)

    # Pass 3 — fuzzy
    if fuzz is None or rf_process is None or not idx.fuzzy_choices:
        return ResolveResult(None, 4)

    # Prefer the stripped key for fuzzy — age/gender tokens inflate the
    # denominator of token_set_ratio otherwise.
    query = stripped_key or raw_key
    if not query:
        return ResolveResult(None, 4)

    match = rf_process.extractOne(
        query,
        idx.fuzzy_choices,
        scorer=fuzz.token_set_ratio,
        score_cutoff=threshold,
    )
    if match is None:
        return ResolveResult(None, 4)

    matched_choice, score, match_idx = match
    club_id = idx.fuzzy_club_ids[match_idx]
    return ResolveResult(club_id, 3, score=int(score), matched_choice=matched_choice)


# ---------------------------------------------------------------------------
# DB row iterators + updaters
# ---------------------------------------------------------------------------

def _fetch_null_event_teams(cur, limit: Optional[int]) -> List[Tuple[int, str]]:
    sql = (
        "SELECT id, team_name_raw FROM event_teams "
        "WHERE canonical_club_id IS NULL "
        "AND team_name_raw IS NOT NULL AND team_name_raw <> '' "
        "ORDER BY id"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    return list(cur.fetchall())


def _fetch_null_matches(cur, side: str, limit: Optional[int]) -> List[Tuple[int, str]]:
    assert side in ("home", "away")
    club_col = f"{side}_club_id"
    name_col = f"{side}_team_name"
    sql = (
        f"SELECT id, {name_col} FROM matches "
        f"WHERE {club_col} IS NULL "
        f"AND {name_col} IS NOT NULL AND {name_col} <> '' "
        f"ORDER BY id"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    return list(cur.fetchall())


def _update_event_team(cur, row_id: int, club_id: int) -> None:
    cur.execute(
        "UPDATE event_teams SET canonical_club_id = %s "
        "WHERE id = %s AND canonical_club_id IS NULL",
        (club_id, row_id),
    )


def _update_match_side(cur, row_id: int, side: str, club_id: int) -> None:
    assert side in ("home", "away")
    club_col = f"{side}_club_id"
    cur.execute(
        f"UPDATE matches SET {club_col} = %s "
        f"WHERE id = %s AND {club_col} IS NULL",
        (club_id, row_id),
    )


def _fetch_null_roster_snapshots(
    cur, limit: Optional[int]
) -> List[Tuple[int, str]]:
    sql = (
        "SELECT id, club_name_raw FROM club_roster_snapshots "
        "WHERE club_id IS NULL "
        "AND club_name_raw IS NOT NULL AND club_name_raw <> '' "
        "ORDER BY id"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    return list(cur.fetchall())


def _update_roster_snapshot(cur, row_id: int, club_id: int) -> None:
    cur.execute(
        "UPDATE club_roster_snapshots SET club_id = %s "
        "WHERE id = %s AND club_id IS NULL",
        (club_id, row_id),
    )


def _fetch_null_roster_diffs(cur, limit: Optional[int]) -> List[Tuple[int, str]]:
    sql = (
        "SELECT id, club_name_raw FROM roster_diffs "
        "WHERE club_id IS NULL "
        "AND club_name_raw IS NOT NULL AND club_name_raw <> '' "
        "ORDER BY id"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    return list(cur.fetchall())


def _update_roster_diff(cur, row_id: int, club_id: int) -> None:
    cur.execute(
        "UPDATE roster_diffs SET club_id = %s "
        "WHERE id = %s AND club_id IS NULL",
        (club_id, row_id),
    )


def _fetch_null_tryouts(cur, limit: Optional[int]) -> List[Tuple[int, str]]:
    sql = (
        "SELECT id, club_name_raw FROM tryouts "
        "WHERE club_id IS NULL "
        "AND club_name_raw IS NOT NULL AND club_name_raw <> '' "
        "ORDER BY id"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    return list(cur.fetchall())


def _update_tryout(cur, row_id: int, club_id: int) -> None:
    cur.execute(
        "UPDATE tryouts SET club_id = %s "
        "WHERE id = %s AND club_id IS NULL",
        (club_id, row_id),
    )


def _insert_alias(cur, club_id: int, alias_name: str) -> None:
    """
    Cache a fuzzy-hit alias so future runs short-circuit at pass #1.
    Unique on (club_id, alias_name) — ON CONFLICT DO NOTHING.
    """
    cur.execute(
        "INSERT INTO club_aliases (club_id, alias_name, source, is_official) "
        "VALUES (%s, %s, 'linker-fuzzy', false) "
        "ON CONFLICT ON CONSTRAINT club_aliases_club_alias_uq DO NOTHING",
        (club_id, alias_name),
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

@dataclass
class LinkerStats:
    event_teams_linked: int = 0
    matches_home_linked: int = 0
    matches_away_linked: int = 0
    roster_snapshots_linked: int = 0
    roster_diffs_linked: int = 0
    tryouts_linked: int = 0
    unmatched_names: Counter = field(default_factory=Counter)
    pass_hits: Counter = field(default_factory=Counter)
    aliases_written: int = 0

    def total_linked(self) -> int:
        return (
            self.event_teams_linked
            + self.matches_home_linked
            + self.matches_away_linked
            + self.roster_snapshots_linked
            + self.roster_diffs_linked
            + self.tryouts_linked
        )

    def unmatched_sample(self, n: int = 20) -> List[str]:
        return [name for name, _count in self.unmatched_names.most_common(n)]

    def to_details(self) -> dict:
        return {
            "event_teams_linked": self.event_teams_linked,
            "matches_home_linked": self.matches_home_linked,
            "matches_away_linked": self.matches_away_linked,
            "roster_snapshots_linked": self.roster_snapshots_linked,
            "roster_diffs_linked": self.roster_diffs_linked,
            "tryouts_linked": self.tryouts_linked,
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
    """
    Main entry point. Runs the 4-pass resolver over every NULL-FK row in
    event_teams + matches, writing FKs + fuzzy-hit aliases.

    Args:
        conn: open psycopg2 connection (autocommit OK; we use one txn).
        dry_run: print what WOULD be linked, do not write.
        limit: if set, process only first N NULL rows PER source table
            (event_teams / matches-home / matches-away). For smoke
            testing — NOT a global cap.
    """
    stats = LinkerStats()

    with conn.cursor() as cur:
        log.info("Loading canonical-club index…")
        idx = load_club_index(cur)
        log.info(
            "Loaded %d canonical names, %d aliases (combined fuzzy choices: %d)",
            len(idx.canonical_exact),
            len(idx.alias_exact),
            idx.size(),
        )

        event_team_rows = _fetch_null_event_teams(cur, limit)
        matches_home = _fetch_null_matches(cur, "home", limit)
        matches_away = _fetch_null_matches(cur, "away", limit)
        roster_snapshot_rows = _fetch_null_roster_snapshots(cur, limit)
        roster_diff_rows = _fetch_null_roster_diffs(cur, limit)
        tryout_rows = _fetch_null_tryouts(cur, limit)
        log.info(
            "Candidates: %d event_teams, %d matches.home, %d matches.away, "
            "%d roster_snapshots, %d roster_diffs, %d tryouts",
            len(event_team_rows),
            len(matches_home),
            len(matches_away),
            len(roster_snapshot_rows),
            len(roster_diff_rows),
            len(tryout_rows),
        )

        def _handle(raw: str) -> ResolveResult:
            res = resolve_raw_team_name(raw, idx)
            stats.pass_hits[res.pass_number] += 1
            if res.club_id is None:
                # Record the stripped guess when non-empty; otherwise
                # fall back to raw. We want the sample to be actionable.
                bucket = strip_team_descriptors(raw) or raw.strip()
                stats.unmatched_names[bucket] += 1
            elif res.pass_number == 3 and not dry_run:
                # Cache fuzzy hits as aliases on the stripped key —
                # that's what we used as the query.
                alias_to_write = strip_team_descriptors(raw) or raw.strip()
                if alias_to_write:
                    _insert_alias(cur, res.club_id, alias_to_write)
                    # Keep the in-memory index warm too so the next row
                    # with the same raw name short-circuits at pass #1
                    # in this same run (typical for matches where the
                    # same club appears dozens of times as home + away).
                    idx.alias_exact[alias_to_write.lower()] = res.club_id
                    stats.aliases_written += 1
            return res

        # event_teams
        for row_id, raw in event_team_rows:
            res = _handle(raw)
            if res.club_id is None:
                continue
            stats.event_teams_linked += 1
            if not dry_run:
                _update_event_team(cur, row_id, res.club_id)

        # matches.home
        for row_id, raw in matches_home:
            res = _handle(raw)
            if res.club_id is None:
                continue
            stats.matches_home_linked += 1
            if not dry_run:
                _update_match_side(cur, row_id, "home", res.club_id)

        # matches.away
        for row_id, raw in matches_away:
            res = _handle(raw)
            if res.club_id is None:
                continue
            stats.matches_away_linked += 1
            if not dry_run:
                _update_match_side(cur, row_id, "away", res.club_id)

        # club_roster_snapshots
        for row_id, raw in roster_snapshot_rows:
            res = _handle(raw)
            if res.club_id is None:
                continue
            stats.roster_snapshots_linked += 1
            if not dry_run:
                _update_roster_snapshot(cur, row_id, res.club_id)

        # roster_diffs
        for row_id, raw in roster_diff_rows:
            res = _handle(raw)
            if res.club_id is None:
                continue
            stats.roster_diffs_linked += 1
            if not dry_run:
                _update_roster_diff(cur, row_id, res.club_id)

        # tryouts
        for row_id, raw in tryout_rows:
            res = _handle(raw)
            if res.club_id is None:
                continue
            stats.tryouts_linked += 1
            if not dry_run:
                _update_tryout(cur, row_id, res.club_id)

        if dry_run:
            conn.rollback()
        else:
            conn.commit()

    return stats


def run_cli(dry_run: bool = False, limit: Optional[int] = None) -> int:
    """
    Entry point for `python run.py --source link-canonical-clubs`. Opens
    a DB connection, runs the linker, writes a scrape_run_logs row.

    Returns the process exit code (0 on success, 1 on DB unavailable).
    """
    from scrape_run_logger import ScrapeRunLogger, FailureKind

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
            scraper_key="link-canonical-clubs",
            league_name=None,
        )
        run_log.start(source_url=None)

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
        return 1

    print(
        f"Linked {stats.event_teams_linked} event_teams, "
        f"{stats.matches_home_linked + stats.matches_away_linked} match sides, "
        f"{stats.roster_snapshots_linked} roster_snapshots, "
        f"{stats.roster_diffs_linked} roster_diffs, "
        f"{stats.tryouts_linked} tryouts, "
        f"{len(stats.unmatched_names)} unmatched unique raw names."
    )
    if stats.unmatched_names:
        print("Top 10 unmatched raw names:")
        for name, count in stats.unmatched_names.most_common(10):
            print(f"  {count:>4}  {name}")

    if run_log is not None:
        import json
        details = stats.to_details()
        # Details live in the error_message column as JSON until we add
        # a dedicated details column to scrape_run_logs. Keep under 4000
        # chars — scrape_run_logger truncates.
        details_json = json.dumps(details)[:3900]
        run_log.finish_ok(
            records_created=0,
            records_updated=stats.total_linked(),
        )
        # Also emit a log line that captures the details for ops.
        log.info("linker-details: %s", details_json)

    return 0
