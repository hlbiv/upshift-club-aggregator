"""
canonical_club_merger.py — Tier and merge near-duplicate canonical_clubs.

Companion to ``club_dedup.py`` (the report-only fuzzy detector). Where the
detector flags suspect pairs at >= 0.85 similarity, this module:

  1. **Classifies** each pair into ``auto_merge`` / ``review`` /
     ``skip`` tiers (see ``tier_pairs``). The auto-merge tier requires
     compounding confidence: high score AND same state AND no conflicting
     metadata (different websites, ``manually_merged=True``).
  2. **Merges** auto-tier pairs by FK-redirecting every dependent row
     to the winner and inserting a ``club_aliases`` row that records the
     loser's name + id (``merged_from_canonical_id`` is the audit pointer).

The merger is destructive. It runs in a single psycopg2 transaction; a
caller failure mid-merge is rolled back. Default mode in the CLI
(``__main__.py``) is ``--dry-run``; ``--no-dry-run`` is required to commit.

FK redirect coverage — every table that points at ``canonical_clubs.id``:

  - ``club_aliases.club_id``               (cascade)
  - ``club_affiliations.club_id``          (cascade; UQ on (club_id, source_name))
  - ``coach_discoveries.club_id``          (cascade; UQ on (club_id, name, title))
  - ``event_teams.canonical_club_id``      (set null; UQ on (event_id, team_name_raw))
  - ``matches.home_club_id``               (set null)
  - ``matches.away_club_id``               (set null)
  - ``club_results.club_id``               (cascade; UQ on full natural key)
  - ``coach_scrape_snapshots.club_id``     (cascade; UQ on (club_id, scraped_at))
  - ``club_roster_snapshots.club_id``      (cascade; natural key keys off
                                            club_name_raw, not club_id, so
                                            no UQ collision)
  - ``club_site_changes.club_id``          (cascade; UQ on natural key)
  - ``roster_diffs.club_id``               (cascade; same as above)
  - ``tryouts.club_id``                    (cascade)
  - ``player_id_selections.club_id``       (set null)
  - ``coach_career_history.entity_id``     (polymorphic; only redirected
                                            where entity_type='club')

Several of these tables have unique constraints that *include* ``club_id``;
collapsing two rows into the winner can therefore violate them. Strategy:

  - Update the loser's row to point at the winner where the resulting
    composite key would not already exist on the winner side (deduped
    update).
  - Delete the loser's row otherwise — the winner already has the same
    semantic record and the loser's copy is a duplicate of a duplicate.

This means a merge can implicitly delete a few of the loser's rows. They
were always going to be duplicates of the winner's rows; the merge is the
moment we decide which copy to keep. The loser's discoveries / aliases
/ affiliations are never *more authoritative* than the winner's by
construction (the merge picks the row with more filled columns as winner).

The whole sequence runs inside a single transaction so a per-row error
rolls back the entire merge.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

# Allow direct module imports when run as `python3 -m dedup.canonical_club_merger`
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover — tested envs have psycopg2
    psycopg2 = None  # type: ignore

from dedup.club_dedup import DedupPair  # noqa: E402

log = logging.getLogger("canonical_club_merger")


# ---------------------------------------------------------------------------
# Tiering
# ---------------------------------------------------------------------------

# Thresholds — tuned to be conservative. Auto-merge requires strong
# similarity AND no metadata conflict. Review tier captures the rest.
AUTO_MERGE_SIMILARITY = 0.95
REVIEW_MIN_SIMILARITY = 0.85


@dataclass
class TieredPair:
    """A DedupPair plus the tier decision and reasoning."""
    pair: DedupPair
    tier: str  # 'auto_merge' | 'review' | 'skip'
    reasoning: str
    # Recommended winner_id when tier == 'auto_merge'. None for review/skip.
    recommended_winner_id: Optional[int] = None


@dataclass
class ClubMeta:
    """Subset of canonical_clubs columns the tierer needs per id."""
    id: int
    name: str
    state: Optional[str]
    website: Optional[str]
    manually_merged: bool
    completeness: int  # count of non-null/non-empty columns


# Columns we count toward "data completeness" when picking a winner. Pure
# presence test — quality is a separate concern.
_COMPLETENESS_COLUMNS = (
    "city", "state", "website", "logo_url", "founded_year", "twitter",
    "instagram", "facebook", "staff_page_url",
)


def fetch_club_meta(conn, club_ids: List[int]) -> Dict[int, ClubMeta]:
    """Bulk-load the metadata the tierer needs for a list of club ids."""
    if not club_ids:
        return {}
    cols = ["id", "club_name_canonical", "state", "website", "manually_merged"]
    cols.extend(_COMPLETENESS_COLUMNS)
    # Dedup the SELECT list — state/website appear in both blocks above.
    seen, ordered = set(), []
    for c in cols:
        if c not in seen:
            ordered.append(c)
            seen.add(c)
    select_list = ", ".join(ordered)

    out: Dict[int, ClubMeta] = {}
    with conn.cursor() as cur:
        # ANY(%s) keeps the param shape psycopg2-friendly without
        # building a variadic IN clause.
        cur.execute(
            f"SELECT {select_list} FROM canonical_clubs WHERE id = ANY(%s)",
            (list(club_ids),),
        )
        rows = cur.fetchall()
        col_index = {name: i for i, name in enumerate(ordered)}
        for row in rows:
            cid = row[col_index["id"]]
            completeness = 0
            for c in _COMPLETENESS_COLUMNS:
                v = row[col_index[c]]
                if v is None:
                    continue
                if isinstance(v, str) and v.strip() == "":
                    continue
                completeness += 1
            out[cid] = ClubMeta(
                id=cid,
                name=row[col_index["club_name_canonical"]] or "",
                state=row[col_index["state"]],
                website=row[col_index["website"]],
                manually_merged=bool(row[col_index["manually_merged"]]),
                completeness=completeness,
            )
    return out


def _normalize_website(w: Optional[str]) -> Optional[str]:
    """Loose website comparison — protocol + trailing slash + 'www.' agnostic."""
    if not w:
        return None
    s = w.strip().lower()
    if not s:
        return None
    for prefix in ("https://", "http://"):
        if s.startswith(prefix):
            s = s[len(prefix):]
            break
    if s.startswith("www."):
        s = s[4:]
    return s.rstrip("/") or None


def pick_winner(a: ClubMeta, b: ClubMeta) -> Tuple[int, int]:
    """Pick (winner_id, loser_id) — most-complete row wins, oldest id breaks ties."""
    if a.completeness != b.completeness:
        winner = a if a.completeness > b.completeness else b
    else:
        # Older row (lower id) wins by convention — gives stable choice.
        winner = a if a.id < b.id else b
    loser = b if winner.id == a.id else a
    return winner.id, loser.id


def tier_pairs(
    pairs: List[DedupPair],
    meta_by_id: Dict[int, ClubMeta],
) -> List[TieredPair]:
    """
    Classify each pair into auto_merge / review / skip with a one-line
    reasoning string. Pure function — no DB writes.

    skip      — a row is missing from `meta_by_id` (e.g. already deleted)
                or both rows have manually_merged=true.
    auto_merge — similarity >= AUTO_MERGE_SIMILARITY AND same non-empty
                 state AND no manually_merged AND no conflicting websites.
    review     — everything else above REVIEW_MIN_SIMILARITY.
    """
    out: List[TieredPair] = []
    for p in pairs:
        a = meta_by_id.get(p.club_a_id)
        b = meta_by_id.get(p.club_b_id)

        if a is None or b is None:
            out.append(TieredPair(p, "skip", "missing canonical row"))
            continue

        if a.manually_merged or b.manually_merged:
            out.append(TieredPair(
                p, "skip",
                f"manually_merged guard (a={a.manually_merged} b={b.manually_merged})",
            ))
            continue

        # State-mismatch → review (or skip if both states are confidently set
        # to *different* values; treat absent state as unknown rather than
        # contradictory).
        state_a = (a.state or "").strip().upper() or None
        state_b = (b.state or "").strip().upper() or None
        same_state = state_a == state_b and state_a is not None

        site_a = _normalize_website(a.website)
        site_b = _normalize_website(b.website)
        websites_conflict = (
            site_a is not None and site_b is not None and site_a != site_b
        )

        if websites_conflict:
            out.append(TieredPair(
                p, "review",
                f"websites differ ({site_a} vs {site_b}); manual review",
            ))
            continue

        if not same_state:
            out.append(TieredPair(
                p, "review",
                f"state mismatch ({state_a or 'NULL'} vs {state_b or 'NULL'})",
            ))
            continue

        if p.similarity < AUTO_MERGE_SIMILARITY:
            out.append(TieredPair(
                p, "review",
                f"similarity {p.similarity:.3f} < {AUTO_MERGE_SIMILARITY:.2f}",
            ))
            continue

        winner_id, _ = pick_winner(a, b)
        out.append(TieredPair(
            p, "auto_merge",
            (
                f"sim={p.similarity:.3f} same_state={state_a} "
                f"winner_completeness={meta_by_id[winner_id].completeness}"
            ),
            recommended_winner_id=winner_id,
        ))

    return out


# ---------------------------------------------------------------------------
# Merger
# ---------------------------------------------------------------------------

@dataclass
class MergeResult:
    """Outcome of a single merge invocation."""
    loser_id: int
    winner_id: int
    committed: bool
    dry_run: bool
    rows_redirected: Dict[str, int] = field(default_factory=dict)
    rows_deleted_from_loser: Dict[str, int] = field(default_factory=dict)
    alias_inserted: bool = False
    skipped: bool = False
    skip_reason: Optional[str] = None
    error: Optional[str] = None

    def to_log_dict(self) -> dict:
        """Plain-dict form suitable for scrape_run_logs.details JSON."""
        return {
            "loser_id": self.loser_id,
            "winner_id": self.winner_id,
            "committed": self.committed,
            "dry_run": self.dry_run,
            "rows_redirected": self.rows_redirected,
            "rows_deleted_from_loser": self.rows_deleted_from_loser,
            "alias_inserted": self.alias_inserted,
            "skipped": self.skipped,
            "skip_reason": self.skip_reason,
            "error": self.error,
        }


# Spec for one FK column. The (table, column) identifies the FK; the
# `unique_cols` list (when non-empty) is the additional columns of the
# table's natural-key UQ that includes club_id — those determine where a
# straight UPDATE would collide with an existing winner row, requiring
# delete-then-leave-winner-row instead.
_REDIRECT_TABLES: List[Tuple[str, str, Tuple[str, ...]]] = [
    # table, club_id_column, additional UQ cols (excluding club_id)
    ("club_aliases", "club_id", ("alias_name",)),
    ("club_affiliations", "club_id", ("source_name",)),
    ("coach_discoveries", "club_id", ("name", "title")),
    # SET NULL FKs — straight UPDATE; no UQ involves club_id.
    ("event_teams", "canonical_club_id", ()),
    ("matches", "home_club_id", ()),
    ("matches", "away_club_id", ()),
    ("player_id_selections", "club_id", ()),
    # CASCADE FKs whose UQ key is on the raw name, not club_id — straight
    # UPDATE is safe (no composite-key collision through club_id).
    ("club_roster_snapshots", "club_id", ()),
    ("roster_diffs", "club_id", ()),
    ("tryouts", "club_id", ()),
    # CASCADE FKs with full UQ — collision possible.
    ("club_results", "club_id", (
        "season", "league", "division", "age_group", "gender",
    )),
    ("coach_scrape_snapshots", "club_id", ("scraped_at",)),
    ("club_site_changes", "club_id", (
        "snapshot_hash_before", "snapshot_hash_after", "change_type",
    )),
]


def _redirect_table(
    cur,
    table: str,
    column: str,
    unique_cols: Tuple[str, ...],
    loser_id: int,
    winner_id: int,
) -> Tuple[int, int]:
    """
    Redirect rows in `table.column` from loser_id to winner_id.

    For tables with composite uniques on (column + unique_cols):
      1. DELETE loser-side rows whose composite key already exists on the
         winner — those would conflict on UPDATE.
      2. UPDATE the rest to winner_id.

    For tables with no composite UQ involving column: a single UPDATE.

    Returns (rows_updated, rows_deleted_from_loser).
    """
    rows_deleted = 0
    if unique_cols:
        # Build a DELETE that removes loser rows whose composite key is
        # already present under winner_id. Standard Postgres pattern:
        # DELETE … USING … WHERE.
        uq_eq = " AND ".join(f"l.{c} = w.{c}" for c in unique_cols)
        delete_sql = (
            f"DELETE FROM {table} AS l "
            f"USING {table} AS w "
            f"WHERE l.{column} = %s AND w.{column} = %s "
            f"AND {uq_eq}"
        )
        cur.execute(delete_sql, (loser_id, winner_id))
        rows_deleted = cur.rowcount or 0

    update_sql = f"UPDATE {table} SET {column} = %s WHERE {column} = %s"
    cur.execute(update_sql, (winner_id, loser_id))
    rows_updated = cur.rowcount or 0
    return rows_updated, rows_deleted


def _redirect_polymorphic_career_history(
    cur, loser_id: int, winner_id: int
) -> Tuple[int, int]:
    """
    `coach_career_history` uses (entity_type, entity_id) as a polymorphic
    pointer rather than an FK. Only redirect club entries.

    UQ: (coach_id, entity_type, entity_id, role, start_year). Same
    delete-then-update treatment.
    """
    cur.execute(
        "DELETE FROM coach_career_history AS l "
        "USING coach_career_history AS w "
        "WHERE l.entity_type = 'club' AND w.entity_type = 'club' "
        "AND l.entity_id = %s AND w.entity_id = %s "
        "AND l.coach_id = w.coach_id AND l.role = w.role "
        "AND COALESCE(l.start_year, -1) = COALESCE(w.start_year, -1)",
        (loser_id, winner_id),
    )
    rows_deleted = cur.rowcount or 0
    cur.execute(
        "UPDATE coach_career_history SET entity_id = %s "
        "WHERE entity_type = 'club' AND entity_id = %s",
        (winner_id, loser_id),
    )
    rows_updated = cur.rowcount or 0
    return rows_updated, rows_deleted


def _insert_merge_alias(
    cur, loser_id: int, loser_name: str, winner_id: int
) -> bool:
    """
    Cache the loser's canonical name as an alias on the winner so
    future scrapers / linker passes resolve the loser-name string back to
    the winner.

    Idempotent on (winner_id, loser_name) via the UQ constraint. Returns
    True if a row was inserted (i.e. not pre-existing).
    """
    cur.execute(
        "INSERT INTO club_aliases ("
        "  club_id, alias_name, source, is_official, "
        "  merged_from_canonical_id, merged_at"
        ") VALUES (%s, %s, 'merger-auto', false, %s, NOW()) "
        "ON CONFLICT ON CONSTRAINT club_aliases_club_alias_uq DO NOTHING",
        (winner_id, loser_name, loser_id),
    )
    # rowcount is 0 when ON CONFLICT short-circuits the insert.
    return (cur.rowcount or 0) > 0


def _row_exists(cur, table: str, club_id_col: str, club_id: int) -> bool:
    cur.execute(
        f"SELECT 1 FROM {table} WHERE {club_id_col} = %s LIMIT 1",
        (club_id,),
    )
    return cur.fetchone() is not None


def merge_canonical_clubs(
    loser_id: int,
    winner_id: int,
    *,
    conn=None,
    dry_run: bool = False,
) -> MergeResult:
    """
    Collapse `loser_id` into `winner_id`. Single transaction; rollback on
    any error.

    Idempotency: if `loser_id` no longer exists in canonical_clubs, the
    function returns a `skipped` MergeResult without touching the DB.
    """
    if loser_id == winner_id:
        return MergeResult(
            loser_id=loser_id,
            winner_id=winner_id,
            committed=False,
            dry_run=dry_run,
            skipped=True,
            skip_reason="loser_id == winner_id",
        )

    if conn is None:
        if psycopg2 is None:
            raise RuntimeError("psycopg2 not available")
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL not set")
        conn = psycopg2.connect(dsn)
        owns_conn = True
    else:
        owns_conn = False

    result = MergeResult(
        loser_id=loser_id,
        winner_id=winner_id,
        committed=False,
        dry_run=dry_run,
    )

    try:
        with conn.cursor() as cur:
            # Existence + safety checks under the same transaction.
            cur.execute(
                "SELECT id, club_name_canonical, manually_merged "
                "FROM canonical_clubs WHERE id IN (%s, %s) FOR UPDATE",
                (loser_id, winner_id),
            )
            rows = {r[0]: r for r in cur.fetchall()}

            if winner_id not in rows:
                result.skipped = True
                result.skip_reason = f"winner_id {winner_id} not found"
                conn.rollback()
                return result

            if loser_id not in rows:
                # Idempotent no-op — already merged in a previous run.
                result.skipped = True
                result.skip_reason = f"loser_id {loser_id} already absent"
                conn.rollback()
                return result

            loser_name = rows[loser_id][1] or ""
            loser_pinned = bool(rows[loser_id][2])
            winner_pinned = bool(rows[winner_id][2])
            if loser_pinned or winner_pinned:
                result.skipped = True
                result.skip_reason = (
                    f"manually_merged guard (loser={loser_pinned} "
                    f"winner={winner_pinned})"
                )
                conn.rollback()
                return result

            # FK redirect cascade.
            for table, column, unique_cols in _REDIRECT_TABLES:
                updated, deleted = _redirect_table(
                    cur, table, column, unique_cols, loser_id, winner_id,
                )
                if updated:
                    result.rows_redirected[f"{table}.{column}"] = updated
                if deleted:
                    result.rows_deleted_from_loser[f"{table}.{column}"] = deleted

            # Polymorphic career-history redirect (no FK).
            updated, deleted = _redirect_polymorphic_career_history(
                cur, loser_id, winner_id,
            )
            if updated:
                result.rows_redirected["coach_career_history.entity_id"] = updated
            if deleted:
                result.rows_deleted_from_loser["coach_career_history.entity_id"] = deleted

            # Insert alias pointing the loser's name at the winner.
            inserted = _insert_merge_alias(
                cur, loser_id, loser_name, winner_id,
            )
            result.alias_inserted = inserted

            # Finally, drop the loser canonical row. By this point the
            # only remaining FK (if any) will have been redirected; the
            # FK relationships are cascade or set-null so a stray miss
            # would not raise, but everything we know about should be
            # gone.
            cur.execute(
                "DELETE FROM canonical_clubs WHERE id = %s",
                (loser_id,),
            )
            if (cur.rowcount or 0) == 0:
                # Race: row vanished mid-transaction. Treat as skipped.
                result.skipped = True
                result.skip_reason = "loser canonical_clubs row vanished mid-merge"
                conn.rollback()
                return result

            if dry_run:
                conn.rollback()
                result.committed = False
            else:
                conn.commit()
                result.committed = True
            return result

    except Exception as exc:  # pragma: no cover — defensive guard
        log.error("merge failed (loser=%s winner=%s): %s", loser_id, winner_id, exc)
        try:
            conn.rollback()
        except Exception:
            pass
        result.error = str(exc)
        return result
    finally:
        if owns_conn:
            try:
                conn.close()
            except Exception:
                pass
