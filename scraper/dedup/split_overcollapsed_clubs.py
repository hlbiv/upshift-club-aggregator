"""
split_overcollapsed_clubs.py — audit + split canonical_clubs rows whose
attached aliases describe distinct underlying clubs (task #85).

Background: the old normalizer + linker pipeline could collapse
team-level strings like "FC Dallas 16G Pre-ECNL McAnally", "Dallas Texans
17B" and unrelated "Dallas Sting" entries onto a single canonical row
named "Dallas" via:

  * normalizer stripping "FC" -> "FC Dallas" becomes canonical "Dallas"
  * linker pass-3 token_set_ratio matching every alias containing "Dallas"
    to that single bare canonical row

Two modes:

1. Audit (default, no flags) — re-strip every alias with the post-fix
   `canonical_club_linker.strip_team_descriptors`, group by club-root,
   print rows whose aliases partition into >= 2 distinct meaningful
   roots. Read-only.

2. Apply (`--apply --canonical NAME --keep-root ROOT
   --new ROOT=NEW_NAME [--new ROOT=NEW_NAME ...]`) — split a single
   over-collapsed canonical row in one transaction:

     a. INSERT a new canonical_clubs row for each `--new ROOT=NAME`.
     b. Re-point every club_aliases row whose stripped root matches
        that ROOT at the new canonical id.
     c. For every linker-managed dependent table (event_teams, matches,
        rosters, tryouts, commitments, ynt_call_ups, odp_roster_entries,
        player_id_selections, club_roster_snapshots, roster_diffs),
        re-bucket rows whose raw-name string strips to that ROOT.
     d. Aliases / dependents whose stripped root is `--keep-root`
        (defaults to the original canonical's own name) stay on the
        original canonical row.

   Splitting is per-row and per-call — the operator names exactly
   which roots become new rows, ensuring no implicit decisions. Runs
   inside one transaction; rollback on any error. Use `--dry-run` to
   preview the redirect counts without committing.

Usage:
    # Audit
    python3 -m dedup.split_overcollapsed_clubs
    python3 -m dedup.split_overcollapsed_clubs --canonical "Dallas"

    # Split (Dallas case)
    python3 -m dedup.split_overcollapsed_clubs --apply \\
        --canonical "Dallas" \\
        --keep-root "fc dallas" \\
        --new "dallas texans=Dallas Texans" \\
        --new "dallas sting=Dallas Sting"
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

from canonical_club_linker import strip_team_descriptors  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("split_overcollapsed_clubs")


@dataclass
class OvercollapseFinding:
    canonical_id: int
    canonical_name: str
    state: Optional[str]
    alias_count: int
    distinct_roots: List[str]  # distinct stripped roots, descending count
    sample_aliases_per_root: Dict[str, List[str]]


def _stripped_root(alias: str) -> str:
    """Apply the post-fix stripper and lowercase the result."""
    s = strip_team_descriptors(alias or "")
    return s.lower().strip()


def _is_meaningful_root(root: str, canonical_root: str) -> bool:
    """A stripped-alias root is "meaningful" evidence of over-collapse iff
    it is non-empty AND has >= 2 tokens (single-token roots like "dallas"
    are exactly the canonical name itself or a degenerate alias that
    doesn't prove a distinct underlying club exists).
    """
    if not root:
        return False
    if root == canonical_root:
        return False
    return len(root.split()) >= 2


def find_overcollapsed_rows(
    conn,
    *,
    canonical_filter: Optional[str] = None,
    min_distinct_roots: int = 2,
    min_alias_count: int = 3,
) -> List[OvercollapseFinding]:
    """Scan canonical_clubs for rows whose aliases imply multiple roots.

    A row is flagged when its aliases, after running through the new
    `strip_team_descriptors`, partition into >= `min_distinct_roots`
    *meaningful* roots (see `_is_meaningful_root`: non-empty AND
    multi-token AND distinct from the canonical row's own name) AND the
    row has >= `min_alias_count` aliases overall.

    The canonical row's own root is added back into the report buckets
    so operators can see what proportion of aliases match the
    canonical name vs the over-collapsed groups.
    """
    findings: List[OvercollapseFinding] = []

    sql = (
        "SELECT cc.id, cc.club_name_canonical, cc.state, "
        "       a.alias_name "
        "FROM canonical_clubs cc "
        "LEFT JOIN club_aliases a ON a.club_id = cc.id "
        "WHERE cc.manually_merged IS NOT TRUE "
    )
    params: list = []
    if canonical_filter:
        sql += "AND LOWER(cc.club_name_canonical) = LOWER(%s) "
        params.append(canonical_filter)
    sql += "ORDER BY cc.id"

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    aliases_by_id: Dict[int, List[str]] = defaultdict(list)
    meta_by_id: Dict[int, Tuple[str, Optional[str]]] = {}
    for cid, cname, state, alias in rows:
        meta_by_id[cid] = (cname or "", state)
        if alias:
            aliases_by_id[cid].append(alias)

    for cid, aliases in aliases_by_id.items():
        cname, state = meta_by_id[cid]
        if len(aliases) < min_alias_count:
            continue
        roots = Counter(_stripped_root(a) for a in aliases)
        canonical_root = cname.lower().strip()
        meaningful_roots = {
            r: c for r, c in roots.items()
            if _is_meaningful_root(r, canonical_root)
        }
        if len(meaningful_roots) < min_distinct_roots:
            continue
        # Add the canonical-matching root back as an additional bucket
        # so the printed report shows it next to the over-collapsed
        # groups; it does NOT count toward min_distinct_roots.
        report_roots = dict(meaningful_roots)
        if any(r == canonical_root for r in roots):
            report_roots[canonical_root] = roots[canonical_root]
        sample: Dict[str, List[str]] = defaultdict(list)
        for a in aliases:
            r = _stripped_root(a)
            if r in report_roots and len(sample[r]) < 5:
                sample[r].append(a)
        ordered_roots = [
            r for r, _ in sorted(
                report_roots.items(), key=lambda kv: kv[1], reverse=True
            )
        ]
        findings.append(OvercollapseFinding(
            canonical_id=cid,
            canonical_name=cname,
            state=state,
            alias_count=len(aliases),
            distinct_roots=ordered_roots,
            sample_aliases_per_root={r: sample[r] for r in ordered_roots},
        ))

    return findings


# ---------------------------------------------------------------------------
# Splitter — operator-driven, per-row, transactional
# ---------------------------------------------------------------------------

# Tables whose rows reference canonical_clubs.id AND carry a raw team-name
# column we can re-bucket on. (canonical_id_column, raw_name_column)
_RAW_NAME_TABLES: List[Tuple[str, str, str]] = [
    ("event_teams", "canonical_club_id", "team_name_raw"),
    ("club_roster_snapshots", "club_id", "club_name_raw"),
    ("roster_diffs", "club_id", "club_name_raw"),
    ("tryouts", "club_id", "club_name_raw"),
    ("commitments", "club_id", "club_name_raw"),
    ("ynt_call_ups", "club_id", "club_name_raw"),
    ("odp_roster_entries", "club_id", "club_name_raw"),
    ("player_id_selections", "club_id", "club_name_raw"),
]

# matches has two raw-name columns sharing one row.
_MATCH_SIDES: List[Tuple[str, str]] = [
    ("home_club_id", "home_team_name"),
    ("away_club_id", "away_team_name"),
]


@dataclass
class SplitResult:
    """Outcome of one `split_canonical_row` invocation."""
    source_canonical_id: int
    source_canonical_name: str
    new_rows: Dict[str, int]  # root -> new canonical_clubs.id
    aliases_redirected: Dict[str, int] = None  # type: ignore[assignment]
    dependents_redirected: Dict[str, Dict[str, int]] = None  # type: ignore[assignment]
    committed: bool = False
    dry_run: bool = False
    skipped: bool = False
    skip_reason: Optional[str] = None
    error: Optional[str] = None

    def __post_init__(self):
        if self.aliases_redirected is None:
            self.aliases_redirected = {}
        if self.dependents_redirected is None:
            self.dependents_redirected = {}


def split_canonical_row(
    conn,
    *,
    source_canonical_id: int,
    keep_root: str,
    new_canonicals: Dict[str, str],
    state: Optional[str] = None,
    dry_run: bool = False,
) -> SplitResult:
    """Split a single over-collapsed canonical row.

    Args:
      source_canonical_id: id of the over-collapsed canonical_clubs row.
      keep_root: the lowercase stripped root that stays on
        `source_canonical_id`. Aliases / dependents matching this root
        are not moved.
      new_canonicals: ``{lowercase_root: new_canonical_name}``. For each
        entry, INSERT a new canonical_clubs row, then move every
        alias + dependent row whose stripped raw-name root matches.
      state: optional state to copy onto each new canonical row (defaults
        to the source row's state).
      dry_run: rollback at the end and report counts only.

    Returns a SplitResult. Idempotency: the function is NOT idempotent
    (re-running after a successful apply will create duplicate new rows
    if the operator passes the same args again — guard at the CLI by
    re-auditing first).
    """
    keep_root = (keep_root or "").lower().strip()
    new_canonicals = {k.lower().strip(): v for k, v in new_canonicals.items()}

    result = SplitResult(
        source_canonical_id=source_canonical_id,
        source_canonical_name="",
        new_rows={},
        dry_run=dry_run,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, club_name_canonical, state "
                "FROM canonical_clubs WHERE id = %s FOR UPDATE",
                (source_canonical_id,),
            )
            row = cur.fetchone()
            if row is None:
                result.skipped = True
                result.skip_reason = "source canonical_id not found"
                conn.rollback()
                return result
            result.source_canonical_name = row[1] or ""
            source_state = state if state is not None else row[2]

            # Step 1 — create the new canonical_clubs rows.
            for root, new_name in new_canonicals.items():
                cur.execute(
                    "INSERT INTO canonical_clubs "
                    "(club_name_canonical, state) "
                    "VALUES (%s, %s) RETURNING id",
                    (new_name, source_state),
                )
                new_id = cur.fetchone()[0]
                result.new_rows[root] = new_id
                result.dependents_redirected[root] = {}

            # Step 2 — re-point club_aliases rows.
            cur.execute(
                "SELECT id, alias_name FROM club_aliases WHERE club_id = %s",
                (source_canonical_id,),
            )
            alias_rows = cur.fetchall()
            for alias_id, alias_name in alias_rows:
                root = _stripped_root(alias_name or "")
                if root == keep_root or root not in result.new_rows:
                    continue
                target = result.new_rows[root]
                cur.execute(
                    "UPDATE club_aliases SET club_id = %s WHERE id = %s",
                    (target, alias_id),
                )
                result.aliases_redirected[root] = (
                    result.aliases_redirected.get(root, 0) + 1
                )

            # Step 3 — re-bucket dependents on raw-name string.
            for table, fk_col, raw_col in _RAW_NAME_TABLES:
                cur.execute(
                    f"SELECT id, {raw_col} FROM {table} "
                    f"WHERE {fk_col} = %s "
                    f"AND {raw_col} IS NOT NULL AND {raw_col} <> ''",
                    (source_canonical_id,),
                )
                for row_id, raw_name in cur.fetchall():
                    root = _stripped_root(raw_name)
                    if root == keep_root or root not in result.new_rows:
                        continue
                    target = result.new_rows[root]
                    cur.execute(
                        f"UPDATE {table} SET {fk_col} = %s WHERE id = %s",
                        (target, row_id),
                    )
                    bucket = result.dependents_redirected[root].setdefault(
                        f"{table}.{fk_col}", 0
                    )
                    result.dependents_redirected[root][f"{table}.{fk_col}"] = bucket + 1

            # Step 3b — matches has two FK columns per row.
            for fk_col, raw_col in _MATCH_SIDES:
                cur.execute(
                    f"SELECT id, {raw_col} FROM matches "
                    f"WHERE {fk_col} = %s "
                    f"AND {raw_col} IS NOT NULL AND {raw_col} <> ''",
                    (source_canonical_id,),
                )
                for row_id, raw_name in cur.fetchall():
                    root = _stripped_root(raw_name)
                    if root == keep_root or root not in result.new_rows:
                        continue
                    target = result.new_rows[root]
                    cur.execute(
                        f"UPDATE matches SET {fk_col} = %s WHERE id = %s",
                        (target, row_id),
                    )
                    key = f"matches.{fk_col}"
                    bucket = result.dependents_redirected[root].setdefault(key, 0)
                    result.dependents_redirected[root][key] = bucket + 1

            if dry_run:
                conn.rollback()
                result.committed = False
            else:
                conn.commit()
                result.committed = True
            return result

    except Exception as exc:
        log.error("split failed (canonical=%s): %s", source_canonical_id, exc)
        try:
            conn.rollback()
        except Exception:
            pass
        result.error = str(exc)
        return result


def print_report(findings: List[OvercollapseFinding]) -> None:
    print("=" * 80)
    print("  Overcollapsed canonical_clubs report")
    print("=" * 80)
    print(f"  Rows flagged: {len(findings)}")
    print()
    for f in findings:
        print(
            f"  id={f.canonical_id:<6} state={f.state or '??':<3} "
            f"name={f.canonical_name!r}  ({f.alias_count} aliases, "
            f"{len(f.distinct_roots)} distinct roots)"
        )
        for root in f.distinct_roots:
            samples = f.sample_aliases_per_root.get(root, [])
            print(f"    root={root!r}")
            for s in samples:
                print(f"      - {s}")
        print()
    print("=" * 80)


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def _parse_new_canonical(spec: str) -> Tuple[str, str]:
    """Parse a `--new ROOT=NAME` spec into (root, name)."""
    if "=" not in spec:
        raise argparse.ArgumentTypeError(
            f"--new value {spec!r} must be ROOT=NEW_NAME"
        )
    root, name = spec.split("=", 1)
    root = root.strip().lower()
    name = name.strip()
    if not root or not name:
        raise argparse.ArgumentTypeError(
            f"--new value {spec!r} has empty root or name"
        )
    return root, name


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Audit + split canonical_clubs rows whose aliases describe "
            "distinct underlying clubs (task #85)."
        ),
    )
    parser.add_argument(
        "--canonical", metavar="NAME",
        help="Restrict the audit / split to canonical rows with this name.",
    )
    parser.add_argument(
        "--min-roots", type=int, default=2,
        help="Minimum distinct stripped-root buckets to flag a row.",
    )
    parser.add_argument(
        "--min-aliases", type=int, default=3,
        help="Skip rows with fewer than this many aliases.",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help=(
            "Perform a split. Requires --canonical, --keep-root, "
            "and one or more --new ROOT=NAME entries."
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="With --apply, run inside a transaction and roll back at the end.",
    )
    parser.add_argument(
        "--keep-root", metavar="ROOT",
        help=(
            "(--apply) Lowercase stripped root that stays on the original "
            "canonical row. Defaults to the canonical's own name lowercased."
        ),
    )
    parser.add_argument(
        "--new", action="append", default=[], metavar="ROOT=NAME",
        type=_parse_new_canonical,
        help=(
            "(--apply) Each ROOT=NAME pair becomes a new canonical_clubs "
            "row. Aliases/dependents whose raw name strips to ROOT are "
            "moved onto the new row."
        ),
    )
    args = parser.parse_args()

    conn = _get_connection()
    try:
        if not args.apply:
            findings = find_overcollapsed_rows(
                conn,
                canonical_filter=args.canonical,
                min_distinct_roots=args.min_roots,
                min_alias_count=args.min_aliases,
            )
            print_report(findings)
            return

        # Apply path — strict argument validation.
        if not args.canonical:
            parser.error("--apply requires --canonical NAME")
        if not args.new:
            parser.error("--apply requires at least one --new ROOT=NAME")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, club_name_canonical FROM canonical_clubs "
                "WHERE LOWER(club_name_canonical) = LOWER(%s)",
                (args.canonical,),
            )
            matches = cur.fetchall()
        if not matches:
            parser.error(f"no canonical_clubs row matched name={args.canonical!r}")
        if len(matches) > 1:
            ids = ", ".join(str(m[0]) for m in matches)
            parser.error(
                f"multiple canonical_clubs rows match name={args.canonical!r} "
                f"(ids={ids}); narrow further before --apply"
            )
        cid, cname = matches[0]
        keep_root = (args.keep_root or cname or "").lower().strip()
        new_dict = dict(args.new)
        result = split_canonical_row(
            conn,
            source_canonical_id=cid,
            keep_root=keep_root,
            new_canonicals=new_dict,
            dry_run=args.dry_run,
        )
        log.info("split result: %s", result)
        print(result)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
