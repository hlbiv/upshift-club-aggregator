"""
test_partial_index_writer_alignment.py — Structural guard against the bug
PR 2 / PR 3 / PR 4 fixed: a partial-unique-index COALESCE expression
diverging from the equivalent COALESCE expression in a writer's SQL.

Background
----------
Several writers in `scraper/ingest/` perform pre-sweep UPDATEs whose
WHERE clauses must byte-match the corresponding partial unique index
expressions in Drizzle schema. Postgres compares index expressions
text-exact, so even a whitespace or sentinel mismatch on a COALESCE
makes the writer fail to find rows that the index would otherwise
match — producing duplicate rows or missed upgrades.

The hand-rolled SQL in `scraper/ingest/matches_writer.py:_PRESWEEP_PLATFORM_ID`
is the canonical example: its WHERE clause repeats the same COALESCE
expressions as the `matches_natural_key_uq` partial unique index in
`lib/db/src/schema/matches.ts`. Diverge them and the split-brain guard
silently breaks.

Scope
-----
This guard targets ONLY the `matches` table — the one writer where the
divergence pattern is real risk (the writer SQL explicitly repeats
COALESCE expressions and must byte-match the index for the WHERE
predicate to identify the same row).

Other tables with COALESCE in partial unique indexes (`roster_diffs`,
`tryouts`) currently use `ON CONFLICT ON CONSTRAINT <name>` exclusively
in their writers and do NOT repeat the COALESCE expressions in writer
SQL — there is nothing to byte-match, so no divergence risk. If a future
PR introduces a pre-sweep pattern in `tryouts_writer.py` or
`roster_snapshot_writer.py`, extend `_TABLE_TO_WRITER` to cover it.

How it works
------------
1. Regex-parse `lib/db/src/schema/matches.ts` looking for
   ``uniqueIndex("...").on(...)`` blocks for tables in `_TABLE_TO_WRITER`.
2. Extract every ``sql\\`COALESCE(${t.<colCamel>}, '<sentinel>'<cast?>)`\\``
   inside that .on(...) block.
3. Convert the camelCase column to snake_case (Drizzle's
   ``"col_name"`` → ``colName`` mapping is conventional and stable in
   this repo's schema).
4. Grep the mapped writer file for the byte-equivalent
   ``COALESCE(<col_snake>, '<sentinel>'<cast?>)``.
5. Fail with a greppable message naming the index + writer + missing
   expression. To deliberately defer a fix, add the index name to
   `_ALLOWED_DIVERGENCES`.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = REPO_ROOT / "lib" / "db" / "src" / "schema"
INGEST_DIR = REPO_ROOT / "scraper" / "ingest"

# Mapping: schema-file-stem → list of (drizzle table-const name → consumer writer file).
# Only tables where the writer contains its own COALESCE expressions (and
# therefore must byte-match the index) are listed. See module docstring.
_TABLE_TO_WRITER: dict[str, dict[str, Path]] = {
    "matches": {
        "matches": INGEST_DIR / "matches_writer.py",
    },
}

# Allowlist of (schema_file, table_name, column_snake, sentinel) tuples.
# Add an entry here to deliberately defer a fix; remove it after the
# divergence is repaired. Empty by default.
_ALLOWED_DIVERGENCES: set[tuple[str, str, str, str]] = set()


# Drizzle column-name convention in this repo:
#   `text("home_team_name")` exported as `homeTeamName`.
# So the JS-side `${t.homeTeamName}` substitution emits `home_team_name`
# in the rendered SQL string. We invert that mapping here.
def _camel_to_snake(name: str) -> str:
    out: list[str] = []
    for i, ch in enumerate(name):
        if ch.isupper() and i > 0:
            out.append("_")
            out.append(ch.lower())
        else:
            out.append(ch.lower())
    return "".join(out)


# Match `uniqueIndex("name").on(...)` — capture the index name and the
# .on(...) argument list. The .on(...) block can be multi-line.
_UNIQUE_INDEX_BLOCK_RE = re.compile(
    r"""uniqueIndex\(\s*"(?P<name>[^"]+)"\s*\)\s*\.on\(\s*(?P<args>(?:[^()]|\([^()]*\))*)\)""",
    re.DOTALL,
)

# Match `sql\`COALESCE(${t.<colCamel>}, '<sentinel>'<optional cast>)\``.
# Cast may be ``::timestamp``, ``::int``, etc. — captured greedily up to
# the close-paren.
_COALESCE_RE = re.compile(
    r"""sql`COALESCE\(\$\{t\.(?P<col>[A-Za-z_][A-Za-z0-9_]*)\}\s*,\s*'(?P<sentinel>[^']*)'(?P<cast>(?:::[A-Za-z_][A-Za-z0-9_]*)?)\)`"""
)


def _parse_index_coalesces(schema_text: str) -> list[tuple[str, str, str, str]]:
    """Return list of (index_name, column_snake, sentinel, cast) tuples."""
    found: list[tuple[str, str, str, str]] = []
    for m in _UNIQUE_INDEX_BLOCK_RE.finditer(schema_text):
        idx_name = m.group("name")
        args = m.group("args")
        for c in _COALESCE_RE.finditer(args):
            col_snake = _camel_to_snake(c.group("col"))
            sentinel = c.group("sentinel")
            cast = c.group("cast") or ""
            found.append((idx_name, col_snake, sentinel, cast))
    return found


def test_partial_index_coalesces_byte_match_writer_sql() -> None:
    """Every COALESCE in a partial unique index appears verbatim in the
    consumer writer's SQL.

    See module docstring for scope and rationale.
    """
    assert SCHEMA_DIR.is_dir(), f"expected schema dir at {SCHEMA_DIR}"

    failures: list[str] = []
    for schema_stem, table_to_writer in _TABLE_TO_WRITER.items():
        schema_path = SCHEMA_DIR / f"{schema_stem}.ts"
        assert schema_path.is_file(), f"missing schema file {schema_path}"
        schema_text = schema_path.read_text(encoding="utf-8")
        coalesces = _parse_index_coalesces(schema_text)
        assert coalesces, (
            f"no COALESCE-bearing uniqueIndex blocks found in {schema_path} — "
            "either the schema was reshaped (update _TABLE_TO_WRITER) or "
            "the regex needs refresh"
        )

        for table_name, writer_path in table_to_writer.items():
            assert writer_path.is_file(), f"missing writer file {writer_path}"
            writer_text = writer_path.read_text(encoding="utf-8")
            for idx_name, col_snake, sentinel, cast in coalesces:
                if (schema_stem, table_name, col_snake, sentinel) in _ALLOWED_DIVERGENCES:
                    continue
                expected = f"COALESCE({col_snake}, '{sentinel}'{cast})"
                if expected not in writer_text:
                    failures.append(
                        f"{schema_path.name}::{idx_name}: writer "
                        f"{writer_path.name} missing byte-equivalent "
                        f"`{expected}` (required to match the partial "
                        f"unique index expression)"
                    )

    assert not failures, (
        "Partial-unique-index COALESCE expressions diverged from writer "
        "SQL — Postgres compares index expressions text-exact, so a "
        "mismatch silently breaks the writer's row-lookup. To "
        "deliberately defer a fix, add a tuple to _ALLOWED_DIVERGENCES "
        "in this test module.\n"
        + "\n".join(failures)
    )
