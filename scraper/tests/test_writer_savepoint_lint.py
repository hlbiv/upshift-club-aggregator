"""
test_writer_savepoint_lint.py — Structural guard against the bug PR 1 fixed.

Background
----------
Several `scraper/ingest/*_writer.py` modules historically used the pattern

    for row in rows:
        try:
            cur.execute(SQL, row)
        except Exception:
            conn.rollback()
            continue

inside a row loop. ``conn.rollback()`` rolls back the WHOLE transaction —
including every prior successful row in the batch and any prior pre-sweep
UPDATE that ran for THIS row. PR 1 ("savepoint-per-row isolation in
writers", commit ``fcb2499``) replaced this in the matches and
roster-snapshot writers with the correct pattern:

    for row in rows:
        cur.execute("SAVEPOINT name")
        try:
            cur.execute(SQL, row)
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT name")
            continue
        cur.execute("RELEASE SAVEPOINT name")

This test is a tripwire: if any future change reintroduces a literal
``conn.rollback()`` call inside a ``for`` loop body in a writer module,
this test fails with the file path + line number so CI catches it.

Allowlist
---------
A pre-existing offender (a writer not yet cleaned up by a later PR) can
suppress the failure with an end-of-line comment on the ``conn.rollback()``
line:

    conn.rollback()  # noqa: writer-rollback

The ``# noqa: writer-rollback`` marker explicitly admits the line is a
known TODO; removing the marker re-enables enforcement after the file is
migrated to SAVEPOINT-per-row.
"""

from __future__ import annotations

import ast
from pathlib import Path

INGEST_DIR = Path(__file__).resolve().parents[1] / "ingest"
NOQA_MARKER = "# noqa: writer-rollback"


def _is_conn_rollback_call(node: ast.AST) -> bool:
    """Return True if `node` is a Call expression matching ``conn.rollback(...)``."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "rollback"
        and isinstance(func.value, ast.Name)
        and func.value.id == "conn"
    )


def _walk_for_loops(tree: ast.AST):
    """Yield every (For-or-AsyncFor node, every descendant node inside its body)."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.For, ast.AsyncFor)):
            for stmt in node.body:
                for descendant in ast.walk(stmt):
                    yield node, descendant


def _line_has_noqa(file_lines: list[str], lineno: int) -> bool:
    """Return True iff the source line at `lineno` (1-based) carries the
    ``# noqa: writer-rollback`` marker.
    """
    if lineno < 1 or lineno > len(file_lines):
        return False
    return NOQA_MARKER in file_lines[lineno - 1]


def test_no_conn_rollback_in_writer_for_loops() -> None:
    """Fail if any writer reintroduces ``conn.rollback()`` inside a for-loop body.

    Walks every ``*.py`` file under ``scraper/ingest/``. Allowlist via
    end-of-line ``# noqa: writer-rollback`` marker on the offending line.
    """
    assert INGEST_DIR.is_dir(), f"expected ingest dir at {INGEST_DIR}"

    offenders: list[str] = []
    for py_file in sorted(INGEST_DIR.glob("*.py")):
        source = py_file.read_text(encoding="utf-8")
        try:
            tree = ast.parse(source, filename=str(py_file))
        except SyntaxError as exc:  # pragma: no cover — defensive only
            offenders.append(f"{py_file}: SyntaxError {exc}")
            continue
        file_lines = source.splitlines()
        seen_linenos: set[int] = set()
        for _for_node, descendant in _walk_for_loops(tree):
            if not _is_conn_rollback_call(descendant):
                continue
            lineno = descendant.lineno
            if lineno in seen_linenos:
                continue
            seen_linenos.add(lineno)
            if _line_has_noqa(file_lines, lineno):
                continue
            offenders.append(f"{py_file}:{lineno}: conn.rollback() inside for-loop body")

    assert not offenders, (
        "conn.rollback() inside a writer for-loop body rolls back the WHOLE "
        "transaction — use SAVEPOINT/ROLLBACK TO SAVEPOINT instead "
        "(see PR 1, commit fcb2499). To deliberately defer a fix, append "
        "'# noqa: writer-rollback' to the offending line.\n"
        + "\n".join(offenders)
    )
