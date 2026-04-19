"""
Tests for the --source replay-html handler.

Covers:

1. ``ARCHIVE_RAW_HTML_ENABLED`` unset → ``fetch_archived_html`` raises
   (replay must not silently return ``None``).
2. Given N archived rows for a run_id, ``_handle_replay_html`` calls
   ``fetch_archived_html`` once per row and dispatches to the matching
   extractor when the extractor module exposes a pure-function
   ``parse_html``.
3. ``_handle_replay_html`` with a run_id that has zero archive rows
   returns cleanly (no exit) with a warning-level log message.
4. ``_handle_replay_html`` with no ``--run-id`` argument exits 2 with
   a clear error message.

The DB and Replit Object Storage SDK are both mocked — no live
resources are touched.

Run:
    python -m pytest scraper/tests/test_replay_html.py -v
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Import run.py behind the shared Playwright stub (see test_run_py_dispatch.py)
# ---------------------------------------------------------------------------

def _install_playwright_stub() -> None:
    if "playwright" in sys.modules:
        return
    pw = types.ModuleType("playwright")
    pw_sync = types.ModuleType("playwright.sync_api")

    class _StubError(Exception):
        pass

    pw_sync.sync_playwright = lambda *a, **kw: None  # type: ignore[attr-defined]
    pw_sync.TimeoutError = _StubError  # type: ignore[attr-defined]
    pw_sync.Error = _StubError  # type: ignore[attr-defined]
    sys.modules["playwright"] = pw
    sys.modules["playwright.sync_api"] = pw_sync


_install_playwright_stub()

try:
    import run  # type: ignore  # noqa: E402
    from utils import html_archive  # noqa: E402
except Exception as exc:  # pragma: no cover — env without deps
    pytest.skip(
        f"run.py imports unavailable in this environment: {exc}",
        allow_module_level=True,
    )


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_archive_state(monkeypatch):
    """Clear html_archive module-level caches + env before/after."""
    monkeypatch.delenv("ARCHIVE_RAW_HTML_ENABLED", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    html_archive._reset_for_tests()
    yield
    html_archive._reset_for_tests()


def _make_args(**overrides) -> argparse.Namespace:
    """Build an argparse Namespace that looks like a --source invocation."""
    base = dict(
        source="replay-html",
        run_id=None,
        dry_run=True,
        no_dry_run=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


# ---------------------------------------------------------------------------
# Test: no --run-id → exit 2 with clear error
# ---------------------------------------------------------------------------

def test_replay_html_requires_run_id(caplog):
    """Handler must exit 2 when --run-id is not supplied."""
    import logging as py_logging
    caplog.set_level(py_logging.ERROR, logger="run")

    with pytest.raises(SystemExit) as exc_info:
        run._handle_replay_html(_make_args(run_id=None))

    assert exc_info.value.code == 2
    assert any(
        "requires --run-id" in rec.getMessage() for rec in caplog.records
    ), f"expected a --run-id error, got: {[r.getMessage() for r in caplog.records]}"


# ---------------------------------------------------------------------------
# Test: fetch_archived_html raises when the flag is unset
# ---------------------------------------------------------------------------

def test_fetch_archived_html_requires_flag():
    """
    fetch_archived_html must raise when ARCHIVE_RAW_HTML_ENABLED != 'true'.
    Replay should fail loud, not silently return None.
    """
    with pytest.raises(RuntimeError) as exc_info:
        html_archive.fetch_archived_html("deadbeef" * 8)

    assert "ARCHIVE_RAW_HTML_ENABLED" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test: 0 rows for run_id → clean warn + return (no raise)
# ---------------------------------------------------------------------------

def test_replay_html_zero_rows_exits_cleanly(monkeypatch, caplog):
    """
    When raw_html_archive has no rows for the given run_id, the handler
    logs a warning and returns — it does NOT raise or exit non-zero.
    """
    import logging as py_logging
    caplog.set_level(py_logging.WARNING, logger="run")

    monkeypatch.setenv("DATABASE_URL", "postgres://fake/fake")

    # Install a fake psycopg2 whose cursor returns an empty rowset.
    fake_cursor = MagicMock()
    fake_cursor.fetchall.return_value = []
    fake_cursor.__enter__ = MagicMock(return_value=fake_cursor)
    fake_cursor.__exit__ = MagicMock(return_value=False)

    fake_conn = MagicMock()
    fake_conn.cursor.return_value = fake_cursor
    fake_conn.__enter__ = MagicMock(return_value=fake_conn)
    fake_conn.__exit__ = MagicMock(return_value=False)

    fake_psycopg2 = types.ModuleType("psycopg2")
    fake_psycopg2.connect = MagicMock(return_value=fake_conn)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "psycopg2", fake_psycopg2)

    # Must not raise or exit.
    run._handle_replay_html(_make_args(run_id="00000000-0000-0000-0000-000000000000"))

    msgs = [rec.getMessage() for rec in caplog.records]
    assert any(
        "no archived HTML for run_id" in m for m in msgs
    ), f"expected 'no archived HTML' warning, got: {msgs}"


# ---------------------------------------------------------------------------
# Test: 2 rows → fetch called twice + extractor dispatched
# ---------------------------------------------------------------------------

def test_replay_html_dispatches_to_pure_parser(monkeypatch, capsys):
    """
    Given 2 archived rows for a run_id, the handler must:
      - call fetch_archived_html twice (one per sha256),
      - look up the registered extractor for each source_url,
      - dispatch to the extractor module's `parse_html` when present,
      - surface the counts in the summary block.
    """
    # --- Arrange DB: cursor returns 2 rows. ------------------------------
    monkeypatch.setenv("DATABASE_URL", "postgres://fake/fake")

    sha_a = "a" * 64
    sha_b = "b" * 64

    fake_cursor = MagicMock()
    fake_cursor.fetchall.return_value = [
        (sha_a, "https://replay-extractor-a.test/page"),
        (sha_b, "https://replay-extractor-b.test/other"),
    ]
    fake_cursor.__enter__ = MagicMock(return_value=fake_cursor)
    fake_cursor.__exit__ = MagicMock(return_value=False)

    fake_conn = MagicMock()
    fake_conn.cursor.return_value = fake_cursor
    fake_conn.__enter__ = MagicMock(return_value=fake_conn)
    fake_conn.__exit__ = MagicMock(return_value=False)

    fake_psycopg2 = types.ModuleType("psycopg2")
    fake_psycopg2.connect = MagicMock(return_value=fake_conn)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "psycopg2", fake_psycopg2)

    # --- Arrange registry: register two fake extractors. -----------------
    # One has a module-level `parse_html` (pure), one does not (run-only).
    import extractors.registry as _reg

    pure_mod = types.ModuleType("fake_pure_extractor")
    pure_calls: list[dict] = []

    def _pure_parse_html(html, source_url=None, league_name=None):
        pure_calls.append({
            "html": html,
            "source_url": source_url,
            "league_name": league_name,
        })
        return [{"club_name": "fake FC", "source_url": source_url}]

    pure_mod.parse_html = _pure_parse_html  # type: ignore[attr-defined]

    def _pure_extractor(url, league_name):  # pragma: no cover — not exercised
        raise AssertionError("registered extractor should not be called during replay")

    _pure_extractor.__module__ = "fake_pure_extractor"
    sys.modules["fake_pure_extractor"] = pure_mod

    run_only_mod = types.ModuleType("fake_run_only_extractor")
    # NO parse_html → should be skipped with a warning.
    def _run_only_extractor(url, league_name):  # pragma: no cover — not exercised
        raise AssertionError("registered extractor should not be called during replay")

    _run_only_extractor.__module__ = "fake_run_only_extractor"
    sys.modules["fake_run_only_extractor"] = run_only_mod

    # Snapshot and mutate the registry; restore after the test.
    saved_registry = list(_reg._registry)
    import re as _re
    _reg._registry.append(
        (_re.compile(r"replay-extractor-a\.test", _re.IGNORECASE), _pure_extractor),
    )
    _reg._registry.append(
        (_re.compile(r"replay-extractor-b\.test", _re.IGNORECASE), _run_only_extractor),
    )

    # --- Arrange fetch_archived_html: return distinct HTML per sha256. ---
    fetch_calls: list[str] = []

    def _fake_fetch(sha256: str) -> str:
        fetch_calls.append(sha256)
        return f"<html><body>{sha256[:8]}</body></html>"

    monkeypatch.setattr(html_archive, "fetch_archived_html", _fake_fetch)

    # --- Act -------------------------------------------------------------
    try:
        run._handle_replay_html(
            _make_args(run_id="11111111-1111-1111-1111-111111111111"),
        )
    finally:
        # Restore registry to avoid polluting other tests.
        _reg._registry[:] = saved_registry
        sys.modules.pop("fake_pure_extractor", None)
        sys.modules.pop("fake_run_only_extractor", None)

    # --- Assert ----------------------------------------------------------
    assert fetch_calls == [sha_a, sha_b], (
        f"expected fetch called with [{sha_a}, {sha_b}], got {fetch_calls}"
    )

    # The pure-parser extractor was dispatched with the fetched HTML.
    assert len(pure_calls) == 1, f"expected 1 parse_html call, got {pure_calls}"
    call = pure_calls[0]
    assert call["source_url"] == "https://replay-extractor-a.test/page"
    assert call["html"].startswith("<html>")

    # Summary printed to stdout.
    captured = capsys.readouterr()
    assert "replay-html summary" in captured.out
    assert "pages_replayed" in captured.out
    # 2 rows fetched, 2 extractors matched (both URLs had registrations),
    # 1 skipped_not_pure (the run-only one), 1 row "written" from the pure one.
    assert "pages_replayed" in captured.out and " 2" in captured.out
