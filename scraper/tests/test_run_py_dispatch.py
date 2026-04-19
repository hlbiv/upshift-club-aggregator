"""
Tests for the dict-based --source dispatch in run.py.

Refactor guardrails (Wave 2, item #0):
  - Every SOURCE_HELP key must have a handler.
  - Every SOURCE_HANDLERS key must either appear in SOURCE_HELP or be
    the kebab/snake twin of a key that does.
  - A handful of real dispatch paths round-trip correctly: the dict
    maps the key → a handler that ends up calling the underlying
    runner module.

Run:
    python -m pytest scraper/tests/test_run_py_dispatch.py -v
"""

from __future__ import annotations

import argparse
import os
import sys
import types
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _install_playwright_stub() -> None:
    """Pre-install a stub for playwright.sync_api so run.py → scraper_js
    can be imported in environments (dev laptops, CI) that don't have the
    playwright package installed. Real scraping still requires the real
    dep on Replit; this stub only makes the MODULE importable for test
    purposes."""
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
except Exception as exc:  # pragma: no cover — env without pandas etc.
    pytest.skip(f"run.py imports unavailable in this environment: {exc}", allow_module_level=True)


def _twin(key: str) -> str:
    """Return the kebab/snake twin of a source key."""
    if "-" in key:
        return key.replace("-", "_")
    if "_" in key:
        return key.replace("_", "-")
    return key


# ---------------------------------------------------------------------------
# Structural parity between SOURCE_HELP and SOURCE_HANDLERS
# ---------------------------------------------------------------------------


def test_every_help_key_has_a_handler():
    missing = [k for k in run.SOURCE_HELP if k not in run.SOURCE_HANDLERS]
    assert not missing, f"SOURCE_HELP keys missing a handler: {missing}"


def test_no_orphaned_handlers():
    """
    Every SOURCE_HANDLERS key must either be in SOURCE_HELP (canonical
    kebab form) or be the kebab/snake twin of a key that is. Anything
    else is an orphan — a handler that the --help output does not
    advertise.
    """
    orphans: list[str] = []
    for k in run.SOURCE_HANDLERS:
        if k in run.SOURCE_HELP:
            continue
        if _twin(k) in run.SOURCE_HELP:
            continue
        orphans.append(k)
    assert not orphans, f"Orphaned SOURCE_HANDLERS entries: {orphans}"


def test_snake_aliases_point_to_same_handler_as_kebab():
    """
    Snake-case aliases exist as compatibility shims. They MUST resolve
    to the exact same callable as their kebab twin, otherwise the two
    paths can drift silently.
    """
    mismatches: list[str] = []
    for k, handler in run.SOURCE_HANDLERS.items():
        twin = _twin(k)
        if twin == k:
            continue
        if twin in run.SOURCE_HANDLERS and run.SOURCE_HANDLERS[twin] is not handler:
            mismatches.append(k)
    assert not mismatches, f"Snake/kebab aliases point to different handlers: {mismatches}"


def test_build_source_help_contains_all_keys():
    """The dynamically-built --source help string must list every unique source."""
    rendered = run._build_source_help()
    for k in run.SOURCE_HELP:
        assert k in rendered, f"{k!r} missing from --source help block"


# ---------------------------------------------------------------------------
# Dispatch round-trip: pick 3 existing sources, patch the underlying runner
# module, call the handler via SOURCE_HANDLERS[key], confirm the runner ran.
# ---------------------------------------------------------------------------


def _ns(**kwargs) -> argparse.Namespace:
    """Build an argparse.Namespace with sane defaults for every run.py arg."""
    defaults = dict(
        source=None,
        rollup=None,
        dry_run=True,
        event_id=None,
        season=None,
        league_name=None,
        tid=None,
        limit=None,
        state=None,
        force=False,
        platform_family=None,
        league=None,
        priority=None,
        tier=None,
        gender=None,
        scope=None,
        teams=False,
        list=False,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _install_fake_module(monkeypatch, name: str, **attrs) -> types.ModuleType:
    """Install a fake module into sys.modules so the lazy import inside a
    handler picks it up. Returns the module so the test can inspect the
    mocks it left behind."""
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    monkeypatch.setitem(sys.modules, name, mod)
    return mod


def test_dispatch_sincsports_events_calls_runner(monkeypatch):
    run_mock = MagicMock(return_value=[])
    print_mock = MagicMock()
    _install_fake_module(
        monkeypatch,
        "events_runner",
        run_sincsports_events=run_mock,
        print_summary=print_mock,
    )

    args = _ns(source="sincsports-events", tid="GULFC")
    run.SOURCE_HANDLERS["sincsports-events"](args)

    run_mock.assert_called_once()
    assert run_mock.call_args.kwargs == {"dry_run": True, "only_tid": "GULFC"}
    print_mock.assert_called_once_with([])


def test_dispatch_tryouts_wordpress_calls_runner(monkeypatch):
    run_mock = MagicMock(return_value=[])
    print_mock = MagicMock()
    _install_fake_module(
        monkeypatch,
        "tryouts_runner",
        run_tryouts_wordpress=run_mock,
        run_tryouts=MagicMock(),
        print_summary=print_mock,
    )

    args = _ns(source="tryouts-wordpress", limit=5)
    run.SOURCE_HANDLERS["tryouts-wordpress"](args)

    run_mock.assert_called_once_with(dry_run=True, limit=5)
    print_mock.assert_called_once_with([])


def test_dispatch_link_canonical_clubs_exits_with_runner_rc(monkeypatch):
    """link-canonical-clubs handler calls sys.exit(rc). Confirm dispatch
    went through the linker module."""
    run_cli_mock = MagicMock(return_value=0)
    _install_fake_module(
        monkeypatch,
        "canonical_club_linker",
        run_cli=run_cli_mock,
    )

    args = _ns(source="link-canonical-clubs", limit=100)
    with pytest.raises(SystemExit) as ei:
        run.SOURCE_HANDLERS["link-canonical-clubs"](args)
    assert ei.value.code == 0
    run_cli_mock.assert_called_once_with(dry_run=True, limit=100)


def test_unknown_source_raises_valueerror():
    """_run_source must reject bogus keys with a clear ValueError listing
    the valid set."""
    args = _ns(source="bogus-key")
    with pytest.raises(ValueError) as ei:
        run._run_source(args)
    msg = str(ei.value)
    assert "bogus-key" in msg
    # The message should enumerate the valid unique keys.
    for sample in ("gotsport-matches", "tryouts-wordpress"):
        assert sample in msg
