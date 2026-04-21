"""
Shared test helpers for extractor fixture replay tests.

The bulk of extractor tests follow a near-identical shape:

    1. Load an HTML fixture from ``scraper/tests/fixtures/``.
    2. Call the extractor module's ``parse_html(html, source_url=..., league_name=...)``
       pure function.
    3. Assert on the returned list of dicts (row count, field shape, club names).

This module packages steps 1+2 so new extractor tests can stay focused on the
assertions. See ``scraper/tests/README.md`` for the recommended pattern and
``test_socal_parse.py`` / ``test_edp_parse.py`` / ``test_mspsp_parse.py`` for
worked examples that already use these helpers.

Intentionally minimal: fixtures are static files and extractors are pure
functions, so there's no need for DB mocking, network stubs, or Playwright
setup in this helper. Tests that need those things (composite extractors
like sincsports, orchestration tests) should NOT use this helper.
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import Any, Callable

# Ensure the scraper root is on sys.path so `from extractors import X` works
# regardless of which test file imports this helper first.
_SCRAPER_ROOT = os.path.join(os.path.dirname(__file__), "..")
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)


FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(relative_path: str | Path) -> str:
    """
    Read a fixture file as UTF-8 text.

    ``relative_path`` is resolved against ``scraper/tests/fixtures/``. Pass
    a subpath (e.g. ``"sincsports/teamlist_GULFC.html"``) for nested fixtures.
    Raises FileNotFoundError with a clear message if the fixture is missing.
    """
    path = FIXTURES_DIR / relative_path
    if not path.exists():
        raise FileNotFoundError(
            f"fixture not found: {path} "
            f"(relative to {FIXTURES_DIR})"
        )
    return path.read_text(encoding="utf-8")


def _resolve_extractor(extractor: str | Any) -> Any:
    """Accept either a module name ("ecnl") or an already-imported module."""
    if isinstance(extractor, str):
        return importlib.import_module(f"extractors.{extractor}")
    return extractor


def parse_fixture(
    extractor: str | Any,
    fixture_path: str | Path,
    *,
    source_url: str = "",
    league_name: str = "",
) -> list[dict]:
    """
    Load a fixture and run it through ``extractor.parse_html``.

    Convenience wrapper that collapses the 3-line pattern
    ``_load() → parse_html(html, ...) → assert`` into a single call.

    ``extractor`` is either:
      * a module name string like ``"ecnl"``, resolved as ``extractors.ecnl``, OR
      * an already-imported extractor module.

    Raises AttributeError with a clear message if the extractor doesn't
    expose ``parse_html`` — this signals "use a custom test, not the helper"
    rather than hiding the issue.
    """
    mod = _resolve_extractor(extractor)
    parse_fn: Callable[..., list[dict]] | None = getattr(mod, "parse_html", None)
    if parse_fn is None:
        raise AttributeError(
            f"{mod.__name__} has no parse_html(). Composite extractors "
            f"without a pure-parser entry point cannot use parse_fixture(); "
            f"write a bespoke test exercising the next-best parse function."
        )
    html = load_fixture(fixture_path)
    return parse_fn(html, source_url=source_url, league_name=league_name)
