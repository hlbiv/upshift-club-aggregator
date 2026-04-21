"""
Registry-coverage gate for extractors.

For every extractor imported in ``scraper/extractors/registry.py`` this test
asserts that at least one ``scraper/tests/test_*.py`` file exercises it. The
matching heuristic is:

    the extractor's module name (e.g. ``ecnl``) appears in the file name of
    at least one test file

This is a deliberately loose heuristic — it tolerates multiple test files
per extractor (e.g. ``test_sincsports_events.py`` + ``test_sincsports_rosters.py``
both count as coverage for ``sincsports``) and does not require the test to
call ``parse_html`` directly. The intent is to fail CI when a new extractor
is added to the registry without any accompanying test file, not to police
test contents.

Explicit opt-outs live in ``COVERAGE_ALLOWLIST`` below, each with a one-line
reason string. Do not add an entry without a justification future-you can
act on.

Run:
    python -m pytest scraper/tests/test_extractor_coverage.py -v
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

REGISTRY_PATH = Path(__file__).parent.parent / "extractors" / "registry.py"
TESTS_DIR = Path(__file__).parent


# Extractors intentionally not covered by a dedicated test file. Empty today;
# kept as a documented escape hatch so the test can tell "missing coverage
# by oversight" apart from "missing coverage on purpose". Each entry must
# carry a reason string.
COVERAGE_ALLOWLIST: dict[str, str] = {
    # e.g. "some_extractor": "stub that proxies to generic path; no parse surface",
}


def _registered_extractor_modules() -> list[str]:
    """
    Parse registry.py statically and return the list of extractor module
    names it imports (one entry per ``from extractors import X`` line).

    Importing the registry for runtime discovery would work too, but parsing
    the source keeps this test hermetic — no need to install Playwright or
    any extractor's transitive deps just to check coverage.
    """
    source = REGISTRY_PATH.read_text(encoding="utf-8")
    pattern = re.compile(r"^from\s+extractors\s+import\s+(\w+)\s*", re.MULTILINE)
    modules = pattern.findall(source)
    if not modules:
        raise AssertionError(
            f"could not parse any 'from extractors import X' lines from "
            f"{REGISTRY_PATH} — has the registry's import style changed?"
        )
    return modules


def _test_filenames() -> list[str]:
    """All ``test_*.py`` files under ``scraper/tests/`` (this dir)."""
    return [p.name for p in TESTS_DIR.glob("test_*.py")]


def test_registry_is_parseable():
    modules = _registered_extractor_modules()
    # Guard against accidental empty allowlist entries.
    assert all(v for v in COVERAGE_ALLOWLIST.values()), (
        "every COVERAGE_ALLOWLIST entry must have a non-empty reason string"
    )
    # Sanity: known-good extractor names are present.
    assert "ecnl" in modules
    assert "girls_academy" in modules


@pytest.mark.parametrize("module_name", _registered_extractor_modules())
def test_every_registered_extractor_has_a_test_file(module_name: str):
    """Each registered extractor must have at least one test file whose name
    contains the extractor's module name, OR be listed in the allowlist."""
    if module_name in COVERAGE_ALLOWLIST:
        pytest.skip(
            f"{module_name}: allowlisted — {COVERAGE_ALLOWLIST[module_name]}"
        )

    test_files = _test_filenames()
    matching = [f for f in test_files if module_name in f]
    assert matching, (
        f"no test file under scraper/tests/ references extractor "
        f"'{module_name}'. Either add a test_{module_name}_parse.py using "
        f"_fixture_helpers.parse_fixture (see scraper/tests/README.md), or "
        f"add an entry to COVERAGE_ALLOWLIST in this file with a reason."
    )
