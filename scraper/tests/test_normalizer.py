"""
Unit tests for `scraper/normalizer.py` — focused on the over-collapse
regression (task #85): single-token club prefixes like "FC Dallas" must
not normalize to "Dallas" and merge with every Dallas-area club.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from normalizer import _canonical  # noqa: E402


# ---------------------------------------------------------------------------
# Over-collapse regressions
# ---------------------------------------------------------------------------

def test_fc_dallas_does_not_strip_to_dallas():
    """`FC Dallas` is a proper club name — stripping FC would leave a
    bare 'Dallas' token that collides with every Dallas-area club.
    """
    assert _canonical("FC Dallas") == "Fc Dallas"


def test_sc_freiburg_keeps_sc_when_only_one_other_token():
    assert _canonical("SC Freiburg") == "Sc Freiburg"


def test_fc_cincinnati_keeps_fc():
    assert _canonical("FC Cincinnati") == "Fc Cincinnati"


def test_concorde_fc_strips_fc_when_multitoken_remains():
    """Trailing FC on a multi-token club name remains safe to strip."""
    assert _canonical("Concorde Fire FC") == "Concorde Fire"


def test_atlanta_united_fc_keeps_fc_when_only_atlanta_remains():
    """After stripping `United` (always-safe), only "Atlanta FC" remains.
    Stripping FC further would leave a single bare token "Atlanta" which
    is exactly the over-collapse we're guarding against — keep FC.
    """
    assert _canonical("Atlanta United FC") == "Atlanta Fc"


def test_strip_descriptors_still_works():
    """Non-proper-name tokens are still always stripped."""
    # 'Boys' / 'Youth' belong to the always-safe set.
    assert _canonical("Concorde Fire Youth Boys") == "Concorde Fire"


def test_handles_non_string():
    assert _canonical(None) == ""  # type: ignore[arg-type]
    assert _canonical(123) == ""  # type: ignore[arg-type]


def test_parenthetical_suffix_stripped():
    assert _canonical("Dallas Texans (U-12)") == "Dallas Texans"
