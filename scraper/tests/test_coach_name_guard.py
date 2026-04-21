"""
Tests for the shared coach-name guard.

Covers the public surface of ``scraper.extractors._coach_name_guard``:

- ``looks_like_name(text)`` accepts real-looking names and rejects the
  pollution categories observed in a April-2026 production audit of
  ``coach_discoveries``.
- ``looks_like_name(text, counter)`` records the first-fail reason on
  the provided ``RejectCounter``.
- Every documented reason in ``REJECT_REASONS`` is reachable via a
  crafted input string.
- ``RejectCounter.summary()`` returns a copy (mutations don't leak).
- Counts accumulate across multiple calls.

Separate from ``test_youth_club_coaches.py`` — that suite exercises
the youth-club-coaches scraper end-to-end using the re-exported
symbols. This suite is scoped to the shared module's behaviour.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors._coach_name_guard import (  # noqa: E402
    PARSE_COUNT_GUARD,
    REJECT_REASONS,
    RejectCounter,
    _BLOCKLIST_TOKENS,
    _NAME_BLOCKLIST,
    looks_like_name,
)


# ---------------------------------------------------------------------------
# Pollution inputs observed in the April-2026 audit
# ---------------------------------------------------------------------------

# Real strings pulled from the pre-guard ``coach_discoveries.name``
# column in production. Each one is a different category of pollution
# the guard must reject.
POLLUTION_STRINGS = [
    "Newsletter Sign-Up",        # blocklist phrase ("sign up" after normalize)
    "OPEN TRAINING & TRYOUTS",   # all-caps
    "RELATED ARTICLES",          # all-caps
    "Saturday, April 11",        # contains digits + weekday fragment
    "⭐️ Great for all levels",    # emoji-prefixed CTA (not_alpha_start)
    "Head Coach",                # blocklist phrase
    "Follow Us",                 # blocklist phrase
]


# Real-looking names that MUST pass the guard. Chosen to cover the
# common Western-name shapes the scrapers see (Anglo, Hispanic,
# Irish, hyphenated French, short last names).
VALID_NAMES = [
    "John Smith",
    "Maria Rodriguez",
    "Kevin O'Brien",
    "Jean-Pierre Dubois",
    "Anna Lee",
]


# ---------------------------------------------------------------------------
# Happy path — valid names
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", VALID_NAMES)
def test_accepts_common_name_shapes(name: str):
    assert looks_like_name(name) is True, f"{name!r} should be accepted"


@pytest.mark.parametrize("name", VALID_NAMES)
def test_accepts_do_not_touch_counter(name: str):
    """Accepting a name must NOT bump any counter slot."""
    counter = RejectCounter()
    assert looks_like_name(name, counter) is True
    assert counter.summary() == {}
    assert counter.total() == 0


# ---------------------------------------------------------------------------
# Pollution rejects
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text", POLLUTION_STRINGS)
def test_rejects_audit_pollution_strings(text: str):
    assert looks_like_name(text) is False, f"{text!r} should be rejected"


@pytest.mark.parametrize("text", POLLUTION_STRINGS)
def test_rejects_audit_pollution_bumps_counter(text: str):
    counter = RejectCounter()
    assert looks_like_name(text, counter) is False
    # Exactly one counter slot must have been incremented (first-fail
    # short-circuits).
    summary = counter.summary()
    assert sum(summary.values()) == 1, (
        f"{text!r} should bump exactly one counter slot, got {summary!r}"
    )
    reason, count = next(iter(summary.items()))
    assert reason in REJECT_REASONS
    assert count == 1


# ---------------------------------------------------------------------------
# Reject reason coverage — each REJECT_REASONS slot is reachable
# ---------------------------------------------------------------------------

def test_too_short():
    counter = RejectCounter()
    assert looks_like_name("Al", counter) is False
    assert counter.summary() == {"too_short": 1}


def test_too_long():
    counter = RejectCounter()
    long_name = "A" + "b" * 60  # 61 chars
    assert looks_like_name(long_name, counter) is False
    assert counter.summary() == {"too_long": 1}


def test_wrong_token_count_one_token():
    counter = RejectCounter()
    # "Madonna" is 7 chars, 1 token — passes length, fails on token
    # count before any other check.
    assert looks_like_name("Madonna", counter) is False
    assert counter.summary() == {"wrong_token_count": 1}


def test_wrong_token_count_too_many_tokens():
    counter = RejectCounter()
    assert looks_like_name("A B C D E", counter) is False
    assert counter.summary() == {"wrong_token_count": 1}


def test_first_char_not_upper():
    counter = RejectCounter()
    assert looks_like_name("john Smith", counter) is False
    assert counter.summary() == {"first_char_not_upper": 1}


def test_all_caps():
    counter = RejectCounter()
    assert looks_like_name("JOHN SMITH", counter) is False
    assert counter.summary() == {"all_caps": 1}


def test_not_alpha_start():
    counter = RejectCounter()
    # "John -Smith" — two tokens, both pass length, first token's
    # first char is upper, string is mixed-case, but the second
    # token's first char is "-" (not alpha). This reason comes before
    # the digit check in ``REJECT_REASONS``.
    assert looks_like_name("John -Smith", counter) is False
    assert counter.summary() == {"not_alpha_start": 1}


def test_contains_digit():
    counter = RejectCounter()
    # "John Smith3" — starts alpha on both tokens, fails the digit
    # regex. Proves ``contains_digit`` is the reason that fires, not
    # ``not_alpha_start``.
    assert looks_like_name("John Smith3", counter) is False
    assert counter.summary() == {"contains_digit": 1}


def test_in_blocklist_phrase():
    counter = RejectCounter()
    assert looks_like_name("About Us", counter) is False
    assert counter.summary() == {"in_blocklist": 1}


def test_token_in_blocklist():
    counter = RejectCounter()
    # "John Soccer" — passes every earlier check (alpha, title-case,
    # 2 tokens, normal length, no digits, not a blocklist phrase)
    # but "soccer" is in _BLOCKLIST_TOKENS.
    assert looks_like_name("John Soccer", counter) is False
    assert counter.summary() == {"token_in_blocklist": 1}


def test_all_documented_reasons_are_reachable():
    """Every reason in REJECT_REASONS must have been exercised by at
    least one of the per-reason tests above. This is a guard against
    adding a reason to the enum and forgetting the test."""
    counter = RejectCounter()
    triggers = [
        "Al",                 # too_short
        "A" + "b" * 60,       # too_long
        "Madonna",            # wrong_token_count
        "john Smith",         # first_char_not_upper
        "JOHN SMITH",         # all_caps
        "John -Smith",        # not_alpha_start (second token starts "-")
        "John Smith3",        # contains_digit
        "About Us",           # in_blocklist
        "John Soccer",        # token_in_blocklist
    ]
    for t in triggers:
        looks_like_name(t, counter)
    summary = counter.summary()
    for reason in REJECT_REASONS:
        assert reason in summary, (
            f"reason {reason!r} was not exercised by any trigger string"
        )


# ---------------------------------------------------------------------------
# RejectCounter behaviour
# ---------------------------------------------------------------------------

class TestRejectCounter:
    def test_starts_empty(self):
        counter = RejectCounter()
        assert counter.summary() == {}
        assert counter.total() == 0

    def test_record_increments_known_reason(self):
        counter = RejectCounter()
        counter.record("too_short")
        counter.record("too_short")
        counter.record("in_blocklist")
        assert counter.summary() == {"too_short": 2, "in_blocklist": 1}
        assert counter.total() == 3

    def test_record_ignores_unknown_reason(self):
        """Defensive: a typoed reason string must not silently create a
        new slot (which would mask bugs in caller code)."""
        counter = RejectCounter()
        counter.record("too_shoooort")  # typo
        assert counter.summary() == {}
        assert counter.total() == 0

    def test_summary_returns_copy(self):
        """Callers must not be able to mutate internal state through
        the returned dict."""
        counter = RejectCounter()
        counter.record("too_short")
        snapshot = counter.summary()
        snapshot["too_short"] = 99
        assert counter.summary() == {"too_short": 1}

    def test_counter_accumulates_multiple_calls(self):
        """Hand the same counter to every call across a batch; counts
        must sum."""
        counter = RejectCounter()
        inputs = ["Al", "A B C D E", "JOHN SMITH", "John Smith3"]
        for s in inputs:
            looks_like_name(s, counter)
        assert counter.summary() == {
            "too_short": 1,
            "wrong_token_count": 1,
            "all_caps": 1,
            "contains_digit": 1,
        }
        assert counter.total() == 4

    def test_counter_is_optional(self):
        """``looks_like_name`` must work without a counter — the
        existing youth_club_coaches call sites rely on the default."""
        assert looks_like_name("JOHN SMITH") is False
        assert looks_like_name("John Smith") is True


# ---------------------------------------------------------------------------
# Module-level constants are the canonical copies
# ---------------------------------------------------------------------------

def test_parse_count_guard_is_fifteen():
    """Guard value is load-bearing — ``college_coaches.py`` and
    ``youth_club_coaches.py`` both depend on the 15-rows-is-a-false-
    positive threshold. Adjust carefully."""
    assert PARSE_COUNT_GUARD == 15


def test_blocklist_sets_are_non_empty_and_closed():
    """Sanity: both blocklist sets are populated. If a future refactor
    accidentally wipes them out, this test catches it — with empty
    sets the guard would start accepting 'Contact Us' as a coach."""
    assert len(_NAME_BLOCKLIST) > 0
    assert len(_BLOCKLIST_TOKENS) > 0
    # Spot-check known pollution entries from the audit live in the
    # sets today.
    assert "sign up" in _NAME_BLOCKLIST
    assert "head coach" in _NAME_BLOCKLIST
    assert "coach" in _BLOCKLIST_TOKENS
    assert "staff" in _BLOCKLIST_TOKENS
