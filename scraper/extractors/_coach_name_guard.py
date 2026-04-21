"""
_coach_name_guard.py — Shared coach-name validation used by every
scraper that writes to ``coach_discoveries``.

Why this module exists
----------------------
A Path A audit in April 2026 found that ~90% of the rows in
``coach_discoveries`` were nav-menu text, marketing copy, CTA strings
("Newsletter Sign-Up"), article titles ("RELATED ARTICLES"), date
strings ("Saturday, April 11"), or emoji-decorated tag lines —
pollution written by HTML extractors that never validated the
``name`` field before persisting. ``youth_club_coaches.py`` had the
only working guard; the other four paths (``scrape_staff.py``,
``sportsengine_clubs.py``, ``squarespace_clubs.py``, and anything
downstream) were writing whatever string the DOM handed them.

This module lifts the existing guard into one place. Every writer
that reaches ``coach_discoveries`` MUST call ``looks_like_name()``
before constructing its row. The ``RejectCounter`` dataclass is
provided so runners can log a per-run breakdown of why rows were
dropped (critical for spotting novel pollution sources).

Design decisions preserved from the original TS port
----------------------------------------------------
* Check order is fixed: length → token count → first-char upper →
  all-caps → all-alpha-start → digit check → blocklist phrase →
  blocklist token. Early reasons short-circuit — each rejection row
  records exactly one reason, matching the observed production
  behaviour before this refactor.
* The blocklist sets are CLOSED. Additions require a PR. No runtime
  extension hooks, no env-var toggles: the whole point of the guard
  is that it's reviewed before it changes.
* ``looks_like_name()`` returns a plain ``bool`` — the optional
  ``counter`` argument is purely observational. Callers that don't
  care about reason breakdowns can ignore it (matches the contract
  in ``youth_club_coaches`` prior to this refactor).

Backcompat
----------
``youth_club_coaches.py`` re-exports ``PARSE_COUNT_GUARD``,
``_NAME_BLOCKLIST``, ``_BLOCKLIST_TOKENS``, ``RejectCounter`` and
``looks_like_name`` from this module under their pre-refactor names.
Existing import sites keep working unchanged.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Optional

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

# Strategies (selector/table branches) returning more than this many
# candidates are treated as false positives — the selector obviously
# matched something generic (all <p> tags, every nav link, every
# article heading). Same heuristic as ``college_coaches.py``.
PARSE_COUNT_GUARD = 15


# Closed-set phrases we know are NOT coach names. Matched against a
# lowercased, whitespace-collapsed, punctuation-stripped version of
# the input string. When a site prints "Meet Our Staff" as the page's
# only <h3> we do NOT want to write a coach_discovery row for it.
_NAME_BLOCKLIST = {
    "about us", "contact us", "click here", "read more", "learn more",
    "meet the", "meet our", "our staff", "our team", "coaching staff",
    "support staff", "athletic staff", "staff directory",
    "head coach", "assistant coach", "associate head", "associate coach",
    "volunteer coach", "graduate assistant", "director of coaching",
    "director of operations", "athletic director", "technical director",
    "club director", "club president", "executive director",
    "men soccer", "women soccer", "mens soccer", "womens soccer",
    "soccer coaches", "coaching team", "soccer team",
    "social media", "quick links", "campus map", "office hours",
    "follow us", "connect with", "stay connected", "more information",
    "sign up", "log in", "new member", "member login",
}


# Closed-set individual TOKENS (single whole words) that a human name
# will never contain. If ANY token of the candidate string matches,
# reject. Catches sloppier extractors that grab "Coach" or "Director"
# out of nav menus without a matching blocklist phrase firing.
_BLOCKLIST_TOKENS = {
    "soccer", "football", "basketball", "baseball", "softball", "volleyball",
    "lacrosse", "swimming", "tennis", "golf", "track", "wrestling",
    "coach", "coaching", "staff", "director", "athletic", "athletics",
    "university", "college", "school", "program", "department", "club",
    "email", "phone", "fax", "office", "contact", "bio",
    "schedule", "roster", "recruiting", "camps", "news", "media",
    "facebook", "twitter", "instagram", "youtube", "tiktok",
    "home", "about", "menu", "search", "login", "signup", "register",
    "calendar", "events", "tournament", "league", "division",
}


# Enumeration of reasons ``looks_like_name`` can reject a string, in
# the ORDER the function checks them. Each reason fires at most once
# per input (first-match-wins). The tuple is exported so tests can
# assert every reason is reachable — if we add a new reason we have
# to extend both the function AND this list.
REJECT_REASONS: tuple[str, ...] = (
    "too_short",           # len < 4
    "too_long",            # len > 50
    "wrong_token_count",   # < 2 tokens or > 4 tokens
    "first_char_not_upper",  # first token's first char not uppercase
    "all_caps",            # entire string uppercase
    "not_alpha_start",     # any token's first char not alphabetic
    "contains_digit",      # any digit anywhere in the string
    "in_blocklist",        # normalized phrase matches _NAME_BLOCKLIST
    "token_in_blocklist",  # any token matches _BLOCKLIST_TOKENS
)


# ---------------------------------------------------------------------------
# RejectCounter — optional per-run reason breakdown
# ---------------------------------------------------------------------------

@dataclass
class RejectCounter:
    """Counts coach-name rejections by reason.

    Intended use::

        counter = RejectCounter()
        for raw in candidates:
            if not looks_like_name(raw, counter):
                continue
            ...
        logger.info("name-guard rejects: %s", counter.summary())

    ``summary()`` returns a dict snapshot (callers cannot mutate the
    internal state through the returned object). ``record()`` is called
    internally by ``looks_like_name`` — most external callers just
    construct the counter and pass it in.
    """

    counts: Dict[str, int] = field(default_factory=dict)

    def record(self, reason: str) -> None:
        """Increment the counter for ``reason``. Ignores reasons that
        aren't in ``REJECT_REASONS`` (defensive — the module's own
        function is the only caller and it only passes known reasons)."""
        if reason not in REJECT_REASONS:
            return
        self.counts[reason] = self.counts.get(reason, 0) + 1

    def summary(self) -> Dict[str, int]:
        """Return a COPY of the current counts dict. Callers cannot
        mutate the counter through the returned dict."""
        return dict(self.counts)

    def total(self) -> int:
        """Sum of all rejection counts across all reasons."""
        return sum(self.counts.values())


# ---------------------------------------------------------------------------
# looks_like_name — the actual guard
# ---------------------------------------------------------------------------

def looks_like_name(text: str, counter: Optional[RejectCounter] = None) -> bool:
    """Return True if *text* looks like a person name (2-4 Title-case
    tokens, no digits, not all-caps, not a blocklisted phrase/token).

    Checks are applied in the order documented in ``REJECT_REASONS``.
    The first failing check short-circuits — when ``counter`` is
    provided the rejection reason is recorded on it.

    Parameters
    ----------
    text : str
        The candidate name. Whitespace is stripped before validation;
        passing ``""`` or ``"   "`` rejects with ``too_short``.
    counter : RejectCounter, optional
        If provided, the first failing check records its reason on
        this counter. Accept calls do NOT touch the counter.

    Returns
    -------
    bool
        True iff the string passes every check.
    """
    t = text.strip()
    if len(t) < 4:
        if counter is not None:
            counter.record("too_short")
        return False
    if len(t) > 50:
        if counter is not None:
            counter.record("too_long")
        return False
    parts = t.split()
    if len(parts) < 2 or len(parts) > 4:
        if counter is not None:
            counter.record("wrong_token_count")
        return False
    if not parts[0][0].isupper():
        if counter is not None:
            counter.record("first_char_not_upper")
        return False
    if t == t.upper():
        if counter is not None:
            counter.record("all_caps")
        return False
    if not all(p[0].isalpha() for p in parts):
        if counter is not None:
            counter.record("not_alpha_start")
        return False
    if re.search(r"\d", t):
        if counter is not None:
            counter.record("contains_digit")
        return False
    normalized = re.sub(r"[^a-z ]", "", t.lower()).strip()
    normalized = re.sub(r"\s+", " ", normalized)
    if normalized in _NAME_BLOCKLIST:
        if counter is not None:
            counter.record("in_blocklist")
        return False
    lower_parts = [re.sub(r"[^a-z]", "", p.lower()) for p in parts]
    if any(p in _BLOCKLIST_TOKENS for p in lower_parts):
        if counter is not None:
            counter.record("token_in_blocklist")
        return False
    return True
