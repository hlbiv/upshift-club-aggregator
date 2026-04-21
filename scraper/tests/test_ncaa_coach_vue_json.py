"""
Tests for Strategy 5 of ``extract_head_coach_from_html``: the
Sidearm Vue-embedded JSON coach extractor.

Reuses the same ``data: () => ({ roster: {...} })`` prelude + balanced-
brace extractor that Strategy 5 of ``parse_roster_html`` uses for
players, but reads a ``coaches`` / ``coaching_staff`` / ``staff`` array
instead of ``players``. Filters to strict head coach via
``_is_strict_head_coach``.

Fixture shape is synthetic — modeled on what Sidearm's backend
likely ships based on the player-side JSON structure we've observed
(first_name/last_name, title, email). Production pages' exact key
names may drift per tenant; the parser tries multiple likely keys
for each field and the tests exercise the key-name tolerance
explicitly.

Run::

    python -m pytest scraper/tests/test_ncaa_coach_vue_json.py -v
"""

from __future__ import annotations

import os
import sys
from textwrap import dedent

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ncaa_rosters import (  # noqa: E402
    _parse_sidearm_vue_embedded_head_coach,
    extract_head_coach_from_html,
)


def _wrap_vue(roster_json: str) -> str:
    """Construct a minimal HTML page with a Sidearm Vue data factory
    containing the given roster JSON literal. Matches the prelude
    pattern the extractor regex locates.
    """
    return dedent(f"""
        <html><body>
        <script>
        new Vue({{
            el: '#vue-rosters',
            data: () => ({{
                loading: false,
                roster: {roster_json}
            }}),
            methods: {{}}
        }})
        </script>
        </body></html>
    """).strip()


# ---------------------------------------------------------------------------
# Happy path — head coach extracted, associates/assistants filtered
# ---------------------------------------------------------------------------


class TestVueJsonCoachHappyPath:
    def test_extracts_head_coach_with_all_fields(self):
        html = _wrap_vue('''{
            "id": 42,
            "players": [],
            "coaches": [
                {
                    "first_name": "Jay",
                    "last_name": "Boyd",
                    "title": "Head Coach",
                    "email": "JBOYD@EXAMPLE.EDU",
                    "phone": "(650) 555-0100"
                },
                {
                    "first_name": "Abby",
                    "last_name": "Christopher",
                    "title": "Associate Head Coach"
                }
            ]
        }''')
        coach = _parse_sidearm_vue_embedded_head_coach(html)
        assert coach is not None
        assert coach["name"] == "Jay Boyd"
        assert coach["title"] == "Head Coach"
        # Email is normalized to lowercase
        assert coach["email"] == "jboyd@example.edu"
        assert coach["phone"] == "(650) 555-0100"
        assert coach["is_head_coach"] is True
        assert coach["_strategy"] == "vue-embedded-json"

    def test_filters_associate_and_assistant(self):
        """Associate Head Coach + Assistant Coach must both be rejected,
        no matter which order they appear in the coaches array."""
        html = _wrap_vue('''{
            "coaches": [
                {"first_name": "A", "last_name": "One", "title": "Associate Head Coach"},
                {"first_name": "A", "last_name": "Two", "title": "Assistant Coach"},
                {"first_name": "Real", "last_name": "HeadCoach", "title": "Head Coach"}
            ]
        }''')
        coach = _parse_sidearm_vue_embedded_head_coach(html)
        assert coach is not None
        assert coach["name"] == "Real HeadCoach"

    def test_returns_first_matching_head_coach(self):
        """If two entries both pass _is_strict_head_coach, return the first.
        Keeps behavior deterministic on pages that (incorrectly) list two
        Head Coach rows."""
        html = _wrap_vue('''{
            "coaches": [
                {"first_name": "First", "last_name": "Coach", "title": "Head Coach"},
                {"first_name": "Second", "last_name": "Coach", "title": "Head Coach"}
            ]
        }''')
        coach = _parse_sidearm_vue_embedded_head_coach(html)
        assert coach is not None
        assert coach["name"] == "First Coach"


# ---------------------------------------------------------------------------
# Key-name tolerance
# ---------------------------------------------------------------------------


class TestVueJsonCoachKeyVariants:
    def test_coaching_staff_array_key(self):
        """Some tenants use ``coaching_staff`` instead of ``coaches``."""
        html = _wrap_vue('''{
            "coaching_staff": [
                {"first_name": "Jane", "last_name": "Doe", "title": "Head Coach"}
            ]
        }''')
        coach = _parse_sidearm_vue_embedded_head_coach(html)
        assert coach is not None
        assert coach["name"] == "Jane Doe"

    def test_staff_array_key(self):
        """Some tenants use ``staff``."""
        html = _wrap_vue('''{
            "staff": [
                {"first_name": "J", "last_name": "D", "title": "Head Coach"}
            ]
        }''')
        coach = _parse_sidearm_vue_embedded_head_coach(html)
        assert coach is not None
        assert coach["name"] == "J D"

    def test_title_from_position_long_fallback(self):
        """Some entries use ``position_long`` instead of ``title``."""
        html = _wrap_vue('''{
            "coaches": [
                {"first_name": "A", "last_name": "B", "position_long": "Head Coach"}
            ]
        }''')
        coach = _parse_sidearm_vue_embedded_head_coach(html)
        assert coach is not None
        assert coach["title"] == "Head Coach"

    def test_name_from_single_field_fallback(self):
        """If first_name/last_name are missing, try ``name`` / ``full_name`` /
        ``display_name``."""
        html = _wrap_vue('''{
            "coaches": [
                {"full_name": "Solo Name", "title": "Head Coach"}
            ]
        }''')
        coach = _parse_sidearm_vue_embedded_head_coach(html)
        assert coach is not None
        assert coach["name"] == "Solo Name"


# ---------------------------------------------------------------------------
# Miss paths — regression guards (parser must not hallucinate)
# ---------------------------------------------------------------------------


class TestVueJsonCoachMiss:
    def test_no_vue_prelude_returns_none(self):
        """Non-Vue page → None, doesn't error."""
        html = "<html><body>no Vue factory here</body></html>"
        assert _parse_sidearm_vue_embedded_head_coach(html) is None

    def test_malformed_json_returns_none(self):
        """Truncated JSON blob → None, doesn't error."""
        html = dedent("""
            <html><body>
            <script>
            new Vue({
                data: () => ({
                    roster: {"players": [], "coaches": [{"name": "broken
        """).strip()
        assert _parse_sidearm_vue_embedded_head_coach(html) is None

    def test_empty_coaches_array_returns_none(self):
        html = _wrap_vue('{"players": [], "coaches": []}')
        assert _parse_sidearm_vue_embedded_head_coach(html) is None

    def test_no_coaches_key_returns_none(self):
        """Roster has players but no coaches/staff/coaching_staff key."""
        html = _wrap_vue('{"players": [{"first_name": "X", "last_name": "Y"}]}')
        assert _parse_sidearm_vue_embedded_head_coach(html) is None

    def test_only_associates_returns_none(self):
        """All coaches are Associate/Assistant; no strict head coach → None."""
        html = _wrap_vue('''{
            "coaches": [
                {"first_name": "A", "last_name": "One", "title": "Associate Head Coach"},
                {"first_name": "A", "last_name": "Two", "title": "Assistant Coach"},
                {"first_name": "A", "last_name": "Three", "title": "Volunteer Assistant"}
            ]
        }''')
        assert _parse_sidearm_vue_embedded_head_coach(html) is None

    def test_head_coach_but_name_empty_returns_none(self):
        """Title matches but name is unpopulated → caller can't write; return None."""
        html = _wrap_vue('''{
            "coaches": [
                {"first_name": "", "last_name": "", "title": "Head Coach"}
            ]
        }''')
        assert _parse_sidearm_vue_embedded_head_coach(html) is None

    def test_non_dict_coach_entries_skipped(self):
        """If the array contains non-dict entries (stringified HTML, nulls,
        etc.), skip them and keep looking."""
        html = _wrap_vue('''{
            "coaches": [
                "not a dict",
                null,
                {"first_name": "Real", "last_name": "Coach", "title": "Head Coach"}
            ]
        }''')
        coach = _parse_sidearm_vue_embedded_head_coach(html)
        assert coach is not None
        assert coach["name"] == "Real Coach"


# ---------------------------------------------------------------------------
# End-to-end integration via extract_head_coach_from_html
# ---------------------------------------------------------------------------


class TestStrategy5Integration:
    def test_extract_head_coach_from_html_hits_strategy_5(self):
        """Page with only Vue JSON (no DOM staff markup) should route to
        Strategy 5 and return the Vue-JSON coach."""
        html = _wrap_vue('''{
            "coaches": [
                {"first_name": "Pat", "last_name": "Staff", "title": "Head Coach"}
            ]
        }''')
        coach = extract_head_coach_from_html(html)
        assert coach is not None
        assert coach["name"] == "Pat Staff"
        assert coach["_strategy"] == "vue-embedded-json"

    def test_dom_strategy_preferred_over_vue_json(self):
        """If a page has BOTH a DOM-match AND Vue JSON, the earlier
        DOM-based strategy should win. Prevents double-extraction when
        the JSON matches the DOM (the common case on fully-hydrated pages)."""
        dom_markup = (
            '<div class="sidearm-staff-member">'
            '  <div class="sidearm-staff-member-title">Head Coach</div>'
            '  <h3><a>DOM Name</a></h3>'
            '</div>'
        )
        vue_json = _wrap_vue('''{
            "coaches": [
                {"first_name": "JSON", "last_name": "Name", "title": "Head Coach"}
            ]
        }''')
        combined = dom_markup + vue_json
        coach = extract_head_coach_from_html(combined)
        assert coach is not None
        assert coach["name"] == "DOM Name"
        assert coach["_strategy"] == "sidearm-staff-member"

    def test_all_strategies_miss_returns_none(self):
        """Page with no coach data anywhere → extract returns None."""
        html = "<html><body>just players, no coaches</body></html>"
        assert extract_head_coach_from_html(html) is None
