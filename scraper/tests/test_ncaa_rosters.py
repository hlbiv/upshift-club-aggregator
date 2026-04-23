"""
Tests for the NCAA roster scraper.

Extraction tests run against fixture HTML files. DB-write tests stub
psycopg2 to verify dry_run behaviour without Postgres.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest import mock

import pytest

# Ensure scraper package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ncaa_soccer_rosters import (  # noqa: E402
    normalize_year,
    parse_roster_html,
    build_column_index,
    ColumnIndex,
    current_academic_year,
    scrape_college_rosters,
    _fetch_colleges,
    extract_head_coach_from_html,
    compose_coaches_urls,
    probe_coaches_pages,
    _coaches_cache_key,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ncaa"


def _read(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# parse_roster_html — table strategy
# ---------------------------------------------------------------------------


class TestParseRosterTable:
    """Strategy 2: header-aware <table> parsing."""

    def test_extracts_all_players(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        assert len(players) == 6

    def test_player_names(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        names = [p.player_name for p in players]
        assert "Emma Johnson" in names
        assert "Sofia Martinez" in names
        assert "Aisha Williams" in names
        assert "Riley O'Connor" in names
        assert "Mei Chen" in names
        assert "Daniela Ruiz-Fernandez" in names

    def test_jersey_numbers(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        assert by_name["Emma Johnson"].jersey_number == "1"
        assert by_name["Sofia Martinez"].jersey_number == "7"
        assert by_name["Mei Chen"].jersey_number == "22"

    def test_positions(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        assert by_name["Emma Johnson"].position == "GK"
        assert by_name["Sofia Martinez"].position == "MF"
        assert by_name["Aisha Williams"].position == "FW"
        assert by_name["Riley O'Connor"].position == "DF"

    def test_year_normalized(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        assert by_name["Emma Johnson"].year == "senior"
        assert by_name["Sofia Martinez"].year == "junior"
        assert by_name["Aisha Williams"].year == "freshman"
        assert by_name["Riley O'Connor"].year == "freshman"  # RS-Fr → freshman
        assert by_name["Mei Chen"].year == "grad"
        assert by_name["Daniela Ruiz-Fernandez"].year == "sophomore"

    def test_hometown(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        assert by_name["Emma Johnson"].hometown == "Alpharetta, Ga."
        assert by_name["Aisha Williams"].hometown == "Manchester, England"

    def test_prev_club(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        assert by_name["Emma Johnson"].prev_club == "Concorde Fire"
        assert by_name["Sofia Martinez"].prev_club == "Orlando City YS"


# ---------------------------------------------------------------------------
# parse_roster_html — Sidearm strategy
# ---------------------------------------------------------------------------


class TestParseSidearmRoster:
    """Strategy 1: Sidearm roster elements."""

    def test_extracts_sidearm_players(self):
        html = _read("sidearm_roster.html")
        players = parse_roster_html(html)
        assert len(players) == 2

    def test_sidearm_fields(self):
        html = _read("sidearm_roster.html")
        players = parse_roster_html(html)
        marcus = players[0]
        assert marcus.player_name == "Marcus Thompson"
        assert marcus.jersey_number == "9"
        assert marcus.position == "Forward"
        assert marcus.year == "junior"
        assert marcus.hometown == "Atlanta, Ga"
        assert marcus.prev_club == "Atlanta United Academy"

    def test_sidearm_redshirt(self):
        html = _read("sidearm_roster.html")
        players = parse_roster_html(html)
        javier = players[1]
        assert javier.player_name == "Javier Lopez"
        assert javier.year == "sophomore"  # R-So → sophomore


# ---------------------------------------------------------------------------
# parse_roster_html — card strategy
# ---------------------------------------------------------------------------


class TestParseCardRoster:
    """Strategy 3: card/div layout."""

    def test_extracts_card_players(self):
        html = _read("card_roster.html")
        players = parse_roster_html(html)
        assert len(players) == 2

    def test_card_fields(self):
        html = _read("card_roster.html")
        players = parse_roster_html(html)
        kwame = players[0]
        assert kwame.player_name == "Kwame Asante"
        assert kwame.jersey_number == "11"
        assert kwame.position == "MF"
        assert kwame.year == "grad"  # 5th → grad
        assert kwame.hometown == "Accra, Ghana"


# ---------------------------------------------------------------------------
# parse_roster_html — Sidearm Vue-embedded JSON strategy
# ---------------------------------------------------------------------------


class TestParseSidearmVueEmbeddedJson:
    """Strategy 5: Sidearm classic sites whose roster <li> elements are
    never rendered server-side, but ship the full player list inline
    inside a ``new Vue({ data: () => ({ roster: {...} }) })`` block.

    Fixture is a trimmed real capture from gomason.com (George Mason
    men's soccer) — one of the D1 programs where both the static
    SIDEARM-card parser AND the Playwright fallback returned 0 players
    in production before this strategy existed.
    """

    def test_extracts_at_least_ten_players(self):
        html = _read("sidearm_vue_embedded_sample.html")
        players = parse_roster_html(html)
        assert len(players) >= 10, (
            f"expected >=10 players from Vue-embedded JSON, got {len(players)}"
        )

    def test_first_player_fields(self):
        html = _read("sidearm_vue_embedded_sample.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        # John Balkey is the first player in the George Mason roster JSON
        assert "John Balkey" in by_name
        balkey = by_name["John Balkey"]
        assert balkey.jersey_number == "12"
        assert balkey.position == "M"
        assert balkey.year == "sophomore"  # "So." normalizes to sophomore
        assert balkey.hometown == "Leesburg, Va"

    def test_prev_club_falls_back_to_highschool(self):
        """When ``previous_school`` is blank, the parser uses ``highschool``."""
        html = _read("sidearm_vue_embedded_sample.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        # Balkey has previous_school="" and highschool="Riverside High School"
        # — the parser should prefer previous_school but fall through when empty.
        assert by_name["John Balkey"].prev_club == "Riverside High School"

    def test_redshirt_year_normalized(self):
        """RS-year formats in ``academic_year_short`` map to the base enum."""
        html = _read("sidearm_vue_embedded_sample.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        # Jack Desroches is R-Sr. in the source JSON — should normalize to "senior"
        assert by_name["Jack Desroches"].year == "senior"


# ---------------------------------------------------------------------------
# extract_head_coach_from_html — inline coach selector strategies (Task #34)
# ---------------------------------------------------------------------------


class TestExtractHeadCoachInline:
    """Strategies 2-4 of ``extract_head_coach_from_html``: inline coach
    markup variants discovered in the Task #34 diagnostic probe of
    cached D1 men's-soccer roster pages.

    Strategy 1 (legacy ``.sidearm-staff-member``) already has implicit
    coverage via ``sample_staff_sidearm.html`` in the dry-run + writer
    tests; we add an explicit assertion below so any regression in the
    legacy path also fails.
    """

    def test_legacy_sidearm_staff_member(self):
        """Strategy 1 — the original `.sidearm-staff-member` block."""
        html = _read("sample_staff_sidearm.html")
        out = extract_head_coach_from_html(html)
        assert out is not None, "legacy sidearm-staff-member strategy regressed"
        assert out["name"] == "John Smith"
        assert "Head Coach" in out["title"]
        assert out["is_head_coach"] is True
        assert out["email"] == "jsmith@testuniv.edu"

    def test_modern_sidearm_s_person_card(self):
        """Strategy 2 — modern SIDEARM `.s-person-card` markup. The
        dominant pattern across current D1 roster pages (Xavier, Ohio
        State, Providence, Seton Hall, etc.). Must pick the plain
        "Head Coach" card and skip both player cards AND the
        "Associate Head Coach" card sitting next to it.
        """
        html = _read("sidearm_s_person_card_head_coach.html")
        out = extract_head_coach_from_html(html)
        assert out is not None
        assert out["name"] == "John Higgins"
        assert out["title"] == "Head Coach"
        assert out["is_head_coach"] is True
        # Email + phone live in the contact-details sub-block.
        assert out["email"] == "higginsj5@xavier.edu"
        assert out["phone"] == "513.745.3879"

    def test_s_person_card_skips_associate_when_no_real_head(self):
        """Even when only Associate / Assistant Head Coach cards exist,
        we must NOT misclassify them as the head coach. Returning None
        lets the caller fall back to the separate /coaches page.
        """
        html = """
        <div class="s-person-card">
          <div class="s-person-details__personal-single-line">Jane Doe</div>
          <div class="s-person-details__position">Associate Head Coach</div>
        </div>
        <div class="s-person-card">
          <div class="s-person-details__personal-single-line">Bob Roe</div>
          <div class="s-person-details__position">Assistant Head Coach</div>
        </div>
        """
        assert extract_head_coach_from_html(html) is None

    def test_s_person_card_aria_label_name_fallback(self):
        """When `.s-person-details__personal-single-line` is absent (older
        payload variant), the parser falls back to the `aria-label` on
        the bio link, which is consistently "<Name> full bio".
        """
        html = """
        <div class="s-person-card">
          <a aria-label="Pat Example full bio" href="/staff/pat-example">link</a>
          <div class="s-person-details__position">Head Coach</div>
        </div>
        """
        out = extract_head_coach_from_html(html)
        assert out is not None
        assert out["name"] == "Pat Example"
        assert out["is_head_coach"] is True

    def test_legacy_sidearm_roster_coach_inline(self):
        """Strategy 3 — `.sidearm-roster-coach` block embedded directly
        on the roster page (Portland-style)."""
        html = _read("sidearm_inline_roster_coach.html")
        out = extract_head_coach_from_html(html)
        assert out is not None
        assert out["name"] == "Nick Carlin-Voigt"
        assert out["title"] == "Head Coach"
        assert out["is_head_coach"] is True

    def test_wmt_vue_staff_card(self):
        """Strategy 4 — Stanford-style `.roster-staff-members-card-item`
        with title in `.roster-card-item__position` and name in
        `.roster-card-item__title`. Must pick the plain "Head Coach"
        card, not the Associate/Assistant cards.
        """
        html = _read("staff_card_position_title.html")
        out = extract_head_coach_from_html(html)
        assert out is not None
        assert out["name"] == "Jeremy Gunn"
        assert out["title"] == "Head Coach"
        assert out["is_head_coach"] is True

    def test_returns_none_for_pure_player_roster(self):
        """A roster page with only players and no coach blocks must
        return None so the caller can fall back to a separate /coaches
        probe instead of silently dropping head-coach signal.
        """
        html = """
        <html><body>
          <div class="s-person-card">
            <div class="s-person-details__personal-single-line">Player One</div>
            <div class="s-person-details__position">Forward</div>
          </div>
          <div class="s-person-card">
            <div class="s-person-details__personal-single-line">Player Two</div>
            <div class="s-person-details__position">Midfielder</div>
          </div>
        </body></html>
        """
        assert extract_head_coach_from_html(html) is None

    @pytest.mark.parametrize(
        "subordinate_title",
        [
            "Associate Head Coach",
            "Assistant Head Coach",
            "Assoc. Head Coach",
            "Assoc Head Coach",
            "Asst. Head Coach",
            "Asst Head Coach",
            "Assistant to the Head Coach",
            "Volunteer Assistant Head Coach",
        ],
    )
    def test_subordinate_head_coach_titles_are_rejected(self, subordinate_title):
        """Negative-coverage matrix: every subordinate-of-head-coach
        variant we've seen on D1-D3 staff cards must NOT be promoted to
        head coach, even when it's the only card on the page. The
        caller relies on a None return to fall back to a /coaches probe.
        """
        html = f"""
        <div class="s-person-card">
          <div class="s-person-details__personal-single-line">Pat Example</div>
          <div class="s-person-details__position">{subordinate_title}</div>
        </div>
        """
        assert extract_head_coach_from_html(html) is None, (
            f"subordinate title {subordinate_title!r} was misclassified as head coach"
        )


# ---------------------------------------------------------------------------
# normalize_year — exhaustive edge cases
# ---------------------------------------------------------------------------


class TestComposeCoachesUrls:
    """Pure URL-discovery for the PR-9 coaches-page fallback."""

    def test_basic_roster_url(self):
        urls = compose_coaches_urls("https://athletics.example.edu/sports/mens-soccer/roster")
        assert urls == [
            "https://athletics.example.edu/sports/mens-soccer/coaches",
            "https://athletics.example.edu/sports/mens-soccer/coaches-and-staff",
            "https://athletics.example.edu/sports/mens-soccer/staff",
            "https://athletics.example.edu/sports/mens-soccer/staff-directory",
        ]

    def test_strips_trailing_slash_after_roster(self):
        urls = compose_coaches_urls("https://x.edu/sports/wsoc/roster/")
        assert urls[0] == "https://x.edu/sports/wsoc/coaches"

    def test_handles_url_without_roster_suffix(self):
        # When the input doesn't end in /roster, candidates are appended
        # as-is to the base. Real callers only ever pass /roster URLs,
        # but the function should not corrupt other inputs.
        urls = compose_coaches_urls("https://x.edu/sports/mens-soccer")
        assert urls[0] == "https://x.edu/sports/mens-soccer/coaches"

    def test_strips_query_and_fragment(self):
        urls = compose_coaches_urls(
            "https://x.edu/sports/mens-soccer/roster?season=2025#top"
        )
        assert urls[0] == "https://x.edu/sports/mens-soccer/coaches"

    def test_case_insensitive_roster_strip(self):
        urls = compose_coaches_urls("https://x.edu/sports/mens-soccer/Roster")
        assert urls[0] == "https://x.edu/sports/mens-soccer/coaches"

    def test_empty_url_raises(self):
        with pytest.raises(ValueError):
            compose_coaches_urls("")

    def test_cache_key_is_program_scoped(self):
        # Men's and women's at the same host should NOT collide.
        mk = _coaches_cache_key("https://x.edu/sports/mens-soccer/roster")
        wk = _coaches_cache_key("https://x.edu/sports/womens-soccer/roster")
        assert mk != wk
        # But repeat calls for the same program should match (case +
        # trailing slash + roster suffix all normalized).
        assert mk == _coaches_cache_key("https://x.edu/sports/mens-soccer/Roster/")


class TestProbeCoachesPages:
    """End-to-end test for the PR-9 fallback's HTTP + extract loop.

    Mocks ``fetch_with_retry`` to simulate an athletics CMS where the
    roster page is JS-rendered (returns a shell with no inline coach
    markup) but ``/coaches`` server-renders a head-coach card.
    """

    def _fake_session(self):
        return mock.MagicMock()

    @mock.patch("extractors.ncaa_soccer_rosters.fetch_with_retry")
    def test_finds_head_coach_when_roster_misses(self, mock_fetch):
        coaches_html = _read("coaches_page_server_rendered.html")
        # First candidate (/coaches) returns the staff page; nothing
        # else should be probed because the loop returns on first hit.
        mock_fetch.return_value = coaches_html

        result = probe_coaches_pages(
            self._fake_session(),
            "https://athletics.example.edu/sports/mens-soccer/roster",
        )
        assert result is not None
        assert result["name"] == "Marcus Reyes"
        assert result["title"] == "Head Coach"
        assert result["_strategy"] == "coaches-page-fallback:sidearm-staff-member"
        assert result["_source_url"] == (
            "https://athletics.example.edu/sports/mens-soccer/coaches"
        )
        # Only the first candidate URL should have been fetched.
        assert mock_fetch.call_count == 1

    @mock.patch("extractors.ncaa_soccer_rosters.fetch_with_retry")
    def test_inline_extractor_gives_up_on_js_shell(self, _mock_fetch):
        # Sanity check: the JS-shell fixture genuinely has no inline
        # coach markup, so the inline extractor returns None and the
        # fallback is the only path that can recover the coach.
        shell = _read("js_rendered_roster_shell.html")
        assert extract_head_coach_from_html(shell) is None

    @mock.patch("extractors.ncaa_soccer_rosters.fetch_with_retry")
    def test_returns_none_when_all_candidates_miss(self, mock_fetch):
        # Every candidate returns either nothing or HTML with no head
        # coach markup. Should exhaust all 4 candidates and return None.
        mock_fetch.return_value = _read("js_rendered_roster_shell.html")
        result = probe_coaches_pages(
            self._fake_session(),
            "https://x.edu/sports/mens-soccer/roster",
        )
        assert result is None
        assert mock_fetch.call_count == 4  # 4 candidates, all probed

    @mock.patch("extractors.ncaa_soccer_rosters.fetch_with_retry")
    def test_skips_too_short_responses(self, mock_fetch):
        # A 200 with a tiny body (e.g. an error JSON) should be
        # treated as a miss without trying to parse it.
        mock_fetch.return_value = "<html><body>Not Found</body></html>"
        result = probe_coaches_pages(
            self._fake_session(),
            "https://x.edu/sports/mens-soccer/roster",
        )
        assert result is None
        assert mock_fetch.call_count == 4

    @mock.patch("extractors.ncaa_soccer_rosters.fetch_with_retry")
    def test_cache_short_circuits_repeat_probe(self, mock_fetch):
        coaches_html = _read("coaches_page_server_rendered.html")
        mock_fetch.return_value = coaches_html
        cache: dict = {}
        url = "https://x.edu/sports/mens-soccer/roster"

        first = probe_coaches_pages(self._fake_session(), url, cache=cache)
        assert first is not None
        first_calls = mock_fetch.call_count

        # Second call against the same program must come from cache —
        # zero additional HTTP hits — and return an equivalent result.
        second = probe_coaches_pages(self._fake_session(), url, cache=cache)
        assert second == first
        assert mock_fetch.call_count == first_calls

    @mock.patch("extractors.ncaa_soccer_rosters.fetch_with_retry")
    def test_cache_records_negative_result(self, mock_fetch):
        # Negative results must also be cached, so a host with no
        # staff page doesn't get re-probed (4 wasted fetches per repeat).
        mock_fetch.return_value = ""
        cache: dict = {}
        url = "https://x.edu/sports/mens-soccer/roster"

        assert probe_coaches_pages(self._fake_session(), url, cache=cache) is None
        first_calls = mock_fetch.call_count

        assert probe_coaches_pages(self._fake_session(), url, cache=cache) is None
        assert mock_fetch.call_count == first_calls  # cached miss

    @mock.patch("extractors.ncaa_soccer_rosters._render_with_playwright")
    @mock.patch("extractors.ncaa_soccer_rosters.fetch_with_retry")
    def test_playwright_fallback_recovers_js_only_coaches_page(
        self, mock_fetch, mock_render, monkeypatch
    ):
        # Static fetch on the /coaches candidate returns a JS shell
        # with no inline coach markup (mirrors D1 SIDEARM NextGen +
        # some Nuxt tenants whose staff pages are also JS-rendered).
        # When NCAA_PLAYWRIGHT_FALLBACK is on, the renderer must be
        # invoked and the hydrated DOM re-extracted.
        monkeypatch.setenv("NCAA_PLAYWRIGHT_FALLBACK", "true")
        shell = _read("js_rendered_roster_shell.html")
        rendered = _read("coaches_page_server_rendered.html")
        mock_fetch.return_value = shell
        mock_render.return_value = rendered

        result = probe_coaches_pages(
            self._fake_session(),
            "https://js.example.edu/sports/mens-soccer/roster",
        )

        assert result is not None
        assert result["name"] == "Marcus Reyes"
        # First candidate is the /coaches URL → renderer is hit on it
        # and we break on first success (no further candidates probed).
        assert mock_render.call_count == 1
        assert mock_render.call_args.args[0] == (
            "https://js.example.edu/sports/mens-soccer/coaches"
        )
        assert mock_fetch.call_count == 1
        # Strategy is tagged so end-of-run breakdown can tell rendered
        # hits apart from static-fetch hits at the same fallback bucket.
        assert result["_strategy"].startswith(
            "coaches-page-fallback:rendered:"
        )
        assert result["_source_url"] == (
            "https://js.example.edu/sports/mens-soccer/coaches"
        )

    @mock.patch("extractors.ncaa_soccer_rosters._render_with_playwright")
    @mock.patch("extractors.ncaa_soccer_rosters.fetch_with_retry")
    def test_playwright_fallback_skipped_when_env_disabled(
        self, mock_fetch, mock_render, monkeypatch
    ):
        # Same shell-only HTML, but env flag off → renderer must NOT
        # be invoked (CI / sandbox safety: keeps current behavior
        # when Playwright isn't installed or the operator hasn't
        # opted in yet).
        monkeypatch.delenv("NCAA_PLAYWRIGHT_FALLBACK", raising=False)
        mock_fetch.return_value = _read("js_rendered_roster_shell.html")

        result = probe_coaches_pages(
            self._fake_session(),
            "https://x.edu/sports/mens-soccer/roster",
        )

        assert result is None
        assert mock_render.call_count == 0

    @mock.patch("extractors.ncaa_soccer_rosters._render_with_playwright")
    @mock.patch("extractors.ncaa_soccer_rosters.fetch_with_retry")
    def test_playwright_negative_result_is_cached(
        self, mock_fetch, mock_render, monkeypatch
    ):
        # When every candidate misses inline AND the rendered DOM
        # also misses, the negative result must be cached so the
        # next call against the same program doesn't re-render
        # (renders are 3-5s each — ~20s of waste per repeat probe).
        monkeypatch.setenv("NCAA_PLAYWRIGHT_FALLBACK", "true")
        shell = _read("js_rendered_roster_shell.html")
        mock_fetch.return_value = shell
        mock_render.return_value = shell  # rendered DOM also empty
        cache: dict = {}
        url = "https://x.edu/sports/mens-soccer/roster"

        assert probe_coaches_pages(
            self._fake_session(), url, cache=cache
        ) is None
        first_render_calls = mock_render.call_count
        assert first_render_calls > 0  # confirm fallback actually fired

        assert probe_coaches_pages(
            self._fake_session(), url, cache=cache
        ) is None
        # Cached miss → no additional renders on the second probe.
        assert mock_render.call_count == first_render_calls


class TestYearNormalization:
    """Verify all year/class variants map correctly."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Fr", "freshman"),
            ("Fr.", "freshman"),
            ("Freshman", "freshman"),
            ("fr", "freshman"),
            ("So", "sophomore"),
            ("So.", "sophomore"),
            ("Sophomore", "sophomore"),
            ("Jr", "junior"),
            ("Jr.", "junior"),
            ("Junior", "junior"),
            ("Sr", "senior"),
            ("Sr.", "senior"),
            ("Senior", "senior"),
            ("Gr", "grad"),
            ("Gr.", "grad"),
            ("Grad", "grad"),
            ("Graduate", "grad"),
            ("5th", "grad"),
            # Redshirt variants
            ("RS-Fr", "freshman"),
            ("RS-Fr.", "freshman"),
            ("R-Fr", "freshman"),
            ("RS Fr", "freshman"),
            ("R-So", "sophomore"),
            ("RS-So", "sophomore"),
            ("R-Jr", "junior"),
            ("RS-Jr", "junior"),
            ("R-Sr", "senior"),
            ("RS-Sr", "senior"),
            # Edge cases
            (None, None),
            ("", None),
            ("  Fr.  ", "freshman"),
            ("SENIOR", "senior"),  # lowercased to "senior" which is a known key
        ],
    )
    def test_normalize(self, raw, expected):
        assert normalize_year(raw) == expected


# ---------------------------------------------------------------------------
# build_column_index
# ---------------------------------------------------------------------------


class TestBuildColumnIndex:
    def test_standard_headers(self):
        headers = ["No.", "Name", "Pos.", "Yr.", "Ht.", "Hometown / High School", "Previous School"]
        idx = build_column_index(headers)
        assert idx.jersey_number == 0
        assert idx.player_name == 1
        assert idx.position == 2
        assert idx.class_year == 3
        assert idx.height == 4
        assert idx.hometown == 5
        assert idx.high_school == 6

    def test_alternate_headers(self):
        headers = ["#", "Player", "Position", "Class", "Height", "From", "HS"]
        idx = build_column_index(headers)
        assert idx.jersey_number == 0
        assert idx.player_name == 1
        assert idx.position == 2
        assert idx.class_year == 3
        assert idx.hometown == 5
        assert idx.high_school == 6

    def test_missing_columns(self):
        headers = ["Name", "Position"]
        idx = build_column_index(headers)
        assert idx.player_name == 0
        assert idx.position == 1
        assert idx.jersey_number is None
        assert idx.class_year is None
        assert idx.hometown is None


# ---------------------------------------------------------------------------
# current_academic_year
# ---------------------------------------------------------------------------


class TestCurrentAcademicYear:
    def test_format(self):
        year = current_academic_year()
        # Should be like "2025-26"
        assert len(year) == 7
        assert year[4] == "-"
        assert year[:4].isdigit()
        assert year[5:].isdigit()


# ---------------------------------------------------------------------------
# dry_run — no DB writes
# ---------------------------------------------------------------------------


class TestDryRunNoWrites:
    """Verify dry_run=True never calls psycopg2."""

    @mock.patch("extractors.ncaa_soccer_rosters.psycopg2", None)
    def test_dry_run_without_db_returns_zero(self):
        """With no DB available, dry_run returns zeros without error."""
        result = scrape_college_rosters(division="D1", dry_run=True)
        assert result["scraped"] == 0
        assert result["rows_inserted"] == 0
        assert result["rows_updated"] == 0

    @mock.patch("extractors.ncaa_soccer_rosters._get_connection")
    @mock.patch("extractors.ncaa_soccer_rosters.fetch_with_retry")
    @mock.patch("extractors.ncaa_soccer_rosters._fetch_colleges")
    @mock.patch("extractors.ncaa_soccer_rosters.time.sleep")
    def test_dry_run_parses_but_skips_writes(
        self, mock_sleep, mock_fetch_colleges, mock_fetch_retry, mock_get_conn,
    ):
        """With a mock DB for college list, dry_run parses but never upserts."""
        # Provide a fake college list
        mock_conn = mock.MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_fetch_colleges.return_value = [
            {
                "id": 1,
                "name": "Test University",
                "slug": "test-university",
                "division": "D2",
                "conference": "Test Conf",
                "state": "GA",
                "city": "Atlanta",
                "website": "https://testathletics.com",
                "soccer_program_url": "https://testathletics.com/sports/womens-soccer/roster",
                "gender_program": "womens",
                "last_scraped_at": None,
            }
        ]

        # Return fixture HTML when roster URL is fetched
        fixture_html = _read("sample_roster.html")
        mock_fetch_retry.return_value = fixture_html

        result = scrape_college_rosters(division="D2", gender="womens", limit=1, dry_run=True)

        assert result["scraped"] == 1
        assert result["rows_inserted"] == 0  # dry_run: no writes
        assert result["rows_updated"] == 0
        # Verify no cursor execute calls were made for upserts
        mock_conn.cursor.return_value.execute.assert_not_called()


# ---------------------------------------------------------------------------
# skip_unresolved — don't fetch rosters for schools with no URL for the
# requested gender.
# ---------------------------------------------------------------------------


class TestSkipUnresolvedColleges:
    """Regression guard: a ``colleges`` row with NULL ``soccer_program_url``
    means the PR-2 resolver probed every SIDEARM candidate path and found
    none responding — i.e. the school doesn't field that sport. The runner
    must NEVER issue an HTTP fetch for those rows; doing so burns ~6s per
    school on static-fetch + Playwright fallback before SKIPping with
    "no players parsed", which both wastes time and inflates error counts
    into looking like parser bugs.

    Schools hit by this in production: Minnesota, Oregon, USC (all women's
    only; seeded as mens+womens in ``seed_colleges.py`` because they're
    Big Ten members). See PR description for full list.
    """

    def test_fetch_colleges_sql_includes_not_null_clause(self):
        """Default query must filter out rows with NULL soccer_program_url."""
        fake_cursor = mock.MagicMock()
        fake_cursor.description = [
            ("id",), ("name",), ("slug",), ("division",), ("conference",),
            ("state",), ("city",), ("website",), ("soccer_program_url",),
            ("gender_program",), ("last_scraped_at",),
        ]
        fake_cursor.fetchall.return_value = []
        fake_conn = mock.MagicMock()
        fake_conn.cursor.return_value.__enter__.return_value = fake_cursor

        _fetch_colleges(fake_conn, division="D1", gender="mens")

        # Inspect the SQL that was executed
        sql = fake_cursor.execute.call_args[0][0]
        assert "soccer_program_url IS NOT NULL" in sql
        # Keep the other filters intact
        assert "division = %s" in sql
        assert "gender_program = %s" in sql

    def test_fetch_colleges_opt_out_drops_the_clause(self):
        """Passing skip_unresolved=False must omit the IS NOT NULL filter
        so debug / audit callers still see every seeded row.
        """
        fake_cursor = mock.MagicMock()
        fake_cursor.description = [
            ("id",), ("name",), ("slug",), ("division",), ("conference",),
            ("state",), ("city",), ("website",), ("soccer_program_url",),
            ("gender_program",), ("last_scraped_at",),
        ]
        fake_cursor.fetchall.return_value = []
        fake_conn = mock.MagicMock()
        fake_conn.cursor.return_value.__enter__.return_value = fake_cursor

        _fetch_colleges(fake_conn, division="D1", gender="mens",
                        skip_unresolved=False)

        sql = fake_cursor.execute.call_args[0][0]
        assert "soccer_program_url IS NOT NULL" not in sql

    @mock.patch("extractors.ncaa_soccer_rosters._get_connection")
    @mock.patch("extractors.ncaa_soccer_rosters.fetch_with_retry")
    @mock.patch("extractors.ncaa_soccer_rosters._fetch_colleges")
    @mock.patch("extractors.ncaa_soccer_rosters.time.sleep")
    def test_fetch_never_called_for_unresolved_college(
        self, mock_sleep, mock_fetch_colleges, mock_fetch_retry, mock_get_conn,
    ):
        """End-to-end: if ``_fetch_colleges`` returns only rows with real
        URLs (which is what skip_unresolved=True guarantees), no HTTP
        fetch ever targets the ``website`` base of a NULL-URL school.

        The critical assertion: ``fetch_with_retry`` is NEVER called with
        any URL under the 'no-mens-program' school's hostname. This is
        the regression-guard contract for the skip-missing-gender fix.
        """
        # Simulate the post-filter list: only the resolved school is
        # present. Minnesota (no men's program) is absent because its
        # soccer_program_url is NULL → filtered out by _fetch_colleges.
        mock_conn = mock.MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_fetch_colleges.return_value = [
            {
                "id": 1,
                "name": "Georgetown University",
                "slug": "georgetown-d1-m",
                "division": "D1",
                "conference": "Big East",
                "state": "DC",
                "city": "Washington",
                "website": "https://guhoyas.com",
                "soccer_program_url": "https://guhoyas.com/sports/mens-soccer/roster",
                "gender_program": "mens",
                "last_scraped_at": None,
            },
        ]
        # Return a trivial 'no players' HTML so the parser path executes
        # but produces 0 — the call-tracking assertion is what matters.
        mock_fetch_retry.return_value = "<html><body></body></html>"

        scrape_college_rosters(division="D1", gender="mens", dry_run=True)

        # Verify _fetch_colleges was called with skip_unresolved=True
        mock_fetch_colleges.assert_called_once()
        call_kwargs = mock_fetch_colleges.call_args.kwargs
        assert call_kwargs.get("skip_unresolved") is True, (
            "scrape_college_rosters must default to skip_unresolved=True"
        )

        # Collect every URL that fetch_with_retry was asked to fetch.
        fetched_urls = [
            call.args[1] if len(call.args) >= 2 else call.kwargs.get("url")
            for call in mock_fetch_retry.call_args_list
        ]
        # The KEY assertion: no fetch ever targeted a school that wasn't
        # in the post-filter list. Minnesota's base host must not appear.
        minnesota_hosts = ("gophersports.com",)
        oregon_hosts = ("goducks.com",)
        usc_hosts = ("usctrojans.com",)
        for url in fetched_urls:
            if url is None:
                continue
            for host in minnesota_hosts + oregon_hosts + usc_hosts:
                assert host not in url, (
                    f"fetch_with_retry was called for {host!r} "
                    f"(URL: {url!r}) — this host has no men's soccer "
                    f"program and should have been filtered out at "
                    f"enumeration time by skip_unresolved=True"
                )


# ---------------------------------------------------------------------------
# Empty / malformed HTML
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_html_returns_no_players(self):
        players = parse_roster_html("<html><body></body></html>")
        assert players == []

    def test_table_without_name_header_skipped(self):
        html = """
        <table>
          <thead><tr><th>Col A</th><th>Col B</th></tr></thead>
          <tbody><tr><td>foo</td><td>bar</td></tr></tbody>
        </table>
        """
        players = parse_roster_html(html)
        assert players == []

    def test_short_names_filtered(self):
        html = """
        <table>
          <thead><tr><th>Name</th><th>Pos</th></tr></thead>
          <tbody>
            <tr><td>A</td><td>GK</td></tr>
            <tr><td>Jo Smith</td><td>MF</td></tr>
          </tbody>
        </table>
        """
        players = parse_roster_html(html)
        assert len(players) == 1
        assert players[0].player_name == "Jo Smith"
