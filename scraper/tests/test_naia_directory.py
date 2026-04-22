"""
Tests for the naia.org-backed seeder (NAIA).

Fixture models naia.org's 2021-22 Men's Soccer teams index — a list
of ``<a href="/sports/msoc/2021-22/teams/<slug>">NAME (STATE)</a>``
anchors mixed in with unrelated navigation / non-team hrefs. Tests
cover:

- Parser pulls team-href anchors out of the page and skips everything
  else (nav, footer, release-archive links, empty anchors)
- Anchor text parsed into (name, state) — handles "Calif.", 2-letter
  "KS", full-name "Iowa", no-parens "Aquinas"
- Dedup by (name.lower(), gender) when the same program shows up
  multiple times (alphabetical + conference subindexes coexist)
- Division/gender assigned to all seeds
- ``fetch_naia_programs`` composes the right URL per gender
- Defensive: a future-season anchor (``/2025-26/teams/...``) still
  matches if naia.org ever starts rendering current-season lists

Run::

    python -m pytest scraper/tests/test_naia_directory.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.naia_directory import (  # noqa: E402
    _name_and_state_from_anchor_text,
    _parse_state_parenthetical,
    directory_url,
    discover_naia_program_url,
    fetch_naia_programs,
    naia_team_detail_url,
    parse_naia_index,
    parse_naia_index_slugs,
    parse_naia_team_page,
    supported_genders,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ncaa"
MENS_FIXTURE = FIXTURE_DIR / "naia_2021_22_mens_index.html"
TEAM_DETAIL_FIXTURE = FIXTURE_DIR / "naia_team_detail_baker.html"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# directory_url / supported_genders
# ---------------------------------------------------------------------------


class TestDirectoryUrl:
    def test_mens_url_shape(self):
        assert directory_url("mens") == "https://www.naia.org/sports/msoc/2021-22/teams"

    def test_womens_url_uses_earlier_season(self):
        """naia.org womens teams-list broke at 2021-22 (renders a landing
        page instead of a teams index). Last working season is 2020-21,
        one season earlier than the mens endpoint. Regression guards
        against reverting to a uniform-season constant."""
        assert directory_url("womens") == "https://www.naia.org/sports/wsoc/2020-21/teams"

    def test_genders_use_different_seasons(self):
        """Structural invariant: the per-gender season map is not
        accidentally collapsed to one constant."""
        mens_url = directory_url("mens")
        womens_url = directory_url("womens")
        assert "2021-22" in mens_url
        assert "2020-21" in womens_url

    def test_unsupported_gender_raises(self):
        with pytest.raises(ValueError):
            directory_url("boys")

    def test_supported_genders_list(self):
        assert set(supported_genders()) == {"mens", "womens"}


# ---------------------------------------------------------------------------
# _parse_state_parenthetical
# ---------------------------------------------------------------------------


class TestParseStateParenthetical:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            # naia.org's canonical abbreviated-with-period form
            ("Calif.", "CA"),
            ("Mo.", "MO"),
            ("Neb.", "NE"),
            ("Ariz.", "AZ"),
            ("Ky.", "KY"),
            ("Miss.", "MS"),
            ("Texas", "TX"),  # full name
            ("N.Y.", "NY"),
            ("N.M.", "NM"),
            ("S.C.", "SC"),
            # 2-letter passthrough
            ("KS", "KS"),
            ("FL", "FL"),
            # naia.org uses "Iowa" / "Ohio" / "Utah" as full name (no abbreviation)
            ("Iowa", "IA"),
            ("Ohio", "OH"),
            ("Utah", "UT"),
            # Full state names
            ("California", "CA"),
            ("Missouri", "MO"),
        ],
    )
    def test_known_states(self, raw, expected):
        assert _parse_state_parenthetical(raw) == expected

    @pytest.mark.parametrize(
        "raw",
        ["", "   ", "Atlantis", "Moon", "Ontario"],
    )
    def test_unknown_returns_none(self, raw):
        assert _parse_state_parenthetical(raw) is None

    def test_strips_surrounding_whitespace(self):
        assert _parse_state_parenthetical("  Calif.  ") == "CA"


# ---------------------------------------------------------------------------
# _name_and_state_from_anchor_text
# ---------------------------------------------------------------------------


class TestNameAndStateFromAnchorText:
    def test_with_state_parenthetical(self):
        name, state = _name_and_state_from_anchor_text("Antelope Valley (Calif.)")
        assert name == "Antelope Valley"
        assert state == "CA"

    def test_with_two_letter_state(self):
        name, state = _name_and_state_from_anchor_text("Benedictine (KS)")
        assert name == "Benedictine"
        assert state == "KS"

    def test_no_parens_means_no_state(self):
        name, state = _name_and_state_from_anchor_text("Aquinas")
        assert name == "Aquinas"
        assert state is None

    def test_multiword_school_with_state(self):
        name, state = _name_and_state_from_anchor_text("Wayland Baptist (Texas)")
        assert name == "Wayland Baptist"
        assert state == "TX"

    def test_unknown_state_parens_still_keeps_name(self):
        name, state = _name_and_state_from_anchor_text("Mystery U (Atlantis)")
        assert name == "Mystery U"
        assert state is None

    def test_whitespace_normalized(self):
        name, state = _name_and_state_from_anchor_text("  Antelope  Valley  (Calif.) ")
        assert name == "Antelope Valley"
        assert state == "CA"

    def test_empty_input(self):
        name, state = _name_and_state_from_anchor_text("")
        assert name == ""
        assert state is None


# ---------------------------------------------------------------------------
# parse_naia_index
# ---------------------------------------------------------------------------


class TestParseNaiaIndex:
    def test_parses_core_programs(self):
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        names = [s.name for s in seeds]
        assert "Antelope Valley" in names
        assert "Aquinas" in names
        assert "Benedictine" in names
        assert "Wayland Baptist" in names
        assert "William Woods" in names

    def test_dedupes_by_name_and_gender(self):
        """Fixture repeats 'Antelope Valley' in a conference subindex.
        Parser must collapse into one seed."""
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        av = [s for s in seeds if s.name == "Antelope Valley"]
        assert len(av) == 1

    def test_skips_empty_anchor(self):
        """Fixture has an empty-text <a> pointing at /teams/phantom."""
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        assert all(s.name != "" for s in seeds)

    def test_skips_non_team_anchors(self):
        """Fixture has /releases/25_Polls and /Fan_Central nav links.
        Parser must not emit 'Polls Archive' or 'FAN CENTRAL' rows."""
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        names = {s.name for s in seeds}
        assert "Polls Archive" not in names
        assert "FAN CENTRAL" not in names
        assert "Men's Soccer Home" not in names

    def test_matches_future_season_hrefs_defensively(self):
        """If naia.org ever starts rendering current-season listings,
        a /2025-26/teams/... anchor should still parse correctly."""
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        assert any(s.name == "Future Program" and s.state == "NM" for s in seeds)

    def test_states_extracted(self):
        """Confirm the state extraction pipeline works end-to-end."""
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        state_by_name = {s.name: s.state for s in seeds}
        assert state_by_name["Antelope Valley"] == "CA"
        assert state_by_name["Avila"] == "MO"
        assert state_by_name["Bellevue"] == "NE"
        assert state_by_name["Benedictine"] == "KS"
        assert state_by_name["Briar Cliff"] == "IA"
        assert state_by_name["Aquinas"] is None  # no parens

    def test_division_and_gender_in_all_seeds(self):
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        assert seeds  # non-empty
        for s in seeds:
            assert s.division == "NAIA"
            assert s.gender_program == "mens"

    def test_invalid_gender_raises(self):
        with pytest.raises(ValueError):
            parse_naia_index("<html></html>", "boys")

    def test_empty_html_returns_empty(self):
        assert parse_naia_index("<html><body></body></html>", "mens") == []

    def test_expected_count_from_fixture(self):
        """Fixture has 13 unique programs + 1 future-season + 2 dupes/empties
        that should be filtered. Expected: 14 unique seeds."""
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        assert len(seeds) == 14


# ---------------------------------------------------------------------------
# fetch_naia_programs — happy path with mocked HTTP
# ---------------------------------------------------------------------------


class TestFetchNaiaPrograms:
    def test_fetch_parses_response_body(self):
        html = _read(MENS_FIXTURE)
        fake_response = mock.Mock()
        fake_response.text = html
        fake_response.raise_for_status = mock.Mock()

        fake_session = mock.Mock()
        fake_session.get.return_value = fake_response
        fake_session.close = mock.Mock()

        with mock.patch(
            "extractors.naia_directory.requests.Session",
            return_value=fake_session,
        ):
            seeds = fetch_naia_programs("mens")

        assert len(seeds) == 14
        assert "Antelope Valley" in {s.name for s in seeds}
        fake_session.get.assert_called_once()
        called_url = fake_session.get.call_args[0][0]
        assert called_url == "https://www.naia.org/sports/msoc/2021-22/teams"

    def test_fetch_womens_uses_wsoc_url_with_earlier_season(self):
        """Regression for the production-run bug where womens returned 0
        programs: the 2021-22 wsoc page is a landing page, not a teams
        index. Handler must fetch 2020-21 for womens."""
        html = _read(MENS_FIXTURE)  # content irrelevant for URL check
        fake_response = mock.Mock()
        fake_response.text = html
        fake_response.raise_for_status = mock.Mock()

        fake_session = mock.Mock()
        fake_session.get.return_value = fake_response
        fake_session.close = mock.Mock()

        with mock.patch(
            "extractors.naia_directory.requests.Session",
            return_value=fake_session,
        ):
            fetch_naia_programs("womens")

        called_url = fake_session.get.call_args[0][0]
        assert called_url == "https://www.naia.org/sports/wsoc/2020-21/teams"

    def test_fetch_unsupported_gender_raises(self):
        with pytest.raises(ValueError):
            fetch_naia_programs("boys")


# ---------------------------------------------------------------------------
# parse_naia_index_slugs — slug capture for URL-discovery flow
# ---------------------------------------------------------------------------


class TestParseNaiaIndexSlugs:
    def test_extracts_slug_per_program(self):
        slugs = parse_naia_index_slugs(_read(MENS_FIXTURE), "mens")
        # Spot-check a handful of name → slug mappings from the fixture.
        assert slugs["antelope valley"] == "antelopevalley"
        assert slugs["aquinas"] == "aquinas"
        assert slugs["benedictine"] == "benedictineks"
        assert slugs["wayland baptist"] == "wayland"

    def test_dedup_first_occurrence_wins(self):
        """Antelope Valley appears twice in the fixture (alphabetical +
        a conference subindex repeat). Both rows point at the same slug,
        so the dedup logic must collapse them into one entry — not raise
        and not double-count."""
        slugs = parse_naia_index_slugs(_read(MENS_FIXTURE), "mens")
        assert slugs["antelope valley"] == "antelopevalley"
        # Count: 13 unique programs + 1 future-season variant = 14.
        # Same total as parse_naia_index returns.
        assert len(slugs) == 14

    def test_skips_empty_anchors(self):
        """``/teams/phantom`` in the fixture has empty anchor text — no
        name to key on, so it must be skipped (not stored under '')."""
        slugs = parse_naia_index_slugs(_read(MENS_FIXTURE), "mens")
        assert "" not in slugs
        assert "phantom" not in slugs.values()

    def test_invalid_gender_raises(self):
        with pytest.raises(ValueError):
            parse_naia_index_slugs("<html></html>", "boys")

    def test_returns_lowercase_keys(self):
        """Keys must be lowercased so the run.py handler can join via
        ``lower(college.name)`` without per-call str.lower() shuffling."""
        slugs = parse_naia_index_slugs(_read(MENS_FIXTURE), "mens")
        assert all(k == k.lower() for k in slugs)


# ---------------------------------------------------------------------------
# naia_team_detail_url — URL composition for per-team pages
# ---------------------------------------------------------------------------


class TestNaiaTeamDetailUrl:
    def test_mens_uses_msoc_and_2021_22(self):
        """Detail-page URLs reuse the same per-gender season pinning as
        the index (``_NAIA_SEASONS``). Regression guard against drifting
        the two season constants out of sync."""
        url = naia_team_detail_url("aquinas", "mens")
        assert url == "https://www.naia.org/sports/msoc/2021-22/teams/aquinas"

    def test_womens_uses_wsoc_and_2020_21(self):
        url = naia_team_detail_url("baker", "womens")
        assert url == "https://www.naia.org/sports/wsoc/2020-21/teams/baker"

    def test_invalid_gender_raises(self):
        with pytest.raises(ValueError):
            naia_team_detail_url("anything", "boys")


# ---------------------------------------------------------------------------
# parse_naia_team_page — extract athletics-website outbound link
# ---------------------------------------------------------------------------


class TestParseNaiaTeamPage:
    def test_extracts_official_athletics_website(self):
        """Fixture's ``<a class="external-link">Official Athletics
        Website</a>`` should be picked up via the visible-text label
        match."""
        website = parse_naia_team_page(_read(TEAM_DETAIL_FIXTURE))
        assert website == "https://www.bakerwildcats.com"

    def test_normalizes_to_scheme_and_host_only(self):
        """Returned URL must be stripped to ``scheme://host`` so the
        downstream SIDEARM resolver can re-compose roster paths cleanly.
        A trailing slash on the source href must NOT survive."""
        html = """
        <html><body>
        <a href="https://athletics.example.edu/some/deep/path?x=1#frag"
           class="external-link">Official Site</a>
        </body></html>
        """
        assert parse_naia_team_page(html) == "https://athletics.example.edu"

    def test_matches_athletics_website_label(self):
        html = """
        <html><body>
        <a href="https://www.examplecats.com">Athletics Website</a>
        </body></html>
        """
        assert parse_naia_team_page(html) == "https://www.examplecats.com"

    def test_matches_aria_label_when_visible_text_is_icon(self):
        """Some NAIA detail-page templates use icon-only anchors with
        the label on ``aria-label``. Extractor must fall back to that
        attribute, not just visible text."""
        html = """
        <html><body>
        <a href="https://www.examplecats.com"
           aria-label="Visit Athletics Site"><img src="/icon.png"/></a>
        </body></html>
        """
        assert parse_naia_team_page(html) == "https://www.examplecats.com"

    def test_skips_social_media_links(self):
        """A page with ONLY social links (no athletics anchor) must
        return None — facebook/twitter/instagram are blocklisted even
        if their visible text accidentally contains 'Site'."""
        html = """
        <html><body>
        <a href="https://www.facebook.com/team">Facebook Site</a>
        <a href="https://twitter.com/team">Twitter Site</a>
        <a href="https://www.instagram.com/team">Instagram Site</a>
        </body></html>
        """
        assert parse_naia_team_page(html) is None

    def test_skips_naia_self_links(self):
        """The detail page is full of /sports/... and naia.org links
        (schedule, roster, conference). None of those are the school's
        athletics homepage and must not be returned."""
        html = """
        <html><body>
        <a href="https://www.naia.org/about">Official Site</a>
        <a href="/sports/msoc/2021-22/teams/baker/schedule">Athletics Website</a>
        </body></html>
        """
        assert parse_naia_team_page(html) is None

    def test_skips_mailto_and_tel(self):
        html = """
        <html><body>
        <a href="mailto:ad@example.edu">Athletics Website</a>
        <a href="tel:+15551234567">Official Site</a>
        </body></html>
        """
        assert parse_naia_team_page(html) is None

    def test_returns_none_when_no_label_match(self):
        """Anchor exists with a real URL but no recognized label — must
        return None rather than guess (false positive on a random link
        is worse than a miss because the URL gets persisted)."""
        html = """
        <html><body>
        <a href="https://example.edu/news">Latest news</a>
        <a href="https://example.edu/give">Donate</a>
        </body></html>
        """
        assert parse_naia_team_page(html) is None

    def test_handles_empty_html(self):
        assert parse_naia_team_page("") is None
        assert parse_naia_team_page("<html></html>") is None

    def test_picks_first_valid_match(self):
        """When multiple anchors match, the first (in document order)
        wins — sidebar Official Site link beats any later footer link."""
        html = """
        <html><body>
        <a href="https://www.first.edu" class="external">Official Site</a>
        <a href="https://www.second.edu" class="external">Official Site</a>
        </body></html>
        """
        assert parse_naia_team_page(html) == "https://www.first.edu"


# ---------------------------------------------------------------------------
# discover_naia_program_url — happy path + failure modes (mocked HTTP)
# ---------------------------------------------------------------------------


class TestDiscoverNaiaProgramUrl:
    """The discover function fetches the naia.org detail page through
    ``utils.http.get`` (proxy-aware wrapper) and then probes SIDEARM via
    ``ncaa_directory.resolve_soccer_program_url``. Tests mock both."""

    def _fake_http_response(self, html: str, *, status: int = 200):
        resp = mock.Mock()
        resp.status_code = status
        resp.text = html
        resp.raise_for_status = mock.Mock()
        return resp

    def _fake_session(self):
        """The session is still passed to the SIDEARM resolver (which
        owns its own HEAD-probe loop on the school's domain). The
        detail-page fetch goes through utils.http, not this session, so
        nothing on it should be called for naia.org."""
        sess = mock.Mock()
        sess.close = mock.Mock()
        return sess

    def test_happy_path_returns_website_and_program_url(self):
        """Detail-page extraction yields a website; the SIDEARM probe
        (mocked) finds a roster URL. Both halves come back to the
        caller, ready for a single UPDATE."""
        sess = self._fake_session()
        with mock.patch(
            "utils.http.get",
            return_value=self._fake_http_response(_read(TEAM_DETAIL_FIXTURE)),
        ) as http_get, mock.patch(
            "extractors.ncaa_directory.resolve_soccer_program_url",
            return_value="https://www.bakerwildcats.com/sports/msoc/roster",
        ) as resolver:
            website, program = discover_naia_program_url(
                "baker", "mens", session=sess
            )

        assert website == "https://www.bakerwildcats.com"
        assert program == "https://www.bakerwildcats.com/sports/msoc/roster"
        # naia.org detail-page URL passed to the proxy-aware GET.
        assert http_get.call_args[0][0] == (
            "https://www.naia.org/sports/msoc/2021-22/teams/baker"
        )
        # Resolver was called with the extracted website and gender.
        resolver.assert_called_once()
        assert resolver.call_args[0][0] == "https://www.bakerwildcats.com"

    def test_website_extracted_but_no_sidearm_hit(self):
        """When the school's site isn't on SIDEARM, the resolver
        returns None — caller still gets ``website`` so the
        ``colleges.website`` column can be backfilled (input for future
        non-SIDEARM probe strategies)."""
        sess = self._fake_session()
        with mock.patch(
            "utils.http.get",
            return_value=self._fake_http_response(_read(TEAM_DETAIL_FIXTURE)),
        ), mock.patch(
            "extractors.ncaa_directory.resolve_soccer_program_url",
            return_value=None,
        ):
            website, program = discover_naia_program_url(
                "baker", "mens", session=sess
            )
        assert website == "https://www.bakerwildcats.com"
        assert program is None

    def test_detail_page_404_returns_none_none(self):
        """Missing detail page (slug typo, naia.org pruned the season,
        WAF block) — both halves None so the caller logs a miss and
        moves on without writing anything."""
        sess = self._fake_session()
        with mock.patch(
            "utils.http.get",
            return_value=self._fake_http_response("", status=404),
        ), mock.patch(
            "extractors.ncaa_directory.resolve_soccer_program_url",
            return_value="https://should-not-be-called.example",
        ) as resolver:
            website, program = discover_naia_program_url(
                "missing", "mens", session=sess
            )
        assert (website, program) == (None, None)
        # Don't waste a resolver call when the detail page failed.
        resolver.assert_not_called()

    def test_detail_page_200_but_no_extractable_link(self):
        """Page loads but has no athletics-website anchor — return
        ``(None, None)`` and skip the resolver."""
        sess = self._fake_session()
        html = (
            "<html><body><a href='https://twitter.com/x'>Twitter</a></body></html>"
        )
        with mock.patch(
            "utils.http.get",
            return_value=self._fake_http_response(html),
        ), mock.patch(
            "extractors.ncaa_directory.resolve_soccer_program_url",
        ) as resolver:
            website, program = discover_naia_program_url(
                "baker", "mens", session=sess
            )
        assert (website, program) == (None, None)
        resolver.assert_not_called()

    def test_request_exception_returns_none_none(self):
        """Network failure (timeout, DNS, connection reset) must not
        propagate — caller handles thousands of rows and one bad
        request shouldn't kill the whole batch."""
        import requests as _requests

        sess = self._fake_session()
        with mock.patch(
            "utils.http.get",
            side_effect=_requests.ConnectionError("boom"),
        ), mock.patch(
            "extractors.ncaa_directory.resolve_soccer_program_url",
        ) as resolver:
            website, program = discover_naia_program_url(
                "baker", "mens", session=sess
            )
        assert (website, program) == (None, None)
        resolver.assert_not_called()


# ---------------------------------------------------------------------------
# _normalize_naia_name + suffix-drift slug joins
# ---------------------------------------------------------------------------


class TestNormalizeNaiaNameForSlugJoin:
    """The slug map exposes both ``lower(name)`` and a normalized form
    so the run.py handler can join on either when DB names drift from
    naia.org's short anchor text (e.g. "Wayland Baptist University")."""

    def test_normalized_name_resolves_university_suffix(self):
        """DB has 'Wayland Baptist University' but naia.org anchor is
        'Wayland Baptist'. The normalized form must let the slug map
        be queried by either spelling."""
        slugs = parse_naia_index_slugs(_read(MENS_FIXTURE), "mens")
        # Original lowercased form (anchor text) — direct hit.
        assert slugs["wayland baptist"] == "wayland"
        # Normalized form for the DB-side "...University" name.
        # Caller does: slugs.get(_normalize_naia_name(college.name))
        from extractors.naia_directory import _normalize_naia_name
        assert _normalize_naia_name("Wayland Baptist University") == "wayland baptist"
        assert slugs[_normalize_naia_name("Wayland Baptist University")] == "wayland"

    def test_does_not_strip_state_suffix(self):
        """'Kansas State' is a real school name — 'State' must stay.
        Stripping it would falsely join to a different school."""
        from extractors.naia_directory import _normalize_naia_name
        assert _normalize_naia_name("Kansas State") == "kansas state"

    def test_does_not_strip_christian_suffix(self):
        """'Arizona Christian' — Christian is part of the name."""
        from extractors.naia_directory import _normalize_naia_name
        assert _normalize_naia_name("Arizona Christian") == "arizona christian"

    def test_strips_punctuation(self):
        """St. Mary -> st mary; O'Connell -> o connell."""
        from extractors.naia_directory import _normalize_naia_name
        assert _normalize_naia_name("St. Mary") == "st mary"
        assert _normalize_naia_name("O'Connell") == "o connell"

    def test_handles_empty_input(self):
        from extractors.naia_directory import _normalize_naia_name
        assert _normalize_naia_name("") == ""
        assert _normalize_naia_name("   ") == ""

    def test_strips_college_suffix(self):
        from extractors.naia_directory import _normalize_naia_name
        assert _normalize_naia_name("Aquinas College") == "aquinas"

    def test_handler_two_pass_lookup_resolves_drifted_db_name(self):
        """End-to-end check of the run.py handler's two-pass slug join:
        DB name has a 'University' suffix that naia.org's anchor text
        drops. Pass 1 (lower(name)) misses; pass 2 (normalized name)
        hits the same slug. Verifies the slug map + handler lookup
        contract that drives ``soccer_program_url`` fill-rate."""
        from extractors.naia_directory import _normalize_naia_name

        slugs = parse_naia_index_slugs(_read(MENS_FIXTURE), "mens")
        # Simulate a DB row whose name drifted from naia.org's short form.
        db_name = "Wayland Baptist University"

        # Pass 1: exact lowercased lookup (what _handle_naia_resolve_urls
        # tries first). This MUST miss for the test to be meaningful —
        # the drift case the fallback exists to cover.
        slug_pass1 = slugs.get(db_name.lower())
        assert slug_pass1 is None, (
            "test setup invalid: pass-1 should miss for drifted name "
            "(if it hits the fallback isn't being exercised)"
        )

        # Pass 2: normalized lookup (the fallback). Must hit.
        slug_pass2 = slugs.get(_normalize_naia_name(db_name))
        assert slug_pass2 == "wayland", (
            f"normalized fallback failed: got {slug_pass2!r} for "
            f"{db_name!r}; expected 'wayland'"
        )

        # And the same fallback must NOT hit for an unrelated name —
        # guards against the fallback being a wildcard that creates
        # false-positive joins.
        unrelated = "Some Nonexistent University"
        assert slugs.get(_normalize_naia_name(unrelated)) is None
