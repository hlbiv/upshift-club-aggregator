"""
Tests for the NCAA D1 enumeration + URL-resolver (PR-2).

Covers:

- ``compose_sidearm_roster_url`` — pure URL composition (origin + canonical path)
- ``resolve_soccer_program_url`` — HEAD-probe, hit + miss + redirect-away paths
- ``_handle_ncaa_rosters`` ``--all`` dispatch — routes to
  ``scrape_college_rosters(division, gender)`` rather than the single-school
  path
- ``_handle_ncaa_rosters`` mutex — exactly one of ``--all`` or ``--school-url``
  must be set; both or neither exit 2

Run::

    python -m pytest scraper/tests/test_ncaa_roster_enumeration.py -v
"""

from __future__ import annotations

import argparse
import os
import sys
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ncaa_directory import (  # noqa: E402
    compose_sidearm_roster_url,
    resolve_soccer_program_url,
)

# run.py transitively imports scraper_js → playwright, which isn't always
# available in CI sandboxes. Follow the pattern used by
# test_scrape_run_logger.py: skip handler-dispatch tests cleanly when
# run.py isn't importable. Replit has playwright; CI paths that do too
# will exercise these tests.
try:
    from run import _handle_ncaa_rosters  # type: ignore # noqa: E402
    _RUN_IMPORTABLE = True
except Exception:  # pragma: no cover — environment-gated
    _RUN_IMPORTABLE = False
    _handle_ncaa_rosters = None  # type: ignore


requires_run = pytest.mark.skipif(
    not _RUN_IMPORTABLE, reason="run.py imports unavailable (playwright missing?)"
)


# ---------------------------------------------------------------------------
# compose_sidearm_roster_url
# ---------------------------------------------------------------------------


class TestComposeSidearmRosterUrl:
    def test_https_origin_mens(self):
        assert (
            compose_sidearm_roster_url("https://guhoyas.com", "mens")
            == "https://guhoyas.com/sports/mens-soccer/roster"
        )

    def test_https_origin_womens(self):
        assert (
            compose_sidearm_roster_url("https://guhoyas.com", "womens")
            == "https://guhoyas.com/sports/womens-soccer/roster"
        )

    def test_trailing_slash_stripped(self):
        assert (
            compose_sidearm_roster_url("https://goheels.com/", "mens")
            == "https://goheels.com/sports/mens-soccer/roster"
        )

    def test_bare_hostname_gets_https_scheme(self):
        assert (
            compose_sidearm_roster_url("virginiasports.com", "mens")
            == "https://virginiasports.com/sports/mens-soccer/roster"
        )

    def test_existing_path_trimmed_to_origin(self):
        assert (
            compose_sidearm_roster_url(
                "https://gostanford.com/about-us/history", "mens"
            )
            == "https://gostanford.com/sports/mens-soccer/roster"
        )

    def test_http_scheme_preserved(self):
        """http:// isn't rewritten to https://. Follow-redirects handles the upgrade."""
        assert (
            compose_sidearm_roster_url("http://example.edu", "mens")
            == "http://example.edu/sports/mens-soccer/roster"
        )

    def test_empty_website_raises(self):
        with pytest.raises(ValueError):
            compose_sidearm_roster_url("", "mens")

    def test_invalid_gender_raises(self):
        with pytest.raises(ValueError):
            compose_sidearm_roster_url("https://example.edu", "boys")


# ---------------------------------------------------------------------------
# resolve_soccer_program_url — hit path
# ---------------------------------------------------------------------------


class TestResolveSoccerProgramUrlHit:
    def test_200_returns_composed_url(self):
        fake_resp = mock.Mock()
        fake_resp.status_code = 200
        fake_resp.url = "https://guhoyas.com/sports/mens-soccer/roster"
        fake_session = mock.Mock()
        fake_session.head.return_value = fake_resp

        result = resolve_soccer_program_url(
            "https://guhoyas.com", "mens", session=fake_session
        )
        assert result == "https://guhoyas.com/sports/mens-soccer/roster"

        fake_session.head.assert_called_once()
        probed_url = fake_session.head.call_args[0][0]
        assert probed_url == "https://guhoyas.com/sports/mens-soccer/roster"


# ---------------------------------------------------------------------------
# resolve_soccer_program_url — miss paths (regression guards)
# ---------------------------------------------------------------------------


class TestResolveSoccerProgramUrlMiss:
    """These exist so PR-2 can't regress into 'always returns a URL'."""

    def test_404_returns_none(self):
        fake_resp = mock.Mock()
        fake_resp.status_code = 404
        fake_resp.url = "https://example.edu/sports/mens-soccer/roster"
        fake_session = mock.Mock()
        fake_session.head.return_value = fake_resp

        result = resolve_soccer_program_url(
            "https://example.edu", "mens", session=fake_session
        )
        assert result is None

    def test_500_returns_none(self):
        fake_resp = mock.Mock()
        fake_resp.status_code = 500
        fake_resp.url = "https://example.edu/sports/mens-soccer/roster"
        fake_session = mock.Mock()
        fake_session.head.return_value = fake_resp

        result = resolve_soccer_program_url(
            "https://example.edu", "mens", session=fake_session
        )
        assert result is None

    def test_redirect_away_from_path_returns_none(self):
        """200 that landed on the site's homepage (e.g. catch-all 301 → /)
        is treated as a miss — valid roster paths keep /sports/<g>-soccer/roster
        in the final URL.
        """
        fake_resp = mock.Mock()
        fake_resp.status_code = 200
        fake_resp.url = "https://example.edu/"
        fake_session = mock.Mock()
        fake_session.head.return_value = fake_resp

        result = resolve_soccer_program_url(
            "https://example.edu", "mens", session=fake_session
        )
        assert result is None

    def test_connection_error_returns_none(self):
        import requests as _requests

        fake_session = mock.Mock()
        fake_session.head.side_effect = _requests.ConnectionError("boom")

        result = resolve_soccer_program_url(
            "https://example.edu", "mens", session=fake_session
        )
        assert result is None

    def test_empty_website_returns_none(self):
        assert resolve_soccer_program_url(None, "mens") is None
        assert resolve_soccer_program_url("", "mens") is None


# ---------------------------------------------------------------------------
# _handle_ncaa_rosters — --all dispatch + mutex
# ---------------------------------------------------------------------------


def _run_ns(**kwargs) -> argparse.Namespace:
    defaults = dict(
        school_url=None,
        school_name=None,
        division=None,
        gender=None,
        state=None,
        all=False,
        dry_run=False,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


@requires_run
class TestHandleNcaaRostersDispatch:
    def test_all_flag_routes_to_enumerator(self):
        """--all --division D1 --gender mens must call scrape_college_rosters("D1", "mens")."""
        ns = _run_ns(all=True, division="D1", gender="mens", dry_run=True)

        with mock.patch("extractors.ncaa_rosters.scrape_college_rosters") as mock_bulk:
            mock_bulk.return_value = {
                "scraped": 0, "rows_inserted": 0,
                "rows_updated": 0, "errors": 0,
            }
            _handle_ncaa_rosters(ns)

        mock_bulk.assert_called_once()
        call_kwargs = mock_bulk.call_args.kwargs
        assert call_kwargs["division"] == "D1"
        assert call_kwargs["gender"] == "mens"
        assert call_kwargs["dry_run"] is True

    def test_all_with_gender_girls_alias_normalizes_to_womens(self):
        ns = _run_ns(all=True, division="D1", gender="girls", dry_run=True)

        with mock.patch("extractors.ncaa_rosters.scrape_college_rosters") as mock_bulk:
            mock_bulk.return_value = {
                "scraped": 0, "rows_inserted": 0,
                "rows_updated": 0, "errors": 0,
            }
            _handle_ncaa_rosters(ns)

        assert mock_bulk.call_args.kwargs["gender"] == "womens"

    def test_school_url_preserves_single_school_path(self):
        """Without --all, --school-url must still hit scrape_school_url (not the bulk path)."""
        ns = _run_ns(
            school_url="https://guhoyas.com/sports/mens-soccer/roster",
            school_name="Georgetown",
            division="D1",
            gender="mens",
            dry_run=True,
        )
        parsed = {
            "college": {
                "name": "Georgetown", "division": "D1",
                "gender_program": "mens", "website": "https://guhoyas.com",
            },
            "coaches": [],
            "players": [],
            "academic_year": "2025-26",
            "sidearm": True,
        }
        with mock.patch("extractors.ncaa_rosters.scrape_school_url", return_value=parsed) as mock_single, \
             mock.patch("extractors.ncaa_rosters.scrape_college_rosters") as mock_bulk:
            _handle_ncaa_rosters(ns)

        mock_single.assert_called_once()
        mock_bulk.assert_not_called()


@requires_run
class TestHandleNcaaRostersMutex:
    def test_both_all_and_school_url_exit_2(self):
        ns = _run_ns(
            school_url="https://guhoyas.com/sports/mens-soccer/roster",
            all=True,
            division="D1",
            gender="mens",
        )
        with pytest.raises(SystemExit) as exc:
            _handle_ncaa_rosters(ns)
        assert exc.value.code == 2

    def test_neither_all_nor_school_url_exits_2(self):
        ns = _run_ns(division="D1", gender="mens")
        with pytest.raises(SystemExit) as exc:
            _handle_ncaa_rosters(ns)
        assert exc.value.code == 2
