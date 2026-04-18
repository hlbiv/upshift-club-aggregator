"""
Tests for the Squarespace + JSON-LD club extractor.

All tests are fixture-driven and pure — no network, no DB. Mirrors the
inline-fixture style of ``test_jsonld_parser.py`` and ``test_cms_detect``
so we don't need a separate fixture directory.

Coverage:
  (a) Squarespace detection on a real-looking HTTP response
  (b) SportsTeam.athlete[] extraction → roster rows
  (c) Person blocks (top-level + nested under member/coach) → coach rows
  (d) Event tryout-keyword filter accepts tryouts and rejects tournaments
  (e) Fail-soft when the site is not Squarespace
  (f) Organization metadata → enrichment row
  (g) End-to-end harvest with mocked HTTP
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.cms_detect import detect_cms  # noqa: E402
from extractors.squarespace_clubs import (  # noqa: E402
    DISCOVERY_CONFIDENCE,
    SquarespaceClubSite,
    _athlete_to_roster_row,
    _event_to_tryout_row,
    _organization_to_enrichment_row,
    _parse_event_date,
    _person_to_coach_row,
    harvest_squarespace_club,
    is_tryout_event,
)
from extractors.jsonld_parser import extract_athletes, extract_jsonld, extract_persons  # noqa: E402


# ---------------------------------------------------------------- shared helpers


class _FakeResponse:
    """Minimal stand-in for ``requests.Response`` — just ``.headers``,
    ``.text``, ``.status_code``, ``.url``."""

    def __init__(
        self,
        *,
        headers: Optional[Dict[str, str]] = None,
        text: str = "",
        status_code: int = 200,
        url: str = "",
    ):
        self.headers = headers or {}
        self.text = text
        self.status_code = status_code
        self.url = url


class _FakeSession:
    """Routes requests by URL into a pre-built dict of responses.

    Any URL not in the dict returns a 404. Mirrors ``requests.Session``
    just enough that ``squarespace_clubs._fetch`` works against it.
    """

    def __init__(self, responses: Dict[str, _FakeResponse]):
        self.responses = responses
        self.calls: List[str] = []

    def get(self, url, timeout=None, allow_redirects=True):
        self.calls.append(url)
        if url in self.responses:
            r = self.responses[url]
            r.url = url
            return r
        return _FakeResponse(status_code=404, url=url)

    def close(self):
        pass


def _wrap_jsonld(payload: str, *, server_squarespace: bool = True) -> _FakeResponse:
    """Wrap a JSON-LD payload into a Squarespace-looking page response.

    Headers use lowercase keys to mirror the case-insensitive lookup
    that ``requests.Response.headers`` actually provides — our
    extractor reads ``r.headers.get('content-type', '')`` and our
    ``_FakeResponse`` uses a plain dict.
    """
    body = (
        "<!doctype html><html><head>"
        '<link href="https://static1.squarespace.com/static/abc/site.css">'
        f'<script type="application/ld+json">{payload}</script>'
        "</head><body></body></html>"
    )
    headers = (
        {"Server": "Squarespace", "content-type": "text/html"}
        if server_squarespace
        else {"content-type": "text/html"}
    )
    return _FakeResponse(headers=headers, text=body, status_code=200)


# --------------------------------------------------------------- (a) detection


def test_squarespace_detection_on_real_looking_response():
    """A response with the Squarespace ``Server`` banner AND the
    static1.squarespace.com signature in the body must classify as
    Squarespace."""
    resp = _wrap_jsonld(
        '{"@context":"https://schema.org","@type":"Organization","name":"Foley FC"}'
    )
    assert detect_cms(resp) == "squarespace"


def test_non_squarespace_response_rejected_in_harvest():
    """Fail-soft: if the homepage isn't Squarespace, the harvest returns
    is_squarespace=False and ALL output lists are empty."""
    site = SquarespaceClubSite(
        club_id=1, club_name_canonical="Wix Club FC",
        website="https://wixclub.example.com",
    )
    # Wix-shaped homepage: detect_cms returns "wix", not "squarespace".
    wix_resp = _FakeResponse(
        headers={"X-Wix-Request-Id": "abc"},
        text="<html><body>wix</body></html>",
    )
    session = _FakeSession({"https://wixclub.example.com/": wix_resp})

    harvest = harvest_squarespace_club(site, session=session)
    assert harvest.is_squarespace is False
    assert harvest.roster_rows == []
    assert harvest.coach_rows == []
    assert harvest.tryout_rows == []
    assert harvest.enrichment_row is None


# --------------------------------------------------------------- (b) athletes


def test_extract_sports_team_athletes_into_roster_rows():
    """A Squarespace page with a SportsTeam JSON-LD block carrying an
    ``athlete`` list must surface as one roster_row per player with
    name, jersey, position populated."""
    payload = """
    {
      "@context":"https://schema.org",
      "@type":"SportsTeam",
      "name":"Foley FC U12 Boys",
      "athlete":[
        {"@type":"Person","name":"Player One","identifier":"7","jobTitle":"Forward"},
        {"@type":"Person","name":"Player Two","jerseyNumber":"10","jobTitle":"Midfielder"}
      ]
    }
    """
    blocks = extract_jsonld(_wrap_jsonld(payload).text)
    athletes = extract_athletes(blocks)
    team = blocks[0]
    rows = []
    snap = datetime(2026, 4, 18)
    for a in athletes:
        row = _athlete_to_roster_row(
            a, team=team, club_name_raw="Foley FC",
            source_url="https://foleyfc.example.com/teams",
            snapshot_date=snap,
        )
        if row is not None:
            rows.append(row)

    assert len(rows) == 2
    by_name = {r["player_name"]: r for r in rows}
    assert by_name["Player One"]["jersey_number"] == "7"
    assert by_name["Player One"]["position"] == "Forward"
    assert by_name["Player Two"]["jersey_number"] == "10"
    # Age + gender pulled from team name.
    assert by_name["Player One"]["age_group"] == "U12"
    assert by_name["Player One"]["gender"] == "M"
    # Linker contract: every row has club_name_raw set, club_id is NOT
    # present (writer adds NULL).
    assert all(r["club_name_raw"] == "Foley FC" for r in rows)
    assert all("club_id" not in r for r in rows)
    assert all(r["snapshot_date"] == snap for r in rows)


def test_athlete_row_drops_unnamed_athletes():
    rows = []
    bad = {"@type": "Person"}
    row = _athlete_to_roster_row(
        bad, team={"@type": "SportsTeam", "name": "X"},
        club_name_raw="X", source_url="https://x/",
        snapshot_date=datetime(2026, 1, 1),
    )
    assert row is None


# --------------------------------------------------------------- (c) persons


def test_extract_top_level_and_nested_person_blocks():
    """Both top-level Person blocks AND Person dicts nested under
    ``member`` / ``coach`` on Organization-like blocks must surface as
    coach_discoveries rows."""
    payload = """
    {
      "@context":"https://schema.org",
      "@graph":[
        {"@type":"Person","name":"Jane Doe","jobTitle":"Director of Coaching",
         "email":"jane@example.com"},
        {"@type":"SportsOrganization","name":"Foley FC",
         "member":[
           {"@type":"Person","name":"Coach Alpha","jobTitle":"Head Coach",
            "email":"alpha@example.com"},
           {"@type":"Person","name":"Coach Beta","jobTitle":"Assistant"}
         ],
         "coach":{"@type":"Person","name":"Coach Gamma","jobTitle":"Head Coach U12"}
        }
      ]
    }
    """
    blocks = extract_jsonld(_wrap_jsonld(payload).text)
    persons = extract_persons(blocks)
    rows: List[Dict[str, Any]] = []
    for p in persons:
        row = _person_to_coach_row(
            p, club_id=42,
            source_url="https://foleyfc.example.com/coaches",
        )
        if row is not None:
            rows.append(row)

    names = {r["name"] for r in rows}
    assert {"Jane Doe", "Coach Alpha", "Coach Beta", "Coach Gamma"}.issubset(names)
    by_name = {r["name"]: r for r in rows}
    # Email + title surface; club_id is set (we know the canonical club).
    assert by_name["Jane Doe"]["email"] == "jane@example.com"
    assert by_name["Jane Doe"]["title"] == "Director of Coaching"
    assert by_name["Coach Alpha"]["club_id"] == 42
    # Schema platform_family enum currently restricts to a 4-value set;
    # squarespace is not in there yet, so we stamp 'unknown' to keep
    # CHECK constraint happy until a follow-up enum extension lands.
    assert all(r["platform_family"] == "unknown" for r in rows)
    assert all(r["confidence"] == DISCOVERY_CONFIDENCE for r in rows)


def test_person_row_strips_mailto_prefix():
    row = _person_to_coach_row(
        {"@type": "Person", "name": "X", "email": "MAILTO:foo@BAR.com?cc=x"},
        club_id=1, source_url="https://x/",
    )
    assert row is not None
    assert row["email"] == "foo@bar.com"


# --------------------------------------------------------------- (d) events


def test_event_tryout_keyword_filter_accepts_tryouts():
    assert is_tryout_event({"name": "Spring Tryouts 2026"}) is True
    assert is_tryout_event({"name": "U12 ID Clinic"}) is True
    assert is_tryout_event({"name": "Open Tryout"}) is True
    assert is_tryout_event(
        {"name": "Annual event", "description": "Open training and player evaluation"}
    ) is True


def test_event_tryout_keyword_filter_rejects_other_events():
    assert is_tryout_event({"name": "Spring Tournament"}) is False
    assert is_tryout_event({"name": "Annual Picnic"}) is False
    assert is_tryout_event({}) is False


def test_event_to_tryout_row_pulls_date_and_location():
    event = {
        "@type": "Event",
        "name": "U12 Tryouts",
        "startDate": "2026-08-05",
        "location": {
            "@type": "Place",
            "name": "Foley Sports Complex",
            "address": {
                "@type": "PostalAddress",
                "streetAddress": "123 Main St",
                "addressLocality": "Foley",
                "addressRegion": "AL",
            },
        },
    }
    row = _event_to_tryout_row(
        event, club_name_raw="Foley FC",
        source_url="https://foleyfc.example.com/tryouts",
    )
    assert row["club_name_raw"] == "Foley FC"
    assert row["tryout_date"] == datetime(2026, 8, 5)
    assert "Foley Sports Complex" in row["location"]
    assert "123 Main St" in row["location"]
    assert row["source"] == "site_monitor"
    assert row["status"] == "upcoming"


def test_event_to_tryout_row_handles_string_location_and_iso_timestamp():
    event = {
        "@type": "Event",
        "name": "ID Clinic",
        "startDate": "2026-08-05T10:00:00Z",
        "location": "Field 7",
    }
    row = _event_to_tryout_row(
        event, club_name_raw="X", source_url="https://x/",
    )
    assert row["tryout_date"] is not None
    assert row["tryout_date"].year == 2026
    assert row["location"] == "Field 7"


def test_parse_event_date_handles_date_only_and_returns_none_on_garbage():
    assert _parse_event_date("2026-08-05") == datetime(2026, 8, 5)
    assert _parse_event_date("not a date") is None
    assert _parse_event_date(None) is None
    assert _parse_event_date("") is None


# --------------------------------------------------------------- (f) enrichment


def test_organization_metadata_to_enrichment_row():
    org = {
        "@type": "SportsOrganization",
        "name": "Foley FC",
        "logo": "https://static1.squarespace.com/static/abc/logo.png",
        "sameAs": [
            "https://www.facebook.com/foleyfc",
            "https://www.instagram.com/foleyfc/",
            "https://twitter.com/foleyfc",
        ],
    }
    row = _organization_to_enrichment_row(org, club_id=99)
    assert row is not None
    assert row["club_id"] == 99
    assert row["logo_url"].endswith("/logo.png")
    assert "facebook.com" in row["facebook"]
    assert "instagram.com" in row["instagram"]
    assert "twitter.com" in row["twitter"]
    assert row["scrape_confidence"] == DISCOVERY_CONFIDENCE


def test_organization_with_no_signal_returns_none():
    assert _organization_to_enrichment_row(
        {"@type": "Organization", "name": "X"}, club_id=1,
    ) is None


def test_organization_logo_dict_form():
    """schema.org logo can be an ImageObject — accept .url."""
    org = {
        "@type": "Organization",
        "name": "X",
        "logo": {"@type": "ImageObject", "url": "https://x/logo.png"},
    }
    row = _organization_to_enrichment_row(org, club_id=1)
    assert row is not None
    assert row["logo_url"] == "https://x/logo.png"


# --------------------------------------------------------------- (g) end-to-end harvest


def test_harvest_squarespace_club_full_flow_with_mocked_session():
    """End-to-end harvest with a real-looking Squarespace homepage and
    a /coaches subpath carrying Person blocks. Verifies the four sinks
    are populated and pages_fetched is correct."""
    home = _wrap_jsonld(
        """
        {
          "@context":"https://schema.org",
          "@type":"SportsOrganization",
          "name":"Foley FC",
          "logo":"https://static1.squarespace.com/static/abc/logo.png",
          "sameAs":["https://www.instagram.com/foleyfc/"]
        }
        """
    )
    teams_page = _wrap_jsonld(
        """
        {
          "@context":"https://schema.org",
          "@type":"SportsTeam",
          "name":"Foley U12 Boys",
          "athlete":[{"@type":"Person","name":"Player A","identifier":"3"}]
        }
        """
    )
    coaches_page = _wrap_jsonld(
        """
        {
          "@context":"https://schema.org",
          "@type":"Person",
          "name":"Coach Z",
          "jobTitle":"Head Coach"
        }
        """
    )
    tryouts_page = _wrap_jsonld(
        """
        {
          "@context":"https://schema.org",
          "@type":"Event",
          "name":"U12 Tryouts",
          "startDate":"2026-08-05",
          "location":"Foley Field"
        }
        """
    )

    site = SquarespaceClubSite(
        club_id=42,
        club_name_canonical="Foley FC",
        website="https://foleyfc.example.com",
    )
    session = _FakeSession({
        "https://foleyfc.example.com/": home,
        "https://foleyfc.example.com/teams": teams_page,
        "https://foleyfc.example.com/coaches": coaches_page,
        "https://foleyfc.example.com/about": tryouts_page,
        # /team, /staff, /roster, /players → 404s, harvest skips them.
    })

    harvest = harvest_squarespace_club(site, session=session)

    assert harvest.is_squarespace is True
    # Homepage + 3 successful subpaths = 4 fetched.
    assert harvest.pages_fetched == 4
    # Roster: one row from /teams.
    assert len(harvest.roster_rows) == 1
    assert harvest.roster_rows[0]["player_name"] == "Player A"
    # Coaches: one row from /coaches.
    assert len(harvest.coach_rows) == 1
    assert harvest.coach_rows[0]["name"] == "Coach Z"
    assert harvest.coach_rows[0]["club_id"] == 42
    # Tryouts: one row from /about (the keyword "Tryouts" matches).
    assert len(harvest.tryout_rows) == 1
    assert harvest.tryout_rows[0]["tryout_date"] == datetime(2026, 8, 5)
    # Enrichment: pulled from homepage Organization block.
    assert harvest.enrichment_row is not None
    assert harvest.enrichment_row["club_id"] == 42
    assert "instagram.com" in (harvest.enrichment_row["instagram"] or "")


def test_harvest_fails_soft_on_homepage_404():
    """If the homepage returns 404, harvest reports is_squarespace=False
    and produces empty output — never raises."""
    site = SquarespaceClubSite(
        club_id=1, club_name_canonical="Dead FC",
        website="https://dead.example.com",
    )
    session = _FakeSession({})  # everything 404s
    harvest = harvest_squarespace_club(site, session=session)
    assert harvest.is_squarespace is False
    assert harvest.pages_fetched == 0


def test_harvest_skips_unparseable_website_url():
    site = SquarespaceClubSite(
        club_id=1, club_name_canonical="Bad URL",
        website="not-a-url",
    )
    session = _FakeSession({})
    harvest = harvest_squarespace_club(site, session=session)
    assert harvest.is_squarespace is False
    assert harvest.pages_fetched == 0
    # Session was never called for the bad URL.
    assert session.calls == []


def test_harvest_dedups_repeated_persons_across_pages():
    """The same coach showing up on /staff and /coaches should only
    yield one row (first wins)."""
    org_payload = '{"@context":"https://schema.org","@type":"Person","name":"Coach Z","jobTitle":"Head"}'
    site = SquarespaceClubSite(
        club_id=1, club_name_canonical="X",
        website="https://x.example.com",
    )
    page = _wrap_jsonld(org_payload)
    session = _FakeSession({
        "https://x.example.com/": page,
        "https://x.example.com/coaches": page,
        "https://x.example.com/staff": page,
    })
    harvest = harvest_squarespace_club(site, session=session)
    assert len(harvest.coach_rows) == 1
