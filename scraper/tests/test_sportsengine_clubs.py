"""
Tests for the SportsEngine + JSON-LD club extractor.

All tests are fixture-driven and pure — no network, no DB. Mirrors the
inline-fixture style of ``test_squarespace_clubs`` (PR #58).

Coverage:
  (a) SportsEngine detection on a real-looking HTTP response (header
      and HTML-signature paths)
  (b) SportsTeam.athlete[] extraction → roster rows
  (c) Person blocks (top-level + nested under member/coach) → coach
      rows tagged with platform_family='sportsengine'
  (d) Event tryout-keyword filter accepts tryouts and rejects
      tournaments
  (e) Fail-soft when the site is not SportsEngine (Wix / Squarespace)
  (f) Organization metadata → enrichment row
  (g) /page/show/<id>-<slug> URL discovery from homepage anchors
  (h) End-to-end harvest with mocked HTTP including a /page/show URL
  (i) Auth-walled (HTTP 401/403) responses are counted, never crashed
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.cms_detect import detect_cms  # noqa: E402
from extractors.jsonld_parser import (  # noqa: E402
    extract_athletes,
    extract_jsonld,
    extract_persons,
)
from extractors.sportsengine_clubs import (  # noqa: E402
    DISCOVERY_CONFIDENCE,
    SportsEngineClubSite,
    _athlete_to_roster_row,
    _event_to_tryout_row,
    _organization_to_enrichment_row,
    _parse_event_date,
    _person_to_coach_row,
    discover_page_show_urls,
    harvest_sportsengine_club,
    is_tryout_event,
)


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
    just enough that ``sportsengine_clubs._fetch`` works against it.
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


def _wrap_jsonld(
    payload: str,
    *,
    server_sportsengine: bool = True,
    extra_body: str = "",
) -> _FakeResponse:
    """Wrap a JSON-LD payload into a SportsEngine-looking page response.

    By default sets the ``X-Powered-By: ngin`` header (the SE-specific
    branding ``cms_detect`` keys off of) AND drops a ``sportngin.com``
    asset reference into the body so HTML-signature detection also
    works. Pass ``extra_body=...`` to inject additional HTML (used by
    the /page/show discovery test).
    """
    body = (
        "<!doctype html><html><head>"
        '<link href="https://assets.sportngin.com/site/abc/site.css">'
        f'<script type="application/ld+json">{payload}</script>'
        "</head><body>" + extra_body + "</body></html>"
    )
    headers = (
        {"X-Powered-By": "ngin", "content-type": "text/html"}
        if server_sportsengine
        else {"content-type": "text/html"}
    )
    return _FakeResponse(headers=headers, text=body, status_code=200)


# --------------------------------------------------------------- (a) detection


def test_sportsengine_detection_via_powered_by_header():
    """``X-Powered-By: ngin`` is the canonical SE header signature."""
    resp = _FakeResponse(
        headers={"X-Powered-By": "ngin", "content-type": "text/html"},
        text="<html><body>hello</body></html>",
    )
    assert detect_cms(resp) == "sportsengine"


def test_sportsengine_detection_via_html_signature():
    """A response without the ``X-Powered-By`` header but with
    ``sportngin.com`` asset references must still classify as SE."""
    resp = _FakeResponse(
        headers={"content-type": "text/html"},
        text=(
            "<html><head>"
            "<script src=\"https://assets.sportngin.com/site/x.js\"></script>"
            "</head><body></body></html>"
        ),
    )
    assert detect_cms(resp) == "sportsengine"


def test_sportsengine_detection_on_realistic_response():
    """A response with both the SE header AND the body signature must
    classify as SportsEngine."""
    resp = _wrap_jsonld(
        '{"@context":"https://schema.org","@type":"Organization","name":"Foley FC"}'
    )
    assert detect_cms(resp) == "sportsengine"


def test_non_sportsengine_response_rejected_in_harvest():
    """Fail-soft: if the homepage isn't SportsEngine, the harvest
    returns is_sportsengine=False and ALL output lists are empty."""
    site = SportsEngineClubSite(
        club_id=1, club_name_canonical="Wix Club FC",
        website="https://wixclub.example.com",
    )
    wix_resp = _FakeResponse(
        headers={"X-Wix-Request-Id": "abc", "content-type": "text/html"},
        text="<html><body>wix</body></html>",
    )
    session = _FakeSession({"https://wixclub.example.com/": wix_resp})

    harvest = harvest_sportsengine_club(site, session=session)
    assert harvest.is_sportsengine is False
    assert harvest.roster_rows == []
    assert harvest.coach_rows == []
    assert harvest.tryout_rows == []
    assert harvest.enrichment_row is None


def test_squarespace_response_rejected_in_harvest():
    """Cross-cutting fail-soft check: a Squarespace site must NOT be
    misclassified by this extractor (the two share the same shared
    infra)."""
    site = SportsEngineClubSite(
        club_id=1, club_name_canonical="Squarespace FC",
        website="https://sqsp.example.com",
    )
    sqsp_resp = _FakeResponse(
        headers={"Server": "Squarespace", "content-type": "text/html"},
        text=(
            "<html><head>"
            "<link href=\"https://static1.squarespace.com/x.css\">"
            "</head><body></body></html>"
        ),
    )
    session = _FakeSession({"https://sqsp.example.com/": sqsp_resp})

    harvest = harvest_sportsengine_club(site, session=session)
    assert harvest.is_sportsengine is False


# --------------------------------------------------------------- (b) athletes


def test_extract_sports_team_athletes_into_roster_rows():
    """A SE page with a SportsTeam JSON-LD block carrying an
    ``athlete`` list must surface as one roster_row per player."""
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
    assert by_name["Player One"]["age_group"] == "U12"
    assert by_name["Player One"]["gender"] == "M"
    # Linker contract: every row has club_name_raw set, club_id is NOT
    # present (writer adds NULL).
    assert all(r["club_name_raw"] == "Foley FC" for r in rows)
    assert all("club_id" not in r for r in rows)
    assert all(r["snapshot_date"] == snap for r in rows)


def test_athlete_row_drops_unnamed_athletes():
    bad = {"@type": "Person"}
    row = _athlete_to_roster_row(
        bad, team={"@type": "SportsTeam", "name": "X"},
        club_name_raw="X", source_url="https://x/",
        snapshot_date=datetime(2026, 1, 1),
    )
    assert row is None


# --------------------------------------------------------------- (c) persons


def test_extract_top_level_and_nested_person_blocks_tagged_sportsengine():
    """Both top-level Person blocks AND Person dicts nested under
    ``member`` / ``coach`` must surface as coach rows. Every row must
    be tagged ``platform_family='sportsengine'`` (the enum already
    enumerates this value).

    Fixture names are real-looking Western names; earlier placeholder
    names like "Coach Alpha" are now filtered by the shared
    coach-name guard (token "coach" is blocklisted).
    """
    payload = """
    {
      "@context":"https://schema.org",
      "@graph":[
        {"@type":"Person","name":"Jane Doe","jobTitle":"Director of Coaching",
         "email":"jane@example.com"},
        {"@type":"SportsOrganization","name":"Foley FC",
         "member":[
           {"@type":"Person","name":"Maria Rodriguez","jobTitle":"Head Coach",
            "email":"alpha@example.com"},
           {"@type":"Person","name":"Kevin O'Brien","jobTitle":"Assistant"}
         ],
         "coach":{"@type":"Person","name":"Jean-Pierre Dubois","jobTitle":"Head Coach U12"}
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
    assert {"Jane Doe", "Maria Rodriguez", "Kevin O'Brien", "Jean-Pierre Dubois"}.issubset(names)
    by_name = {r["name"]: r for r in rows}
    assert by_name["Jane Doe"]["email"] == "jane@example.com"
    assert by_name["Jane Doe"]["title"] == "Director of Coaching"
    assert by_name["Maria Rodriguez"]["club_id"] == 42
    # Every row must be tagged sportsengine — distinguishes this from
    # the Squarespace extractor's 'unknown' tag.
    assert all(r["platform_family"] == "sportsengine" for r in rows)
    assert all(r["confidence"] == DISCOVERY_CONFIDENCE for r in rows)


def test_person_row_rejects_polluted_name():
    """Regression guard: the shared name guard MUST filter nav-menu
    text, CTA strings, and blocklist-phrase values from JSON-LD
    Person.name. This is the first line of defense against the
    pollution category that accounted for ~90% of coach_discoveries
    rows in the April-2026 audit."""
    for polluted in (
        "Newsletter Sign-Up",
        "About Us",
        "Head Coach",
        "OPEN TRAINING & TRYOUTS",
    ):
        row = _person_to_coach_row(
            {"@type": "Person", "name": polluted, "jobTitle": "X"},
            club_id=1,
            source_url="https://x.example.com/staff",
        )
        assert row is None, f"{polluted!r} should be filtered by the name guard"


def test_person_row_strips_mailto_prefix():
    row = _person_to_coach_row(
        {
            "@type": "Person",
            "name": "John Smith",
            "email": "MAILTO:foo@BAR.com?cc=x",
        },
        club_id=1,
        source_url="https://x/",
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
        "logo": "https://assets.sportngin.com/site/abc/logo.png",
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


# --------------------------------------------------------------- (g) page-show URL discovery


def test_discover_page_show_urls_finds_keyword_slugs():
    """Anchors whose href matches /page/show/<id>-<slug> AND whose
    slug ends in (or contains) a known keyword get discovered."""
    html = (
        '<a href="/page/show/123-staff">Staff</a>'
        '<a href="/page/show/456-boys-roster">Boys Roster</a>'
        '<a href="/page/show/789-coaching-staff">Coaches</a>'
        '<a href="/page/show/999-news">News</a>'   # not a keyword → skipped
        '<a href="/blog/post-1">Random</a>'         # not page/show → skipped
    )
    out = discover_page_show_urls(html, base="https://foleyfc.example.com")
    assert "https://foleyfc.example.com/page/show/123-staff" in out
    assert "https://foleyfc.example.com/page/show/456-boys-roster" in out
    assert "https://foleyfc.example.com/page/show/789-coaching-staff" in out
    assert all("999-news" not in u for u in out)
    assert all("blog" not in u for u in out)


def test_discover_page_show_urls_dedups_and_caps_at_limit():
    html = (
        '<a href="/page/show/1-staff">a</a>'
        '<a href="/page/show/1-staff">b</a>'  # duplicate
        '<a href="/page/show/2-roster">c</a>'
        '<a href="/page/show/3-coaches">d</a>'
    )
    out = discover_page_show_urls(
        html, base="https://x.example.com", limit=2,
    )
    assert len(out) == 2
    assert out[0].endswith("1-staff")
    assert out[1].endswith("2-roster")


def test_discover_page_show_urls_handles_empty_html():
    assert discover_page_show_urls("", base="https://x.example.com") == []


# --------------------------------------------------------------- (h) end-to-end harvest


def test_harvest_sportsengine_club_full_flow_with_mocked_session():
    """End-to-end harvest with a real-looking SE homepage that links
    to a /page/show staff URL plus a generic /coaches subpath. Verifies
    the four sinks are populated and pages_fetched is correct."""
    home = _wrap_jsonld(
        """
        {
          "@context":"https://schema.org",
          "@type":"SportsOrganization",
          "name":"Foley FC",
          "logo":"https://assets.sportngin.com/site/abc/logo.png",
          "sameAs":["https://www.instagram.com/foleyfc/"]
        }
        """,
        # Inject anchors so the discovery scans pick them up.
        extra_body=(
            '<nav>'
            '<a href="/page/show/1234-staff">Staff</a>'
            '<a href="/page/show/5678-tryouts">Tryouts</a>'
            '</nav>'
        ),
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
          "name":"Anna Lee",
          "jobTitle":"Head Coach"
        }
        """
    )
    page_show_tryouts = _wrap_jsonld(
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

    site = SportsEngineClubSite(
        club_id=42,
        club_name_canonical="Foley FC",
        website="https://foleyfc.example.com",
    )
    session = _FakeSession({
        "https://foleyfc.example.com/": home,
        "https://foleyfc.example.com/teams": teams_page,
        "https://foleyfc.example.com/coaches": coaches_page,
        "https://foleyfc.example.com/page/show/5678-tryouts": page_show_tryouts,
        # /page/show/1234-staff → 404 (intentional: confirms harvest
        # tolerates a discovered URL that doesn't resolve).
        # /staff, /about, /tryouts → 404, harvest skips them.
    })

    harvest = harvest_sportsengine_club(site, session=session)

    assert harvest.is_sportsengine is True
    # Homepage + 3 successful subpaths/discovered URLs = 4 fetched.
    assert harvest.pages_fetched == 4
    # Roster: one row from /teams.
    assert len(harvest.roster_rows) == 1
    assert harvest.roster_rows[0]["player_name"] == "Player A"
    # Coaches: one row from /coaches, tagged sportsengine.
    assert len(harvest.coach_rows) == 1
    assert harvest.coach_rows[0]["name"] == "Anna Lee"
    assert harvest.coach_rows[0]["club_id"] == 42
    assert harvest.coach_rows[0]["platform_family"] == "sportsengine"
    # Tryouts: one row from the discovered /page/show/...-tryouts URL.
    assert len(harvest.tryout_rows) == 1
    assert harvest.tryout_rows[0]["tryout_date"] == datetime(2026, 8, 5)
    # Enrichment: pulled from homepage Organization block.
    assert harvest.enrichment_row is not None
    assert harvest.enrichment_row["club_id"] == 42
    assert "instagram.com" in (harvest.enrichment_row["instagram"] or "")


def test_harvest_fails_soft_on_homepage_404():
    site = SportsEngineClubSite(
        club_id=1, club_name_canonical="Dead FC",
        website="https://dead.example.com",
    )
    session = _FakeSession({})  # everything 404s
    harvest = harvest_sportsengine_club(site, session=session)
    assert harvest.is_sportsengine is False
    assert harvest.pages_fetched == 0


def test_harvest_skips_unparseable_website_url():
    site = SportsEngineClubSite(
        club_id=1, club_name_canonical="Bad URL",
        website="not-a-url",
    )
    session = _FakeSession({})
    harvest = harvest_sportsengine_club(site, session=session)
    assert harvest.is_sportsengine is False
    assert harvest.pages_fetched == 0
    assert session.calls == []


def test_harvest_dedups_repeated_persons_across_pages():
    """Same coach surfaced on /staff and /coaches yields only one row."""
    org_payload = '{"@context":"https://schema.org","@type":"Person","name":"Anna Lee","jobTitle":"Head"}'
    site = SportsEngineClubSite(
        club_id=1, club_name_canonical="X",
        website="https://x.example.com",
    )
    page = _wrap_jsonld(org_payload)
    session = _FakeSession({
        "https://x.example.com/": page,
        "https://x.example.com/coaches": page,
        "https://x.example.com/staff": page,
    })
    harvest = harvest_sportsengine_club(site, session=session)
    assert len(harvest.coach_rows) == 1


# --------------------------------------------------------------- (i) auth-walled


def test_harvest_counts_auth_walled_subpath_and_continues():
    """A 401/403 on a subpath increments auth_walled_endpoints, doesn't
    crash, and the harvest proceeds with whatever else succeeded."""
    home = _wrap_jsonld(
        '{"@context":"https://schema.org","@type":"SportsOrganization",'
        '"name":"X","logo":"https://assets.sportngin.com/x/logo.png"}'
    )
    auth_walled = _FakeResponse(
        headers={"X-Powered-By": "ngin", "content-type": "text/html"},
        text="<html>nope</html>",
        status_code=403,
    )
    site = SportsEngineClubSite(
        club_id=1, club_name_canonical="X",
        website="https://x.example.com",
    )
    session = _FakeSession({
        "https://x.example.com/": home,
        # /staff is auth-walled → counted, not crashed.
        "https://x.example.com/staff": auth_walled,
    })
    harvest = harvest_sportsengine_club(site, session=session)
    assert harvest.is_sportsengine is True
    assert harvest.auth_walled_endpoints == 1
    # Enrichment from homepage still landed.
    assert harvest.enrichment_row is not None


def test_harvest_bails_when_homepage_is_auth_walled():
    """If the homepage itself is 401/403 we can't run cms_detect — the
    harvest returns ``is_sportsengine=False`` and counts the wall."""
    auth_walled = _FakeResponse(
        headers={"X-Powered-By": "ngin", "content-type": "text/html"},
        text="<html>nope</html>",
        status_code=401,
    )
    site = SportsEngineClubSite(
        club_id=1, club_name_canonical="X",
        website="https://x.example.com",
    )
    session = _FakeSession({"https://x.example.com/": auth_walled})
    harvest = harvest_sportsengine_club(site, session=session)
    assert harvest.is_sportsengine is False
    assert harvest.auth_walled_endpoints == 1
    assert harvest.pages_fetched == 0
