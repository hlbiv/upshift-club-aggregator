"""
Tests for the Duda CMS + 360Player club extractor.

Pure-shape tests — no DB, no real network. The fetch hook is injected
to keep tests offline; ``probe_site`` reaches into ``requests.get``
which we monkey-patch via ``unittest.mock``.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.duda_360player_clubs import (  # noqa: E402
    ProbeResult,
    discover_360player_directory,
    event_block_to_tryout_row,
    extract_rows_from_probe,
    person_block_to_coach_dict,
    probe_site,
    scrape_duda_360player_clubs,
)


# ---------------------------------------------------------------- fake response


class FakeResponse:
    """Minimal stand-in for ``requests.Response``.

    Only the attrs ``probe_site`` + ``cms_detect`` look at need to be
    populated: ``status_code``, ``headers``, ``text``.
    """

    def __init__(
        self,
        *,
        text: str = "",
        status_code: int = 200,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.text = text
        self.status_code = status_code
        self.headers = headers or {}


# ---------------------------------------------------------------- HTML fixtures (inline)


_DUDA_HOMEPAGE_WITH_EVENT = """
<!doctype html>
<html>
  <head>
    <link rel="preconnect" href="https://irp.cdn-website.com/">
    <script type="application/ld+json">
    {
      "@context": "https://schema.org",
      "@type": "Event",
      "name": "U12 Boys Tryouts",
      "startDate": "2026-08-05T18:00:00",
      "url": "https://example-duda-club.com/tryouts/",
      "location": {
        "@type": "Place",
        "name": "Acme Soccer Park",
        "address": {
          "streetAddress": "123 Pitch Way",
          "addressLocality": "Springfield",
          "addressRegion": "OH",
          "postalCode": "45501"
        }
      }
    }
    </script>
  </head>
  <body>Welcome</body>
</html>
""".strip()


_360PLAYER_HOMEPAGE_WITH_PERSON = """
<!doctype html>
<html>
  <head>
    <script src="https://cdn.360player.com/widget.js"></script>
    <script type="application/ld+json">
    {
      "@context": "https://schema.org",
      "@type": "SportsOrganization",
      "name": "Acme FC",
      "member": [
        {"@type": "Person", "name": "Jane Coach", "jobTitle": "Director of Coaching",
         "email": "jane@example.com"}
      ]
    }
    </script>
  </head>
  <body>Acme FC official site</body>
</html>
""".strip()


_NON_TARGET_HOMEPAGE = """
<!doctype html>
<html>
  <head>
    <meta name="generator" content="WordPress 6.4.2">
  </head>
  <body>This is a WordPress site, not Duda or 360Player.</body>
</html>
""".strip()


_360PLAYER_DIRECTORY_HTML = """
<!doctype html>
<html>
  <body>
    <h1>Clubs on 360Player</h1>
    <ul>
      <li><a href="/acme-fc">Acme FC</a></li>
      <li><a href="https://360player.com/blue-stars">Blue Stars Soccer</a></li>
      <li><a href="/about">About 360Player</a></li>
      <li><a href="https://example.com/external">External Link</a></li>
      <li><a href="/clubs">All Clubs</a></li>
    </ul>
  </body>
</html>
""".strip()


# ---------------------------------------------------------------- probe_site


def test_probe_site_detects_duda_and_extracts_event_jsonld():
    """A Duda homepage with an Event JSON-LD block → ``probe_site`` returns
    detected_platform='duda' and one JSON-LD block."""
    fake = FakeResponse(text=_DUDA_HOMEPAGE_WITH_EVENT)
    with mock.patch(
        "extractors.duda_360player_clubs.requests.get",
        return_value=fake,
    ):
        probe = probe_site("Acme Duda FC", "https://example-duda-club.com")

    assert probe.detected_platform == "duda"
    assert len(probe.jsonld_blocks) == 1
    assert probe.jsonld_blocks[0]["@type"] == "Event"
    assert probe.probed_urls == ["https://example-duda-club.com/"]


def test_probe_site_detects_360player():
    fake = FakeResponse(text=_360PLAYER_HOMEPAGE_WITH_PERSON)
    with mock.patch(
        "extractors.duda_360player_clubs.requests.get",
        return_value=fake,
    ):
        probe = probe_site("Acme FC", "https://acme-fc.example.com")

    assert probe.detected_platform == "360player"
    # SportsOrganization block is present; extract_persons will surface
    # the nested Person separately.
    assert any(b.get("@type") == "SportsOrganization" for b in probe.jsonld_blocks)


def test_probe_site_skips_non_target_platforms():
    fake = FakeResponse(text=_NON_TARGET_HOMEPAGE)
    with mock.patch(
        "extractors.duda_360player_clubs.requests.get",
        return_value=fake,
    ):
        probe = probe_site("Some WP Club", "https://wp-club.example.com")

    # WordPress is not a target — detected_platform stays None and we
    # skip subpath probing entirely (saves HTTP requests).
    assert probe.detected_platform is None
    assert probe.jsonld_blocks == []


def test_probe_site_fail_soft_on_network_error():
    """A network exception during fetch → empty ProbeResult, not a raise."""
    import requests as _req

    with mock.patch(
        "extractors.duda_360player_clubs.requests.get",
        side_effect=_req.exceptions.ConnectionError("boom"),
    ):
        probe = probe_site("Bad Site", "https://invalid.example")

    assert probe.detected_platform is None
    assert probe.jsonld_blocks == []
    # Documents that probe_site never raises; the runner depends on this.


def test_probe_site_fail_soft_on_non_200():
    fake = FakeResponse(text="", status_code=404)
    with mock.patch(
        "extractors.duda_360player_clubs.requests.get",
        return_value=fake,
    ):
        probe = probe_site("Missing", "https://missing.example")

    assert probe.detected_platform is None
    assert probe.error == "homepage unreachable"


# ---------------------------------------------------------------- JSON-LD → row mapping


def test_event_block_to_tryout_row_maps_full_event():
    event = {
        "@type": "Event",
        "name": "Girls U14 Tryouts",
        "startDate": "2026-08-12",
        "url": "https://acme.example.com/tryouts/",
        "location": {
            "@type": "Place",
            "name": "Acme Field",
            "address": "1 Pitch Ln, Town, ST",
        },
    }
    row = event_block_to_tryout_row(
        event,
        club_name_raw="Acme FC",
        source_url="https://acme.example.com/",
    )
    assert row is not None
    assert row["club_name_raw"] == "Acme FC"
    assert row["tryout_date"] == datetime(2026, 8, 12)
    assert row["age_group"] == "U14"
    assert row["gender"] == "F"
    assert "Acme Field" in row["location"]
    assert row["url"] == "https://acme.example.com/tryouts/"
    assert "jsonld_event" in (row["notes"] or "")


def test_event_block_to_tryout_row_skips_when_no_date_and_no_url():
    """An Event with neither date nor URL → None (nothing useful to write)."""
    event = {
        "@type": "Event",
        "name": "Some Tryout",
    }
    row = event_block_to_tryout_row(
        event, club_name_raw="X", source_url="https://x/",
    )
    assert row is None


def test_event_block_to_tryout_row_url_only_emits_registration_row():
    """Event with a URL but no date should still emit a row — it carries
    the registration link for downstream UI."""
    event = {
        "@type": "Event",
        "name": "Boys U10 Registration",
        "url": "https://acme.example.com/register/",
    }
    row = event_block_to_tryout_row(
        event, club_name_raw="Acme", source_url="https://acme.example.com/",
    )
    assert row is not None
    assert row["tryout_date"] is None
    assert row["url"] == "https://acme.example.com/register/"
    assert row["age_group"] == "U10"
    assert row["gender"] == "M"


def test_person_block_to_coach_dict_strips_blank_fields():
    person = {"@type": "Person", "name": "Jane Doe", "jobTitle": "Head Coach", "email": ""}
    out = person_block_to_coach_dict(
        person, club_name_raw="Acme", source_url="https://acme/",
    )
    assert out is not None
    assert out["name"] == "Jane Doe"
    assert out["title"] == "Head Coach"
    assert out["email"] is None  # blank → None, not ""


def test_person_block_to_coach_dict_requires_name():
    out = person_block_to_coach_dict(
        {"@type": "Person", "name": ""},
        club_name_raw="X", source_url="https://x/",
    )
    assert out is None


def test_extract_rows_from_probe_collects_events_and_persons():
    """Given a ProbeResult with both an Event and a SportsOrganization
    holding a Person, the extractor surfaces one tryout row + one coach."""
    probe = ProbeResult(
        club_name_raw="Acme FC",
        website="https://acme.example.com",
        detected_platform="360player",
        probed_urls=["https://acme.example.com/"],
        jsonld_blocks=[
            {
                "@type": "Event",
                "name": "U13 Boys Tryouts",
                "startDate": "2026-08-05",
                "url": "https://acme.example.com/tryouts/",
            },
            {
                "@type": "SportsOrganization",
                "name": "Acme FC",
                "member": [
                    {"@type": "Person", "name": "Jane Coach", "jobTitle": "DOC"}
                ],
            },
        ],
    )
    out = extract_rows_from_probe(probe)
    assert len(out["tryouts"]) == 1
    assert len(out["coach_discoveries"]) == 1
    assert out["tryouts"][0]["club_name_raw"] == "Acme FC"
    assert out["coach_discoveries"][0]["name"] == "Jane Coach"


def test_extract_rows_from_probe_empty_when_no_jsonld():
    probe = ProbeResult(club_name_raw="X", website="https://x")
    out = extract_rows_from_probe(probe)
    assert out == {"tryouts": [], "coach_discoveries": []}


# ---------------------------------------------------------------- 360Player directory


def test_discover_360player_directory_parses_anchors():
    fake = FakeResponse(text=_360PLAYER_DIRECTORY_HTML)

    def _stub_fetch(url: str, timeout: int = 15):
        return fake

    out = discover_360player_directory(fetch=_stub_fetch)
    # acme-fc + blue-stars are valid; about / external / clubs are filtered.
    names = {entry["club_name_raw"] for entry in out}
    assert "Acme FC" in names
    assert "Blue Stars Soccer" in names
    assert "About 360Player" not in names
    assert "External Link" not in names
    assert "All Clubs" not in names


def test_discover_360player_directory_returns_empty_on_404():
    def _stub_fetch(url: str, timeout: int = 15):
        return None

    assert discover_360player_directory(fetch=_stub_fetch) == []


def test_discover_360player_directory_returns_empty_on_empty_body():
    def _stub_fetch(url: str, timeout: int = 15):
        return FakeResponse(text="")

    assert discover_360player_directory(fetch=_stub_fetch) == []


# ---------------------------------------------------------------- batch entry


def test_scrape_duda_360player_clubs_aggregates_stats():
    """The batch entry runs probe_site on each entry and aggregates counts.

    We mock ``requests.get`` to return the duda fixture for the first site
    and the WordPress (non-target) fixture for the second, then assert
    the platform breakdown lines up.
    """
    duda_resp = FakeResponse(text=_DUDA_HOMEPAGE_WITH_EVENT)
    other_resp = FakeResponse(text=_NON_TARGET_HOMEPAGE)
    responses = iter([duda_resp, other_resp])

    def _stub_get(url: str, **_kw):
        try:
            return next(responses)
        except StopIteration:
            return FakeResponse(text="", status_code=404)

    with mock.patch(
        "extractors.duda_360player_clubs.requests.get",
        side_effect=_stub_get,
    ):
        result = scrape_duda_360player_clubs([
            {"club_name_raw": "Duda Club", "website": "https://duda.example"},
            {"club_name_raw": "WP Club", "website": "https://wp.example"},
        ])

    assert result["stats"]["sites_probed"] == 2
    assert result["stats"]["duda_sites"] == 1
    assert result["stats"]["other_or_unknown"] == 1
    assert result["stats"]["sites_with_jsonld"] == 1
    # The Duda fixture's Event maps to one tryout row.
    assert len(result["tryouts"]) == 1
    assert result["tryouts"][0]["club_name_raw"] == "Duda Club"


def test_scrape_duda_360player_clubs_skips_blank_entries():
    """Entries missing club_name_raw or website are silently skipped."""
    result = scrape_duda_360player_clubs([
        {"club_name_raw": "", "website": "https://x"},
        {"club_name_raw": "X", "website": ""},
        {"club_name_raw": "", "website": ""},
    ])
    assert result["stats"]["sites_probed"] == 0
    assert result["tryouts"] == []
    assert result["coach_discoveries"] == []
