"""
Tests for the US Club iD scraper scaffold.

Discovery test mocks the SoccerWire WP REST API. The Option B stub
test asserts the documented credential-flip error message.
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, List
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.usclub_id import (  # noqa: E402
    scrape_soccerwire_id_articles,
    scrape_usclubsoccer_members,
)
from ingest.id_selection_writer import _normalize_row  # noqa: E402


# --------------------------------------------------------------------------- discovery


class _FakeResponse:
    def __init__(
        self,
        payload: List[Dict[str, Any]],
        status_code: int = 200,
        total_pages: int = 1,
    ):
        self._payload = payload
        self.status_code = status_code
        self.headers = {"X-WP-TotalPages": str(total_pages)}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> List[Dict[str, Any]]:
        return self._payload


def _post(post_id: int, title: str, slug: str) -> Dict[str, Any]:
    return {
        "id": post_id,
        "title": {"rendered": title},
        "slug": slug,
        "link": f"https://www.soccerwire.com/news/{slug}/",
        "date": "2026-03-15T12:00:00",
        "excerpt": {"rendered": f"<p>Excerpt for {title}</p>"},
    }


def test_scrape_soccerwire_id_articles_returns_list_filtered_by_keywords():
    """Mock WP API → only iD-related posts should be returned, in dict shape."""
    payload = [
        _post(1, "U.S. Club iD National Pool U-15 Boys Selections",
              "us-club-id-national-pool-u15-boys"),
        _post(2, "Some unrelated MLS Next signing",
              "mls-next-signing-acme-fc"),
        _post(3, "iD Training Center Northeast Recap",
              "id-training-center-northeast-recap"),
        _post(4, "ECNL all-star roster announced",
              "ecnl-all-star-roster"),
    ]

    with mock.patch(
        "extractors.usclub_id.requests.get",
        return_value=_FakeResponse(payload, total_pages=1),
    ):
        rows = scrape_soccerwire_id_articles(max_pages=1)

    assert isinstance(rows, list)
    assert len(rows) == 2
    titles = [r["title"] for r in rows]
    assert any("National Pool" in t for t in titles)
    assert any("Training Center" in t for t in titles)
    # Excerpt HTML must be stripped.
    for r in rows:
        assert "<" not in r["excerpt"]
        assert r["url"].startswith("https://www.soccerwire.com/")
        assert r["id"]
        assert r["slug"]


def test_scrape_soccerwire_id_articles_fail_soft_on_network_error():
    """A network exception during pagination must yield an empty list,
    never raise — runner depends on fail-soft."""
    with mock.patch(
        "extractors.usclub_id.requests.get",
        side_effect=__import__("requests").RequestException("boom"),
    ):
        rows = scrape_soccerwire_id_articles(max_pages=2)
    assert rows == []


# --------------------------------------------------------------------------- Option B stub


def test_scrape_usclubsoccer_members_stub_raises_with_documented_message():
    with pytest.raises(NotImplementedError) as excinfo:
        scrape_usclubsoccer_members()
    msg = str(excinfo.value)
    assert "US Club iD members area requires login" in msg
    assert "USCLUB_USERNAME" in msg
    assert "USCLUB_PASSWORD" in msg


# --------------------------------------------------------------------------- writer normalization


def test_writer_normalize_row_requires_natural_key_fields():
    """The writer's row normalizer must reject rows missing any required
    natural-key field. This is the contract that protects the natural-key
    upsert from silent partial writes."""
    valid = {
        "player_name": "John Smith",
        "selection_year": 2026,
        "birth_year": 2010,
        "gender": "M",
        "pool_tier": "national",
        "source": "soccerwire",
    }
    out = _normalize_row(valid)
    assert out["player_name"] == "John Smith"
    assert out["selection_year"] == 2026
    assert out["birth_year"] == 2010

    for missing in ("player_name", "selection_year", "gender", "pool_tier", "source"):
        bad = dict(valid)
        bad[missing] = None
        with pytest.raises(ValueError):
            _normalize_row(bad)
