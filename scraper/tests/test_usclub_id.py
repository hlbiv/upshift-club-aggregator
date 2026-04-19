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
    _classify_header_row,
    _infer_birth_year,
    _infer_gender,
    _infer_pool_tier,
    _infer_selection_year,
    _parse_hometown,
    parse_article_body,
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


# --------------------------------------------------------------------------- inference helpers


def test_infer_pool_tier_recognises_national_selection():
    assert _infer_pool_tier(
        "Boys roster selected for 2024 id2 National Selection International Tour",
        "boys-roster-selected-for-2024-id2-national-selection-international-tour",
    ) == "national"


def test_infer_pool_tier_recognises_training_camp():
    assert _infer_pool_tier(
        "US Club Soccer hosts 102 players at the 2026 id2 Boys National Training Camp",
        "us-club-soccer-hosts-102-players-at-the-2026-id2-boys-national-training-camp",
    ) == "training-center"


def test_infer_pool_tier_recognises_regional_selection_event():
    assert _infer_pool_tier(
        "US Club Soccer SOCAL id2 Selection Event rosters revealed (Boys)",
        "us-club-soccer-socal-id2-selection-event-rosters-revealed-boys",
    ) == "regional"


def test_infer_pool_tier_returns_none_for_announcement():
    """Announcement / overview posts shouldn't masquerade as roster
    articles. The runner drops these to keep the writer clean."""
    assert _infer_pool_tier(
        "US Club Soccer announces id2 program schedule for new cycle",
        "us-club-soccer-announces-id2-program-schedule",
    ) is None


def test_infer_gender_returns_M_for_boys_only():
    assert _infer_gender("2024 id2 National Selection Boys", "x") == "M"


def test_infer_gender_returns_F_for_girls_only():
    assert _infer_gender("2024 id2 National Selection Girls", "x") == "F"


def test_infer_gender_returns_none_for_mixed():
    """Mixed-gender articles must NOT be auto-stamped — the natural-key
    uniqueness check on `(player_name, selection_year, birth_year,
    gender, pool_tier)` would silently corrupt the dedup contract.
    Better to skip the article and let an operator carve it up."""
    assert _infer_gender("Girls and Boys rosters revealed", "x") is None


def test_infer_birth_year_explicit_born_in():
    body = "id2 cycle targeting players born in 2010. Eighteen boys were selected..."
    assert _infer_birth_year(body) == 2010


def test_infer_birth_year_age_group():
    body = "Top players from the 2010 age group have been selected."
    assert _infer_birth_year(body) == 2010


def test_infer_birth_year_falls_back_to_u_age():
    """If the article only carries a 'U-15' qualifier, derive birth
    year from selection_year. Approximate (off by one for late-year
    birthdays) but better than NULL."""
    assert _infer_birth_year(
        "U-15 boys selected for...", selection_year=2026,
    ) == 2011


def test_infer_birth_year_returns_none_when_nothing_matches():
    assert _infer_birth_year(
        "Generic article body without any year hint at all",
    ) is None


def test_infer_selection_year_from_iso_date():
    assert _infer_selection_year("2024-02-08T16:46:00") == 2024


def test_infer_selection_year_returns_none_on_garbage():
    assert _infer_selection_year(None) is None
    assert _infer_selection_year("not-a-date") is None


def test_parse_hometown_extracts_state_from_city_state():
    assert _parse_hometown("Laguna Niguel, CA") == "CA"
    assert _parse_hometown("San Francisco, CA") == "CA"


def test_parse_hometown_handles_missing_comma():
    assert _parse_hometown("Worthington OH") == "OH"


def test_parse_hometown_returns_none_for_empty():
    assert _parse_hometown("") is None
    assert _parse_hometown("   ") is None


def test_classify_header_row_requires_name():
    """Without a NAME column we can't anchor a row — classification
    must reject the row so the parser skips the table entirely."""
    assert _classify_header_row(["Position", "Hometown", "Club"]) is None


def test_classify_header_row_maps_all_known_columns():
    cols = _classify_header_row(["NAME", "POSITION", "HOMETOWN", "CLUB"])
    assert cols == {"name": 0, "position": 1, "hometown": 2, "club": 3}


# --------------------------------------------------------------------------- body parsing


# Real shape captured from
# https://www.soccerwire.com/news/boys-roster-selected-for-2024-id2-national-selection-international-tour/
# Trimmed to two players — same structure as the live page (table dir,
# colgroup, tbody → tr → td with data-sheets-value attributes).
_FIXTURE_TABLE_HTML = """
<html><body>
<div class="single__content">
<p>US Club Soccer has selected its boys id2 National Selection team. The id2
program is targeting players born in 2010.</p>
<table dir="ltr" border="1" cellspacing="0" cellpadding="0" data-sheets-root="1">
<colgroup><col width="177" /><col width="79" /><col width="150" /><col width="217" /></colgroup>
<tbody>
<tr>
<td><strong>NAME</strong></td>
<td><strong>POSITION</strong></td>
<td><strong>HOMETOWN</strong></td>
<td><strong>CLUB</strong></td>
</tr>
<tr>
<td>Carter Biondolillo</td>
<td>D</td>
<td>Laguna Niguel, CA</td>
<td>Laguna United FC</td>
</tr>
<tr>
<td>Easton Brooks</td>
<td>MF</td>
<td>Carrollton, TX</td>
<td>AlphaForms</td>
</tr>
</tbody>
</table>
</div></body></html>
"""

_FIXTURE_ARTICLE = {
    "id": 12345,
    "title": "Boys roster selected for 2024 id2 National Selection International Tour",
    "slug": "boys-roster-selected-for-2024-id2-national-selection-international-tour",
    "url": "https://www.soccerwire.com/news/boys-roster-selected-for-2024-id2-national-selection-international-tour/",
    "date": "2024-02-08T16:46:00",
}


def test_parse_article_body_extracts_two_player_rows_with_birth_year():
    rows = parse_article_body(_FIXTURE_TABLE_HTML, article=_FIXTURE_ARTICLE)
    assert len(rows) == 2
    first = rows[0]
    assert first["player_name"] == "Carter Biondolillo"
    assert first["selection_year"] == 2024
    # Birth year inferred from "born in 2010" in surrounding prose.
    assert first["birth_year"] == 2010
    assert first["gender"] == "M"
    assert first["pool_tier"] == "national"
    assert first["club_name_raw"] == "Laguna United FC"
    assert first["state"] == "CA"
    assert first["position"] == "D"
    assert first["source"] == "soccerwire"
    assert first["source_url"].startswith("https://www.soccerwire.com/")
    # announced_at is the raw article date string — the writer parses.
    assert first["announced_at"] == "2024-02-08T16:46:00"

    second = rows[1]
    assert second["player_name"] == "Easton Brooks"
    assert second["state"] == "TX"
    assert second["club_name_raw"] == "AlphaForms"
    # Same selection_year + birth_year + gender + pool_tier across rows
    assert second["selection_year"] == 2024
    assert second["birth_year"] == 2010
    assert second["gender"] == "M"
    assert second["pool_tier"] == "national"


def test_parse_article_body_rows_satisfy_writer_normalizer():
    """Round-trip: every row the parser emits must pass the writer's
    natural-key normalizer. Catches drift between parser output keys
    and the writer's required-key contract."""
    rows = parse_article_body(_FIXTURE_TABLE_HTML, article=_FIXTURE_ARTICLE)
    for row in rows:
        normalized = _normalize_row(row)
        assert normalized["player_name"] == row["player_name"]


# Image-only roster fixture — no <table>, just an <img>. The real
# 2024 East Regional Camp post (and most regional Selection Event posts)
# look like this. Parser must skip + log warning, never crash.
_FIXTURE_IMAGE_ONLY_HTML = """
<html><body>
<div class="single__content">
<p>The top 36 boys from the recently-completed wave of id2 Selection Events
were chosen to participate in the camp. Click the image below.</p>
<p><img src="https://www.soccerwire.com/wp-content/uploads/boys-roster.jpg" /></p>
</div></body></html>
"""

_FIXTURE_IMAGE_ARTICLE = {
    "id": 9999,
    "title": "US Club Soccer SOCAL id2 Selection Event rosters revealed (Boys)",
    "slug": "us-club-soccer-socal-id2-selection-event-rosters-revealed-boys",
    "url": "https://www.soccerwire.com/news/us-club-soccer-socal-id2-selection-event-rosters-revealed-boys/",
    "date": "2022-11-21T12:00:00",
}


def test_parse_article_body_image_only_yields_zero_rows_no_crash(caplog):
    """The most common 'broken' template: roster as an image. We must
    skip + log, never raise. Otherwise one image-roster post would
    crash the whole runner because runners iterate articles in order."""
    import logging as _logging
    caplog.set_level(_logging.WARNING)

    rows = parse_article_body(_FIXTURE_IMAGE_ONLY_HTML, article=_FIXTURE_IMAGE_ARTICLE)
    assert rows == []
    warned = [r for r in caplog.records if "no <table>" in r.getMessage().lower()]
    assert warned, "expected a 'no <table>' warning"


# Announcement-style article — body prose only, no tables. Real pattern:
# https://www.soccerwire.com/news/us-club-soccer-announces-upcoming-id2-programming-for-2024-25-cycle/
_FIXTURE_ANNOUNCEMENT_HTML = """
<html><body>
<div class="single__content">
<p>US Club Soccer is preparing its id2 Program with a schedule of 2024-25
id2 programming targeted to boys and girls born in 2011.</p>
<p>NPL member leagues and other top leagues will co-host id2 Selection
events in the fall and winter.</p>
</div></body></html>
"""

_FIXTURE_ANNOUNCEMENT_ARTICLE = {
    "id": 11111,
    "title": "US Club Soccer announces upcoming id2 programming for 2024-25 cycle",
    "slug": "us-club-soccer-announces-upcoming-id2-programming-for-2024-25-cycle",
    "url": "https://www.soccerwire.com/news/us-club-soccer-announces-upcoming-id2-programming-for-2024-25-cycle/",
    "date": "2024-09-10T12:00:00",
}


def test_parse_article_body_announcement_post_yields_zero_rows():
    """Announcement posts have no roster table. The pool-tier inference
    on the title returns None (no 'national'/'regional'/'training'
    marker), so we exit early with zero rows."""
    rows = parse_article_body(
        _FIXTURE_ANNOUNCEMENT_HTML, article=_FIXTURE_ANNOUNCEMENT_ARTICLE,
    )
    assert rows == []


# Header-mismatch fixture: a roster table with the right columns but
# different casing / wording (real article variation).
_FIXTURE_LOWERCASE_HEADERS_HTML = """
<html><body>
<div class="single__content">
<p>The 2025 id2 East Regional roster (Girls). Born in 2012.</p>
<table>
<tbody>
<tr>
<td>Player Name</td>
<td>Position</td>
<td>City, State</td>
<td>Club</td>
</tr>
<tr>
<td>Aria Smith</td>
<td>F</td>
<td>Boston, MA</td>
<td>NEFC</td>
</tr>
</tbody>
</table>
</div></body></html>
"""


def test_parse_article_body_lowercase_or_lower_specificity_headers_still_work():
    """`Player Name` instead of `NAME` and `City, State` instead of
    `HOMETOWN` are both real variations. Match must be substring +
    case-insensitive so the parser doesn't get brittle to header copy."""
    article = {
        "id": 22222,
        "title": "2025 id2 East Regional Selection roster (Girls)",
        "slug": "2025-id2-east-regional-selection-roster-girls",
        "url": "https://www.soccerwire.com/news/x/",
        "date": "2025-01-15T12:00:00",
    }
    rows = parse_article_body(_FIXTURE_LOWERCASE_HEADERS_HTML, article=article)
    assert len(rows) == 1
    r = rows[0]
    assert r["player_name"] == "Aria Smith"
    assert r["club_name_raw"] == "NEFC"
    assert r["gender"] == "F"
    assert r["birth_year"] == 2012


# Unrelated <table> fixture — articles can have a related-posts widget
# rendered as a table, or an editorial sidebar table. Without a NAME
# column header the parser must produce 0 rows (and not crash).
_FIXTURE_UNRELATED_TABLE_HTML = """
<html><body>
<div class="single__content">
<p>2024 id2 Boys National Selection. Players born in 2010.</p>
<table>
<tr><th>Date</th><th>Opponent</th><th>Score</th></tr>
<tr><td>March 1</td><td>FC Porto</td><td>2-1</td></tr>
</table>
</div></body></html>
"""


def test_parse_article_body_ignores_non_roster_tables():
    """Articles often contain unrelated tables (schedule grids, related
    posts widgets). Without a NAME column we MUST treat them as
    non-rosters — otherwise we'd silently emit a 'March 1' player."""
    article = {
        "id": 33333,
        "title": "2024 id2 Boys National Selection match results",
        "slug": "2024-id2-boys-national-selection-match-results",
        "url": "https://www.soccerwire.com/news/y/",
        "date": "2024-04-01T12:00:00",
    }
    rows = parse_article_body(_FIXTURE_UNRELATED_TABLE_HTML, article=article)
    assert rows == []


def test_parse_article_body_no_html_yields_empty_no_crash():
    rows = parse_article_body("", article=_FIXTURE_ARTICLE)
    assert rows == []


def test_parse_article_body_missing_date_skips_article(caplog):
    import logging as _logging
    caplog.set_level(_logging.WARNING)

    bad_article = dict(_FIXTURE_ARTICLE)
    bad_article["date"] = ""
    rows = parse_article_body(_FIXTURE_TABLE_HTML, article=bad_article)
    assert rows == []
    assert any("missing/unparseable date" in r.getMessage() for r in caplog.records)


def test_parse_article_body_ambiguous_gender_skips_article(caplog):
    import logging as _logging
    caplog.set_level(_logging.WARNING)

    mixed_article = dict(_FIXTURE_ARTICLE)
    mixed_article["title"] = (
        "US Club Soccer unveils girls and boys rosters for 2024 id2 East Regional Camp"
    )
    mixed_article["slug"] = (
        "us-club-soccer-unveils-girls-and-boys-rosters-for-2024-id2-east-regional-camp"
    )
    rows = parse_article_body(_FIXTURE_TABLE_HTML, article=mixed_article)
    assert rows == []
    assert any("ambiguous or mixed gender" in r.getMessage() for r in caplog.records)
