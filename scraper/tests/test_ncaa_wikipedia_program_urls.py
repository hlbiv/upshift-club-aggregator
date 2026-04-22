"""
Tests for the per-program Wikipedia URL discovery extractor.

Covers:

- ``parse_program_articles`` (4): uses the existing D2 men's fixture to
  pull (school_name, article_title) pairs; rejects rows with no anchor;
  rejects rows whose anchor href isn't a /wiki/<page> link; dedups
  within a page.
- ``_article_title_from_href`` (parametrized): /wiki/, percent-decoding,
  underscore-to-space, special-namespace + redlink rejection.
- ``extract_website_from_wikitext`` (parametrized): bare URL,
  ``{{URL|host/path}}``, ``[https://... text]`` external-link form,
  ``url=`` alias, missing field returns None, comments + ref tags
  stripped.
- ``normalize_school_name`` (parametrized): matches the run-handler's
  fuzzy join behavior.
- ``fetch_program_websites`` (mocked HTTP): single batch, multi-batch
  pagination, batch failure leaves rows as None, ``redirects`` and
  ``normalized`` mapping.
- ``discover_program_urls`` (mocked SIDEARM probe): wires everything
  together end-to-end without DB.

Run::

    python -m pytest scraper/tests/test_ncaa_wikipedia_program_urls.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ncaa_wikipedia_program_urls import (  # noqa: E402
    ProgramArticleRef,
    _article_title_from_href,
    discover_program_urls,
    extract_website_from_wikitext,
    fetch_program_websites,
    normalize_school_name,
    parse_program_articles,
    supported_divisions,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ncaa"
D2_MENS_FIXTURE = FIXTURE_DIR / "wikipedia_d2_mens.html"


# ---------------------------------------------------------------------------
# supported_divisions
# ---------------------------------------------------------------------------


def test_supported_divisions_is_d1_d2():
    assert set(supported_divisions()) == {"D1", "D2"}


# ---------------------------------------------------------------------------
# _article_title_from_href
# ---------------------------------------------------------------------------


class TestArticleTitleFromHref:
    @pytest.mark.parametrize(
        "href,expected",
        [
            ("/wiki/Adelphi_Panthers", "Adelphi Panthers"),
            ("/wiki/Stanford_Cardinal_men%27s_soccer", "Stanford Cardinal men's soccer"),
            ("/wiki/Cal_State_LA_Golden_Eagles", "Cal State LA Golden Eagles"),
        ],
    )
    def test_internal_wiki_href(self, href, expected):
        assert _article_title_from_href(href) == expected

    @pytest.mark.parametrize(
        "href",
        [
            "https://example.com/foo",  # external
            "/wiki/File:Logo.png",  # file namespace
            "/wiki/Category:Soccer",  # category namespace
            "/wiki/Special:Random",  # special namespace
            "/w/index.php?title=Foo&action=edit&redlink=1",  # redlink
            "",  # empty
        ],
    )
    def test_rejects_non_article(self, href):
        assert _article_title_from_href(href) is None


# ---------------------------------------------------------------------------
# parse_program_articles
# ---------------------------------------------------------------------------


class TestParseProgramArticles:
    def test_parses_d2_fixture_pairs(self):
        html = D2_MENS_FIXTURE.read_text(encoding="utf-8")
        refs = parse_program_articles(html, "D2", "mens")

        # Fixture has 4 unique programs (Adelphi, Barry, Cal State LA,
        # Saint Leo), one duplicate Adelphi row, and a "Total" footer.
        # Parser must dedup → 4 refs, all with article titles.
        assert len(refs) == 4
        names = sorted(r.school_name for r in refs)
        assert names == [
            "Adelphi University",
            "Barry University",
            "Cal State LA",
            "Saint Leo University",
        ]
        adelphi = next(r for r in refs if r.school_name == "Adelphi University")
        assert adelphi.article_title == "Adelphi Panthers"

    def test_rejects_unsupported_division(self):
        with pytest.raises(ValueError, match="unsupported division"):
            parse_program_articles("<html></html>", "D3", "mens")

    def test_rejects_invalid_gender(self):
        with pytest.raises(ValueError, match="gender must be"):
            parse_program_articles("<html></html>", "D1", "coed")

    def test_skips_rows_without_anchor(self):
        # Plain text "Total" cell has no anchor → no ref. Confirms the
        # parser doesn't synthesize refs from text-only cells.
        html = """
        <html><body>
          <table class="wikitable">
            <tr><th>Institution</th><th>State</th></tr>
            <tr><td>Plain Text School</td><td>NY</td></tr>
            <tr><td>Total</td><td></td></tr>
          </table>
        </body></html>
        """
        refs = parse_program_articles(html, "D1", "mens")
        assert refs == []

    def test_skips_external_link_rows(self):
        html = """
        <html><body>
          <table class="wikitable">
            <tr><th>Institution</th></tr>
            <tr><td><a href="https://example.com/external">Outside Link U</a></td></tr>
            <tr><td><a href="/wiki/Real_Wiki_Article">Real School</a></td></tr>
          </table>
        </body></html>
        """
        refs = parse_program_articles(html, "D1", "mens")
        assert len(refs) == 1
        assert refs[0].school_name == "Real School"
        assert refs[0].article_title == "Real Wiki Article"


# ---------------------------------------------------------------------------
# extract_website_from_wikitext
# ---------------------------------------------------------------------------


class TestExtractWebsiteFromWikitext:
    @pytest.mark.parametrize(
        "wikitext,expected",
        [
            (
                "{{Infobox college soccer team\n"
                "| name = Stanford Cardinal\n"
                "| website = https://gostanford.com/sports/m-soccer\n"
                "}}",
                "https://gostanford.com/sports/m-soccer",
            ),
            (
                "{{Infobox\n| website = {{URL|gostanford.com/sports/m-soccer}}\n}}",
                "https://gostanford.com/sports/m-soccer",
            ),
            (
                "{{Infobox\n| website = [https://godeacs.com/sports/wsoc Wake Forest soccer]\n}}",
                "https://godeacs.com/sports/wsoc",
            ),
            (
                "{{Infobox\n|URL = https://goheels.com\n}}",  # alias + spacing
                "https://goheels.com",
            ),
            (
                "{{Infobox\n| website = https://example.com <ref>cite</ref>\n}}",
                "https://example.com",
            ),
            (
                "{{Infobox\n| website = <!-- not yet known -->\n}}",
                None,
            ),
            (
                "{{Infobox\n| name = Foo\n| coach = Bar\n}}",
                None,
            ),
            ("", None),
        ],
    )
    def test_extraction(self, wikitext, expected):
        assert extract_website_from_wikitext(wikitext) == expected


# ---------------------------------------------------------------------------
# normalize_school_name
# ---------------------------------------------------------------------------


class TestNormalizeSchoolName:
    @pytest.mark.parametrize(
        "a,b",
        [
            ("Adelphi University", "Adelphi"),
            ("The University of Notre Dame", "Notre Dame"),
            ("Saint Leo University", "saint leo"),
            ("UNC-Chapel Hill", "UNC Chapel Hill"),
            ("Texas A&M", "Texas A M"),
        ],
    )
    def test_pairs_match(self, a, b):
        assert normalize_school_name(a) == normalize_school_name(b)

    def test_distinct_schools_dont_collide(self):
        # Sanity: removing "University" mustn't fold "Boston University"
        # into "Boston College" (different schools).
        assert normalize_school_name("Boston University") != normalize_school_name(
            "Boston College"
        )


# ---------------------------------------------------------------------------
# fetch_program_websites (mocked MediaWiki API)
# ---------------------------------------------------------------------------


def _api_response(pages: list[dict], redirects: list[dict] | None = None) -> mock.Mock:
    """Build a fake MediaWiki API JSON response for testing."""
    resp = mock.Mock()
    resp.status_code = 200
    resp.json.return_value = {
        "query": {
            "pages": {str(i): page for i, page in enumerate(pages)},
            **({"redirects": redirects} if redirects else {}),
        }
    }
    resp.raise_for_status = mock.Mock()
    return resp


def _make_revision(wikitext: str) -> dict:
    return {"slots": {"main": {"*": wikitext}}}


class TestFetchProgramWebsites:
    def test_single_batch(self):
        session = mock.Mock()
        session.get.return_value = _api_response([
            {
                "title": "Adelphi Panthers",
                "revisions": [_make_revision(
                    "{{Infobox\n| website = https://aupanthers.com\n}}"
                )],
            },
            {
                "title": "Barry Buccaneers",
                "revisions": [_make_revision(
                    "{{Infobox\n| website = {{URL|barrybucs.com}}\n}}"
                )],
            },
        ])

        out = fetch_program_websites(
            ["Adelphi Panthers", "Barry Buccaneers"], session=session
        )
        assert out == {
            "Adelphi Panthers": "https://aupanthers.com",
            "Barry Buccaneers": "https://barrybucs.com",
        }
        # One API call (both titles fit in one batch)
        assert session.get.call_count == 1

    def test_multi_batch_pagination(self):
        session = mock.Mock()
        session.get.side_effect = [
            _api_response([
                {"title": "A", "revisions": [_make_revision("| website = https://a.com")]},
                {"title": "B", "revisions": [_make_revision("| website = https://b.com")]},
            ]),
            _api_response([
                {"title": "C", "revisions": [_make_revision("| website = https://c.com")]},
            ]),
        ]

        out = fetch_program_websites(["A", "B", "C"], session=session, batch_size=2)
        assert out == {
            "A": "https://a.com",
            "B": "https://b.com",
            "C": "https://c.com",
        }
        assert session.get.call_count == 2

    def test_missing_page_maps_to_none(self):
        session = mock.Mock()
        session.get.return_value = _api_response([
            {"title": "Nonexistent", "missing": ""},
        ])
        out = fetch_program_websites(["Nonexistent"], session=session)
        assert out == {"Nonexistent": None}

    def test_no_infobox_website_maps_to_none(self):
        session = mock.Mock()
        session.get.return_value = _api_response([
            {
                "title": "NoWebsite",
                "revisions": [_make_revision("{{Infobox\n| name = Foo\n}}")],
            },
        ])
        out = fetch_program_websites(["NoWebsite"], session=session)
        assert out == {"NoWebsite": None}

    def test_redirect_remap(self):
        session = mock.Mock()
        session.get.return_value = _api_response(
            pages=[
                {
                    "title": "New Title",
                    "revisions": [_make_revision("| website = https://example.com")],
                },
            ],
            redirects=[{"from": "Old Title", "to": "New Title"}],
        )
        out = fetch_program_websites(["Old Title"], session=session)
        # The caller asked for "Old Title"; we remap so the returned
        # dict uses the requested key.
        assert out == {"Old Title": "https://example.com"}

    def test_batch_failure_leaves_titles_as_none(self):
        import requests as _requests

        session = mock.Mock()
        session.get.side_effect = _requests.RequestException("boom")
        out = fetch_program_websites(["A", "B"], session=session)
        assert out == {"A": None, "B": None}

    def test_empty_input(self):
        assert fetch_program_websites([], session=mock.Mock()) == {}

    def test_dedups_repeated_titles(self):
        session = mock.Mock()
        session.get.return_value = _api_response([
            {"title": "A", "revisions": [_make_revision("| website = https://a.com")]},
        ])
        out = fetch_program_websites(["A", "A", "A"], session=session)
        assert out == {"A": "https://a.com"}
        assert session.get.call_count == 1


# ---------------------------------------------------------------------------
# discover_program_urls (end-to-end without DB)
# ---------------------------------------------------------------------------


class TestDiscoverProgramUrls:
    def test_happy_path_with_override(self):
        refs = [
            ProgramArticleRef("Adelphi University", "Adelphi Panthers"),
            ProgramArticleRef("Barry University", "Barry Buccaneers"),
        ]
        websites = {
            "Adelphi Panthers": "https://aupanthers.com",
            "Barry Buccaneers": "https://barrybucs.com",
        }

        # Mock SIDEARM probe — Adelphi resolves, Barry doesn't.
        with mock.patch(
            "extractors.ncaa_wikipedia_program_urls.resolve_soccer_program_url"
        ) as m:
            m.side_effect = lambda website, gender, **kw: (
                f"{website}/sports/mens-soccer/roster"
                if "aupanthers" in website else None
            )
            results = discover_program_urls(
                refs, "mens", websites_override=websites
            )

        assert len(results) == 2
        adelphi = next(r for r in results if r.school_name == "Adelphi University")
        assert adelphi.website == "https://aupanthers.com"
        assert adelphi.soccer_program_url == "https://aupanthers.com/sports/mens-soccer/roster"
        barry = next(r for r in results if r.school_name == "Barry University")
        assert barry.website == "https://barrybucs.com"
        assert barry.soccer_program_url is None

    def test_no_website_skips_probe(self):
        refs = [ProgramArticleRef("School", "Article")]
        with mock.patch(
            "extractors.ncaa_wikipedia_program_urls.resolve_soccer_program_url"
        ) as m:
            results = discover_program_urls(
                refs, "womens", websites_override={"Article": None}
            )
            m.assert_not_called()
        assert results[0].website is None
        assert results[0].soccer_program_url is None

    def test_invalid_gender_raises(self):
        with pytest.raises(ValueError, match="gender must be"):
            discover_program_urls(
                [], "coed", websites_override={}
            )
