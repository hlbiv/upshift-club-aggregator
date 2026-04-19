"""
Tests for the HubSpot-hosted PDF ODP extractor.

Only the parsing layer is exercised — no network. The fixture is a
real-world Cal North 2025-26 "State Pool List" PDF checked into
``scraper/tests/fixtures/odp/odp_hubspot_sample.pdf``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


pytest.importorskip("pdfplumber")

from extractors.odp_hubspot_pdf import (  # noqa: E402
    PARSER_KEYS,
    parse_html,
    parse_pdf,
)


FIXTURE_PDF = Path(__file__).parent / "fixtures" / "odp" / "odp_hubspot_sample.pdf"


def _load_pdf() -> bytes:
    return FIXTURE_PDF.read_bytes()


# ---------------------------------------------------------------------------
# Fixture sanity
# ---------------------------------------------------------------------------


def test_fixture_pdf_is_checked_in():
    assert FIXTURE_PDF.exists(), f"fixture missing: {FIXTURE_PDF}"
    b = _load_pdf()
    # A real PDF starts with the magic bytes "%PDF-".
    assert b.startswith(b"%PDF-"), "fixture is not a valid PDF"
    # The real Cal North PDF is ~515KB — sanity check the fixture
    # wasn't accidentally committed as an HTML error page.
    assert len(b) > 10_000, f"fixture suspiciously small: {len(b)} bytes"


# ---------------------------------------------------------------------------
# parse_pdf — row shape + content
# ---------------------------------------------------------------------------


def test_parse_pdf_extracts_many_rows():
    """The Cal North fixture covers 12 pools with hundreds of players;
    the parser must surface far more than the 10-row minimum."""
    rows = parse_pdf(
        _load_pdf(),
        source_url="https://www.calnorth.org/hubfs/fixture.pdf",
        state="CA-N",
        program_year="2025-26",
    )
    assert len(rows) >= 10, f"expected >=10 rows, got {len(rows)}"
    # It's actually 500+; assert a tighter bound so a silent regression
    # to "only parses the first page" would fail CI.
    assert len(rows) >= 300, f"expected >=300 rows from the full PDF, got {len(rows)}"


def test_parse_pdf_every_row_has_required_fields():
    rows = parse_pdf(
        _load_pdf(),
        source_url="https://www.calnorth.org/hubfs/fixture.pdf",
        state="CA-N",
        program_year="2025-26",
    )
    assert rows, "parser returned no rows"

    for r in rows:
        # Required by the ODP writer.
        assert r["player_name"], "player_name must be populated"
        assert r["state"] == "CA-N"
        assert r["program_year"] == "2025-26"
        assert r["source_url"] == "https://www.calnorth.org/hubfs/fixture.pdf"
        # PDF-derived fields.
        assert r["gender"] in ("B", "G"), f"unexpected gender: {r['gender']!r}"
        assert isinstance(r["age_group"], str) and r["age_group"].startswith("U")
        assert isinstance(r["birth_year"], int)
        assert 2005 <= r["birth_year"] <= 2020
        # Optional fields exist on the dict even when None, so the
        # writer can rely on the shape.
        for optional in ("graduation_year", "position", "club_name_raw"):
            assert optional in r


def test_parse_pdf_decodes_pool_codes_into_age_group_and_gender():
    rows = parse_pdf(
        _load_pdf(),
        source_url="https://www.calnorth.org/hubfs/fixture.pdf",
        state="CA-N",
        program_year="2025-26",
    )
    # The Cal North PDF advertises pools 09B-14B and 09G-14G. Every
    # expected birth-year string must appear in the output.
    birth_years = {r["birth_year"] for r in rows}
    expected_years = set(range(2009, 2015))
    assert expected_years.issubset(birth_years), (
        f"missing birth years: {expected_years - birth_years}"
    )
    genders = {r["gender"] for r in rows}
    assert genders == {"B", "G"}, f"expected both genders, got {genders}"


def test_parse_pdf_rejects_header_and_prose_rows():
    """Column-header leakage ("Last Name", "First Name") or prose from
    the cover letter must never make it into the output."""
    rows = parse_pdf(
        _load_pdf(),
        source_url="https://www.calnorth.org/hubfs/fixture.pdf",
        state="CA-N",
        program_year="2025-26",
    )
    for r in rows:
        name = r["player_name"].lower()
        assert "last name" not in name
        assert "first name" not in name
        # Cover-letter words that would indicate prose leaked through.
        assert "thank you" not in name
        assert "september" not in name


def test_parse_pdf_on_empty_bytes_returns_empty():
    assert parse_pdf(b"", source_url="x", state="CA-N", program_year="2025-26") == []


def test_parse_pdf_on_garbage_bytes_returns_empty_not_raise():
    # Should log a warning and return [], not raise.
    assert parse_pdf(b"not a pdf", source_url="x", state="CA-N", program_year="2025-26") == []


# ---------------------------------------------------------------------------
# parse_html shim
# ---------------------------------------------------------------------------


def test_parse_html_accepts_pdf_bytes_directly():
    rows = parse_html(
        _load_pdf(),
        source_url="x",
        state="CA-N",
        program_year="2025-26",
    )
    assert len(rows) >= 10


def test_parse_html_accepts_latin1_str_roundtrip():
    # Some replay harnesses stash bodies as latin-1 strings; verify
    # the str path round-trips losslessly back to the same rows.
    pdf_bytes = _load_pdf()
    as_str = pdf_bytes.decode("latin-1")
    rows = parse_html(
        as_str,
        source_url="x",
        state="CA-N",
        program_year="2025-26",
    )
    assert len(rows) >= 10


def test_parse_html_rejects_unexpected_type():
    # Non-bytes, non-str input must not raise — just returns [].
    assert parse_html(
        12345, source_url="x", state="CA-N", program_year="2025-26",
    ) == []


# ---------------------------------------------------------------------------
# Parser-key registry
# ---------------------------------------------------------------------------


def test_calnorth_parser_key_is_registered():
    assert "calnorth" in PARSER_KEYS
