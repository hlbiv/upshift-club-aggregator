"""
Tests for the NJCAA roster scraper stub.

The NJCAA scraper is intentionally a stub — these tests verify the
scaffolding runs without error and returns zero rows.
"""

from __future__ import annotations

import os
import sys
from unittest import mock

import pytest

# Ensure scraper package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.njcaa_rosters import (  # noqa: E402
    scrape_njcaa_rosters,
    SCRAPER_KEY,
)


class TestStubReturnsEmpty:
    """Verify the stub runs without error and returns 0 rows."""

    @mock.patch("extractors.njcaa_rosters.psycopg2", None)
    @mock.patch("extractors.ncaa_soccer_rosters.psycopg2", None)
    def test_stub_returns_zero_without_db(self):
        """No DB connection — stub still completes cleanly."""
        result = scrape_njcaa_rosters(dry_run=True)
        assert result["scraped"] == 0
        assert result["rows_inserted"] == 0
        assert result["rows_updated"] == 0
        assert result["errors"] == 0

    @mock.patch("extractors.njcaa_rosters._get_connection")
    def test_stub_returns_zero_with_db(self, mock_get_conn):
        """With a DB that returns colleges, stub still produces 0 rows."""
        mock_conn = mock.MagicMock()
        mock_get_conn.return_value = mock_conn

        # Simulate a cursor that returns 2 NJCAA colleges
        mock_cursor = mock.MagicMock()
        mock_cursor.description = [
            ("id",), ("name",), ("slug",), ("division",), ("conference",),
            ("state",), ("city",), ("website",), ("soccer_program_url",),
            ("gender_program",), ("last_scraped_at",),
        ]
        mock_cursor.fetchall.return_value = [
            (1, "Tyler Junior College", "tyler-jc-njcaa-m", "NJCAA", "Region XIV",
             "TX", "Tyler", "https://apacheathletics.com", None, "mens", None),
            (2, "Monroe College", "monroe-njcaa-w", "NJCAA", "Region XV",
             "NY", "New Rochelle", "https://monroemustangs.com", None, "womens", None),
        ]
        mock_conn.cursor.return_value.__enter__ = mock.MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = mock.MagicMock(return_value=False)

        result = scrape_njcaa_rosters()
        assert result["scraped"] == 0
        assert result["rows_inserted"] == 0
        assert result["rows_updated"] == 0
        assert result["errors"] == 0

    def test_scraper_key(self):
        assert SCRAPER_KEY == "njcaa-rosters"


class TestDryRun:
    """Verify dry_run flag is accepted and produces clean output."""

    @mock.patch("extractors.njcaa_rosters.psycopg2", None)
    @mock.patch("extractors.ncaa_soccer_rosters.psycopg2", None)
    def test_dry_run_flag(self):
        result = scrape_njcaa_rosters(dry_run=True)
        assert isinstance(result, dict)
        assert result["scraped"] == 0
        assert result["errors"] == 0

    @mock.patch("extractors.njcaa_rosters.psycopg2", None)
    @mock.patch("extractors.ncaa_soccer_rosters.psycopg2", None)
    def test_dry_run_with_gender_filter(self):
        result = scrape_njcaa_rosters(gender="womens", dry_run=True)
        assert result["scraped"] == 0

    @mock.patch("extractors.njcaa_rosters.psycopg2", None)
    @mock.patch("extractors.ncaa_soccer_rosters.psycopg2", None)
    def test_dry_run_with_limit(self):
        result = scrape_njcaa_rosters(limit=5, dry_run=True)
        assert result["scraped"] == 0
