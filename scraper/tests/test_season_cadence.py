from __future__ import annotations

import datetime
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scraper.lib.season_cadence import scrape_cadence, should_run_today


def test_cadence_aug_sep_is_weekly():
    assert scrape_cadence(datetime.date(2025, 8, 15)) == "weekly"
    assert scrape_cadence(datetime.date(2025, 9, 30)) == "weekly"


def test_cadence_oct_jan_is_monthly():
    assert scrape_cadence(datetime.date(2025, 10, 1)) == "monthly"
    assert scrape_cadence(datetime.date(2025, 11, 15)) == "monthly"
    assert scrape_cadence(datetime.date(2025, 12, 31)) == "monthly"
    assert scrape_cadence(datetime.date(2026, 1, 31)) == "monthly"


def test_cadence_feb_jul_is_quarterly():
    assert scrape_cadence(datetime.date(2026, 2, 1)) == "quarterly"
    assert scrape_cadence(datetime.date(2026, 5, 15)) == "quarterly"
    assert scrape_cadence(datetime.date(2026, 7, 31)) == "quarterly"


def test_should_run_today_weekly():
    today = datetime.date(2025, 9, 10)
    assert should_run_today(datetime.date(2025, 9, 2), today) is True   # 8 days ago
    assert should_run_today(datetime.date(2025, 9, 5), today) is False  # 5 days ago
    assert should_run_today(None, today) is True


def test_should_run_today_monthly():
    today = datetime.date(2025, 11, 20)
    assert should_run_today(datetime.date(2025, 10, 15), today) is True   # 36 days
    assert should_run_today(datetime.date(2025, 11, 1), today) is False   # 19 days


def test_should_run_today_quarterly():
    today = datetime.date(2026, 4, 15)
    assert should_run_today(datetime.date(2026, 1, 1), today) is True    # 104 days
    assert should_run_today(datetime.date(2026, 3, 1), today) is False   # 45 days
