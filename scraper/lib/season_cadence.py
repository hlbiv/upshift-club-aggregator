"""Season-aware cadence helper for NCAA roster scrapes.

Cadence by month:
  Aug, Sep  → weekly   (season kick-off, rosters change frequently)
  Oct–Jan   → monthly  (mid-season, changes stabilise)
  Feb–Jul   → quarterly (off-season, minimal movement)
"""
from __future__ import annotations

import datetime

MONTHLY_CADENCE: dict[int, str] = {
    8: "weekly",  9: "weekly",
    10: "monthly", 11: "monthly", 12: "monthly", 1: "monthly",
}


def scrape_cadence(today: datetime.date | None = None) -> str:
    """Return 'weekly' | 'monthly' | 'quarterly' based on current month."""
    m = (today or datetime.date.today()).month
    return MONTHLY_CADENCE.get(m, "quarterly")


def should_run_today(
    last_run_date: datetime.date | None,
    today: datetime.date | None = None,
) -> bool:
    """True if enough time has passed since last run, given current cadence."""
    today = today or datetime.date.today()
    cadence = scrape_cadence(today)
    if last_run_date is None:
        return True
    days_since = (today - last_run_date).days
    if cadence == "weekly":
        return days_since >= 7
    if cadence == "monthly":
        return days_since >= 30
    return days_since >= 90  # quarterly
