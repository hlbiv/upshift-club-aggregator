"""
tryouts_status_updater.py — Expire past-date tryouts.

Transitions tryouts whose ``tryout_date`` is in the past from
``status IN ('upcoming', 'active')`` to ``status = 'expired'``.

Can be invoked standalone for ad-hoc cleanup:

    python scraper/tryouts_status_updater.py [--dry-run]

Or imported and called from ``tryouts_runner.run_tryouts()``.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.dirname(__file__))

logger = logging.getLogger("tryouts_status_updater")

_EXPIRE_SQL = """
UPDATE tryouts
   SET status = 'expired',
       scraped_at = now()
 WHERE tryout_date < CURRENT_DATE
   AND status IN ('upcoming', 'active')
"""

_COUNT_SQL = """
SELECT count(*)
  FROM tryouts
 WHERE tryout_date < CURRENT_DATE
   AND status IN ('upcoming', 'active')
"""


def _get_default_conn():
    """Get a psycopg2 connection using DATABASE_URL."""
    import psycopg2  # noqa: E402
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(db_url)


def expire_past_tryouts(
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Expire past-date tryouts.

    Parameters
    ----------
    conn : optional
        A psycopg2-compatible connection. If None, opens one from
        ``DATABASE_URL``.
    dry_run : bool
        If True, counts candidates without updating.

    Returns
    -------
    dict
        ``{"expired": N}`` — number of rows expired (or that would be).
    """
    own_conn = conn is None
    if own_conn:
        conn = _get_default_conn()

    try:
        cur = conn.cursor()
        if dry_run:
            cur.execute(_COUNT_SQL)
            row = cur.fetchone()
            count = row[0] if row else 0
            logger.info("[tryouts-status] dry-run: %d tryout(s) would be expired", count)
            cur.close()
            return {"expired": count}

        cur.execute(_EXPIRE_SQL)
        count = cur.rowcount
        conn.commit()
        logger.info("[tryouts-status] expired %d past-date tryout(s)", count)
        cur.close()
        return {"expired": count}
    finally:
        if own_conn and conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser(description="Expire past-date tryouts")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    result = expire_past_tryouts(dry_run=args.dry_run)
    print(f"Expired: {result['expired']}")
