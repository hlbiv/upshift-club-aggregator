"""
Reconcilers — derived-state post-processing that runs after scrapers.

Currently:
  - scrape_health.reconcile_scrape_health — writes `scrape_health` rows from
    entity tables' freshness timestamps.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

from .scrape_health import reconcile_scrape_health

log = logging.getLogger("reconcilers")


def end_of_run_reconcile(conn=None) -> Optional[dict]:
    """Run all end-of-run reconcilers. Never raises.

    Opens (and closes) its own DB connection if ``conn`` is None.
    Returns the reconciler summary dict, or None if the DB is unavailable
    or the reconciler itself errored (errors are logged, not raised).
    """
    if psycopg2 is None:
        log.info("reconcilers: psycopg2 unavailable — skipping")
        return None

    owns_conn = False
    if conn is None:
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            log.info("reconcilers: DATABASE_URL unset — skipping")
            return None
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = True
            owns_conn = True
        except Exception as exc:
            log.warning("reconcilers: connect failed — %s", exc)
            return None

    try:
        return reconcile_scrape_health(conn)
    except Exception as exc:
        log.warning("reconcilers: scrape_health reconciler failed — %s", exc)
        return None
    finally:
        if owns_conn:
            try:
                conn.close()
            except Exception:
                pass


__all__ = ["reconcile_scrape_health", "end_of_run_reconcile"]
