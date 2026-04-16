"""
Tests for scraper.reconcilers.scrape_health.

These tests require a live Postgres (DATABASE_URL) because the reconciler
is SQL-heavy — the critical behaviour is the UPSERT + partial UPDATE
interacting with the `scrape_health` check constraints and unique index.
Stubbing psycopg2 would only test the Python shape.

Run:
    DATABASE_URL=postgres://... python -m pytest \\
        scraper/tests/test_scrape_health_reconciler.py -v

The tests skip cleanly when DATABASE_URL is unset so CI without a
Postgres still passes (mirroring test_run_py_failure_kind_matches_logger_and_db_enum).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore


pytestmark = pytest.mark.skipif(
    psycopg2 is None or not os.environ.get("DATABASE_URL"),
    reason="DATABASE_URL + psycopg2 required for reconciler SQL tests",
)


@pytest.fixture
def conn():
    c = psycopg2.connect(os.environ["DATABASE_URL"])
    c.autocommit = False
    try:
        yield c
    finally:
        try:
            c.rollback()
        except Exception:
            pass
        c.close()


def _ensure_schema(cur) -> None:
    """Skip the whole test if the required schema isn't present."""
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name='scrape_health'"
    )
    if cur.fetchone() is None:
        pytest.skip("scrape_health table not present — run drizzle push first")


def _cleanup(cur, label: str) -> None:
    """Remove any test rows from a prior failed run keyed on `club_slug`."""
    cur.execute(
        "DELETE FROM scrape_health WHERE entity_type='club' AND entity_id IN "
        "(SELECT id FROM canonical_clubs WHERE club_slug LIKE %s)",
        (f"{label}-%",),
    )
    cur.execute(
        "DELETE FROM canonical_clubs WHERE club_slug LIKE %s",
        (f"{label}-%",),
    )


def test_reconcile_marks_recent_rows_ok(conn):
    from reconcilers.scrape_health import reconcile_scrape_health

    with conn.cursor() as cur:
        _ensure_schema(cur)
        _cleanup(cur, "reconciler-test-recent")

        now = datetime.utcnow()
        cur.execute(
            """
            INSERT INTO canonical_clubs
                (club_name_canonical, club_slug, last_scraped_at)
            VALUES (%s, %s, %s), (%s, %s, %s)
            RETURNING id
            """,
            (
                "Reconciler Test A", "reconciler-test-recent-a", now,
                "Reconciler Test B", "reconciler-test-recent-b", now - timedelta(hours=1),
            ),
        )
        ids = [row[0] for row in cur.fetchall()]
        conn.commit()

        summary = reconcile_scrape_health(conn, window_hours=168)

        assert summary["club"]["refreshed"] >= 2
        cur.execute(
            "SELECT status, consecutive_failures, last_error "
            "FROM scrape_health WHERE entity_type='club' AND entity_id = ANY(%s)",
            (ids,),
        )
        rows = cur.fetchall()
        assert len(rows) == 2
        for status, fails, err in rows:
            assert status == "ok"
            assert fails == 0
            assert err is None

        _cleanup(cur, "reconciler-test-recent")
        conn.commit()


def test_reconcile_demotes_stale(conn):
    from reconcilers.scrape_health import reconcile_scrape_health

    with conn.cursor() as cur:
        _ensure_schema(cur)
        _cleanup(cur, "reconciler-test-stale")

        stale_ts = datetime.utcnow() - timedelta(days=30)
        cur.execute(
            """
            INSERT INTO canonical_clubs
                (club_name_canonical, club_slug, last_scraped_at)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            ("Reconciler Stale Club", "reconciler-test-stale-a", stale_ts),
        )
        club_id = cur.fetchone()[0]

        # Seed a `status='ok'` row already, so the demote branch has
        # something to act on (the upsert won't re-create it because the
        # source row is outside the window).
        cur.execute(
            """
            INSERT INTO scrape_health
                (entity_type, entity_id, last_scraped_at, last_success_at, status)
            VALUES ('club', %s, %s, %s, 'ok')
            """,
            (club_id, stale_ts, stale_ts),
        )
        conn.commit()

        reconcile_scrape_health(conn, window_hours=168)

        cur.execute(
            "SELECT status FROM scrape_health WHERE entity_type='club' AND entity_id=%s",
            (club_id,),
        )
        (status,) = cur.fetchone()
        assert status == "stale"

        _cleanup(cur, "reconciler-test-stale")
        conn.commit()


def test_reconcile_skips_missing_tables(conn, monkeypatch):
    """
    If a configured entity's source table does not exist (e.g. `colleges`
    not yet deployed), reconciler logs and continues without raising.
    We simulate by temporarily adding a bogus entity to _ENTITY_CONFIG.
    """
    from reconcilers import scrape_health as rh

    bogus = ("__test_missing__", "__definitely_not_a_table__", ("updated_at",))
    monkeypatch.setattr(
        rh, "_ENTITY_CONFIG", rh._ENTITY_CONFIG + [bogus], raising=True,
    )

    summary = rh.reconcile_scrape_health(conn, window_hours=168)
    assert "__test_missing__" in summary
    assert "skipped" in summary["__test_missing__"]


def test_reconcile_skips_tables_without_freshness_column(conn):
    """
    `leagues_master` has no freshness column configured and should be
    reported as skipped — not an error, just reference data.
    """
    from reconcilers.scrape_health import reconcile_scrape_health

    summary = reconcile_scrape_health(conn, window_hours=168)
    assert "league" in summary
    assert summary["league"] == {"skipped": "no freshness column"}
