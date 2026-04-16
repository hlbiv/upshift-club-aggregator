"""
club_enrichment_writer.py — UPDATE ``canonical_clubs`` with enrichment data
(logo, socials, website status, staff page URL, scrape confidence).

Only overwrites existing values when the new scrape_confidence >= the
existing confidence. This prevents a low-quality re-scrape from clobbering
high-quality data from a previous run.

psycopg2 is imported lazily so this module stays importable without
DATABASE_URL (tests that only exercise extraction skip DB altogether).
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional, Sequence

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

log = logging.getLogger("club_enrichment_writer")


# UPDATE with confidence guard: only overwrite non-NULL fields when
# new confidence >= existing confidence (or existing is NULL).
_UPDATE_ENRICHMENT_SQL = """
UPDATE canonical_clubs SET
    logo_url = CASE
        WHEN %(logo_url)s IS NOT NULL
         AND (scrape_confidence IS NULL OR %(scrape_confidence)s >= scrape_confidence)
        THEN %(logo_url)s
        ELSE COALESCE(logo_url, %(logo_url)s)
    END,
    instagram = CASE
        WHEN %(instagram)s IS NOT NULL
         AND (scrape_confidence IS NULL OR %(scrape_confidence)s >= scrape_confidence)
        THEN %(instagram)s
        ELSE COALESCE(instagram, %(instagram)s)
    END,
    facebook = CASE
        WHEN %(facebook)s IS NOT NULL
         AND (scrape_confidence IS NULL OR %(scrape_confidence)s >= scrape_confidence)
        THEN %(facebook)s
        ELSE COALESCE(facebook, %(facebook)s)
    END,
    twitter = CASE
        WHEN %(twitter)s IS NOT NULL
         AND (scrape_confidence IS NULL OR %(scrape_confidence)s >= scrape_confidence)
        THEN %(twitter)s
        ELSE COALESCE(twitter, %(twitter)s)
    END,
    staff_page_url = CASE
        WHEN %(staff_page_url)s IS NOT NULL
         AND (scrape_confidence IS NULL OR %(scrape_confidence)s >= scrape_confidence)
        THEN %(staff_page_url)s
        ELSE COALESCE(staff_page_url, %(staff_page_url)s)
    END,
    website_status = CASE
        WHEN %(website_status)s IS NOT NULL
         AND (scrape_confidence IS NULL OR %(scrape_confidence)s >= scrape_confidence)
        THEN %(website_status)s
        ELSE COALESCE(website_status, %(website_status)s)
    END,
    scrape_confidence = CASE
        WHEN %(scrape_confidence)s IS NOT NULL
         AND (scrape_confidence IS NULL OR %(scrape_confidence)s >= scrape_confidence)
        THEN %(scrape_confidence)s
        ELSE scrape_confidence
    END,
    website_last_checked_at = now(),
    last_scraped_at = now()
WHERE id = %(club_id)s
RETURNING id
"""


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure every expected key exists, with None for optional fields."""
    club_id = row.get("club_id")
    if not club_id:
        raise ValueError("enrichment row missing club_id")
    return {
        "club_id": club_id,
        "logo_url": row.get("logo_url") or None,
        "instagram": row.get("instagram") or None,
        "facebook": row.get("facebook") or None,
        "twitter": row.get("twitter") or None,
        "staff_page_url": row.get("staff_page_url") or None,
        "website_status": row.get("website_status") or None,
        "scrape_confidence": row.get("scrape_confidence"),
    }


def update_club_enrichment(
    rows: Sequence[Dict[str, Any]],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Update canonical_clubs with enrichment data.

    Parameters
    ----------
    rows
        Iterable of row dicts with at least ``club_id`` and any enrichment
        fields (logo_url, instagram, facebook, twitter, staff_page_url,
        website_status, scrape_confidence).
    conn
        Optional existing psycopg2 connection. If omitted, a new one is
        opened, used, and closed.
    dry_run
        When True, no DB I/O occurs; returns zero counts.

    Returns
    -------
    ``{"updated": N, "skipped": N}``.
    """
    counts = {"updated": 0, "skipped": 0}
    if not rows:
        return counts
    if dry_run:
        log.info("[club-enrichment-writer] dry-run: would update %d rows", len(rows))
        return counts

    normalized = []
    for raw in rows:
        try:
            normalized.append(_normalize_row(raw))
        except ValueError as exc:
            log.warning("[club-enrichment-writer] skipping bad row: %s", exc)
            counts["skipped"] += 1
    if not normalized:
        return counts

    own_conn = conn is None
    if own_conn:
        conn = _get_connection()

    try:
        with conn.cursor() as cur:
            for row in normalized:
                try:
                    cur.execute(_UPDATE_ENRICHMENT_SQL, row)
                    result = cur.fetchone()
                except Exception as exc:
                    log.warning(
                        "[club-enrichment-writer] update failed for club_id=%s: %s",
                        row.get("club_id"), exc,
                    )
                    counts["skipped"] += 1
                    conn.rollback()
                    continue
                if result is not None:
                    counts["updated"] += 1
                else:
                    log.warning(
                        "[club-enrichment-writer] club_id=%s not found",
                        row.get("club_id"),
                    )
                    counts["skipped"] += 1

        if own_conn:
            conn.commit()
    finally:
        if own_conn and conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    log.info(json.dumps({
        "event": "club-enrichment-writer",
        "updated": counts["updated"],
        "skipped": counts["skipped"],
    }))
    return counts
