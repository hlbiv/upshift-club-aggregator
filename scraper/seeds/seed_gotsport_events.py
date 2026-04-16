"""
Seed script — bootstrap the ``events`` table with GotSport event rows
extracted from ``leagues_master.csv`` notes.

Usage:
    python -m scraper.seeds.seed_gotsport_events
    python -m scraper.seeds.seed_gotsport_events --dry-run

Each row gets ``source='gotsport'`` and ``platform_event_id=<event_id>``.
ON CONFLICT (source, platform_event_id) DO NOTHING — safe to re-run.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    import psycopg2  # type: ignore
except ImportError:
    psycopg2 = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("seed_gotsport_events")

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
_GOTSPORT_EVENT_RE = re.compile(r"GotSport\s+events?\s+(\d+)", re.IGNORECASE)

_INSERT_SQL = """
    INSERT INTO events (
        name, slug, source, platform_event_id, source_url, league_name,
        last_scraped_at
    )
    VALUES (%s, %s, 'gotsport', %s, %s, %s, NULL)
    ON CONFLICT ON CONSTRAINT events_source_platform_id_uq DO NOTHING
"""


def _extract_seeds() -> list[dict]:
    """Extract GotSport event IDs + league names from CSV."""
    csv_path = os.path.join(_DATA_DIR, "leagues_master.csv")
    if not os.path.exists(csv_path):
        logger.error("leagues_master.csv not found at %s", csv_path)
        return []

    seeds = []
    seen: set = set()

    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            notes = row.get("notes", "")
            league_name = row.get("league_name", "").strip()
            if not notes:
                continue

            for m in _GOTSPORT_EVENT_RE.finditer(notes):
                eid = m.group(1)
                if eid not in seen:
                    seen.add(eid)
                    seeds.append({
                        "event_id": eid,
                        "league_name": league_name,
                    })

            # Catch additional bare IDs in GotSport-context notes.
            if "gotsport" in notes.lower():
                for extra_m in re.finditer(r"\b(\d{4,6})\b", notes):
                    eid = extra_m.group(1)
                    if eid in seen:
                        continue
                    start = extra_m.start()
                    end = extra_m.end()
                    if start > 0 and notes[start - 1] in ("-", "/"):
                        continue
                    if end < len(notes) and notes[end] in ("-", "/"):
                        continue
                    seen.add(eid)
                    seeds.append({
                        "event_id": eid,
                        "league_name": league_name,
                    })

    return seeds


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed events table with GotSport event IDs from CSV")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be inserted without touching DB")
    args = parser.parse_args()

    seeds = _extract_seeds()
    if not seeds:
        logger.warning("No GotSport event IDs found in leagues_master.csv")
        return

    logger.info("Found %d GotSport event IDs in CSV", len(seeds))

    if args.dry_run:
        for s in seeds:
            logger.info(
                "[dry-run] Would insert event_id=%s league=%s",
                s["event_id"], s["league_name"],
            )
        return

    if psycopg2 is None:
        logger.error("psycopg2 not installed — cannot write to DB")
        return

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        logger.error("DATABASE_URL not set")
        return

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    inserted = 0

    try:
        with conn.cursor() as cur:
            for s in seeds:
                eid = s["event_id"]
                league = s["league_name"]
                slug = f"gotsport-{eid}"
                name = f"{league} (GotSport {eid})" if league else f"GotSport Event {eid}"
                source_url = f"https://system.gotsport.com/org_event/events/{eid}/teams?showall=clean"

                cur.execute(_INSERT_SQL, (name, slug, str(eid), source_url, league or None))
                if cur.rowcount > 0:
                    inserted += 1

        conn.commit()
        logger.info("Seeded %d new event rows (%d already existed)", inserted, len(seeds) - inserted)
    except Exception as exc:
        conn.rollback()
        logger.error("Seed failed: %s", exc)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
