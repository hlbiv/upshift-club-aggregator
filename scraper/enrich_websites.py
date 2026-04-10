"""
enrich_websites.py — Search-based website discovery for clubs with no website.

Uses the Brave Search API to find official club websites for clubs that are
missing one in canonical_clubs (or master.csv when --csv mode is used).

Usage:
    python enrich_websites.py [--dry-run] [--limit N] [--tier N] [--csv]

Flags:
    --dry-run    Show which clubs would be queried; no API calls or DB writes.
    --limit N    Process at most N clubs (useful for testing).
    --tier N     Only process clubs affiliated with leagues of this tier (1-4).
    --csv        Read/write from master.csv instead of the PostgreSQL database.

Requirements:
    - BRAVE_API_KEY environment variable (Replit secret)
    - DATABASE_URL environment variable (for DB mode)

Rate limits:
    - Max 1 request per second (Brave Search free tier)
    - Stops and checkpoints on repeated API errors

Output:
    - Updates canonical_clubs.website and website_discovered_at in DB
    - Writes output/website_enrichment_progress.json after every 50 clubs
    - Prints a summary report on completion

website_status is set to "search" to distinguish from directory-sourced links.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BRAVE_API_URL = "https://api.search.brave.com/res/v1/web/search"

CHECKPOINT_PATH = os.path.join(os.path.dirname(__file__), "output", "website_enrichment_progress.json")

CHECKPOINT_BATCH_SIZE = 50

RATE_LIMIT_SLEEP = 1.1

MAX_RETRIES = 3

EXCLUDED_DOMAINS = {
    "gotsport.com",
    "google.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "girlsacademyleague.com",
    "theecnl.com",
    "mlsnextsoccer.com",
    "ussoccer.com",
    "usyouthsoccer.org",
    "usys.net",
    "wikipedia.org",
    "yelp.com",
    "linkedin.com",
    "tiktok.com",
    "sportengine.com",
    "arbiter-sport.com",
    "arbitersports.com",
    "teamsnap.com",
    "rampinteractive.com",
    "demosphere.com",
    "blustarapp.com",
    "sportsengine.com",
    "reddit.com",
    "amazon.com",
    "snapchat.com",
    "eventbrite.com",
}

ACCEPTED_TLDS = {".com", ".org", ".net"}

# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------


def _load_checkpoint() -> Dict:
    """Load progress checkpoint from disk. Returns empty dict if not found."""
    if os.path.exists(CHECKPOINT_PATH):
        try:
            with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            logger.warning("Could not read checkpoint: %s", exc)
    return {"processed": {}, "found": 0, "queried": 0, "errors": 0}


def _save_checkpoint(checkpoint: Dict) -> None:
    """Write checkpoint to disk."""
    os.makedirs(os.path.dirname(CHECKPOINT_PATH), exist_ok=True)
    try:
        with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, indent=2)
    except Exception as exc:
        logger.warning("Could not write checkpoint: %s", exc)


# ---------------------------------------------------------------------------
# Domain / URL filtering
# ---------------------------------------------------------------------------


def _get_apex_domain(url: str) -> str:
    """Extract the apex domain from a URL (e.g. 'foo.bar.com' -> 'bar.com')."""
    try:
        host = urlparse(url).hostname or ""
        parts = host.lower().split(".")
        if len(parts) >= 2:
            return ".".join(parts[-2:])
        return host
    except Exception:
        return ""


def _is_excluded_domain(url: str) -> bool:
    """Return True if this URL belongs to an excluded (non-club) domain."""
    apex = _get_apex_domain(url)
    if not apex:
        return True
    if apex in EXCLUDED_DOMAINS:
        return True
    for excl in EXCLUDED_DOMAINS:
        if apex.endswith("." + excl) or apex == excl:
            return True
    return False


def _has_accepted_tld(url: str) -> bool:
    """Return True if the URL ends with an accepted TLD."""
    try:
        host = urlparse(url).hostname or ""
        for tld in ACCEPTED_TLDS:
            if host.endswith(tld):
                return True
    except Exception:
        pass
    return False


def _score_result(url: str, title: str, club_name: str, city: str, state: str) -> int:
    """
    Score a search result 0-100. Higher is better.
    Prefers results where the club name or city appears in the URL or title.
    """
    score = 0

    url_lower = url.lower()
    title_lower = title.lower() if title else ""

    name_tokens = [t.lower() for t in re.split(r"\W+", club_name) if len(t) > 2]
    city_tokens = [t.lower() for t in re.split(r"\W+", city) if len(t) > 2] if city else []
    state_token = state.lower().strip() if state else ""

    matched_name_tokens = sum(1 for tok in name_tokens if tok in url_lower or tok in title_lower)
    if name_tokens:
        score += int(50 * matched_name_tokens / len(name_tokens))

    if city_tokens:
        matched_city = sum(1 for tok in city_tokens if tok in url_lower or tok in title_lower)
        score += int(20 * matched_city / len(city_tokens))

    if state_token and (state_token in url_lower or state_token in title_lower):
        score += 10

    path = urlparse(url).path.lower()
    if path in ("", "/", "/home", "/index.html"):
        score += 15

    if "soccer" in url_lower or "fc" in url_lower or "sc" in url_lower:
        score += 5

    return min(score, 100)


# ---------------------------------------------------------------------------
# Brave Search API
# ---------------------------------------------------------------------------


def _search_club_website(
    club_name: str,
    city: str,
    state: str,
    api_key: str,
    retries: int = MAX_RETRIES,
) -> Optional[str]:
    """
    Query Brave Search for the official website of a soccer club.

    Returns the best matching URL or None if nothing suitable found.
    Raises RuntimeError on repeated API failures (caller should checkpoint+abort).
    """
    city_part = f'"{city}"' if city else ""
    state_part = f'"{state}"' if state else ""
    query_parts = [f'"{club_name}"']
    if city_part:
        query_parts.append(city_part)
    if state_part:
        query_parts.append(state_part)
    query_parts.append("soccer")
    query = " ".join(query_parts)

    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": api_key,
    }
    params = {
        "q": query,
        "count": 10,
        "search_lang": "en",
        "country": "US",
        "safesearch": "off",
    }

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(BRAVE_API_URL, headers=headers, params=params, timeout=15)
            if resp.status_code == 429:
                logger.warning("Rate limited by Brave Search (attempt %d/%d), sleeping 5s", attempt, retries)
                time.sleep(5)
                continue
            if resp.status_code != 200:
                logger.warning("Brave Search returned %d for query '%s'", resp.status_code, query)
                if attempt == retries:
                    raise RuntimeError(f"Brave Search API error: HTTP {resp.status_code}")
                time.sleep(2)
                continue

            data = resp.json()
            results = data.get("web", {}).get("results", [])

            candidates = []
            for r in results:
                url = r.get("url", "")
                title = r.get("title", "")
                if not url:
                    continue
                if _is_excluded_domain(url):
                    continue
                if not _has_accepted_tld(url):
                    continue
                sc = _score_result(url, title, club_name, city, state)
                candidates.append((sc, url))

            if not candidates:
                return None

            candidates.sort(key=lambda x: -x[0])
            best_score, best_url = candidates[0]
            logger.debug("Best result for '%s': score=%d url=%s", club_name, best_score, best_url)
            return best_url

        except RuntimeError:
            raise
        except Exception as exc:
            logger.warning("Brave Search exception (attempt %d/%d): %s", attempt, retries, exc)
            if attempt == retries:
                raise RuntimeError(f"Brave Search failed after {retries} attempts: {exc}")
            time.sleep(2)

    return None


# ---------------------------------------------------------------------------
# Database mode
# ---------------------------------------------------------------------------


def _get_db_connection():
    """Return a psycopg2 connection using DATABASE_URL."""
    try:
        import psycopg2
    except ImportError:
        logger.error("psycopg2 not installed. Run: pip install psycopg2-binary")
        sys.exit(1)

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL environment variable is not set.")
        sys.exit(1)

    return psycopg2.connect(db_url)


def _fetch_clubs_from_db(tier: Optional[int] = None) -> List[Dict]:
    """
    Return clubs from canonical_clubs that have no website.
    Optionally filter by league tier via club_affiliations -> leagues_master.
    """
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            if tier is not None:
                sql = """
                    SELECT DISTINCT cc.id, cc.club_name_canonical, cc.city, cc.state
                    FROM canonical_clubs cc
                    JOIN club_affiliations ca ON ca.club_id = cc.id
                    JOIN leagues_master lm ON lm.league_name = ca.source_name
                    WHERE (cc.website IS NULL OR cc.website = '')
                      AND lm.tier_numeric = %s
                    ORDER BY cc.id
                """
                cur.execute(sql, (tier,))
            else:
                sql = """
                    SELECT id, club_name_canonical, city, state
                    FROM canonical_clubs
                    WHERE (website IS NULL OR website = '')
                    ORDER BY id
                """
                cur.execute(sql)

            rows = cur.fetchall()
            return [
                {
                    "id": row[0],
                    "club_name": row[1],
                    "city": row[2] or "",
                    "state": row[3] or "",
                }
                for row in rows
            ]
    finally:
        conn.close()


def _update_club_in_db(club_id: int, website: str) -> None:
    """Update canonical_clubs.website and website_discovered_at for a club."""
    conn = _get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE canonical_clubs
                SET website = %s,
                    website_status = 'search',
                    website_discovered_at = %s
                WHERE id = %s
                """,
                (website, datetime.now(timezone.utc), club_id),
            )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CSV mode
# ---------------------------------------------------------------------------


def _fetch_clubs_from_csv(tier: Optional[int] = None) -> List[Dict]:
    """
    Read clubs with no website from master.csv.
    Optionally filter by tier via the league_name column matched against config.
    """
    master_path = os.path.join(os.path.dirname(__file__), "output", "master.csv")
    if not os.path.exists(master_path):
        logger.error("master.csv not found at: %s", master_path)
        sys.exit(1)

    df = pd.read_csv(master_path, dtype=str).fillna("")

    if "website" not in df.columns:
        df["website"] = ""

    no_website = df[df["website"].str.strip() == ""]

    if tier is not None:
        from config import LEAGUES  # noqa: F401 (relative import when run from scraper/)
        tier_league_names = {lg["name"] for lg in LEAGUES if lg.get("tier") == tier}
        if "league_name" in no_website.columns:
            no_website = no_website[no_website["league_name"].isin(tier_league_names)]

    seen_names: set = set()
    clubs = []
    for _, row in no_website.iterrows():
        name = row.get("club_name", "").strip() or row.get("canonical_name", "").strip()
        if not name or name in seen_names:
            continue
        seen_names.add(name)
        clubs.append({
            "id": None,
            "club_name": name,
            "city": row.get("city", "").strip(),
            "state": row.get("state", "").strip(),
        })

    return clubs


def _update_club_in_csv(club_name: str, website: str) -> None:
    """Update the website field in master.csv for matching club_name rows."""
    master_path = os.path.join(os.path.dirname(__file__), "output", "master.csv")
    df = pd.read_csv(master_path, dtype=str).fillna("")
    if "website" not in df.columns:
        df["website"] = ""
    if "website_status" not in df.columns:
        df["website_status"] = ""
    if "website_discovered_at" not in df.columns:
        df["website_discovered_at"] = ""

    mask = df["club_name"] == club_name
    df.loc[mask, "website"] = website
    df.loc[mask, "website_status"] = "search"
    df.loc[mask, "website_discovered_at"] = datetime.now(timezone.utc).isoformat()
    df.to_csv(master_path, index=False)


# ---------------------------------------------------------------------------
# Main enrichment loop
# ---------------------------------------------------------------------------


def run_enrichment(
    api_key: str,
    clubs: List[Dict],
    dry_run: bool,
    use_csv: bool,
    checkpoint: Dict,
) -> Dict:
    """
    Core enrichment loop. Returns updated checkpoint dict.

    In dry-run mode: prints what would be queried but makes no API calls,
    no DB/CSV writes, and does NOT mutate or persist the checkpoint.
    """
    processed = checkpoint.get("processed", {})
    found = checkpoint.get("found", 0)
    queried = checkpoint.get("queried", 0)
    errors = checkpoint.get("errors", 0)

    batch_since_save = 0
    consecutive_errors = 0
    dry_run_count = 0

    for club in clubs:
        club_key = str(club["id"]) if club["id"] else club["club_name"]

        if dry_run:
            logger.info("[dry-run] Would search for: %s (%s, %s)", club["club_name"], club["city"], club["state"])
            dry_run_count += 1
            continue

        if club_key in processed:
            logger.debug("Skipping already-processed club: %s", club["club_name"])
            continue

        logger.info("Searching: %s (%s, %s)", club["club_name"], club["city"], club["state"])
        website = None

        try:
            website = _search_club_website(
                club_name=club["club_name"],
                city=club["city"],
                state=club["state"],
                api_key=api_key,
            )
            queried += 1
            consecutive_errors = 0

            if website:
                found += 1
                logger.info("  Found: %s", website)
                processed[club_key] = {"status": "found", "website": website}

                if use_csv:
                    _update_club_in_csv(club["club_name"], website)
                elif club["id"] is not None:
                    _update_club_in_db(club["id"], website)
            else:
                logger.info("  No result found")
                processed[club_key] = {"status": "not_found", "website": None}

        except RuntimeError as exc:
            errors += 1
            consecutive_errors += 1
            logger.error("API error for '%s': %s", club["club_name"], exc)
            processed[club_key] = {"status": "error", "website": None}

            if consecutive_errors >= 3:
                logger.error("3 consecutive API errors — checkpointing and aborting.")
                break

        batch_since_save += 1
        if batch_since_save >= CHECKPOINT_BATCH_SIZE:
            checkpoint = {
                "processed": processed,
                "found": found,
                "queried": queried,
                "errors": errors,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
            _save_checkpoint(checkpoint)
            logger.info("Checkpoint saved (%d processed so far)", len(processed))
            batch_since_save = 0

        time.sleep(RATE_LIMIT_SLEEP)

    if dry_run:
        return {
            "processed": checkpoint.get("processed", {}),
            "found": checkpoint.get("found", 0),
            "queried": checkpoint.get("queried", 0),
            "errors": checkpoint.get("errors", 0),
            "dry_run_count": dry_run_count,
        }

    return {
        "processed": processed,
        "found": found,
        "queried": queried,
        "errors": errors,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Enrich club records with websites discovered via Brave Search"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show clubs that would be queried; no API calls or writes",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N clubs (default: all)",
    )
    parser.add_argument(
        "--tier",
        type=int,
        default=None,
        metavar="N",
        help="Only process clubs from leagues of this tier (1-4)",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Read/write from master.csv instead of the PostgreSQL database",
    )
    args = parser.parse_args()

    api_key = os.environ.get("BRAVE_API_KEY", "")
    if not api_key and not args.dry_run:
        logger.error(
            "BRAVE_API_KEY environment variable is not set. "
            "Add it as a Replit secret named BRAVE_API_KEY."
        )
        sys.exit(1)
    elif not api_key and args.dry_run:
        logger.info("[dry-run] BRAVE_API_KEY not set, but continuing in dry-run mode.")

    checkpoint = _load_checkpoint()
    already_processed = len(checkpoint.get("processed", {}))
    if already_processed:
        logger.info("Resuming from checkpoint: %d clubs already processed", already_processed)

    logger.info("Loading clubs with no website%s...", f" (tier={args.tier})" if args.tier else "")
    if args.csv:
        clubs = _fetch_clubs_from_csv(tier=args.tier)
    else:
        clubs = _fetch_clubs_from_db(tier=args.tier)

    total_eligible = len(clubs)
    logger.info("Found %d eligible clubs (no website)", total_eligible)

    if args.limit is not None and not args.dry_run:
        already_done = set(checkpoint.get("processed", {}).keys())
        clubs_unprocessed = [
            c for c in clubs
            if (str(c["id"]) if c["id"] else c["club_name"]) not in already_done
        ]
        clubs = clubs_unprocessed[: args.limit]
        logger.info(
            "Limiting to %d new clubs per --limit flag (%d already processed)",
            len(clubs),
            len(already_done),
        )
    elif args.limit is not None:
        clubs = clubs[: args.limit]
        logger.info("Limiting to %d clubs per --limit flag", len(clubs))

    if not clubs:
        logger.info("No clubs to process. Exiting.")
        return

    final_checkpoint = run_enrichment(
        api_key=api_key,
        clubs=clubs,
        dry_run=args.dry_run,
        use_csv=args.csv,
        checkpoint=checkpoint,
    )

    if not args.dry_run:
        _save_checkpoint(final_checkpoint)

    total_queried = final_checkpoint["queried"]
    total_found = final_checkpoint["found"]
    total_errors = final_checkpoint["errors"]
    total_processed = len(final_checkpoint["processed"])
    dry_run_count = final_checkpoint.get("dry_run_count", 0)

    failure_rate = (
        (total_queried - total_found) / total_queried * 100 if total_queried > 0 else 0.0
    )

    print("\n" + "=" * 60)
    print("WEBSITE ENRICHMENT SUMMARY")
    print("=" * 60)
    print(f"  Total eligible clubs (no website): {total_eligible}")
    if args.dry_run:
        print(f"  Clubs that would be queried:       {dry_run_count}")
    else:
        print(f"  Clubs processed this run:          {total_processed}")
    print(f"  API queries made:                  {total_queried}")
    print(f"  Websites found:                    {total_found}")
    print(f"  API errors:                        {total_errors}")
    print(f"  Failure rate (no result):          {failure_rate:.1f}%")
    if not args.dry_run:
        print(f"  Checkpoint saved to:               {CHECKPOINT_PATH}")
    print("=" * 60)

    if args.dry_run:
        print("\n[dry-run mode] No API calls, DB/CSV writes, or checkpoint updates were made.")


if __name__ == "__main__":
    main()
