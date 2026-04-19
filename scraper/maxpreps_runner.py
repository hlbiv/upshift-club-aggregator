"""
maxpreps_runner.py — Orchestrate the MaxPreps HS roster extractor.

Framework-only in this PR. The extractor + schema + writer are fully
wired, but MaxPreps has aggressive anti-bot defenses and the current
``scraper/proxy_config.yaml`` ships with an empty proxy pool for
``maxpreps.com``. Live volume will 403 until proxy credentials are
added. Use ``--limit`` (default 20) to cap any smoke run.

Typical invocation::

    python3 scraper/run.py --source maxpreps-rosters --dry-run --limit 3

Workflow per seed:
  1. Fetch the roster URL via ``scraper.utils.http.get`` (routes through
     the proxy pool if configured for maxpreps.com).
  2. If status != 200, count the response class (403 / other) and
     continue. MaxPreps 403s are the common failure mode and are
     logged once per URL with the "add proxy creds" hint.
  3. Otherwise parse via ``parse_maxpreps_roster`` and upsert the
     per-player rows against ``hs_rosters``.

This runner is fail-soft at the per-URL level — a single bad fetch
never stops the batch.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(__file__))

from utils.http import get as http_get  # noqa: E402
from extractors.maxpreps_rosters import parse_maxpreps_roster  # noqa: E402
from ingest.hs_rosters_writer import insert_hs_rosters  # noqa: E402
from scrape_run_logger import (  # noqa: E402
    FailureKind,
    ScrapeRunLogger,
    classify_exception,
)
from alerts import alert_scraper_failure  # noqa: E402

logger = logging.getLogger("maxpreps_runner")

SCRAPER_KEY = "maxpreps-rosters"
# Default cap on seeds processed per run. MaxPreps rate-limits hard; live
# scraping at volume without proxies will 403 most requests. Operators
# can raise this with --limit but should expect 403s until proxy
# credentials land in proxy_config.yaml.
DEFAULT_LIMIT = 20

# Framework seed list — a handful of real public HS soccer programs
# across a few states. URLs follow the MaxPreps canonical path:
#
#     /high-schools/<school-slug>/soccer-<gender>/roster.htm
#
# The seed list is deliberately tiny (~10 rows). Full state-by-state
# discovery will be a follow-up PR once proxy infra is in place.
MAXPREPS_SEEDS: List[Dict[str, Any]] = [
    # California — boys
    {
        "school_name_raw": "Mater Dei",
        "school_state": "CA",
        "school_city": "Santa Ana",
        "team_level": "Varsity",
        "gender": "boys",
        "url": "https://www.maxpreps.com/high-schools/mater-dei-monarchs-(santa-ana,ca)/soccer/roster.htm",
    },
    {
        "school_name_raw": "Loyola",
        "school_state": "CA",
        "school_city": "Los Angeles",
        "team_level": "Varsity",
        "gender": "boys",
        "url": "https://www.maxpreps.com/high-schools/loyola-cubs-(los-angeles,ca)/soccer/roster.htm",
    },
    {
        "school_name_raw": "Servite",
        "school_state": "CA",
        "school_city": "Anaheim",
        "team_level": "Varsity",
        "gender": "boys",
        "url": "https://www.maxpreps.com/high-schools/servite-friars-(anaheim,ca)/soccer/roster.htm",
    },
    # California — girls
    {
        "school_name_raw": "Mater Dei",
        "school_state": "CA",
        "school_city": "Santa Ana",
        "team_level": "Varsity",
        "gender": "girls",
        "url": "https://www.maxpreps.com/high-schools/mater-dei-monarchs-(santa-ana,ca)/soccer-winter-girls/roster.htm",
    },
    # Texas — boys
    {
        "school_name_raw": "Jesuit",
        "school_state": "TX",
        "school_city": "Dallas",
        "team_level": "Varsity",
        "gender": "boys",
        "url": "https://www.maxpreps.com/high-schools/jesuit-rangers-(dallas,tx)/soccer/roster.htm",
    },
    {
        "school_name_raw": "Coppell",
        "school_state": "TX",
        "school_city": "Coppell",
        "team_level": "Varsity",
        "gender": "boys",
        "url": "https://www.maxpreps.com/high-schools/coppell-cowboys-(coppell,tx)/soccer/roster.htm",
    },
    # Texas — girls
    {
        "school_name_raw": "Highland Park",
        "school_state": "TX",
        "school_city": "Dallas",
        "team_level": "Varsity",
        "gender": "girls",
        "url": "https://www.maxpreps.com/high-schools/highland-park-scots-(dallas,tx)/soccer-winter-girls/roster.htm",
    },
    # Florida — boys
    {
        "school_name_raw": "Montverde Academy",
        "school_state": "FL",
        "school_city": "Montverde",
        "team_level": "Varsity",
        "gender": "boys",
        "url": "https://www.maxpreps.com/high-schools/montverde-academy-eagles-(montverde,fl)/soccer/roster.htm",
    },
    {
        "school_name_raw": "IMG Academy",
        "school_state": "FL",
        "school_city": "Bradenton",
        "team_level": "Varsity",
        "gender": "boys",
        "url": "https://www.maxpreps.com/high-schools/img-academy-ascenders-(bradenton,fl)/soccer/roster.htm",
    },
    # Florida — girls
    {
        "school_name_raw": "IMG Academy",
        "school_state": "FL",
        "school_city": "Bradenton",
        "team_level": "Varsity",
        "gender": "girls",
        "url": "https://www.maxpreps.com/high-schools/img-academy-ascenders-(bradenton,fl)/soccer-winter-girls/roster.htm",
    },
]


def _current_season() -> str:
    """Return the current HS soccer season tag, e.g. '2025-26'."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    y = now.year
    if now.month >= 8:
        return f"{y}-{str(y + 1)[-2:]}"
    return f"{y - 1}-{str(y)[-2:]}"


@dataclass
class MaxPrepsRunOutcome:
    pages_fetched: int = 0
    players_parsed: int = 0
    rows_upserted: int = 0
    http_403s: int = 0
    http_other_errors: int = 0
    counts: Dict[str, int] = field(default_factory=dict)
    failure_kind: Optional[FailureKind] = None
    error: Optional[str] = None


def run_maxpreps_rosters(
    *,
    dry_run: bool = False,
    limit: Optional[int] = None,
    state: Optional[str] = None,
    **kwargs: Any,
) -> MaxPrepsRunOutcome:
    """Fetch → parse → upsert MaxPreps roster pages.

    Parameters
    ----------
    dry_run :
        If True, parse but do not write rows.
    limit :
        Max number of seed URLs to hit. Defaults to :data:`DEFAULT_LIMIT`.
        MaxPreps rate-limits hard at volume.
    state :
        Optional 2-letter state filter. Case-insensitive.
    """
    effective_limit = DEFAULT_LIMIT if limit is None else int(limit)

    seeds = list(MAXPREPS_SEEDS)
    if state:
        want = state.strip().upper()
        seeds = [s for s in seeds if s["school_state"].upper() == want]
    seeds = seeds[:effective_limit]

    outcome = MaxPrepsRunOutcome()
    if not seeds:
        logger.info("[maxpreps] no seeds match the filter (state=%s)", state)
        return outcome

    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=SCRAPER_KEY,
            league_name="MaxPreps HS rosters",
        )
        run_log.start(source_url="seed:MAXPREPS_SEEDS")

    season = _current_season()
    all_rows: List[Dict[str, Any]] = []

    for seed in seeds:
        url = seed["url"]
        try:
            resp = http_get(
                url,
                timeout=15,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (X11; Linux x86_64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Accept": "text/html,application/xhtml+xml,*/*",
                },
            )
        except Exception as exc:
            outcome.http_other_errors += 1
            logger.warning("[maxpreps] fetch error at %s: %s", url, exc)
            continue

        outcome.pages_fetched += 1

        if resp.status_code == 403:
            outcome.http_403s += 1
            logger.warning(
                "[maxpreps] 403 at %s — add proxy creds for maxpreps.com "
                "in scraper/proxy_config.yaml",
                url,
            )
            continue

        if resp.status_code != 200:
            outcome.http_other_errors += 1
            logger.warning(
                "[maxpreps] HTTP %s at %s",
                resp.status_code, url,
            )
            continue

        try:
            players = parse_maxpreps_roster(resp.text)
        except Exception as exc:
            outcome.http_other_errors += 1
            logger.warning("[maxpreps] parse error at %s: %s", url, exc)
            continue

        if not players:
            logger.info("[maxpreps] 0 players parsed from %s", url)
            continue

        outcome.players_parsed += len(players)

        for p in players:
            all_rows.append({
                "school_name_raw": seed["school_name_raw"],
                "school_state": seed["school_state"],
                "school_city": seed.get("school_city"),
                "team_level": seed.get("team_level"),
                "season": season,
                "gender": seed["gender"],
                "player_name": p["player_name"],
                "jersey_number": p.get("jersey_number"),
                "graduation_year": p.get("graduation_year"),
                "position": p.get("position"),
                "height": p.get("height"),
                "source_url": url,
            })

    # Persist. Dry-run short-circuits inside the writer.
    if all_rows:
        try:
            counts = insert_hs_rosters(all_rows, dry_run=dry_run)
        except Exception as exc:
            kind = classify_exception(exc)
            outcome.failure_kind = kind
            outcome.error = str(exc)
            logger.error("[maxpreps] write failed: %s", exc)
            if run_log is not None:
                run_log.finish_failed(kind, error_message=str(exc))
            alert_scraper_failure(
                scraper_key=SCRAPER_KEY,
                failure_kind=kind.value,
                error_message=str(exc),
                source_url="seed:MAXPREPS_SEEDS",
                league_name="MaxPreps HS rosters",
            )
            return outcome
        outcome.rows_upserted = counts.get("inserted", 0) + counts.get("updated", 0)
        outcome.counts = counts

    logger.info(
        "[maxpreps] seeds=%d pages_fetched=%d players_parsed=%d "
        "rows_upserted=%d http_403s=%d http_other_errors=%d",
        len(seeds), outcome.pages_fetched, outcome.players_parsed,
        outcome.rows_upserted, outcome.http_403s, outcome.http_other_errors,
    )

    if run_log is not None:
        if outcome.http_403s == len(seeds) and len(seeds) > 0:
            # Every seed blocked — treat as network-class failure so
            # scrape_health rollups surface the need for proxies.
            run_log.finish_failed(
                FailureKind.NETWORK,
                error_message=(
                    f"{outcome.http_403s} / {len(seeds)} seeds returned 403 — "
                    "add proxy creds for maxpreps.com"
                ),
            )
        elif outcome.rows_upserted == 0 and outcome.players_parsed == 0:
            run_log.finish_partial(
                records_failed=0,
                error_message="no rows produced; likely blocked or empty pages",
            )
        else:
            run_log.finish_ok(
                records_created=outcome.counts.get("inserted", 0),
                records_updated=outcome.counts.get("updated", 0),
                records_failed=outcome.counts.get("skipped", 0),
            )

    return outcome


def print_summary(outcome: MaxPrepsRunOutcome) -> None:
    print("\n" + "=" * 60)
    print("  MaxPreps HS rosters — run summary")
    print("=" * 60)
    print(f"  Pages fetched       : {outcome.pages_fetched}")
    print(f"  Players parsed      : {outcome.players_parsed}")
    print(f"  Rows upserted       : {outcome.rows_upserted}")
    print(f"  HTTP 403s           : {outcome.http_403s}")
    print(f"  HTTP other errors   : {outcome.http_other_errors}")
    if outcome.http_403s:
        print(
            "\n  Note: MaxPreps blocks aggressively at volume. Add "
            "proxy credentials for maxpreps.com in "
            "scraper/proxy_config.yaml to reduce 403 rate."
        )
    if outcome.failure_kind is not None:
        print(f"\n  Failure kind : {outcome.failure_kind.value}")
        if outcome.error:
            print(f"  Error        : {outcome.error[:160]}")
    print("=" * 60)
