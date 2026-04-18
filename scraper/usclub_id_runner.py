"""
usclub_id_runner.py — Discover + ingest US Club Soccer iD selections.

Two responsibilities:
  1. Discover SoccerWire articles announcing iD pool / Training Center
     selections (Option A, public).
  2. (Future) Walk the usclubsoccer.org members area for canonical
     rosters (Option B, login-gated). Stubbed today; see
     ``scraper/extractors/usclub_id.py``.

For this PR the discovery step is metadata-only: it returns parsed
post titles/urls/dates and prints them. The follow-up PR will add
per-template body parsing into ``player_id_selections`` rows and
feed them through ``ingest.id_selection_writer``.

Invoked from ``run.py --source usclub-id``.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(__file__))

from extractors.usclub_id import scrape_soccerwire_id_articles  # noqa: E402
from ingest.id_selection_writer import insert_player_id_selections  # noqa: E402
from scrape_run_logger import ScrapeRunLogger, classify_exception  # noqa: E402
from alerts import alert_scraper_failure  # noqa: E402

logger = logging.getLogger("usclub_id_runner")


_SCRAPER_KEY = "usclub-id-soccerwire"
_LEAGUE_NAME = "US Club iD program"
_SOURCE_URL = "https://www.soccerwire.com/wp-json/wp/v2/posts"


@dataclass
class RunOutcome:
    phase: str
    posts_discovered: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_skipped: int = 0
    error: Optional[str] = None
    discovered: List[Dict[str, Any]] = field(default_factory=list)


def run_usclub_id(
    *,
    dry_run: bool = False,
    limit: Optional[int] = None,
) -> List[RunOutcome]:
    """Main entry point.

    Phase 1 (this PR): discover SoccerWire iD articles.
    Phase 2 (follow-up): parse each article body into selection rows
    and upsert via :func:`insert_player_id_selections`.
    """
    outcomes: List[RunOutcome] = []

    # --- Phase 1: SoccerWire discovery ---
    outcome = RunOutcome(phase="soccerwire-discovery")
    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=_SCRAPER_KEY,
            league_name=_LEAGUE_NAME,
        )
        run_log.start(source_url=_SOURCE_URL)

    try:
        discovered = scrape_soccerwire_id_articles()
    except Exception as exc:
        kind = classify_exception(exc)
        logger.error("[usclub-id] SoccerWire discovery failed: %s", exc)
        outcome.error = str(exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=_SCRAPER_KEY,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url=_SOURCE_URL,
            league_name=_LEAGUE_NAME,
        )
        outcomes.append(outcome)
        return outcomes

    if limit is not None:
        discovered = discovered[:limit]

    outcome.posts_discovered = len(discovered)
    outcome.discovered = discovered

    # Body parsing into player rows is a follow-up PR. Until it lands
    # the writer call is a no-op (zero rows). We still wire the call
    # site so the follow-up just slots in the parser output.
    rows: List[Dict[str, Any]] = []
    counts = insert_player_id_selections(rows, dry_run=dry_run)
    outcome.rows_inserted = counts["inserted"]
    outcome.rows_updated = counts["updated"]
    outcome.rows_skipped = counts["skipped"]

    if run_log is not None:
        run_log.finish_ok(
            records_created=outcome.rows_inserted,
            records_updated=outcome.rows_updated,
            records_failed=outcome.rows_skipped,
        )

    outcomes.append(outcome)
    logger.info(
        "[usclub-id] Phase 1 done: %d posts discovered, %d rows written "
        "(body parsing arrives in follow-up PR)",
        outcome.posts_discovered, outcome.rows_inserted,
    )
    return outcomes


def print_summary(outcomes: List[RunOutcome]) -> None:
    print("\n" + "=" * 60)
    print("  US Club iD — run summary")
    print("=" * 60)
    for o in outcomes:
        print(f"\n  Phase: {o.phase}")
        print(f"    Posts discovered  : {o.posts_discovered}")
        print(f"    Rows inserted     : {o.rows_inserted}")
        print(f"    Rows updated      : {o.rows_updated}")
        print(f"    Rows skipped      : {o.rows_skipped}")
        if o.error:
            print(f"    ERROR             : {o.error}")
        for post in o.discovered[:20]:
            print(f"      - {post.get('date', '?')} | {post.get('title', '?')}")
        if len(o.discovered) > 20:
            print(f"      ... and {len(o.discovered) - 20} more")
    print("=" * 60)
