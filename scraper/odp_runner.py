"""
odp_runner.py — Orchestrate the ODP state-roster scrape.

Invoked via ``run.py --source odp-rosters`` (with optional ``--state CA``
and ``--limit N``). Loads the per-state URL seed from
``scraper/extractors/odp_seed_urls.yaml``, iterates URLs, picks the
parser registered in ``extractors.odp_rosters.PARSERS`` for each
state, writes through ``ingest.odp_writer.insert_odp_entries``.

Fails soft — a single URL failing (HTTP error, 404, DOM drift) logs a
warning and is recorded in the outcome counts but never stops the
batch. Individual rows that fail validation are skipped at the writer
layer, not here.

Because the YAML only carries (state, parser, program_year, urls),
the age-group / gender / source_url metadata is inferred per-URL:

  - source_url  → the URL being fetched
  - age_group   → "ALL"   (site-level aggregate; a future PR can split
                           per-page if we add explicit age_group hints
                           to the YAML)
  - gender      → "ALL"   (same rationale as age_group)
  - program_year → from the YAML

The natural-key uniqueness is (player_name, state, program_year,
age_group, gender), so ALL/ALL on the first pass simply means
"this player was in the state's ODP program in 2025-26." If a future
PR splits per-age-group URLs, swapping the age/gender columns to the
real values will create new rows for historical retention — not
overwrite the aggregate row.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(__file__))

from extractors.odp_rosters import PARSERS, parse_odp_page  # noqa: E402
from extractors import odp_hubspot_pdf  # noqa: E402
from ingest.odp_writer import insert_odp_entries  # noqa: E402
from utils import http as _http  # noqa: E402
from utils.retry import retry_with_backoff  # noqa: E402
from scrape_run_logger import (  # noqa: E402
    FailureKind,
    ScrapeRunLogger,
    classify_exception,
)
from alerts import alert_scraper_failure  # noqa: E402

logger = logging.getLogger("odp_runner")

SEED_PATH = os.path.join(os.path.dirname(__file__), "extractors", "odp_seed_urls.yaml")


@dataclass
class OdpRunSummary:
    states_scanned: int = 0
    pages_fetched: int = 0
    entries_parsed: int = 0
    rows_upserted: int = 0
    http_errors: int = 0
    per_state: Dict[str, Dict[str, int]] = field(default_factory=dict)


def _load_seed() -> Dict[str, Any]:
    """Load the YAML seed. Returns an empty states map if the file is
    missing or malformed — runners should degrade, not crash."""
    try:
        import yaml  # local import — keeps test envs without pyyaml importable
    except ImportError:
        logger.error("[odp-rosters] PyYAML not installed; cannot load seed URLs")
        return {"states": {}}

    if not os.path.exists(SEED_PATH):
        logger.warning("[odp-rosters] seed YAML missing at %s", SEED_PATH)
        return {"states": {}}

    try:
        with open(SEED_PATH, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except yaml.YAMLError as exc:
        logger.error("[odp-rosters] seed YAML malformed: %s", exc)
        return {"states": {}}

    if not isinstance(data, dict) or not isinstance(data.get("states"), dict):
        logger.error("[odp-rosters] seed YAML has unexpected shape; expected top-level 'states' mapping")
        return {"states": {}}

    return data


def _fetch_html(url: str, timeout: int = 20) -> Optional[str]:
    """GET ``url`` with retry + proxy rotation. Returns None on failure."""
    def _do_get() -> str:
        resp = _http.get(url, timeout=timeout, headers={
            "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)",
        })
        resp.raise_for_status()
        return resp.text

    try:
        return retry_with_backoff(_do_get, max_retries=3, base_delay=2.0)
    except Exception as exc:
        logger.warning("[odp-rosters] fetch failed for %s: %s", url, exc)
        return None


def run_odp_rosters(
    *,
    dry_run: bool = False,
    limit: Optional[int] = None,
    state: Optional[str] = None,
    **_kwargs: Any,
) -> OdpRunSummary:
    """Entry point for ``--source odp-rosters``.

    Parameters
    ----------
    dry_run
        When True, no DB writes occur; parsing and HTTP still run.
    limit
        Optional cap on TOTAL URLs processed across all states —
        useful for smoke-testing before a full run.
    state
        Optional two-letter state key (e.g. "CA") to restrict the
        scrape to a single state.
    """
    summary = OdpRunSummary()

    seed = _load_seed()
    states_map: Dict[str, Dict[str, Any]] = seed.get("states", {}) or {}

    if state:
        states_map = {k: v for k, v in states_map.items() if k.upper() == state.upper()}
        if not states_map:
            logger.error("[odp-rosters] --state %s not found in seed YAML", state)
            return summary

    urls_processed = 0
    scraper_key = "odp-rosters"

    # Top-level run log — a single row covering the whole batch. Each
    # URL's failure is logged as a soft warning + alert_scraper_failure
    # call; the outer log row only flips to 'failed' if the whole
    # batch can't run (e.g. YAML missing).
    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=scraper_key,
            league_name="ODP state rosters",
        )
        run_log.start(source_url="seed:odp_seed_urls.yaml")

    try:
        for state_key, state_cfg in sorted(states_map.items()):
            parser_key = state_cfg.get("parser")
            platform = (state_cfg.get("platform") or "").strip().lower()
            program_year = state_cfg.get("program_year") or "unknown"
            urls: List[str] = list(state_cfg.get("urls") or [])

            # HTML parsers must resolve against the PARSERS registry;
            # PDF parsers resolve against odp_hubspot_pdf.PARSER_KEYS.
            is_pdf_platform = platform == "hubspot-pdf"
            if parser_key:
                if is_pdf_platform:
                    if parser_key not in odp_hubspot_pdf.PARSER_KEYS:
                        logger.warning(
                            "[odp-rosters] state %s (hubspot-pdf) references unknown PDF parser %r — skipping",
                            state_key, parser_key,
                        )
                        continue
                elif parser_key not in PARSERS:
                    logger.warning(
                        "[odp-rosters] state %s references unknown parser %r — skipping",
                        state_key, parser_key,
                    )
                    continue

            if not urls:
                logger.info(
                    "[odp-rosters] state %s has no seed URLs (follow-up) — skipping",
                    state_key,
                )
                continue

            summary.states_scanned += 1
            per_state = summary.per_state.setdefault(
                state_key,
                {"pages_fetched": 0, "entries_parsed": 0, "rows_upserted": 0, "http_errors": 0},
            )

            for url in urls:
                if limit is not None and urls_processed >= limit:
                    break
                urls_processed += 1

                rows: List[Dict[str, Any]] = []

                if is_pdf_platform:
                    # PDF path: the extractor downloads + parses in one
                    # call and returns fully-stamped dicts.
                    entries = odp_hubspot_pdf.download_and_parse(
                        url,
                        state=state_key,
                        program_year=program_year,
                    )
                    if not entries:
                        # We can't distinguish "fetch failed" from "PDF
                        # parsed to zero rows" without plumbing more
                        # status through the extractor — log + alert
                        # as a network failure, which is the more likely
                        # cause and the one ops cares about catching.
                        summary.http_errors += 1
                        per_state["http_errors"] += 1
                        alert_scraper_failure(
                            scraper_key=scraper_key,
                            failure_kind=FailureKind.NETWORK.value,
                            error_message=f"hubspot-pdf fetch/parse returned 0 rows: {url}",
                            source_url=url,
                            league_name=f"ODP {state_key}",
                        )
                        continue
                    summary.pages_fetched += 1
                    per_state["pages_fetched"] += 1

                    for e in entries:
                        if not e.get("player_name"):
                            continue
                        rows.append({
                            "player_name": e["player_name"],
                            "graduation_year": e.get("graduation_year"),
                            "position": e.get("position"),
                            "club_name_raw": e.get("club_name_raw"),
                            "state": state_key,
                            "program_year": program_year,
                            "age_group": e.get("age_group") or "ALL",
                            "gender": e.get("gender") or "ALL",
                            "source_url": url,
                        })
                else:
                    # HTML path (original behavior).
                    html = _fetch_html(url)
                    if html is None:
                        summary.http_errors += 1
                        per_state["http_errors"] += 1
                        alert_scraper_failure(
                            scraper_key=scraper_key,
                            failure_kind=FailureKind.NETWORK.value,
                            error_message=f"fetch failed: {url}",
                            source_url=url,
                            league_name=f"ODP {state_key}",
                        )
                        continue

                    summary.pages_fetched += 1
                    per_state["pages_fetched"] += 1

                    entries = parse_odp_page(parser_key or "", html)
                    if not entries:
                        logger.info("[odp-rosters] %s: 0 entries parsed from %s", state_key, url)
                        continue

                    # Stamp runner-supplied metadata onto every row. See
                    # module docstring for the ALL/ALL rationale.
                    for e in entries:
                        if not e.get("player_name"):
                            continue
                        rows.append({
                            "player_name": e["player_name"],
                            "graduation_year": e.get("graduation_year"),
                            "position": e.get("position"),
                            "club_name_raw": e.get("club_name_raw"),
                            "state": state_key,
                            "program_year": program_year,
                            "age_group": e.get("age_group") or "ALL",
                            "gender": e.get("gender") or "ALL",
                            "source_url": url,
                        })

                summary.entries_parsed += len(rows)
                per_state["entries_parsed"] += len(rows)

                try:
                    counts = insert_odp_entries(rows, dry_run=dry_run)
                except Exception as exc:
                    logger.warning(
                        "[odp-rosters] write failed for %s / %s: %s",
                        state_key, url, exc,
                    )
                    continue

                written = counts.get("inserted", 0) + counts.get("updated", 0)
                summary.rows_upserted += written
                per_state["rows_upserted"] += written

            if limit is not None and urls_processed >= limit:
                break

    except Exception as exc:
        kind = classify_exception(exc)
        logger.error("[odp-rosters] batch failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        return summary

    if run_log is not None:
        if summary.entries_parsed == 0 and summary.pages_fetched == 0:
            run_log.finish_partial(
                records_failed=0,
                error_message="no pages fetched and no entries parsed",
            )
        else:
            run_log.finish_ok(
                records_created=summary.rows_upserted,
                records_updated=0,
                records_failed=summary.http_errors,
            )

    return summary


def print_summary(summary: OdpRunSummary) -> None:
    print("\n" + "=" * 60)
    print("  ODP state rosters — run summary")
    print("=" * 60)
    print(f"  States scanned   : {summary.states_scanned}")
    print(f"  Pages fetched    : {summary.pages_fetched}")
    print(f"  Entries parsed   : {summary.entries_parsed}")
    print(f"  Rows upserted    : {summary.rows_upserted}")
    print(f"  HTTP errors      : {summary.http_errors}")
    if summary.per_state:
        print("\n  Per-state:")
        for state_key in sorted(summary.per_state):
            p = summary.per_state[state_key]
            print(
                f"    {state_key:<6} pages={p['pages_fetched']:<3} "
                f"entries={p['entries_parsed']:<4} upserted={p['rows_upserted']:<4} "
                f"http_errors={p['http_errors']}"
            )
    print("=" * 60)
