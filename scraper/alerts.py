"""
Post-run failure alerting.

Fires a webhook when a scraper run fails. No-op when ALERT_WEBHOOK_URL
is not set. Designed to be called from run.py after finish_failed().
"""
import os
import json
import logging
import urllib.request
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

ALERT_WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL")


def alert_scraper_failure(
    scraper_key: str,
    failure_kind: str,
    error_message: str,
    source_url: str | None = None,
    league_name: str | None = None,
) -> None:
    """Fire a webhook on scraper failure. No-op if ALERT_WEBHOOK_URL unset."""
    if not ALERT_WEBHOOK_URL:
        return

    payload = {
        "event": "scraper_failure",
        "scraper_key": scraper_key,
        "league_name": league_name,
        "failure_kind": failure_kind,
        "error_message": error_message[:500],  # cap to avoid huge payloads
        "source_url": source_url,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        req = urllib.request.Request(
            ALERT_WEBHOOK_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
        logger.info("alert sent for %s failure", scraper_key)
    except Exception as exc:
        # Alert delivery must never crash the scraper
        logger.warning("alert delivery failed: %s", exc)
