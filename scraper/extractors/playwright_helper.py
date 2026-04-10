"""
Shared Playwright helper for custom extractors.

Provides a simple render_page() function that:
  - Launches headless Chromium with sandbox-safe flags
  - Waits for network idle (up to timeout seconds)
  - Returns the rendered HTML string

Returns None if the browser cannot resolve DNS (sandboxed environment).
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import MAX_RETRIES, RETRY_BASE_DELAY_SECONDS
from utils.retry import retry_with_backoff, TransientError

logger = logging.getLogger(__name__)

_CHROMIUM_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--single-process",
]

_NETWORK_ERRORS = (
    "ERR_NAME_NOT_RESOLVED",
    "ERR_CONNECTION_REFUSED",
    "ERR_INTERNET_DISCONNECTED",
    "net::ERR_",
)


def _is_network_error(msg: str) -> bool:
    return any(tag in msg for tag in _NETWORK_ERRORS)


def _is_navigation_timeout(msg: str) -> bool:
    return "timeout" in msg.lower() and "navigation" in msg.lower()


def render_page(url: str, wait_until: str = "networkidle", timeout_ms: int = 30_000) -> Optional[str]:
    """
    Render a page with Playwright and return the HTML string.

    Retries up to MAX_RETRIES times on transient errors (network errors and
    navigation timeouts) using exponential backoff from config.

    Returns None on any unrecoverable failure (network error, navigation
    timeout after retries, or other rendering error). Never raises.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("Playwright not installed; cannot render JS pages")
        return None

    logger.info("[Playwright] Rendering %s (wait_until=%s, timeout=%dms)", url, wait_until, timeout_ms)

    def _render_once() -> str:
        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True, args=_CHROMIUM_ARGS)
                ctx = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    )
                )
                page = ctx.new_page()
                page.goto(url, wait_until=wait_until, timeout=timeout_ms)
                html = page.content()
                browser.close()
                return html
        except Exception as exc:
            msg = str(exc)
            if _is_network_error(msg) or _is_navigation_timeout(msg):
                raise TransientError(msg) from exc
            raise

    try:
        return retry_with_backoff(
            _render_once,
            max_retries=MAX_RETRIES,
            base_delay=RETRY_BASE_DELAY_SECONDS,
            label=f"playwright:{url}",
        )
    except TransientError as exc:
        logger.warning("[Playwright] Network/timeout error after retries (sandbox?): %s — falling back", exc)
        return None
    except Exception as exc:
        logger.error("[Playwright] Error rendering %s: %s", url, exc)
        return None
