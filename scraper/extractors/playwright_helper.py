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
from typing import Optional

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


def render_page(url: str, wait_until: str = "networkidle", timeout_ms: int = 30_000) -> Optional[str]:
    """
    Render a page with Playwright and return the HTML string.

    Returns None on network error (sandbox DNS restriction).
    Raises on other errors.
    """
    try:
        from playwright.sync_api import sync_playwright, Error as PlaywrightError
    except ImportError:
        logger.error("Playwright not installed; cannot render JS pages")
        return None

    logger.info("[Playwright] Rendering %s (wait_until=%s, timeout=%dms)", url, wait_until, timeout_ms)
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
        if any(tag in msg for tag in _NETWORK_ERRORS):
            logger.warning("[Playwright] Network error (sandbox?): %s — falling back", exc)
            return None
        logger.error("[Playwright] Error rendering %s: %s", url, exc)
        return None
