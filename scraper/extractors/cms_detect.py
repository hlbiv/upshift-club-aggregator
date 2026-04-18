"""
cms_detect.py — Identify the CMS / hosting platform behind a club site.

Used by greenfield extractors (Squarespace, SportsEngine, Duda/360Player,
WordPress) to short-circuit detection before running platform-specific
parsing. Cheap, header- and HTML-signature based — no external lookups.

Returns a normalized lowercase string or ``None``. Caller decides what
to do with the answer; this module makes no scraping decisions itself.

Detection precedence (ordered): the function checks platforms in order
and returns the first hit. Headers are checked first (cheaper, more
authoritative); HTML signatures are fallback for platforms that don't
brand their headers (Squarespace, WordPress).
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# Each detection rule cites the signature source in its inline comment.
# When adding a new rule, document WHERE the signature was observed so a
# future maintainer can verify it hasn't drifted.


def detect_cms(response) -> Optional[str]:
    """Return the platform name for ``response`` or ``None`` if unknown.

    Accepts a ``requests.Response``-like object: must expose ``.headers``
    (dict-like, case-insensitive recommended) and ``.text`` (the body
    string). Both are accessed defensively — missing attrs degrade to
    "no signature found" rather than raising.

    Possible return values:
      ``"squarespace"`` | ``"sportsengine"`` | ``"duda"`` |
      ``"wordpress"`` | ``"wix"`` | ``None``
    """
    headers = _safe_headers(response)
    body = _safe_body(response)

    # ---- Header-based detection (cheap, authoritative when present) ----

    # Squarespace fronts every site with their own server banner.
    # Source: any squarespace.com-hosted site returns ``Server: Squarespace``.
    server = headers.get("Server", "") or headers.get("server", "")
    if "squarespace" in server.lower():
        return "squarespace"

    # SportsEngine sets ``X-Powered-By: ngin`` (their internal stack name).
    # Source: observed on *.sportngin.com and *.sportsengine.com responses.
    powered = headers.get("X-Powered-By", "") or headers.get("x-powered-by", "")
    if "ngin" in powered.lower() or "sportsengine" in powered.lower():
        return "sportsengine"

    # Wix exposes ``X-Wix-Request-Id`` on every CDN response.
    # Source: observed on *.wixsite.com and custom-domain Wix sites.
    if any(h.lower().startswith("x-wix-") for h in headers.keys()):
        return "wix"

    # Duda is whitelabeled across many CDNs but their static asset host
    # is constant: ``irp.cdn-website.com``. Header check is best-effort
    # because Duda doesn't always brand the response server.
    if "duda" in server.lower():
        return "duda"

    # ---- HTML-signature detection (fallback) ----

    if not body:
        return None

    body_lower = body.lower()

    # Squarespace assets always come from ``static1.squarespace.com``.
    # Source: any Squarespace site embeds CSS/JS from that CDN.
    if "static1.squarespace.com" in body_lower:
        return "squarespace"

    # SportsEngine embeds widgets / assets from sportngin.com or
    # sportsengine.com domains. Public team pages render server-side
    # with these references in the HTML.
    if "sportngin.com" in body_lower or "sportsengine.com" in body_lower:
        return "sportsengine"

    # Duda's CDN host is constant across whitelabeled deployments.
    # Source: ``irp.cdn-website.com`` references in <link>/<script> srcs.
    if "irp.cdn-website.com" in body_lower or "irp-cdn.multiscreensite.com" in body_lower:
        return "duda"

    # WordPress emits a generator meta tag and ``/wp-content/`` /
    # ``/wp-includes/`` asset paths. Either signature is conclusive.
    # Source: default WP themes + the vast majority of WP-hosted clubs.
    if 'name="generator" content="wordpress' in body_lower:
        return "wordpress"
    if "/wp-content/" in body_lower or "/wp-includes/" in body_lower:
        return "wordpress"

    # Wix sometimes ships without the X-Wix-* headers (custom domain
    # CDN edge). The body always references the wixstatic CDN.
    # Source: observed on *.wixsite.com and custom Wix domains.
    if "static.wixstatic.com" in body_lower or "static.parastorage.com" in body_lower:
        return "wix"

    return None


# ---------------------------------------------------------------- defensive accessors


def _safe_headers(response) -> dict:
    """Return ``response.headers`` as a dict; empty dict on any failure."""
    try:
        headers = getattr(response, "headers", None) or {}
        # CaseInsensitiveDict from requests behaves like a dict for our purposes.
        return headers
    except Exception:
        return {}


def _safe_body(response) -> str:
    """Return ``response.text`` (string); empty string on any failure."""
    try:
        body = getattr(response, "text", "") or ""
        if isinstance(body, bytes):
            body = body.decode("utf-8", errors="replace")
        return body
    except Exception:
        return ""
