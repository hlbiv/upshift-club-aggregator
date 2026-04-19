"""
Outbound HTTP with per-domain proxy rotation.

Public surface is a single function, :func:`get`, which wraps
``requests.get`` and transparently routes through a proxy pool defined
in ``scraper/proxy_config.yaml``. The pool is **empty by default** —
when no proxies are configured for the target host, ``get`` is a
direct pass-through to ``requests.get``.

Design notes
------------
- Sits *inside* the retry wrapper in ``scraper/utils/retry.py``. The
  retry layer still owns "call the HTTP op up to N times on transient
  failure"; this module owns "pick a working proxy for the current
  call". Each retry re-enters ``get`` and re-picks a proxy, which is
  the behaviour we want.
- Per-proxy cooldown: if a proxy returns 429 / raises a connection
  error ≥ 3 times in a 60 s sliding window, it is banished for
  ``cooldown_seconds`` (default 300 s). All state is in-memory.
- Fallback: if every proxy in the pool is cooling down or failing,
  we drop to a direct request and log a warning. This is a deliberate
  design choice — a misconfigured pool should not take scrapers
  offline entirely, it should degrade to baseline behaviour.

YAML format
-----------
::

    domains:
      <hostname>:
        proxies:
          - http://user:pass@host:port
          - http://user:pass@host:port
        cooldown_seconds: 300   # optional; default 300

An empty ``domains: {}`` map (the initial state) is fine — every
call falls through to a direct request and this module does
effectively nothing.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from typing import Any, Deque, Dict, Optional
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Config loading (lazy, cached)
# --------------------------------------------------------------------------

_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "proxy_config.yaml",
)

_CONFIG: Optional[Dict[str, Any]] = None
_CONFIG_LOCK = threading.Lock()

# Cooldown bookkeeping — all in-memory, per-process.
_COOLDOWN_UNTIL: Dict[str, float] = {}      # proxy_url -> unix ts
_FAILURE_WINDOW: Dict[str, Deque[float]] = {}  # proxy_url -> recent failure timestamps
_STATE_LOCK = threading.Lock()

# Rolling failure window + threshold — see module docstring.
_FAILURE_WINDOW_SECONDS = 60.0
_FAILURE_THRESHOLD = 3
_DEFAULT_COOLDOWN_SECONDS = 300


def _load_config() -> Dict[str, Any]:
    """
    Return the parsed proxy config. Loaded once per process; subsequent
    calls return the cached dict.

    On I/O error or malformed YAML, returns ``{"domains": {}}`` and
    logs a warning. Scrapers keep working via direct connection.
    """
    global _CONFIG
    if _CONFIG is not None:
        return _CONFIG

    with _CONFIG_LOCK:
        if _CONFIG is not None:
            return _CONFIG

        try:
            import yaml  # local import — keeps requests-only callers cheap
        except ImportError:
            logger.warning(
                "[http] PyYAML not installed; proxy config unavailable, "
                "all requests will go direct."
            )
            _CONFIG = {"domains": {}}
            return _CONFIG

        try:
            with open(_CONFIG_PATH, "r", encoding="utf-8") as fh:
                raw = yaml.safe_load(fh)
        except FileNotFoundError:
            logger.warning(
                "[http] proxy_config.yaml not found at %s; "
                "falling back to direct connection.",
                _CONFIG_PATH,
            )
            _CONFIG = {"domains": {}}
            return _CONFIG
        except (yaml.YAMLError, OSError) as exc:
            logger.warning(
                "[http] proxy_config.yaml is malformed (%s); "
                "falling back to direct connection.",
                exc,
            )
            _CONFIG = {"domains": {}}
            return _CONFIG

        if not isinstance(raw, dict) or not isinstance(raw.get("domains"), dict):
            logger.warning(
                "[http] proxy_config.yaml has unexpected shape; "
                "expected top-level 'domains' mapping. "
                "Falling back to direct connection."
            )
            _CONFIG = {"domains": {}}
            return _CONFIG

        _CONFIG = raw
        return _CONFIG


def _reset_config_cache_for_tests() -> None:
    """Clear the config + cooldown caches. Test-only helper."""
    global _CONFIG
    with _CONFIG_LOCK:
        _CONFIG = None
    with _STATE_LOCK:
        _COOLDOWN_UNTIL.clear()
        _FAILURE_WINDOW.clear()


# --------------------------------------------------------------------------
# Proxy selection + cooldown bookkeeping
# --------------------------------------------------------------------------

def _proxies_for_host(hostname: str) -> tuple[list[str], int]:
    """Return (proxy_urls, cooldown_seconds) for ``hostname``."""
    cfg = _load_config()
    domains = cfg.get("domains", {}) or {}
    entry = domains.get(hostname) or {}
    proxies = list(entry.get("proxies") or [])
    cooldown = int(entry.get("cooldown_seconds") or _DEFAULT_COOLDOWN_SECONDS)
    return proxies, cooldown


def _is_in_cooldown(proxy_url: str, now: float) -> bool:
    with _STATE_LOCK:
        until = _COOLDOWN_UNTIL.get(proxy_url, 0.0)
        return until > now


def _record_failure(proxy_url: str, cooldown_seconds: int) -> None:
    """
    Append a failure timestamp for ``proxy_url``. If ≥ ``_FAILURE_THRESHOLD``
    failures occurred in the last ``_FAILURE_WINDOW_SECONDS``, put the proxy
    into cooldown.
    """
    now = time.time()
    cutoff = now - _FAILURE_WINDOW_SECONDS
    with _STATE_LOCK:
        window = _FAILURE_WINDOW.setdefault(proxy_url, deque())
        window.append(now)
        # Evict entries outside the window.
        while window and window[0] < cutoff:
            window.popleft()
        if len(window) >= _FAILURE_THRESHOLD:
            _COOLDOWN_UNTIL[proxy_url] = now + cooldown_seconds
            logger.warning(
                "[http] proxy %s: %d failures in last %ds — cooldown %ds",
                proxy_url, len(window), int(_FAILURE_WINDOW_SECONDS), cooldown_seconds,
            )
            # Reset the window so the cooldown replaces, not compounds.
            window.clear()


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------

def get(
    url: str,
    *,
    timeout: int = 15,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> requests.Response:
    """
    ``requests.get`` with transparent per-domain proxy rotation.

    Semantics
    ---------
    1. Resolve the hostname from ``url``.
    2. If no proxies are configured for that host (or the config is
       empty), make a direct ``requests.get`` call. This is the default
       path and is hit on every call when ``proxy_config.yaml`` is
       empty.
    3. Otherwise iterate the proxy list in order:
       - Skip any proxy currently in cooldown.
       - Attempt the request through the proxy.
       - On 2xx: return the response.
       - On 429 or connection error: record a failure; if the failure
         threshold is tripped, put the proxy into cooldown and move on.
    4. If every proxy fails / is cooling down, fall back to a direct
       request and log a warning.

    Notes
    -----
    - 4xx other than 429 are returned as-is to the caller — they're
      not proxy problems.
    - ``**kwargs`` is passed through to ``requests.get`` so callers
      keep access to ``cookies``, ``allow_redirects``, etc.
    """
    parsed = urlparse(url)
    hostname = parsed.hostname or ""

    proxies, cooldown_seconds = _proxies_for_host(hostname)

    # Fast path: no proxies configured for this host.
    if not proxies:
        return requests.get(url, timeout=timeout, headers=headers, **kwargs)

    now = time.time()
    last_response: Optional[requests.Response] = None

    for proxy_url in proxies:
        if _is_in_cooldown(proxy_url, now):
            logger.debug("[http] proxy %s in cooldown; skipping", proxy_url)
            continue

        try:
            resp = requests.get(
                url,
                timeout=timeout,
                headers=headers,
                proxies={"http": proxy_url, "https": proxy_url},
                **kwargs,
            )
        except (requests.ConnectionError, requests.Timeout) as exc:
            logger.warning(
                "[http] proxy %s: connection error (%s); trying next proxy",
                proxy_url, exc,
            )
            _record_failure(proxy_url, cooldown_seconds)
            continue

        if resp.status_code == 429:
            logger.warning(
                "[http] proxy %s: 429 rate-limited; trying next proxy",
                proxy_url,
            )
            _record_failure(proxy_url, cooldown_seconds)
            last_response = resp
            continue

        # 2xx/3xx/other 4xx/5xx: return to the caller. Retry-on-5xx is
        # handled by retry_with_backoff one level up.
        return resp

    # All proxies exhausted — degrade to a direct call rather than
    # take the scraper offline.
    logger.warning(
        "[http] all %d proxies for %s failed or in cooldown; "
        "falling back to direct connection",
        len(proxies), hostname,
    )
    return requests.get(url, timeout=timeout, headers=headers, **kwargs)


def pick_proxy_server(hostname: str) -> Optional[str]:
    """
    Return the first non-cooldown proxy URL for ``hostname``, or ``None``
    if none are configured / available.

    Used by the Playwright helper in :mod:`scraper_js` to seed a browser
    context's ``proxy={"server": ...}`` kwarg. The 429 cooldown loop is
    only wired up for the ``requests`` path — see the TODO in
    ``scraper_js.py``.
    """
    proxies, _cooldown = _proxies_for_host(hostname)
    if not proxies:
        return None
    now = time.time()
    for proxy_url in proxies:
        if not _is_in_cooldown(proxy_url, now):
            return proxy_url
    return None
