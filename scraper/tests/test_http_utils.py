"""
Tests for scraper/utils/http.py — proxy rotation + cooldown behaviour.

The module ships with an empty proxy pool, so the most important test
is that the empty-pool path calls ``requests.get`` directly with no
``proxies`` kwarg. The remaining tests pin down what happens once a
provider's credentials are added.

Run:
    python -m pytest scraper/tests/test_http_utils.py -v
"""

from __future__ import annotations

import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests

# Make "utils.http" importable exactly the way scraper code imports it.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils import http as http_mod  # noqa: E402


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_http_state():
    """Each test starts with a clean config + cooldown state."""
    http_mod._reset_config_cache_for_tests()
    yield
    http_mod._reset_config_cache_for_tests()


def _set_config(monkeypatch, yaml_text: str, tmp_path):
    """Write ``yaml_text`` into a temp file and point the module at it."""
    cfg_path = tmp_path / "proxy_config.yaml"
    cfg_path.write_text(yaml_text, encoding="utf-8")
    monkeypatch.setattr(http_mod, "_CONFIG_PATH", str(cfg_path))


def _ok_response(status: int = 200) -> requests.Response:
    resp = requests.Response()
    resp.status_code = status
    return resp


# --------------------------------------------------------------------------
# Test 1 — empty pool → direct request, no `proxies` kwarg
# --------------------------------------------------------------------------

def test_empty_config_makes_direct_request(monkeypatch, tmp_path):
    _set_config(monkeypatch, "domains: {}\n", tmp_path)

    with patch.object(http_mod.requests, "get", return_value=_ok_response()) as mock_get:
        resp = http_mod.get("https://example.com/path", timeout=10)

    assert resp.status_code == 200
    assert mock_get.call_count == 1
    _args, kwargs = mock_get.call_args
    assert "proxies" not in kwargs, (
        f"empty pool must not pass a proxies kwarg, got: {kwargs}"
    )
    # Sanity: kwargs the caller actually asked for are preserved.
    assert kwargs["timeout"] == 10


def test_domain_with_no_proxy_entry_is_direct(monkeypatch, tmp_path):
    """A host not listed in `domains:` gets direct treatment."""
    yaml_text = (
        "domains:\n"
        "  other.example.com:\n"
        "    proxies:\n"
        "      - http://user:pass@proxy.example:8080\n"
    )
    _set_config(monkeypatch, yaml_text, tmp_path)

    with patch.object(http_mod.requests, "get", return_value=_ok_response()) as mock_get:
        http_mod.get("https://unlisted.example.com/path")

    _args, kwargs = mock_get.call_args
    assert "proxies" not in kwargs


# --------------------------------------------------------------------------
# Test 2 — one configured proxy → request sent through it
# --------------------------------------------------------------------------

def test_single_proxy_is_used(monkeypatch, tmp_path):
    yaml_text = (
        "domains:\n"
        "  target.example.com:\n"
        "    proxies:\n"
        "      - http://user:pass@proxy.example:8080\n"
    )
    _set_config(monkeypatch, yaml_text, tmp_path)

    with patch.object(http_mod.requests, "get", return_value=_ok_response()) as mock_get:
        resp = http_mod.get("https://target.example.com/foo")

    assert resp.status_code == 200
    assert mock_get.call_count == 1
    _args, kwargs = mock_get.call_args
    assert kwargs["proxies"] == {
        "http": "http://user:pass@proxy.example:8080",
        "https": "http://user:pass@proxy.example:8080",
    }


# --------------------------------------------------------------------------
# Test 3 — 3× 429 trips cooldown; next call goes direct
# --------------------------------------------------------------------------

def test_three_429s_trigger_cooldown_and_fall_back_to_direct(monkeypatch, tmp_path):
    yaml_text = (
        "domains:\n"
        "  target.example.com:\n"
        "    proxies:\n"
        "      - http://user:pass@proxy.example:8080\n"
        "    cooldown_seconds: 300\n"
    )
    _set_config(monkeypatch, yaml_text, tmp_path)

    proxy_url = "http://user:pass@proxy.example:8080"

    def _fake_get(url, **kwargs):
        # Proxy path returns 429; direct path returns 200.
        proxies = kwargs.get("proxies")
        if proxies and proxies.get("https") == proxy_url:
            return _ok_response(429)
        return _ok_response(200)

    with patch.object(http_mod.requests, "get", side_effect=_fake_get) as mock_get:
        # First three calls all hit the proxy and get 429 back.
        for _ in range(3):
            r = http_mod.get("https://target.example.com/x")
            # Once all proxies are exhausted, we fall through to direct.
            # The fallback returns 200 in this test harness.
            assert r.status_code == 200

        # After 3 failures, the proxy is in cooldown. Call #4 should
        # skip the proxy entirely and go straight to direct — i.e. no
        # call with a `proxies` kwarg happens on this invocation.
        calls_before = mock_get.call_count
        r = http_mod.get("https://target.example.com/x")
        calls_after = mock_get.call_count

        assert r.status_code == 200
        new_calls = mock_get.call_args_list[calls_before:calls_after]
        assert len(new_calls) == 1, (
            f"expected exactly one call after cooldown, got {new_calls}"
        )
        _args, kwargs = new_calls[0]
        assert "proxies" not in kwargs, (
            "once the proxy is in cooldown the next call must be direct, "
            f"but got proxies kwarg: {kwargs.get('proxies')}"
        )


def test_connection_errors_also_trigger_cooldown(monkeypatch, tmp_path):
    """Same threshold logic applies to connection errors, not just 429s."""
    yaml_text = (
        "domains:\n"
        "  target.example.com:\n"
        "    proxies:\n"
        "      - http://user:pass@proxy.example:8080\n"
    )
    _set_config(monkeypatch, yaml_text, tmp_path)

    proxy_url = "http://user:pass@proxy.example:8080"
    call_counter = {"proxy": 0, "direct": 0}

    def _fake_get(url, **kwargs):
        proxies = kwargs.get("proxies")
        if proxies and proxies.get("https") == proxy_url:
            call_counter["proxy"] += 1
            raise requests.ConnectionError("simulated")
        call_counter["direct"] += 1
        return _ok_response(200)

    with patch.object(http_mod.requests, "get", side_effect=_fake_get):
        for _ in range(3):
            http_mod.get("https://target.example.com/x")

        # Cooldown engaged; next call must bypass the proxy.
        proxy_before = call_counter["proxy"]
        http_mod.get("https://target.example.com/x")
        assert call_counter["proxy"] == proxy_before, (
            "proxy should not have been attempted after cooldown engaged"
        )


# --------------------------------------------------------------------------
# Test 4 — malformed YAML → warn and fall back to direct
# --------------------------------------------------------------------------

def test_malformed_yaml_falls_back_to_direct(monkeypatch, tmp_path, caplog):
    import logging as py_logging

    # Deliberately broken YAML — unbalanced brackets.
    _set_config(monkeypatch, "domains: { unbalanced\n", tmp_path)
    caplog.set_level(py_logging.WARNING, logger="utils.http")

    with patch.object(http_mod.requests, "get", return_value=_ok_response()) as mock_get:
        resp = http_mod.get("https://anything.example.com/")

    assert resp.status_code == 200
    _args, kwargs = mock_get.call_args
    assert "proxies" not in kwargs, (
        "broken YAML must degrade to direct request"
    )

    warned = [
        r for r in caplog.records
        if "malformed" in r.getMessage().lower()
        or "unexpected shape" in r.getMessage().lower()
    ]
    assert warned, (
        f"expected a malformed-YAML warning, got records: "
        f"{[r.getMessage() for r in caplog.records]}"
    )


def test_missing_yaml_file_falls_back_to_direct(monkeypatch, tmp_path):
    """A non-existent config path is also handled gracefully."""
    monkeypatch.setattr(
        http_mod,
        "_CONFIG_PATH",
        str(tmp_path / "definitely-does-not-exist.yaml"),
    )

    with patch.object(http_mod.requests, "get", return_value=_ok_response()) as mock_get:
        resp = http_mod.get("https://anything.example.com/")

    assert resp.status_code == 200
    _args, kwargs = mock_get.call_args
    assert "proxies" not in kwargs


# --------------------------------------------------------------------------
# Test 5 — pick_proxy_server helper (used by Playwright)
# --------------------------------------------------------------------------

def test_pick_proxy_server_returns_none_on_empty_pool(monkeypatch, tmp_path):
    _set_config(monkeypatch, "domains: {}\n", tmp_path)
    assert http_mod.pick_proxy_server("target.example.com") is None


def test_pick_proxy_server_returns_configured_proxy(monkeypatch, tmp_path):
    yaml_text = (
        "domains:\n"
        "  target.example.com:\n"
        "    proxies:\n"
        "      - http://p1.example:8080\n"
        "      - http://p2.example:8080\n"
    )
    _set_config(monkeypatch, yaml_text, tmp_path)
    assert http_mod.pick_proxy_server("target.example.com") == "http://p1.example:8080"
