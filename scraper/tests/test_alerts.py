"""Tests for scraper.alerts — failure webhook delivery."""

from __future__ import annotations

import json
import os
import sys
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Import after path fixup so the module resolves.
import alerts as alerts_mod
from alerts import alert_scraper_failure


class TestAlertNoopWithoutEnv:
    """No HTTP call when ALERT_WEBHOOK_URL is unset."""

    def test_alert_noop_without_env(self, monkeypatch):
        monkeypatch.setattr(alerts_mod, "ALERT_WEBHOOK_URL", None)
        with mock.patch("urllib.request.urlopen") as mock_urlopen:
            alert_scraper_failure(
                scraper_key="ecnl-boys",
                failure_kind="timeout",
                error_message="timed out after 30s",
            )
            mock_urlopen.assert_not_called()


class TestAlertFiresWebhook:
    """Webhook is called with correct JSON payload when URL is set."""

    def test_alert_fires_webhook(self, monkeypatch):
        monkeypatch.setattr(alerts_mod, "ALERT_WEBHOOK_URL", "https://hooks.example.com/test")
        with mock.patch("urllib.request.urlopen") as mock_urlopen:
            alert_scraper_failure(
                scraper_key="ecnl-boys",
                failure_kind="timeout",
                error_message="timed out after 30s",
                source_url="https://theecnl.com/teams",
                league_name="ECNL Boys National",
            )
            mock_urlopen.assert_called_once()
            req = mock_urlopen.call_args[0][0]
            body = json.loads(req.data.decode("utf-8"))
            assert body["event"] == "scraper_failure"
            assert body["scraper_key"] == "ecnl-boys"
            assert body["failure_kind"] == "timeout"
            assert body["error_message"] == "timed out after 30s"
            assert body["source_url"] == "https://theecnl.com/teams"
            assert body["league_name"] == "ECNL Boys National"
            assert "timestamp" in body
            assert req.get_header("Content-type") == "application/json"


class TestAlertSwallowsExceptions:
    """Webhook delivery failure must not propagate."""

    def test_alert_swallows_exceptions(self, monkeypatch):
        monkeypatch.setattr(alerts_mod, "ALERT_WEBHOOK_URL", "https://hooks.example.com/test")
        with mock.patch("urllib.request.urlopen", side_effect=Exception("connection refused")):
            # Must not raise
            alert_scraper_failure(
                scraper_key="ecnl-boys",
                failure_kind="network",
                error_message="DNS resolution failed",
            )
