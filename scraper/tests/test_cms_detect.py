"""
Tests for extractors.cms_detect.

Uses a tiny in-process FakeResponse rather than ``requests_mock`` so the
test runs without extra dev deps. Mirrors the ``.headers`` + ``.text``
shape that ``detect_cms`` reads.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.cms_detect import detect_cms  # noqa: E402


class FakeResponse:
    """Minimal stand-in for ``requests.Response``.

    ``detect_cms`` only reads ``.headers`` and ``.text`` — that's all we
    expose. Headers are a plain dict (caller can pass either casing;
    ``detect_cms`` checks both).
    """

    def __init__(self, *, headers: dict | None = None, text: str = ""):
        self.headers = headers or {}
        self.text = text


# --------------------------------------------------------------------------- header-based


def test_squarespace_via_server_header():
    resp = FakeResponse(headers={"Server": "Squarespace"}, text="<html></html>")
    assert detect_cms(resp) == "squarespace"


def test_sportsengine_via_powered_by_header():
    resp = FakeResponse(headers={"X-Powered-By": "ngin"}, text="<html></html>")
    assert detect_cms(resp) == "sportsengine"


def test_wix_via_x_wix_header():
    resp = FakeResponse(
        headers={"X-Wix-Request-Id": "abc123"}, text="<html></html>"
    )
    assert detect_cms(resp) == "wix"


def test_duda_via_server_header():
    resp = FakeResponse(headers={"Server": "Duda"}, text="<html></html>")
    assert detect_cms(resp) == "duda"


# --------------------------------------------------------------------------- HTML-signature fallback


def test_squarespace_via_html_signature():
    body = (
        "<html><head>"
        '<link rel="stylesheet" '
        'href="https://static1.squarespace.com/static/abc/site.css">'
        "</head></html>"
    )
    assert detect_cms(FakeResponse(text=body)) == "squarespace"


def test_sportsengine_via_html_signature():
    body = (
        "<html><body>"
        '<script src="https://www.sportngin.com/widgets/team.js"></script>'
        "</body></html>"
    )
    assert detect_cms(FakeResponse(text=body)) == "sportsengine"


def test_duda_via_html_signature():
    body = (
        "<html><head>"
        '<link rel="preconnect" href="https://irp.cdn-website.com/">'
        "</head></html>"
    )
    assert detect_cms(FakeResponse(text=body)) == "duda"


def test_wordpress_via_generator_meta():
    body = (
        '<html><head><meta name="generator" content="WordPress 6.4.2">'
        "</head></html>"
    )
    assert detect_cms(FakeResponse(text=body)) == "wordpress"


def test_wordpress_via_wp_content_path():
    body = (
        "<html><body>"
        '<img src="/wp-content/uploads/logo.png">'
        "</body></html>"
    )
    assert detect_cms(FakeResponse(text=body)) == "wordpress"


def test_wix_via_html_signature():
    body = (
        "<html><body>"
        '<img src="https://static.wixstatic.com/media/foo.png">'
        "</body></html>"
    )
    assert detect_cms(FakeResponse(text=body)) == "wix"


# --------------------------------------------------------------------------- negative + safety


def test_unknown_site_returns_none():
    body = "<html><body>just a plain HTML page</body></html>"
    assert detect_cms(FakeResponse(headers={"Server": "nginx"}, text=body)) is None


def test_empty_response_returns_none():
    assert detect_cms(FakeResponse()) is None


def test_missing_attributes_do_not_raise():
    """A degenerate object without .headers or .text → None, not crash."""

    class Bare:
        pass

    assert detect_cms(Bare()) is None


def test_header_precedence_over_html():
    """When both header and conflicting HTML signal fire, header wins."""
    body = '<link href="https://static1.squarespace.com/site.css">'
    resp = FakeResponse(headers={"Server": "Squarespace"}, text=body)
    # Both signals point to squarespace → still "squarespace".
    assert detect_cms(resp) == "squarespace"
