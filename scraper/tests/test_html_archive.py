"""
Tests for scraper/utils/html_archive.py.

Covers the four gating + failure paths the scraper depends on:

1. ``ARCHIVE_RAW_HTML_ENABLED`` unset → no-op, no Object Storage call.
2. Flag set but client init raises → single warning, latched disabled,
   subsequent calls return ``None`` silently.
3. Flag set + successful upload → returns the expected sha256 +
   bucket_path dict.
4. sha256 collision (same HTML twice) → second call skips the upload.

The Replit Object Storage SDK is mocked — we never touch the real
service from tests. DB writes are also mocked out (the writer is
covered separately and the archive module's only job is to call it
with the right args; we assert via the mocked psycopg2 path).

Run:
    python -m pytest scraper/tests/test_html_archive.py -v
"""

from __future__ import annotations

import hashlib
import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils import html_archive  # noqa: E402


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_state(monkeypatch):
    """Clear module-level caches + env before/after each test."""
    monkeypatch.delenv("ARCHIVE_RAW_HTML_ENABLED", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    html_archive._reset_for_tests()
    yield
    html_archive._reset_for_tests()


def _install_fake_replit_sdk(bucket: MagicMock) -> types.ModuleType:
    """
    Install a fake ``replit.object_storage`` module so
    ``from replit import object_storage`` succeeds with our mock
    client. Returns the fake module for inspection.
    """
    fake_client = MagicMock()
    fake_client.Bucket = MagicMock(return_value=bucket)

    object_storage_mod = types.ModuleType("replit.object_storage")
    object_storage_mod.Client = MagicMock(return_value=fake_client)  # type: ignore[attr-defined]

    replit_mod = types.ModuleType("replit")
    replit_mod.object_storage = object_storage_mod  # type: ignore[attr-defined]

    sys.modules["replit"] = replit_mod
    sys.modules["replit.object_storage"] = object_storage_mod
    return replit_mod


def _cleanup_fake_replit_sdk():
    sys.modules.pop("replit.object_storage", None)
    sys.modules.pop("replit", None)


# --------------------------------------------------------------------------
# Test 1 — flag unset → no-op
# --------------------------------------------------------------------------

def test_archive_disabled_when_flag_unset():
    """
    With ARCHIVE_RAW_HTML_ENABLED unset, the module is a hard no-op.
    Object Storage is never touched even if the SDK is importable.
    """
    # Even if a fake SDK is around, the gating check runs first.
    bucket = MagicMock()
    _install_fake_replit_sdk(bucket)
    try:
        result = html_archive.archive_raw_html(
            "https://example.com/page",
            "<html><body>hi</body></html>",
        )
        assert result is None
        bucket.upload_from_bytes.assert_not_called()
    finally:
        _cleanup_fake_replit_sdk()


def test_archive_disabled_when_flag_set_to_false(monkeypatch):
    """Only the literal string 'true' enables archival."""
    monkeypatch.setenv("ARCHIVE_RAW_HTML_ENABLED", "false")

    bucket = MagicMock()
    _install_fake_replit_sdk(bucket)
    try:
        result = html_archive.archive_raw_html("https://x", "<html/>")
        assert result is None
        bucket.upload_from_bytes.assert_not_called()
    finally:
        _cleanup_fake_replit_sdk()


# --------------------------------------------------------------------------
# Test 2 — client init failure latches + subsequent calls silent
# --------------------------------------------------------------------------

def test_client_init_failure_latches_disabled(monkeypatch, caplog):
    """
    If the Replit SDK package is missing / client init raises, we log
    a single warning, latch ``_DISABLED = True``, and every subsequent
    call returns ``None`` silently (no duplicate warnings).
    """
    import logging as py_logging
    monkeypatch.setenv("ARCHIVE_RAW_HTML_ENABLED", "true")

    # Install a fake 'replit' package whose Client() constructor raises.
    object_storage_mod = types.ModuleType("replit.object_storage")

    def _boom(*_a, **_kw):
        raise RuntimeError("credentials missing")

    object_storage_mod.Client = _boom  # type: ignore[attr-defined]
    replit_mod = types.ModuleType("replit")
    replit_mod.object_storage = object_storage_mod  # type: ignore[attr-defined]
    sys.modules["replit"] = replit_mod
    sys.modules["replit.object_storage"] = object_storage_mod

    caplog.set_level(py_logging.WARNING, logger="html_archive")

    try:
        r1 = html_archive.archive_raw_html("https://x", "<html/>")
        r2 = html_archive.archive_raw_html("https://y", "<body/>")
        r3 = html_archive.archive_raw_html("https://z", "<p/>")
    finally:
        _cleanup_fake_replit_sdk()

    assert r1 is None
    assert r2 is None
    assert r3 is None

    # Exactly one warning — the latched flag prevents repeats.
    init_warnings = [
        rec for rec in caplog.records
        if "disabled for this process" in rec.getMessage()
    ]
    assert len(init_warnings) == 1, (
        f"expected exactly 1 init warning, got {len(init_warnings)}: "
        f"{[r.getMessage() for r in init_warnings]}"
    )


def test_client_init_failure_when_sdk_missing(monkeypatch, caplog):
    """Missing 'replit' package path — same latch behaviour."""
    import logging as py_logging
    monkeypatch.setenv("ARCHIVE_RAW_HTML_ENABLED", "true")

    # Ensure no fake SDK is registered. If the real replit-object-storage
    # package is installed (which it is on Replit, by design), force the
    # import to fail by stubbing sys.modules entries to None — Python
    # treats a None entry as "import explicitly disabled" and raises
    # ImportError on `from replit import object_storage`.
    _cleanup_fake_replit_sdk()
    monkeypatch.setitem(sys.modules, "replit", None)
    monkeypatch.setitem(sys.modules, "replit.object_storage", None)

    caplog.set_level(py_logging.WARNING, logger="html_archive")

    r1 = html_archive.archive_raw_html("https://x", "<html/>")
    r2 = html_archive.archive_raw_html("https://y", "<html/>")

    assert r1 is None
    assert r2 is None

    msgs = [rec.getMessage() for rec in caplog.records]
    assert any("disabled for this process" in m for m in msgs), (
        f"expected a disabled-process warning, got: {msgs}"
    )


# --------------------------------------------------------------------------
# Test 3 — happy path: upload returns the expected sha256 + bucket path
# --------------------------------------------------------------------------

def test_archive_success_returns_sha256_and_bucket_path(monkeypatch):
    monkeypatch.setenv("ARCHIVE_RAW_HTML_ENABLED", "true")

    bucket = MagicMock()
    bucket.upload_from_bytes = MagicMock(return_value=None)
    _install_fake_replit_sdk(bucket)

    html = "<html><body>hello world</body></html>"
    expected_sha = hashlib.sha256(html.encode("utf-8")).hexdigest()

    # Stub the DB dedup check to "nothing archived yet" and the insert to
    # a plain no-op; the scraper mustn't need a live DB connection for
    # archival to succeed from Object Storage's point of view.
    monkeypatch.setattr(
        html_archive,
        "_sha256_exists_in_db",
        lambda _sha: False,
    )
    monkeypatch.setattr(
        html_archive,
        "_insert_archive_row",
        lambda **_kw: None,
    )

    try:
        result = html_archive.archive_raw_html(
            "https://example.com/page",
            html,
            scrape_run_log_id=42,
        )
    finally:
        _cleanup_fake_replit_sdk()

    assert result is not None
    assert result["sha256"] == expected_sha
    assert result["bucket_path"].startswith("upshift-raw-html/")
    assert result["bucket_path"].endswith(f"/{expected_sha}.html.gz")
    # Gzip compression is non-empty and smaller than the raw bytes for
    # this payload. We don't pin an exact size because gzip headers
    # vary by library version.
    assert result["content_bytes"] > 0

    # Object Storage was called exactly once with the right kwargs.
    assert bucket.upload_from_bytes.call_count == 1
    args, kwargs = bucket.upload_from_bytes.call_args
    key = args[0]
    payload = args[1]
    assert key.endswith(f"{expected_sha}.html.gz")
    # Payload is gzipped (magic bytes 1f 8b).
    assert payload[:2] == b"\x1f\x8b"
    # content_type kwarg is application/gzip when the SDK accepts it.
    assert kwargs.get("content_type") == "application/gzip"


# --------------------------------------------------------------------------
# Test 4 — sha256 collision skips re-upload
# --------------------------------------------------------------------------

def test_duplicate_sha256_skips_upload(monkeypatch):
    """
    Second call with the same HTML must not re-upload to Object Storage.
    The sha256 pre-check simulates the "already in raw_html_archive" case.
    """
    monkeypatch.setenv("ARCHIVE_RAW_HTML_ENABLED", "true")

    bucket = MagicMock()
    bucket.upload_from_bytes = MagicMock(return_value=None)
    _install_fake_replit_sdk(bucket)

    html = "<html><body>dup</body></html>"

    # First call: pretend the DB has no record. Second call: pretend the
    # first call's insert went through, so the pre-check now says True.
    seen: dict[str, bool] = {}

    def _fake_exists(sha: str) -> bool:
        if sha in seen:
            return True
        seen[sha] = True
        return False

    monkeypatch.setattr(html_archive, "_sha256_exists_in_db", _fake_exists)
    monkeypatch.setattr(html_archive, "_insert_archive_row", lambda **_kw: None)

    try:
        r1 = html_archive.archive_raw_html("https://example.com/a", html)
        r2 = html_archive.archive_raw_html("https://example.com/a", html)
    finally:
        _cleanup_fake_replit_sdk()

    assert r1 is not None  # first call uploaded
    assert r2 is None       # second call skipped

    # Only one upload to Object Storage — the dedup pre-check worked.
    assert bucket.upload_from_bytes.call_count == 1
