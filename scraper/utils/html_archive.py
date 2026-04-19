"""
Raw HTML archival to Replit Object Storage.

Every successful HTTP fetch from the static scraper (``scraper_static.py``)
gets handed to :func:`archive_raw_html`. When enabled, the function:

1. Gzips the response body.
2. Computes the sha256 of the *uncompressed* bytes so the same HTML never
   produces two different archive rows.
3. Uploads the gzip blob to the Replit Object Storage bucket
   ``upshift-raw-html`` under the key ``YYYY/MM/DD/<sha256>.html.gz``.
4. Writes a row to the ``raw_html_archive`` Postgres table via
   :mod:`scraper.ingest.raw_html_archive_writer`, using
   ``ON CONFLICT (sha256) DO NOTHING`` so repeated fetches of the same
   page don't bloat the table.

Gating
------
Archival is off by default — it only runs when the environment variable
``ARCHIVE_RAW_HTML_ENABLED`` is set to the string ``"true"``. Any other
value (including unset, ``"false"``, ``"1"``, etc.) makes this module a
no-op. That keeps local dev and CI runs from needing Object Storage
credentials.

Failure policy
--------------
The archive path is *strictly defensive*: if the Replit Object Storage
package isn't installed, or bucket initialisation fails, or an upload
errors out, we log a single warning and latch an in-module disabled
flag. Subsequent calls return ``None`` silently. Scraping itself never
fails because archival failed.

Playwright / JS scraper
-----------------------
This module is only wired into the static scraper. Wiring it into
``scraper_js.py`` is intentionally deferred — ``page.content()`` gives
you the rendered HTML but doing it without doubling per-page memory
overhead is a separate design problem. See the TODO in
``scraper_js.py``.
"""

from __future__ import annotations

import gzip
import hashlib
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("html_archive")


_BUCKET_NAME = "upshift-raw-html"

# Process-wide state, protected by the lock. Lazily initialised on
# first call to archive_raw_html().
_CLIENT_LOCK = threading.Lock()
_CLIENT: Optional[Any] = None
_BUCKET: Optional[Any] = None
_DISABLED = False  # latches True after init failure


def _is_enabled() -> bool:
    """Return True iff the ``ARCHIVE_RAW_HTML_ENABLED`` env flag is set."""
    return os.environ.get("ARCHIVE_RAW_HTML_ENABLED", "").strip().lower() == "true"


def _reset_for_tests() -> None:
    """Clear module-level cached client state. Test-only helper."""
    global _CLIENT, _BUCKET, _DISABLED
    with _CLIENT_LOCK:
        _CLIENT = None
        _BUCKET = None
        _DISABLED = False


def _init_client() -> bool:
    """
    Initialise the Replit Object Storage client on first call.

    Returns True if the client is ready, False if the module has been
    latched disabled (missing package, bucket init error, etc.). The
    warning is logged exactly once per process.
    """
    global _CLIENT, _BUCKET, _DISABLED

    if _DISABLED:
        return False
    if _CLIENT is not None and _BUCKET is not None:
        return True

    with _CLIENT_LOCK:
        if _DISABLED:
            return False
        if _CLIENT is not None and _BUCKET is not None:
            return True

        try:
            # The Replit package is only installed in the Replit runtime.
            # Local dev + CI will fall through to the ImportError handler.
            from replit import object_storage  # type: ignore
        except ImportError:
            log.warning(
                "[html_archive] replit.object_storage package not available; "
                "raw HTML archival disabled for this process."
            )
            _DISABLED = True
            return False
        except Exception as exc:  # pragma: no cover — defensive
            log.warning(
                "[html_archive] replit package import failed (%s); "
                "raw HTML archival disabled for this process.",
                exc,
            )
            _DISABLED = True
            return False

        try:
            client = object_storage.Client()
            # The Replit SDK exposes buckets through the client. The
            # exact attribute shape has varied over minor releases, so
            # we accept either ``.Bucket(name)`` or ``.bucket(name)``.
            # If neither exists we treat the client itself as the
            # bucket-like object (current SDK default behaviour).
            if hasattr(client, "Bucket"):
                bucket = client.Bucket(_BUCKET_NAME)
            elif hasattr(client, "bucket"):
                bucket = client.bucket(_BUCKET_NAME)
            else:
                bucket = client
        except Exception as exc:
            log.warning(
                "[html_archive] Replit Object Storage client init failed "
                "(%s); raw HTML archival disabled for this process.",
                exc,
            )
            _DISABLED = True
            return False

        _CLIENT = client
        _BUCKET = bucket
        return True


def _sha256_exists_in_db(sha256: str) -> bool:
    """
    Return True if ``sha256`` is already recorded in ``raw_html_archive``.

    Defensive: any DB error (no DATABASE_URL, psycopg2 not installed,
    network hiccup) returns False so we fall through to attempting the
    upload + write. The unique index on sha256 + ``ON CONFLICT DO
    NOTHING`` in the writer keeps the end state correct even if this
    pre-check is wrong.
    """
    try:
        import psycopg2  # type: ignore
    except ImportError:  # pragma: no cover — Replit has psycopg2
        return False

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        return False

    try:
        conn = psycopg2.connect(dsn)
    except Exception:  # pragma: no cover — network / auth
        return False

    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM raw_html_archive WHERE sha256 = %s LIMIT 1",
                (sha256,),
            )
            return cur.fetchone() is not None
    except Exception:
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _insert_archive_row(
    *,
    run_id: Optional[str],
    source_url: str,
    sha256: str,
    bucket_path: str,
    content_bytes: int,
) -> None:
    """
    Insert a row into ``raw_html_archive``. Swallows DB errors — the
    Object Storage blob is the source of truth; missing a DB row is
    recoverable by a future reconcile.
    """
    try:
        import psycopg2  # type: ignore
    except ImportError:  # pragma: no cover
        return

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        return

    try:
        conn = psycopg2.connect(dsn)
    except Exception:  # pragma: no cover
        return

    try:
        from ingest.raw_html_archive_writer import insert_raw_html_archive_row
    except Exception:
        # Fall back to inline insert if the writer module cannot be
        # imported (e.g. sys.path weirdness in a test harness).
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw_html_archive
                        (run_id, source_url, sha256, bucket_path, content_bytes)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (sha256) DO NOTHING
                    """,
                    (run_id, source_url, sha256, bucket_path, content_bytes),
                )
        except Exception as exc:
            log.warning("[html_archive] DB insert failed: %s", exc)
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return

    try:
        with conn, conn.cursor() as cur:
            insert_raw_html_archive_row(
                cur,
                run_id=run_id,
                source_url=source_url,
                sha256=sha256,
                bucket_path=bucket_path,
                content_bytes=content_bytes,
            )
    except Exception as exc:
        log.warning("[html_archive] DB insert failed: %s", exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _upload_blob(bucket: Any, key: str, gz_bytes: bytes) -> bool:
    """
    Upload ``gz_bytes`` to ``bucket`` under ``key``. Returns True on
    success, False on any error (logged as warning, not raised).

    The Replit Object Storage SDK has shifted method names across minor
    releases; we try the common variants.
    """
    content_type = "application/gzip"

    # Preferred API: bucket.upload_from_bytes(key, data, content_type=...)
    uploader = getattr(bucket, "upload_from_bytes", None)
    if callable(uploader):
        try:
            try:
                uploader(key, gz_bytes, content_type=content_type)
            except TypeError:
                # Older SDKs may not accept content_type kwarg.
                uploader(key, gz_bytes)
            return True
        except Exception as exc:
            log.warning("[html_archive] upload_from_bytes failed: %s", exc)
            return False

    # Fallback: client-level `upload_from_bytes(key, data)` (no bucket object).
    client_uploader = getattr(bucket, "put", None) or getattr(bucket, "write", None)
    if callable(client_uploader):
        try:
            client_uploader(key, gz_bytes)
            return True
        except Exception as exc:
            log.warning("[html_archive] blob write failed: %s", exc)
            return False

    log.warning(
        "[html_archive] Replit Object Storage SDK surface unrecognised; "
        "no known upload method on bucket object."
    )
    return False


def _download_blob(bucket: Any, key: str) -> Optional[bytes]:
    """
    Download a blob by key from ``bucket``. Returns raw gzip bytes on
    success, ``None`` when no known download method exists on the SDK
    surface. Any SDK-level exception is re-raised to the caller — unlike
    the upload path, replay fetch must fail loud so the caller can tell
    a missing blob apart from a bug.
    """
    # Preferred API: bucket.download_as_bytes(key) → bytes.
    for attr in ("download_as_bytes", "download_bytes", "read", "get"):
        fn = getattr(bucket, attr, None)
        if callable(fn):
            return fn(key)
    return None


def fetch_archived_html(sha256: str) -> Optional[str]:
    """
    Fetch the archived HTML for a given sha256 from Replit Object Storage.

    This is the inverse of :func:`archive_raw_html`: given the content hash
    of a previously archived page, look up the bucket path in
    ``raw_html_archive``, download the gzipped blob, decompress, and
    return the uncompressed HTML string.

    Parameters
    ----------
    sha256:
        Hex-encoded sha256 of the uncompressed HTML bytes. This is the
        same value stored in ``raw_html_archive.sha256`` and embedded in
        ``raw_html_archive.bucket_path``.

    Returns
    -------
    str
        Decoded (UTF-8) HTML content on success. Raises otherwise —
        replay code MUST fail loud rather than silently return ``None``
        on a missing or corrupt blob, because a parse run over partial
        data is worse than no run at all.

    Raises
    ------
    RuntimeError
        ``ARCHIVE_RAW_HTML_ENABLED`` is not ``"true"`` (replay requires
        archival to be turned on).
    RuntimeError
        Replit Object Storage SDK could not be initialised (package
        missing, credentials absent, etc.).
    LookupError
        ``raw_html_archive`` has no row matching ``sha256``.
    RuntimeError
        Blob download or gzip decompression failed.
    """
    if not _is_enabled():
        raise RuntimeError(
            "fetch_archived_html: ARCHIVE_RAW_HTML_ENABLED is not 'true'; "
            "replay requires archival to be enabled so the bucket client "
            "can be initialised."
        )

    if not _init_client():
        raise RuntimeError(
            "fetch_archived_html: Replit Object Storage client failed to "
            "initialise (see prior warning); cannot fetch archived HTML."
        )

    # Look up the bucket path + run_id from the DB. The bucket path is
    # authoritative — we never reconstruct it from sha256 + archived_at
    # because the key-prefix format may change over time.
    try:
        import psycopg2  # type: ignore
    except ImportError as exc:  # pragma: no cover — Replit has psycopg2
        raise RuntimeError(
            "fetch_archived_html: psycopg2 is required to look up the "
            "bucket path from raw_html_archive."
        ) from exc

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "fetch_archived_html: DATABASE_URL is not set; cannot look up "
            "the bucket path for sha256."
        )

    try:
        conn = psycopg2.connect(dsn)
    except Exception as exc:
        raise RuntimeError(
            f"fetch_archived_html: failed to connect to DATABASE_URL "
            f"({exc!s})"
        ) from exc

    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT bucket_path FROM raw_html_archive "
                "WHERE sha256 = %s LIMIT 1",
                (sha256,),
            )
            row = cur.fetchone()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not row:
        raise LookupError(
            f"fetch_archived_html: no raw_html_archive row for "
            f"sha256={sha256}"
        )

    bucket_path = row[0]

    # bucket_path is stored as "upshift-raw-html/YYYY/MM/DD/<sha>.html.gz"
    # but the Replit bucket client takes the key relative to the bucket.
    key = bucket_path
    prefix = f"{_BUCKET_NAME}/"
    if key.startswith(prefix):
        key = key[len(prefix):]

    assert _BUCKET is not None  # guaranteed by _init_client() above
    try:
        gz_bytes = _download_blob(_BUCKET, key)
    except Exception as exc:
        raise RuntimeError(
            f"fetch_archived_html: blob download failed for key={key!r} "
            f"({exc!s})"
        ) from exc

    if gz_bytes is None:
        raise RuntimeError(
            "fetch_archived_html: Replit Object Storage SDK surface "
            "unrecognised; no known download method on bucket object."
        )
    if not isinstance(gz_bytes, (bytes, bytearray)):
        raise RuntimeError(
            f"fetch_archived_html: expected bytes from bucket download, "
            f"got {type(gz_bytes).__name__}"
        )

    try:
        raw_bytes = gzip.decompress(bytes(gz_bytes))
    except Exception as exc:
        raise RuntimeError(
            f"fetch_archived_html: gzip decompression failed for "
            f"sha256={sha256} (key={key!r}): {exc!s}"
        ) from exc

    # Integrity check — the sha256 we just downloaded should match the
    # one we were asked for. A mismatch means the bucket blob is corrupt
    # or the DB row points at the wrong key.
    actual_sha = hashlib.sha256(raw_bytes).hexdigest()
    if actual_sha != sha256:
        raise RuntimeError(
            f"fetch_archived_html: sha256 mismatch for key={key!r} — "
            f"expected {sha256}, got {actual_sha}"
        )

    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeError(
            f"fetch_archived_html: failed to decode UTF-8 for "
            f"sha256={sha256}: {exc!s}"
        ) from exc


def archive_raw_html(
    source_url: str,
    html: str,
    run_id: Optional[str] = None,
) -> Optional[dict]:
    """
    Archive ``html`` for ``source_url`` to Replit Object Storage and
    record it in ``raw_html_archive``.

    Parameters
    ----------
    source_url:
        Final URL the HTML was fetched from (post-redirect).
    html:
        Uncompressed response body as text. Encoded to UTF-8 before
        hashing + compressing.
    run_id:
        Optional UUID string tying this archive row to a logical scrape
        run. Pass ``None`` when the fetch isn't inside a tracked run
        (e.g. ad-hoc extractor calls).

    Returns
    -------
    dict | None
        ``{"sha256", "bucket_path", "content_bytes"}`` on success; ``None``
        when archival is disabled, the SDK is missing, or any step
        failed. Callers should treat ``None`` as "archival didn't happen,
        but scraping continues" — never raise on ``None``.
    """
    if not _is_enabled():
        return None

    if not _init_client():
        return None

    # Hash the uncompressed bytes — gzip output is not deterministic
    # across library versions (headers, timestamp), but the payload is.
    raw_bytes = html.encode("utf-8")
    sha256 = hashlib.sha256(raw_bytes).hexdigest()

    # Key layout: upshift-raw-html/YYYY/MM/DD/<sha>.html.gz — bucket name
    # is already implied by the Object Storage client, so bucket_path
    # stored in the DB row includes the bucket name for clarity.
    now = datetime.now(timezone.utc)
    key = f"{now.year:04d}/{now.month:02d}/{now.day:02d}/{sha256}.html.gz"
    bucket_path = f"{_BUCKET_NAME}/{key}"

    # Skip upload if we've already archived this exact HTML. The DB's
    # unique index on sha256 is the authoritative guard; this check just
    # avoids a redundant Object Storage PUT.
    if _sha256_exists_in_db(sha256):
        log.debug("[html_archive] sha256 %s already archived; skipping upload", sha256[:12])
        return None

    gz_bytes = gzip.compress(raw_bytes)
    content_bytes = len(gz_bytes)

    assert _BUCKET is not None  # guaranteed by _init_client()
    ok = _upload_blob(_BUCKET, key, gz_bytes)
    if not ok:
        return None

    _insert_archive_row(
        run_id=run_id,
        source_url=source_url,
        sha256=sha256,
        bucket_path=bucket_path,
        content_bytes=content_bytes,
    )

    return {
        "sha256": sha256,
        "bucket_path": bucket_path,
        "content_bytes": content_bytes,
    }
