"""
raw_html_archive_writer.py — Idempotent insert for ``raw_html_archive``.

See lib/db/src/schema/scrape-health.ts for the table shape. The unique
index on ``sha256`` is the dedup guard: the same HTML bytes (which hash
to the same sha256) can be fed in many times; ``ON CONFLICT (sha256)
DO NOTHING`` means the second call is a no-op.

Caller owns the psycopg2 connection + transaction. This module only
exposes a single cursor-taking helper so it composes cleanly with the
rest of the ingest layer (see matches_writer.py for the same pattern).
"""

from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger("raw_html_archive_writer")


_INSERT_SQL = """
INSERT INTO raw_html_archive (
    scrape_run_log_id, source_url, sha256, bucket_path, content_bytes
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (sha256) DO NOTHING
"""


def insert_raw_html_archive_row(
    cur,
    *,
    scrape_run_log_id: Optional[int],
    source_url: str,
    sha256: str,
    bucket_path: str,
    content_bytes: int,
) -> None:
    """
    Insert one row into ``raw_html_archive``. No-ops if ``sha256`` already
    exists (the unique index makes the insert a conflict).

    Parameters
    ----------
    cur:
        Open psycopg2 cursor. Commit/rollback is the caller's
        responsibility.
    scrape_run_log_id:
        Optional integer FK to ``scrape_run_logs.id`` identifying the
        owning scrape run. ``None`` is stored as SQL ``NULL`` and is
        expected whenever the fetch isn't inside a tracked run (e.g.
        ad-hoc extractor calls).
    source_url:
        Final URL the HTML came from (after redirects).
    sha256:
        Hex-encoded sha256 of the *uncompressed* HTML bytes.
    bucket_path:
        Full Object Storage path, e.g.
        ``upshift-raw-html/2026/04/18/<sha>.html.gz``.
    content_bytes:
        Size of the gzipped blob that was uploaded.
    """
    cur.execute(
        _INSERT_SQL,
        (scrape_run_log_id, source_url, sha256, bucket_path, content_bytes),
    )
