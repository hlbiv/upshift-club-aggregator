"""
odp_hubspot_pdf.py — Parser for ODP state-pool rosters published as
HubSpot-hosted PDFs.

As of the wave-3b seed sweep (2026-04-19) exactly one state uses this
platform: Cal North (`CA-N`). Cal North's annual "State Pool List"
letter is a single PDF at a ``calnorth.org/hubfs/...`` URL — publicly
downloadable, no auth. The PDF is:

- Page 1:  a cover letter (prose, no roster data).
- Pages 2-N: a three-column table repeating on every page::

        Pool    Last Name   First Name
        09B     Allen       Quinn
        09B     Bainotti    Alec
        ...
        14G     Zhu         Samantha

The ``Pool`` column is a compact code: two digits for the birth year
(e.g. ``09`` → 2009) and a trailing ``B`` or ``G`` for boys/girls.
We decode it into::

    graduation_year = 2000 + birth_year_two_digit + 18   # typical HS-sr year
    gender          = "B" | "G"
    age_group       = f"U{YYYY}"  # e.g. "U2009"  — birth-year based

``age_group`` is deliberately emitted as the birth-year string ("U2009")
rather than a U-# bucket (e.g. "U17") because Cal North keys their
program rosters by birth year directly; converting to U-# would
require knowing the reference season and is out of scope for this
parser — the runner/normalizer can do that downstream if needed.

Public surface
--------------
``parse_pdf(pdf_bytes, *, source_url, state, program_year) -> list[dict]``
    The primary entry point. Accepts raw PDF bytes and returns fully-
    stamped roster-entry dicts ready for ``ingest.odp_writer``.

``parse_html(html, *, source_url, state, program_year) -> list[dict]``
    Thin wrapper that expects ``html`` to actually be PDF bytes
    (either a ``bytes`` object or ``str`` containing latin-1-encodable
    PDF bytes). Exists so the shared ODP replay / batch-test
    machinery that funnels page bodies through ``parse_*`` functions
    continues to work. Web scrapers never fetch this extractor's
    URL as HTML, so ``parse_html`` is only for replay fixtures.

``register_downloader()`` / the module-level ``download_and_parse``
    Download-then-parse convenience that the runner calls when it
    sees ``platform: hubspot-pdf`` in the seed YAML. Encapsulates the
    HTTP retry wrapping so the runner stays platform-agnostic.

All parsers degrade silently — a shifted table layout returns ``[]``
with a logged warning, never raises.
"""

from __future__ import annotations

import io
import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pool-code decoding
# ---------------------------------------------------------------------------

# Cal North pool codes are exactly ``\d{2}[BG]`` — 2-digit birth year +
# gender. Codes like "09B" / "14G" repeat in column 0 of every data row.
_POOL_RE = re.compile(r"^\s*(\d{2})([BG])\s*$")

# Headers we recognize as table-of-contents / column-header rows; they
# must be rejected before the name heuristic.
_HEADER_TOKENS = {"pool", "last name", "first name", "name"}


def _clean(text: Optional[str]) -> str:
    if text is None:
        return ""
    return " ".join(str(text).split()).strip()


def _decode_pool_code(code: str) -> Optional[Dict[str, Any]]:
    """Decode a Cal North pool code like ``"09B"`` into structured fields.

    Returns ``None`` for anything that doesn't match — the runner
    will skip the row rather than emit garbage.
    """
    m = _POOL_RE.match(code or "")
    if not m:
        return None
    yy = int(m.group(1))
    gender_char = m.group(2).upper()
    # Two-digit Cal North years always refer to 20xx birth years —
    # the program only pools U10-U15 kids, so there's no 1990s roster.
    birth_year = 2000 + yy
    return {
        "birth_year": birth_year,
        "age_group": f"U{birth_year}",
        "gender": "B" if gender_char == "B" else "G",
    }


def _looks_like_name_token(token: str) -> bool:
    """A last-name / first-name cell must be non-empty, start with a
    letter, and contain only letters / hyphens / apostrophes / spaces.

    Cal North allows multi-word first names like "Maiella Jo Franzen"
    — so spaces are allowed inside name tokens.
    """
    t = (token or "").strip()
    if not t:
        return False
    # Cells that are obviously table-headers or prose should be filtered.
    if t.lower() in _HEADER_TOKENS:
        return False
    # First char must be a letter — weeds out numeric cells and rows
    # that leaked header whitespace.
    if not t[0].isalpha():
        return False
    # Permissive character class; rejects "Subtotal: 6", "TBD (2)", etc.
    if not re.match(r"^[A-Za-z][A-Za-z.'\- ]{0,60}$", t):
        return False
    return True


# ---------------------------------------------------------------------------
# Core parser
# ---------------------------------------------------------------------------


def parse_pdf(
    pdf_bytes: bytes,
    *,
    source_url: str,
    state: str,
    program_year: str,
) -> List[Dict[str, Any]]:
    """Parse a Cal North-style "State Pool List" PDF into roster entries.

    Parameters
    ----------
    pdf_bytes
        The raw bytes of the PDF document.
    source_url
        Canonical URL the PDF was fetched from. Stamped on every
        emitted row so the writer's ``source_url`` column is populated.
    state
        Two-letter (or dashed) state code from the seed YAML, e.g.
        ``"CA-N"``. Stamped on every row.
    program_year
        The season string from the seed YAML, e.g. ``"2025-26"``.

    Returns
    -------
    list[dict]
        One dict per player. Shape matches what ``ingest.odp_writer``
        expects. On any failure (unreadable PDF, empty page set,
        shifted columns) the list is empty and a warning is logged —
        this function never raises.
    """
    if not pdf_bytes:
        logger.warning("[odp-hubspot-pdf] empty pdf_bytes; nothing to parse")
        return []

    try:
        import pdfplumber  # type: ignore
    except ImportError:
        logger.error(
            "[odp-hubspot-pdf] pdfplumber is not installed; "
            "add it to scraper/requirements.txt and reinstall"
        )
        return []

    rows: List[Dict[str, Any]] = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                try:
                    page_tables = page.extract_tables() or []
                except Exception as exc:  # pragma: no cover — defensive
                    logger.warning(
                        "[odp-hubspot-pdf] table extraction failed on a page: %s",
                        exc,
                    )
                    continue
                for tbl in page_tables:
                    rows.extend(
                        _rows_from_table(
                            tbl,
                            source_url=source_url,
                            state=state,
                            program_year=program_year,
                        )
                    )
    except Exception as exc:
        logger.warning("[odp-hubspot-pdf] failed to open PDF: %s", exc)
        return []

    return rows


def _rows_from_table(
    tbl: List[List[Optional[str]]],
    *,
    source_url: str,
    state: str,
    program_year: str,
) -> List[Dict[str, Any]]:
    """Extract player rows from a single pdfplumber table.

    The table is three columns wide — pool / last / first. Header rows
    repeat on every page after the first, so we can't assume the first
    row is always a header. Instead we key on column 0: if it matches
    ``\\d{2}[BG]``, it's a data row; anything else is prose / header
    and is skipped.
    """
    rows: List[Dict[str, Any]] = []

    for raw_row in tbl:
        if not raw_row:
            continue
        cells = [_clean(c) for c in raw_row]

        # Expect at least three populated columns for a player row;
        # tolerate trailing empty cells that pdfplumber sometimes
        # introduces from ruled borders.
        while cells and cells[-1] == "":
            cells.pop()
        if len(cells) < 3:
            continue

        pool_cell = cells[0]
        decoded = _decode_pool_code(pool_cell)
        if decoded is None:
            # Non-data row (header / prose leak). Skip silently.
            continue

        last_name = cells[1]
        first_name = cells[2]
        if not _looks_like_name_token(last_name) or not _looks_like_name_token(first_name):
            continue

        player_name = f"{first_name} {last_name}"

        rows.append({
            "player_name": player_name,
            "first_name": first_name,
            "last_name": last_name,
            "graduation_year": None,  # Cal North PDF doesn't publish
            "position": None,
            "club_name_raw": None,    # cover letter is the only club-ish mention
            "state": state,
            "program_year": program_year,
            "age_group": decoded["age_group"],
            "gender": decoded["gender"],
            "birth_year": decoded["birth_year"],
            "source_url": source_url,
        })

    return rows


# ---------------------------------------------------------------------------
# parse_html shim — replay support
# ---------------------------------------------------------------------------


def parse_html(
    html: Any,
    *,
    source_url: str,
    state: str,
    program_year: str,
) -> List[Dict[str, Any]]:
    """Replay-compatibility shim.

    The rest of the ODP extractor family takes an HTML string via
    ``parse_odp_page(parser_key, html)``. This module's real input is
    PDF bytes, so we accept either:

    - ``bytes``  — treated as the PDF directly.
    - ``str``    — ``latin-1``-encoded back into bytes (the only
      lossless round-trip for binary PDF content through a text
      channel). Replay harnesses that stash fixture bodies as text
      can use this path.
    """
    if isinstance(html, bytes):
        pdf_bytes = html
    elif isinstance(html, str):
        try:
            pdf_bytes = html.encode("latin-1")
        except UnicodeEncodeError:
            logger.warning(
                "[odp-hubspot-pdf] parse_html got a str that isn't latin-1 "
                "encodable; cannot round-trip to PDF bytes"
            )
            return []
    else:
        logger.warning(
            "[odp-hubspot-pdf] parse_html got unexpected type %s",
            type(html).__name__,
        )
        return []

    return parse_pdf(
        pdf_bytes,
        source_url=source_url,
        state=state,
        program_year=program_year,
    )


# ---------------------------------------------------------------------------
# Download-and-parse convenience
# ---------------------------------------------------------------------------


def download_and_parse(
    url: str,
    *,
    state: str,
    program_year: str,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """Download the PDF at ``url`` and parse it.

    Uses ``utils.http.get`` so the per-domain proxy rotation + cooldown
    logic is shared with every other scraper. On any HTTP failure we
    return ``[]`` — the runner logs the failure separately via its
    own alerting hooks.
    """
    from utils import http as _http  # local import — keeps tests that
    # only exercise parse_pdf free of the requests dependency.
    from utils.retry import retry_with_backoff

    def _fetch_bytes() -> bytes:
        resp = _http.get(url, timeout=timeout, headers={
            "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)",
            "Accept": "application/pdf,*/*;q=0.8",
        })
        resp.raise_for_status()
        content = resp.content
        if not content:
            raise RuntimeError(f"empty body for {url}")
        return content

    try:
        pdf_bytes = retry_with_backoff(_fetch_bytes, max_retries=3, base_delay=2.0)
    except Exception as exc:
        logger.warning("[odp-hubspot-pdf] fetch failed for %s: %s", url, exc)
        return []

    return parse_pdf(
        pdf_bytes,
        source_url=url,
        state=state,
        program_year=program_year,
    )


# ---------------------------------------------------------------------------
# Registry hook — referenced by the runner's hubspot-pdf dispatch
# ---------------------------------------------------------------------------

# The canonical parser key for Cal North. The runner inspects
# ``platform: hubspot-pdf`` on each seed entry and, when it matches,
# calls ``download_and_parse`` directly rather than going through the
# HTML-parser registry. Parser key is exposed here so a future second
# hubspot-pdf state (there's a known pipeline at KOA / other
# associations) can dispatch via parser name rather than just platform.
PARSER_KEYS = ("calnorth",)
