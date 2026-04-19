"""
ussoccer_ynt.py — Parse US Soccer Youth National Team (YNT) call-up
press releases into structured rows.

Target: press-release articles on ``www.ussoccer.com/news`` / ``.../stories/``
announcing player call-ups for training camps and international
tournaments at each age group (U-14 … U-20, boys & girls).

Output contract — each returned dict matches ``ynt_call_ups``:

    {
        "player_name":      str,           # required
        "graduation_year":  int | None,
        "position":         str | None,
        "club_name_raw":    str | None,    # the linker resolves club_id later
        "age_group":        str,           # "U-14" … "U-20"
        "gender":           str,           # "boys" | "girls"
        "camp_event":       str | None,    # e.g. "January 2026 Training Camp"
        "camp_start_date":  date | None,
        "camp_end_date":    date | None,
        "source_url":       str,
    }

The parser is intentionally lenient — US Soccer's article template has
drifted over the years and different PR authors format the roster
differently. Two layouts we handle:

  1. Roster ``<table>`` with header columns NAME / POSITION / CLUB /
     YEAR (any order, case-insensitive).
  2. Inline roster — a paragraph or ``<ul>`` where each player line is
     "First Last — Position, Club (YYYY)" or similar.

On layout drift the parser returns an empty list; the runner logs a
warning and continues. Pure function — no I/O.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Regexes
# ---------------------------------------------------------------------------

# "U-17", "U17", "Under-17", "Under 17", "U-17 Men's", "U-20 Women's"
_AGE_RE = re.compile(r"\b(?:U|Under)[-\s]?(1[4-9]|20)\b", re.IGNORECASE)

# Gender markers seen in US Soccer headlines and URL slugs:
# "Boys", "Men's", "MYNT", "U-17 BNT" → boys;
# "Girls", "Women's", "WYNT", "U-17 GNT" → girls.
_BOYS_MARKERS = ("boys", "bnt", "myynt", "mynt", "men's", "mens", "men ")
_GIRLS_MARKERS = ("girls", "gnt", "wynt", "women's", "womens", "women ")

# Month name → number
_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Date range in press-release prose:
#   "January 5-12, 2026"
#   "Jan 5 - 12, 2026"
#   "January 5 to January 12, 2026"
#   "January 28 - February 3, 2026"
_DATE_RANGE_RE = re.compile(
    r"(?P<m1>[A-Za-z]+)\s+(?P<d1>\d{1,2})"
    r"(?:\s*[-–to]+\s*(?:(?P<m2>[A-Za-z]+)\s+)?(?P<d2>\d{1,2}))?"
    r"(?:,|\s)\s*(?P<y>\d{4})",
    re.IGNORECASE,
)

# "Player Name — GK, Real Colorado (2027)"
# "Player Name – GK – Real Colorado – 2027"
# Separators: em-dash, en-dash, hyphen, pipe, comma.
_INLINE_SEP = re.compile(r"\s*[–—|,-]\s*")

# Position abbrevs commonly used in YNT rosters.
_POSITION_ABBR = {
    "GK", "D", "DF", "DEF", "CB", "FB", "LB", "RB", "LWB", "RWB",
    "M", "MF", "MID", "CM", "DM", "CDM", "AM", "CAM", "LM", "RM",
    "F", "FW", "FWD", "ST", "CF", "LW", "RW", "W",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_article_html(
    html: str,
    *,
    source_url: str,
    age_group_hint: Optional[str] = None,
    gender_hint: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Parse a US Soccer press-release article into ``ynt_call_ups`` rows.

    ``age_group_hint`` / ``gender_hint`` may be passed when the caller
    already knows them from the URL slug (e.g. ``/u17-boys-roster/``).
    The parser falls back to sniffing the page headline / ``<title>``.
    """
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")

    title = _page_title(soup)
    age_group = age_group_hint or _detect_age_group(title) or _detect_age_group(source_url)
    gender = gender_hint or _detect_gender(title) or _detect_gender(source_url)

    if not age_group or not gender:
        logger.info(
            "[ussoccer-ynt] skipping %s: could not detect age_group/gender "
            "(title=%r)",
            source_url, (title or "")[:80],
        )
        return []

    body_text = _article_body_text(soup)
    camp_event, camp_start, camp_end = _detect_camp_event(title, body_text)

    rows: List[Dict[str, Any]] = []

    # Layout 1 — explicit roster <table>.
    for table in soup.find_all("table"):
        rows.extend(_parse_roster_table(table))

    # Layout 2 — inline list (often a <ul> inside the article body).
    if not rows:
        rows.extend(_parse_inline_roster(soup))

    # Stamp camp + source metadata + age/gender onto every row.
    for r in rows:
        r.setdefault("position", None)
        r.setdefault("club_name_raw", None)
        r.setdefault("graduation_year", None)
        r["age_group"] = age_group
        r["gender"] = gender
        r["camp_event"] = camp_event
        r["camp_start_date"] = camp_start
        r["camp_end_date"] = camp_end
        r["source_url"] = source_url

    # Dedup — two layouts running against the same article can double
    # up; key is (name, position, club).
    seen: set[Tuple[str, str, str]] = set()
    deduped: List[Dict[str, Any]] = []
    for r in rows:
        key = (
            (r.get("player_name") or "").strip().lower(),
            (r.get("position") or "").strip().lower(),
            (r.get("club_name_raw") or "").strip().lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    return deduped


# ---------------------------------------------------------------------------
# Internals — heading / URL sniffing
# ---------------------------------------------------------------------------


def _page_title(soup: BeautifulSoup) -> str:
    for selector in ("h1", "title"):
        node = soup.find(selector)
        if node and node.get_text(strip=True):
            return node.get_text(" ", strip=True)
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        return str(og.get("content"))
    return ""


def _detect_age_group(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    m = _AGE_RE.search(s)
    if not m:
        return None
    return f"U-{m.group(1)}"


def _detect_gender(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    low = s.lower()
    for marker in _GIRLS_MARKERS:
        if marker in low:
            return "girls"
    for marker in _BOYS_MARKERS:
        if marker in low:
            return "boys"
    return None


def _article_body_text(soup: BeautifulSoup) -> str:
    """Return all visible text in the article body — used for camp date
    and event-name detection. Prefer <article>/<main>; fall back to body."""
    for selector in ("article", "main", "div.article-body", "div.content"):
        node = soup.select_one(selector)
        if node:
            return node.get_text(" ", strip=True)
    body = soup.find("body")
    if body:
        return body.get_text(" ", strip=True)
    return soup.get_text(" ", strip=True)


def _detect_camp_event(
    title: str, body: str,
) -> Tuple[Optional[str], Optional[date], Optional[date]]:
    """Return (camp_event, start_date, end_date). All three may be None."""
    haystack = f"{title} {body}"

    # Camp event name — try the title first, falling back to sentences
    # that mention "training camp", "tournament", "identification".
    camp_event = None
    for phrase in (
        r"([A-Z][A-Za-z]+\s+\d{4}\s+Training Camp)",
        r"([A-Z][A-Za-z]+\s+\d{4}\s+Identification Camp)",
        r"([A-Z][A-Za-z]+\s+\d{4}\s+Camp)",
        r"([A-Z][A-Za-z]+\s+\d{4}\s+Tournament)",
        r"(Concacaf\s+[A-Za-z0-9-]+\s+Championship)",
        r"(UEFA\s+[A-Za-z0-9-]+\s+Tournament)",
    ):
        m = re.search(phrase, haystack)
        if m:
            camp_event = m.group(1).strip()
            break

    start, end = _detect_camp_dates(haystack)
    return camp_event, start, end


def _detect_camp_dates(text: str) -> Tuple[Optional[date], Optional[date]]:
    """Scan ``text`` for a date range and return (start, end).

    Preference order:
      1. The first match that is a true range (``d2`` captured and
         different from ``d1``) — this filters out bylines like
         "December 15, 2025" that are single dates.
      2. Otherwise fall back to the first single-date match.
    """
    matches = list(_DATE_RANGE_RE.finditer(text))
    if not matches:
        return None, None

    def _to_dates(match: "re.Match[str]") -> Optional[Tuple[date, date]]:
        m1 = _MONTHS.get(match.group("m1").lower())
        m2_raw = match.group("m2")
        m2 = _MONTHS.get(m2_raw.lower()) if m2_raw else m1
        d1 = match.group("d1")
        d2_raw = match.group("d2")
        d2 = d2_raw or d1
        year = match.group("y")
        try:
            return (
                date(int(year), m1, int(d1)),  # type: ignore[arg-type]
                date(int(year), m2, int(d2)),  # type: ignore[arg-type]
            )
        except (TypeError, ValueError):
            return None

    # Pass 1 — prefer true ranges.
    for m in matches:
        d2_raw = m.group("d2")
        if d2_raw and d2_raw != m.group("d1"):
            result = _to_dates(m)
            if result:
                return result

    # Pass 2 — fall back to the first parseable single-date match.
    for m in matches:
        result = _to_dates(m)
        if result:
            return result
    return None, None


# ---------------------------------------------------------------------------
# Layout 1 — roster table
# ---------------------------------------------------------------------------


def _parse_roster_table(table: Tag) -> List[Dict[str, Any]]:
    headers = _table_headers(table)
    if not headers:
        return []
    # We need at minimum a "name" column. Everything else is optional.
    if "name" not in headers.values():
        return []

    rows: List[Dict[str, Any]] = []
    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        # Skip header rows in tables that don't use <thead>.
        if all(c.name == "th" for c in cells):
            continue

        row: Dict[str, Any] = {}
        for i, cell in enumerate(cells):
            key = headers.get(i)
            if not key:
                continue
            value = cell.get_text(" ", strip=True)
            if not value:
                continue
            if key == "name":
                row["player_name"] = value
            elif key == "position":
                row["position"] = value.upper() if value.upper() in _POSITION_ABBR else value
            elif key == "club":
                row["club_name_raw"] = value
            elif key == "year":
                row["graduation_year"] = _parse_year(value)
        if row.get("player_name"):
            rows.append(row)
    return rows


def _table_headers(table: Tag) -> Dict[int, str]:
    """Map column-index → canonical header key."""
    headers: Dict[int, str] = {}
    head_row = None
    thead = table.find("thead")
    if thead:
        head_row = thead.find("tr")
    if head_row is None:
        first_row = table.find("tr")
        if first_row and all(c.name == "th" for c in first_row.find_all(["td", "th"])):
            head_row = first_row
    if head_row is None:
        return {}

    for i, cell in enumerate(head_row.find_all(["td", "th"])):
        text = cell.get_text(" ", strip=True).lower()
        key = _canonicalize_header(text)
        if key:
            headers[i] = key
    return headers


def _canonicalize_header(text: str) -> Optional[str]:
    text = text.strip().lower()
    if not text:
        return None
    if "name" in text or text == "player":
        return "name"
    if "pos" in text:
        return "position"
    if "club" in text or "team" in text:
        return "club"
    if "year" in text or "grad" in text or "class" in text or text in {"yr", "yob"}:
        return "year"
    return None


def _parse_year(value: str) -> Optional[int]:
    m = re.search(r"\b(19|20)\d{2}\b", value)
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Layout 2 — inline list
# ---------------------------------------------------------------------------


def _parse_inline_roster(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Pull player rows from an inline roster list.

    Typical pattern inside the article body:

        <ul>
          <li>Firstname Lastname – GK, Club Name (2027)</li>
          <li>Firstname Lastname – MF – Club Name – 2028</li>
        </ul>

    Or a paragraph sequence with hard line breaks. We try <li> first,
    then fall back to <p>/<br>-delimited lines inside the article body.
    """
    candidates: List[str] = []
    for li in soup.find_all("li"):
        line = li.get_text(" ", strip=True)
        if line:
            candidates.append(line)

    if not candidates:
        # Some templates hand-roll the roster inside a single <div>
        # with <br> separators. Split on newline after fetching text.
        body = soup.find("article") or soup.find("main") or soup.find("body")
        if body:
            raw = body.get_text("\n", strip=True)
            for line in raw.split("\n"):
                line = line.strip()
                if line:
                    candidates.append(line)

    rows: List[Dict[str, Any]] = []
    for line in candidates:
        parsed = _parse_inline_line(line)
        if parsed:
            rows.append(parsed)
    return rows


def _parse_inline_line(line: str) -> Optional[Dict[str, Any]]:
    """Best-effort parse of a single roster line.

    Returns None for lines that clearly aren't a player (too short,
    no comma/dash, no capitalized first word, etc.).
    """
    if len(line) < 5 or len(line) > 240:
        return None
    # Must look like "Firstname Lastname" at start.
    m_name = re.match(
        r"([A-Z][A-Za-z'’\-]+(?:\s+[A-Z][A-Za-z'’\-]+){1,3})\b",
        line,
    )
    if not m_name:
        return None
    name = m_name.group(1).strip()
    rest = line[m_name.end():].strip()
    if not rest:
        return None
    # Must have at least one separator after the name.
    if not re.search(r"[–—|,\-]", rest):
        return None

    parts = [p.strip() for p in _INLINE_SEP.split(rest) if p.strip()]
    if not parts:
        return None

    row: Dict[str, Any] = {"player_name": name}

    for part in parts:
        # Year in parentheses — "(2027)"
        m_year = re.search(r"\((\d{4})\)", part)
        if m_year:
            try:
                row["graduation_year"] = int(m_year.group(1))
            except ValueError:
                pass
            part = re.sub(r"\(\d{4}\)", "", part).strip()
            if not part:
                continue
        # Bare 4-digit year standing alone.
        if re.fullmatch(r"(19|20)\d{2}", part):
            try:
                row["graduation_year"] = int(part)
            except ValueError:
                pass
            continue
        # Position abbrev.
        upper = part.upper().replace(".", "")
        if upper in _POSITION_ABBR and "position" not in row:
            row["position"] = upper
            continue
        # Everything else left over → candidate club name.
        if "club_name_raw" not in row:
            row["club_name_raw"] = part

    return row
