"""
club_website.py — Extract enrichment data from a club's website.

Given a club website URL, this module:
  1. Checks HTTP response status to determine website_status
  2. Discovers the club logo (favicon, og:image, prominent <img> tags)
  3. Extracts social media handles (Facebook, Instagram, Twitter/X, YouTube, TikTok)
  4. Detects staff/coaching pages for future staff scraping
  5. Computes a scrape_confidence score (0-100) based on data found

No Playwright/Selenium — uses requests + BeautifulSoup only.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("club_website_extractor")

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_REQUEST_TIMEOUT = 15

# Parked-page indicators (case-insensitive substring match on page text)
_PARKED_INDICATORS = [
    "this domain is for sale",
    "domain parking",
    "buy this domain",
    "this webpage is parked",
    "domain has expired",
    "website is under construction",
    "coming soon",
    "parked by",
    "godaddy",
    "hugedomains.com",
    "sedoparking",
    "undeveloped.com",
]

# Social platform URL patterns → (platform_key, handle_extraction_regex)
_SOCIAL_PATTERNS: List[tuple[str, str, re.Pattern]] = [
    ("instagram", "instagram.com", re.compile(r"instagram\.com/([A-Za-z0-9_.]+)", re.I)),
    ("facebook", "facebook.com", re.compile(r"facebook\.com/([A-Za-z0-9_.]+)", re.I)),
    ("twitter", "twitter.com", re.compile(r"(?:twitter|x)\.com/([A-Za-z0-9_]+)", re.I)),
    ("twitter", "x.com", re.compile(r"(?:twitter|x)\.com/([A-Za-z0-9_]+)", re.I)),
    ("youtube", "youtube.com", re.compile(r"youtube\.com/(?:@|channel/|c/|user/)?([A-Za-z0-9_-]+)", re.I)),
    ("tiktok", "tiktok.com", re.compile(r"tiktok\.com/@?([A-Za-z0-9_.]+)", re.I)),
]

# Keywords that indicate a staff/coaching page
_STAFF_PAGE_KEYWORDS = [
    "staff", "coaches", "coaching", "our team", "club staff",
    "technical staff", "coaching staff", "meet the team",
    "directors", "trainers",
]

# Logo-like image src patterns (case-insensitive)
_LOGO_SRC_PATTERNS = re.compile(
    r"(logo|crest|badge|emblem|brand|shield)", re.I
)


@dataclass
class ClubEnrichmentResult:
    """Result of enriching a single club website."""
    club_id: int
    website_url: str
    website_status: str  # active, dead, redirected, parked, unchecked
    logo_url: Optional[str] = None
    instagram: Optional[str] = None
    facebook: Optional[str] = None
    twitter: Optional[str] = None
    youtube: Optional[str] = None
    tiktok: Optional[str] = None
    staff_page_url: Optional[str] = None
    scrape_confidence: float = 0.0
    error: Optional[str] = None


def _check_website_status(response: requests.Response, page_text: str) -> str:
    """Determine the website status from HTTP response."""
    if response.status_code >= 400:
        return "dead"

    # Check for redirect chains
    if response.history:
        # If redirected to a completely different domain, it's a redirect
        original_domain = urlparse(response.history[0].url).hostname or ""
        final_domain = urlparse(response.url).hostname or ""
        if original_domain and final_domain:
            orig_parts = original_domain.split(".")
            final_parts = final_domain.split(".")
            orig_apex = ".".join(orig_parts[-2:]) if len(orig_parts) >= 2 else original_domain
            final_apex = ".".join(final_parts[-2:]) if len(final_parts) >= 2 else final_domain
            if orig_apex != final_apex:
                return "redirected"

    # Check for parked page
    text_lower = page_text.lower()
    parked_hits = sum(1 for p in _PARKED_INDICATORS if p in text_lower)
    if parked_hits >= 2:
        return "dead"

    return "active"


def _discover_logo(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    """Find the club's logo URL."""
    # 1. Check og:image meta tag
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        return urljoin(base_url, og["content"])

    # 2. Check apple-touch-icon (often high-res logo)
    apple = soup.find("link", rel=lambda r: r and "apple-touch-icon" in r)
    if apple and apple.get("href"):
        href = apple["href"]
        if _LOGO_SRC_PATTERNS.search(href) or "apple-touch" in href:
            return urljoin(base_url, href)

    # 3. Check <link rel="icon"> (favicon)
    icon = soup.find("link", rel=lambda r: r and "icon" in r)
    if icon and icon.get("href"):
        href = icon["href"]
        # Skip tiny default favicons
        sizes = icon.get("sizes", "")
        if sizes and "x" in sizes:
            try:
                w = int(sizes.split("x")[0])
                if w >= 32:
                    return urljoin(base_url, href)
            except (ValueError, IndexError):
                pass
        elif not href.endswith(".ico"):
            return urljoin(base_url, href)

    # 4. Look for <img> tags with logo-like src/alt/class
    for img in soup.find_all("img", src=True)[:30]:
        src = img.get("src", "")
        alt = img.get("alt", "")
        css_class = " ".join(img.get("class", []))
        if _LOGO_SRC_PATTERNS.search(src) or _LOGO_SRC_PATTERNS.search(alt) or _LOGO_SRC_PATTERNS.search(css_class):
            return urljoin(base_url, src)

    return None


def _extract_socials(soup: BeautifulSoup) -> Dict[str, str]:
    """Extract social media handles from <a> links."""
    socials: Dict[str, str] = {}
    seen_platforms: set = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        if not href.startswith("http"):
            continue

        for platform_key, domain_marker, pattern in _SOCIAL_PATTERNS:
            if platform_key in seen_platforms:
                continue
            if domain_marker not in href.lower():
                continue
            m = pattern.search(href)
            if m:
                handle = m.group(1)
                # Skip generic/non-handle paths
                if handle.lower() in (
                    "share", "sharer", "intent", "hashtag", "search",
                    "dialog", "home", "watch", "login", "signup",
                    "about", "help", "policy", "terms", "privacy",
                ):
                    continue
                socials[platform_key] = handle
                seen_platforms.add(platform_key)

    return socials


def _find_staff_page(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    """Look for a link to a staff/coaching page."""
    for a_tag in soup.find_all("a", href=True):
        text = (a_tag.get_text(strip=True) or "").lower()
        href = a_tag["href"].strip().lower()

        for kw in _STAFF_PAGE_KEYWORDS:
            if kw in text or kw in href:
                full_url = urljoin(base_url, a_tag["href"].strip())
                # Skip external links and anchors
                if urlparse(full_url).hostname == urlparse(base_url).hostname:
                    return full_url

    return None


def _compute_confidence(result: ClubEnrichmentResult) -> float:
    """Compute a confidence score (0-100) based on how much data was found."""
    if result.website_status != "active":
        return 10.0

    score = 20.0  # base score for an active site

    if result.logo_url:
        score += 25.0

    social_count = sum(1 for v in [
        result.instagram, result.facebook, result.twitter,
        result.youtube, result.tiktok,
    ] if v)
    score += min(social_count * 10.0, 30.0)  # up to 30 for socials

    if result.staff_page_url:
        score += 15.0

    # Cap at 100 (shouldn't exceed but safety)
    return min(score, 100.0)


def extract_club_enrichment(
    club_id: int,
    website_url: str,
) -> ClubEnrichmentResult:
    """Fetch a club website and extract enrichment data.

    Never raises — returns a result with error field set on failure.
    """
    result = ClubEnrichmentResult(
        club_id=club_id,
        website_url=website_url,
        website_status="unchecked",
    )

    # Normalise URL
    url = website_url.strip()
    if not url.startswith("http"):
        url = "https://" + url

    try:
        resp = requests.get(
            url,
            headers=_HEADERS,
            timeout=_REQUEST_TIMEOUT,
            allow_redirects=True,
        )
    except requests.exceptions.Timeout:
        result.website_status = "dead"
        result.error = "timeout"
        result.scrape_confidence = 5.0
        return result
    except requests.exceptions.ConnectionError as exc:
        result.website_status = "dead"
        result.error = f"connection_error: {str(exc)[:200]}"
        result.scrape_confidence = 5.0
        return result
    except Exception as exc:
        result.website_status = "unchecked"
        result.error = f"request_error: {str(exc)[:200]}"
        result.scrape_confidence = 0.0
        return result

    # Get page text for status detection
    page_text = resp.text or ""
    result.website_status = _check_website_status(resp, page_text)

    if result.website_status != "active":
        result.scrape_confidence = _compute_confidence(result)
        return result

    # Parse HTML
    try:
        soup = BeautifulSoup(page_text, "html.parser")
    except Exception as exc:
        result.error = f"parse_error: {str(exc)[:200]}"
        result.scrape_confidence = 15.0
        return result

    base_url = resp.url  # use final URL after redirects

    # Extract data
    result.logo_url = _discover_logo(soup, base_url)
    socials = _extract_socials(soup)
    result.instagram = socials.get("instagram")
    result.facebook = socials.get("facebook")
    result.twitter = socials.get("twitter")
    result.youtube = socials.get("youtube")
    result.tiktok = socials.get("tiktok")
    result.staff_page_url = _find_staff_page(soup, base_url)

    result.scrape_confidence = _compute_confidence(result)
    return result
