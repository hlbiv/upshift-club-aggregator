"""
Club name normalization.

Produces a `canonical_name` field by:
  1. Stripping leading/trailing whitespace and collapsing internal spaces
  2. Removing common suffixes/prefixes (SC, FC, AC, CF, United, etc.)
  3. Title-casing the result
  4. Deduplicating within a DataFrame using RapidFuzz token_sort_ratio
"""

from __future__ import annotations

import re
import logging
from typing import List

import pandas as pd
from rapidfuzz import fuzz, process

from config import FUZZY_THRESHOLD

logger = logging.getLogger(__name__)

# Tokens that appear as standalone words to strip from club names
_STRIP_TOKENS = {
    "sc", "fc", "ac", "cf", "afc", "sfc", "fsc", "bc",
    "united", "utd", "city", "town", "club", "soccer",
    "youth", "boys", "girls", "men", "women",
    "f.c.", "s.c.", "a.c.", "f.c", "s.c", "a.c",
}

_STRIP_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(t) for t in _STRIP_TOKENS) + r")\b",
    flags=re.IGNORECASE,
)

_WHITESPACE = re.compile(r"\s+")

# Non-club strings that can appear when a generic HTML scraper picks up
# page navigation / social media / boilerplate text.
_GARBAGE_EXACT: frozenset = frozenset({
    "skip to main content", "skip to primary navigation", "skip to content",
    "skip to navigation", "skip to footer", "skip to main content",
    "facebook", "twitter", "instagram", "youtube", "tiktok", "linkedin",
    "pinterest", "snapchat", "vimeo", "flickr", "rss", "email",
    "menu", "search", "close", "home", "about", "contact",
    "like us", "tweet", "follow us",
})

_GARBAGE_PREFIXES: tuple = (
    "skip to ", "skip to ", "connect with ", "follow us",
    "sign up", "log in", "register ", "like us on ", "tweet ",
)


def is_valid_club_name(name: str) -> bool:
    """Return False if name is a known-garbage navigation or social-media token."""
    if not isinstance(name, str):
        return False
    stripped = name.strip()
    if len(stripped) < 3:
        return False
    lower = stripped.lower()
    if lower in _GARBAGE_EXACT:
        return False
    if any(lower.startswith(p) for p in _GARBAGE_PREFIXES):
        return False
    return True


def _canonical(name: str) -> str:
    """Return a normalised canonical form of a club name."""
    if not isinstance(name, str):
        return ""
    name = name.strip()
    # Remove parenthetical suffixes like "(U-12)" or "(Boys)"
    name = re.sub(r"\(.*?\)", "", name)
    # Remove strippable tokens
    name = _STRIP_PATTERN.sub("", name)
    # Collapse whitespace
    name = _WHITESPACE.sub(" ", name).strip()
    # Title-case
    name = name.title()
    return name


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a `canonical_name` column derived from `club_name`.

    Parameters
    ----------
    df : DataFrame with at least a `club_name` column.

    Returns
    -------
    DataFrame with an added `canonical_name` column.
    """
    if df.empty:
        df["canonical_name"] = pd.Series(dtype=str)
        return df

    df = df.copy()
    # Filter out navigation/social-media garbage before canonicalising
    before = len(df)
    df = df[df["club_name"].apply(is_valid_club_name)].reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        logger.warning("Filtered %d garbage club-name entries", dropped)
    df["canonical_name"] = df["club_name"].apply(_canonical)
    logger.info("Normalization complete: %d records", len(df))
    return df


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove near-duplicate clubs using fuzzy matching on `canonical_name`.

    Two rows are considered duplicates if their `canonical_name` values
    score >= FUZZY_THRESHOLD using token_sort_ratio. The first occurrence
    (after sorting) is kept.

    Parameters
    ----------
    df : DataFrame with `canonical_name` column.

    Returns
    -------
    Deduplicated DataFrame.
    """
    if df.empty:
        return df

    df = df.copy().reset_index(drop=True)
    canonical_names: List[str] = df["canonical_name"].tolist()
    keep_mask = [True] * len(canonical_names)

    for i in range(len(canonical_names)):
        if not keep_mask[i]:
            continue
        # Compare against all later entries
        for j in range(i + 1, len(canonical_names)):
            if not keep_mask[j]:
                continue
            score = fuzz.token_sort_ratio(canonical_names[i], canonical_names[j])
            if score >= FUZZY_THRESHOLD:
                keep_mask[j] = False
                logger.debug(
                    "Dedup: '%s' ≈ '%s' (score=%d) — dropping row %d",
                    canonical_names[i],
                    canonical_names[j],
                    score,
                    j,
                )

    before = len(df)
    df = df[keep_mask].reset_index(drop=True)
    after = len(df)
    logger.info("Deduplication: %d → %d records (%d removed)", before, after, before - after)
    return df
