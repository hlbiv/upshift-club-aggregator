"""
CSV storage — save per-league files and append to the master dataset.
"""

from __future__ import annotations

import logging
import os
import re

import pandas as pd

from config import LEAGUES_DIR, MASTER_CSV
from normalizer import _canonical, deduplicate

logger = logging.getLogger(__name__)

COLUMNS = ["club_name", "canonical_name", "league_name", "city", "state", "source_url", "website"]

TEAMS_DIR = os.path.join(os.path.dirname(MASTER_CSV), "teams")
CONTACTS_DIR = os.path.join(os.path.dirname(MASTER_CSV), "contacts")

TEAMS_COLUMNS = [
    "club_name", "team_name", "team_name_raw", "age_group", "gender",
    "division", "bracket", "conference", "org_season_id", "event_id",
    "club_id", "team_id",
    "league_name", "rank", "gp", "w", "l", "d", "gf", "ga", "gd",
    "ppg", "pts", "qualification", "source_url",
]

CONTACTS_COLUMNS = [
    "club_name", "role", "name", "email", "phone", "league_name", "event_id", "source_url",
]


def _slug(name: str) -> str:
    """Convert a league name to a safe filename slug."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def save_league_csv(df: pd.DataFrame, league_name: str) -> str:
    """
    Write (or overwrite) a per-league CSV file.

    Returns the path of the written file.
    """
    os.makedirs(LEAGUES_DIR, exist_ok=True)
    path = os.path.join(LEAGUES_DIR, f"{_slug(league_name)}.csv")
    _ensure_columns(df).to_csv(path, index=False)
    logger.info("Saved %d clubs to %s", len(df), path)
    return path


def append_to_master(df: pd.DataFrame) -> str:
    """
    Upsert `df` into the master CSV, creating it if absent.

    Two deduplication passes keep the master idempotent across incremental runs:

    1. Exact-key pass: drop rows with the same (club_name, league_name), keeping
       the newest (last) occurrence so re-running a league extractor is safe.
    2. Fuzzy pass (per-league): call deduplicate() within each league_name group
       so near-duplicate club names accumulated across runs are collapsed using the
       same FUZZY_THRESHOLD used in per-league processing.

    Returns the path of the master file.
    """
    os.makedirs(os.path.dirname(MASTER_CSV) or ".", exist_ok=True)
    df_out = _ensure_columns(df)

    if os.path.exists(MASTER_CSV):
        existing = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
        combined = pd.concat([existing, df_out], ignore_index=True)
    else:
        combined = df_out

    # Pass 1: exact dedup on (club_name, league_name) — keep newest row
    dedup_keys = [c for c in ("club_name", "league_name") if c in combined.columns]
    if dedup_keys:
        before = len(combined)
        combined = combined.drop_duplicates(subset=dedup_keys, keep="last")
        removed = before - len(combined)
        if removed:
            logger.info("Master exact-dedup removed %d duplicate rows", removed)

    # Pass 2: fuzzy dedup on the combined DataFrame
    # Ensure canonical_name is populated so deduplicate() can work.
    if "canonical_name" in combined.columns:
        missing_mask = combined["canonical_name"].fillna("").eq("")
        if missing_mask.any():
            combined.loc[missing_mask, "canonical_name"] = (
                combined.loc[missing_mask, "club_name"].apply(_canonical)
            )
        combined = deduplicate(combined.reset_index(drop=True))

    combined.to_csv(MASTER_CSV, index=False)
    logger.info("Master CSV updated: %d total records", len(combined))
    return MASTER_CSV


def save_teams_csv(records: list, league_name: str) -> str:
    """
    Write (or overwrite) a per-league teams CSV to output/teams/.
    Records should be dicts; missing columns are filled with empty string.
    Returns the path of the written file.
    """
    if not records:
        return ""
    os.makedirs(TEAMS_DIR, exist_ok=True)
    path = os.path.join(TEAMS_DIR, f"{_slug(league_name)}.csv")
    df = pd.DataFrame(records)
    for col in TEAMS_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df[TEAMS_COLUMNS].to_csv(path, index=False)
    logger.info("Saved %d team rows to %s", len(df), path)
    return path


def save_contacts_csv(records: list, league_name: str) -> str:
    """
    Write (or overwrite) a per-league contacts CSV to output/contacts/.
    Returns the path of the written file.
    """
    if not records:
        return ""
    os.makedirs(CONTACTS_DIR, exist_ok=True)
    path = os.path.join(CONTACTS_DIR, f"{_slug(league_name)}.csv")
    df = pd.DataFrame(records)
    for col in CONTACTS_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df[CONTACTS_COLUMNS].to_csv(path, index=False)
    logger.info("Saved %d contacts to %s", len(df), path)
    return path


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Guarantee all output columns exist (in order), filling missing with empty string."""
    df = df.copy()
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[COLUMNS]
