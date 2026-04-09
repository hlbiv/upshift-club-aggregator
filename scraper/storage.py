"""
CSV storage — save per-league files and append to the master dataset.
"""

from __future__ import annotations

import logging
import os
import re

import pandas as pd

from config import LEAGUES_DIR, MASTER_CSV

logger = logging.getLogger(__name__)

COLUMNS = ["club_name", "canonical_name", "league_name", "city", "state", "source_url"]

TEAMS_DIR = os.path.join(os.path.dirname(MASTER_CSV), "teams")
CONTACTS_DIR = os.path.join(os.path.dirname(MASTER_CSV), "contacts")

TEAMS_COLUMNS = [
    "club_name", "team_name", "team_name_raw", "age_group", "gender",
    "division", "bracket", "conference", "org_season_id", "event_id",
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
    Append `df` to the master CSV, creating it if absent.

    Returns the path of the master file.
    """
    os.makedirs(os.path.dirname(MASTER_CSV) or ".", exist_ok=True)
    df_out = _ensure_columns(df)

    if os.path.exists(MASTER_CSV):
        existing = pd.read_csv(MASTER_CSV)
        combined = pd.concat([existing, df_out], ignore_index=True)
    else:
        combined = df_out

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
