-- 0007_tournament_matches_coach_licenses.sql
--
-- Adds:
--   1. 'athleteone' value to events_source_enum (must run outside transaction)
--   2. tournament_matches table — match results from showcase/tournament events
--      (separate from the `matches` league-play table per schema decision Option B)
--   3. coach_licenses table — USSF coaching license tier per coach
--
-- Apply on Replit:
--   psql "$DATABASE_URL" -f scripts/src/migrations/0007_tournament_matches_coach_licenses.sql
--
-- Verification:
--   SELECT enum_range(NULL::events_source_enum);
--   SELECT COUNT(*) FROM tournament_matches;
--   SELECT COUNT(*) FROM coach_licenses;

-- ============================================================
-- Phase 1: widen events_source_enum
-- ALTER TYPE ... ADD VALUE must run outside a transaction block.
-- ============================================================
ALTER TYPE events_source_enum ADD VALUE IF NOT EXISTS 'athleteone';

-- ============================================================
-- Phase 2: tournament_matches + coach_licenses (transactional)
-- ============================================================
BEGIN;

CREATE TABLE IF NOT EXISTS tournament_matches (
    id                 SERIAL PRIMARY KEY,
    event_id           INTEGER REFERENCES events(id) ON DELETE SET NULL,
    home_club_id       INTEGER REFERENCES canonical_clubs(id) ON DELETE SET NULL,
    away_club_id       INTEGER REFERENCES canonical_clubs(id) ON DELETE SET NULL,
    home_team_name     TEXT NOT NULL,
    away_team_name     TEXT NOT NULL,
    home_score         INTEGER,
    away_score         INTEGER,
    match_date         TIMESTAMP,
    age_group          TEXT,
    gender             TEXT,
    division           TEXT,
    season             TEXT,
    tournament_name    TEXT,
    flight             TEXT,
    group_name         TEXT,
    bracket_round      TEXT,
    match_type         TEXT CHECK (match_type IN ('group','knockout','placement','friendly')),
    status             TEXT NOT NULL DEFAULT 'scheduled'
                       CHECK (status IN ('scheduled','final','cancelled','forfeit','postponed')),
    source             events_source_enum,
    source_url         TEXT,
    platform_match_id  TEXT,
    scraped_at         TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Primary dedup: (source, platform_match_id) when platform id is known.
CREATE UNIQUE INDEX IF NOT EXISTS tournament_matches_source_platform_id_uq
    ON tournament_matches (source, platform_match_id)
    WHERE platform_match_id IS NOT NULL;

-- Fallback dedup for sources without stable IDs.
-- COALESCE sentinels match matches_writer COALESCE usage.
CREATE UNIQUE INDEX IF NOT EXISTS tournament_matches_natural_key_uq
    ON tournament_matches (
        home_team_name,
        away_team_name,
        COALESCE(match_date, 'epoch'::timestamp),
        COALESCE(age_group, ''),
        COALESCE(gender, ''),
        COALESCE(tournament_name, '')
    )
    WHERE platform_match_id IS NULL;

CREATE INDEX IF NOT EXISTS tournament_matches_home_club_date_idx
    ON tournament_matches (home_club_id, match_date);

CREATE INDEX IF NOT EXISTS tournament_matches_away_club_date_idx
    ON tournament_matches (away_club_id, match_date);

CREATE INDEX IF NOT EXISTS tournament_matches_event_id_idx
    ON tournament_matches (event_id);

CREATE INDEX IF NOT EXISTS tournament_matches_tournament_name_idx
    ON tournament_matches (tournament_name);

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS coach_licenses (
    id            SERIAL PRIMARY KEY,
    coach_id      INTEGER REFERENCES coaches(id) ON DELETE SET NULL,
    license_tier  TEXT NOT NULL
                  CHECK (license_tier IN (
                      'grassroots_online',
                      'grassroots_in_person',
                      'D',
                      'C',
                      'B',
                      'A',
                      'Pro'
                  )),
    state         TEXT,
    issue_date    TIMESTAMP,
    expires_at    TIMESTAMP,
    source_url    TEXT,
    first_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_seen_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- One license tier per coach per state (upsert key).
CREATE UNIQUE INDEX IF NOT EXISTS coach_licenses_coach_tier_state_uq
    ON coach_licenses (coach_id, license_tier, COALESCE(state, ''))
    WHERE coach_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS coach_licenses_coach_id_idx
    ON coach_licenses (coach_id);

CREATE INDEX IF NOT EXISTS coach_licenses_tier_idx
    ON coach_licenses (license_tier);

COMMIT;
