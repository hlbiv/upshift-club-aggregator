-- 0008_add_club_rankings.sql
--
-- Adds the club_rankings table for storing scraped club ranking
-- snapshots from external platforms (SincSports, GotSport, USARank).
--
-- Canonical-club-linker pattern: canonical_club_id is NULL at scrape
-- time and resolved by a subsequent linker pass.
--
-- Natural key: (platform, club_name_raw, age_group, gender, season, division)
-- allows a ranking row to be upserted idempotently on re-runs.

CREATE TABLE IF NOT EXISTS club_rankings (
    id                  SERIAL PRIMARY KEY,

    -- FK to canonical_clubs; NULL at scrape time, resolved by linker.
    canonical_club_id   INTEGER REFERENCES canonical_clubs(id) ON DELETE SET NULL,

    -- Raw club name as scraped from the source platform.
    club_name_raw       TEXT NOT NULL,

    -- Source platform identifier: 'sincsports' | 'gotsport' | 'usarank'
    platform            TEXT NOT NULL,

    -- Numeric rank position (1 = best); NULL when platform only provides a rating.
    rank_value          INTEGER,

    -- Raw rating string (some platforms use decimals or tier labels like Gold/Silver).
    rating_value        TEXT,

    age_group           TEXT,
    gender              TEXT,
    season              TEXT,
    division            TEXT,
    source_url          TEXT,
    scraped_at          TIMESTAMP DEFAULT NOW()
);

-- Natural-key unique constraint for idempotent upserts.
ALTER TABLE club_rankings
    ADD CONSTRAINT club_rankings_natural_uq
    UNIQUE (platform, club_name_raw, age_group, gender, season, division);

-- Index for linker lookups (rows with NULL canonical_club_id).
CREATE INDEX IF NOT EXISTS club_rankings_canonical_club_idx
    ON club_rankings (canonical_club_id);

-- Index for platform-scoped queries.
CREATE INDEX IF NOT EXISTS club_rankings_platform_idx
    ON club_rankings (platform);
