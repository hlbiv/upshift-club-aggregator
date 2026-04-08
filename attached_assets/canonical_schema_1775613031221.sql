
CREATE TABLE IF NOT EXISTS leagues_master (
    id SERIAL PRIMARY KEY,
    league_name TEXT NOT NULL,
    league_family TEXT NOT NULL,
    governing_body TEXT,
    tier_numeric INTEGER,
    tier_label TEXT,
    gender TEXT,
    geographic_scope TEXT,
    has_public_clubs BOOLEAN DEFAULT FALSE,
    scrape_priority TEXT,
    source_type TEXT,
    official_url TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS league_sources (
    id SERIAL PRIMARY KEY,
    platform_name TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_kind TEXT,
    source_url TEXT NOT NULL,
    verification_status TEXT DEFAULT 'verified',
    notes TEXT
);

CREATE TABLE IF NOT EXISTS canonical_clubs (
    id SERIAL PRIMARY KEY,
    club_name_canonical TEXT UNIQUE NOT NULL,
    club_slug TEXT UNIQUE,
    city TEXT,
    state TEXT,
    country TEXT DEFAULT 'USA',
    status TEXT DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS club_aliases (
    id SERIAL PRIMARY KEY,
    club_id INTEGER REFERENCES canonical_clubs(id),
    alias_name TEXT NOT NULL,
    alias_slug TEXT,
    source TEXT,
    is_official BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS club_affiliations (
    id SERIAL PRIMARY KEY,
    club_id INTEGER REFERENCES canonical_clubs(id),
    gender_program TEXT,
    platform_name TEXT,
    platform_tier TEXT,
    conference_name TEXT,
    division_name TEXT,
    season TEXT,
    source_url TEXT,
    source_name TEXT,
    verification_status TEXT DEFAULT 'verified',
    notes TEXT
);
