CREATE TABLE IF NOT EXISTS linker_ignores (
  id            serial PRIMARY KEY,
  raw_team_name text NOT NULL UNIQUE,
  reason        text,
  created_by    integer REFERENCES admin_users(id) ON DELETE SET NULL,
  created_at    timestamptz NOT NULL DEFAULT NOW()
);
