CREATE TABLE IF NOT EXISTS tryout_alert_subscriptions (
  id SERIAL PRIMARY KEY,
  email VARCHAR(255) NOT NULL,
  zip_code VARCHAR(10) NOT NULL,
  radius_miles INTEGER NOT NULL DEFAULT 25,
  age_group VARCHAR(20),
  gender VARCHAR(10),
  min_tier VARCHAR(50),
  created_at TIMESTAMP DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMP DEFAULT NOW() NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS tryout_alerts_email_zip_bracket_uq
  ON tryout_alert_subscriptions (
    email,
    zip_code,
    COALESCE(age_group, ''),
    COALESCE(gender, '')
  );
