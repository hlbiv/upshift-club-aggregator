-- PR 2: Align club_roster_snapshots unique index with writer's
-- COALESCE expressions so NULL-vs-empty-string sentinels match.
DROP INDEX IF EXISTS club_roster_snapshots_name_season_age_gender_player_uq;
CREATE UNIQUE INDEX club_roster_snapshots_name_season_age_gender_player_uq
  ON club_roster_snapshots (
    club_name_raw,
    COALESCE(season, ''),
    COALESCE(age_group, ''),
    COALESCE(gender, ''),
    player_name
  );

-- ROLLBACK (paste-ready if push fails or counts diverge):
-- DROP INDEX IF EXISTS club_roster_snapshots_name_season_age_gender_player_uq;
-- CREATE UNIQUE INDEX club_roster_snapshots_name_season_age_gender_player_uq
--   ON club_roster_snapshots (club_name_raw, season, age_group, gender, player_name);
