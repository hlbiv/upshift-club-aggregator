/**
 * Schema smoke test — static assertions that the Path A schema compiles
 * and has the shape the data model promises. No database required.
 *
 * Run: pnpm --filter @workspace/db exec tsx src/schema/__tests__/smoke.ts
 *
 * This is deliberately lightweight — the repo has no vitest harness, and
 * the most valuable correctness check is the drizzle-kit push integration
 * test (./integration-push.ts), which requires a real Postgres.
 */

import { getTableConfig } from "drizzle-orm/pg-core";
import * as schema from "../index";

type Failure = { table: string; issue: string };
const failures: Failure[] = [];

function assert(cond: unknown, table: string, issue: string) {
  if (!cond) failures.push({ table, issue });
}

function assertTable(
  t: unknown,
  name: string,
  opts: {
    cols?: string[];
    uniques?: string[];
    checks?: string[];
    indexes?: string[];
  } = {},
) {
  if (!t || typeof t !== "object") {
    failures.push({ table: name, issue: "not exported" });
    return;
  }
  const cfg = getTableConfig(t as any);
  const colNames = new Set(cfg.columns.map((c) => c.name));
  for (const want of opts.cols ?? []) {
    assert(colNames.has(want), name, `missing column ${want}`);
  }
  const uniqueNames = new Set(cfg.uniqueConstraints.map((u) => u.name));
  const indexNames = new Set(cfg.indexes.map((i) => i.config.name));
  for (const want of opts.uniques ?? []) {
    assert(
      uniqueNames.has(want) || indexNames.has(want),
      name,
      `missing unique ${want}`,
    );
  }
  const checkNames = new Set(cfg.checks.map((c) => c.name));
  for (const want of opts.checks ?? []) {
    assert(checkNames.has(want), name, `missing check ${want}`);
  }
  for (const want of opts.indexes ?? []) {
    assert(indexNames.has(want), name, `missing index ${want}`);
  }
}

// ---------------------------------------------------------------------------
// Existing tables — extension verification
// ---------------------------------------------------------------------------

assertTable(schema.canonicalClubs, "canonical_clubs", {
  cols: [
    "club_name_canonical",
    "logo_url",
    "founded_year",
    "twitter",
    "instagram",
    "facebook",
    "staff_page_url",
    "website_status",
    "website_last_checked_at",
    "last_scraped_at",
    "scrape_confidence",
  ],
  checks: ["canonical_clubs_website_status_enum"],
});

assertTable(schema.coachDiscoveries, "coach_discoveries", {
  cols: [
    "coach_id",
    "phone",
    "first_seen_at",
    "last_seen_at",
    "platform_family",
  ],
  uniques: ["coach_discoveries_club_name_title_uq"],
  checks: [
    "coach_discoveries_confidence_range",
    "coach_discoveries_platform_family_enum",
  ],
});

// ---------------------------------------------------------------------------
// Domain 1 — Clubs extensions
// ---------------------------------------------------------------------------

assertTable(schema.clubRosterSnapshots, "club_roster_snapshots", {
  cols: [
    "club_id",
    "club_name_raw",
    "source_url",
    "snapshot_date",
    "season",
    "age_group",
    "gender",
    "player_name",
    "event_id",
  ],
  uniques: ["club_roster_snapshots_name_season_age_gender_player_uq"],
  indexes: ["club_roster_snapshots_club_season_idx"],
});

assertTable(schema.clubSiteChanges, "club_site_changes", {
  cols: [
    "club_id",
    "change_type",
    "change_detail",
    "snapshot_hash_before",
    "snapshot_hash_after",
  ],
  uniques: ["club_site_changes_unique"],
  checks: ["club_site_changes_change_type_enum"],
});

// ---------------------------------------------------------------------------
// Domain 2 — Colleges
// ---------------------------------------------------------------------------

assertTable(schema.colleges, "colleges", {
  cols: ["name", "slug", "division", "gender_program", "conference", "state"],
  uniques: ["colleges_name_division_gender_uq"],
  checks: ["colleges_division_enum", "colleges_gender_program_enum"],
});

assertTable(schema.collegeCoaches, "college_coaches", {
  cols: ["college_id", "coach_id", "name", "title", "is_head_coach"],
  uniques: ["college_coaches_college_name_title_uq"],
  indexes: ["college_coaches_coach_id_idx"],
});

assertTable(schema.collegeRosterHistory, "college_roster_history", {
  cols: ["college_id", "player_name", "academic_year", "year"],
  uniques: ["college_roster_history_college_player_year_uq"],
  checks: ["college_roster_history_year_enum"],
});

// ---------------------------------------------------------------------------
// Domain 3 — Coaches
// ---------------------------------------------------------------------------

assertTable(schema.coaches, "coaches", {
  cols: [
    "person_hash",
    "display_name",
    "primary_email",
    "manually_merged",
    "created_at",
    "updated_at",
  ],
});

assertTable(schema.coachCareerHistory, "coach_career_history", {
  cols: ["coach_id", "entity_type", "entity_id", "role", "is_current"],
  uniques: ["coach_career_history_unique"],
  checks: [
    "coach_career_history_entity_type_enum",
    "coach_career_history_role_enum",
  ],
});

assertTable(schema.coachMovementEvents, "coach_movement_events", {
  cols: [
    "coach_id",
    "event_type",
    "from_entity_type",
    "from_entity_id",
    "to_entity_type",
    "to_entity_id",
    "detected_at",
  ],
  uniques: ["coach_movement_events_unique"],
  checks: [
    "coach_movement_events_event_type_enum",
    "coach_movement_events_from_entity_type_enum",
    "coach_movement_events_to_entity_type_enum",
  ],
});

assertTable(schema.coachScrapeSnapshots, "coach_scrape_snapshots", {
  cols: ["club_id", "scraped_at", "raw_staff", "parse_confidence"],
  uniques: ["coach_scrape_snapshots_club_scraped_uq"],
});

assertTable(schema.coachEffectiveness, "coach_effectiveness", {
  cols: [
    "coach_id",
    "players_placed_d1",
    "players_placed_total",
    "clubs_coached",
  ],
  uniques: ["coach_effectiveness_coach_uq"],
});

// ---------------------------------------------------------------------------
// Domain 4 — Events
// ---------------------------------------------------------------------------

assertTable(schema.events, "events", {
  cols: ["name", "slug", "source", "platform_event_id", "age_group", "gender"],
  uniques: ["events_source_platform_id_uq"],
});

assertTable(schema.eventTeams, "event_teams", {
  cols: ["event_id", "canonical_club_id", "team_name_raw", "team_name_canonical"],
  uniques: ["event_teams_event_team_name_uq"],
});

// ---------------------------------------------------------------------------
// Domain 5 — Matches
// ---------------------------------------------------------------------------

assertTable(schema.matches, "matches", {
  cols: [
    "event_id",
    "home_club_id",
    "away_club_id",
    "home_team_name",
    "away_team_name",
    "match_date",
    "status",
    "platform_match_id",
  ],
  uniques: ["matches_source_platform_id_uq", "matches_natural_key_uq"],
  checks: ["matches_status_enum"],
});

assertTable(schema.clubResults, "club_results", {
  cols: [
    "club_id",
    "season",
    "wins",
    "losses",
    "draws",
    "goals_for",
    "goals_against",
  ],
  uniques: ["club_results_unique"],
});

// ---------------------------------------------------------------------------
// Domain 6 & 7 — Roster diffs + tryouts
// ---------------------------------------------------------------------------

assertTable(schema.rosterDiffs, "roster_diffs", {
  cols: [
    "club_id",
    "club_name_raw",
    "season",
    "age_group",
    "gender",
    "player_name",
    "diff_type",
    "from_jersey_number",
    "to_jersey_number",
    "from_position",
    "to_position",
    "detected_at",
  ],
  uniques: ["roster_diffs_name_season_age_gender_player_type_uq"],
  checks: ["roster_diffs_diff_type_enum"],
});

assertTable(schema.tryouts, "tryouts", {
  cols: [
    "club_id",
    "club_name_raw",
    "tryout_date",
    "age_group",
    "gender",
    "source",
    "status",
    "site_change_id",
  ],
  uniques: ["tryouts_name_date_bracket_uq"],
  checks: ["tryouts_source_enum", "tryouts_status_enum"],
});

// ---------------------------------------------------------------------------
// Domain 8 — Scrape health
// ---------------------------------------------------------------------------

assertTable(schema.scrapeRunLogs, "scrape_run_logs", {
  cols: [
    "scraper_key",
    "league_name",
    "started_at",
    "completed_at",
    "status",
    "failure_kind",
    "records_created",
    "records_updated",
    "records_failed",
    "records_touched",
  ],
  checks: [
    "scrape_run_logs_status_enum",
    "scrape_run_logs_failure_kind_enum",
  ],
  indexes: [
    "scrape_run_logs_scraper_started_idx",
    "scrape_run_logs_status_started_idx",
  ],
});

// Generated column check — records_touched must be marked generated.
{
  const cfg = getTableConfig(schema.scrapeRunLogs);
  const col = cfg.columns.find((c) => c.name === "records_touched");
  assert(
    col && (col as any).generated !== undefined,
    "scrape_run_logs",
    "records_touched is not a generated column",
  );
}

assertTable(schema.scrapeHealth, "scrape_health", {
  cols: [
    "entity_type",
    "entity_id",
    "last_scraped_at",
    "last_success_at",
    "status",
    "consecutive_failures",
    "priority",
  ],
  uniques: ["scrape_health_entity_uq"],
  checks: [
    "scrape_health_entity_type_enum",
    "scrape_health_status_enum",
    "scrape_health_priority_range",
  ],
});

assertTable(schema.rawHtmlArchive, "raw_html_archive", {
  cols: [
    "run_id",
    "source_url",
    "sha256",
    "bucket_path",
    "content_bytes",
    "archived_at",
  ],
  uniques: ["raw_html_archive_sha256_uq"],
  indexes: ["raw_html_archive_run_id_idx"],
});

// ---------------------------------------------------------------------------
// Domain 10 — Player iD selections (US Club iD program)
// ---------------------------------------------------------------------------

assertTable(schema.playerIdSelections, "player_id_selections", {
  cols: [
    "player_name",
    "selection_year",
    "birth_year",
    "gender",
    "pool_tier",
    "region",
    "club_name_raw",
    "club_id",
    "state",
    "position",
    "source_url",
    "source",
    "announced_at",
    "scraped_at",
  ],
  uniques: ["player_id_selections_player_year_birth_gender_tier_uq"],
  indexes: [
    "player_id_selections_year_tier_idx",
    "player_id_selections_club_idx",
  ],
  checks: ["player_id_selections_source_enum"],
});

// ---------------------------------------------------------------------------
// Domain 11 — Commitments (TopDrawerSoccer + future sources)
// ---------------------------------------------------------------------------

assertTable(schema.commitments, "commitments", {
  cols: [
    "player_name",
    "graduation_year",
    "position",
    "club_id",
    "club_name_raw",
    "college_id",
    "college_name_raw",
    "commitment_date",
    "source_url",
    "first_seen_at",
    "last_seen_at",
  ],
  uniques: ["commitments_natural_key_uq"],
  indexes: ["commitments_grad_year_idx"],
});

// ---------------------------------------------------------------------------
// Domain 9 — API keys (M2M auth)
// ---------------------------------------------------------------------------

assertTable(schema.apiKeys, "api_keys", {
  cols: [
    "name",
    "key_hash",
    "key_prefix",
    "created_at",
    "last_used_at",
    "revoked_at",
    "scopes",
  ],
});

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if (failures.length === 0) {
  console.log("[schema-smoke] OK — all Path A tables present with expected constraints");
  process.exit(0);
} else {
  console.error(`[schema-smoke] ${failures.length} failure(s):`);
  for (const f of failures) {
    console.error(`  ${f.table}: ${f.issue}`);
  }
  process.exit(1);
}
