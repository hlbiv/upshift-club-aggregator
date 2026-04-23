/**
 * Hand-maintained Zod schemas for API endpoints that are served by
 * `@workspace/api-server` but not yet documented in `lib/api-spec/openapi.yaml`.
 *
 * These live outside `generated/` so that a full Orval regeneration does not
 * wipe them out. They should migrate into `openapi.yaml` in a follow-up PR —
 * at that point, the corresponding export here can be removed in favor of the
 * auto-generated version.
 *
 * Endpoints covered:
 *   - /api/events/{id}, /api/events/batch, /api/events/{id}/teams
 *   - /api/coaches/{id}, /api/coaches/{id}/career, /api/coaches/{id}/movements,
 *     /api/coaches/{id}/effectiveness, /api/coaches/leaderboard
 *   - /api/matches/search, /api/matches/{id}, /api/matches/batch
 *   - /api/rosters/snapshots, /api/rosters/diffs
 *   - /api/standings/search
 *   - /api/colleges (list/detail), /api/colleges/{id}/coaches,
 *     /api/colleges/{id}/rosters
 *   - /api/tryouts/search, /api/tryouts/submit, /api/tryouts/stats
 */

import { z as zod } from "zod";

// ---------------------------------------------------------------------------
// Events (D4.2) — single detail, batch, teams
// ---------------------------------------------------------------------------

/**
 * @summary Single event detail with embedded teams
 */
export const EventDetailResponse = zod.object({
  id: zod.number(),
  name: zod.string(),
  slug: zod.string(),
  league_name: zod.string().nullable().optional(),
  season: zod.string().nullable().optional(),
  age_group: zod.string().nullable().optional(),
  gender: zod.string().nullable().optional(),
  division: zod.string().nullable().optional(),
  location_city: zod.string().nullable().optional(),
  location_state: zod.string().nullable().optional(),
  start_date: zod.string().nullable().optional(),
  end_date: zod.string().nullable().optional(),
  registration_url: zod.string().nullable().optional(),
  source_url: zod.string().nullable().optional(),
  source: zod.string().nullable().optional(),
  platform_event_id: zod.string().nullable().optional(),
  teams: zod.array(
    zod.object({
      id: zod.number(),
      canonical_club_id: zod.number().nullable().optional(),
      team_name_raw: zod.string(),
      team_name_canonical: zod.string().nullable().optional(),
      age_group: zod.string().nullable().optional(),
      gender: zod.string().nullable().optional(),
      division_code: zod.string().nullable().optional(),
      source_url: zod.string().nullable().optional(),
    }),
  ),
});

/**
 * @summary Batch event lookup
 */
export const EventBatchResponse = zod.object({
  events: zod.array(EventDetailResponse.omit({ teams: true })),
  total: zod.number(),
});

/**
 * @summary Paginated teams for a single event
 */
export const EventTeamItem = zod.object({
  id: zod.number(),
  event_id: zod.number(),
  canonical_club_id: zod.number().nullable().optional(),
  team_name_raw: zod.string(),
  team_name_canonical: zod.string().nullable().optional(),
  age_group: zod.string().nullable().optional(),
  gender: zod.string().nullable().optional(),
  division_code: zod.string().nullable().optional(),
  source_url: zod.string().nullable().optional(),
});

export const EventTeamsResponse = zod.object({
  teams: zod.array(EventTeamItem),
  total: zod.number(),
  page: zod.number(),
  page_size: zod.number(),
});

// ---------------------------------------------------------------------------
// Coach detail, career, movements, effectiveness, leaderboard (D3.2)
// ---------------------------------------------------------------------------

const CoachCareerHistoryItem = zod.object({
  id: zod.number(),
  coach_id: zod.number(),
  entity_type: zod.string(),
  entity_id: zod.number(),
  entity_name: zod.string().nullable().optional(),
  role: zod.string(),
  start_year: zod.number().nullable().optional(),
  end_year: zod.number().nullable().optional(),
  is_current: zod.boolean(),
  source: zod.string().nullable().optional(),
  source_url: zod.string().nullable().optional(),
  confidence: zod.number().nullable().optional(),
});

const CoachEffectivenessItem = zod.object({
  id: zod.number(),
  coach_id: zod.number(),
  players_placed_d1: zod.number(),
  players_placed_d2: zod.number(),
  players_placed_d3: zod.number(),
  players_placed_naia: zod.number(),
  players_placed_njcaa: zod.number(),
  players_placed_total: zod.number(),
  clubs_coached: zod.number(),
  seasons_tracked: zod.number(),
  last_calculated_at: zod.string().nullable().optional(),
});

const CoachMovementItem = zod.object({
  id: zod.number(),
  coach_id: zod.number(),
  event_type: zod.string(),
  from_entity_type: zod.string().nullable().optional(),
  from_entity_id: zod.number().nullable().optional(),
  from_entity_name: zod.string().nullable().optional(),
  to_entity_type: zod.string().nullable().optional(),
  to_entity_id: zod.number().nullable().optional(),
  to_entity_name: zod.string().nullable().optional(),
  from_role: zod.string().nullable().optional(),
  to_role: zod.string().nullable().optional(),
  detected_at: zod.string(),
  confidence: zod.number().nullable().optional(),
});

/**
 * @summary Single coach with career history and effectiveness
 */
export const CoachDetailResponse = zod.object({
  id: zod.number(),
  person_hash: zod.string(),
  display_name: zod.string(),
  primary_email: zod.string().nullable().optional(),
  first_seen_at: zod.string().nullable().optional(),
  last_seen_at: zod.string().nullable().optional(),
  career: zod.array(CoachCareerHistoryItem),
  effectiveness: CoachEffectivenessItem.nullable().optional(),
});

/**
 * @summary Paginated career history for a coach
 */
export const CoachCareerResponse = zod.object({
  career: zod.array(CoachCareerHistoryItem),
  total: zod.number(),
  page: zod.number(),
  page_size: zod.number(),
});

/**
 * @summary Paginated movement events for a coach
 */
export const CoachMovementsResponse = zod.object({
  movements: zod.array(CoachMovementItem),
  total: zod.number(),
  page: zod.number(),
  page_size: zod.number(),
});

/**
 * @summary Single effectiveness record for a coach
 */
export const CoachEffectivenessResponse = CoachEffectivenessItem;

/**
 * @summary Top coaches by placements
 */
export const CoachLeaderboardResponse = zod.object({
  coaches: zod.array(
    zod.object({
      id: zod.number(),
      display_name: zod.string(),
      players_placed_total: zod.number(),
      players_placed_d1: zod.number(),
      players_placed_d2: zod.number(),
      players_placed_d3: zod.number(),
      players_placed_naia: zod.number(),
      players_placed_njcaa: zod.number(),
      clubs_coached: zod.number(),
      seasons_tracked: zod.number(),
    }),
  ),
  total: zod.number(),
  page: zod.number(),
  page_size: zod.number(),
});

// ---------------------------------------------------------------------------
// Matches (D5.2)
// ---------------------------------------------------------------------------

const MatchItem = zod.object({
  id: zod.number(),
  event_id: zod.number().nullable().optional(),
  home_club_id: zod.number().nullable().optional(),
  away_club_id: zod.number().nullable().optional(),
  home_team_name: zod.string(),
  away_team_name: zod.string(),
  home_club_name: zod.string().nullable().optional(),
  away_club_name: zod.string().nullable().optional(),
  home_score: zod.number().nullable().optional(),
  away_score: zod.number().nullable().optional(),
  match_date: zod.string().nullable().optional(),
  age_group: zod.string().nullable().optional(),
  gender: zod.string().nullable().optional(),
  division: zod.string().nullable().optional(),
  season: zod.string().nullable().optional(),
  league: zod.string().nullable().optional(),
  status: zod.string(),
  source: zod.string().nullable().optional(),
  source_url: zod.string().nullable().optional(),
  platform_match_id: zod.string().nullable().optional(),
  scraped_at: zod.string(),
});

/**
 * @summary Paginated match search
 */
export const MatchSearchResponse = zod.object({
  matches: zod.array(MatchItem),
  total: zod.number(),
  page: zod.number(),
  page_size: zod.number(),
});

/**
 * @summary Single match detail
 */
export const MatchDetailResponse = MatchItem;

/**
 * @summary Batch match lookup
 */
export const MatchBatchResponse = zod.object({
  matches: zod.array(MatchItem),
  total: zod.number(),
});

// ---------------------------------------------------------------------------
// Rosters (D5.2)
// ---------------------------------------------------------------------------

const RosterSnapshotItem = zod.object({
  id: zod.number(),
  club_id: zod.number().nullable().optional(),
  club_name_raw: zod.string(),
  season: zod.string(),
  age_group: zod.string(),
  gender: zod.string(),
  division: zod.string().nullable().optional(),
  player_name: zod.string(),
  jersey_number: zod.string().nullable().optional(),
  position: zod.string().nullable().optional(),
  grad_year: zod.number().nullable().optional(),
  hometown: zod.string().nullable().optional(),
  state: zod.string().nullable().optional(),
  country: zod.string().nullable().optional(),
  nationality: zod.string().nullable().optional(),
  college_commitment: zod.string().nullable().optional(),
  academic_year: zod.string().nullable().optional(),
  prev_club: zod.string().nullable().optional(),
  league: zod.string().nullable().optional(),
  source_url: zod.string().nullable().optional(),
  snapshot_date: zod.string().nullable().optional(),
  scraped_at: zod.string(),
  source: zod.string().nullable().optional(),
  event_id: zod.number().nullable().optional(),
});

/**
 * @summary Paginated roster snapshot search
 */
export const RosterSnapshotSearchResponse = zod.object({
  snapshots: zod.array(RosterSnapshotItem),
  total: zod.number(),
  page: zod.number(),
  page_size: zod.number(),
});

const RosterDiffItem = zod.object({
  id: zod.number(),
  club_id: zod.number().nullable().optional(),
  club_name_raw: zod.string(),
  season: zod.string().nullable().optional(),
  age_group: zod.string().nullable().optional(),
  gender: zod.string().nullable().optional(),
  player_name: zod.string(),
  diff_type: zod.string(),
  from_jersey_number: zod.string().nullable().optional(),
  to_jersey_number: zod.string().nullable().optional(),
  from_position: zod.string().nullable().optional(),
  to_position: zod.string().nullable().optional(),
  detected_at: zod.string(),
});

/**
 * @summary Paginated roster diff search
 */
export const RosterDiffSearchResponse = zod.object({
  diffs: zod.array(RosterDiffItem),
  total: zod.number(),
  page: zod.number(),
  page_size: zod.number(),
});

// ---------------------------------------------------------------------------
// Standings (D5.2)
// ---------------------------------------------------------------------------

const StandingItem = zod.object({
  id: zod.number(),
  club_id: zod.number(),
  club_name: zod.string().nullable().optional(),
  season: zod.string(),
  league: zod.string().nullable().optional(),
  division: zod.string().nullable().optional(),
  age_group: zod.string().nullable().optional(),
  gender: zod.string().nullable().optional(),
  wins: zod.number(),
  losses: zod.number(),
  draws: zod.number(),
  goals_for: zod.number(),
  goals_against: zod.number(),
  matches_played: zod.number(),
  last_calculated_at: zod.string(),
});

/**
 * @summary Paginated standings search
 */
export const StandingsSearchResponse = zod.object({
  standings: zod.array(StandingItem),
  total: zod.number(),
  page: zod.number(),
  page_size: zod.number(),
});

// ---------------------------------------------------------------------------
// Colleges (D2)
// ---------------------------------------------------------------------------

const CollegeItem = zod.object({
  id: zod.number(),
  name: zod.string(),
  slug: zod.string(),
  ncaa_id: zod.string().nullable().optional(),
  division: zod.string(),
  conference: zod.string().nullable().optional(),
  state: zod.string().nullable().optional(),
  city: zod.string().nullable().optional(),
  website: zod.string().nullable().optional(),
  soccer_program_url: zod.string().nullable().optional(),
  gender_program: zod.string(),
  enrollment: zod.number().nullable().optional(),
  scholarship_available: zod.boolean().nullable().optional(),
  logo_url: zod.string().nullable().optional(),
  twitter: zod.string().nullable().optional(),
  last_scraped_at: zod.string().nullable().optional(),
  scrape_confidence: zod.number().nullable().optional(),
});

/**
 * @summary Paginated college list
 */
export const CollegeListResponse = zod.object({
  colleges: zod.array(CollegeItem),
  total: zod.number(),
  page: zod.number(),
  page_size: zod.number(),
});

/**
 * @summary Single college detail
 */
export const CollegeDetailResponse = CollegeItem;

const CollegeCoachItem = zod.object({
  id: zod.number(),
  college_id: zod.number(),
  coach_id: zod.number().nullable().optional(),
  name: zod.string(),
  title: zod.string().nullable().optional(),
  email: zod.string().nullable().optional(),
  phone: zod.string().nullable().optional(),
  twitter: zod.string().nullable().optional(),
  linkedin: zod.string().nullable().optional(),
  is_head_coach: zod.boolean(),
  source: zod.string().nullable().optional(),
  source_url: zod.string().nullable().optional(),
  scraped_at: zod.string().nullable().optional(),
  confidence: zod.number().nullable().optional(),
  first_seen_at: zod.string().nullable().optional(),
  last_seen_at: zod.string().nullable().optional(),
});

/**
 * @summary Coaches for a college
 */
export const CollegeCoachesResponse = zod.object({
  coaches: zod.array(CollegeCoachItem),
});

const CollegeRosterItem = zod.object({
  id: zod.number(),
  college_id: zod.number(),
  player_name: zod.string(),
  position: zod.string().nullable().optional(),
  year: zod.string().nullable().optional(),
  academic_year: zod.string(),
  hometown: zod.string().nullable().optional(),
  prev_club: zod.string().nullable().optional(),
  jersey_number: zod.string().nullable().optional(),
  scraped_at: zod.string().nullable().optional(),
});

/**
 * @summary Paginated roster history for a college
 */
export const CollegeRostersResponse = zod.object({
  roster: zod.array(CollegeRosterItem),
  total: zod.number(),
  page: zod.number(),
  page_size: zod.number(),
});

// ---------------------------------------------------------------------------
// D7 — Tryout listings
// ---------------------------------------------------------------------------

/** Shape of a single tryout row returned by the API. */
export const TryoutItem = zod.object({
  id: zod.number(),
  club_id: zod.number().nullable().optional(),
  club_name_raw: zod.string(),
  age_group: zod.string().nullable().optional(),
  gender: zod.string().nullable().optional(),
  division: zod.string().nullable().optional(),
  tryout_date: zod.string().nullable().optional(),
  registration_deadline: zod.string().nullable().optional(),
  location_name: zod.string().nullable().optional(),
  location_address: zod.string().nullable().optional(),
  location_city: zod.string().nullable().optional(),
  location_state: zod.string().nullable().optional(),
  cost: zod.string().nullable().optional(),
  url: zod.string().nullable().optional(),
  notes: zod.string().nullable().optional(),
  source: zod.string(),
  status: zod.string(),
  detected_at: zod.string().optional(),
  scraped_at: zod.string().optional(),
  expires_at: zod.string().nullable().optional(),
});

/**
 * Public, consumer-safe shape of a tryout row.
 *
 * This is the contract the Player Platform consumes via the
 * `/api/tryouts/search` and `/api/tryouts/upcoming` endpoints. It
 * intentionally omits internal-only columns: `site_change_id`,
 * `scraped_at`, `detected_at`, `expires_at`. Treat it as stable —
 * adding new fields is backwards-compatible, but removing/renaming
 * existing ones is a breaking change for the Player Platform.
 */
export const TryoutPublic = zod.object({
  id: zod.number(),
  club_id: zod.number().nullable().optional(),
  club_name_raw: zod.string(),
  age_group: zod.string().nullable().optional(),
  gender: zod.string().nullable().optional(),
  division: zod.string().nullable().optional(),
  tryout_date: zod.string().nullable().optional(),
  registration_deadline: zod.string().nullable().optional(),
  location_name: zod.string().nullable().optional(),
  location_address: zod.string().nullable().optional(),
  location_city: zod.string().nullable().optional(),
  location_state: zod.string().nullable().optional(),
  cost: zod.string().nullable().optional(),
  url: zod.string().nullable().optional(),
  notes: zod.string().nullable().optional(),
  source: zod.string(),
  status: zod.string(),
});

/**
 * Query parameters for GET /api/tryouts/search.
 *
 * `date_from` / `date_to` are inclusive ISO-8601 date bounds applied
 * to `tryout_date`. The endpoint also unconditionally floors results
 * at "now" — past-dated rows are never returned even if `status` is
 * stale. Page size is capped at 100.
 */
export const TryoutSearchParams = zod.object({
  club_name: zod.string().optional(),
  age_group: zod.string().optional(),
  gender: zod.string().optional(),
  state: zod.string().optional(),
  status: zod.string().optional(),
  source: zod.string().optional(),
  date_from: zod.string().optional(),
  date_to: zod.string().optional(),
  page: zod.coerce.number().optional(),
  page_size: zod.coerce.number().optional(),
});

/** Paginated response for tryout search / listing endpoints. */
export const TryoutSearchResponse = zod.object({
  items: zod.array(TryoutPublic),
  total: zod.number(),
  page: zod.number(),
  page_size: zod.number(),
});

/** Body for POST /api/tryouts/submit (manual tryout submission). */
export const TryoutSubmitBody = zod.object({
  club_name_raw: zod.string().min(1),
  age_group: zod.string().optional(),
  gender: zod.string().optional(),
  tryout_date: zod.string().optional(),
  location_name: zod.string().optional(),
  location_city: zod.string().optional(),
  location_state: zod.string().optional(),
  url: zod.string().optional(),
  notes: zod.string().optional(),
});

/** Response for GET /api/tryouts/stats. */
export const TryoutStatsResponse = zod.object({
  total: zod.number(),
  by_status: zod.record(zod.string(), zod.number()),
  by_source: zod.record(zod.string(), zod.number()),
});
