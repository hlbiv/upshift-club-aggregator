import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import { events, eventTeams } from "@workspace/db/schema";
import { eq, ilike, gte, lte, sql, asc, desc, inArray } from "drizzle-orm";
import {
  EventSearchResponse,
  EventDetailResponse,
  EventBatchResponse,
  EventTeamsResponse,
} from "@hlbiv/api-zod";
import { parsePagination, buildWhere } from "../lib/pagination";

const router: IRouter = Router();

/**
 * GET /api/events/search
 *
 * Reads from `event_teams` (one row per team-in-event) joined to `events`
 * (tournament/showcase metadata). Replaces the legacy `club_events`
 * single-table route; the old table was dropped in a follow-up schema PR.
 *
 * Response shape is kept bit-for-bit compatible with EventSearchResponse:
 * callers see the same keys they saw before. A few fields no longer
 * have a column in the new model and always return null:
 *   - org_season_id (ECNL-specific; can be re-added when a scraper needs it)
 *   - conference     (absorbed into `events.division` or `event_teams.division_code`)
 *
 * Age/gender/division filters use COALESCE across both tables because
 * single-bracket events store the value on `events` while multi-bracket
 * events store the per-team value on `event_teams`.
 */

// ---------------------------------------------------------------------------
// GET /api/events/list — paginated event list for the Explorer panel
// ---------------------------------------------------------------------------

// Allowed `source` values, mirrored from the `events_source_enum` pgEnum
// declared in lib/db/src/schema/events.ts. Used to narrow request-query
// strings into the enum-typed column.
const EVENTS_SOURCE_VALUES = [
  "gotsport",
  "sincsports",
  "manual",
  "other",
] as const;
type EventsSource = (typeof EVENTS_SOURCE_VALUES)[number];
function asEventsSource(raw: string | undefined): EventsSource | undefined {
  if (!raw) return undefined;
  return (EVENTS_SOURCE_VALUES as readonly string[]).includes(raw)
    ? (raw as EventsSource)
    : undefined;
}

router.get("/events/list", async (req, res, next): Promise<void> => {
  try {
    const season = req.query.season as string | undefined;
    const source = asEventsSource(req.query.source as string | undefined);
    const league = req.query.league as string | undefined;
    const state = req.query.state as string | undefined;
    const nameQuery = req.query.name as string | undefined;
    const timeframe = req.query.timeframe as string | undefined; // "past" | "upcoming" | "all"

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const conditions = [];

    if (season) conditions.push(eq(events.season, season));
    if (source) conditions.push(eq(events.source, source));
    if (state) conditions.push(ilike(events.locationState, state));
    if (league) conditions.push(ilike(events.leagueName, `%${league.replace(/%/g, "\\%")}%`));
    if (nameQuery) conditions.push(ilike(events.name, `%${nameQuery.replace(/%/g, "\\%")}%`));

    if (timeframe === "past") {
      conditions.push(lte(events.startDate, new Date()));
    } else if (timeframe === "upcoming") {
      conditions.push(gte(events.startDate, new Date()));
    }

    const where = buildWhere(conditions);

    // Count + team counts in parallel
    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(events)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select({
        id: events.id,
        name: events.name,
        slug: events.slug,
        leagueName: events.leagueName,
        season: events.season,
        ageGroup: events.ageGroup,
        gender: events.gender,
        division: events.division,
        locationCity: events.locationCity,
        locationState: events.locationState,
        startDate: events.startDate,
        endDate: events.endDate,
        source: events.source,
        platformEventId: events.platformEventId,
        sourceUrl: events.sourceUrl,
        teamCount: sql<number>`(
          SELECT count(*)::int FROM event_teams et WHERE et.event_id = ${events.id}
        )`,
      })
      .from(events)
      .where(where)
      .orderBy(desc(events.startDate), asc(events.name))
      .limit(pageSize)
      .offset(offset);

    // Get distinct seasons + sources for filter dropdowns
    const [seasonRows, sourceRows] = await Promise.all([
      db
        .selectDistinct({ season: events.season })
        .from(events)
        .where(sql`${events.season} IS NOT NULL`)
        .orderBy(desc(events.season)),
      db
        .selectDistinct({ source: events.source })
        .from(events)
        .where(sql`${events.source} IS NOT NULL`)
        .orderBy(asc(events.source)),
    ]);

    res.json({
      events: rows.map((r) => ({
        id: r.id,
        name: r.name,
        slug: r.slug,
        league_name: r.leagueName ?? null,
        season: r.season ?? null,
        age_group: r.ageGroup ?? null,
        gender: r.gender ?? null,
        division: r.division ?? null,
        location_city: r.locationCity ?? null,
        location_state: r.locationState ?? null,
        start_date: r.startDate ? r.startDate.toISOString() : null,
        end_date: r.endDate ? r.endDate.toISOString() : null,
        source: r.source ?? null,
        platform_event_id: r.platformEventId ?? null,
        source_url: r.sourceUrl ?? null,
        team_count: r.teamCount ?? 0,
      })),
      filters: {
        seasons: seasonRows.map((r) => r.season).filter(Boolean) as string[],
        sources: sourceRows.map((r) => r.source).filter(Boolean) as string[],
      },
      total,
      page,
      page_size: pageSize,
    });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /api/events/stats — summary stats for the Events panel header
// ---------------------------------------------------------------------------

router.get("/events/stats", async (_req, res, next): Promise<void> => {
  try {
    const rows = await db.execute(sql`
      SELECT
        count(*)::int AS total_events,
        count(DISTINCT season)::int AS total_seasons,
        count(CASE WHEN start_date >= NOW() THEN 1 END)::int AS upcoming_events,
        count(CASE WHEN start_date < NOW() THEN 1 END)::int AS past_events,
        (SELECT count(*)::int FROM event_teams) AS total_teams
      FROM events
    `);

    const row = Array.isArray(rows) ? rows[0] : (rows as any).rows?.[0];

    const bySeason = await db.execute(sql`
      SELECT
        COALESCE(season, 'Unknown') AS season,
        count(*)::int AS event_count,
        (
          SELECT count(*)::int FROM event_teams et
          JOIN events e2 ON e2.id = et.event_id
          WHERE COALESCE(e2.season, 'Unknown') = COALESCE(events.season, 'Unknown')
        ) AS team_count
      FROM events
      GROUP BY season
      ORDER BY season DESC
    `);

    const seasonData = Array.isArray(bySeason) ? bySeason : (bySeason as any).rows ?? [];

    const bySource = await db.execute(sql`
      SELECT
        COALESCE(source, 'unknown') AS source,
        count(*)::int AS event_count
      FROM events
      GROUP BY source
      ORDER BY event_count DESC
    `);

    const sourceData = Array.isArray(bySource) ? bySource : (bySource as any).rows ?? [];

    res.json({
      total_events: Number(row?.total_events ?? 0),
      total_seasons: Number(row?.total_seasons ?? 0),
      upcoming_events: Number(row?.upcoming_events ?? 0),
      past_events: Number(row?.past_events ?? 0),
      total_teams: Number(row?.total_teams ?? 0),
      by_season: seasonData.map((r: any) => ({
        season: r.season,
        event_count: Number(r.event_count),
        team_count: Number(r.team_count),
      })),
      by_source: sourceData.map((r: any) => ({
        source: r.source,
        event_count: Number(r.event_count),
      })),
    });
  } catch (err) {
    next(err);
  }
});

function escapeLike(raw: string): string {
  return raw.replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_");
}

router.get("/events/search", async (req, res, next): Promise<void> => {
  try {
    const clubIdRaw = req.query.club_id as string | undefined;
    const clubId = clubIdRaw ? Number(clubIdRaw) : undefined;
    const league = req.query.league as string | undefined;
    const ageGroup = req.query.age_group as string | undefined;
    const gender = req.query.gender as string | undefined;
    const season = req.query.season as string | undefined;
    const source = req.query.source as string | undefined;
    const startDateFromRaw = req.query.start_date_from as string | undefined;
    const startDateToRaw = req.query.start_date_to as string | undefined;

    const parseDate = (raw: string | undefined): Date | undefined | null => {
      if (!raw) return undefined;
      const d = new Date(raw);
      if (isNaN(d.getTime())) return null;
      return d;
    };

    const startDateFrom = parseDate(startDateFromRaw);
    const startDateTo = parseDate(startDateToRaw);

    if (startDateFrom === null || startDateTo === null) {
      res.status(400).json({
        error: "Invalid date format. Use ISO 8601 (e.g. 2024-08-01).",
      });
      return;
    }

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    // Age / gender / source filters use COALESCE(team_value, event_value)
    // so the per-team column wins when populated and the event-level column
    // is consulted only as a fallback. Plain `OR` across both tables is too
    // broad: in a single-bracket event where `events.age_group='U15'`, an
    // `age_group=U15` query would pull in every team row even if a specific
    // team has `event_teams.age_group='U14'` (scrape drift or mixed
    // bracket). COALESCE preserves the original single-column semantics.
    const ageGroupMatch = ageGroup
      ? sql`COALESCE(${eventTeams.ageGroup}, ${events.ageGroup}) ILIKE ${
          `%${escapeLike(ageGroup)}%`
        }`
      : undefined;

    const genderMatch = gender
      ? sql`COALESCE(${eventTeams.gender}, ${events.gender}) ILIKE ${
          `%${escapeLike(gender)}%`
        }`
      : undefined;

    const sourceMatch = source
      ? sql`COALESCE(${eventTeams.sourceUrl}, ${events.sourceUrl}) ILIKE ${
          `%${escapeLike(source)}%`
        }`
      : undefined;

    const where = buildWhere([
      clubId !== undefined && !isNaN(clubId)
        ? eq(eventTeams.canonicalClubId, clubId)
        : undefined,
      league ? ilike(events.leagueName, `%${escapeLike(league)}%`) : undefined,
      ageGroupMatch,
      genderMatch,
      season ? ilike(events.season, `%${escapeLike(season)}%`) : undefined,
      sourceMatch,
      startDateFrom ? gte(events.startDate, startDateFrom) : undefined,
      startDateTo ? lte(events.startDate, startDateTo) : undefined,
    ]);

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(eventTeams)
      .innerJoin(events, eq(events.id, eventTeams.eventId))
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select({
        // event_teams columns
        teamId: eventTeams.id,
        canonicalClubId: eventTeams.canonicalClubId,
        teamAgeGroup: eventTeams.ageGroup,
        teamGender: eventTeams.gender,
        teamDivisionCode: eventTeams.divisionCode,
        teamSourceUrl: eventTeams.sourceUrl,
        // events columns
        eventLeagueName: events.leagueName,
        eventPlatformEventId: events.platformEventId,
        eventAgeGroup: events.ageGroup,
        eventGender: events.gender,
        eventDivision: events.division,
        eventSeason: events.season,
        eventStartDate: events.startDate,
        eventEndDate: events.endDate,
        eventSourceUrl: events.sourceUrl,
      })
      .from(eventTeams)
      .innerJoin(events, eq(events.id, eventTeams.eventId))
      .where(where)
      .orderBy(asc(events.startDate), asc(eventTeams.id))
      .limit(pageSize)
      .offset(offset);

    res.json(
      EventSearchResponse.parse({
        events: rows.map((r) => ({
          id: r.teamId,
          club_id: r.canonicalClubId ?? null,
          league_name: r.eventLeagueName ?? null,
          event_id: r.eventPlatformEventId ?? null,
          // Removed from model — see route-level comment above.
          org_season_id: null,
          age_group: r.teamAgeGroup ?? r.eventAgeGroup ?? null,
          gender: r.teamGender ?? r.eventGender ?? null,
          division: r.teamDivisionCode ?? r.eventDivision ?? null,
          conference: null,
          season: r.eventSeason ?? null,
          start_date: r.eventStartDate ? r.eventStartDate.toISOString() : null,
          end_date: r.eventEndDate ? r.eventEndDate.toISOString() : null,
          source_url: r.teamSourceUrl ?? r.eventSourceUrl ?? null,
        })),
        total,
        page,
        page_size: pageSize,
      }),
    );
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// Helper: map an events row to the flat event shape used in batch + detail
// ---------------------------------------------------------------------------

function mapEventRow(r: typeof events.$inferSelect) {
  return {
    id: r.id,
    name: r.name,
    slug: r.slug,
    league_name: r.leagueName ?? null,
    season: r.season ?? null,
    age_group: r.ageGroup ?? null,
    gender: r.gender ?? null,
    division: r.division ?? null,
    location_city: r.locationCity ?? null,
    location_state: r.locationState ?? null,
    start_date: r.startDate ? r.startDate.toISOString() : null,
    end_date: r.endDate ? r.endDate.toISOString() : null,
    registration_url: r.registrationUrl ?? null,
    source_url: r.sourceUrl ?? null,
    source: r.source ?? null,
    platform_event_id: r.platformEventId ?? null,
  };
}

function mapTeamRow(r: typeof eventTeams.$inferSelect) {
  return {
    id: r.id,
    event_id: r.eventId,
    canonical_club_id: r.canonicalClubId ?? null,
    team_name_raw: r.teamNameRaw,
    team_name_canonical: r.teamNameCanonical ?? null,
    age_group: r.ageGroup ?? null,
    gender: r.gender ?? null,
    division_code: r.divisionCode ?? null,
    source_url: r.sourceUrl ?? null,
  };
}

// ---------------------------------------------------------------------------
// GET /api/events/batch?ids=1,2,3
// ---------------------------------------------------------------------------

const MAX_BATCH_SIZE = 100;

router.get("/events/batch", async (req, res, next): Promise<void> => {
  try {
    const idsRaw = req.query.ids as string | undefined;
    if (!idsRaw || idsRaw.trim().length === 0) {
      res.status(400).json({ error: "ids query parameter is required" });
      return;
    }

    const parts = idsRaw.split(",").map((s) => s.trim());
    const parsed: number[] = [];
    for (const part of parts) {
      const n = Number(part);
      if (!Number.isInteger(n) || n < 0) {
        res
          .status(400)
          .json({ error: `Invalid event id: "${part}". All IDs must be non-negative integers.` });
        return;
      }
      parsed.push(n);
    }

    if (parsed.length > MAX_BATCH_SIZE) {
      res
        .status(400)
        .json({ error: `Too many IDs. Maximum is ${MAX_BATCH_SIZE}, got ${parsed.length}.` });
      return;
    }

    const rows = await db
      .select()
      .from(events)
      .where(inArray(events.id, parsed))
      .orderBy(asc(events.id));

    res.json(
      EventBatchResponse.parse({
        events: rows.map(mapEventRow),
        total: rows.length,
      }),
    );
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /api/events/:id
// ---------------------------------------------------------------------------

router.get("/events/:id", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const [event] = await db.select().from(events).where(eq(events.id, id));

    if (!event) {
      res.status(404).json({ error: "Event not found" });
      return;
    }

    const teams = await db
      .select()
      .from(eventTeams)
      .where(eq(eventTeams.eventId, id))
      .orderBy(asc(eventTeams.id));

    res.json(
      EventDetailResponse.parse({
        ...mapEventRow(event),
        teams: teams.map(mapTeamRow),
      }),
    );
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /api/events/:id/teams
// ---------------------------------------------------------------------------

router.get("/events/:id/teams", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    // Verify event exists
    const [event] = await db
      .select({ id: events.id })
      .from(events)
      .where(eq(events.id, id));

    if (!event) {
      res.status(404).json({ error: "Event not found" });
      return;
    }

    const ageGroup = req.query.age_group as string | undefined;
    const gender = req.query.gender as string | undefined;

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const where = buildWhere([
      eq(eventTeams.eventId, id),
      ageGroup
        ? ilike(eventTeams.ageGroup, `%${escapeLike(ageGroup)}%`)
        : undefined,
      gender
        ? ilike(eventTeams.gender, `%${escapeLike(gender)}%`)
        : undefined,
    ]);

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(eventTeams)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select()
      .from(eventTeams)
      .where(where)
      .orderBy(asc(eventTeams.id))
      .limit(pageSize)
      .offset(offset);

    res.json(
      EventTeamsResponse.parse({
        teams: rows.map(mapTeamRow),
        total,
        page,
        page_size: pageSize,
      }),
    );
  } catch (err) {
    next(err);
  }
});

export default router;
