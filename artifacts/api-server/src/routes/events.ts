import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import { events, eventTeams } from "@workspace/db/schema";
import { eq, ilike, gte, lte, sql, asc } from "drizzle-orm";
import { EventSearchResponse } from "@workspace/api-zod";
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

export default router;
