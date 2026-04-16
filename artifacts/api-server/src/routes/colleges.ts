import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import {
  colleges,
  collegeCoaches,
  collegeRosterHistory,
} from "@workspace/db/schema";
import { eq, ilike, and, asc, sql } from "drizzle-orm";
import {
  CollegeListResponse,
  CollegeDetailResponse,
  CollegeCoachesResponse,
  CollegeRostersResponse,
} from "@hlbiv/api-zod";
import { parsePagination, buildWhere } from "../lib/pagination";

const router: IRouter = Router();

/**
 * Escape special LIKE pattern characters so user input is treated literally.
 * Without this, a search for "50%" matches everything.
 */
function escapeLike(raw: string): string {
  return raw.replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_");
}

// ---------------------------------------------------------------------------
// Row mappers — Drizzle camelCase → API snake_case
// ---------------------------------------------------------------------------

function mapCollegeRow(r: typeof colleges.$inferSelect) {
  return {
    id: r.id,
    name: r.name,
    slug: r.slug,
    ncaa_id: r.ncaaId ?? null,
    division: r.division,
    conference: r.conference ?? null,
    state: r.state ?? null,
    city: r.city ?? null,
    website: r.website ?? null,
    soccer_program_url: r.soccerProgramUrl ?? null,
    gender_program: r.genderProgram,
    enrollment: r.enrollment ?? null,
    scholarship_available: r.scholarshipAvailable ?? null,
    logo_url: r.logoUrl ?? null,
    twitter: r.twitter ?? null,
    last_scraped_at: r.lastScrapedAt ? r.lastScrapedAt.toISOString() : null,
    scrape_confidence: r.scrapeConfidence ?? null,
  };
}

function mapCollegeCoachRow(r: typeof collegeCoaches.$inferSelect) {
  return {
    id: r.id,
    college_id: r.collegeId,
    coach_id: r.coachId ?? null,
    name: r.name,
    title: r.title ?? null,
    email: r.email ?? null,
    phone: r.phone ?? null,
    twitter: r.twitter ?? null,
    linkedin: r.linkedin ?? null,
    is_head_coach: r.isHeadCoach,
    source: r.source ?? null,
    source_url: r.sourceUrl ?? null,
    scraped_at: r.scrapedAt ? r.scrapedAt.toISOString() : null,
    confidence: r.confidence ?? null,
    first_seen_at: r.firstSeenAt ? r.firstSeenAt.toISOString() : null,
    last_seen_at: r.lastSeenAt ? r.lastSeenAt.toISOString() : null,
  };
}

function mapRosterRow(r: typeof collegeRosterHistory.$inferSelect) {
  return {
    id: r.id,
    college_id: r.collegeId,
    player_name: r.playerName,
    position: r.position ?? null,
    year: r.year ?? null,
    academic_year: r.academicYear,
    hometown: r.hometown ?? null,
    prev_club: r.prevClub ?? null,
    jersey_number: r.jerseyNumber ?? null,
    scraped_at: r.scrapedAt ? r.scrapedAt.toISOString() : null,
  };
}

router.get("/colleges", async (req, res, next): Promise<void> => {
  try {
    const q = req.query.q as string | undefined;
    const division = req.query.division as string | undefined;
    const state = req.query.state as string | undefined;
    const genderProgram = req.query.gender_program as string | undefined;
    const conference = req.query.conference as string | undefined;
    const scholarshipRaw = req.query.scholarship_available as string | undefined;
    const scholarshipAvailable =
      scholarshipRaw === "true"
        ? true
        : scholarshipRaw === "false"
          ? false
          : undefined;

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const where = buildWhere([
      q ? ilike(colleges.name, `%${escapeLike(q)}%`) : undefined,
      division ? eq(colleges.division, division) : undefined,
      state ? ilike(colleges.state, state) : undefined,
      genderProgram ? eq(colleges.genderProgram, genderProgram) : undefined,
      conference
        ? ilike(colleges.conference, `%${escapeLike(conference)}%`)
        : undefined,
      scholarshipAvailable !== undefined
        ? eq(colleges.scholarshipAvailable, scholarshipAvailable)
        : undefined,
    ]);

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(colleges)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select()
      .from(colleges)
      .where(where)
      .orderBy(asc(colleges.name))
      .limit(pageSize)
      .offset(offset);

    res.json(
      CollegeListResponse.parse({
        colleges: rows.map(mapCollegeRow),
        total,
        page,
        page_size: pageSize,
      }),
    );
  } catch (err) {
    next(err);
  }
});

router.get("/colleges/:id", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const [college] = await db
      .select()
      .from(colleges)
      .where(eq(colleges.id, id));

    if (!college) {
      res.status(404).json({ error: "College not found" });
      return;
    }

    res.json(CollegeDetailResponse.parse(mapCollegeRow(college)));
  } catch (err) {
    next(err);
  }
});

router.get("/colleges/:id/coaches", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const [college] = await db
      .select({ id: colleges.id })
      .from(colleges)
      .where(eq(colleges.id, id));

    if (!college) {
      res.status(404).json({ error: "College not found" });
      return;
    }

    const rows = await db
      .select()
      .from(collegeCoaches)
      .where(eq(collegeCoaches.collegeId, id))
      .orderBy(asc(collegeCoaches.name));

    res.json(CollegeCoachesResponse.parse({ coaches: rows.map(mapCollegeCoachRow) }));
  } catch (err) {
    next(err);
  }
});

router.get("/colleges/:id/rosters", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const [college] = await db
      .select({ id: colleges.id })
      .from(colleges)
      .where(eq(colleges.id, id));

    if (!college) {
      res.status(404).json({ error: "College not found" });
      return;
    }

    const academicYear = req.query.academic_year as string | undefined;
    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const where = buildWhere([
      eq(collegeRosterHistory.collegeId, id),
      academicYear
        ? eq(collegeRosterHistory.academicYear, academicYear)
        : undefined,
    ]);

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(collegeRosterHistory)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select()
      .from(collegeRosterHistory)
      .where(where)
      .orderBy(asc(collegeRosterHistory.playerName))
      .limit(pageSize)
      .offset(offset);

    res.json(
      CollegeRostersResponse.parse({
        roster: rows.map(mapRosterRow),
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
