import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import {
  canonicalClubs,
  clubAliases,
  clubAffiliations,
  coachDiscoveries,
} from "@workspace/db/schema";
import { eq, ilike, and, inArray, isNotNull, ne, sql, asc, desc } from "drizzle-orm";
import {
  ListClubsResponse,
  GetClubResponse,
  GetRelatedClubsResponse,
  ClubSearchResponse,
  ClubStaffResponse,
} from "@hlbiv/api-zod";
import { parsePagination, buildWhere } from "../lib/pagination";

const router: IRouter = Router();

const DEFAULT_PAGE_SIZE = 20;
const MAX_PAGE_SIZE = 100;

router.get("/clubs", async (req, res, next): Promise<void> => {
  try {
    const state = req.query.state as string | undefined;
    const tier = req.query.tier ? Number(req.query.tier) : undefined;
    const genderProgram = req.query.gender_program as string | undefined;
    const page = Math.max(1, Number(req.query.page) || 1);
    const pageSize = Math.min(
      MAX_PAGE_SIZE,
      Math.max(1, Number(req.query.page_size) || DEFAULT_PAGE_SIZE),
    );
    const offset = (page - 1) * pageSize;

    let clubIds: number[] | null = null;

    if (tier !== undefined || genderProgram) {
      const affWhere = [];
      if (tier !== undefined) {
        affWhere.push(eq(clubAffiliations.platformTier, String(tier)));
      }
      if (genderProgram) {
        affWhere.push(eq(clubAffiliations.genderProgram, genderProgram));
      }

      const affRows = await db
        .select({ clubId: clubAffiliations.clubId })
        .from(clubAffiliations)
        .where(and(...affWhere));

      clubIds = [...new Set(affRows.map((r) => r.clubId!))];
      if (clubIds.length === 0) {
        res.json(
          ListClubsResponse.parse({ clubs: [], total: 0, page, page_size: pageSize }),
        );
        return;
      }
    }

    const conditions = [];
    if (state) conditions.push(ilike(canonicalClubs.state, state));
    if (clubIds) conditions.push(inArray(canonicalClubs.id, clubIds));

    const where = conditions.length > 0 ? and(...conditions) : undefined;

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(canonicalClubs)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select()
      .from(canonicalClubs)
      .where(where)
      .orderBy(asc(canonicalClubs.clubNameCanonical))
      .limit(pageSize)
      .offset(offset);

    res.json(
      ListClubsResponse.parse({
        clubs: rows.map((r) => ({
          id: r.id,
          club_name_canonical: r.clubNameCanonical,
          club_slug: r.clubSlug ?? "",
          city: r.city ?? "",
          state: r.state ?? "",
          country: r.country ?? "USA",
          status: r.status ?? "active",
          website: r.website ?? null,
          website_status: r.websiteStatus ?? null,
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

router.get("/clubs/search", async (req, res, next): Promise<void> => {
  try {
    const name = req.query.name as string | undefined;
    const state = req.query.state as string | undefined;
    const league = req.query.league as string | undefined;
    const hasWebsiteRaw = req.query.has_website as string | undefined;
    const hasWebsite =
      hasWebsiteRaw === "true"
        ? true
        : hasWebsiteRaw === "false"
          ? false
          : undefined;

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    let clubIds: number[] | null = null;

    if (league) {
      const affRows = await db
        .select({ clubId: clubAffiliations.clubId })
        .from(clubAffiliations)
        .where(ilike(clubAffiliations.sourceName, `%${league}%`));
      clubIds = [...new Set(affRows.map((r) => r.clubId!).filter(Boolean))];
      if (clubIds.length === 0) {
        res.json(
          ClubSearchResponse.parse({
            clubs: [],
            total: 0,
            page,
            page_size: pageSize,
          }),
        );
        return;
      }
    }

    const where = buildWhere([
      name
        ? ilike(canonicalClubs.clubNameCanonical, `%${name}%`)
        : undefined,
      state ? ilike(canonicalClubs.state, `%${state}%`) : undefined,
      clubIds ? inArray(canonicalClubs.id, clubIds) : undefined,
      hasWebsite === true ? isNotNull(canonicalClubs.website) : undefined,
      hasWebsite === true ? ne(canonicalClubs.website, "") : undefined,
      hasWebsite === false
        ? sql`(${canonicalClubs.website} IS NULL OR ${canonicalClubs.website} = '')`
        : undefined,
    ]);

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(canonicalClubs)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select()
      .from(canonicalClubs)
      .where(where)
      .orderBy(asc(canonicalClubs.clubNameCanonical))
      .limit(pageSize)
      .offset(offset);

    res.json(
      ClubSearchResponse.parse({
        clubs: rows.map((r) => ({
          id: r.id,
          club_name_canonical: r.clubNameCanonical,
          club_slug: r.clubSlug ?? "",
          city: r.city ?? "",
          state: r.state ?? "",
          country: r.country ?? "USA",
          status: r.status ?? "active",
          website: r.website ?? null,
          website_status: r.websiteStatus ?? null,
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
// D1: Club enrichment endpoints (non-parameterized — must precede :id)
// ---------------------------------------------------------------------------

router.get("/clubs/enrichment-coverage", async (req, res, next): Promise<void> => {
  try {
    const [stats] = await db
      .select({
        total: sql<number>`count(*)::int`,
        with_logo: sql<number>`count(logo_url)::int`,
        with_instagram: sql<number>`count(instagram)::int`,
        with_facebook: sql<number>`count(facebook)::int`,
        with_twitter: sql<number>`count(twitter)::int`,
        with_website_status: sql<number>`count(website_status)::int`,
        with_staff_page: sql<number>`count(staff_page_url)::int`,
        avg_confidence: sql<number>`round(avg(scrape_confidence)::numeric, 1)`,
      })
      .from(canonicalClubs);

    res.json({
      total_clubs: stats?.total ?? 0,
      with_logo: stats?.with_logo ?? 0,
      with_socials: {
        instagram: stats?.with_instagram ?? 0,
        facebook: stats?.with_facebook ?? 0,
        twitter: stats?.with_twitter ?? 0,
      },
      with_website_status: stats?.with_website_status ?? 0,
      with_staff_page: stats?.with_staff_page ?? 0,
      avg_scrape_confidence: stats?.avg_confidence ?? null,
    });
  } catch (err) {
    next(err);
  }
});

router.get("/clubs/needing-enrichment", async (req, res, next): Promise<void> => {
  try {
    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const where = buildWhere([
      isNotNull(canonicalClubs.website),
      ne(canonicalClubs.website, ""),
      sql`(${canonicalClubs.logoUrl} IS NULL OR ${canonicalClubs.scrapeConfidence} IS NULL)`,
    ]);

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(canonicalClubs)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select({
        id: canonicalClubs.id,
        clubNameCanonical: canonicalClubs.clubNameCanonical,
        state: canonicalClubs.state,
        website: canonicalClubs.website,
        websiteStatus: canonicalClubs.websiteStatus,
        logoUrl: canonicalClubs.logoUrl,
        scrapeConfidence: canonicalClubs.scrapeConfidence,
      })
      .from(canonicalClubs)
      .where(where)
      .orderBy(asc(canonicalClubs.clubNameCanonical))
      .limit(pageSize)
      .offset(offset);

    res.json({
      clubs: rows.map((r) => ({
        id: r.id,
        club_name_canonical: r.clubNameCanonical,
        state: r.state ?? "",
        website: r.website ?? null,
        website_status: r.websiteStatus ?? null,
        logo_url: r.logoUrl ?? null,
        scrape_confidence: r.scrapeConfidence ?? null,
      })),
      total,
      page,
      page_size: pageSize,
    });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------

router.get("/clubs/:id", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const [club] = await db
      .select()
      .from(canonicalClubs)
      .where(eq(canonicalClubs.id, id));

    if (!club) {
      res.status(404).json({ error: "Club not found" });
      return;
    }

    const aliases = await db
      .select()
      .from(clubAliases)
      .where(eq(clubAliases.clubId, id));

    const affiliations = await db
      .select()
      .from(clubAffiliations)
      .where(eq(clubAffiliations.clubId, id));

    res.json(
      GetClubResponse.parse({
        id: club.id,
        club_name_canonical: club.clubNameCanonical,
        club_slug: club.clubSlug ?? "",
        city: club.city ?? "",
        state: club.state ?? "",
        country: club.country ?? "USA",
        status: club.status ?? "active",
        website: club.website ?? null,
        website_status: club.websiteStatus ?? null,
        aliases: aliases.map((a) => ({
          id: a.id,
          alias_name: a.aliasName,
          alias_slug: a.aliasSlug ?? "",
          source: a.source ?? "",
          is_official: a.isOfficial ?? false,
        })),
        affiliations: affiliations.map((a) => ({
          id: a.id,
          gender_program: a.genderProgram ?? "",
          platform_name: a.platformName ?? "",
          platform_tier: a.platformTier ?? "",
          conference_name: a.conferenceName ?? "",
          division_name: a.divisionName ?? "",
          season: a.season ?? "",
          source_url: a.sourceUrl ?? "",
          source_name: a.sourceName ?? "",
          verification_status: a.verificationStatus ?? "verified",
          notes: a.notes ?? "",
        })),
      }),
    );
  } catch (err) {
    next(err);
  }
});

router.get("/clubs/:id/related", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const [club] = await db
      .select()
      .from(canonicalClubs)
      .where(eq(canonicalClubs.id, id));

    if (!club) {
      res.status(404).json({ error: "Club not found" });
      return;
    }

    const myAffiliations = await db
      .select({ sourceName: clubAffiliations.sourceName })
      .from(clubAffiliations)
      .where(eq(clubAffiliations.clubId, id));

    const leagueNames = [
      ...new Set(myAffiliations.map((a) => a.sourceName).filter(Boolean)),
    ] as string[];

    if (leagueNames.length === 0) {
      res.json(GetRelatedClubsResponse.parse({ clubs: [] }));
      return;
    }

    const relatedIds = await db
      .select({ clubId: clubAffiliations.clubId })
      .from(clubAffiliations)
      .where(inArray(clubAffiliations.sourceName, leagueNames));

    const uniqueIds = [
      ...new Set(relatedIds.map((r) => r.clubId!).filter((cid) => cid !== id)),
    ];

    if (uniqueIds.length === 0) {
      res.json(GetRelatedClubsResponse.parse({ clubs: [] }));
      return;
    }

    const relatedClubs = await db
      .select()
      .from(canonicalClubs)
      .where(inArray(canonicalClubs.id, uniqueIds))
      .orderBy(asc(canonicalClubs.clubNameCanonical));

    res.json(
      GetRelatedClubsResponse.parse({
        clubs: relatedClubs.map((r) => ({
          id: r.id,
          club_name_canonical: r.clubNameCanonical,
          club_slug: r.clubSlug ?? "",
          city: r.city ?? "",
          state: r.state ?? "",
          country: r.country ?? "USA",
          status: r.status ?? "active",
          website: r.website ?? null,
          website_status: r.websiteStatus ?? null,
        })),
      }),
    );
  } catch (err) {
    next(err);
  }
});

router.get("/clubs/:id/staff", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const [club] = await db
      .select()
      .from(canonicalClubs)
      .where(eq(canonicalClubs.id, id));

    if (!club) {
      res.status(404).json({ error: "Club not found" });
      return;
    }

    const staff = await db
      .select()
      .from(coachDiscoveries)
      .where(eq(coachDiscoveries.clubId, id))
      .orderBy(
        desc(coachDiscoveries.confidence),
        asc(coachDiscoveries.name),
      );

    res.json(
      ClubStaffResponse.parse({
        club_id: id,
        staff: staff.map((s) => ({
          id: s.id,
          club_id: s.clubId ?? null,
          name: s.name,
          title: s.title ?? null,
          email: s.email ?? null,
          source_url: s.sourceUrl ?? null,
          scraped_at: s.scrapedAt ? s.scrapedAt.toISOString() : null,
          confidence: s.confidence ?? null,
          platform_family: s.platformFamily ?? null,
        })),
      }),
    );
  } catch (err) {
    next(err);
  }
});

router.get("/clubs/:id/enrichment", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const [club] = await db
      .select({
        id: canonicalClubs.id,
        logoUrl: canonicalClubs.logoUrl,
        foundedYear: canonicalClubs.foundedYear,
        instagram: canonicalClubs.instagram,
        facebook: canonicalClubs.facebook,
        twitter: canonicalClubs.twitter,
        staffPageUrl: canonicalClubs.staffPageUrl,
        websiteStatus: canonicalClubs.websiteStatus,
        scrapeConfidence: canonicalClubs.scrapeConfidence,
        websiteLastCheckedAt: canonicalClubs.websiteLastCheckedAt,
        lastScrapedAt: canonicalClubs.lastScrapedAt,
      })
      .from(canonicalClubs)
      .where(eq(canonicalClubs.id, id));

    if (!club) {
      res.status(404).json({ error: "Club not found" });
      return;
    }

    res.json({
      id: club.id,
      logo_url: club.logoUrl ?? null,
      founded_year: club.foundedYear ?? null,
      socials: {
        instagram: club.instagram ?? null,
        facebook: club.facebook ?? null,
        twitter: club.twitter ?? null,
      },
      staff_page_url: club.staffPageUrl ?? null,
      website_status: club.websiteStatus ?? null,
      scrape_confidence: club.scrapeConfidence ?? null,
      website_last_checked_at: club.websiteLastCheckedAt
        ? club.websiteLastCheckedAt.toISOString()
        : null,
      last_scraped_at: club.lastScrapedAt
        ? club.lastScrapedAt.toISOString()
        : null,
    });
  } catch (err) {
    next(err);
  }
});

export default router;
