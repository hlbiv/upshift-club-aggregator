import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import {
  canonicalClubs,
  clubAliases,
  clubAffiliations,
} from "@workspace/db/schema";
import { eq, ilike, and, inArray, sql, asc } from "drizzle-orm";
import {
  ListClubsResponse,
  GetClubResponse,
  GetRelatedClubsResponse,
} from "@workspace/api-zod";

const router: IRouter = Router();

const DEFAULT_PAGE_SIZE = 20;
const MAX_PAGE_SIZE = 100;

router.get("/clubs", async (req, res, next) => {
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
        const response = ListClubsResponse.parse({
          clubs: [],
          total: 0,
          page,
          page_size: pageSize,
        });
        return res.json(response);
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

    const response = ListClubsResponse.parse({
      clubs: rows.map((r) => ({
        id: r.id,
        club_name_canonical: r.clubNameCanonical,
        club_slug: r.clubSlug ?? "",
        city: r.city ?? "",
        state: r.state ?? "",
        country: r.country ?? "USA",
        status: r.status ?? "active",
      })),
      total,
      page,
      page_size: pageSize,
    });

    res.json(response);
  } catch (err) {
    next(err);
  }
});

router.get("/clubs/:id", async (req, res, next) => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) return res.status(400).json({ error: "Invalid id" });

    const [club] = await db
      .select()
      .from(canonicalClubs)
      .where(eq(canonicalClubs.id, id));

    if (!club) return res.status(404).json({ error: "Club not found" });

    const aliases = await db
      .select()
      .from(clubAliases)
      .where(eq(clubAliases.clubId, id));

    const affiliations = await db
      .select()
      .from(clubAffiliations)
      .where(eq(clubAffiliations.clubId, id));

    const response = GetClubResponse.parse({
      id: club.id,
      club_name_canonical: club.clubNameCanonical,
      club_slug: club.clubSlug ?? "",
      city: club.city ?? "",
      state: club.state ?? "",
      country: club.country ?? "USA",
      status: club.status ?? "active",
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
    });

    res.json(response);
  } catch (err) {
    next(err);
  }
});

router.get("/clubs/:id/related", async (req, res, next) => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) return res.status(400).json({ error: "Invalid id" });

    const [club] = await db
      .select()
      .from(canonicalClubs)
      .where(eq(canonicalClubs.id, id));

    if (!club) return res.status(404).json({ error: "Club not found" });

    const myAffiliations = await db
      .select({ sourceName: clubAffiliations.sourceName })
      .from(clubAffiliations)
      .where(eq(clubAffiliations.clubId, id));

    const leagueNames = [
      ...new Set(myAffiliations.map((a) => a.sourceName).filter(Boolean)),
    ] as string[];

    if (leagueNames.length === 0) {
      const response = GetRelatedClubsResponse.parse({ clubs: [] });
      return res.json(response);
    }

    const relatedIds = await db
      .select({ clubId: clubAffiliations.clubId })
      .from(clubAffiliations)
      .where(inArray(clubAffiliations.sourceName, leagueNames));

    const uniqueIds = [
      ...new Set(
        relatedIds.map((r) => r.clubId!).filter((cid) => cid !== id),
      ),
    ];

    if (uniqueIds.length === 0) {
      const response = GetRelatedClubsResponse.parse({ clubs: [] });
      return res.json(response);
    }

    const relatedClubs = await db
      .select()
      .from(canonicalClubs)
      .where(inArray(canonicalClubs.id, uniqueIds.slice(0, 50)))
      .orderBy(asc(canonicalClubs.clubNameCanonical));

    const response = GetRelatedClubsResponse.parse({
      clubs: relatedClubs.map((r) => ({
        id: r.id,
        club_name_canonical: r.clubNameCanonical,
        club_slug: r.clubSlug ?? "",
        city: r.city ?? "",
        state: r.state ?? "",
        country: r.country ?? "USA",
        status: r.status ?? "active",
      })),
    });

    res.json(response);
  } catch (err) {
    next(err);
  }
});

export default router;
