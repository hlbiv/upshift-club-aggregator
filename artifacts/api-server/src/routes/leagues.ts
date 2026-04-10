import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import {
  leaguesMaster,
  canonicalClubs,
  clubAffiliations,
} from "@workspace/db/schema";
import { eq, inArray, asc } from "drizzle-orm";
import { ListLeaguesResponse, GetLeagueClubsResponse } from "@workspace/api-zod";

const router: IRouter = Router();

router.get("/leagues", async (_req, res, next): Promise<void> => {
  try {
    const rows = await db
      .select()
      .from(leaguesMaster)
      .orderBy(asc(leaguesMaster.tierNumeric), asc(leaguesMaster.leagueName));

    res.json(
      ListLeaguesResponse.parse({
        leagues: rows.map((r) => ({
          id: r.id,
          league_name: r.leagueName,
          league_family: r.leagueFamily,
          governing_body: r.governingBody ?? "",
          tier_numeric: r.tierNumeric ?? 0,
          tier_label: r.tierLabel ?? "",
          gender: r.gender ?? "",
          geographic_scope: r.geographicScope ?? "",
          has_public_clubs: r.hasPublicClubs ?? false,
          scrape_priority: r.scrapePriority ?? "",
          source_type: r.sourceType ?? "",
          official_url: r.officialUrl ?? "",
          notes: r.notes ?? "",
        })),
      }),
    );
  } catch (err) {
    next(err);
  }
});

router.get("/leagues/:id/clubs", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const [league] = await db
      .select()
      .from(leaguesMaster)
      .where(eq(leaguesMaster.id, id));

    if (!league) {
      res.status(404).json({ error: "League not found" });
      return;
    }

    const affiliationRows = await db
      .select({ clubId: clubAffiliations.clubId })
      .from(clubAffiliations)
      .where(eq(clubAffiliations.sourceName, league.leagueName));

    const clubIds = affiliationRows
      .map((r) => r.clubId!)
      .filter(Boolean);

    if (clubIds.length === 0) {
      res.json(GetLeagueClubsResponse.parse({ league_id: id, clubs: [] }));
      return;
    }

    const clubs = await db
      .select()
      .from(canonicalClubs)
      .where(inArray(canonicalClubs.id, clubIds))
      .orderBy(asc(canonicalClubs.clubNameCanonical));

    res.json(
      GetLeagueClubsResponse.parse({
        league_id: id,
        clubs: clubs.map((r) => ({
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

export default router;
