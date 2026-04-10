/**
 * Seed the graph database from scraper CSV output.
 *
 * Sources:
 *   scraper/data/leagues_master.csv        → leagues_master table
 *   scraper/output/leagues/*.csv           → canonical_clubs, club_aliases, club_affiliations
 *   scraper/output/clubs_enriched.csv      → enriches city/state/zip on canonical_clubs
 *
 * Run from workspace root:
 *   pnpm --filter @workspace/db run seed
 */

import { fileURLToPath } from "url";
import path from "path";
import fs from "fs";
import { parse } from "csv-parse/sync";
import { db, pool } from "./index.js";
import {
  leaguesMaster,
  canonicalClubs,
  clubAliases,
  clubAffiliations,
} from "./schema/index.js";
import { eq, and, sql } from "drizzle-orm";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const WORKSPACE_ROOT = path.resolve(__dirname, "..", "..", "..");

function slugify(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 120);
}

function readCsv(filePath: string): Record<string, string>[] {
  if (!fs.existsSync(filePath)) return [];
  const content = fs.readFileSync(filePath, "utf-8");
  return parse(content, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_quotes: true,
  }) as Record<string, string>[];
}

async function seedLeagues(
  leagueRows: Record<string, string>[],
): Promise<Map<string, number>> {
  console.log(`Seeding ${leagueRows.length} leagues…`);
  const leagueIdMap = new Map<string, number>();

  for (const row of leagueRows) {
    const hasPublic =
      row.has_public_clubs?.toLowerCase() === "true" ? true : false;
    const tierNum = row.tier_numeric ? parseInt(row.tier_numeric, 10) : null;

    const [inserted] = await db
      .insert(leaguesMaster)
      .values({
        leagueName: row.league_name,
        leagueFamily: row.league_family || "",
        governingBody: row.governing_body || null,
        tierNumeric: isNaN(tierNum!) ? null : tierNum,
        tierLabel: row.tier_label || null,
        gender: row.gender || null,
        geographicScope: row.geographic_scope || null,
        hasPublicClubs: hasPublic,
        scrapePriority: row.scrape_priority || null,
        sourceType: row.source_type || null,
        officialUrl: row.official_url || null,
        notes: row.notes || null,
      })
      .onConflictDoUpdate({
        target: leaguesMaster.leagueName,
        set: {
          leagueFamily: sql`excluded.league_family`,
          governingBody: sql`excluded.governing_body`,
          tierNumeric: sql`excluded.tier_numeric`,
          tierLabel: sql`excluded.tier_label`,
          gender: sql`excluded.gender`,
          geographicScope: sql`excluded.geographic_scope`,
          hasPublicClubs: sql`excluded.has_public_clubs`,
          scrapePriority: sql`excluded.scrape_priority`,
          sourceType: sql`excluded.source_type`,
          officialUrl: sql`excluded.official_url`,
          notes: sql`excluded.notes`,
        },
      })
      .returning({ id: leaguesMaster.id });

    leagueIdMap.set(row.league_name, inserted.id);
  }

  console.log(`  → leagues_master: ${leagueIdMap.size} upserted`);
  return leagueIdMap;
}

async function main() {
  const leaguesCsvPath = path.join(
    WORKSPACE_ROOT,
    "scraper",
    "data",
    "leagues_master.csv",
  );
  const leaguesOutputDir = path.join(
    WORKSPACE_ROOT,
    "scraper",
    "output",
    "leagues",
  );
  const enrichedPath = path.join(
    WORKSPACE_ROOT,
    "scraper",
    "output",
    "clubs_enriched.csv",
  );

  const leagueRows = readCsv(leaguesCsvPath);
  const leagueMeta = new Map(leagueRows.map((r) => [r.league_name, r]));

  const leagueIdMap = await seedLeagues(leagueRows);

  const enrichedRows = readCsv(enrichedPath);
  const enrichmentMap = new Map<string, { city: string; state: string }>();
  for (const row of enrichedRows) {
    const key = row.club_name_official?.trim();
    if (key && (row.city || row.state)) {
      enrichmentMap.set(key.toLowerCase(), {
        city: row.city || "",
        state: row.state || "",
      });
    }
  }
  console.log(`Enrichment map: ${enrichmentMap.size} entries from clubs_enriched.csv`);

  const leagueCsvFiles = fs
    .readdirSync(leaguesOutputDir)
    .filter((f) => f.endsWith(".csv"))
    .map((f) => path.join(leaguesOutputDir, f));

  console.log(`Reading ${leagueCsvFiles.length} league CSV files…`);

  type ClubRecord = {
    clubName: string;
    canonicalName: string;
    leagueName: string;
    city: string;
    state: string;
    sourceUrl: string;
  };

  const allRows: ClubRecord[] = [];
  for (const csvFile of leagueCsvFiles) {
    const rows = readCsv(csvFile);
    for (const row of rows) {
      if (!row.canonical_name) continue;
      allRows.push({
        clubName: row.club_name || "",
        canonicalName: row.canonical_name,
        leagueName: row.league_name || "",
        city: row.city || "",
        state: row.state || "",
        sourceUrl: row.source_url || "",
      });
    }
  }
  console.log(`Total raw club-league rows: ${allRows.length}`);

  const canonicalBySlug = new Map<
    string,
    { name: string; city: string; state: string }
  >();
  for (const row of allRows) {
    const slug = slugify(row.canonicalName);
    if (!canonicalBySlug.has(slug)) {
      const enriched = enrichmentMap.get(row.clubName.toLowerCase()) ||
        enrichmentMap.get(row.canonicalName.toLowerCase());
      canonicalBySlug.set(slug, {
        name: row.canonicalName,
        city: enriched?.city || row.city || "",
        state: enriched?.state || row.state || "",
      });
    } else {
      const existing = canonicalBySlug.get(slug)!;
      if (!existing.city && row.city) existing.city = row.city;
      if (!existing.state && row.state) existing.state = row.state;
    }
  }

  console.log(`Unique canonical clubs: ${canonicalBySlug.size}`);

  const clubSlugToId = new Map<string, number>();
  let upsertedClubs = 0;

  for (const [slug, info] of canonicalBySlug) {
    const [rec] = await db
      .insert(canonicalClubs)
      .values({
        clubNameCanonical: info.name,
        clubSlug: slug,
        city: info.city || null,
        state: info.state || null,
        country: "USA",
        status: "active",
      })
      .onConflictDoUpdate({
        target: canonicalClubs.clubSlug,
        set: {
          clubNameCanonical: sql`excluded.club_name_canonical`,
          city: sql`COALESCE(NULLIF(excluded.city, ''), canonical_clubs.city)`,
          state: sql`COALESCE(NULLIF(excluded.state, ''), canonical_clubs.state)`,
        },
      })
      .returning({ id: canonicalClubs.id });

    clubSlugToId.set(slug, rec.id);
    upsertedClubs++;
  }
  console.log(`  → canonical_clubs: ${upsertedClubs} upserted`);

  const aliasInserts: Array<{
    clubId: number;
    aliasName: string;
    aliasSlug: string;
    source: string;
  }> = [];
  const affiliationInserts: Array<{
    clubId: number;
    genderProgram: string;
    platformName: string;
    platformTier: string;
    sourceName: string;
    sourceUrl: string;
  }> = [];

  const seenAliases = new Set<string>();
  const seenAffiliations = new Set<string>();

  for (const row of allRows) {
    const slug = slugify(row.canonicalName);
    const clubId = clubSlugToId.get(slug);
    if (!clubId) continue;

    if (
      row.clubName &&
      row.clubName !== row.canonicalName
    ) {
      const aliasKey = `${clubId}::${row.clubName}`;
      if (!seenAliases.has(aliasKey)) {
        seenAliases.add(aliasKey);
        aliasInserts.push({
          clubId,
          aliasName: row.clubName,
          aliasSlug: slugify(row.clubName),
          source: row.leagueName,
        });
      }
    }

    if (row.leagueName) {
      const affKey = `${clubId}::${row.leagueName}`;
      if (!seenAffiliations.has(affKey)) {
        seenAffiliations.add(affKey);
        const meta = leagueMeta.get(row.leagueName);
        affiliationInserts.push({
          clubId,
          genderProgram: meta?.gender || "",
          platformName: meta?.governing_body || meta?.league_family || "",
          platformTier: meta?.tier_numeric || "",
          sourceName: row.leagueName,
          sourceUrl: row.sourceUrl,
        });
      }
    }
  }

  if (aliasInserts.length > 0) {
    const BATCH = 500;
    for (let i = 0; i < aliasInserts.length; i += BATCH) {
      await db
        .insert(clubAliases)
        .values(aliasInserts.slice(i, i + BATCH))
        .onConflictDoNothing();
    }
    console.log(`  → club_aliases: ${aliasInserts.length} inserted`);
  }

  if (affiliationInserts.length > 0) {
    const BATCH = 500;
    for (let i = 0; i < affiliationInserts.length; i += BATCH) {
      await db
        .insert(clubAffiliations)
        .values(affiliationInserts.slice(i, i + BATCH))
        .onConflictDoNothing();
    }
    console.log(`  → club_affiliations: ${affiliationInserts.length} inserted`);
  }

  await pool.end();
  console.log("\nSeed complete.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
