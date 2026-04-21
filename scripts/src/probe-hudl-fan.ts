/**
 * Pipeline 3 Phase 0 — fan.hudl.com egress-IP probe
 *
 * This script MUST be run from a production-egress IP (i.e. inside a deployed
 * Replit container — NOT a laptop, NOT the Replit dev shell). See
 * `docs/design/hudl-phase-0-egress.md` for the "why".
 *
 * What it does:
 *   - Fetches each URL in the target list via native `fetch` with a single
 *     light-UA header set (Chrome UA + Accept + Accept-Language only).
 *   - Captures status, elapsed ms, content-length, response headers, the
 *     first 500 chars of body, and a heuristic body classification.
 *   - Writes a JSON report to /tmp/hudl-fan-probe-<timestamp>.json and prints
 *     a human-readable summary to stdout.
 *
 * Scope cuts: no yt-dlp, no video download, no embed-following, no DB writes,
 * no secrets. Purely a reconnaissance HTTP GET.
 *
 * Usage:
 *   pnpm --filter @workspace/scripts run probe-hudl-fan
 *   pnpm --filter @workspace/scripts run probe-hudl-fan -- --org-id 12345
 *   pnpm --filter @workspace/scripts run probe-hudl-fan -- --url https://example.com
 */

import {
  buildProbeUrlList,
  buildReportHeader,
  fetchOnce,
  formatFetchBlockLines,
  lightUaHeaders,
  parseCommonProbeArgs,
  writeJsonReport,
  type BaseProbeReportHeader,
  type SingleFetchResult,
} from "./lib/spa-probe.js";

// ---- Config ----------------------------------------------------------------

// Default organization id — Concorde Fire (per video-intelligence.md in
// upshift-studio). Override with `--org-id <id>`.
const DEFAULT_ORG_ID = "65443";

// Default player id — illustrative placeholder; the profile endpoint is what
// we care about shape-wise, not this specific profile. Override with
// `--player-id <id>`.
const DEFAULT_PLAYER_ID = "placeholder";

// ---- Types -----------------------------------------------------------------

interface ProbeResult extends SingleFetchResult {
  url: string;
  label: string;
}

interface ProbeReport extends BaseProbeReportHeader {
  results: ProbeResult[];
}

// ---- Arg parsing -----------------------------------------------------------

interface HudlArgs {
  orgId: string;
  playerId: string;
  onlyUrls: string[];
  extraUrls: string[];
}

function parseArgs(argv: string[]): HudlArgs {
  let orgId = DEFAULT_ORG_ID;
  let playerId = DEFAULT_PLAYER_ID;
  const common = parseCommonProbeArgs(argv, printUsageAndExit, (a, next, advance) => {
    if (a === "--org-id" && next !== undefined) {
      orgId = next;
      advance();
      return true;
    }
    if (a === "--player-id" && next !== undefined) {
      playerId = next;
      advance();
      return true;
    }
    return false;
  });
  return { orgId, playerId, ...common };
}

function printUsageAndExit(code: number): never {
  const msg = [
    "probe-hudl-fan — Pipeline 3 Phase 0 egress-IP probe",
    "",
    "Usage:",
    "  pnpm --filter @workspace/scripts run probe-hudl-fan [options]",
    "",
    "Options:",
    `  --org-id <id>        Hudl organization id (default: ${DEFAULT_ORG_ID})`,
    `  --player-id <id>     Hudl player id (default: ${DEFAULT_PLAYER_ID})`,
    "  --url <url>          Probe ONLY this URL (repeatable). Skips defaults.",
    "  --extra-url <url>    Probe this URL in addition to defaults (repeatable).",
    "  -h, --help           Show this help.",
    "",
    "IMPORTANT: run this from the deployed Replit app, not from a laptop or",
    "the Replit dev shell. See docs/design/hudl-phase-0-egress.md.",
    "",
  ].join("\n");
  process.stdout.write(msg);
  process.exit(code);
}

// ---- URL patterns ----------------------------------------------------------

/**
 * The 3 URL patterns probed by default. Drawn from the Hudl "fan" surface
 * documented in docs/design/data-sources-backlog.md (§Hudl) and the Concorde
 * Fire org-id reference in upshift-studio's video-intelligence.md.
 *
 * Phase 0 questions these answer:
 *   1. Is the profile page reachable from prod egress?
 *   2. Does the org-scoped page (team/roster view) render server-side?
 *   3. Does the Hudl fan index / marketing surface differ in CDN treatment?
 */
function buildDefaultUrls(args: HudlArgs): Array<{ url: string; label: string }> {
  return [
    {
      url: `https://fan.hudl.com/profile/${encodeURIComponent(args.playerId)}`,
      label: "profile-page",
    },
    {
      url: `https://fan.hudl.com/organization/${encodeURIComponent(args.orgId)}`,
      label: "organization-page",
    },
    { url: "https://fan.hudl.com/", label: "fan-index" },
  ];
}

// ---- Reporting -------------------------------------------------------------

function printSummary(report: ProbeReport): void {
  const lines: string[] = [];
  lines.push("");
  lines.push("=== Hudl fan.hudl.com Phase 0 probe ===");
  lines.push(`Ran at:   ${report.ranAt}`);
  lines.push(`Node:     ${report.nodeVersion} (${report.platform})`);
  lines.push(`UA:       ${report.userAgent}`);
  lines.push("");
  for (const r of report.results) {
    lines.push(`--- ${r.label} ---`);
    lines.push(`URL:       ${r.url}`);
    lines.push(...formatFetchBlockLines(r, "  "));
    lines.push("");
  }
  process.stdout.write(lines.join("\n") + "\n");
}

// ---- Main ------------------------------------------------------------------

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const targets = buildProbeUrlList(args, buildDefaultUrls(args));

  process.stdout.write(
    `Probing ${targets.length} URL(s). Remember: this must run from ` +
      `production-egress, not a laptop. See docs/design/hudl-phase-0-egress.md.\n`,
  );

  const headers = lightUaHeaders();
  const results: ProbeResult[] = [];
  for (const t of targets) {
    const r = await fetchOnce(t.url, "single", { headers });
    results.push({ ...r, url: t.url, label: t.label });
  }

  const report: ProbeReport = {
    ...buildReportHeader(
      "Phase 0 reconnaissance only. Captured headers + first 500 chars of " +
        "each response body. No video download, no DB writes, no secrets used.",
    ),
    results,
  };

  const outPath = writeJsonReport("hudl-fan-probe", report);

  printSummary(report);
  process.stdout.write(`\nJSON report written to: ${outPath}\n`);
}

main().catch((err: unknown) => {
  const msg = err instanceof Error ? err.stack ?? err.message : String(err);
  process.stderr.write(`probe-hudl-fan failed: ${msg}\n`);
  process.exit(1);
});
