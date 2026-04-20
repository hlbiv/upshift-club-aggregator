/**
 * Pipeline 3 Phase 0 — fan.hudl.com egress-IP probe
 *
 * This script MUST be run from a production-egress IP (i.e. inside a deployed
 * Replit container — NOT a laptop, NOT the Replit dev shell). See
 * `docs/design/hudl-phase-0-egress.md` for the "why".
 *
 * What it does:
 *   - Fetches each URL in `URLS_TO_PROBE` via native `fetch`.
 *   - Captures status, elapsed ms, content-length, and the headers that tell
 *     us which CDN / bot-wall is in front of Hudl.
 *   - Dumps the first 500 chars of the response body so the operator can
 *     eyeball real-HTML vs. JS-challenge vs. empty-shell-needing-JS.
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

import { writeFileSync } from "node:fs";
import { join } from "node:path";

// ---- Config ----------------------------------------------------------------

// Default organization id — Concorde Fire (per video-intelligence.md in
// upshift-studio). Override with `--org-id <id>`.
const DEFAULT_ORG_ID = "65443";

// Default player id — illustrative placeholder; the profile endpoint is what
// we care about shape-wise, not this specific profile. Override with
// `--player-id <id>`.
const DEFAULT_PLAYER_ID = "placeholder";

// Headers we specifically inspect for CDN / bot-wall fingerprinting. All
// other response headers are still captured in the JSON report.
const HEADERS_OF_INTEREST = [
  "server",
  "via",
  "cf-ray",
  "cf-cache-status",
  "x-akamai-transformed",
  "x-akamai-request-id",
  "x-cache",
  "x-amz-cf-id",
  "content-type",
  "content-length",
  "set-cookie",
  "x-content-type-options",
  "x-frame-options",
  "strict-transport-security",
];

// Per-request budget. If Hudl's bot-wall hangs us, don't wait forever.
const REQUEST_TIMEOUT_MS = 15_000;

// Body snippet size captured for eyeballing.
const BODY_SNIPPET_CHARS = 500;

// Realistic browser-ish UA. Not trying to evade fingerprinting — just
// avoiding being flagged as a trivially-identifiable bot during a
// reconnaissance probe.
const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

// ---- Types -----------------------------------------------------------------

interface ProbeResult {
  url: string;
  label: string;
  status: number | null;
  statusText: string | null;
  elapsedMs: number;
  contentLength: number | null;
  headers: Record<string, string>;
  headersOfInterest: Record<string, string | null>;
  bodySnippet: string;
  bodySnippetBytes: number;
  error: string | null;
  /** Heuristic classification of the response body. */
  bodyClass: BodyClass;
}

type BodyClass =
  | "real-html"
  | "js-challenge"
  | "bot-wall"
  | "empty-shell"
  | "redirect"
  | "error"
  | "unknown";

interface ProbeReport {
  ranAt: string;
  nodeVersion: string;
  platform: string;
  userAgent: string;
  note: string;
  results: ProbeResult[];
}

// ---- Arg parsing -----------------------------------------------------------

interface CliArgs {
  orgId: string;
  playerId: string;
  extraUrls: string[];
  onlyUrls: string[]; // if set, ignore the default 3 patterns
}

function parseArgs(argv: string[]): CliArgs {
  const args: CliArgs = {
    orgId: DEFAULT_ORG_ID,
    playerId: DEFAULT_PLAYER_ID,
    extraUrls: [],
    onlyUrls: [],
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--org-id" && i + 1 < argv.length) {
      args.orgId = argv[++i]!;
    } else if (a === "--player-id" && i + 1 < argv.length) {
      args.playerId = argv[++i]!;
    } else if (a === "--url" && i + 1 < argv.length) {
      args.onlyUrls.push(argv[++i]!);
    } else if (a === "--extra-url" && i + 1 < argv.length) {
      args.extraUrls.push(argv[++i]!);
    } else if (a === "--help" || a === "-h") {
      printUsageAndExit(0);
    }
  }
  return args;
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
function buildDefaultUrls(args: CliArgs): Array<{ url: string; label: string }> {
  return [
    {
      url: `https://fan.hudl.com/profile/${encodeURIComponent(args.playerId)}`,
      label: "profile-page",
    },
    {
      url: `https://fan.hudl.com/organization/${encodeURIComponent(args.orgId)}`,
      label: "organization-page",
    },
    {
      url: "https://fan.hudl.com/",
      label: "fan-index",
    },
  ];
}

// ---- Fetch + classification -----------------------------------------------

async function probeOne(url: string, label: string): Promise<ProbeResult> {
  const start = Date.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "manual",
      headers: {
        "User-Agent": USER_AGENT,
        Accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
      },
      signal: controller.signal,
    });

    const elapsedMs = Date.now() - start;

    const headers: Record<string, string> = {};
    res.headers.forEach((v, k) => {
      headers[k.toLowerCase()] = v;
    });

    const headersOfInterest: Record<string, string | null> = {};
    for (const name of HEADERS_OF_INTEREST) {
      headersOfInterest[name] = headers[name] ?? null;
    }

    const body = await res.text();
    const snippet = body.slice(0, BODY_SNIPPET_CHARS);
    const contentLength = body.length;

    return {
      url,
      label,
      status: res.status,
      statusText: res.statusText,
      elapsedMs,
      contentLength,
      headers,
      headersOfInterest,
      bodySnippet: snippet,
      bodySnippetBytes: snippet.length,
      error: null,
      bodyClass: classifyBody(res.status, headers, body),
    };
  } catch (err) {
    const elapsedMs = Date.now() - start;
    const message = err instanceof Error ? err.message : String(err);
    return {
      url,
      label,
      status: null,
      statusText: null,
      elapsedMs,
      contentLength: null,
      headers: {},
      headersOfInterest: {},
      bodySnippet: "",
      bodySnippetBytes: 0,
      error: message,
      bodyClass: "error",
    };
  } finally {
    clearTimeout(timer);
  }
}

function classifyBody(
  status: number,
  headers: Record<string, string>,
  body: string,
): BodyClass {
  if (status >= 300 && status < 400) return "redirect";
  if (status >= 400) {
    // Cloudflare challenge pages are 403 with specific markers.
    if (status === 403 && /cf-chl|challenge-platform|__cf_chl_/i.test(body)) {
      return "js-challenge";
    }
    if (status === 451) return "bot-wall";
    return "error";
  }

  const lower = body.toLowerCase();
  if (
    lower.includes("cf-chl") ||
    lower.includes("challenge-platform") ||
    lower.includes("__cf_chl_")
  ) {
    return "js-challenge";
  }
  if (
    lower.includes("access denied") ||
    lower.includes("pardon our interruption") ||
    lower.includes("request blocked") ||
    lower.includes("are you a robot") ||
    /akamai/.test(headers["server"] ?? "") && status === 403
  ) {
    return "bot-wall";
  }
  // Client-side-rendered SPAs commonly ship a near-empty <div id="root"></div>
  // shell under 2KB. That's a strong signal we need Playwright.
  if (body.length < 2_048 && /<div[^>]+id=["'](root|app|__next)["']/i.test(body)) {
    return "empty-shell";
  }
  if (body.length > 2_048 && /<html/i.test(body)) {
    return "real-html";
  }
  return "unknown";
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
    if (r.error) {
      lines.push(`ERROR:     ${r.error}`);
      lines.push(`Elapsed:   ${r.elapsedMs}ms`);
      lines.push(`Class:     ${r.bodyClass}`);
      lines.push("");
      continue;
    }
    lines.push(`Status:    ${r.status} ${r.statusText ?? ""}`);
    lines.push(`Elapsed:   ${r.elapsedMs}ms`);
    lines.push(`Body size: ${r.contentLength} bytes`);
    lines.push(`Class:     ${r.bodyClass}`);
    lines.push("Headers of interest:");
    for (const [k, v] of Object.entries(r.headersOfInterest)) {
      if (v != null) lines.push(`  ${k}: ${v}`);
    }
    lines.push(`Body snippet (first ${r.bodySnippetBytes} chars):`);
    lines.push(
      r.bodySnippet
        .split("\n")
        .map((l) => `  | ${l}`)
        .join("\n"),
    );
    lines.push("");
  }
  process.stdout.write(lines.join("\n") + "\n");
}

// ---- Main ------------------------------------------------------------------

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));

  const targets: Array<{ url: string; label: string }> =
    args.onlyUrls.length > 0
      ? args.onlyUrls.map((u, i) => ({ url: u, label: `custom-${i + 1}` }))
      : buildDefaultUrls(args);

  for (const u of args.extraUrls) {
    targets.push({ url: u, label: `extra-${targets.length + 1}` });
  }

  process.stdout.write(
    `Probing ${targets.length} URL(s). Remember: this must run from ` +
      `production-egress, not a laptop. See docs/design/hudl-phase-0-egress.md.\n`,
  );

  const results: ProbeResult[] = [];
  for (const t of targets) {
    const r = await probeOne(t.url, t.label);
    results.push(r);
  }

  const report: ProbeReport = {
    ranAt: new Date().toISOString(),
    nodeVersion: process.version,
    platform: `${process.platform} ${process.arch}`,
    userAgent: USER_AGENT,
    note:
      "Phase 0 reconnaissance only. Captured headers + first " +
      `${BODY_SNIPPET_CHARS} chars of each response body. No video download, no ` +
      "DB writes, no secrets used.",
    results,
  };

  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outPath = join("/tmp", `hudl-fan-probe-${stamp}.json`);
  writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");

  printSummary(report);
  process.stdout.write(`\nJSON report written to: ${outPath}\n`);
}

main().catch((err: unknown) => {
  const msg = err instanceof Error ? err.stack ?? err.message : String(err);
  process.stderr.write(`probe-hudl-fan failed: ${msg}\n`);
  process.exit(1);
});
