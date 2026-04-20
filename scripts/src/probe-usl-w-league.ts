/**
 * USL W League Phase 0 — uslwleague.com reconnaissance probe
 *
 * Naive fetches of https://uslwleague.com/ currently return HTTP 403. This
 * probe distinguishes the three possible causes:
 *
 *   - hard-go  → UA filtering only (UA-shod fetch unblocks), or clean
 *                server-rendered HTML, or Modular11 iframe/XHR hints, or a
 *                discoverable JSON API. Extractor can be built with native
 *                `fetch` + Cheerio (or clone of scraper/extractors/usl_academy.py).
 *   - soft-go  → Site reachable, data present, but only rendered via full
 *                browser (SPA shell with no inlined JSON). Would require
 *                Playwright-in-TS — a first-in-codebase precedent.
 *   - no-go    → Cloudflare JS challenge, Akamai wall, captcha, login gate,
 *                or HTTP 451. Blocked at the CDN.
 *
 * What it does:
 *   - For each target URL, runs TWO fetches back-to-back:
 *       1. Naive fetch (minimal headers, no UA) — reproduces the initial 403.
 *       2. UA-shod fetch (Chrome UA + Accept headers) — does UA alone unblock?
 *   - Captures status, elapsed ms, content-length, response headers, and
 *     the first 500 chars of each response body for eyeballing.
 *   - Regex-searches response bodies for `modular11.com` hints (iframe src,
 *     script src, fetch-looking URLs).
 *   - Runs body classifier against known challenge / shell patterns.
 *   - Writes a JSON report to /tmp/usl-w-league-probe-<timestamp>.json and
 *     prints a human-readable summary to stdout.
 *
 * Scope cuts: no browser, no DB writes, no new deps, no extractor. Purely a
 * reconnaissance HTTP GET.
 *
 * Usage:
 *   pnpm --filter @workspace/scripts run probe-usl-w-league
 *   pnpm --filter @workspace/scripts run probe-usl-w-league -- --url https://uslwleague.com/
 *   pnpm --filter @workspace/scripts run probe-usl-w-league -- --extra-url https://uslwleague.com/standings
 */

import { writeFileSync } from "node:fs";
import { join } from "node:path";

// ---- Config ----------------------------------------------------------------

// Headers we specifically inspect for CDN / bot-wall fingerprinting. All
// other response headers are still captured in the JSON report.
const HEADERS_OF_INTEREST = [
  "server",
  "via",
  "cf-ray",
  "cf-cache-status",
  "cf-mitigated",
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
  "retry-after",
  "location",
];

// Per-request budget. If the bot-wall hangs us, don't wait forever.
const REQUEST_TIMEOUT_MS = 15_000;

// Body snippet size captured for eyeballing.
const BODY_SNIPPET_CHARS = 500;

// Realistic browser-ish UA — matches the Chrome 124 / macOS UA used by
// probe-hudl-fan.ts. Not trying to evade fingerprinting, just avoiding
// being flagged as a trivially-identifiable bot.
const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

// Modular11 is the platform behind USL Academy League (see
// scraper/extractors/usl_academy.py). If USL W League uses the same stack
// the extractor is a clean clone — that's the hard-go lane.
const MODULAR11_RE = /modular11\.com/gi;

// Cloudflare challenge markers — both inside the body and in headers.
const CF_CHALLENGE_RE = /cf-chl|challenge-platform|__cf_chl_|cf_chl_opt/i;

// Akamai / generic WAF block page markers.
const WAF_BLOCK_RE =
  /access denied|pardon our interruption|request blocked|are you a robot|reference #[0-9a-f]+/i;

// SPA empty-shell markers — near-empty body with a root div that hydrates
// client-side. If UA-shod fetch returns one of these, the site is renderable
// only under a full browser → soft-go.
const SPA_SHELL_RE = /<div[^>]+id=["'](root|app|__next|__nuxt|svelte)["']/i;
const APP_ROOT_RE = /<app-root[\s>]/i;

// ---- Types -----------------------------------------------------------------

type FetchMode = "naive" | "ua-shod";

interface SingleFetchResult {
  mode: FetchMode;
  status: number | null;
  statusText: string | null;
  elapsedMs: number;
  contentLength: number | null;
  headers: Record<string, string>;
  headersOfInterest: Record<string, string | null>;
  bodySnippet: string;
  bodySnippetBytes: number;
  error: string | null;
  bodyClass: BodyClass;
  /** Count of `modular11.com` substring occurrences in the response body. */
  modular11Matches: number;
  /**
   * Up to 5 example snippets surrounding each `modular11.com` occurrence,
   * 80 chars on either side. Empty if modular11Matches === 0.
   */
  modular11Contexts: string[];
}

interface UrlProbeResult {
  url: string;
  label: string;
  naive: SingleFetchResult;
  uaShod: SingleFetchResult;
  /**
   * Whether UA-shod fetch produced a different status than naive — if true,
   * the site is doing UA filtering and the hard-go lane is open.
   */
  uaUnblocks: boolean;
}

type BodyClass =
  | "real-html"
  | "js-challenge"
  | "bot-wall"
  | "empty-shell"
  | "redirect"
  | "error"
  | "unknown";

type Decision = "hard-go" | "soft-go" | "no-go" | "inconclusive";

interface ProbeReport {
  ranAt: string;
  nodeVersion: string;
  platform: string;
  userAgent: string;
  note: string;
  results: UrlProbeResult[];
  decision: Decision;
  decisionRationale: string;
  modular11HintsAnywhere: boolean;
}

// ---- Arg parsing -----------------------------------------------------------

interface CliArgs {
  extraUrls: string[];
  onlyUrls: string[]; // if set, ignore the default patterns
}

function parseArgs(argv: string[]): CliArgs {
  const args: CliArgs = {
    extraUrls: [],
    onlyUrls: [],
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--url" && i + 1 < argv.length) {
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
    "probe-usl-w-league — Phase 0 uslwleague.com reconnaissance probe",
    "",
    "Usage:",
    "  pnpm --filter @workspace/scripts run probe-usl-w-league [options]",
    "",
    "Options:",
    "  --url <url>          Probe ONLY this URL (repeatable). Skips defaults.",
    "  --extra-url <url>    Probe this URL in addition to defaults (repeatable).",
    "  -h, --help           Show this help.",
    "",
    "The probe runs BOTH a naive (minimal-headers) and a UA-shod (Chrome UA)",
    "fetch against every URL and lands on one of three classifications:",
    "  hard-go  — UA filtering only, clean HTML, or Modular11 platform",
    "  soft-go  — SPA shell requiring a full browser (Playwright decision)",
    "  no-go    — Cloudflare / Akamai / captcha / login wall",
    "",
  ].join("\n");
  process.stdout.write(msg);
  process.exit(code);
}

// ---- URL patterns ----------------------------------------------------------

/**
 * Default URL sequence probed. Targets the site index plus two likely teams
 * directory / standings paths, and a per-team path guess based on common
 * wordpress/SquareSpace/Modular11 slug patterns. If none of the per-team
 * guesses 200, the operator can supply `--extra-url` once they identify a
 * real team slug from the 200'd index HTML.
 */
function buildDefaultUrls(): Array<{ url: string; label: string }> {
  return [
    { url: "https://uslwleague.com/", label: "index" },
    { url: "https://uslwleague.com/teams", label: "teams-directory" },
    { url: "https://uslwleague.com/standings", label: "standings" },
    { url: "https://uslwleague.com/schedule", label: "schedule" },
    { url: "https://uslwleague.com/clubs", label: "clubs-directory" },
  ];
}

// ---- Fetch + classification -----------------------------------------------

async function fetchOnce(
  url: string,
  mode: FetchMode,
): Promise<SingleFetchResult> {
  const start = Date.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  // Naive = minimal headers, no UA. Browsers still send some default headers,
  // but Node's fetch lets us be deliberately spare. UA-shod adds Chrome UA +
  // the Accept / Accept-Language pair a real browser sends.
  const requestHeaders: Record<string, string> =
    mode === "ua-shod"
      ? {
          "User-Agent": USER_AGENT,
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
          "Accept-Language": "en-US,en;q=0.9",
          "Accept-Encoding": "gzip, deflate, br",
          "Upgrade-Insecure-Requests": "1",
          "Sec-Fetch-Dest": "document",
          "Sec-Fetch-Mode": "navigate",
          "Sec-Fetch-Site": "none",
          "Sec-Fetch-User": "?1",
        }
      : {};

  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "manual",
      headers: requestHeaders,
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
    const { count, contexts } = scanModular11(body);

    return {
      mode,
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
      modular11Matches: count,
      modular11Contexts: contexts,
    };
  } catch (err) {
    const elapsedMs = Date.now() - start;
    const message = err instanceof Error ? err.message : String(err);
    return {
      mode,
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
      modular11Matches: 0,
      modular11Contexts: [],
    };
  } finally {
    clearTimeout(timer);
  }
}

function scanModular11(body: string): {
  count: number;
  contexts: string[];
} {
  if (!body) return { count: 0, contexts: [] };
  // Reset lastIndex in case the global regex was reused.
  MODULAR11_RE.lastIndex = 0;
  const contexts: string[] = [];
  let match: RegExpExecArray | null;
  let count = 0;
  while ((match = MODULAR11_RE.exec(body)) !== null) {
    count++;
    if (contexts.length < 5) {
      const start = Math.max(0, match.index - 80);
      const end = Math.min(body.length, match.index + match[0].length + 80);
      contexts.push(
        body
          .slice(start, end)
          .replace(/\s+/g, " ")
          .trim(),
      );
    }
  }
  return { count, contexts };
}

function classifyBody(
  status: number,
  headers: Record<string, string>,
  body: string,
): BodyClass {
  if (status >= 300 && status < 400) return "redirect";

  // Headers-first: a Cloudflare challenge 403 has cf-mitigated: challenge
  // or cf-ray + challenge markers. Akamai 403s carry server: AkamaiGHost
  // or x-akamai-* headers.
  const cfRay = headers["cf-ray"];
  const cfMitigated = headers["cf-mitigated"];
  const server = headers["server"] ?? "";
  const akamaiHeader = Object.keys(headers).some((h) =>
    h.startsWith("x-akamai"),
  );

  if (status >= 400) {
    if (
      status === 403 &&
      (CF_CHALLENGE_RE.test(body) ||
        cfMitigated === "challenge" ||
        (cfRay && CF_CHALLENGE_RE.test(body)))
    ) {
      return "js-challenge";
    }
    if (status === 403 && akamaiHeader) return "bot-wall";
    if (status === 403 && /akamaighost/i.test(server)) return "bot-wall";
    if (status === 403 && WAF_BLOCK_RE.test(body)) return "bot-wall";
    if (status === 451) return "bot-wall";
    return "error";
  }

  if (CF_CHALLENGE_RE.test(body)) return "js-challenge";
  if (WAF_BLOCK_RE.test(body)) return "bot-wall";

  // Client-side-rendered SPAs commonly ship a near-empty root shell under
  // 5KB. That's a strong signal a full browser is required.
  if (body.length < 5_120 && (SPA_SHELL_RE.test(body) || APP_ROOT_RE.test(body))) {
    return "empty-shell";
  }
  if (body.length > 2_048 && /<html/i.test(body)) {
    return "real-html";
  }
  return "unknown";
}

// ---- Decision engine -------------------------------------------------------

/**
 * Classify the overall probe into one of the tri-state outcomes. The logic
 * mirrors the escalation tree in the task brief:
 *
 *   1. If ANY URL shows Modular11 hints → hard-go (platform is known).
 *   2. If ANY URL returns real-html (server-rendered) on either fetch mode
 *      → hard-go (clone / custom parser).
 *   3. If ANY URL has naive=403 but ua-shod=200 and real-html → hard-go
 *      (UA filtering only).
 *   4. If ALL 200 responses are empty-shell SPAs → soft-go (needs browser).
 *   5. If ANY URL is js-challenge or bot-wall and no URL is reachable as
 *      real-html → no-go (CDN block).
 *   6. Else inconclusive (all errors, all 404s, etc.) — report as no-go with
 *      the specific reason in the rationale.
 */
function decide(results: UrlProbeResult[]): {
  decision: Decision;
  rationale: string;
} {
  const allFetches = results.flatMap((r) => [r.naive, r.uaShod]);

  // Rule 1: Modular11 hints anywhere → hard-go.
  const modular11Fetches = allFetches.filter((f) => f.modular11Matches > 0);
  if (modular11Fetches.length > 0) {
    const firstHit = modular11Fetches[0]!;
    return {
      decision: "hard-go",
      rationale:
        `Modular11 platform detected: ${modular11Fetches.length} fetch(es) contained ` +
        `\`modular11.com\` references in the response body. First hit's first context: ` +
        `"${firstHit.modular11Contexts[0] ?? "(context capture disabled)"}". ` +
        `This matches the platform behind scraper/extractors/usl_academy.py, so the ` +
        `extractor is a clean clone — discover the UID_event/league_id parameters and ` +
        `point at uslwleague.com's Modular11 tenant.`,
    };
  }

  // Rule 2: Real-html server-rendered body anywhere → hard-go.
  const realHtmlFetches = allFetches.filter(
    (f) => f.bodyClass === "real-html" && (f.status ?? 0) < 400,
  );
  if (realHtmlFetches.length > 0) {
    const hit = realHtmlFetches[0]!;
    // Sub-rule 2a: naive=403, ua-shod=200 on any URL → UA filtering.
    const uaUnblocks = results.some((r) => r.uaUnblocks);
    if (uaUnblocks) {
      const unblocker = results.find((r) => r.uaUnblocks)!;
      return {
        decision: "hard-go",
        rationale:
          `UA filtering detected: ${unblocker.label} returned ${unblocker.naive.status} ` +
          `on naive fetch and ${unblocker.uaShod.status} with a Chrome User-Agent. ` +
          `Real-HTML body (${hit.contentLength} bytes) renders server-side, so a ` +
          `\`fetch\`+Cheerio extractor can ship once the team-page HTML structure is ` +
          `parsed. No browser needed.`,
      };
    }
    return {
      decision: "hard-go",
      rationale:
        `Server-rendered HTML reachable: at least one URL returned real-html ` +
        `(${hit.contentLength} bytes, status ${hit.status}). A \`fetch\`+parser ` +
        `extractor can be built without Playwright. Next step: confirm roster / ` +
        `team-page structure on the 200'd URLs.`,
    };
  }

  // Rule 3: All 200s are empty-shell → soft-go.
  const twoHundreds = allFetches.filter(
    (f) => (f.status ?? 0) >= 200 && (f.status ?? 0) < 300,
  );
  if (
    twoHundreds.length > 0 &&
    twoHundreds.every((f) => f.bodyClass === "empty-shell")
  ) {
    return {
      decision: "soft-go",
      rationale:
        `SPA shell detected on all reachable URLs: every 200 response is a ` +
        `near-empty shell with a client-side root div and no inlined JSON. ` +
        `Rendering the page requires a full browser (Playwright or similar). ` +
        `Building this extractor would be a first-in-codebase Playwright-in-TS ` +
        `precedent — flag for operator decision before proceeding to Phase 1.`,
    };
  }

  // Rule 4: Any challenge or bot-wall, no real-html → no-go.
  const challenges = allFetches.filter(
    (f) => f.bodyClass === "js-challenge" || f.bodyClass === "bot-wall",
  );
  if (challenges.length > 0) {
    const hit = challenges[0]!;
    const cdnHint =
      hit.headers["cf-ray"] != null
        ? "Cloudflare (cf-ray header present)"
        : Object.keys(hit.headers).some((h) => h.startsWith("x-akamai"))
          ? "Akamai (x-akamai-* headers present)"
          : /akamaighost/i.test(hit.headers["server"] ?? "")
            ? "Akamai (AkamaiGHost server)"
            : "unknown CDN (no canonical fingerprint)";
    return {
      decision: "no-go",
      rationale:
        `Active bot-wall / JS challenge: ${hit.bodyClass} detected (status ${hit.status}). ` +
        `CDN fingerprint: ${cdnHint}. File a backlog follow-up to revisit with a ` +
        `residential-proxy vendor or a browser-automation path — neither is in scope ` +
        `for this probe.`,
    };
  }

  // Rule 5: All 403s with no challenge markers → no-go (bare WAF).
  const allFourOhThree = allFetches.every((f) => f.status === 403);
  if (allFourOhThree) {
    return {
      decision: "no-go",
      rationale:
        `All URLs returned a bare 403 on both naive and UA-shod fetches, with no ` +
        `challenge body and no CDN fingerprint headers. This is consistent with a ` +
        `geo-block, an origin-level deny, or a bot-protection vendor that returns a ` +
        `sanitized response. Manual investigation (curl with verbose TLS, different ` +
        `egress IP, browser visit) required before committing to an extractor.`,
    };
  }

  // Rule 6: Nothing reachable, nothing clear → no-go / inconclusive.
  const allErrors = allFetches.every(
    (f) => f.error != null || (f.status ?? 999) >= 500,
  );
  if (allErrors) {
    return {
      decision: "no-go",
      rationale:
        `Every fetch errored or returned 5xx. Site is unreachable from this egress, ` +
        `or the probe's timeout (${REQUEST_TIMEOUT_MS}ms) is too tight. Re-run from ` +
        `Replit production egress before filing as a hard block.`,
    };
  }

  return {
    decision: "inconclusive",
    rationale:
      `Mixed signals: no Modular11 hints, no real-html 200, no SPA shell, no ` +
      `challenge body. Hand-inspect the JSON report and decide manually.`,
  };
}

// ---- Reporting -------------------------------------------------------------

function printSummary(report: ProbeReport): void {
  const lines: string[] = [];
  lines.push("");
  lines.push("=== USL W League Phase 0 probe ===");
  lines.push(`Ran at:   ${report.ranAt}`);
  lines.push(`Node:     ${report.nodeVersion} (${report.platform})`);
  lines.push(`UA (shod): ${report.userAgent}`);
  lines.push("");
  lines.push(`DECISION: ${report.decision.toUpperCase()}`);
  lines.push(`Rationale: ${report.decisionRationale}`);
  lines.push("");
  for (const r of report.results) {
    lines.push(`--- ${r.label} (${r.url}) ---`);
    lines.push(`UA unblocks? ${r.uaUnblocks ? "YES" : "no"}`);
    for (const f of [r.naive, r.uaShod]) {
      lines.push(`  [${f.mode}]`);
      if (f.error) {
        lines.push(`    ERROR:   ${f.error}`);
        lines.push(`    Elapsed: ${f.elapsedMs}ms`);
        lines.push(`    Class:   ${f.bodyClass}`);
        continue;
      }
      lines.push(`    Status:  ${f.status} ${f.statusText ?? ""}`);
      lines.push(`    Elapsed: ${f.elapsedMs}ms`);
      lines.push(`    Size:    ${f.contentLength} bytes`);
      lines.push(`    Class:   ${f.bodyClass}`);
      if (f.modular11Matches > 0) {
        lines.push(`    Mod11:   ${f.modular11Matches} match(es)`);
      }
      const sig = Object.entries(f.headersOfInterest).filter(
        ([, v]) => v != null,
      );
      if (sig.length > 0) {
        lines.push(`    Headers:`);
        for (const [k, v] of sig) lines.push(`      ${k}: ${v}`);
      }
      lines.push(`    Snippet (first ${f.bodySnippetBytes} chars):`);
      lines.push(
        f.bodySnippet
          .split("\n")
          .map((l) => `      | ${l}`)
          .join("\n"),
      );
    }
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
      : buildDefaultUrls();

  for (const u of args.extraUrls) {
    targets.push({ url: u, label: `extra-${targets.length + 1}` });
  }

  process.stdout.write(
    `Probing ${targets.length} URL(s). For each URL the script runs BOTH a ` +
      `naive fetch and a UA-shod fetch, then classifies the aggregate result ` +
      `as hard-go | soft-go | no-go. Report will be written to /tmp.\n`,
  );

  const results: UrlProbeResult[] = [];
  for (const t of targets) {
    // Naive first, UA-shod second. Sequential — we care about the before /
    // after for each URL, and a 100ms gap between requests keeps us from
    // triggering any burst-rate limiter.
    const naive = await fetchOnce(t.url, "naive");
    const uaShod = await fetchOnce(t.url, "ua-shod");
    const uaUnblocks =
      naive.status !== uaShod.status &&
      (naive.status ?? 0) >= 400 &&
      (uaShod.status ?? 0) >= 200 &&
      (uaShod.status ?? 0) < 400;
    results.push({ url: t.url, label: t.label, naive, uaShod, uaUnblocks });
  }

  const { decision, rationale } = decide(results);

  const modular11HintsAnywhere = results.some(
    (r) => r.naive.modular11Matches > 0 || r.uaShod.modular11Matches > 0,
  );

  const report: ProbeReport = {
    ranAt: new Date().toISOString(),
    nodeVersion: process.version,
    platform: `${process.platform} ${process.arch}`,
    userAgent: USER_AGENT,
    note:
      "Phase 0 reconnaissance only. Captured headers + first " +
      `${BODY_SNIPPET_CHARS} chars of each response body, for both naive and ` +
      "UA-shod fetches. Tri-state decision: hard-go | soft-go | no-go. No DB " +
      "writes, no new deps, no browser automation.",
    results,
    decision,
    decisionRationale: rationale,
    modular11HintsAnywhere,
  };

  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outPath = join("/tmp", `usl-w-league-probe-${stamp}.json`);
  writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");

  printSummary(report);
  process.stdout.write(`\nJSON report written to: ${outPath}\n`);
}

main().catch((err: unknown) => {
  const msg = err instanceof Error ? err.stack ?? err.message : String(err);
  process.stderr.write(`probe-usl-w-league failed: ${msg}\n`);
  process.exit(1);
});
