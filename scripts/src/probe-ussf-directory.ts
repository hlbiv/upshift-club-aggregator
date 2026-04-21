/**
 * USSF Learning Center directory — Phase 0 reconnaissance probe
 *
 * Target: https://learning.ussoccer.com/directory
 *
 * Question this probe answers: can a shell-level `fetch` client (no browser,
 * no JS execution) extract directory / coach data from the USSF Learning
 * Center? If not, document precisely what blocks it so the Phase 1 plan can
 * decide whether a headless browser (Playwright) is required.
 *
 * Escalation tree (mirrors the structure of probe-usl-w-league.ts but adds a
 * step-3 bundle inspector):
 *
 *   1. Naive fetch          — minimal headers, no UA. Reproduces a default-
 *                             client baseline. Captures status / body / class.
 *   2. UA-shod fetch        — Chrome 124 / macOS UA + realistic Accept-* and
 *                             Sec-Fetch-* headers. Tells us whether UA
 *                             filtering alone is the gate.
 *   3. Bundle inspection    — On the best 200 HTML response (UA-shod
 *                             preferred, naive fallback), grep for inlined
 *                             JSON state hooks, enumerate referenced
 *                             `<script src>` bundles, fetch each, grep for
 *                             API-shape patterns, and attempt up to N
 *                             discovered API URLs.
 *
 * The probe HARD-STOPS at step 3. No Playwright, no Puppeteer, no headless
 * browser is installed or referenced. If steps 1–3 all fail to yield data,
 * the report says so and Phase 1 owns the decision.
 *
 * Scope cuts: no DB writes, no new deps, no auth attempts, no extractor.
 * Purely a reconnaissance HTTP GET sequence.
 *
 * Usage:
 *   pnpm --filter @workspace/scripts run probe-ussf-directory
 *   pnpm --filter @workspace/scripts run probe-ussf-directory -- --url https://learning.ussoccer.com/directory
 *   pnpm --filter @workspace/scripts run probe-ussf-directory -- --extra-url https://learning.ussoccer.com/api/foo
 */

import {
  BODY_SNIPPET_CHARS,
  DEFAULT_API_PATTERNS,
  DEFAULT_STATE_HOOKS,
  buildProbeUrlList,
  buildReportHeader,
  classifyBody,
  extractScriptSrcs,
  fetchBundleLike,
  fetchOnce,
  formatFetchBlockLines,
  grepForApiPatterns,
  naiveHeaders,
  parseCommonProbeArgs,
  uaShodHeaders,
  writeJsonReport,
  type BaseProbeReportHeader,
  type BodyClass,
  type FetchMode,
  type SingleFetchResult,
} from "./lib/spa-probe.js";

// ---- Probe-specific config -------------------------------------------------

// Cap how many script bundles we follow per page. The point is to learn the
// API surface, not enumerate every chunk in a Webpack manifest.
const BUNDLE_FETCH_LIMIT = 12;
// Cap how many discovered API URLs we'll actually GET. Keeps the probe fast
// and avoids hammering newly discovered endpoints.
const API_FETCH_LIMIT = 8;

// USSF-specific API-shape patterns layered on top of the shared defaults.
// `/directory/` matches the SPA's likely data path; the bare host check
// catches bundles that hard-code their own backend URL.
const USSF_API_PATTERNS: ReadonlyArray<{ name: string; re: RegExp }> = [
  ...DEFAULT_API_PATTERNS,
  { name: "/directory/", re: /["'`](\/directory\/[A-Za-z0-9_\-\/]+)/g },
  { name: "learning.ussoccer.com host", re: /learning\.ussoccer\.com/g },
];

// ---- Types -----------------------------------------------------------------

interface UrlProbeResult {
  url: string;
  label: string;
  naive: SingleFetchResult;
  uaShod: SingleFetchResult;
  uaUnblocks: boolean;
}

interface BundleHit {
  bundleUrl: string;
  status: number | null;
  contentLength: number | null;
  contentType: string | null;
  error: string | null;
  /** Per-pattern match counts inside this bundle's body. */
  patternHits: Record<string, number>;
  /** Up to 8 example absolute or root-relative URLs this bundle references. */
  discoveredUrls: string[];
}

interface ApiAttempt {
  url: string;
  /** Source of the URL — which bundle (or `inlined-html`) it came from. */
  source: string;
  status: number | null;
  statusText: string | null;
  contentType: string | null;
  contentLength: number | null;
  bodySnippet: string;
  bodyClass: BodyClass;
  error: string | null;
}

interface BundleInspection {
  /** Which fetch result we inspected — preferred ua-shod, fall back to naive. */
  inspectedMode: FetchMode;
  inspectedUrl: string;
  inspectedStatus: number | null;
  /** State-hook markers found in the HTML body. */
  stateHooksFound: string[];
  /**
   * Captured snippet around each state-hook so we can eyeball whether the
   * inlined blob looks parseable. Up to 600 chars centered on the hit.
   */
  stateHookSnippets: Array<{ name: string; snippet: string }>;
  /** Total `<script src>` URLs discovered on the page. */
  scriptSrcsFound: number;
  /** All discovered script src URLs (resolved against the page URL). */
  scriptSrcs: string[];
  /** Per-bundle fetch + grep results. */
  bundles: BundleHit[];
  /** All distinct candidate API URLs discovered across bundles. */
  candidateApiUrls: string[];
  /** Subset that we actually attempted (capped at API_FETCH_LIMIT). */
  apiAttempts: ApiAttempt[];
  /** Whether any API attempt yielded JSON (i.e. data is reachable shell-side). */
  jsonDataReachable: boolean;
}

type Decision = "shell-fetchable" | "needs-browser" | "blocked" | "inconclusive";

interface ProbeReport extends BaseProbeReportHeader {
  results: UrlProbeResult[];
  bundleInspection: BundleInspection | null;
  decision: Decision;
  decisionRationale: string;
}

// ---- Arg parsing -----------------------------------------------------------

function printUsageAndExit(code: number): never {
  const msg = [
    "probe-ussf-directory — Phase 0 USSF Learning Center reconnaissance probe",
    "",
    "Usage:",
    "  pnpm --filter @workspace/scripts run probe-ussf-directory [options]",
    "",
    "Options:",
    "  --url <url>          Probe ONLY this URL (repeatable). Skips defaults.",
    "  --extra-url <url>    Probe this URL in addition to defaults (repeatable).",
    "  -h, --help           Show this help.",
    "",
    "For each URL the script runs a naive fetch + a UA-shod fetch, then runs",
    "a one-shot bundle inspection on the best 200 HTML response. Decision:",
    "  shell-fetchable | needs-browser | blocked | inconclusive",
    "",
    "Hard stop at step 3 — no Playwright, no Puppeteer, no headless browser.",
    "",
  ].join("\n");
  process.stdout.write(msg);
  process.exit(code);
}

// ---- Default URLs ----------------------------------------------------------

function buildDefaultUrls(): Array<{ url: string; label: string }> {
  return [
    { url: "https://learning.ussoccer.com/directory", label: "directory" },
    { url: "https://learning.ussoccer.com/", label: "index" },
  ];
}

// ---- Bundle inspection (step 3) -------------------------------------------

/**
 * Run the bundle / inlined-JSON inspection against whichever per-URL fetch
 * gave us the best 200 HTML to chew on. Preference order:
 *
 *   1. UA-shod fetch on the directory URL.
 *   2. Naive fetch on the directory URL.
 *   3. UA-shod / naive fetch on any other URL.
 *
 * If no URL returned a 200 with HTML-ish body, returns null.
 */
async function inspectBundles(
  results: UrlProbeResult[],
): Promise<BundleInspection | null> {
  type Candidate = {
    url: string;
    mode: FetchMode;
    fetchResult: SingleFetchResult;
    label: string;
  };
  const candidates: Candidate[] = [];
  for (const r of results) {
    for (const f of [r.uaShod, r.naive]) {
      if (
        f.status != null &&
        f.status >= 200 &&
        f.status < 300 &&
        (f.bodyClass === "real-html" ||
          f.bodyClass === "empty-shell" ||
          f.bodyClass === "unknown")
      ) {
        candidates.push({
          url: r.url,
          mode: f.mode,
          fetchResult: f,
          label: r.label,
        });
      }
    }
  }
  candidates.sort((a, b) => {
    const aDir = a.label === "directory" ? 0 : 1;
    const bDir = b.label === "directory" ? 0 : 1;
    if (aDir !== bDir) return aDir - bDir;
    const aMode = a.mode === "ua-shod" ? 0 : 1;
    const bMode = b.mode === "ua-shod" ? 0 : 1;
    return aMode - bMode;
  });
  const chosen = candidates[0];
  if (!chosen) return null;

  // Re-fetch the chosen URL in full so we can grep — the per-URL fetch only
  // kept a 500-char snippet.
  const refetch = await fetchBundleLike(
    chosen.url,
    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  );
  const html = refetch.body;

  // ---- State-hook detection --------------------------------------------
  const stateHooksFound: string[] = [];
  const stateHookSnippets: Array<{ name: string; snippet: string }> = [];
  for (const hook of DEFAULT_STATE_HOOKS) {
    const m = hook.re.exec(html);
    if (m) {
      stateHooksFound.push(hook.name);
      const center = m.index;
      const start = Math.max(0, center - 200);
      const end = Math.min(html.length, center + 400);
      stateHookSnippets.push({ name: hook.name, snippet: html.slice(start, end) });
    }
  }

  const scriptSrcs = extractScriptSrcs(html, chosen.url);

  const bundles: BundleHit[] = [];
  const candidateApiUrls = new Set<string>();
  const apiSourceMap = new Map<string, string>();

  // Also grep the inlined HTML — Next.js often inlines a __NEXT_DATA__ blob
  // or self.__remixContext with full API URLs.
  const inlineHits = grepForApiPatterns(html, chosen.url, {
    patterns: USSF_API_PATTERNS,
  });
  for (const u of inlineHits.candidateUrls) {
    candidateApiUrls.add(u);
    if (!apiSourceMap.has(u)) apiSourceMap.set(u, "inlined-html");
  }

  for (const src of scriptSrcs.slice(0, BUNDLE_FETCH_LIMIT)) {
    const got = await fetchBundleLike(src, "*/*");
    const patternHits: Record<string, number> = {};
    if (got.body) {
      const grep = grepForApiPatterns(got.body, chosen.url, {
        patterns: USSF_API_PATTERNS,
      });
      for (const [name, n] of Object.entries(grep.patternCounts)) {
        if (n > 0) patternHits[name] = n;
      }
      for (const u of grep.candidateUrls) {
        candidateApiUrls.add(u);
        if (!apiSourceMap.has(u)) apiSourceMap.set(u, src);
      }
      bundles.push({
        bundleUrl: src,
        status: got.status,
        contentLength: got.contentLength,
        contentType: got.contentType,
        error: got.error,
        patternHits,
        discoveredUrls: grep.candidateUrls.slice(0, 8),
      });
    } else {
      bundles.push({
        bundleUrl: src,
        status: got.status,
        contentLength: got.contentLength,
        contentType: got.contentType,
        error: got.error,
        patternHits: {},
        discoveredUrls: [],
      });
    }
  }

  const apiAttempts: ApiAttempt[] = [];
  let jsonReachable = false;
  const toAttempt = Array.from(candidateApiUrls).slice(0, API_FETCH_LIMIT);
  for (const url of toAttempt) {
    const got = await fetchBundleLike(url, "application/json,*/*;q=0.5");
    const cls = classifyBody(got.status ?? 0, got.headers, got.body);
    if (cls === "json") jsonReachable = true;
    apiAttempts.push({
      url,
      source: apiSourceMap.get(url) ?? "unknown",
      status: got.status,
      statusText: got.statusText,
      contentType: got.contentType,
      contentLength: got.contentLength,
      bodySnippet: got.body.slice(0, BODY_SNIPPET_CHARS),
      bodyClass: cls,
      error: got.error,
    });
  }

  return {
    inspectedMode: chosen.mode,
    inspectedUrl: chosen.url,
    inspectedStatus: refetch.status,
    stateHooksFound,
    stateHookSnippets,
    scriptSrcsFound: scriptSrcs.length,
    scriptSrcs,
    bundles,
    candidateApiUrls: Array.from(candidateApiUrls),
    apiAttempts,
    jsonDataReachable: jsonReachable,
  };
}

// ---- Decision engine -------------------------------------------------------

function decide(
  results: UrlProbeResult[],
  bundle: BundleInspection | null,
): { decision: Decision; rationale: string } {
  const allFetches = results.flatMap((r) => [r.naive, r.uaShod]);

  if (bundle?.jsonDataReachable) {
    const jsonHit = bundle.apiAttempts.find((a) => a.bodyClass === "json")!;
    return {
      decision: "shell-fetchable",
      rationale:
        `Step-3 bundle inspection discovered an API URL (${jsonHit.url}, source: ` +
        `${jsonHit.source}) that returned JSON (status ${jsonHit.status}, ` +
        `content-type ${jsonHit.contentType ?? "n/a"}, ${jsonHit.contentLength} bytes). ` +
        `A shell-level extractor can hit this endpoint directly — no browser needed.`,
    };
  }

  if (bundle && bundle.stateHooksFound.length > 0) {
    return {
      decision: "shell-fetchable",
      rationale:
        `Step-3 bundle inspection found inlined state hook(s) ` +
        `[${bundle.stateHooksFound.join(", ")}] in the directory HTML. The page ` +
        `ships its initial data inline, so a fetch + JSON-extract pass can read ` +
        `it without a browser. Confirm the blob shape from the captured snippets.`,
    };
  }

  const realHtml = allFetches.find(
    (f) => f.bodyClass === "real-html" && (f.status ?? 0) < 400,
  );
  if (realHtml && bundle && bundle.scriptSrcsFound === 0) {
    return {
      decision: "shell-fetchable",
      rationale:
        `Server-rendered HTML returned (${realHtml.contentLength} bytes, mode ` +
        `${realHtml.mode}) and no client-side script bundles were referenced — ` +
        `the directory content is in the initial HTML. Build a fetch+Cheerio ` +
        `parser.`,
    };
  }

  const anyShell =
    allFetches.some((f) => f.bodyClass === "empty-shell") ||
    (bundle != null &&
      bundle.scriptSrcsFound > 0 &&
      bundle.stateHooksFound.length === 0 &&
      !bundle.jsonDataReachable);
  if (anyShell) {
    const hookList =
      bundle && bundle.stateHooksFound.length > 0
        ? bundle.stateHooksFound.join(", ")
        : "(none)";
    const apiCount = bundle?.candidateApiUrls.length ?? 0;
    const apiAttempted = bundle?.apiAttempts.length ?? 0;
    const apiOk =
      bundle?.apiAttempts.filter(
        (a) => (a.status ?? 0) >= 200 && (a.status ?? 0) < 300,
      ).length ?? 0;
    return {
      decision: "needs-browser",
      rationale:
        `SPA shell with no usable shell-fetchable data path. Bundle inspection ` +
        `found ${bundle?.scriptSrcsFound ?? 0} <script src> references and ` +
        `${apiCount} candidate API URL(s); attempted ${apiAttempted}, ${apiOk} ` +
        `returned 2xx but none were JSON. State hooks found: ${hookList}. ` +
        `Phase 1 will need a headless browser (Playwright) to render the ` +
        `directory page, OR continued investigation (auth, GraphQL POST body ` +
        `discovery, internal API auth tokens) to find a shell-reachable surface.`,
    };
  }

  const challenges = allFetches.filter(
    (f) =>
      f.bodyClass === "js-challenge" ||
      f.bodyClass === "bot-wall" ||
      f.bodyClass === "blocked",
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
            : "unknown CDN";
    return {
      decision: "blocked",
      rationale:
        `Active block detected: ${hit.bodyClass} (status ${hit.status}). CDN ` +
        `fingerprint: ${cdnHint}. Step-3 inspection ${bundle ? "ran" : "did not run"} ` +
        `because no clean 200 HTML body was available. Phase 1 must decide ` +
        `between residential proxy, browser automation, or skipping the source.`,
    };
  }

  return {
    decision: "inconclusive",
    rationale:
      `No conclusive signal: no JSON data reached, no inlined state hook, no ` +
      `real-html with empty bundle list, and no challenge body. Hand-inspect ` +
      `the JSON report — likely the site responded with an unusual shape this ` +
      `probe does not yet classify.`,
  };
}

// ---- Reporting -------------------------------------------------------------

function printSummary(report: ProbeReport): void {
  const lines: string[] = [];
  lines.push("");
  lines.push("=== USSF Learning Center Phase 0 probe ===");
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
      lines.push(...formatFetchBlockLines(f, "    "));
    }
    lines.push("");
  }
  if (report.bundleInspection) {
    const b = report.bundleInspection;
    lines.push(`--- Bundle inspection ---`);
    lines.push(`Inspected: ${b.inspectedUrl} (${b.inspectedMode}, status ${b.inspectedStatus})`);
    lines.push(`State hooks found: ${b.stateHooksFound.join(", ") || "(none)"}`);
    lines.push(`Script srcs found: ${b.scriptSrcsFound}`);
    lines.push(`Bundles fetched:   ${b.bundles.length}`);
    for (const bk of b.bundles) {
      const hits = Object.entries(bk.patternHits)
        .map(([k, v]) => `${k}=${v}`)
        .join(" ");
      lines.push(`  - ${bk.bundleUrl}`);
      lines.push(
        `      status=${bk.status} bytes=${bk.contentLength} ${hits || "(no pattern hits)"}`,
      );
    }
    lines.push(`Candidate API URLs: ${b.candidateApiUrls.length}`);
    for (const u of b.candidateApiUrls.slice(0, 20)) lines.push(`  - ${u}`);
    lines.push(`API attempts: ${b.apiAttempts.length}`);
    for (const a of b.apiAttempts) {
      lines.push(`  - ${a.url}`);
      lines.push(
        `      status=${a.status} ct=${a.contentType ?? "n/a"} class=${a.bodyClass} src=${a.source}`,
      );
    }
    lines.push(`JSON data reachable? ${b.jsonDataReachable ? "YES" : "no"}`);
    lines.push("");
  } else {
    lines.push(`--- Bundle inspection: SKIPPED (no clean 200 HTML available) ---`);
    lines.push("");
  }
  process.stdout.write(lines.join("\n") + "\n");
}

// ---- Main ------------------------------------------------------------------

async function main(): Promise<void> {
  const args = parseCommonProbeArgs(process.argv.slice(2), printUsageAndExit);
  const targets = buildProbeUrlList(args, buildDefaultUrls());

  process.stdout.write(
    `Probing ${targets.length} URL(s). For each URL, runs naive + UA-shod ` +
      `fetches, then a one-shot bundle inspection on the best 200 HTML. ` +
      `Hard stop at step 3 — no browser automation. Report → /tmp.\n`,
  );

  const naive = naiveHeaders();
  const shod = uaShodHeaders();
  const results: UrlProbeResult[] = [];
  for (const t of targets) {
    const naiveR = await fetchOnce(t.url, "naive", { headers: naive });
    const uaShodR = await fetchOnce(t.url, "ua-shod", { headers: shod });
    const uaUnblocks =
      naiveR.status !== uaShodR.status &&
      (naiveR.status ?? 0) >= 400 &&
      (uaShodR.status ?? 0) >= 200 &&
      (uaShodR.status ?? 0) < 400;
    results.push({ url: t.url, label: t.label, naive: naiveR, uaShod: uaShodR, uaUnblocks });
  }

  const bundleInspection = await inspectBundles(results);
  const { decision, rationale } = decide(results, bundleInspection);

  const report: ProbeReport = {
    ...buildReportHeader(
      "Phase 0 reconnaissance. Steps 1+2 capture per-URL fetches with first " +
        `${BODY_SNIPPET_CHARS} chars of body. Step 3 inspects the best 200 HTML ` +
        `for inlined state hooks and JS bundles, then attempts up to ` +
        `${API_FETCH_LIMIT} discovered API URL(s). Hard stop — no browser, no ` +
        `Playwright, no new deps, no DB writes.`,
    ),
    results,
    bundleInspection,
    decision,
    decisionRationale: rationale,
  };

  const outPath = writeJsonReport("ussf-directory-probe", report);

  printSummary(report);
  process.stdout.write(`\nJSON report written to: ${outPath}\n`);
}

main().catch((err: unknown) => {
  const msg = err instanceof Error ? err.stack ?? err.message : String(err);
  process.stderr.write(`probe-ussf-directory failed: ${msg}\n`);
  process.exit(1);
});
