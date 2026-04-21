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
 *                             JSON state hooks (`__INITIAL_STATE__`,
 *                             `__PRELOADED_STATE__`, `__NEXT_DATA__`,
 *                             `__NUXT__`, `window.__APP_STATE__`), enumerate
 *                             referenced `<script src>` bundles, fetch each
 *                             bundle, grep bundle text for `apiBaseUrl`,
 *                             `/api/`, `/directory/`, GraphQL endpoints
 *                             (`graphql`, `gql`, `query{`), Algolia / Typesense
 *                             markers, and the bare `learning.ussoccer.com`
 *                             host. For each discovered API URL, attempt a
 *                             single GET and record status + content-type +
 *                             first 500 chars.
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

import { writeFileSync } from "node:fs";
import { join } from "node:path";

// ---- Config ----------------------------------------------------------------

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
  "x-powered-by",
];

const REQUEST_TIMEOUT_MS = 15_000;
const BODY_SNIPPET_CHARS = 500;
// Cap how much of each JS bundle we hold in memory / capture in the report.
// Real Next.js / Webpack chunks easily run 200–800 KB; we only need to grep
// them, not archive them.
const BUNDLE_FETCH_BYTES_MAX = 2_000_000;
// Cap how many script bundles we follow per page. The point is to learn the
// API surface, not enumerate every chunk in a Webpack manifest.
const BUNDLE_FETCH_LIMIT = 12;
// Cap how many discovered API URLs we'll actually GET. Keeps the probe fast
// and avoids hammering newly discovered endpoints.
const API_FETCH_LIMIT = 8;

const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

// SPA / framework state-hook markers we grep the HTML body for.
const STATE_HOOKS: Array<{ name: string; re: RegExp }> = [
  { name: "__INITIAL_STATE__", re: /__INITIAL_STATE__/ },
  { name: "__PRELOADED_STATE__", re: /__PRELOADED_STATE__/ },
  { name: "__NEXT_DATA__", re: /__NEXT_DATA__/ },
  { name: "__NUXT__", re: /__NUXT__/ },
  { name: "window.__APP_STATE__", re: /window\.__APP_STATE__/ },
  { name: "window.__DATA__", re: /window\.__DATA__/ },
  { name: "window.__APOLLO_STATE__", re: /window\.__APOLLO_STATE__/ },
  { name: "self.__remixContext", re: /self\.__remixContext/ },
];

// API-shape markers we grep bundles for.
const API_PATTERNS: Array<{ name: string; re: RegExp }> = [
  { name: "apiBaseUrl", re: /apiBaseUrl/g },
  { name: "API_BASE_URL", re: /API_BASE_URL/g },
  { name: "/api/", re: /["'`](\/api\/[A-Za-z0-9_\-\/]+)/g },
  { name: "/directory/", re: /["'`](\/directory\/[A-Za-z0-9_\-\/]+)/g },
  { name: "graphql endpoint", re: /["'`]([^"'`]*graphql[^"'`]*)["'`]/g },
  { name: "absolute api URL", re: /https?:\/\/[a-z0-9.\-]+\/(?:api|graphql)\/[^"'`\s)]*/g },
  { name: "algolia", re: /algolia(?:net|search)?\.(?:com|net)/g },
  { name: "typesense", re: /typesense\.(?:net|org|com)/g },
  { name: "learning.ussoccer.com host", re: /learning\.ussoccer\.com/g },
];

const CF_CHALLENGE_RE = /cf-chl|challenge-platform|__cf_chl_|cf_chl_opt/i;
const WAF_BLOCK_RE =
  /access denied|pardon our interruption|request blocked|are you a robot|reference #[0-9a-f]+/i;
const SPA_SHELL_RE =
  /<div[^>]+id=["'](root|app|__next|__nuxt|svelte)["']/i;
const APP_ROOT_RE = /<app-root[\s>]/i;

// ---- Types -----------------------------------------------------------------

type FetchMode = "naive" | "ua-shod";

type BodyClass =
  | "real-html"
  | "json"
  | "js-challenge"
  | "bot-wall"
  | "empty-shell"
  | "redirect"
  | "error"
  | "blocked"
  | "unknown";

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
}

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
  /** Source of the URL — which pattern + which bundle it came from. */
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

type Decision =
  | "shell-fetchable"
  | "needs-browser"
  | "blocked"
  | "inconclusive";

interface ProbeReport {
  ranAt: string;
  nodeVersion: string;
  platform: string;
  userAgent: string;
  note: string;
  results: UrlProbeResult[];
  bundleInspection: BundleInspection | null;
  decision: Decision;
  decisionRationale: string;
}

// ---- Arg parsing -----------------------------------------------------------

interface CliArgs {
  extraUrls: string[];
  onlyUrls: string[];
}

function parseArgs(argv: string[]): CliArgs {
  const args: CliArgs = { extraUrls: [], onlyUrls: [] };
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

// ---- Fetch primitives ------------------------------------------------------

async function fetchOnce(
  url: string,
  mode: FetchMode,
): Promise<SingleFetchResult> {
  const start = Date.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

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

    return {
      mode,
      status: res.status,
      statusText: res.statusText,
      elapsedMs,
      contentLength: body.length,
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
    };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Like fetchOnce, but for arbitrary asset GETs during bundle / API
 * inspection. Returns the full body (capped at BUNDLE_FETCH_BYTES_MAX) so
 * the caller can grep it.
 */
async function fetchBundleLike(
  url: string,
  acceptHeader: string,
): Promise<{
  status: number | null;
  statusText: string | null;
  contentType: string | null;
  contentLength: number | null;
  body: string;
  error: string | null;
  headers: Record<string, string>;
}> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      headers: {
        "User-Agent": USER_AGENT,
        Accept: acceptHeader,
        "Accept-Language": "en-US,en;q=0.9",
      },
      signal: controller.signal,
    });
    const headers: Record<string, string> = {};
    res.headers.forEach((v, k) => {
      headers[k.toLowerCase()] = v;
    });
    const text = await res.text();
    const body =
      text.length > BUNDLE_FETCH_BYTES_MAX
        ? text.slice(0, BUNDLE_FETCH_BYTES_MAX)
        : text;
    return {
      status: res.status,
      statusText: res.statusText,
      contentType: headers["content-type"] ?? null,
      contentLength: text.length,
      body,
      error: null,
      headers,
    };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      status: null,
      statusText: null,
      contentType: null,
      contentLength: null,
      body: "",
      error: message,
      headers: {},
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

  const cfRay = headers["cf-ray"];
  const cfMitigated = headers["cf-mitigated"];
  const server = headers["server"] ?? "";
  const akamaiHeader = Object.keys(headers).some((h) =>
    h.startsWith("x-akamai"),
  );
  const contentType = (headers["content-type"] ?? "").toLowerCase();

  if (status >= 400) {
    if (
      status === 403 &&
      (CF_CHALLENGE_RE.test(body) ||
        cfMitigated === "challenge" ||
        (cfRay != null && CF_CHALLENGE_RE.test(body)))
    ) {
      return "js-challenge";
    }
    if (status === 403 && (akamaiHeader || /akamaighost/i.test(server))) {
      return "bot-wall";
    }
    if (status === 403 && WAF_BLOCK_RE.test(body)) return "bot-wall";
    if (status === 401 || status === 403 || status === 451) return "blocked";
    return "error";
  }

  if (CF_CHALLENGE_RE.test(body)) return "js-challenge";
  if (WAF_BLOCK_RE.test(body)) return "bot-wall";

  if (contentType.includes("json")) return "json";
  // JSON-ish body without a content-type can still be detected.
  const trimmed = body.trimStart();
  if (
    !contentType.includes("html") &&
    (trimmed.startsWith("{") || trimmed.startsWith("[")) &&
    body.length > 1
  ) {
    return "json";
  }

  if (
    body.length < 5_120 &&
    (SPA_SHELL_RE.test(body) || APP_ROOT_RE.test(body))
  ) {
    return "empty-shell";
  }
  if (body.length > 2_048 && /<html/i.test(body)) {
    return "real-html";
  }
  return "unknown";
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
  // Pick the inspection target. We need a 200, with a body class of
  // real-html / empty-shell / unknown (anything that could carry inlined
  // JSON or `<script src>` references).
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
  // Prefer the directory label, then ua-shod, then anything.
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

  // We re-fetch the chosen URL in full (the per-URL fetch only kept a 500-
  // char snippet of the body, so we have to GET it again to grep).
  const refetch = await fetchBundleLike(
    chosen.url,
    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  );
  const html = refetch.body;

  // ---- State-hook detection --------------------------------------------
  const stateHooksFound: string[] = [];
  const stateHookSnippets: Array<{ name: string; snippet: string }> = [];
  for (const hook of STATE_HOOKS) {
    const m = hook.re.exec(html);
    if (m) {
      stateHooksFound.push(hook.name);
      const center = m.index;
      const start = Math.max(0, center - 200);
      const end = Math.min(html.length, center + 400);
      stateHookSnippets.push({
        name: hook.name,
        snippet: html.slice(start, end),
      });
    }
  }

  // ---- Script src enumeration ------------------------------------------
  const scriptSrcs = extractScriptSrcs(html, chosen.url);

  // ---- Bundle fetch + grep ---------------------------------------------
  const bundles: BundleHit[] = [];
  const candidateApiUrls = new Set<string>();
  // Track which (pattern, bundleUrl, hit) tuples we've seen so we can label
  // discovered URLs by their source bundle in the report.
  const apiSourceMap = new Map<string, string>();

  // First, also grep the inlined HTML itself — Next.js often inlines a
  // self.__remixContext or __NEXT_DATA__ blob with full API URLs.
  const inlineHits = grepForApiPatterns(html, chosen.url);
  for (const u of inlineHits.candidateUrls) {
    candidateApiUrls.add(u);
    if (!apiSourceMap.has(u)) apiSourceMap.set(u, "inlined-html");
  }

  for (const src of scriptSrcs.slice(0, BUNDLE_FETCH_LIMIT)) {
    const got = await fetchBundleLike(src, "*/*");
    const patternHits: Record<string, number> = {};
    if (got.body) {
      const grep = grepForApiPatterns(got.body, chosen.url);
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

  // ---- Attempt the discovered API URLs ---------------------------------
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

function extractScriptSrcs(html: string, baseUrl: string): string[] {
  const out = new Set<string>();
  const re = /<script\b[^>]*\bsrc\s*=\s*["']([^"']+)["'][^>]*>/gi;
  let m: RegExpExecArray | null;
  while ((m = re.exec(html)) !== null) {
    const raw = m[1]!;
    try {
      const resolved = new URL(raw, baseUrl).toString();
      out.add(resolved);
    } catch {
      // Ignore unresolvable srcs (e.g. data: URIs, malformed).
    }
  }
  return Array.from(out);
}

// Hosts we drop from candidate API URLs because they're framework-doc
// links accidentally inlined into vendor bundles, not real backends.
const DOC_HOST_DENYLIST = [
  "docs.angularjs.org",
  "angular.io",
  "angular.dev",
  "reactjs.org",
  "react.dev",
  "developer.mozilla.org",
  "github.com",
  "w3.org",
  "ietf.org",
  "schema.org",
];

function grepForApiPatterns(
  body: string,
  pageUrl: string,
): {
  patternCounts: Record<string, number>;
  candidateUrls: string[];
} {
  const patternCounts: Record<string, number> = {};
  const candidateUrls = new Set<string>();
  for (const p of API_PATTERNS) {
    // Re-create the regex each time so .exec/.lastIndex state is clean.
    const re = new RegExp(p.re.source, p.re.flags);
    let count = 0;
    let m: RegExpExecArray | null;
    while ((m = re.exec(body)) !== null) {
      count++;
      const cap = m[1] ?? m[0];
      if (typeof cap === "string" && cap.length > 0 && cap.length < 300) {
        let resolved: string | null = null;
        if (cap.startsWith("http://") || cap.startsWith("https://")) {
          resolved = cap;
        } else if (cap.startsWith("/")) {
          // Resolve root-relative URLs against the page origin so we can
          // actually fetch them in step 3.
          try {
            resolved = new URL(cap, pageUrl).toString();
          } catch {
            resolved = null;
          }
        } else if (cap.includes("graphql") || cap.includes("/api/")) {
          resolved = cap;
        }
        if (resolved) {
          // Drop framework-doc hosts; they're in vendor bundles as comments
          // or angular @link references, not as real endpoints.
          let host = "";
          try {
            host = new URL(resolved).host.toLowerCase();
          } catch {
            host = "";
          }
          if (!DOC_HOST_DENYLIST.includes(host)) {
            candidateUrls.add(resolved);
          }
        }
      }
      if (!re.global) break;
    }
    if (count > 0) patternCounts[p.name] = count;
  }
  return {
    patternCounts,
    candidateUrls: Array.from(candidateUrls),
  };
}

// ---- Decision engine -------------------------------------------------------

function decide(
  results: UrlProbeResult[],
  bundle: BundleInspection | null,
): { decision: Decision; rationale: string } {
  const allFetches = results.flatMap((r) => [r.naive, r.uaShod]);

  // 1. JSON data reachable via discovered API → shell-fetchable.
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

  // 2. State-hook found in HTML → shell-fetchable (inlined JSON).
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

  // 3. Real-html on naive or ua-shod and no SPA-only signal → shell-fetchable.
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

  // 4. SPA shell, no inlined data, no reachable API → needs-browser.
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

  // 5. Bot-wall / challenge anywhere → blocked.
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

  // 6. Mixed / unclear → inconclusive.
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
      lines.push(`      status=${bk.status} bytes=${bk.contentLength} ${hits || "(no pattern hits)"}`);
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
  const args = parseArgs(process.argv.slice(2));

  const targets: Array<{ url: string; label: string }> =
    args.onlyUrls.length > 0
      ? args.onlyUrls.map((u, i) => ({ url: u, label: `custom-${i + 1}` }))
      : buildDefaultUrls();

  for (const u of args.extraUrls) {
    targets.push({ url: u, label: `extra-${targets.length + 1}` });
  }

  process.stdout.write(
    `Probing ${targets.length} URL(s). For each URL, runs naive + UA-shod ` +
      `fetches, then a one-shot bundle inspection on the best 200 HTML. ` +
      `Hard stop at step 3 — no browser automation. Report → /tmp.\n`,
  );

  const results: UrlProbeResult[] = [];
  for (const t of targets) {
    const naive = await fetchOnce(t.url, "naive");
    const uaShod = await fetchOnce(t.url, "ua-shod");
    const uaUnblocks =
      naive.status !== uaShod.status &&
      (naive.status ?? 0) >= 400 &&
      (uaShod.status ?? 0) >= 200 &&
      (uaShod.status ?? 0) < 400;
    results.push({ url: t.url, label: t.label, naive, uaShod, uaUnblocks });
  }

  const bundleInspection = await inspectBundles(results);
  const { decision, rationale } = decide(results, bundleInspection);

  const report: ProbeReport = {
    ranAt: new Date().toISOString(),
    nodeVersion: process.version,
    platform: `${process.platform} ${process.arch}`,
    userAgent: USER_AGENT,
    note:
      "Phase 0 reconnaissance. Steps 1+2 capture per-URL fetches with first " +
      `${BODY_SNIPPET_CHARS} chars of body. Step 3 inspects the best 200 HTML ` +
      `for inlined state hooks and JS bundles, then attempts up to ` +
      `${API_FETCH_LIMIT} discovered API URL(s). Hard stop — no browser, no ` +
      `Playwright, no new deps, no DB writes.`,
    results,
    bundleInspection,
    decision,
    decisionRationale: rationale,
  };

  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outPath = join("/tmp", `ussf-directory-probe-${stamp}.json`);
  writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");

  printSummary(report);
  process.stdout.write(`\nJSON report written to: ${outPath}\n`);
}

main().catch((err: unknown) => {
  const msg = err instanceof Error ? err.stack ?? err.message : String(err);
  process.stderr.write(`probe-ussf-directory failed: ${msg}\n`);
  process.exit(1);
});
