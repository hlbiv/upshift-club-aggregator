/**
 * USSF Learning Center directory — Phase 1A unauthenticated browser probe
 *
 * Follow-up to Phase 0 (`scripts/src/probe-ussf-directory.ts`, decision
 * `needs-browser`). Phase 0 confirmed the page is an Angular SPA on
 * S3+CloudFront with all content rendered client-side; shell-level fetches
 * cannot extract directory data without executing the bundle.
 *
 * This probe drives a real headless Chromium via Playwright and answers
 * questions Phase 0 could not:
 *
 *   1. Without credentials, where does the SPA land? (Auth0 login redirect,
 *      or an unauthenticated landing page with public data?)
 *   2. What XHR / fetch traffic does the SPA fire before any auth gate?
 *      (List endpoints, statuses, content-types, sizes.)
 *   3. Does ANY directory data come back pre-auth? (E.g. a public list of
 *      coaches, an org index, a search facet config.)
 *   4. What's the auth flow — Auth0 client_id, scopes, redirect_uri,
 *      response_type?
 *   5. What does the rendered DOM look like just before / just after the
 *      redirect? (Title, body length, content markers.)
 *
 * Hard scope cuts:
 *   - No credentials are loaded, sent, or stored.
 *   - No login form is filled.
 *   - No cookie injection.
 *   - No DB writes.
 *   - No yt-dlp, no extractor — purely reconnaissance.
 *
 * Usage:
 *   pnpm --filter @workspace/browser-probes run probe-ussf-browser
 *   pnpm --filter @workspace/browser-probes run probe-ussf-browser -- --url <url>
 *   pnpm --filter @workspace/browser-probes run probe-ussf-browser -- --headed
 */

import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { chromium, type Browser, type BrowserContext, type Page, type Request, type Response } from "playwright";

// ---- Constants -------------------------------------------------------------

const DEFAULT_URL = "https://learning.ussoccer.com/directory";

// How long to let the SPA boot, fire its initial XHR storm, and either render
// data or redirect to auth. 25s is generous; the Angular app on S3+CloudFront
// loads fast but Auth0's IdP lookup adds RTT.
const PAGE_TIMEOUT_MS = 25_000;

// After `domcontentloaded`, give the SPA this much extra time to fire XHR.
// Most data fetches happen in the first 2-3s after DOMContentLoaded; we pad
// for slow networks.
const POST_DOM_SETTLE_MS = 6_000;

// Cap how much body we keep per response. Big enough to eyeball JSON shape,
// small enough that the report doesn't bloat into MB.
const RESPONSE_BODY_CHARS = 1_500;

// Resource types we care about for the API-surface analysis. Skips images,
// fonts, stylesheets — those are noise here.
const INTERESTING_RESOURCE_TYPES = new Set([
  "xhr",
  "fetch",
  "document",
  "script",
  "websocket",
  "eventsource",
]);

// ---- Types ----------------------------------------------------------------

interface CapturedRequest {
  url: string;
  method: string;
  resourceType: string;
  /** Initiator URL (referer-ish) — useful for tracing which bundle fired the call. */
  isNavigationRequest: boolean;
  /** Subset of request headers we keep. Strips `cookie` for safety. */
  headers: Record<string, string>;
  postDataSnippet: string | null;
  /** Wall-clock ms since `t0`. */
  startedMs: number;
}

interface CapturedResponse {
  url: string;
  status: number;
  statusText: string;
  contentType: string | null;
  contentLength: number | null;
  bodyChars: number;
  /** First N chars of body, only for JSON / text responses. Binary skipped. */
  bodySnippet: string;
  fromServiceWorker: boolean;
  /** Wall-clock ms since `t0`. */
  receivedMs: number;
}

interface NetworkEvent {
  request: CapturedRequest;
  response: CapturedResponse | null;
}

interface AuthRedirectInfo {
  /** Whether the page navigated to an Auth0 / OIDC URL. */
  redirectedToAuth: boolean;
  /** The first auth URL we observed. */
  authUrl: string | null;
  /** Parsed from the auth URL's query string. */
  authQuery: Record<string, string>;
  /** Auth0 tenant host, if detected. */
  authTenant: string | null;
  /** Time from t0 to the first auth redirect, in ms. */
  redirectMs: number | null;
}

interface PreAuthData {
  /**
   * Number of XHR/fetch responses that returned JSON with HTTP 2xx BEFORE
   * the auth redirect fired. If this is > 0, some data is reachable without
   * credentials.
   */
  jsonResponsesPreAuth: number;
  /** URLs of those JSON responses, for follow-up shell-level fetching. */
  jsonUrlsPreAuth: string[];
  /** Total bytes of JSON returned pre-auth. */
  jsonBytesPreAuth: number;
  /** Total non-asset (xhr/fetch/doc) responses pre-auth, regardless of type. */
  xhrResponsesPreAuth: number;
}

type Decision =
  | "public-data-extractable"
  | "auth-required-no-public-surface"
  | "auth-required-with-public-config"
  | "blocked-or-failed";

interface ProbeReport {
  ranAt: string;
  nodeVersion: string;
  platform: string;
  playwrightVersion: string;
  targetUrl: string;
  finalUrl: string | null;
  pageTitle: string | null;
  loadStatus: "loaded" | "timeout" | "error";
  loadError: string | null;
  bodyLengthChars: number;
  /**
   * First 4KB of the rendered DOM (after JS execution settled). Useful to
   * eyeball whether the page has the directory rendered client-side.
   */
  bodySnippet: string;
  /** Counts of common DOM indicators of directory-shaped content. */
  domShape: {
    tableRows: number;
    listItems: number;
    cardLikeDivs: number;
    inputs: number;
    iframes: number;
    h1Texts: string[];
    h2Texts: string[];
  };
  authRedirect: AuthRedirectInfo;
  preAuthData: PreAuthData;
  totalNetworkEvents: number;
  /** All XHR/fetch/document/script events, in time order. */
  networkEvents: NetworkEvent[];
  /** Console messages from the page (level + text). */
  consoleMessages: Array<{ level: string; text: string; tMs: number }>;
  /** Page-emitted JS errors. */
  pageErrors: Array<{ message: string; tMs: number }>;
  decision: Decision;
  decisionRationale: string;
}

// ---- Arg parsing -----------------------------------------------------------

interface Args {
  url: string;
  headed: boolean;
}

function parseArgs(argv: string[]): Args {
  let url = DEFAULT_URL;
  let headed = false;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (a === "--url" && argv[i + 1] !== undefined) {
      url = argv[i + 1]!;
      i++;
    } else if (a === "--headed") {
      headed = true;
    } else if (a === "--help" || a === "-h") {
      printUsageAndExit(0);
    }
  }
  return { url, headed };
}

function printUsageAndExit(code: number): never {
  process.stdout.write(
    [
      "probe-ussf-browser — Phase 1A USSF unauthenticated browser probe",
      "",
      "Usage:",
      "  pnpm --filter @workspace/browser-probes run probe-ussf-browser [options]",
      "",
      "Options:",
      `  --url <url>      Target URL (default: ${DEFAULT_URL})`,
      "  --headed         Run with visible browser (debug only — usually no display in container).",
      "  -h, --help       Show this help.",
      "",
      "No credentials are loaded, sent, or stored. Probe hard-stops at the",
      "first auth gate and reports what was reachable before it.",
      "",
    ].join("\n"),
  );
  process.exit(code);
}

// ---- Auth detection helpers -----------------------------------------------

/**
 * Heuristic for "is this URL an auth provider redirect target?". Covers
 * Auth0 (.auth0.com tenants and Auth0 Universal Login paths), Okta, Cognito,
 * Azure AD, plus a generic `/oauth2/authorize` / `/login` catch.
 */
function classifyAuthUrl(url: string): { isAuth: boolean; tenant: string | null } {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return { isAuth: false, tenant: null };
  }
  const host = parsed.host.toLowerCase();
  const path = parsed.pathname.toLowerCase();
  if (host.endsWith(".auth0.com")) return { isAuth: true, tenant: host };
  if (host.endsWith(".okta.com") || host.endsWith(".oktapreview.com"))
    return { isAuth: true, tenant: host };
  if (host.includes("cognito") || host.includes("amazoncognito"))
    return { isAuth: true, tenant: host };
  if (host.includes("login.microsoftonline") || host.includes("b2clogin"))
    return { isAuth: true, tenant: host };
  if (
    path.includes("/oauth2/authorize") ||
    path.includes("/oauth/authorize") ||
    path.includes("/u/login") ||
    path.endsWith("/login") ||
    path.includes("/authorize")
  ) {
    return { isAuth: true, tenant: host };
  }
  return { isAuth: false, tenant: null };
}

// ---- Main probe ------------------------------------------------------------

async function runProbe(args: Args): Promise<ProbeReport> {
  const t0 = Date.now();
  const tMs = (): number => Date.now() - t0;

  let browser: Browser | null = null;
  let context: BrowserContext | null = null;
  let page: Page | null = null;

  const events = new Map<string, NetworkEvent>();
  const consoleMessages: ProbeReport["consoleMessages"] = [];
  const pageErrors: ProbeReport["pageErrors"] = [];
  const authRedirect: AuthRedirectInfo = {
    redirectedToAuth: false,
    authUrl: null,
    authQuery: {},
    authTenant: null,
    redirectMs: null,
  };

  let loadStatus: ProbeReport["loadStatus"] = "loaded";
  let loadError: string | null = null;
  let finalUrl: string | null = null;
  let pageTitle: string | null = null;
  let bodyHtml = "";

  try {
    browser = await chromium.launch({
      headless: !args.headed,
      args: ["--no-sandbox", "--disable-dev-shm-usage"],
    });
    context = await browser.newContext({
      // Realistic Chrome UA. Don't try to evade fingerprinting; just don't
      // self-identify as a bot.
      userAgent:
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " +
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      viewport: { width: 1280, height: 800 },
    });
    page = await context.newPage();

    page.on("request", (req: Request) => {
      if (!INTERESTING_RESOURCE_TYPES.has(req.resourceType())) return;
      const headers: Record<string, string> = {};
      for (const [k, v] of Object.entries(req.headers())) {
        if (k.toLowerCase() === "cookie") continue;
        headers[k] = v;
      }
      const post = req.postData();
      events.set(req.url() + "::" + req.method() + "::" + tMs(), {
        request: {
          url: req.url(),
          method: req.method(),
          resourceType: req.resourceType(),
          isNavigationRequest: req.isNavigationRequest(),
          headers,
          postDataSnippet: post ? post.slice(0, 500) : null,
          startedMs: tMs(),
        },
        response: null,
      });
      // Auth redirect detection: if a navigation request goes to an auth
      // URL, record it. The response will fire too but the request is what
      // tells us what initiated.
      if (req.isNavigationRequest()) {
        const cls = classifyAuthUrl(req.url());
        if (cls.isAuth && !authRedirect.redirectedToAuth) {
          authRedirect.redirectedToAuth = true;
          authRedirect.authUrl = req.url();
          authRedirect.authTenant = cls.tenant;
          authRedirect.redirectMs = tMs();
          try {
            const u = new URL(req.url());
            for (const [k, v] of u.searchParams) authRedirect.authQuery[k] = v;
          } catch {
            // Unparseable URL — keep what we have.
          }
        }
      }
    });

    page.on("response", async (res: Response) => {
      const req = res.request();
      if (!INTERESTING_RESOURCE_TYPES.has(req.resourceType())) return;
      const ct = res.headers()["content-type"] ?? null;
      // Body capture: only for textual content. Binary (script bundles can
      // be large; we don't need their bodies here, only the URL list).
      let bodySnippet = "";
      let bodyChars = 0;
      const isText =
        ct != null &&
        (/json/i.test(ct) ||
          /text\//i.test(ct) ||
          /xml/i.test(ct) ||
          /javascript/i.test(ct));
      if (isText && req.resourceType() !== "script") {
        try {
          const text = await res.text();
          bodyChars = text.length;
          bodySnippet = text.slice(0, RESPONSE_BODY_CHARS);
        } catch {
          // Some responses (e.g. redirects) have no body — silent skip.
        }
      }
      const cl = res.headers()["content-length"];
      const captured: CapturedResponse = {
        url: res.url(),
        status: res.status(),
        statusText: res.statusText(),
        contentType: ct,
        contentLength: cl ? Number.parseInt(cl, 10) : null,
        bodyChars,
        bodySnippet,
        fromServiceWorker: res.fromServiceWorker(),
        receivedMs: tMs(),
      };
      // Match to its request entry — the latest entry whose URL+method
      // matches and has no response yet.
      let attached = false;
      for (const ev of Array.from(events.values()).reverse()) {
        if (
          ev.request.url === req.url() &&
          ev.request.method === req.method() &&
          ev.response == null
        ) {
          ev.response = captured;
          attached = true;
          break;
        }
      }
      if (!attached) {
        // Response without a tracked request (e.g. redirected nav) — append.
        events.set("orphan::" + tMs() + "::" + req.url(), {
          request: {
            url: req.url(),
            method: req.method(),
            resourceType: req.resourceType(),
            isNavigationRequest: req.isNavigationRequest(),
            headers: {},
            postDataSnippet: null,
            startedMs: tMs(),
          },
          response: captured,
        });
      }
    });

    page.on("console", (msg) => {
      consoleMessages.push({
        level: msg.type(),
        text: msg.text().slice(0, 500),
        tMs: tMs(),
      });
    });
    page.on("pageerror", (err) => {
      pageErrors.push({ message: err.message.slice(0, 500), tMs: tMs() });
    });

    try {
      await page.goto(args.url, {
        waitUntil: "domcontentloaded",
        timeout: PAGE_TIMEOUT_MS,
      });
    } catch (err) {
      // Goto can throw on auth-redirect timing. We still want to capture
      // whatever state the page reached. Note the error and continue.
      loadStatus = "timeout";
      loadError = err instanceof Error ? err.message : String(err);
    }

    // Let the SPA fire its post-DOMContentLoaded XHR storm. We don't use
    // `networkidle` here because Auth0 redirects can leave the page in a
    // state where networkidle never resolves (long-poll, websocket).
    await page.waitForTimeout(POST_DOM_SETTLE_MS);

    finalUrl = page.url();
    try {
      pageTitle = await page.title();
    } catch {
      pageTitle = null;
    }
    try {
      bodyHtml = await page.content();
    } catch (err) {
      bodyHtml = "";
      if (loadError == null) {
        loadError = err instanceof Error ? err.message : String(err);
      }
    }
  } catch (err) {
    loadStatus = "error";
    loadError = err instanceof Error ? err.message : String(err);
  } finally {
    if (page) await page.close().catch(() => undefined);
    if (context) await context.close().catch(() => undefined);
    if (browser) await browser.close().catch(() => undefined);
  }

  // ---- DOM shape analysis ------------------------------------------------
  const domShape = analyseDom(bodyHtml);

  // ---- Pre-auth data analysis -------------------------------------------
  const networkEvents = Array.from(events.values()).sort(
    (a, b) => a.request.startedMs - b.request.startedMs,
  );
  const cutoffMs = authRedirect.redirectMs ?? Number.POSITIVE_INFINITY;
  let jsonResponsesPreAuth = 0;
  let jsonBytesPreAuth = 0;
  let xhrResponsesPreAuth = 0;
  const jsonUrlsPreAuth: string[] = [];
  for (const ev of networkEvents) {
    if (!ev.response) continue;
    if (ev.response.receivedMs >= cutoffMs) continue;
    if (ev.request.resourceType === "xhr" || ev.request.resourceType === "fetch") {
      xhrResponsesPreAuth++;
      const ct = ev.response.contentType ?? "";
      const status = ev.response.status;
      if (status >= 200 && status < 300 && /json/i.test(ct)) {
        jsonResponsesPreAuth++;
        jsonBytesPreAuth += ev.response.bodyChars;
        jsonUrlsPreAuth.push(ev.response.url);
      }
    }
  }
  const preAuthData: PreAuthData = {
    jsonResponsesPreAuth,
    jsonUrlsPreAuth,
    jsonBytesPreAuth,
    xhrResponsesPreAuth,
  };

  const { decision, rationale } = decide(
    loadStatus,
    authRedirect,
    preAuthData,
    domShape,
    finalUrl,
  );

  return {
    ranAt: new Date().toISOString(),
    nodeVersion: process.version,
    platform: `${process.platform} ${process.arch}`,
    playwrightVersion: getPlaywrightVersion(),
    targetUrl: args.url,
    finalUrl,
    pageTitle,
    loadStatus,
    loadError,
    bodyLengthChars: bodyHtml.length,
    bodySnippet: bodyHtml.slice(0, 4_096),
    domShape,
    authRedirect,
    preAuthData,
    totalNetworkEvents: networkEvents.length,
    networkEvents,
    consoleMessages,
    pageErrors,
    decision,
    decisionRationale: rationale,
  };
}

// ---- DOM analysis ----------------------------------------------------------

function analyseDom(html: string): ProbeReport["domShape"] {
  const tableRows = (html.match(/<tr\b/gi) ?? []).length;
  const listItems = (html.match(/<li\b/gi) ?? []).length;
  const cardLikeDivs = (
    html.match(/<div\b[^>]*\bclass=["'][^"']*\b(?:card|item|row|tile)\b[^"']*["']/gi) ?? []
  ).length;
  const inputs = (html.match(/<input\b/gi) ?? []).length;
  const iframes = (html.match(/<iframe\b/gi) ?? []).length;
  const h1Texts: string[] = [];
  const h2Texts: string[] = [];
  const h1Re = /<h1\b[^>]*>([\s\S]*?)<\/h1>/gi;
  let m: RegExpExecArray | null;
  while ((m = h1Re.exec(html)) !== null && h1Texts.length < 5) {
    h1Texts.push(stripTags(m[1]!).slice(0, 120));
  }
  const h2Re = /<h2\b[^>]*>([\s\S]*?)<\/h2>/gi;
  while ((m = h2Re.exec(html)) !== null && h2Texts.length < 10) {
    h2Texts.push(stripTags(m[1]!).slice(0, 120));
  }
  return { tableRows, listItems, cardLikeDivs, inputs, iframes, h1Texts, h2Texts };
}

function stripTags(s: string): string {
  return s.replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim();
}

// ---- Decision engine -------------------------------------------------------

function decide(
  loadStatus: ProbeReport["loadStatus"],
  auth: AuthRedirectInfo,
  preAuth: PreAuthData,
  dom: ProbeReport["domShape"],
  finalUrl: string | null,
): { decision: Decision; rationale: string } {
  if (loadStatus === "error" || finalUrl == null) {
    return {
      decision: "blocked-or-failed",
      rationale:
        "Browser failed to launch or the page never produced a final URL. " +
        "Check loadError + system-level chromium dependencies before " +
        "drawing any conclusions about the site itself.",
    };
  }

  if (preAuth.jsonResponsesPreAuth > 0) {
    return {
      decision: "public-data-extractable",
      rationale:
        `${preAuth.jsonResponsesPreAuth} JSON XHR response(s) returned 2xx ` +
        `BEFORE any auth redirect (${preAuth.jsonBytesPreAuth} bytes total). ` +
        `Sample URLs: ${preAuth.jsonUrlsPreAuth.slice(0, 3).join(", ")}. ` +
        `These endpoints are reachable without credentials — a shell-level ` +
        `extractor (Phase 0's fetch + bundle-discovery approach) can be ` +
        `pointed at them directly. Browser automation is NOT required for ` +
        `the data they expose. Confirm via curl from production egress.`,
    };
  }

  if (auth.redirectedToAuth) {
    if (preAuth.xhrResponsesPreAuth > 0 || dom.h1Texts.length > 0) {
      return {
        decision: "auth-required-with-public-config",
        rationale:
          `Page redirects to ${auth.authTenant ?? "auth provider"} ` +
          `(${auth.authUrl}) at ${auth.redirectMs}ms. ${preAuth.xhrResponsesPreAuth} ` +
          `XHR/fetch response(s) fired pre-auth but none returned JSON ` +
          `(likely config / asset / 401 fail-fast). Auth0 query: ` +
          `client_id=${auth.authQuery.client_id ?? "?"}, ` +
          `audience=${auth.authQuery.audience ?? "?"}, ` +
          `scope=${auth.authQuery.scope ?? "?"}. No public data surface — ` +
          `extracting directory data requires authenticated session. Without ` +
          `credentials, this lane is closed.`,
      };
    }
    return {
      decision: "auth-required-no-public-surface",
      rationale:
        `Page redirects to ${auth.authTenant ?? "auth provider"} ` +
        `(${auth.authUrl}) at ${auth.redirectMs}ms with NO pre-auth XHR ` +
        `traffic and an empty / minimal DOM. The SPA gates everything ` +
        `behind login. Without credentials there is no extractable surface ` +
        `at this URL. Phase 1B would require an authenticated session.`,
    };
  }

  if (dom.tableRows > 5 || dom.listItems > 10 || dom.cardLikeDivs > 5) {
    return {
      decision: "public-data-extractable",
      rationale:
        `No auth redirect observed and the rendered DOM contains ` +
        `directory-shaped content: ${dom.tableRows} table rows, ` +
        `${dom.listItems} list items, ${dom.cardLikeDivs} card-like divs. ` +
        `A browser-rendered DOM-scrape extractor can ship; alternatively ` +
        `replay the captured XHR URLs from a shell client.`,
    };
  }

  return {
    decision: "blocked-or-failed",
    rationale:
      `Page loaded (final URL: ${finalUrl}) but no auth redirect, no ` +
      `pre-auth JSON, and no directory-shaped DOM was detected. Possible ` +
      `causes: SPA rendered an empty state, the directory route is ` +
      `client-side gated by an in-app auth check (no redirect), or the ` +
      `probe's settle timeout was too tight. Hand-inspect the JSON report.`,
  };
}

// ---- Misc helpers ----------------------------------------------------------

function getPlaywrightVersion(): string {
  // Read from playwright's own package.json if available; else fall back.
  try {
    // require.resolve isn't ideal in ESM, so just parse from process.versions
    // when present; otherwise return "unknown".
    return process.env.npm_package_dependencies_playwright ?? "unknown";
  } catch {
    return "unknown";
  }
}

function writeJsonReport(prefix: string, report: unknown): string {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outPath = join("/tmp", `${prefix}-${stamp}.json`);
  writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");
  return outPath;
}

// ---- Reporting -------------------------------------------------------------

function printSummary(report: ProbeReport): void {
  const lines: string[] = [];
  lines.push("");
  lines.push("=== USSF Learning Center Phase 1A browser probe ===");
  lines.push(`Ran at:    ${report.ranAt}`);
  lines.push(`Node:      ${report.nodeVersion} (${report.platform})`);
  lines.push(`Target:    ${report.targetUrl}`);
  lines.push(`Final URL: ${report.finalUrl ?? "(none)"}`);
  lines.push(`Title:     ${report.pageTitle ?? "(none)"}`);
  lines.push(`Load:      ${report.loadStatus}${report.loadError ? ` (${report.loadError})` : ""}`);
  lines.push("");
  lines.push(`DECISION:  ${report.decision.toUpperCase()}`);
  lines.push(`Rationale: ${report.decisionRationale}`);
  lines.push("");
  lines.push("--- Auth redirect ---");
  lines.push(`Redirected to auth? ${report.authRedirect.redirectedToAuth ? "YES" : "no"}`);
  if (report.authRedirect.redirectedToAuth) {
    lines.push(`  Tenant: ${report.authRedirect.authTenant ?? "(unknown)"}`);
    lines.push(`  URL:    ${report.authRedirect.authUrl}`);
    lines.push(`  At:     ${report.authRedirect.redirectMs}ms`);
    lines.push(`  Query keys: ${Object.keys(report.authRedirect.authQuery).join(", ") || "(none)"}`);
    for (const [k, v] of Object.entries(report.authRedirect.authQuery)) {
      lines.push(`    ${k} = ${v.slice(0, 120)}`);
    }
  }
  lines.push("");
  lines.push("--- Pre-auth data ---");
  lines.push(`XHR/fetch responses pre-auth:  ${report.preAuthData.xhrResponsesPreAuth}`);
  lines.push(`JSON 2xx responses pre-auth:    ${report.preAuthData.jsonResponsesPreAuth}`);
  lines.push(`JSON bytes pre-auth:            ${report.preAuthData.jsonBytesPreAuth}`);
  if (report.preAuthData.jsonUrlsPreAuth.length > 0) {
    lines.push(`JSON URLs pre-auth:`);
    for (const u of report.preAuthData.jsonUrlsPreAuth) lines.push(`  - ${u}`);
  }
  lines.push("");
  lines.push("--- DOM shape ---");
  lines.push(`Body length:    ${report.bodyLengthChars} chars`);
  lines.push(`Table rows:     ${report.domShape.tableRows}`);
  lines.push(`List items:     ${report.domShape.listItems}`);
  lines.push(`Card-like divs: ${report.domShape.cardLikeDivs}`);
  lines.push(`Inputs:         ${report.domShape.inputs}`);
  lines.push(`Iframes:        ${report.domShape.iframes}`);
  if (report.domShape.h1Texts.length > 0) {
    lines.push(`H1: ${report.domShape.h1Texts.map((t) => `"${t}"`).join(", ")}`);
  }
  if (report.domShape.h2Texts.length > 0) {
    lines.push(`H2: ${report.domShape.h2Texts.map((t) => `"${t}"`).join(", ")}`);
  }
  lines.push("");
  lines.push(`--- Network events (${report.totalNetworkEvents} total) ---`);
  for (const ev of report.networkEvents) {
    const r = ev.response;
    const status = r ? `${r.status}` : "----";
    const ct = r?.contentType ?? "n/a";
    const size = r?.bodyChars ?? r?.contentLength ?? "?";
    lines.push(
      `  [${String(ev.request.startedMs).padStart(5)}ms] ${ev.request.method} ` +
        `${status} ${ev.request.resourceType.padEnd(8)} ${size} ${ct.slice(0, 30).padEnd(30)} ${ev.request.url}`,
    );
  }
  lines.push("");
  if (report.pageErrors.length > 0) {
    lines.push(`--- Page errors (${report.pageErrors.length}) ---`);
    for (const e of report.pageErrors) lines.push(`  [${e.tMs}ms] ${e.message}`);
    lines.push("");
  }
  process.stdout.write(lines.join("\n") + "\n");
}

// ---- Main ------------------------------------------------------------------

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  process.stdout.write(
    `Driving headless Chromium against ${args.url}. No credentials will be ` +
      `loaded, sent, or stored. Probe stops at the first auth gate.\n`,
  );
  const report = await runProbe(args);
  const outPath = writeJsonReport("ussf-browser-probe", report);
  printSummary(report);
  process.stdout.write(`\nJSON report written to: ${outPath}\n`);
}

main().catch((err: unknown) => {
  const msg = err instanceof Error ? err.stack ?? err.message : String(err);
  process.stderr.write(`probe-ussf-browser failed: ${msg}\n`);
  process.exit(1);
});
