// Re-export the Orval-generated Zod schemas at the top level.
//
// `./generated/api.js` contains runtime Zod consts (e.g. `HealthCheckResponse
// = zod.object(...)`) — these are what callers invoke via `.parse(...)`.
//
// `./generated/types/index.js` contains TypeScript interfaces (e.g.
// `interface HealthStatus { … }`). When an OpenAPI schema name happens to
// match what Orval auto-derives for a response (e.g. operation `adminLogin`
// → Zod const `AdminLoginResponse`, schema `AdminLoginResponse` → interface
// with the same name), a naive `export *` from both files yields a TS2308
// ambiguity error.
//
// Resolution: Zod schemas win in value space (callers use them for runtime
// validation); the TS interfaces are best reached via the explicit
// `@hlbiv/api-zod/admin` subpath or — for brand-new admin types — via
// `z.infer<typeof SomeZodConst>`. We don't re-export the generated types
// barrel here. If a public-API caller truly needs a generated interface
// that isn't reachable via `z.infer`, import it directly from
// `@hlbiv/api-zod/generated/types/...`.
export * from "./generated/api.js";

// Hand-maintained schemas for endpoints not yet documented in openapi.yaml.
// See extended.ts for the scope and migration path.
export * from "./extended.js";
