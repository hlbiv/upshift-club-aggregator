/**
 * Admin namespace — Zod schemas + inferred types for the Data admin API.
 *
 * Source of truth: upshift-studio
 * docs/planning/upshift-data-admin-api-contract.md
 *
 * Consumers:
 *   import { AdminLoginRequest, ScrapeRunLog } from "@hlbiv/api-zod/admin";
 *
 * Player / upshift-studio MUST NOT import from this namespace — Data admin
 * routes are Data-side only.
 */

export * from "./schemas";
