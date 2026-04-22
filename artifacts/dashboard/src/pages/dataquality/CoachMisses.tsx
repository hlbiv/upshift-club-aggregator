import { AppShell } from "../../components/AppShell";
import { PageHeader } from "../../components/primitives/PageHeader";
import { CoachMissesPanel } from "../DataQuality";

/**
 * `/data-quality/coach-misses` — colleges where the head-coach extractor
 * (inline + the `/coaches` fallback) returned nothing on the most recent
 * scheduled run. Backed by the `coach_misses` table; populated when the
 * NCAA roster scraper runs with `COACH_MISSES_REPORT_ENABLED=true`.
 */
export default function CoachMissesPage() {
  return (
    <AppShell>
      <PageHeader
        eyebrow="Data quality"
        title="Coach misses"
        description="Schools where we still couldn't find a head coach. Use this list to seed the Playwright fallback queue or do manual lookups."
      />
      <CoachMissesPanel />
    </AppShell>
  );
}
