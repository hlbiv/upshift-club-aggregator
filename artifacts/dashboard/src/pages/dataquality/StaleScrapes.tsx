import { AppShell } from "../../components/AppShell";
import { PageHeader } from "../../components/primitives/PageHeader";
import { StaleScrapesPanel } from "../DataQuality";

/**
 * `/data-quality/stale-scrapes` — `scrape_health` rows whose
 * `last_scraped_at` is older than the threshold (default 14d) or never set.
 */
export default function StaleScrapesPage() {
  return (
    <AppShell>
      <PageHeader
        eyebrow="Data quality"
        title="Stale scrapes"
        description="Targets we haven't refreshed inside the lookback window. Use this to seed a backfill batch."
      />
      <StaleScrapesPanel />
    </AppShell>
  );
}
