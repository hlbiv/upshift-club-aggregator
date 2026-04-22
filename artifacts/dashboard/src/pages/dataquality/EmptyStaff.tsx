import { AppShell } from "../../components/AppShell";
import { PageHeader } from "../../components/primitives/PageHeader";
import { EmptyStaffPanel } from "../DataQuality";

/**
 * `/data-quality/empty-staff` — clubs with `staff_page_url` but zero coach
 * discoveries in the last N days. Useful for prioritizing scraper fixes.
 */
export default function EmptyStaffPage() {
  return (
    <AppShell>
      <PageHeader
        eyebrow="Data quality"
        title="Empty staff pages"
        description="Clubs whose staff page URL is set but produced zero coach discoveries inside the lookback window."
      />
      <EmptyStaffPanel />
    </AppShell>
  );
}
