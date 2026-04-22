import { AppShell } from "../../components/AppShell";
import { PageHeader } from "../../components/primitives/PageHeader";
import { NumericOnlyNamesPanel } from "../DataQuality";

/**
 * `/data-quality/numeric-only` — roster snapshots whose name column is
 * a jersey number, date, or other numeric token instead of a real player
 * name.
 */
export default function NumericOnlyPage() {
  return (
    <AppShell>
      <PageHeader
        eyebrow="Data quality"
        title="Numeric-only names"
        description="Roster rows whose player_name field parsed as a number or date. Confirm to flag, dismiss if it's a legitimate identifier."
      />
      <NumericOnlyNamesPanel />
    </AppShell>
  );
}
