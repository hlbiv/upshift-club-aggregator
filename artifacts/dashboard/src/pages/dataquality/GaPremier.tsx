import { AppShell } from "../../components/AppShell";
import { PageHeader } from "../../components/primitives/PageHeader";
import { GaPremierPanel } from "../DataQuality";

/**
 * `/data-quality/ga-premier` — GA Premier orphan cleanup sweep.
 *
 * Wraps the existing GaPremierPanel (state + dialogs unchanged) in the
 * standardized AppShell + PageHeader. The panel itself is a write-action
 * surface so it doesn't need a per-row keyboard queue.
 */
export default function GaPremierPage() {
  return (
    <AppShell>
      <PageHeader
        eyebrow="Data quality"
        title="GA Premier orphans"
        description="Sweep clubs that look like GA Premier roster orphans. Dry-run by default; toggle off to apply changes."
      />
      <GaPremierPanel />
    </AppShell>
  );
}
