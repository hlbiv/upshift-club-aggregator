import { AppShell } from "../../components/AppShell";
import { PageHeader } from "../../components/primitives/PageHeader";
import { NavLeakedNamesPanel } from "../DataQuality";

/**
 * `/data-quality/nav-leaked` — roster snapshots whose player_name column
 * was polluted with site navigation strings. Confirm/dismiss flow lives
 * inside `NavLeakedNamesPanel` (M/R queue shortcuts are wired there).
 */
export default function NavLeakedPage() {
  return (
    <AppShell>
      <PageHeader
        eyebrow="Data quality"
        title="Nav-leaked names"
        description="Suspect roster rows whose name field looks like nav-menu copy (Login, About, Sign in...). Confirm to flag, dismiss if it's a real player."
      />
      <NavLeakedNamesPanel />
    </AppShell>
  );
}
