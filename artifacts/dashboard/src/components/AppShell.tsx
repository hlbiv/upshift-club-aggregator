import { useState, type ReactNode } from "react";
import { Menu, UserCircle2 } from "lucide-react";
import Sidebar from "./Sidebar";
import GlobalSearch from "./GlobalSearch";
import { ShortcutHelpDialog } from "./ShortcutHelpDialog";

/**
 * Top-level dashboard shell. Persistent sidebar on the left, sticky header
 * on top with global search + user menu, page contents in the central
 * scrollable region capped at `max-w-7xl`. Mobile collapses the sidebar
 * behind a hamburger but the design target is desktop laptops.
 */
export function AppShell({ children }: { children: ReactNode }) {
  const [mobileOpen, setMobileOpen] = useState(false);
  return (
    <div className="flex min-h-screen">
      <Sidebar />

      {/* Mobile sidebar drawer (lightweight — not a full sheet to avoid bringing more libs in). */}
      {mobileOpen ? (
        <div className="fixed inset-0 z-40 md:hidden">
          <div
            className="absolute inset-0 bg-black/40"
            onClick={() => setMobileOpen(false)}
            aria-hidden
          />
          <div className="absolute inset-y-0 left-0 w-64 bg-white shadow-xl">
            <Sidebar />
          </div>
        </div>
      ) : null}

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="sticky top-0 z-20 flex h-14 items-center gap-3 border-b border-slate-200 bg-white/85 px-4 backdrop-blur md:px-6">
          <button
            type="button"
            onClick={() => setMobileOpen((o) => !o)}
            className="rounded-md p-1.5 text-slate-500 hover:bg-slate-100 md:hidden"
            aria-label="Toggle navigation"
          >
            <Menu className="h-5 w-5" />
          </button>

          <div className="min-w-0 flex-1">
            <GlobalSearch />
          </div>

          <ShortcutHelpDialog />

          <button
            type="button"
            className="flex items-center gap-2 rounded-full border border-slate-200 bg-white px-2.5 py-1 text-sm text-slate-700 hover:bg-slate-50"
            aria-label="Account"
          >
            <UserCircle2 className="h-4.5 w-4.5 text-slate-400" />
            <span className="hidden sm:inline">Admin</span>
          </button>
        </header>

        <main className="flex-1 overflow-x-auto px-4 py-6 md:px-8 md:py-8">
          <div className="mx-auto w-full max-w-7xl">{children}</div>
        </main>
      </div>
    </div>
  );
}
