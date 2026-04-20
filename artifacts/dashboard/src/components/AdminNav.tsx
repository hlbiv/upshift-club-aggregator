import { NavLink } from "react-router-dom";

/**
 * Minimal admin nav shown at the top of admin pages. Links for Scraper
 * health, Dedup review, Data quality, and Growth. The current page is
 * bold/underlined.
 *
 * No sidebar, no logo — internal tool, low ceremony. Plain text links keep
 * the chrome appropriately minimal for an internal dashboard.
 */
export default function AdminNav() {
  return (
    <nav
      aria-label="Admin sections"
      className="mb-6 flex items-center gap-6 border-b border-neutral-200 pb-3 text-sm"
    >
      <NavLink
        to="/scraper-health"
        end
        className={({ isActive }) =>
          isActive
            ? "font-semibold text-neutral-900 underline underline-offset-4"
            : "text-neutral-500 hover:text-neutral-800"
        }
      >
        Scraper health
      </NavLink>
      <NavLink
        to="/dedup"
        className={({ isActive }) =>
          isActive
            ? "font-semibold text-neutral-900 underline underline-offset-4"
            : "text-neutral-500 hover:text-neutral-800"
        }
      >
        Dedup review
      </NavLink>
      <NavLink
        to="/data-quality"
        className={({ isActive }) =>
          isActive
            ? "font-semibold text-neutral-900 underline underline-offset-4"
            : "text-neutral-500 hover:text-neutral-800"
        }
      >
        Data quality
      </NavLink>
      <NavLink
        to="/growth"
        className={({ isActive }) =>
          isActive
            ? "font-semibold text-neutral-900 underline underline-offset-4"
            : "text-neutral-500 hover:text-neutral-800"
        }
      >
        Growth
      </NavLink>
    </nav>
  );
}
