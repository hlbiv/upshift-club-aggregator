import { Navigate, Route, Routes } from "react-router-dom";
import ProtectedRoute from "./components/ProtectedRoute";
import LoginPage from "./pages/Login";
import OverviewPage from "./pages/Overview";
import SearchPage from "./pages/Search";
import ScraperHealthPage from "./pages/ScraperHealth";
import DedupPage from "./pages/Dedup";
import DedupDetailPage from "./pages/DedupDetail";
import DataQualityRedirect from "./pages/DataQuality";
import GaPremierPage from "./pages/dataquality/GaPremier";
import EmptyStaffPage from "./pages/dataquality/EmptyStaff";
import StaleScrapesPage from "./pages/dataquality/StaleScrapes";
import CoachMissesPage from "./pages/dataquality/CoachMisses";
import NavLeakedPage from "./pages/dataquality/NavLeaked";
import NumericOnlyPage from "./pages/dataquality/NumericOnly";
import ProAcademiesPage from "./pages/dataquality/ProAcademies";
import GrowthPage from "./pages/Growth";
import CoveragePage from "./pages/Coverage";
import CoverageLeaguePage from "./pages/CoverageLeague";
import SchedulerPage from "./pages/Scheduler";

/**
 * Admin dashboard router (rev 2 — sidebar shell + per-check data quality).
 *
 *   /                                 → Overview / Home (KPI strip + queues + 7d trend)
 *   /search?q=…                       → Global search results page
 *   /login                            → public auth surface
 *
 *   /scheduler                        → Scheduler (run now + recent runs)
 *   /scraper-health                   → Scraper health (per-target rollups)
 *   /coverage                         → Coverage (per-league rollup)
 *   /coverage/:leagueId               → Coverage drilldown
 *   /growth                           → Growth (KPI cards + 7d coverage trend)
 *
 *   /dedup                            → Dedup queue (J/K to navigate, Enter to open)
 *   /dedup/:id                        → Dedup pair detail (M=merge, R=reject)
 *
 *   /data-quality                     → redirect → /data-quality/nav-leaked
 *   /data-quality/ga-premier          → GA Premier orphan cleanup sweep
 *   /data-quality/empty-staff         → empty-staff-pages queue
 *   /data-quality/stale-scrapes       → stale-scrapes queue
 *   /data-quality/nav-leaked          → nav-leaked-names queue (M/R workflow)
 *   /data-quality/numeric-only        → numeric-only-names queue (M/R workflow)
 *
 * ProtectedRoute calls GET /api/v1/admin/me on mount; 200 → render
 * children, 401 → <Navigate to="/login" />.
 */
export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />

      <Route
        path="/"
        element={
          <ProtectedRoute>
            <OverviewPage />
          </ProtectedRoute>
        }
      />
      <Route
        path="/search"
        element={
          <ProtectedRoute>
            <SearchPage />
          </ProtectedRoute>
        }
      />

      <Route
        path="/scraper-health"
        element={
          <ProtectedRoute>
            <ScraperHealthPage />
          </ProtectedRoute>
        }
      />
      <Route
        path="/dedup"
        element={
          <ProtectedRoute>
            <DedupPage />
          </ProtectedRoute>
        }
      />
      <Route
        path="/dedup/:id"
        element={
          <ProtectedRoute>
            <DedupDetailPage />
          </ProtectedRoute>
        }
      />

      <Route
        path="/data-quality"
        element={
          <ProtectedRoute>
            <DataQualityRedirect />
          </ProtectedRoute>
        }
      />
      <Route
        path="/data-quality/ga-premier"
        element={
          <ProtectedRoute>
            <GaPremierPage />
          </ProtectedRoute>
        }
      />
      <Route
        path="/data-quality/empty-staff"
        element={
          <ProtectedRoute>
            <EmptyStaffPage />
          </ProtectedRoute>
        }
      />
      <Route
        path="/data-quality/stale-scrapes"
        element={
          <ProtectedRoute>
            <StaleScrapesPage />
          </ProtectedRoute>
        }
      />
      <Route
        path="/data-quality/coach-misses"
        element={
          <ProtectedRoute>
            <CoachMissesPage />
          </ProtectedRoute>
        }
      />
      <Route
        path="/data-quality/nav-leaked"
        element={
          <ProtectedRoute>
            <NavLeakedPage />
          </ProtectedRoute>
        }
      />
      <Route
        path="/data-quality/numeric-only"
        element={
          <ProtectedRoute>
            <NumericOnlyPage />
          </ProtectedRoute>
        }
      />
      <Route
        path="/data-quality/pro-academies"
        element={
          <ProtectedRoute>
            <ProAcademiesPage />
          </ProtectedRoute>
        }
      />

      <Route
        path="/growth"
        element={
          <ProtectedRoute>
            <GrowthPage />
          </ProtectedRoute>
        }
      />
      <Route
        path="/coverage"
        element={
          <ProtectedRoute>
            <CoveragePage />
          </ProtectedRoute>
        }
      />
      <Route
        path="/coverage/:leagueId"
        element={
          <ProtectedRoute>
            <CoverageLeaguePage />
          </ProtectedRoute>
        }
      />
      <Route
        path="/scheduler"
        element={
          <ProtectedRoute>
            <SchedulerPage />
          </ProtectedRoute>
        }
      />

      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
