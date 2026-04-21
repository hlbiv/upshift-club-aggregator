import { Navigate, Route, Routes } from "react-router-dom";
import ProtectedRoute from "./components/ProtectedRoute";
import LoginPage from "./pages/Login";
import ScraperHealthPage from "./pages/ScraperHealth";
import DedupPage from "./pages/Dedup";
import DedupDetailPage from "./pages/DedupDetail";
import DataQualityPage from "./pages/DataQuality";
import GrowthPage from "./pages/Growth";
import CoveragePage from "./pages/Coverage";
import CoverageLeaguePage from "./pages/CoverageLeague";
import SchedulerPage from "./pages/Scheduler";

/**
 * Admin dashboard router.
 *
 *   /                → redirect to /scraper-health (auth-guarded)
 *   /login           → LoginPage (public)
 *   /scraper-health  → ScraperHealthPage (wrapped in ProtectedRoute)
 *   /dedup           → DedupPage (list of duplicate pairs)
 *   /dedup/:id       → DedupDetailPage (single pair, merge/reject)
 *   /data-quality    → DataQualityPage (GA Premier orphan cleanup)
 *   /growth          → GrowthPage (stat cards + coverage-trend chart)
 *   /scheduler       → SchedulerPage (Run now + recent runs for jobKeys)
 *
 * ProtectedRoute calls GET /api/v1/admin/me on mount. 200 → render children,
 * 401 → <Navigate to="/login" />.
 */
export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
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
            <DataQualityPage />
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
      <Route path="/" element={<Navigate to="/scraper-health" replace />} />
      <Route path="*" element={<Navigate to="/scraper-health" replace />} />
    </Routes>
  );
}
