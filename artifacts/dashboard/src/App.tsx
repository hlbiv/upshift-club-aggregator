import { Navigate, Route, Routes } from "react-router-dom";
import ProtectedRoute from "./components/ProtectedRoute";
import LoginPage from "./pages/Login";
import ScraperHealthPage from "./pages/ScraperHealth";

/**
 * Admin dashboard router.
 *
 *   /              → redirect to /scraper-health (auth-guarded)
 *   /login         → LoginPage (public)
 *   /scraper-health → ScraperHealthPage (wrapped in ProtectedRoute)
 *
 * ProtectedRoute calls GET /api/v1/admin/me on mount. 200 → render children,
 * 401 → <Navigate to="/login" />. Phase B.3 only ships these two pages;
 * scheduler / dedup / data-quality pages are future work.
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
      <Route path="/" element={<Navigate to="/scraper-health" replace />} />
      <Route path="*" element={<Navigate to="/scraper-health" replace />} />
    </Routes>
  );
}
