import { type ReactNode } from "react";
import { Navigate } from "react-router-dom";
import { getAdminMeQueryKey, useAdminMe } from "@workspace/api-client-react";

/**
 * Gate that calls GET /api/v1/admin/me on mount.
 *   - 200 → render children
 *   - 401 / network error → redirect to /login
 *   - while the check is in flight → render a small loading shell
 *
 * Routes through the Orval-generated `useAdminMe` hook so the admin session
 * cookie (httpOnly, set by the login endpoint) travels via customFetch's
 * `credentials: 'include'` default for relative URLs. `retry: false` keeps
 * the auth check fast — a 401 should redirect immediately, not after three
 * backoff-spaced retries.
 */
interface ProtectedRouteProps {
  children: ReactNode;
}

export default function ProtectedRoute({ children }: ProtectedRouteProps) {
  const { isSuccess, isError, isPending } = useAdminMe({
    query: { queryKey: getAdminMeQueryKey(), retry: false },
  });

  if (isPending) {
    return (
      <div
        role="status"
        aria-label="Checking session"
        className="flex h-screen items-center justify-center bg-white"
      >
        <div className="flex flex-col items-center gap-3">
          <div className="h-8 w-8 animate-pulse rounded-full bg-neutral-300" />
          <span className="text-sm text-neutral-500">Checking session…</span>
        </div>
      </div>
    );
  }

  if (isError) {
    return <Navigate to="/login" replace />;
  }

  if (isSuccess) {
    return <>{children}</>;
  }

  return null;
}
