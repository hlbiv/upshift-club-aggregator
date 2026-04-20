import { useEffect, useState, type ReactNode } from "react";
import { Navigate } from "react-router-dom";
import { adminFetch } from "../lib/api";

/**
 * Gate that calls GET /v1/admin/me on mount.
 *   - 200 → render children
 *   - 401 / network error → redirect to /login
 *   - while the check is in flight → render a small loading shell
 *
 * No caching / context: each mount re-checks. Session lifetime is owned by
 * the cookie (httpOnly, set by the login endpoint). Future phases can
 * introduce a context provider if multiple components need the admin
 * identity at the same time.
 */
type AuthState = "loading" | "authed" | "unauthed";

interface ProtectedRouteProps {
  children: ReactNode;
}

export default function ProtectedRoute({ children }: ProtectedRouteProps) {
  const [state, setState] = useState<AuthState>("loading");

  useEffect(() => {
    let cancelled = false;
    adminFetch("/v1/admin/me", { method: "GET" })
      .then((res) => {
        if (cancelled) return;
        setState(res.ok ? "authed" : "unauthed");
      })
      .catch(() => {
        if (cancelled) return;
        setState("unauthed");
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (state === "loading") {
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

  if (state === "unauthed") {
    return <Navigate to="/login" replace />;
  }

  return <>{children}</>;
}
