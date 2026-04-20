import { useState, type FormEvent } from "react";
import { useNavigate } from "react-router-dom";
import type { AdminLoginResponse } from "@hlbiv/api-zod/admin";
import { adminFetch } from "../lib/api";

/**
 * Admin login page.
 *
 *   POST /v1/admin/auth/login { email, password }
 *     - 200 → response matches AdminLoginResponse; a session cookie is
 *       set via Set-Cookie. Navigate to /scraper-health.
 *     - 4xx → show {error} body as a red inline message.
 *     - network error → show a generic fallback.
 *
 * Uses plain HTML form + controlled inputs — Radix primitives are
 * available (see @radix-ui/react-label etc.) but for a 2-field login
 * the native form is simpler and equally accessible. Radix form wiring
 * can come in later phases when we need multi-step flows.
 */
export default function LoginPage() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const navigate = useNavigate();

  async function onSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      const res = await adminFetch("/v1/admin/auth/login", {
        method: "POST",
        body: JSON.stringify({ email, password }),
      });
      if (res.ok) {
        // We don't need the body for navigation, but type-check it.
        const body = (await res.json()) as AdminLoginResponse;
        void body;
        navigate("/scraper-health", { replace: true });
        return;
      }
      // Attempt to parse {error: string}; otherwise fall back.
      let message = "Login failed.";
      try {
        const body = (await res.json()) as { error?: unknown };
        if (typeof body.error === "string") message = body.error;
      } catch {
        // ignore JSON parse errors — keep generic message
      }
      setError(message);
    } catch {
      setError("Network error. Please try again.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-neutral-50 px-4">
      <form
        onSubmit={onSubmit}
        className="w-full max-w-sm rounded-lg border border-neutral-200 bg-white p-8 shadow-sm"
        aria-labelledby="login-heading"
      >
        <h1
          id="login-heading"
          className="mb-1 text-xl font-semibold text-neutral-900"
        >
          Upshift Data admin
        </h1>
        <p className="mb-6 text-sm text-neutral-500">
          Sign in to view scrape health.
        </p>

        <div className="mb-4">
          <label
            htmlFor="email"
            className="mb-1 block text-sm font-medium text-neutral-700"
          >
            Email
          </label>
          <input
            id="email"
            name="email"
            type="email"
            autoComplete="email"
            required
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            className="w-full rounded-md border border-neutral-300 px-3 py-2 text-sm focus:border-neutral-900 focus:outline-none focus:ring-1 focus:ring-neutral-900"
          />
        </div>

        <div className="mb-6">
          <label
            htmlFor="password"
            className="mb-1 block text-sm font-medium text-neutral-700"
          >
            Password
          </label>
          <input
            id="password"
            name="password"
            type="password"
            autoComplete="current-password"
            required
            minLength={8}
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            className="w-full rounded-md border border-neutral-300 px-3 py-2 text-sm focus:border-neutral-900 focus:outline-none focus:ring-1 focus:ring-neutral-900"
          />
        </div>

        {error !== null && (
          <div
            role="alert"
            className="mb-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
          >
            {error}
          </div>
        )}

        <button
          type="submit"
          disabled={submitting}
          className="w-full rounded-md bg-neutral-900 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-neutral-800 disabled:cursor-not-allowed disabled:opacity-50"
        >
          {submitting ? "Signing in…" : "Sign in"}
        </button>
      </form>
    </div>
  );
}
