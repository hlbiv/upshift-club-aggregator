import { useState, type FormEvent } from "react";
import { useNavigate } from "react-router-dom";
import { useAdminLogin } from "@workspace/api-client-react";

/**
 * Admin login page.
 *
 *   POST /api/v1/admin/auth/login { email, password }
 *     - 200 → response matches AdminLoginResponse; a session cookie is
 *       set via Set-Cookie. Navigate to /scraper-health.
 *     - 4xx → show the server's `{error}` body as a red inline message.
 *     - network error → show a generic fallback.
 *
 * Migrated from the hand-rolled `adminFetch()` helper to the Orval-generated
 * `useAdminLogin` mutation hook (Workstream A). The customFetch mutator
 * throws an `ApiError` on non-2xx — we unwrap the `{error}` field out of
 * `err.data` to preserve the pre-migration error UX.
 */
export default function LoginPage() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  const loginMutation = useAdminLogin();

  async function onSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError(null);
    try {
      await loginMutation.mutateAsync({ data: { email, password } });
      navigate("/scraper-health", { replace: true });
    } catch (err) {
      setError(extractErrorMessage(err));
    }
  }

  const submitting = loginMutation.isPending;

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

/**
 * The customFetch mutator throws `ApiError` on non-2xx with the parsed body
 * attached as `err.data`. Admin routes surface `{error: string}`; prefer that
 * if present, otherwise fall back to the message or a generic copy.
 */
function extractErrorMessage(err: unknown): string {
  if (err && typeof err === "object") {
    const data = (err as { data?: unknown }).data;
    if (data && typeof data === "object") {
      const maybe = (data as { error?: unknown }).error;
      if (typeof maybe === "string") return maybe;
    }
    const status = (err as { status?: unknown }).status;
    // Distinguish server-returned 4xx/5xx (ApiError has numeric status) from
    // true network failures. The prior impl showed a generic network error
    // for thrown fetches; keep that shape for parity.
    if (typeof status !== "number") return "Network error. Please try again.";
    if (err instanceof Error && err.message) return err.message;
  }
  return "Login failed.";
}
