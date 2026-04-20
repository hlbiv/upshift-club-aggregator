/**
 * `/v1/admin/me` — echo the current admin identity from req.adminAuth.
 *
 * For session auth, the email and role come straight from the DB lookup in
 * requireAdmin. For API-key auth there is no user — we synthesize a
 * "service account" identity so the UI can show "logged in as <key name>"
 * without special-casing the M2M path.
 */
import { Router, type IRouter } from "express";
import { AdminMeResponse } from "@hlbiv/api-zod/admin";

const router: IRouter = Router();

router.get("/me", (req, res, next): void => {
  try {
    if (!req.adminAuth) {
      // Shouldn't happen — the parent router is guarded by requireAdmin.
      res.status(401).json({ error: "unauthorized" });
      return;
    }

    if (req.adminAuth.kind === "session") {
      res.json(
        AdminMeResponse.parse({
          id: req.adminAuth.userId,
          email: req.adminAuth.email,
          role: req.adminAuth.role,
        }),
      );
      return;
    }

    // M2M caller. The Zod shape requires `email` (email-formatted string),
    // so we synthesize a service-account-style address keyed off the key
    // name. Role is "admin" — granular M2M scopes are a future-phase concern.
    const safeKeyName = req.adminAuth.keyName.replace(/[^a-zA-Z0-9-]/g, "-");
    res.json(
      AdminMeResponse.parse({
        id: req.adminAuth.keyId,
        email: `apikey+${safeKeyName}@upshift-data.local`,
        role: "admin",
      }),
    );
  } catch (err) {
    next(err);
  }
});

export default router;
