import { useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import * as Dialog from "@radix-ui/react-dialog";
import {
  useGetClubDuplicate,
  useMergeClubDuplicate,
  useRejectClubDuplicate,
  type ClubDuplicateDetail as ClubDuplicateDetailType,
} from "@workspace/api-client-react";
import AdminNav from "../components/AdminNav";
import { StatusBadge, formatDate, formatError, snapshotName } from "./Dedup";

/**
 * Dedup detail view — side-by-side panels + merge/reject action bar.
 *
 *   GET  /api/v1/admin/dedup/clubs/:id           → ClubDuplicateDetail
 *   POST /api/v1/admin/dedup/clubs/:id/merge     → ClubDuplicateMergeResponse
 *   POST /api/v1/admin/dedup/clubs/:id/reject    → { ok: true }
 *
 * Migrated from `adminFetch()` to the Orval-generated
 * `useGetClubDuplicate` / `useMergeClubDuplicate` / `useRejectClubDuplicate`
 * hooks (Workstream A).
 *
 * The detail response contains `leftCurrent` / `rightCurrent` (the live
 * canonical_club rows at open-time) plus affiliation / roster-snapshot
 * counts. Panels render from the current rows, not the stale snapshots.
 *
 * Merge is gated behind a Radix confirmation dialog — one misfire costs
 * a reparented club graph. Reject is optimistic: one click, redirect.
 */

type Snapshot = Record<string, unknown>;

export default function DedupDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [actionError, setActionError] = useState<string | null>(null);
  const [confirmWinner, setConfirmWinner] = useState<"left" | "right" | null>(
    null,
  );

  const idIsValid = !!id && /^\d+$/.test(id);
  // The generated hook already gates the query on `!!id` (0 → disabled).
  // Pass 0 when the URL param is invalid so the query never fires.
  const numericId = idIsValid ? Number(id) : 0;

  const detailQuery = useGetClubDuplicate(numericId);
  const mergeMutation = useMergeClubDuplicate();
  const rejectMutation = useRejectClubDuplicate();

  const submitting = mergeMutation.isPending || rejectMutation.isPending;

  async function doMerge(winnerSide: "left" | "right") {
    if (!detailQuery.data || !idIsValid) return;
    const detail = detailQuery.data;
    const winnerId =
      winnerSide === "left" ? detail.leftClubId : detail.rightClubId;
    const loserId =
      winnerSide === "left" ? detail.rightClubId : detail.leftClubId;

    setActionError(null);
    try {
      const result = await mergeMutation.mutateAsync({
        id: numericId,
        data: { winnerId, loserId },
      });
      setConfirmWinner(null);
      const summary =
        `Merged into #${result.winnerId}: ` +
        `${result.loserAliasesCreated} alias(es), ` +
        `${result.affiliationsReparented} affiliation(s), ` +
        `${result.rosterSnapshotsReparented} roster snapshot(s) reparented`;
      navigate("/dedup", { replace: true, state: { flash: summary } });
    } catch (err) {
      setActionError(formatError(err));
    }
  }

  async function doReject() {
    if (!idIsValid) return;
    setActionError(null);
    try {
      await rejectMutation.mutateAsync({ id: numericId, data: {} });
      navigate("/dedup", {
        replace: true,
        state: { flash: `Pair #${id} rejected` },
      });
    } catch (err) {
      setActionError(formatError(err));
    }
  }

  return (
    <main className="mx-auto max-w-6xl px-6 py-8">
      <AdminNav />
      <header className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-neutral-900">
            Dedup pair{id ? ` #${id}` : ""}
          </h1>
          <p className="text-sm text-neutral-500">
            Compare the two clubs below, then pick a winner or reject the pair.
          </p>
        </div>
        <button
          type="button"
          onClick={() => navigate("/dedup")}
          className="rounded-md border border-neutral-300 bg-white px-3 py-1.5 text-sm text-neutral-700 hover:bg-neutral-100"
        >
          Back to queue
        </button>
      </header>

      {!idIsValid ? (
        <Placeholder label="Failed to load: Invalid pair id" />
      ) : detailQuery.isLoading ? (
        <Placeholder label="Loading…" />
      ) : detailQuery.error ? (
        <Placeholder
          label={`Failed to load: ${formatError(detailQuery.error)}`}
        />
      ) : detailQuery.data ? (
        <>
          <div className="mb-4 flex items-center gap-3 text-sm text-neutral-600">
            <StatusBadge status={detailQuery.data.status} />
            <span>score: {detailQuery.data.score.toFixed(3)}</span>
            <span className="text-neutral-400">·</span>
            <span>method: {detailQuery.data.method}</span>
            <span className="text-neutral-400">·</span>
            <span>created: {formatDate(detailQuery.data.createdAt)}</span>
          </div>

          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            <ClubPanel
              side="Left"
              clubId={detailQuery.data.leftClubId}
              current={detailQuery.data.leftCurrent}
              affiliationCount={
                detailQuery.data.affiliations.leftAffiliationCount
              }
              rosterCount={detailQuery.data.rosters.leftRosterSnapshotCount}
            />
            <ClubPanel
              side="Right"
              clubId={detailQuery.data.rightClubId}
              current={detailQuery.data.rightCurrent}
              affiliationCount={
                detailQuery.data.affiliations.rightAffiliationCount
              }
              rosterCount={detailQuery.data.rosters.rightRosterSnapshotCount}
            />
          </div>

          {actionError !== null ? (
            <div
              role="alert"
              className="mt-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
            >
              {actionError}
            </div>
          ) : null}

          {detailQuery.data.status === "pending" ? (
            <ActionBar
              disabled={submitting}
              leftName={snapshotName(detailQuery.data.leftCurrent)}
              rightName={snapshotName(detailQuery.data.rightCurrent)}
              onPickLeft={() => setConfirmWinner("left")}
              onPickRight={() => setConfirmWinner("right")}
              onReject={doReject}
            />
          ) : (
            <p className="mt-6 text-sm text-neutral-500">
              This pair has been resolved ({detailQuery.data.status}). No
              further actions available.
            </p>
          )}

          <MergeConfirmDialog
            open={confirmWinner !== null}
            onOpenChange={(o) => {
              if (!o) setConfirmWinner(null);
            }}
            winnerSide={confirmWinner}
            detail={detailQuery.data}
            submitting={submitting}
            onConfirm={() => {
              if (confirmWinner !== null) void doMerge(confirmWinner);
            }}
          />
        </>
      ) : null}
    </main>
  );
}

function ClubPanel({
  side,
  clubId,
  current,
  affiliationCount,
  rosterCount,
}: {
  side: "Left" | "Right";
  clubId: number;
  current: Snapshot;
  affiliationCount: number;
  rosterCount: number;
}) {
  const name = snapshotName(current);
  const aliases = snapshotAliases(current);

  return (
    <section
      aria-labelledby={`panel-${side.toLowerCase()}-heading`}
      className="rounded-lg border border-neutral-200 bg-white p-4"
    >
      <div className="mb-3 flex items-baseline justify-between">
        <h2
          id={`panel-${side.toLowerCase()}-heading`}
          className="text-lg font-semibold text-neutral-900"
        >
          {side}: {name}
        </h2>
        <span className="text-xs text-neutral-500">id: {clubId}</span>
      </div>

      <dl className="space-y-2 text-sm">
        <Row label="Aliases">
          {aliases.length > 0 ? aliases.join(", ") : "—"}
        </Row>
        <Row label="Location">{formatLocation(current)}</Row>
        <Row label="Website">{formatWebsite(current)}</Row>
        <Row label="Founded">{snapshotField(current, "foundedYear", "founded_year") ?? "—"}</Row>
        <Row label="Affiliations">{affiliationCount}</Row>
        <Row label="Roster snapshots">{rosterCount}</Row>
      </dl>
    </section>
  );
}

function Row({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex gap-3">
      <dt className="w-36 shrink-0 text-neutral-500">{label}</dt>
      <dd className="text-neutral-900">{children}</dd>
    </div>
  );
}

function ActionBar({
  disabled,
  leftName,
  rightName,
  onPickLeft,
  onPickRight,
  onReject,
}: {
  disabled: boolean;
  leftName: string;
  rightName: string;
  onPickLeft: () => void;
  onPickRight: () => void;
  onReject: () => void;
}) {
  return (
    <div className="mt-6 flex flex-wrap gap-3 rounded-lg border border-neutral-200 bg-neutral-50 p-4">
      <button
        type="button"
        disabled={disabled}
        onClick={onPickLeft}
        className="rounded-md bg-neutral-900 px-4 py-2 text-sm font-medium text-white hover:bg-neutral-800 disabled:cursor-not-allowed disabled:opacity-50"
      >
        Pick left as winner ({leftName})
      </button>
      <button
        type="button"
        disabled={disabled}
        onClick={onPickRight}
        className="rounded-md bg-neutral-900 px-4 py-2 text-sm font-medium text-white hover:bg-neutral-800 disabled:cursor-not-allowed disabled:opacity-50"
      >
        Pick right as winner ({rightName})
      </button>
      <button
        type="button"
        disabled={disabled}
        onClick={onReject}
        className="ml-auto rounded-md border border-neutral-300 bg-white px-4 py-2 text-sm font-medium text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-50"
      >
        Reject pair
      </button>
    </div>
  );
}

function MergeConfirmDialog({
  open,
  onOpenChange,
  winnerSide,
  detail,
  submitting,
  onConfirm,
}: {
  open: boolean;
  onOpenChange: (o: boolean) => void;
  winnerSide: "left" | "right" | null;
  detail: ClubDuplicateDetailType;
  submitting: boolean;
  onConfirm: () => void;
}) {
  if (winnerSide === null) {
    // Still render Dialog.Root so it can be controlled open=false cleanly.
    return (
      <Dialog.Root open={open} onOpenChange={onOpenChange}>
        <Dialog.Portal />
      </Dialog.Root>
    );
  }

  const winner =
    winnerSide === "left" ? detail.leftCurrent : detail.rightCurrent;
  const loser =
    winnerSide === "left" ? detail.rightCurrent : detail.leftCurrent;
  const winnerId =
    winnerSide === "left" ? detail.leftClubId : detail.rightClubId;
  const loserId =
    winnerSide === "left" ? detail.rightClubId : detail.leftClubId;

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 bg-black/40" />
        <Dialog.Content className="fixed left-1/2 top-1/2 w-full max-w-md -translate-x-1/2 -translate-y-1/2 rounded-lg bg-white p-6 shadow-lg">
          <Dialog.Title className="text-lg font-semibold text-neutral-900">
            Confirm merge
          </Dialog.Title>
          <Dialog.Description className="mt-2 text-sm text-neutral-600">
            Merge{" "}
            <strong className="text-neutral-900">
              {snapshotName(loser)} (#{loserId})
            </strong>{" "}
            into{" "}
            <strong className="text-neutral-900">
              {snapshotName(winner)} (#{winnerId})
            </strong>
            . Aliases, affiliations, and roster snapshots will be reparented.
            This cannot be undone.
          </Dialog.Description>
          <div className="mt-6 flex justify-end gap-2">
            <Dialog.Close asChild>
              <button
                type="button"
                disabled={submitting}
                className="rounded-md border border-neutral-300 bg-white px-3 py-1.5 text-sm text-neutral-800 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-50"
              >
                Cancel
              </button>
            </Dialog.Close>
            <button
              type="button"
              disabled={submitting}
              onClick={onConfirm}
              className="rounded-md bg-neutral-900 px-3 py-1.5 text-sm font-medium text-white hover:bg-neutral-800 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {submitting ? "Merging…" : "Confirm merge"}
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function Placeholder({ label }: { label: string }) {
  return (
    <div className="rounded-lg border border-dashed border-neutral-300 bg-white px-4 py-8 text-center text-sm text-neutral-500">
      {label}
    </div>
  );
}

// --- snapshot helpers ----------------------------------------------------

function snapshotField(
  snapshot: Snapshot,
  ...keys: string[]
): string | null {
  for (const k of keys) {
    const v = snapshot[k];
    if (typeof v === "string" && v.trim().length > 0) return v;
    if (typeof v === "number") return String(v);
  }
  return null;
}

function snapshotAliases(snapshot: Snapshot): string[] {
  const v = snapshot.aliases;
  if (Array.isArray(v)) {
    return v
      .map((x) => {
        if (typeof x === "string") return x;
        if (x && typeof x === "object") {
          const maybe = (x as Record<string, unknown>).aliasName;
          if (typeof maybe === "string") return maybe;
          const alt = (x as Record<string, unknown>).alias_name;
          if (typeof alt === "string") return alt;
        }
        return null;
      })
      .filter((s): s is string => typeof s === "string" && s.length > 0);
  }
  return [];
}

function formatLocation(snapshot: Snapshot): string {
  const city = snapshotField(snapshot, "city");
  const state = snapshotField(snapshot, "state", "stateCode", "state_code");
  if (city && state) return `${city}, ${state}`;
  if (city) return city;
  if (state) return state;
  return "—";
}

function formatWebsite(snapshot: Snapshot): React.ReactNode {
  const url = snapshotField(
    snapshot,
    "websiteUrl",
    "website_url",
    "website",
    "homepage",
  );
  if (!url) return "—";
  return (
    <a
      href={url}
      target="_blank"
      rel="noopener noreferrer"
      className="text-blue-700 underline hover:text-blue-900"
    >
      {url}
    </a>
  );
}
