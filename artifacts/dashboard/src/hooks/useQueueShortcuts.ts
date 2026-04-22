import { useEffect } from "react";

interface QueueShortcutOptions {
  enabled?: boolean;
  onNext?: () => void;
  onPrev?: () => void;
  /** Primary action — typically Merge / Confirm. Bound to "M". */
  onPrimary?: () => void;
  /** Secondary action — typically Reject / Dismiss. Bound to "R". */
  onSecondary?: () => void;
  /** Open the highlighted item (Enter). */
  onOpen?: () => void;
}

/**
 * Tiny hook that wires J/K/M/R/Enter to the supplied callbacks for a
 * queue-style page (Dedup list, NavLeaked, NumericOnly).
 *
 * Ignored when:
 *   - the user is typing in an editable element
 *   - any modifier key is held (so Cmd/Ctrl combos for browser shortcuts
 *     pass through)
 *   - `enabled` is explicitly false
 */
export function useQueueShortcuts({
  enabled = true,
  onNext,
  onPrev,
  onPrimary,
  onSecondary,
  onOpen,
}: QueueShortcutOptions): void {
  useEffect(() => {
    if (!enabled) return;
    function onKey(e: KeyboardEvent) {
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      const target = e.target as HTMLElement | null;
      if (
        target &&
        (target.tagName === "INPUT" ||
          target.tagName === "TEXTAREA" ||
          target.tagName === "SELECT" ||
          target.isContentEditable)
      ) {
        return;
      }
      switch (e.key.toLowerCase()) {
        case "j":
          if (onNext) {
            e.preventDefault();
            onNext();
          }
          break;
        case "k":
          if (onPrev) {
            e.preventDefault();
            onPrev();
          }
          break;
        case "m":
          if (onPrimary) {
            e.preventDefault();
            onPrimary();
          }
          break;
        case "r":
          if (onSecondary) {
            e.preventDefault();
            onSecondary();
          }
          break;
        case "enter":
          if (onOpen) {
            e.preventDefault();
            onOpen();
          }
          break;
        default:
          break;
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [enabled, onNext, onPrev, onPrimary, onSecondary, onOpen]);
}
