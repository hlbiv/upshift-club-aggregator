import { useEffect, useState } from "react";
import * as Dialog from "@radix-ui/react-dialog";
import { Keyboard } from "lucide-react";

/**
 * Global "press ? for shortcuts" help dialog. Opens via:
 *   - Clicking the keyboard icon button (header)
 *   - Pressing `?` (Shift+/) anywhere outside an editable field
 *
 * The shortcut list is intentionally short and stable — each queue page
 * uses the same J/K/M/R verbs, so one help dialog covers everything. If we
 * grow page-specific shortcuts, switch to a context-provider keyed dialog.
 */
export function ShortcutHelpDialog() {
  const [open, setOpen] = useState(false);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const target = e.target as HTMLElement | null;
      const inEditable =
        target &&
        (target.tagName === "INPUT" ||
          target.tagName === "TEXTAREA" ||
          target.isContentEditable);
      if (!inEditable && e.key === "?") {
        e.preventDefault();
        setOpen((o) => !o);
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Trigger asChild>
        <button
          type="button"
          aria-label="Keyboard shortcuts"
          title="Keyboard shortcuts (?)"
          className="rounded-md border border-slate-200 bg-white p-1.5 text-slate-500 hover:bg-slate-50 hover:text-slate-700"
        >
          <Keyboard className="h-4 w-4" />
        </button>
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-40 bg-black/30" />
        <Dialog.Content className="fixed left-1/2 top-1/2 z-50 w-full max-w-md -translate-x-1/2 -translate-y-1/2 rounded-xl bg-white p-6 shadow-xl">
          <Dialog.Title className="text-base font-semibold text-slate-900">
            Keyboard shortcuts
          </Dialog.Title>
          <Dialog.Description className="mt-1 text-sm text-slate-500">
            Work the queues without lifting your hands off the keyboard.
          </Dialog.Description>

          <div className="mt-4 space-y-4 text-sm">
            <ShortcutGroup
              heading="Global"
              rows={[
                ["/", "Focus search"],
                ["Cmd/Ctrl + K", "Focus search"],
                ["?", "Toggle this dialog"],
              ]}
            />
            <ShortcutGroup
              heading="Review queues"
              rows={[
                ["J", "Next item"],
                ["K", "Previous item"],
                ["M", "Merge / confirm"],
                ["R", "Reject / dismiss"],
                ["Enter", "Open selected item"],
              ]}
            />
          </div>

          <div className="mt-6 flex justify-end">
            <Dialog.Close asChild>
              <button
                type="button"
                className="rounded-md bg-indigo-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-indigo-700"
              >
                Got it
              </button>
            </Dialog.Close>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function ShortcutGroup({
  heading,
  rows,
}: {
  heading: string;
  rows: [string, string][];
}) {
  return (
    <div>
      <p className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-500">
        {heading}
      </p>
      <ul className="divide-y divide-slate-100 rounded-md border border-slate-100">
        {rows.map(([key, desc]) => (
          <li
            key={key}
            className="flex items-center justify-between px-3 py-1.5"
          >
            <span className="text-slate-700">{desc}</span>
            <kbd className="rounded border border-slate-200 bg-slate-50 px-1.5 py-0.5 font-mono text-[11px] text-slate-700">
              {key}
            </kbd>
          </li>
        ))}
      </ul>
    </div>
  );
}
