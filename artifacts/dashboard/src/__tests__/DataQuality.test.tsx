import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import DataQualityPage from "../pages/DataQuality";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("DataQualityPage", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("submits defaults (dryRun=true, limit=500) and renders counts + sample names", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        scanned: 1000,
        flagged: 12,
        deleted: 0,
        sampleNames: ["GA Premier 2011B", "GA Premier garbage row"],
      }),
    );
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    render(
      <MemoryRouter>
        <DataQualityPage />
      </MemoryRouter>,
    );

    await user.click(screen.getByRole("button", { name: /run sweep/i }));

    await waitFor(() => {
      expect(screen.getByText("1,000")).toBeInTheDocument();
    });

    // Counts rendered.
    expect(screen.getByText("Scanned")).toBeInTheDocument();
    expect(screen.getByText("Flagged")).toBeInTheDocument();
    expect(screen.getByText("Deleted")).toBeInTheDocument();
    expect(screen.getByText("12")).toBeInTheDocument();

    // Sample names rendered.
    expect(screen.getByText("GA Premier 2011B")).toBeInTheDocument();
    expect(screen.getByText("GA Premier garbage row")).toBeInTheDocument();

    // Request body used the defaults.
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body as string)).toEqual({
      dryRun: true,
      limit: 500,
    });
  });

  it("opens confirmation dialog and commits deletion with dryRun=false", async () => {
    const fetchMock = vi
      .fn()
      // First: dry run returns flagged > 0.
      .mockResolvedValueOnce(
        jsonResponse({
          scanned: 500,
          flagged: 7,
          deleted: 0,
          sampleNames: ["bad-row-1", "bad-row-2"],
        }),
      )
      // Second: real delete.
      .mockResolvedValueOnce(
        jsonResponse({
          scanned: 500,
          flagged: 7,
          deleted: 7,
          sampleNames: [],
        }),
      );
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    render(
      <MemoryRouter>
        <DataQualityPage />
      </MemoryRouter>,
    );

    await user.click(screen.getByRole("button", { name: /run sweep/i }));

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: /commit deletion/i }),
      ).toBeInTheDocument();
    });

    await user.click(
      screen.getByRole("button", { name: /commit deletion/i }),
    );

    // Confirm dialog opens with count in title.
    await waitFor(() => {
      expect(
        screen.getByText(/delete 7 roster snapshot rows\?/i),
      ).toBeInTheDocument();
    });
    expect(screen.getByText(/cannot be undone/i)).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /^delete$/i }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledTimes(2);
    });

    const [, secondInit] = fetchMock.mock.calls[1] as [string, RequestInit];
    expect(secondInit.method).toBe("POST");
    expect(JSON.parse(secondInit.body as string)).toEqual({
      dryRun: false,
      limit: 500,
    });

    // Success toast shows the deleted count.
    await waitFor(() => {
      expect(screen.getByRole("status")).toHaveTextContent(/deleted 7 rows/i);
    });
  });
});
