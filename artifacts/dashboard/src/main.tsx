import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import App from "./App";
import "./index.css";

// Admin dashboard React Query client. First consumer is ScraperHealthPage,
// which reads from the Orval-generated `useListScrapeRuns` /
// `useListScrapeHealth` hooks (lib/api-client-react). Other admin pages
// still use the hand-rolled `adminFetch()` helper — they'll migrate one
// at a time. See Workstream A notes in CLAUDE.md.
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      // No cross-page cache sharing today; retry on network hiccup only.
      retry: 1,
    },
  },
});

createRoot(document.getElementById("root")!).render(
  <QueryClientProvider client={queryClient}>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </QueryClientProvider>,
);
