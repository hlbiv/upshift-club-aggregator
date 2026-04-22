import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import App from "./App";
import "./index.css";

// Admin dashboard React Query client. All admin pages read from the
// Orval-generated hooks in `@workspace/api-client-react`.
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      // No cross-page cache sharing today; retry on network hiccup only.
      retry: 1,
      // 60s default staleTime so the sidebar badge counts + Overview KPI
      // strip don't refire the same list endpoints on every route change.
      // Pages that need fresher data still call `refetch()` explicitly.
      staleTime: 60_000,
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
