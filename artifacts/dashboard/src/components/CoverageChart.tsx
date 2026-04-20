import type { CoverageTrendPoint } from "@hlbiv/api-zod/admin";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

/**
 * Daily scrape-run coverage chart. Split into its own module so the
 * recharts dependency (~400KB) loads lazily from the Growth page and
 * doesn't weigh down the shared bundle used by other admin pages.
 */
export default function CoverageChart({
  points,
}: {
  points: CoverageTrendPoint[];
}) {
  return (
    <div
      className="h-80 w-full rounded-lg border border-neutral-200 bg-white p-4"
      role="img"
      aria-label="Line chart of daily scrape successes and failures"
    >
      <ResponsiveContainer width="100%" height="100%">
        <LineChart
          data={points}
          margin={{ top: 8, right: 16, bottom: 8, left: 0 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="#e5e5e5" />
          <XAxis dataKey="date" tick={{ fontSize: 12 }} />
          <YAxis allowDecimals={false} tick={{ fontSize: 12 }} />
          <Tooltip />
          <Legend />
          <Line
            type="monotone"
            dataKey="successes"
            stroke="#16a34a"
            strokeWidth={2}
            dot={false}
            name="Successes"
          />
          <Line
            type="monotone"
            dataKey="failures"
            stroke="#dc2626"
            strokeWidth={2}
            dot={false}
            name="Failures"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
