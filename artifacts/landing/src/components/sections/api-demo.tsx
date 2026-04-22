import { Terminal } from "lucide-react";
import { Button } from "@/components/ui/button";

export default function ApiDemo() {
  const codeSnippet = `curl -X GET "https://api.upshiftdata.com/v3/clubs/club_8f92a1b/rosters" \\
  -H "Authorization: Bearer ud_live_xxxxxxxxxxxxxxxx"

{
  "data": [
    {
      "id": "ros_992ha",
      "team_name": "FC Dallas U17 Academy",
      "age_group": "U17",
      "gender": "M",
      "season": "2024-2025",
      "league": {
        "id": "lg_mlsnext",
        "name": "MLS NEXT"
      },
      "players": 22,
      "head_coach": {
        "id": "co_77x12",
        "name": "John Doe"
      }
    }
  ]
}`;

  return (
    <section id="api" className="py-24 relative">
      <div className="container mx-auto px-6">
        <div className="grid lg:grid-cols-2 gap-16 items-center">
          
          <div className="order-2 lg:order-1 bg-[#0d1117] rounded-xl border border-white/10 p-6 overflow-hidden relative">
            <div className="absolute top-0 inset-x-0 h-10 bg-white/5 border-b border-white/10 flex items-center px-4 gap-2">
              <Terminal className="w-4 h-4 text-muted-foreground" />
              <span className="text-xs font-mono text-muted-foreground">Terminal</span>
            </div>
            <pre className="mt-8 overflow-x-auto text-sm font-mono leading-relaxed text-blue-300">
              <code>{codeSnippet}</code>
            </pre>
          </div>

          <div className="order-1 lg:order-2">
            <h2 className="text-3xl md:text-4xl font-bold tracking-tight mb-6">
              A Typed REST API.
            </h2>
            <p className="text-muted-foreground text-lg mb-8">
              Access the entire graph via our OpenAPI 3.1 compliant REST API. Protected by API key authentication. Ready for your data engineering or application teams.
            </p>
            <div className="flex gap-4">
              <a href="/dashboard">
                <Button className="bg-primary text-primary-foreground hover:bg-primary/90 font-medium h-11 px-8">
                  Get API Keys
                </Button>
              </a>
            </div>
          </div>

        </div>
      </div>
    </section>
  );
}
