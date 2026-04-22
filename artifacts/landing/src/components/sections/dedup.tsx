import { motion } from "framer-motion";
import { Fingerprint, CheckCircle2 } from "lucide-react";

export default function Dedup() {
  return (
    <section className="py-24 relative">
      <div className="container mx-auto px-6">
        <div className="glass-panel p-8 md:p-12 rounded-3xl overflow-hidden relative">
          <div className="absolute top-0 right-0 w-1/2 h-full bg-gradient-to-l from-primary/10 to-transparent pointer-events-none" />
          
          <div className="grid md:grid-cols-2 gap-12 relative z-10 items-center">
            <div>
              <div className="w-12 h-12 rounded-xl bg-primary/10 border border-primary/20 flex items-center justify-center mb-6">
                <Fingerprint className="w-6 h-6 text-primary" />
              </div>
              <h2 className="text-3xl md:text-4xl font-bold tracking-tight mb-6">
                Relentless Deduplication
              </h2>
              <p className="text-muted-foreground text-lg mb-6">
                Youth soccer is plagued by typos, acronyms, and missing IDs. "FC Dallas", "FC Dallas Youth", and "FCD" are the same club. We fix this automatically.
              </p>
              
              <ul className="space-y-4">
                <li className="flex items-start gap-3 text-sm text-muted-foreground">
                  <CheckCircle2 className="w-5 h-5 text-primary shrink-0" />
                  <span>Algorithmic fuzzy-name matching tuned specifically for soccer clubs (threshold 88).</span>
                </li>
                <li className="flex items-start gap-3 text-sm text-muted-foreground">
                  <CheckCircle2 className="w-5 h-5 text-primary shrink-0" />
                  <span>Manual human-in-the-loop reviewer queue for near-duplicate pairs to ensure 100% accuracy.</span>
                </li>
                <li className="flex items-start gap-3 text-sm text-muted-foreground">
                  <CheckCircle2 className="w-5 h-5 text-primary shrink-0" />
                  <span>Persistent canonical IDs that survive platform migrations and rebranding.</span>
                </li>
              </ul>
            </div>

            <div className="bg-background rounded-xl border border-white/10 p-6 font-mono text-sm overflow-hidden">
              <div className="flex items-center gap-2 mb-4 text-muted-foreground">
                <div className="w-3 h-3 rounded-full bg-red-500/20" />
                <div className="w-3 h-3 rounded-full bg-yellow-500/20" />
                <div className="w-3 h-3 rounded-full bg-green-500/20" />
                <span className="ml-2 text-xs">dedup_engine.log</span>
              </div>
              <div className="space-y-2">
                <div className="text-muted-foreground">{"["}<span className="text-blue-400">INFO</span>{"]"} Evaluating pair: "Crossfire Premier" vs "Crossfire Premier SC"</div>
                <div className="text-muted-foreground">{"["}<span className="text-blue-400">INFO</span>{"]"} Match score: 94 {">"} threshold (88)</div>
                <div className="text-primary">{"["}<span className="text-primary">MERGE</span>{"]"} Assigned to Canonical ID: <span className="text-white">club_8f92a1b</span></div>
                <div className="h-px bg-white/5 my-2" />
                <div className="text-muted-foreground">{"["}<span className="text-blue-400">INFO</span>{"]"} Evaluating pair: "Slammers FC" vs "Slammers FC South"</div>
                <div className="text-muted-foreground">{"["}<span className="text-blue-400">INFO</span>{"]"} Match score: 85 {"<"} threshold (88)</div>
                <div className="text-yellow-400">{"["}<span className="text-yellow-400">QUEUE</span>{"]"} Sent to manual reviewer queue</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
