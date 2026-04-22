import { motion } from "framer-motion";
import { Check, ShieldCheck } from "lucide-react";

export default function Coverage() {
  return (
    <section id="coverage" className="py-24 relative overflow-hidden">
      <div className="container mx-auto px-6 relative z-10">
        <div className="text-center max-w-3xl mx-auto mb-16">
          <h2 className="text-3xl md:text-4xl font-bold tracking-tight mb-4">Complete Ecosystem Coverage</h2>
          <p className="text-muted-foreground text-lg">
            From the elite tiers down to every state association. We index the entire pyramid so you don't have to guess where a player came from.
          </p>
        </div>

        <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-6 mb-16">
          <CoverageMetric title="Tier 1" value="7" desc="National Elite Leagues" />
          <CoverageMetric title="Tier 2" value="13" desc="High-Performance Leagues" />
          <CoverageMetric title="Tier 3" value="41" desc="Regional Leagues" />
          <CoverageMetric title="Tier 4" value="54" desc="State Associations" />
        </div>

        <div className="glass-panel p-8 rounded-2xl">
          <div className="grid md:grid-cols-2 gap-12">
            <div>
              <h3 className="text-xl font-semibold mb-6 flex items-center gap-2">
                <ShieldCheck className="w-5 h-5 text-primary" />
                Supported Leagues & Ecosystems
              </h3>
              <ul className="space-y-4">
                {[
                  "MLS NEXT",
                  "ECNL & ECNL Regional League",
                  "Girls Academy (GA)",
                  "National Premier Leagues (NPL)",
                  "USYS National League (Elite 64, Conferences)",
                  "All 54 USYS State Associations",
                  "US Club Soccer Sanctioned Leagues"
                ].map((item, i) => (
                  <motion.li 
                    initial={{ opacity: 0, x: -10 }}
                    whileInView={{ opacity: 1, x: 0 }}
                    viewport={{ once: true }}
                    transition={{ delay: i * 0.1 }}
                    key={item} 
                    className="flex items-start gap-3 text-muted-foreground"
                  >
                    <Check className="w-5 h-5 text-primary shrink-0 mt-0.5" />
                    <span>{item}</span>
                  </motion.li>
                ))}
              </ul>
            </div>
            <div className="relative">
              <div className="absolute inset-0 bg-gradient-to-tr from-primary/10 to-transparent rounded-xl border border-white/5 flex items-center justify-center">
                <div className="text-center p-6">
                  <div className="text-5xl font-mono font-bold text-primary mb-2">127</div>
                  <div className="text-lg font-medium text-foreground">League Directories Crawled</div>
                  <div className="text-sm text-muted-foreground mt-2">(115 definitively scrapeable)</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function CoverageMetric({ title, value, desc }: { title: string, value: string, desc: string }) {
  return (
    <motion.div 
      initial={{ opacity: 0, y: 20 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true }}
      className="p-6 rounded-xl border border-white/5 bg-card/30"
    >
      <div className="text-sm font-mono text-primary mb-2">{title}</div>
      <div className="text-4xl font-bold mb-2">{value}</div>
      <div className="text-sm text-muted-foreground leading-snug">{desc}</div>
    </motion.div>
  );
}
