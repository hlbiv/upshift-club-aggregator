import { motion } from "framer-motion";
import { Database, GitMerge, FileJson } from "lucide-react";
import pipelineImg from "@/assets/pipeline.png";

export default function Pipeline() {
  return (
    <section className="py-24 relative">
      <div className="container mx-auto px-6">
        <div className="grid lg:grid-cols-2 gap-16 items-center">
          
          <motion.div
            initial={{ opacity: 0, x: -20 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
          >
            <div className="mb-6 inline-flex items-center gap-2 px-3 py-1 rounded-full bg-blue-500/10 border border-blue-500/20 text-blue-400 text-xs font-mono">
              <Database className="w-3 h-3" />
              Continuous Ingestion
            </div>
            
            <h2 className="text-3xl md:text-4xl font-bold tracking-tight mb-6">
              Messy sources in. <br />
              <span className="gradient-text">Clean data out.</span>
            </h2>
            
            <p className="text-muted-foreground text-lg mb-8">
              We extract data from dozens of underlying platforms, normalize disparate schemas, and reconcile fragmented entities into a single unified graph.
            </p>
            
            <div className="space-y-6">
              <div className="flex gap-4">
                <div className="w-10 h-10 rounded-lg bg-card border border-white/10 flex items-center justify-center shrink-0">
                  <FileJson className="w-5 h-5 text-primary" />
                </div>
                <div>
                  <h4 className="font-semibold mb-1">Source Platforms</h4>
                  <p className="text-sm text-muted-foreground">SportsEngine, LeagueApps, GotSport, SincSports, SoccerWire, WordPress, AthleteOne, plus dozens of bespoke per-site extractors.</p>
                </div>
              </div>
              <div className="flex gap-4">
                <div className="w-10 h-10 rounded-lg bg-card border border-white/10 flex items-center justify-center shrink-0">
                  <GitMerge className="w-5 h-5 text-primary" />
                </div>
                <div>
                  <h4 className="font-semibold mb-1">Normalization</h4>
                  <p className="text-sm text-muted-foreground">Automatic parsing of unstructured text, standardized age groups (U13-U19), and unified entity schemas across all sources.</p>
                </div>
              </div>
            </div>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, scale: 0.95 }}
            whileInView={{ opacity: 1, scale: 1 }}
            viewport={{ once: true }}
            className="relative"
          >
            <div className="absolute -inset-4 bg-gradient-to-r from-blue-500/20 to-primary/20 rounded-3xl blur-3xl opacity-50" />
            <div className="glass-panel p-2 rounded-2xl relative">
              <img 
                src={pipelineImg} 
                alt="Data pipeline visualization" 
                className="w-full rounded-xl opacity-90 mix-blend-screen"
              />
            </div>
          </motion.div>

        </div>
      </div>
    </section>
  );
}
