import { motion } from "framer-motion";
import { Users, Building2, Trophy, GraduationCap, Calendar, MapPin } from "lucide-react";

export default function Schema() {
  const entities = [
    { icon: Building2, label: "Clubs", sub: "Canonical, Aliases, Affiliations" },
    { icon: MapPin, label: "Leagues", sub: "Divisions, Standings, Tiers" },
    { icon: Users, label: "Coaches", sub: "Directories, Discoveries" },
    { icon: Calendar, label: "Events", sub: "Matches, Tryouts, Showcases" },
    { icon: Users, label: "Rosters", sub: "Event Teams, Season Rosters" },
    { icon: GraduationCap, label: "Colleges", sub: "NCAA, NAIA, NJCAA, Commitments" },
  ];

  return (
    <section id="schema" className="py-24 bg-card/30 border-y border-white/5 relative">
      <div className="container mx-auto px-6">
        <div className="text-center max-w-3xl mx-auto mb-16">
          <h2 className="text-3xl md:text-4xl font-bold tracking-tight mb-4">A Relational Graph</h2>
          <p className="text-muted-foreground text-lg">
            Everything is connected. We don't just dump flat CSVs — we build a typed PostgreSQL graph that maps the entire ecosystem.
          </p>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-3 gap-4 lg:gap-8 max-w-5xl mx-auto">
          {entities.map((ent, i) => (
            <motion.div
              key={ent.label}
              initial={{ opacity: 0, y: 10 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: i * 0.05 }}
              className="p-6 rounded-xl border border-white/5 bg-background hover:bg-white/[0.02] transition-colors"
            >
              <ent.icon className="w-8 h-8 text-primary mb-4" />
              <h3 className="font-semibold text-lg mb-1">{ent.label}</h3>
              <p className="text-sm text-muted-foreground">{ent.sub}</p>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}
