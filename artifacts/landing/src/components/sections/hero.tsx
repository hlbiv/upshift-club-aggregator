import { motion } from "framer-motion";
import { ArrowRight, Terminal } from "lucide-react";
import { Button } from "@/components/ui/button";
import networkImg from "@/assets/network-hero.png";

export default function Hero() {
  return (
    <section className="relative pt-24 pb-32 lg:pt-36 lg:pb-40 overflow-hidden">
      <div className="container mx-auto px-6">
        <div className="grid lg:grid-cols-2 gap-12 lg:gap-8 items-center">
          
          <motion.div 
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, ease: "easeOut" }}
            className="max-w-2xl"
          >
            <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-primary/10 border border-primary/20 text-primary text-xs font-mono mb-6">
              <div className="w-2 h-2 rounded-full bg-primary animate-pulse" />
              API v3.1 is live
            </div>
            
            <h1 className="text-5xl lg:text-7xl font-bold tracking-tight text-foreground mb-6 leading-[1.1]">
              The Canonical Graph of <br />
              <span className="gradient-text">US Youth Soccer.</span>
            </h1>
            
            <p className="text-lg text-muted-foreground mb-8 leading-relaxed max-w-xl">
              We continuously crawl 127 league directories, deduplicate the noise, and serve a typed REST API of clubs, coaches, events, rosters, and commitments. Infrastructure for serious operators.
            </p>
            
            <div className="flex flex-col sm:flex-row gap-4">
              <a href="/dashboard">
                <Button size="lg" className="h-12 px-8 bg-primary text-primary-foreground hover:bg-primary/90 text-base font-medium rounded-md w-full sm:w-auto">
                  Access Dashboard
                </Button>
              </a>
              <a href="#api">
                <Button size="lg" variant="outline" className="h-12 px-8 border-white/10 hover:bg-white/5 text-base font-medium rounded-md w-full sm:w-auto gap-2">
                  <Terminal className="w-4 h-4" />
                  View API Docs
                </Button>
              </a>
            </div>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, scale: 0.95 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ duration: 0.8, delay: 0.2, ease: "easeOut" }}
            className="relative"
          >
            <div className="absolute -inset-1 bg-gradient-to-r from-primary to-blue-600 rounded-xl blur-2xl opacity-20" />
            <div className="glass-panel rounded-xl overflow-hidden aspect-video relative">
              <img 
                src={networkImg} 
                alt="Data network visualization" 
                className="w-full h-full object-cover mix-blend-screen opacity-80"
              />
              <div className="absolute inset-0 bg-gradient-to-t from-background/80 to-transparent" />
            </div>
          </motion.div>

        </div>
      </div>
    </section>
  );
}
