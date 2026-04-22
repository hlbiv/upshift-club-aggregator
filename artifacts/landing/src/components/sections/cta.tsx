import { Button } from "@/components/ui/button";
import { ArrowRight } from "lucide-react";

export default function Cta() {
  return (
    <section className="py-24 relative border-t border-white/5">
      <div className="absolute inset-0 bg-primary/5" />
      <div className="container mx-auto px-6 relative z-10">
        <div className="max-w-2xl mx-auto text-center">
          <h2 className="text-3xl md:text-5xl font-bold tracking-tight mb-6">
            Ready to build on reliable infrastructure?
          </h2>
          <p className="text-lg text-muted-foreground mb-10">
            Stop spending engineering hours building bespoke scrapers. Get instant access to the most comprehensive dataset in US youth soccer.
          </p>
          <a href="/dashboard">
            <Button size="lg" className="h-14 px-10 text-lg bg-primary text-primary-foreground hover:bg-primary/90 font-medium rounded-full shadow-[0_0_40px_rgba(var(--primary),0.3)] hover:shadow-[0_0_60px_rgba(var(--primary),0.5)] transition-all gap-2">
              Go to Dashboard
              <ArrowRight className="w-5 h-5" />
            </Button>
          </a>
        </div>
      </div>
    </section>
  );
}
