import { Link } from "wouter";
import { Button } from "@/components/ui/button";

export default function Nav() {
  return (
    <nav className="sticky top-0 z-50 w-full border-b border-white/5 bg-background/60 backdrop-blur-md">
      <div className="container mx-auto px-6 h-16 flex items-center justify-between">
        <div className="flex items-center gap-8">
          <Link href="/" className="flex items-center gap-2">
            <div className="w-6 h-6 bg-primary rounded-sm shadow-[0_0_10px_rgba(var(--primary),0.5)]" />
            <span className="font-bold text-lg tracking-tight">Upshift Data</span>
          </Link>
          
          <div className="hidden md:flex items-center gap-6 text-sm text-muted-foreground font-medium">
            <a href="#coverage" className="hover:text-primary transition-colors">Coverage</a>
            <a href="#schema" className="hover:text-primary transition-colors">Schema</a>
            <a href="#api" className="hover:text-primary transition-colors">API Docs</a>
          </div>
        </div>

        <div className="flex items-center gap-4">
          <a href="/dashboard" className="text-sm font-medium text-muted-foreground hover:text-primary transition-colors">
            Log in
          </a>
          <a href="/dashboard">
            <Button size="sm" className="bg-primary text-primary-foreground hover:bg-primary/90 font-medium">
              Dashboard
            </Button>
          </a>
        </div>
      </div>
    </nav>
  );
}
