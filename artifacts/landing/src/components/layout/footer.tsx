export default function Footer() {
  return (
    <footer className="border-t border-white/5 bg-card/30 mt-24">
      <div className="container mx-auto px-6 py-12 flex flex-col md:flex-row justify-between items-center gap-6">
        <div className="flex items-center gap-2 opacity-50 hover:opacity-100 transition-opacity">
          <div className="w-5 h-5 bg-primary rounded-sm grayscale" />
          <span className="font-bold text-sm tracking-tight">Upshift Data</span>
        </div>
        <div className="text-sm text-muted-foreground">
          © {new Date().getFullYear()} Upshift Data. All rights reserved.
        </div>
      </div>
    </footer>
  );
}
