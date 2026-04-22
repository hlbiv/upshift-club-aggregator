import { motion } from "framer-motion";
import Nav from "@/components/layout/nav";
import Footer from "@/components/layout/footer";
import Hero from "@/components/sections/hero";
import Pipeline from "@/components/sections/pipeline";
import Coverage from "@/components/sections/coverage";
import Schema from "@/components/sections/schema";
import Dedup from "@/components/sections/dedup";
import ApiDemo from "@/components/sections/api-demo";
import Cta from "@/components/sections/cta";

export default function Home() {
  return (
    <div className="min-h-screen bg-background flex flex-col overflow-hidden selection:bg-primary/30">
      <div className="fixed inset-0 pointer-events-none z-0">
        <div className="absolute top-[-20%] left-[-10%] w-[50%] h-[50%] rounded-full bg-primary/5 blur-[120px]" />
        <div className="absolute bottom-[-20%] right-[-10%] w-[50%] h-[50%] rounded-full bg-blue-500/5 blur-[120px]" />
      </div>
      
      <Nav />
      
      <main className="flex-1 relative z-10">
        <Hero />
        <Coverage />
        <Pipeline />
        <Schema />
        <Dedup />
        <ApiDemo />
        <Cta />
      </main>

      <Footer />
    </div>
  );
}
