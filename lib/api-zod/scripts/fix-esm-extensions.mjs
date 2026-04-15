#!/usr/bin/env node
/**
 * Post-build fixer: rewrites bare relative imports ("./foo") in compiled
 * dist/**\/*.{js,d.ts} to explicit ESM form ("./foo.js" / "./foo.d.ts"-style
 * resolved by the bundler). Orval-generated barrel files use bare imports,
 * which TypeScript happily compiles but Node ESM cannot resolve at runtime.
 */
import { readdir, readFile, writeFile, stat } from "node:fs/promises";
import { join } from "node:path";

const ROOT = new URL("../dist/", import.meta.url).pathname;

/** @param {string} dir */
async function walk(dir) {
  /** @type {string[]} */
  const out = [];
  for (const entry of await readdir(dir)) {
    const p = join(dir, entry);
    const s = await stat(p);
    if (s.isDirectory()) out.push(...(await walk(p)));
    else out.push(p);
  }
  return out;
}

/**
 * Rewrites: `from "./x"` / `from "./x/y"` -> `from "./x.js"` / `from "./x/y/index.js"`
 * when the target exists. Also handles dynamic `import("./x")`.
 */
async function rewrite(file, allFiles) {
  if (!/\.(js|d\.ts)$/.test(file)) return;
  const src = await readFile(file, "utf8");
  const dirname = file.replace(/\/[^/]+$/, "");

  const isDts = file.endsWith(".d.ts");
  const targetExt = isDts ? ".d.ts" : ".js";

  const rewritten = src.replace(
    /((?:from|import)\s*\(?\s*["'])(\.{1,2}\/[^"']+?)(["'])/g,
    (match, pre, spec, post) => {
      if (/\.(js|d\.ts|json|mjs|cjs)$/.test(spec)) return match;
      const abs = join(dirname, spec);
      // If "abs + targetExt" exists, it's a file import. Otherwise assume dir.
      const asFile = abs + targetExt;
      const asIndex = join(abs, "index" + targetExt);
      if (allFiles.includes(asFile)) return `${pre}${spec}${targetExt === ".d.ts" ? "" : ".js"}${post}`;
      if (allFiles.includes(asIndex)) return `${pre}${spec}/index${targetExt === ".d.ts" ? "" : ".js"}${post}`;
      return match;
    },
  );
  if (rewritten !== src) await writeFile(file, rewritten);
}

const files = await walk(ROOT);
for (const f of files) await rewrite(f, files);
console.log(`[fix-esm-extensions] rewrote imports in ${files.length} file(s) under dist/`);
