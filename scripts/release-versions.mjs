#!/usr/bin/env node
/**
 * Prints `<name> <version>` for each package in nx.json's `release.projects`,
 * in that order, which puts @dianemo/core ahead of the backend that depends on
 * it. With `--changed-since <ref>`, only the packages whose version differs
 * from the one at that git ref.
 *
 * Usage:
 *   node scripts/release-versions.mjs
 *   node scripts/release-versions.mjs --changed-since origin/master
 */
import { readFileSync, readdirSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { resolve, join } from "node:path";

const ROOT = resolve(import.meta.dirname, "..");

const args = process.argv.slice(2);
const sinceIdx = args.indexOf("--changed-since");
const since = sinceIdx !== -1 ? args[sinceIdx + 1] : undefined;
if (sinceIdx !== -1 && !since) {
  console.error("--changed-since needs a git ref");
  process.exit(1);
}

const readJson = (path) => JSON.parse(readFileSync(path, "utf8"));

const manifestPaths = new Map();
for (const dir of readdirSync(join(ROOT, "packages"))) {
  const path = join("packages", dir, "package.json");
  try {
    manifestPaths.set(readJson(join(ROOT, path)).name, path);
  } catch {
    // Not a package directory.
  }
}

function versionAt(ref, path) {
  try {
    const manifest = execFileSync("git", ["show", `${ref}:${path}`], {
      cwd: ROOT,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    });
    return JSON.parse(manifest).version;
  } catch {
    // The package did not exist at that ref, so any version is a change.
    return undefined;
  }
}

for (const name of readJson(join(ROOT, "nx.json")).release.projects) {
  const path = manifestPaths.get(name);
  if (!path) {
    console.error(`nx.json releases "${name}", but no package has that name`);
    process.exit(1);
  }
  const { version } = readJson(join(ROOT, path));
  if (since && versionAt(since, path) === version) continue;
  console.log(`${name} ${version}`);
}
