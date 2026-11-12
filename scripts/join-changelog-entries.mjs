#!/usr/bin/env node
/**
 * Rejoins the first paragraph of each entry in a CHANGELOG's newest section
 * onto one line, with its PR link at the end.
 *
 * Nx appends the link to the first line of a version plan's body. A plan
 * wrapped at 80 columns therefore gets its link in the middle of the opening
 * sentence, with the rest of the paragraph trailing after it.
 *
 * Usage:
 *   node scripts/join-changelog-entries.mjs packages/core/CHANGELOG.md ...
 */
import { readFileSync, writeFileSync } from "node:fs";

const VERSION_HEADING = /^#{1,2} \d/;
const LINKED_ENTRY = /^- (.*) (\(\[#\d+\]\([^)]*\)\))$/;
// An indented prose line. A list item, heading, quote, table row or code fence
// starts a block of its own and is left where it is.
const CONTINUATION = /^ {2}(?![-*+] |\d+\. |#|>|\||```)\S/;

export function joinNewestEntries(changelog) {
  const lines = changelog.split("\n");
  const first = lines.findIndex((line) => VERSION_HEADING.test(line));
  if (first === -1) return changelog;
  const next = lines.findIndex(
    (line, i) => i > first && VERSION_HEADING.test(line)
  );
  const end = next === -1 ? lines.length : next;

  const out = lines.slice(0, first);
  for (let i = first; i < end; i++) {
    const entry = LINKED_ENTRY.exec(lines[i]);
    if (!entry) {
      out.push(lines[i]);
      continue;
    }
    const paragraph = [entry[1]];
    while (i + 1 < end && CONTINUATION.test(lines[i + 1])) {
      paragraph.push(lines[++i].trim());
    }
    out.push(`- ${paragraph.join(" ")} ${entry[2]}`);
  }
  return [...out, ...lines.slice(end)].join("\n");
}

if (import.meta.filename === process.argv[1]) {
  const files = process.argv.slice(2);
  if (files.length === 0) {
    console.error("Usage: join-changelog-entries.mjs <CHANGELOG.md>...");
    process.exit(1);
  }
  for (const file of files) {
    const before = readFileSync(file, "utf8");
    const after = joinNewestEntries(before);
    if (after !== before) writeFileSync(file, after);
    console.log(`${after !== before ? "rejoined" : "unchanged"}  ${file}`);
  }
}
