---
name: pull-requests
description: Opening and maintaining a pull request in dianemo — the conventional-commit title format, branch naming, the body sections this repo expects and how long each should be, why a PR body must never be hard-wrapped, the version plan CI demands, and keeping the body current as the branch changes. Use when naming or creating a branch, when opening, drafting, writing, or updating a PR or its description, when asked to put a change up for review, and after pushing further commits to a branch that already has a PR open.
---

# Pull requests

## Before anything

Run the `pre-commit-checks` skill first — including the two steps a green `npm test` does
not cover: the `--typecheck` form, and a run with a real Redis. Five CI jobs run on every
PR and they are the only enforcement there is, so a red pipeline on a fresh PR means a step
was skipped, not that CI found something clever.

**If your change touches either published package outside Markdown, write the version plan
before you open the PR.** `Version Plan Check` fails the PR otherwise.

## Branch and base

PRs target `master`, branched from `origin/master` rather than whatever is checked out. A
PR stacked on another one targets that PR's branch instead, and says so in its description.

Branches are `<type>/<subject>`, lowercase and kebab-case, with the same type words as the
commit titles: `feat/multiple-rate-limits`, `fix/…`, `docs/…`, `chore/…`. The prefix names
what the PR is *for*; a `feat/` branch carrying `docs:` and `test:` commits is normal.

**Get the name right before the first push.** Renaming a remote branch closes any open PR
against it, and GitHub will not reopen it against the new head.

## Title

The title is a **conventional commit**, lowercase, matching this repo's history — `feat:`,
`fix:`, `docs:`, `refactor:`, `chore:`, with an optional scope and a `!` for a breaking
change:

```
feat!: several rate limits per client, template-declared plans, and one client shape
refactor!: normalise rate limits to one array shape and one client class
docs: correct how tests resolve core, and where the OTP comes from
```

Not Title Case, and not a mechanism description. Let the title carry the **subject** — what
a reader of the changelog needs — rather than the files touched.

**Squash-merged, the title is the commit subject on `master`** (the repo's squash setting
is `COMMIT_OR_PR_TITLE`), so it has to be one a person scanning `git log --oneline` a year
from now can act on. The squash body is the branch's commit messages, but the PR body is
not kept anywhere in git: reasoning a future maintainer needs without opening GitHub belongs
in `docs/design-notes.md` or `docs/concepts.md`, in this PR.

```bash
gh pr create --base master --title "feat: …" --body-file <path>
```

Write the body to a file first; it is long and multi-line, and `--body` mangles it. Use the
session scratchpad, not the repo.

## The version plan

```bash
npm run version-plan          # writes .nx/version-plans/<name>.md
npm run version-plan:check
```

Nx generates each package's `CHANGELOG.md` from these files, so **the plan body is the
release note** — it is read by consumers, not reviewers. Write it as prose in the repo's
voice, and put the consumer's obligation under `BREAKING:`:

```markdown
---
"@dianemo/core": major
"@dianemo/backend-redis": major
---

`rateLimit` takes a list of limits, and a request is sent only when every one of them
admits it.

BREAKING: `rateLimit` no longer accepts a bare limit object — wrap it in a list. Budget
keys move to `<client>:rateLimit:<limitName>`, so a rolling deploy would run two key
layouts at once and can double the send rate; stop the fleet and restart it.
```

Only `@dianemo/core` and `@dianemo/backend-redis` require plans, and `**/*.md` is ignored —
a docs-only PR needs none. Ask `plan:check` rather than reasoning about paths.

**Anything that changes the shared key layout is a rolling-deploy hazard and must say so**,
in the plan and in the PR body. Two replicas metering different key names do not see each
other's balance, so the fleet can send at up to twice the agreed rate for the length of a
rollout. That is a data-plane incident, not a note.

## The body

Follow `.github/pull_request_template.md`. GitHub pre-fills it, so the sections arrive with
you; **delete a heading you have nothing for** rather than writing "N/A" — an empty heading
is what makes a template rot. Delete the template's own guidance text too.

### Line breaks — never hard-wrap

**A PR body is not a `.md` file.** GitHub renders PR, issue and comment bodies with GFM
`breaks: true`, so **every newline becomes a literal `<br>`**. A body wrapped at 80 or 100
columns renders with a forced break at every one of them — ragged, and stopping short of
the viewport on every line. The same wrapping reflows normally in a repo `.md` file, and
every other file in this repo is wrapped by hand, which is why the habit survives unnoticed.

**Write each paragraph and each bullet as one long line.** Only a blank line between blocks,
and the newline before each `-` bullet, are real. Inside a paragraph or a bullet, do not
break the line at all. To check a body already posted:

```bash
gh api /repos/vivo-us/dianemo/pulls/<number> \
  -H "Accept: application/vnd.github.html+json" --jq '.body_html' | grep -c '<br>'
```

Anything but `0` means the body is hard-wrapped.

### Length — 5,000 characters

**5,000 characters is the ceiling, footer included.** 2,000–3,500 is normal here, and a
one-line fix is fine at 800. Past about 4,000 you are usually restating the diff, so treat
that as the point to start cutting. The body should be readable in a couple of minutes.

```bash
wc -m <scratchpad>/pr.md
```

**Over the limit is a signal about the branch, not the writing.** Cut in this order:

1. Detail the diff already shows — file-by-file bullets, renames, the mechanism of a change
   whose bullet already says what it does.
2. The journey — what an intermediate commit did, an approach replaced, a review round's
   findings one by one. Group what review fixed by kind, not by round.
3. Evidence that belongs in a PR comment — benchmark output, a repro transcript. Post it as
   a comment and reference it in one sentence.

**Do not cut the things that are hard to say:** the breaking changes, the rolling-deploy
hazard, the upgrade steps, the version each package goes to, and whether the Redis-gated
suites ran. If it is still over after all three cuts, the branch is two PRs; say so rather
than compressing it into an unreadable one.

### Description — one paragraph

What prompted this, what it does, and what standard it was held to, in **4–6 sentences**.
A reviewer should know what they are about to look at and how much to trust it. No file
lists, no bullets, no class names — those go below, and repeating them makes the PR twice
as long and half as useful. If the reason matters, one concrete detail beats three
sentences of context.

### Changes — headlines, one theme each

**Three to seven themes**, not one per file or per commit. A small change gets bullets; a
change with two or three large themes gets a `###` each, with a short paragraph or a few
bullets under it. Each says what changed and **why this way**, naming the rejected
alternative where there was one — the reasoning the diff cannot show:

> The property that matters is all-or-nothing. Claiming budgets in turn would leave the
> first spent whenever the second declined, and no decline path hands it back.

**Group by the change, not by where it landed**: a fix applied to both backends is one
bullet, not two. A table earns its place when a change has a measurable shape — line counts
before and after, an operation's cost; prose earns it the rest of the time.

### Version plan

The bump each published package takes and the plan file declaring it, or `None — no
published package touched outside Markdown`.

### Breaking changes

One bullet per break, each saying what a consumer must do. Repeat the rolling-deploy warning
here if there is one, and put upgrade steps beside it; a reader who skips the version plan
must not miss them.

### Verification

What you ran, and what you could not:

- The test totals, and **whether the Redis-gated suites actually ran**. "1302 tests, 37
  files, both backends against a real Redis" is the sentence that matters, because a green
  run without `REDIS_URL` leaves out about half the suite and says nothing.
- `check`, `lint`, `format`, `sort-imports:check` and `build` clean.
- **Any test assertion you changed, and why the behaviour under it is still right.**
  Changing a test to match new behaviour is normal; changing one to make it pass is not, and
  the difference is only visible if you say which you did.
- What you could not exercise. An unverified path stated plainly is useful; one presented as
  tested is the failure mode with teeth.

### Additional notes

Everything that is not a change, each with a bold lead-in:

- **Also fixed in passing** — drive-by fixes, so a reviewer knows they are not the thrust.
- **Left alone deliberately** — the related-looking thing you did not touch, and why.
- **Known limitation** — behaviour the PR ships knowingly, and where it is documented.
- **Reverted approach** — if the branch tried something and walked it back, say so: the
  intermediate commits are the only other record, and a squash folds them into a body
  nobody reads. Record a dead end someone would plausibly retry in `docs/design-notes.md`.
- **Convention changed** — a house rule this PR establishes or alters. If it belongs in
  `CLAUDE.md`, `CONTRIBUTING.md`, a skill or the template, change it in this PR.

## What reviewers here look for specifically

- **A backend change that touched only one backend.** The two must stay semantically
  identical. Say explicitly that you changed both, or why one needed no change —
  `test/conformance.test.ts` only proves it if you ran it with Redis.
- **A new or changed error `code` string.** It is public API that hosts duck-type on, so it
  is a breaking change; say so and make sure the plan is a `major`.
- **A key layout change.** As above.
- **A number with no source.** A timeout, a TTL, a band width or a retry ceiling should say
  where it came from and what happens either side of it.

## Keep the body current

**When you push further commits to a branch that already has a PR, update the body in the
same breath.** A stale body is worse than none — a reviewer who reads it and then finds a
change it does not mention stops trusting the whole thing.

**Revise the document; do not append to it.** New commits change what the end state is, so
rewrite the sentences that are now wrong and re-cut the themes that are now stale. A bullet
describing something a later commit undid is deleted, not annotated. Never add "Update:",
"Follow-up:", a dated section or a section per review round — the body must read as though
it was written in one sitting, after the last commit.

```bash
gh pr view <number> --json body -q .body > <scratchpad>/pr.md
# revise, then:
gh pr edit <number> --body-file <scratchpad>/pr.md
```

Re-check on every push:

- Does the **Description** still describe the branch's scope? If it grew, rewrite it rather
  than bolting a sentence on.
- Do the **Changes** still describe the end state? Merge a new change into the theme it
  belongs to rather than adding a parallel one.
- Are the **breaking changes** complete, the **verification** counts the ones you actually
  ran, and the **version plan** still the size of the change?
- **Does the title still fit?**
- `wc -m` again — the ceiling applies to the current version, and a body grows with every
  push. Past the ceiling or past seven themes, the branch has probably outgrown one PR.

## Footer

End the body with:

```
🤖 Generated with [Claude Code](https://claude.com/claude-code)
```
