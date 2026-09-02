# Agent Session Kanban — discovery brief (v2, stripped)

## The problem

You run ~10 Devin sessions on ~10 GitHub issues. Devin's session list tells you what Devin
is doing. GitHub tells you what state the code is in. Neither tells you the one thing you
need: **which of the 10 is waiting on you, and what exactly you do next for it.** So
sessions sit blocked for hours and finished PRs sit unmerged.

## What the board must answer

1. What's waiting on me right now?
2. What's the next step for this one thing?
3. Is everything else still moving without me?
4. What haven't I started yet?
5. What's left on this one thing after that step?

## Frictions in the current way of working (observed, not hypothetical)

All of these come from the same root cause: **the state of an issue lives in a chat thread,
and a chat thread only makes sense to someone who still remembers it.** Threads are
append-only and written for the moment they were written in; nothing in them is a
standing answer to "where is this and what's mine to do."

**F1 — A day away erases the context, and nothing reconstructs it.** Coming back to an
issue after a day, the thread is intact but the mental model is gone. Re-reading a long
thread is the only way to recover it, and it costs more than the step it's protecting.
The cost scales with the number of issues in flight, which is exactly when it's least
affordable.

**F2 — The agent answering your question leaves no next step behind.** You ask something,
the agent answers well, and the thread ends there. The answer resolved the agent's
blocker but did not state yours, so the item silently becomes yours without saying so.
Nothing in the transcript is addressed to future-you.

**F3 — You have to ask for your own next step, repeatedly.** The recurring questions are
always the same shape: *what do you need from me next?*, *what's the next specific step on
your plan?*, and the smaller one, *what else is on your plan for this issue?* Each is a
round trip that costs a message, agent time, and a wait, and it produces an answer that
is again only valid until you forget it.

**F4 — Fast-moving items lose their thread of causation.** Example: a PR merges while
review fixes for it are still in flight, so the fixes land in a follow-up PR, whose own
review finds a further problem needing a third PR. Each hand-off is announced in the
thread as it happens; the current position — which PR is live, which is waiting, what is
outstanding — exists nowhere as a single statement, only as something to be reassembled
by reading in order.

**F5 — What is outstanding is stated once, in passing, and then buried.** Known caveats
("one window left, not worth closing unless you see it happen"), things deliberately
deferred, and things awaiting your judgement are all announced mid-thread and then
scroll away. There is no place where the open items for an issue sit and stay visible.

**F6 — Merge is not the end of the work.** After a PR merges there are often follow-up
tasks: the agent runs follow-up tests, something needs verifying by you in production,
or the merge itself spawns a new PR. A board whose rightmost state is "ready to merge"
declares the item finished at exactly the moment these obligations appear, so post-merge
work has no home and silently falls back into the transcript problem (F1, F5).

What this implies for the board, beyond what's already above:

- The next-step line has to be **durable and re-derived**, not a message. It must read
  correctly to someone with no memory of the thread, and it must survive the agent going
  idle after answering something (F1, F2).
- **The agent's remaining plan for the issue belongs on the card**, not just its current
  step — the answer to "what else is on your plan" should be readable without asking (F3, F5).
- **The card is the issue's position**, so a chain of PRs collapses into one card that
  states where the work actually is, rather than a thread that must be replayed (F4).
- Outstanding items — deferred, caveated, or awaiting your call — need somewhere to live
  on the card that isn't the transcript (F5).
- **Merging must not remove the card while obligations remain.** An issue leaves the
  board only when its checklist is empty — merged-with-follow-ups stays visible (it may
  re-enter Working or Needs you), and only merged-and-done disappears (F6).

## Three design decisions

**1. You never move a card.** The column is computed from Devin + GitHub state every time
the board refreshes. If you have to drag cards, the board goes stale in two days and then
you stop trusting the "needs you" column — at which point it's worse than nothing.

**2. A card is a GitHub issue, not a Devin session.** One issue often has several sessions
(first attempt, re-run, a session to fix review comments) plus one PR. You think in issues
("what's next for the sidebar bug"), so the card is an issue and sessions/PRs hang off it.
This is also what makes column 1 the same kind of object as the other four.

**3. "Needs you" is a list of rules, not a status field.** Devin's `blocked` status catches
maybe a third of the cases. The rest: CI is red, review comments unanswered, Devin finished
and asked you to test something, the session stopped without opening a PR, PR is approved
but you never merged it. Each rule already knows what the next step is — so the "next step"
line is a **template picked by the rule, with one sentence of detail written by an LLM**. It
can't invent a step, it's cheap, and if the LLM call fails the template alone still reads
fine.

## Columns

A card can match several rules, so the first match top-down wins.

| # | Column | A card lands here when |
|---|---|---|
| 3 | **Needs you now** | Devin is blocked or asked you a question · Devin answered your question and stopped · CI is red · review comments unanswered · Devin asked you to verify something · session stopped with no PR |
| 5 | **Ready to merge** | PR open, checks green, approved, no unresolved comments |
| 4 | **In review** | PR open, checks running or review pending — not on you |
| 2 | **Working** | a session is running and not blocked |
| 1 | **Issues** | open issue with no session and no PR |

Cards vanish when the PR merges or the issue closes. No done column.

## Card

```
┌──────────────────────────────────────────────┐
│ Sidebar collapses on mobile            #341  │
│                                              │
│ Now   CI running on PR #354                  │
│ Next  Test the preview URL and confirm the   │
│       sidebar collapses at 375px             │
└──────────────────────────────────────────────┘
```

Three lines, nothing else: short title, what's happening now, what you do next. "Next"
always starts with a verb pointed at you — *Answer Devin's question about… / Test… /
Review the 2 comments on… / Merge, it's green and approved / Decide: re-run or drop, the
session stopped without a PR / Start a session for this issue.* Clicking the card opens the
PR (or the issue, if there's no PR yet).

## Keyboard-first (Superhuman-style)

Mouse is the fallback, not the path. Nothing in the layout may depend on hover, and every
action on a card must have a key.

| Key | Action |
|---|---|
| `j` / `k` | next / previous card in column |
| `h` / `l` | previous / next column |
| `1`–`5` | jump to a column |
| `Enter` | open the card's chat in the panel |
| `Esc` | back to board chat / close panel |
| `r` | reply to Devin in the panel |
| `s` | start a session (Issues column) |
| `e` | merge (Ready to merge) |
| `o` | open the PR (or issue) in a new tab |
| `p` | open the preview URL |
| `u` | re-run the session |
| `g` `g` | first card, `G` last card |
| `c` | board chat composer (create an issue) |
| `?` | shortcut sheet |

Two rules that keep it honest: the whole board is one focus ring (never trapped in a
column), and `Esc` always goes up one level, never sideways.

## The one real engineering risk

Linking issue ↔ session ↔ PR. Two links already exist and are reliable: Devin puts the
session URL in the PR body (session → PR), and the PR says `Fixes #341` (PR → issue). The
missing link is session → issue for sessions you started from Slack with no issue mentioned.
Cleanest fix: the board is also where you start sessions, so it records the mapping itself.

## How it runs

Poll the Devin API and GitHub every 30–60s (Devin has no webhooks), compute the columns, and
store almost nothing — just the session↔issue mapping and the cached "Next" sentence.

Note: the `DEVIN_API_KEY` in this session returns 403 on `/v1/sessions`, so the Devin side
of the feed is unverified. Needs a key with session read access before building.

## What I need from you

Your other tensions — the problems that made you think about this for days. Then I'll fold
them in and write the build plan.
