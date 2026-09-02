# Build spec — agent session board (visual MVP, rebuild from scratch)

A non-functioning visual mockup of a board for managing many concurrent Devin sessions
against GitHub issues. No backend, no real data, no working integrations. Fictional but
realistic content. One standalone HTML file.

## What the board is for

The user runs ~10 sessions on ~10 issues at once. Existing tools show session status and
PR status separately. Neither states which item is waiting on the user or what the user
should do next for that item. The board exists to answer four questions at a glance:

1. What is waiting on me right now?
2. What is the next step for a given item?
3. What is still moving without me?
4. What have I not started yet?

## Structure

Five columns, left to right, no completed column:

1. Issues — open issues with no session and no PR.
2. Working — a session is running and not blocked.
3. Needs you — the user is the blocker.
4. In review — PR open, checks running or review pending; not on the user.
5. Ready to merge — PR open, checks green, approved.

A card represents one GitHub issue, not one session. Column membership is derived from
Devin + GitHub state; the user never moves a card.

## Card content

- A short generated title.
- What is happening right now (one line).
- What the user should do next — present only when there is a next step. Absence of the
  line means there is nothing for the user to do; do not render placeholder text.
- One primary action where an action exists: start a session (Issues), reply, view logs,
  open preview, re-run (Needs you), merge (Ready to merge).
- No issue numbers, PR numbers, or session IDs on cards.

## States to represent across the columns

Session running; session stopped without a PR; Devin asked the user a question; CI failed;
preview ready for the user to check; checks in progress; review comments outstanding;
checks green and approved.

## Side panel

One panel, not two. Two modes:

- Board mode: a conversation for operating the board (e.g. asking it to open an issue,
  asking what is waiting on the user).
- Card mode: the conversation for the selected card, entered by selecting a card, exited
  back to board mode.

## Interaction to demonstrate

Selecting a card switches the panel to that card's conversation and back. Full keyboard
operation is a product requirement: move between cards and columns, open and close the
selected card, focus the composer, trigger a card's action, and show a shortcut reference.
Nothing may depend on hover.

## Visual constraints

- Vercel's Geist design system: the `--ds-*` dark scales, Geist Sans, Geist Mono for
  numbers and timestamps, and the Geist type scale. Reference: `vercel.com/geist/colors.md`,
  `vercel.com/geist/typography.md`, `vercel.com/geist/materials.md`.
- Sharp edges — square corners, not rounded.
- Dark, dense, monochrome first. Color carries meaning only: user action required,
  machine working, failure, ready. Everything else is grey.
- Icons for anything repeated; text kept to a minimum.

## Deliverable

A single self-contained HTML file plus screenshots of both panel modes. Written from
scratch — no reuse of any previous implementation.

## Example content to draw from (real issue titles, invented states)

Drive folder picker lists folders but no files · Split monitors into a standalone Alerts
tab · MCP has no knowledge-base search tool · Test signups leak into the Resend audience ·
Fold the widget key into the ahq_ key system · First-party widget analytics, then drop
PostHog · Slack bot Autopilot transport and monitors · MCP OAuth 2.1 and dynamic client
registration · Slack article approval card cannot be reviewed before approving · German
topic title on an English question · Mirror Autopilot replies into the Slack thread ·
Schedule Drive sync on the website cron · Make the judge eval a re-runnable check · Name
the monitor in the delete approval card · Extract the DashboardPageLayout component
