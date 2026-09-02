# v1: Make AttentionHQ Functional (Devin API + GitHub API)

## Context

AttentionHQ is currently a static, fictional mockup (`index.html`) of a Linear-style Kanban board
for managing concurrent Devin sessions and GitHub issues. This plan turns it into a working app:
real GitHub issues, real Devin sessions, real chat, real actions — while keeping the landed UI
exactly as designed.

Decisions already made with the user:
- **No OAuth.** Plain API keys in `.env` (`DEVIN_API_KEY`, `GITHUB_TOKEN`). Skip all auth flows.
- **FastAPI** backend (user preference), deployed on **Render** as a single web service that serves
  both the static frontend and the JSON API. No CORS, no second deploy.
- **Repos:** `jackyliang/answer-hq` and `jackyliang/answerhq-web`.
- **Chat shows only the concise Devin↔user messages** — the Devin v1 API's session `messages[]`
  array contains exactly this (user_message / devin_message), with no planning, thought traces, or
  tool calls. This is the "Slack-style" output the user wants. No filtering work needed.
- **Issue↔session linking:** tags. Board-created sessions get a tag `issue:<repo>#<number>`.
  Sessions without an issue tag still appear on the board as session-only cards.
- **v1 mutations: all four** — start session, chat with session, create issue, merge PR.
- **No tests** (user opted out). **No update-docs / marketing surfaces** (user opted out).
- **CHANGELOGS.md in repo root** (user-specified filename).
- Board is publicly reachable on Render → gate with a shared token (`BOARD_TOKEN` in `.env`,
  entered once in the UI, stored in localStorage, sent as a header).

## Design / Architecture

### Stack
- `server.py` — single FastAPI app. Serves `static/index.html` + `/api/*` JSON endpoints.
- `static/index.html` — the existing UI, with the hardcoded fictional `cards` array replaced by a
  fetch-and-render loop against `/api/board`. Keep all existing interaction code (WASD, selection
  ring, panel, hints) — only the data source and action handlers change.
- `pyproject.toml` — deps: `fastapi`, `uvicorn`, `httpx`.
- No database. Server state is derived fresh from the two APIs on each poll; an in-memory cache
  serves the frontend.

### Upstream APIs
Devin v1 API (base `https://api.devin.ai/v1`, `Authorization: Bearer $DEVIN_API_KEY`):
- `GET /sessions?limit=100` → sessions with `status_enum` (working | blocked | finished | expired |
  suspend_* | resume_* | resumed), `title`, `tags`, `pull_request{url}`, `structured_output`.
- `GET /session/{id}` → detail incl. `messages[]` (`{origin/type, message, timestamp}`) — the
  concise chat transcript.
- `POST /sessions` `{prompt, tags:["issue:answer-hq#123"], title}` → create session.
- `POST /session/{id}/message` `{message}` → send chat message.

GitHub REST (`Authorization: Bearer $GITHUB_TOKEN`):
- `GET /repos/{o}/{r}/issues?state=open` (excludes PRs client-side via `pull_request` key).
- `GET /repos/{o}/{r}/pulls?state=open` + `GET /pulls/{n}` (mergeable_state) +
  `GET /commits/{sha}/check-runs` for CI, `GET /pulls/{n}/reviews` for approvals.
- `POST /repos/{o}/{r}/issues` → create issue.
- `PUT /repos/{o}/{r}/pulls/{n}/merge` → merge.

### Card model & column derivation (server-side)
One card per GitHub issue; sessions and PRs attach to it via the `issue:<repo>#<n>` tag or by the
PR body containing `#<n>` / `Fixes #<n>`. Sessions with no issue → standalone session card.

Column rules (first match wins):
1. **Needs you** — any attached session `status_enum == "blocked"`, or attached PR has failing CI,
   or PR has changes-requested review.
2. **Ready to merge** — attached PR open, CI green, approved/mergeable (`mergeable_state == "clean"`).
3. **In review** — attached PR open (CI running or awaiting review).
4. **Working** — any attached session with `status_enum == "working"` (or resume states).
5. **Issues** — open issue with no active session and no open PR.

Finished sessions with a merged PR and no open obligations → card leaves the board (issue closed).
Working card face text = last Devin message (truncated); elapsed = now − session `created_at`.
Needs-you ask = last Devin message. Suggested option buttons (1/2/3): parse trailing numbered
options out of the last Devin message when present; otherwise hide the option row.
Progress ring & card intelligence: no checklist API exists, so an **LLM extractor** fills the gap.
An OpenRouter call (`OPENROUTER_API_KEY` in `.env`, default model
`openai/gpt-5.6-terra`, configurable via `OPENROUTER_MODEL`) parses a session's concise
message transcript into structured JSON: `{todos:[{text,owner,state}], current_activity, ask,
options[], progress_pct}`. Runs only when a session has new messages since last extraction
(cache keyed by session_id + last event_id), so cost stays near zero. Powers the panel checklist,
ring fill, Working "currently doing" line, Needs-you ask text, and the 1/2/3 option buttons.
If the extractor fails or the key is unset, degrade to status-based ring + last-message text.

### Server endpoints
- `GET /api/board` → `{columns:[{id,cards:[...]}], generated_at}` (from cache; poller refreshes
  Devin every 15s, GitHub every 60s, respecting rate limits).
- `GET /api/card/{id}/messages` → concise chat transcript for the card's session.
  (Board payload already includes the extractor's todos/ask/options/progress per card.)
- `POST /api/card/{id}/message` `{text}` → forward to Devin session.
- `POST /api/issue/{repo}/{n}/start` → create Devin session with prompt = issue title+body+link,
  tag `issue:<repo>#<n>`.
- `POST /api/issues` `{repo,title}` → create GitHub issue.
- `POST /api/pr/{repo}/{n}/merge` → merge PR.
- All endpoints require header `X-Board-Token: $BOARD_TOKEN`.

### Frontend changes (minimal)
- On load: token prompt if missing → `GET /api/board` → render columns with the existing
  `cardHTML`-style renderer; re-fetch every 15s (simple polling; SSE later — see
  `plan/realtime-updates-request.md`).
- Wire existing actions: E/start → `POST .../start`; composer send → `POST message`; + card Enter →
  `POST /api/issues`; Merge/E → `POST merge`. Keep toasts, now reflecting real responses.
- Card panel: replace fictional thread with `GET /api/card/{id}/messages`.
- Preserve selection/focus across re-renders (diff by card id; only patch changed cards).

## Scenario Map

### A — Board viewer
| # | State | What happens | Plan coverage |
|---|---|---|---|
| A1 | Devin API 5xx/timeout on poll | Serve last cache, show stale badge with age | OK (M2) |
| A2 | GitHub rate-limited | Back off, serve cache | OK (M2) |
| A3 | Session blocked with no message | Card in Needs you, generic "session blocked" ask | OK |
| A4 | Session with no issue tag | Standalone session card by status | OK |
| A5 | Issue closed outside board | Card disappears next poll | OK |
| A6 | Wrong/missing board token | 401 → token prompt | OK (M4) |

### B — Action taker
| # | State | What happens | Plan coverage |
|---|---|---|---|
| B1 | Start pressed twice quickly | Server rejects if issue already has active session | OK (M3) |
| B2 | Message to finished/expired session | Devin API returns detail; surface error toast | OK |
| B3 | Merge fails (branch protection, conflict) | Surface GitHub error message in toast | OK |
| B4 | Create issue with empty title | Client blocks (existing behavior) | OK |
| B5 | Two tabs acting on same card | Both hit server; second gets fresh state next poll | OK — accepted, no locking in v1 |

## Milestones

### Milestone 1: FastAPI skeleton + static serving
**Goal:** One service that serves the current UI and a stub `/api/board`.
- [ ] Create `pyproject.toml` (fastapi, uvicorn, httpx), `server.py`, move `index.html` → `static/`
- [ ] `GET /api/board` returns the current fictional data as JSON (schema locked here)
- [ ] `X-Board-Token` check middleware + `.env.example`
- [ ] `render.yaml` for Render deploy

**Verification:** `uvicorn server:app` locally; board renders identically from fetched data.

### Milestone 2: Real reads
**Goal:** Board reflects real GitHub + Devin state.
- [ ] Devin client: list sessions, get session detail (httpx, bearer key)
- [ ] GitHub client: issues, PRs, checks, reviews for both repos
- [ ] Poller task (15s Devin / 60s GitHub) + in-memory cache + stale-serving on upstream failure
- [ ] Card assembly: tag/PR-body linking, column derivation rules above, standalone session cards
- [ ] OpenRouter extractor: transcript → {todos, current_activity, ask, options, progress_pct},
      cached per session by last event_id; graceful degrade without key
- [ ] Frontend: render from `/api/board`, poll every 15s, patch-don't-rebuild to preserve selection

**Verification:** Real issues from answer-hq/answerhq-web appear; a live Devin session shows in
Working with elapsed time; blocked session lands in Needs you with its last message as the ask.

### Milestone 3: Mutations
**Goal:** All four actions work end-to-end.
- [ ] Start session from issue (E/start hint) with tag + issue-context prompt; double-start guard
- [ ] Card chat: load real messages in panel; composer sends to session
- [ ] + card creates a real GitHub issue (repo picker: default answer-hq)
- [ ] Merge button merges the PR; errors surfaced in toast

**Verification:** Start a session from a test issue and see it move to Working; send it a message
and see the reply appear; create an issue; merge a test PR.

### Milestone 4: Deploy + polish
**Goal:** Live on Render, gated, documented.
- [ ] Deploy to Render (env vars: DEVIN_API_KEY, GITHUB_TOKEN, BOARD_TOKEN, REPOS,
      OPENROUTER_API_KEY, OPENROUTER_MODEL)
- [ ] Token prompt UI on 401; localStorage persistence
- [ ] Create `CHANGELOGS.md` in root with v1 entry; update `README.md` run/deploy instructions

**Verification:** Open the Render URL on phone/laptop, enter token once, board is live.

## Out of scope (v1)
- OAuth / multi-user auth (explicitly skipped)
- Tests (explicitly skipped)
- Real-time push (SSE/webhooks) — polling only; upstream request documented in
  `plan/realtime-updates-request.md`
- Drag-and-drop column moves (columns are derived, not manual)

## Progress Summary

| Milestone | Status | Notes |
|-----------|--------|-------|
| 1. Skeleton + static serving | Not started | |
| 2. Real reads | Not started | |
| 3. Mutations | Not started | |
| 4. Deploy + polish | Not started | |
