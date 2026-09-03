# Attention

Linear-style Kanban for managing concurrent Devin sessions and GitHub issues.

## Run locally

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e .
cp .env.example .env   # fill in keys
set -a; source .env; set +a
.venv/bin/uvicorn server:app --port 8000
```

Open http://localhost:8000. If `BOARD_TOKEN` is set, the UI prompts for it once.

## Boards

Boards are managed in the UI (the `+` and settings tiles in the left rail, or `/settings`) and stored in Postgres (`DATABASE_URL`) or `boards.json`. Each board is a named set of repos at `/b/<slug>`; `/` shows everything. Sessions with no repo yet show on every board until Devin opens a PR or you pin them.

## Environment

| Var | Purpose |
| --- | --- |
| `DEVIN_API_KEY` | Devin API key (v3 org API) |
| `DEVIN_ORG_ID` | Devin org id (`org-...`) |
| `GITHUB_TOKEN` | Fine-grained PAT: issues + PRs read/write, contents read/write |
| `OPENROUTER_API_KEY` | Optional — powers todos/activity/ask extraction |
| `OPENROUTER_MODEL` | Default `openai/gpt-5.6-luna:nitro` |
| `BOARD_TOKEN` | Shared token gating all `/api` routes |
| `DATABASE_URL` | Optional — Postgres for boards + hidden cards; falls back to JSON files |
| `REPOS` | Only seeds the first board when none exist yet; boards are edited in the UI after that |
| `GITHUB_WEBHOOK_SECRET` | Secret shared with the GitHub webhooks (see below). When set, GitHub is polled every 5 min as a reconcile instead of every 20 s |
| `GITHUB_POLL_SECS` | Override the GitHub reconcile interval (default 300 with webhooks, 20 without; backs off up to `GITHUB_POLL_MAX_SECS`) |
| `GITHUB_RATE_RESERVE` | Requests kept in reserve for user actions (create/merge/edit); polling pauses below this until the quota resets. Default 200 |
| `RENDER_API_KEY` | Optional — Render API key; header shows deploy status (deploying now / last deployed) for every non-suspended Render service in the workspace |
| `RENDER_SERVICE_TYPES` | Default `web_service,static_site` — Render service types to show |

## How the board stays fresh

- **GitHub → webhooks.** `POST /api/github/webhook` (HMAC `X-Hub-Signature-256`, not the board token) folds `issues` / `pull_request` payloads straight into the board and does a targeted re-read of just the affected PR for `pull_request_review`, `check_run`, `check_suite`, `workflow_run` and `status`. Every GET uses `If-None-Match`, so unchanged listings cost `304`s that don't count against the quota, and `Retry-After` / `X-RateLimit-Reset` are honoured instead of hammering a limited token.
- **Devin → polling.** Devin has no webhook API yet, so sessions are still polled (with backoff on errors).
- **Browser → SSE.** `GET /api/events?t=<board token>` streams `board` (content changed, re-fetch `/api/board`) and `sync` (poll finished, quota/health only) events. The UI falls back to 5 s polling only while the stream is down. `R` (or `POST /api/refresh`) forces a GitHub + Devin refresh now; the header shows GitHub quota, rate-limit countdown, and webhook delivery status on hover.

### Webhook setup

For each repo in `REPOS` (or once at the org level): Settings → Webhooks → Add webhook

- Payload URL: `https://<your-render-host>/api/github/webhook`
- Content type: `application/json`
- Secret: the value of `GITHUB_WEBHOOK_SECRET`
- Events: Issues, Pull requests, Pull request reviews, Pull request review comments, Check runs, Check suites, Statuses, Workflow runs

GitHub's `ping` on save returns `{"ok": true, "pong": true}`; the board's sync popover then shows deliveries as they arrive.

## Deploy

`render.yaml` defines a single Render web service (`uvicorn server:app`). Set the env vars above in the Render dashboard.

## Tests

```bash
.venv/bin/pip install pytest
.venv/bin/python -m pytest tests -q
```

See `plan/` for the discovery brief and build spec, `plans/` for the functional plan, `CHANGELOGS.md` for history.

attachment test
