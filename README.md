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

## Environment

| Var | Purpose |
| --- | --- |
| `DEVIN_API_KEY` | Devin API key (v3 org API) |
| `DEVIN_ORG_ID` | Devin org id (`org-...`) |
| `GITHUB_TOKEN` | Fine-grained PAT: issues + PRs read/write, contents read/write |
| `OPENROUTER_API_KEY` | Optional — powers todos/activity/ask extraction |
| `OPENROUTER_MODEL` | Default `openai/gpt-5.6-luna:nitro` |
| `BOARD_TOKEN` | Shared token gating all `/api` routes |
| `REPOS` | Comma-separated `owner/repo` list |

## Deploy

`render.yaml` defines a single Render web service (`uvicorn server:app`). Set the env vars above in the Render dashboard.

See `plan/` for the discovery brief and build spec, `plans/` for the functional plan, `CHANGELOGS.md` for history.
