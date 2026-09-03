---
name: testing-attentionhq
description: How to run and test the AttentionHQ session board (FastAPI + static/index.html) locally
---

# Testing AttentionHQ (session-board)

## Run
- `fuser -k 8420/tcp` first — a stale server may hold the port.
- Create the venv if missing: `uv venv -p 3.12 .venv && uv pip install -p .venv/bin/python -e .` (no python3.12 on PATH).
- `cd /home/ubuntu/repos/attentionhq && DEVIN_API_KEY="$DEVIN_API_KEY" GITHUB_TOKEN="$ATTENTIONHQ_GITHUB_TOKEN" OPENROUTER_API_KEY="$ATTENTIONHQ_OPENROUTER_API_KEY" BOARD_TOKEN=testtoken123 .venv/bin/uvicorn server:app --port 8420`
- If `ATTENTIONHQ_GITHUB_TOKEN` / `ATTENTIONHQ_OPENROUTER_API_KEY` aren't set in the session (they aren't org secrets), use `GITHUB_TOKEN="$(gh auth token)"` (works against api.github.com) and `OPENROUTER_API_KEY="$OPENROUTER_API_KEY"` (org secret). Without a GitHub token the log shows `github poll failed: Illegal header value b'Bearer '` and PR cards fall back to `ci: unknown`.
- Open http://localhost:8420, enter the BOARD_TOKEN in the in-app token gate (wrong token shows an inline error; clear with `localStorage.removeItem('board_token')`). First board fetch takes ~5-20s (skeleton cards until then); server and UI poll every 5s (`DEVIN_POLL_SECS`), so wait up to ~15s after mutations for the board to reflect them.

## Auth
- API auth header is `x-board-token: <BOARD_TOKEN>` (NOT `Authorization: Bearer`). Without it /api/* returns 401.

## UI notes
- Brand is "Attention" (window title and header).
- Issue-card panels show the markdown-rendered issue description + label tags; the Todo section appears only on cards with Devin sessions. Thread cache/prefetch makes reopening a card instant.
- Responsive checks: resize with `wmctrl -r "Attention" -e 0,x,y,w,h` (display caps at 1600x1200; xrandr can't add larger modes on this VNC). Short heights (~800px) are the fragile case for the panel.
- F was removed as a shortcut; never press E on issue cards during tests — it starts a REAL Devin session.
- Test-plan files live in `plans/`.
- Keyboard: WASD/arrows move selection ring; Enter opens card chat; Esc closes; `?` (send as `shift+slash` via xdotool — `question` may not register) opens shortcuts overlay; N jumps to Needs-you; Backspace, E and H are two-step: the first press arms the card (hint turns amber, toast says "Press X again"), the second press within 4s fires, Esc or moving to another card disarms. Backspace closes the GitHub issue and/or archives the Devin session (both when an issue card has a session) and removes the card (MUTATES — don't press against production; the same button sits in the card panel header); E starts a Devin session / merges (COSTS MONEY / MUTATES); B focuses board composer.
- Clicking a card opens its chat panel directly.
- The Todo section renders at the very TOP of the card panel scroll area, above a long transcript — scroll/drag the panel scrollbar fully up to see it.
- The "+" new-issue card is the FIRST card in the Issues column (focus a card, press Home); new issues appear directly under it (newest first). Enter opens inline input; Enter submits and creates a REAL GitHub issue in jackyliang/answer-hq.

## Cleanup after tests
- Close test issues: `curl -X PATCH -H "Authorization: Bearer $ATTENTIONHQ_GITHUB_TOKEN" https://api.github.com/repos/jackyliang/answer-hq/issues/<n> -d '{"state":"closed"}'`
- Archive (Backspace on non-session cards) persists to `dismissed.json` in the repo dir — delete it and restart the server to un-archive.

## Devin Secrets Needed
- DEVIN_API_KEY, ATTENTIONHQ_GITHUB_TOKEN, ATTENTIONHQ_OPENROUTER_API_KEY
