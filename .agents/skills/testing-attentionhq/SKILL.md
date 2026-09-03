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

## Testing render/animation behaviour without mutating anything
- `cards`, `renderBoard()`, `selring`, `fetchBoard` are page globals. To simulate a column move: `const c=cards.find(x=>x.col==='issues'); c.col='needs-you'; renderBoard();`; to simulate removal: `cards=cards.filter(c=>c.key!==k); cards.forEach((c,i)=>c.id=i); renderBoard();`.
- The 5s poll (`setInterval(fetchBoard,5000)`) reverts synthetic edits almost immediately — stub it first (`window.__o=fetchBoard; fetchBoard=async()=>{}`) and restore afterwards (`fetchBoard=window.__o`). Reloading the page also resets it.
- Animations are ~140-200ms, too fast for screenshots: right after `renderBoard()` run `document.getAnimations().forEach(a=>a.playbackRate=0.05)` to slow them. Cross-column moves and removals create a `.card-ghost` clone on `body` (no `data-key`, so `.card[data-key]` still matches only real cards); the moving real card sits at inline `opacity:0` until the ghost finishes.
- Ring alignment check: `#selring` itself is 0x0; its four `<i>` edge bars carry `translate(x,y) scaleX(w)`/`scaleY(h)` transforms. Expect the top bar at `translate(left-4, top-4) scaleX(width+8)` of `document.activeElement.getBoundingClientRect()` (rounded). Open with `?fps=1` for a bottom-right fps readout. Live board changes from other sessions can also shift cards mid-test.

## Testing the send / failed-send path without messaging a real Devin session
- DevTools → Ctrl+Shift+P → "Show Network request blocking" → add a pattern and tick "Enable". Use a pattern that ends exactly at `/message` — Chrome patterns are substring matches, so `*/api/card/*/message` ALSO blocks the `/messages` thread poll (blocked counter keeps climbing, thread stops refreshing). Disable the rule as soon as the send has failed.
- Expected failed-send UI: user bubble gets a red border + "not sent" label, toast "Send failed: Failed to fetch", composer text (and attachment chips) restored, card returns to its pre-send column. Verify in the server log that no `POST /api/card/*/message` reached uvicorn.
- Inline title/body editing (`/api/card/{key}/edit`) needs `c.repo`, which is only present when the GitHub poll succeeds. GitHub can hit a *secondary* rate limit for the token's user (403 "API rate limit exceeded", while `/rate_limit` still shows 5000 remaining); the header then says "github unreachable", the Issues column is empty and cards show plain "session" refs — issue view / rename can't be tested until it clears (can take 30+ min). Snapshot the original body (`curl .../issues/N > /tmp/orig.txt`) before any save test so it can be restored exactly.
- Thread `<img>` attachments load `/api/attachment/<uuid>/<name>?t=<BOARD_TOKEN>`; any request without `?t=` shows as a 401 in the console (not a JS exception).

## Testing webhooks / SSE / sync pill (PR #76+)
- Start with `GITHUB_WEBHOOK_SECRET=whsec` to enable `POST /api/github/webhook` (no board token; HMAC only). Sign with `sig=$(printf '%s' "$body" | openssl dgst -sha256 -hmac whsec | awk '{print $2}')` and send `-H 'x-github-event: issues' -H "x-hub-signature-256: sha256=$sig"`. Repo must be in `REPOS` (default includes jackyliang/attentionhq). Unsigned → 401.
- Fake webhook issues are removed again by the next GitHub reconcile (GitHub doesn't know them). Reconciles run far more often than GITHUB_POLL_SECS whenever a recent Devin "filing issue" session without a matched issue exists (`request_refresh()` in assemble_board) — so screenshot within a few seconds. Use a current `created_at` so the card sorts to the top of Issues (column sorts newest first).
- SSE stream: `curl -sN "localhost:8420/api/events?t=testtoken123"` shows `hello`/`board`/`sync` events. Killing the server turns the pill red ("sync failed") and popover "Live updates: reconnecting… (polling)"; restarting reconnects without reload.
- The sync pill (`#hdr-sync`) popover only shows on hover of the text itself (x≈895,y≈104 in a 1024-wide maximized window); hover the dot/left area misses it.
- `?` via `shift+slash` may not open the overlay when focus is off; click the "? shortcuts" header button instead.
- uvicorn access log has no timestamps; wrap `tail -f` with `while read l; do echo "$(date +%T) $l"; done` to measure poll cadence.

## Cleanup after tests
- Close test issues: `curl -X PATCH -H "Authorization: Bearer $ATTENTIONHQ_GITHUB_TOKEN" https://api.github.com/repos/jackyliang/answer-hq/issues/<n> -d '{"state":"closed"}'`
- Archive (Backspace on non-session cards) persists to `dismissed.json` in the repo dir — delete it and restart the server to un-archive.

## Devin Secrets Needed
- DEVIN_API_KEY, ATTENTIONHQ_GITHUB_TOKEN, ATTENTIONHQ_OPENROUTER_API_KEY
