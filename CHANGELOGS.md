# Changelog

## v1.0.0 — Functional board

- FastAPI server (`server.py`) serving the board UI and JSON API from one service.
- Live reads: GitHub issues/PRs (with CI + review state) and Devin sessions via the Devin v3 org API.
- Column derivation server-side: Issues / Working / Needs you / In review / Ready to merge.
- Concise per-session chat from the Devin session messages API (no internal traces).
- OpenRouter extractor (`openai/gpt-5.6-luna:nitro` by default) parses each transcript into todos, current activity, ask, options, and progress; cached until the transcript grows; degrades gracefully without a key.
- Mutations: start a Devin session from an issue, chat with a session, create a GitHub issue, merge a PR.
- Shared-token gate (`BOARD_TOKEN`) on all `/api` routes; UI prompts once and stores it locally.
- Sessions without a linked issue appear as standalone cards.
- Render deployment via `render.yaml`.

## v0.x — Static mockup

- Linear-style dark UI: fixed-size cards, single animated selection ring, WASD/arrow navigation, `E` start / `Enter` open / `1-3` option replies / `Esc` close, overlay chat panel, rotating progress ring on Working cards.
