# What I'm building

I run around 10 Devin sessions on 10 GitHub issues at once. The session list tells me what Devin is doing; GitHub tells me what state the code is in. Neither tells me the thing I actually need: **which of the 10 is waiting on me, and what exactly I do next for it.** So sessions sit blocked for hours and finished PRs sit unmerged.

So I'm building a small board on top of your API. One card per GitHub issue (not per session — one issue is usually an original session, a re-run, some review-fix sessions and a PR or three). Five columns, and I never drag anything between them; membership is computed every cycle from Devin session state joined with GitHub state:

1. **Issues** — open, no session, no PR
2. **Working** — session running, not blocked
3. **Needs you** — Devin is blocked or asked me something · Devin answered me and stopped · CI red · review comments unanswered · session ended with no PR
4. **In review** — PR open, checks running or review pending
5. **Ready to merge** — PR open, green, approved, no unresolved comments

Each card says three things: what it is, what it's doing right now, and — only when it's true — the one next thing I have to do. It's for me and my org, not a product.

The part that matters for this ask: **column 3 is the whole point**, and it's only worth anything if it's true *now*. A board that tells me I was needed four minutes ago is a board I stop trusting.

# What I'd like to request so I don't need to poll

Right now everything session-side is pull-only — `GET /v1/sessions/{session_id}` and `GET /v2/enterprise/sessions` — so to keep the board honest I poll every session on a timer. Three problems with that:

1. **Freshness costs requests.** At a 30–60s interval "needs you now" isn't now. At 5s I'm making N requests per interval to learn that nothing changed, which is the answer ~95% of the time.
2. **Polling gives me state, not transitions.** I can see `status_enum: blocked`. I can't see *when* it became blocked, or that it went `working → blocked → working` between two polls. The case I most need to catch — Devin answered my question and then stopped, so the item silently became mine — is exactly a transition, so it's precisely what polling drops.
3. **Messages need a second fetch and a diff.** To know Devin asked me something I pull the full `messages` array and diff it against what I saw last time, per session, forever.

Either of the following unblocks me. (A) is the smaller ask.

## A. Outbound webhooks on session events

Org-level config (URL + signing secret), POSTing on:

| Event | What it does for the board |
|---|---|
| `session.status_changed` | drives column membership — please include `from` and `to`, the transition is the signal |
| `session.blocked` | the single most important one: Devin is waiting on me |
| `session.message` (origin = devin) | Devin asked something, or answered and stopped |
| `session.finished` / `session.exit` | session ended — with a PR or without one |
| `session.pull_request_opened` / `_updated` | moves the card into review |

Payload wants at least `session_id`, `org_id`, `event_type`, `occurred_at`, `status_enum` before/after, `title`, `tags`, `pull_request`. Signed (HMAC over the raw body, timestamped against replay), retried with backoff, at-least-once with a delivery id so I can dedupe.

Worth flagging that this is the opposite direction from the webhook triggers in Automations today — those are inbound *into* Devin; what I'm requesting is outbound.

## B. A watch stream for sessions

`GET /v2/sessions?watch=true&cursor=<cursor>` with the same semantics you already ship for Outposts (`GET /opbeta/outposts/devins?watch=true`): SSE, `MODIFIED` / `DELETED`, a top-level cursor I persist and reconnect with, five-minute stream ceiling, at-least-once delivery. Filterable by `org_id` / `user_id` / `tags`. You've already solved this shape once — I'd just like it pointed at sessions.

Ideally both: webhooks for a hosted board, watch for anything running locally that can't accept an inbound connection.

## Two smaller things that would matter just as much

1. **A structured reason on `blocked`.** `status_enum: blocked` tells me the session stopped, not what it needs. Something like `blocked_reason: awaiting_user_input | awaiting_approval | awaiting_secret | error`, plus the question text, would let me render the next action directly instead of running an LLM pass over the transcript to guess it.
2. **The session's remaining plan, exposed.** Devin keeps a todo list internally. Today the only way an external tool can know what's left on an issue is to read the whole thread. If the API returned it (ordered items, owner = user or agent, done / not done) my board could answer "what else is on this" without me asking Devin — which right now is a round trip that costs a message and a wait, and I do it several times a day.

## Interim, if none of that is near-term

- An `updated_since` / `changed_since` filter on the enterprise list endpoint, so one request returns only what moved instead of N requests that mostly return unchanged rows.
- Documented rate limits on the session endpoints, so I know what poll interval is actually sanctioned.

## Scale

One org, ~10–30 concurrent sessions, one consumer. Not a volume problem — a freshness and transition-fidelity problem.
