// AttentionHQ ACP bridge: keeps one ACP WebSocket to Devin, attaches to the
// sessions the board is showing and forwards their live events (status,
// activity, streamed messages, PRs) to the FastAPI server, which folds them
// into the board and pushes them to browsers over SSE.
//
// Spawned by server.py (see acp_bridge_loop); can also run standalone:
//   DEVIN_ACP_API_KEY=cog_… BOARD_TOKEN=… ATTENTION_URL=http://127.0.0.1:8420 node acp/bridge.mjs
import { CloudDevin, ConnectionClosedError, eventId, timestamp } from "@cognition-ai/sdk";

const API_KEY = process.env.DEVIN_ACP_API_KEY || process.env.DEVIN_API_KEY || "";
const ORG_ID = process.env.DEVIN_ORG_ID || undefined;
const SERVER = (process.env.ATTENTION_URL || `http://127.0.0.1:${process.env.PORT || "8420"}`).replace(/\/$/, "");
const TOKEN = process.env.BOARD_TOKEN || "";
const LIST_SECS = num(process.env.ACP_LIST_SECS, 5);
const HELLO_SECS = num(process.env.ACP_HELLO_SECS, 10);
const MAX_ATTACH = num(process.env.ACP_MAX_ATTACH, 40);
const LOOKBACK_DAYS = num(process.env.DEVIN_LOOKBACK_DAYS, 14);
const MAX_PAGES = num(process.env.DEVIN_MAX_PAGES, 10);
const SHOW_AUTOMATIONS = /^(1|true|yes)$/i.test(process.env.SHOW_AUTOMATION_SESSIONS || "");
const FLUSH_MS = 150;
const DEBUG = !!process.env.ACP_DEBUG;

function num(v, d) {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : d;
}
const log = (...a) => console.log(new Date().toISOString(), "[acp]", ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// REST/board ids are bare uuids; ACP wants `devin-<uuid>`.
const bare = (id) => String(id).replace(/^devin-/, "");
const acpId = (id) => (String(id).startsWith("devin-") ? id : `devin-${id}`);

async function post(path, body) {
  const r = await fetch(SERVER + path, {
    method: "POST",
    headers: { "content-type": "application/json", "x-board-token": TOKEN },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(10_000),
  });
  if (!r.ok) throw new Error(`${path} -> HTTP ${r.status}`);
  return r.json();
}

const hello = (fields) => post("/api/acp/hello", fields);

// ---------------------------------------------------------------- event batching

const pending = new Map(); // bare session id -> [event]
let flushTimer = null;
const outbox = []; // [{seq, sessions}] not yet acknowledged by the server, oldest first
let pushing = false;
let pushFailures = 0;
let lostEvents = false; // a batch was given up on; ask the server to reconcile from REST
let seq = 0; // (run, seq) identifies a batch so a retry of an already-applied one is a no-op server-side
const run = Math.random().toString(36).slice(2, 10);
const PUSH_RETRIES = 5;
const OUTBOX_MAX = 200;

function queue(id, ev) {
  if (DEBUG) log(`${id.slice(0, 8)} ${JSON.stringify(ev)}`);
  const list = pending.get(id) || [];
  const last = list[list.length - 1];
  // coalesce consecutive append-chunks of the same streamed message
  if (ev.type === "message" && last && last.type === "message" && last.message_id === ev.message_id
      && !ev.overwrite && !ev.aborted && !last.aborted) {
    last.text += ev.text;
    last.ts = ev.ts || last.ts;
    last.event_id = ev.event_id || last.event_id;
  } else {
    list.push(ev);
  }
  pending.set(id, list);
  if (!flushTimer) flushTimer = setTimeout(flush, FLUSH_MS);
}

async function flush() {
  flushTimer = null;
  if (pending.size) {
    outbox.push({ run, seq: ++seq, sessions: Object.fromEntries(pending) });
    pending.clear();
    if (outbox.length > OUTBOX_MAX) {
      outbox.splice(0, outbox.length - OUTBOX_MAX);
      lostEvents = true;
    }
  }
  if (pushing) return;
  pushing = true;
  try {
    // batches leave in order; a failed one is retried before anything newer
    while (outbox.length) {
      const batch = outbox[0];
      try {
        await post("/api/acp/events", batch);
        outbox.shift();
        pushFailures = 0;
      } catch (e) {
        log("event push failed:", e.message);
        if (++pushFailures >= PUSH_RETRIES) {
          log("dropping batch", batch.seq, "after", pushFailures, "attempts");
          outbox.shift();
          pushFailures = 0;
          lostEvents = true;
          continue;
        }
        if (!flushTimer) flushTimer = setTimeout(flush, Math.min(FLUSH_MS * 2 ** pushFailures, 5000));
        return;
      }
    }
  } finally {
    pushing = false;
  }
}

// ---------------------------------------------------------------- translation

function translate(ev) {
  const ts = timestamp(ev.update) || null;
  switch (ev.type) {
    case "status":
      return { type: "status", status: ev.status, message: ev.message ?? null, reason: ev.reason ?? null,
               user_action: ev.userActionRequired ?? null, outcome: ev.finishedOutcome ?? null, ts };
    case "lifecycle":
      return { type: "lifecycle", lifecycle: ev.lifecycle, status: ev.status ?? null, outcome: ev.finishedOutcome ?? null, ts };
    case "activity":
      return { type: "activity", activity: ev.activity, ts };
    case "typing":
      return { type: "typing", typing: ev.typing, ts };
    case "message_delta":
      if (ev.chain && ev.chain !== "main") return null;
      return { type: "message", message_id: ev.messageId || null, text: ev.text, overwrite: ev.overwrite,
               aborted: ev.aborted, ts, event_id: eventId(ev.update) || null };
    case "user_message":
      return { type: "user_message", text: ev.text, ts, event_id: eventId(ev.update) || null };
    case "pull_request":
      return { type: "pull_request", url: ev.prUrl, ts };
    default:
      return null;
  }
}

function fromMeta(meta) {
  const status = meta?.["cognition.ai/statusEnum"] || meta?.["cognition.ai/sessionStatus"];
  if (!status) return null;
  return {
    type: "status",
    status,
    message: meta["cognition.ai/statusMessage"] || null,
    reason: null,
    user_action: meta["cognition.ai/userActionRequired"] ?? null,
    outcome: meta["cognition.ai/finishedOutcome"] || null,
    ts: null,
    snapshot: true,
  };
}

// ---------------------------------------------------------------- discovery

async function listSessions(devin) {
  const cutoff = Date.now() - LOOKBACK_DAYS * 86_400_000;
  const out = [];
  let cursor;
  for (let i = 0; i < MAX_PAGES; i++) {
    const page = await devin.listSessionsPage({ cursor, hideAutomations: SHOW_AUTOMATIONS ? undefined : true });
    let oldest = Infinity;
    for (const s of page.sessions) {
      const m = s._meta || {};
      const upd = Date.parse(s.updatedAt || "") || Date.parse(m["cognition.ai/createdAt"] || "") || Infinity;
      oldest = Math.min(oldest, upd);
      out.push({
        id: bare(s.sessionId),
        title: s.title || null,
        updated_at: s.updatedAt || null,
        created_at: m["cognition.ai/createdAt"] || null,
        status: m["cognition.ai/statusEnum"] || m["cognition.ai/sessionStatus"] || null,
        outcome: m["cognition.ai/finishedOutcome"] || null,
        archived: !!m["cognition.ai/isArchived"],
      });
    }
    cursor = page.nextCursor;
    if (!cursor || !page.sessions.length || oldest < cutoff) break;
  }
  return out;
}

// ---------------------------------------------------------------- attachments

class Bridge {
  constructor(devin) {
    this.devin = devin;
    this.attached = new Map(); // bare id -> AbortController
    this.closed = new Promise((resolve) => (this._close = resolve));
  }

  markClosed(err) {
    this._close(err);
  }

  reconcile(watch) {
    const want = new Set(watch.slice(0, MAX_ATTACH).map(bare));
    for (const [id, abort] of this.attached) {
      if (!want.has(id)) {
        abort.abort();
        this.attached.delete(id);
        try { this.devin.releaseSession(acpId(id)); } catch {}
        log("detached", id);
      }
    }
    for (const id of want) {
      if (!this.attached.has(id)) {
        const abort = new AbortController();
        this.attached.set(id, abort);
        this.watch(id, abort).catch((e) => log("watch crashed", id, e.message));
      }
    }
  }

  async watch(id, abort) {
    const mine = () => this.attached.get(id) === abort;
    let session;
    try {
      session = await this.devin.attach(acpId(id));
    } catch (e) {
      if (mine()) this.attached.delete(id);
      if (e instanceof ConnectionClosedError) return this.markClosed(e);
      log("attach failed", id, e.message);
      return;
    }
    if (abort.signal.aborted || !mine()) {
      // detached (or replaced) while attaching: the SDK now holds a handle nobody streams from
      if (!this.attached.has(id)) try { this.devin.releaseSession(acpId(id)); } catch {}
      return;
    }
    log("attached", id, session.meta?.["cognition.ai/statusEnum"] || "");
    const snap = fromMeta(session.meta);
    if (snap) queue(id, snap);
    try {
      for await (const ev of session.events({ signal: abort.signal })) {
        const out = translate(ev);
        if (out) queue(id, out);
      }
    } catch (e) {
      if (e instanceof ConnectionClosedError) return this.markClosed(e);
      if (!abort.signal.aborted) log("stream ended", id, e.message);
    } finally {
      if (mine()) this.attached.delete(id);
    }
  }

  async run() {
    const beat = setInterval(() => {
      const lost = lostEvents;
      hello({ connected: true, attached: this.attached.size, lost })
        .then(() => { helloFailures = 0; if (lost) lostEvents = false; })
        .catch(() => { if (++helloFailures >= ORPHAN_LIMIT) orphaned("server unreachable"); });
    }, HELLO_SECS * 1000);
    try {
      while (true) {
        const t0 = Date.now();
        let listed = null;
        try {
          listed = await listSessions(this.devin);
        } catch (e) {
          if (e instanceof ConnectionClosedError) throw e;
          log("session list failed:", e.message);
        }
        if (listed) {
          try {
            const res = await post("/api/acp/sessions", { sessions: listed });
            this.reconcile(Array.isArray(res.watch) ? res.watch : []);
          } catch (e) {
            log("session push failed:", e.message);
          }
        }
        const closedErr = await Promise.race([
          this.closed,
          sleep(Math.max(500, LIST_SECS * 1000 - (Date.now() - t0))).then(() => null),
        ]);
        if (closedErr) throw closedErr;
      }
    } finally {
      clearInterval(beat);
      for (const abort of this.attached.values()) abort.abort();
      this.attached.clear();
    }
  }
}

// ---------------------------------------------------------------- main

// The server that spawned us owns our lifetime; if it dies without SIGTERM
// (SIGKILL, OOM) we are reparented to init and must not keep running.
const ORPHAN_LIMIT = 6;
let helloFailures = 0;
function orphaned(why) {
  log(`exiting: ${why}`);
  process.exit(3);
}
const parentPid = process.ppid;
setInterval(() => { if (process.ppid !== parentPid) orphaned("parent exited"); }, 5000).unref();

async function main() {
  if (!API_KEY) {
    log("no DEVIN_ACP_API_KEY; exiting");
    process.exit(2);
  }
  let backoff = 2000;
  while (true) {
    let devin;
    try {
      devin = await CloudDevin.connect({ apiKey: API_KEY, orgId: ORG_ID });
      log("connected; ACP protocol", devin.protocolVersion);
      backoff = 2000;
      await hello({ connected: true, error: null, attached: 0 }).catch((e) => log("hello failed:", e.message));
      await new Bridge(devin).run();
    } catch (e) {
      const msg = String(e?.message || e);
      log("connection lost:", msg);
      await hello({ connected: false, error: msg }).catch(() => {});
    } finally {
      try { devin?.close(); } catch {}
    }
    await flush();
    await sleep(backoff);
    backoff = Math.min(backoff * 2, 60_000);
  }
}

process.on("SIGTERM", () => process.exit(0));
process.on("SIGINT", () => process.exit(0));
main().catch((e) => { log("fatal:", e); process.exit(1); });
