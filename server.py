"""Attention server — FastAPI app serving the board UI and JSON API.

Reads GitHub issues/PRs and Devin sessions, derives Kanban columns, and
exposes mutations: start session, chat, create issue, merge PR.
"""

import asyncio
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

log = logging.getLogger("attentionhq")
logging.basicConfig(level=logging.INFO)

DEVIN_API_KEY = os.environ.get("DEVIN_API_KEY", "")
DEVIN_ORG_ID = os.environ.get("DEVIN_ORG_ID", "org-92a6e4678143473ea829471968691a89")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.environ.get("OPENROUTER_MODEL", "openai/gpt-5.6-luna:nitro")
BOARD_TOKEN = os.environ.get("BOARD_TOKEN", "")
REPOS = [r.strip() for r in os.environ.get("REPOS", "jackyliang/answer-hq,jackyliang/answerhq-web").split(",") if r.strip()]
DEVIN_POLL_SECS = int(os.environ.get("DEVIN_POLL_SECS", "15"))
GITHUB_POLL_SECS = int(os.environ.get("GITHUB_POLL_SECS", "60"))

DEVIN_BASE = f"https://api.devin.ai/v3/organizations/{DEVIN_ORG_ID}"
GH_BASE = "https://api.github.com"
OR_BASE = "https://openrouter.ai/api/v1"

ISSUE_TAG_RE = re.compile(r"issue:([\w.-]+/[\w.-]+)#(\d+)")
PR_ISSUE_RE = re.compile(r"(?:#|issues/)(\d+)")

# ---------------------------------------------------------------- state

state: dict = {
    "issues": {},      # "owner/repo#n" -> issue dict
    "prs": {},         # "owner/repo#n" -> pr dict (enriched with ci/review)
    "sessions": [],    # devin session list
    "board": None,     # assembled board payload
    "devin_ok": True,
    "github_ok": True,
    "generated_at": 0,
}
DISMISSED_FILE = os.environ.get("DISMISSED_FILE", "dismissed.json")

def load_dismissed() -> set:
    try:
        with open(DISMISSED_FILE) as f:
            return set(json.load(f))
    except (OSError, ValueError):
        return set()

def save_dismissed(d: set):
    try:
        with open(DISMISSED_FILE, "w") as f:
            json.dump(sorted(d), f)
    except OSError:
        log.warning("could not persist dismissed list")

dismissed: set = load_dismissed()
extract_cache: dict = {}  # session_id -> {"key": last_msg_key, "data": {...}}
session_msgs_cache: dict = {}  # session_id -> {"msgs": [...], "cursor": str|None, "seen": {event_id}}

# ---------------------------------------------------------------- clients

def devin_client():
    return httpx.AsyncClient(base_url=DEVIN_BASE, headers={"Authorization": f"Bearer {DEVIN_API_KEY}"}, timeout=30)

def gh_client():
    return httpx.AsyncClient(
        base_url=GH_BASE,
        headers={
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        timeout=30,
    )

# ---------------------------------------------------------------- github fetch

def issue_from_gh(repo: str, it: dict) -> dict:
    return {
        "repo": repo, "number": it["number"], "title": it["title"],
        "body": it.get("body") or "", "url": it["html_url"],
        "labels": [l["name"] for l in it.get("labels", [])],
        "created_at": it["created_at"], "updated_at": it["updated_at"],
    }

# GitHub's list endpoint lags behind creates; keep issues we just created visible
# until the listing catches up (or they get closed).
RECENT_ISSUE_TTL = 300
recent_issues: dict[str, tuple[float, dict]] = {}

async def fetch_github():
    issues, prs = {}, {}
    async with gh_client() as gh:
        for repo in REPOS:
            r = await gh.get(f"/repos/{repo}/issues", params={"state": "open", "per_page": 100})
            r.raise_for_status()
            for it in r.json():
                if "pull_request" in it:
                    continue
                issues[f"{repo}#{it['number']}"] = issue_from_gh(repo, it)
            now = time.time()
            for key, (ts, issue) in list(recent_issues.items()):
                if now - ts > RECENT_ISSUE_TTL or key in issues:
                    recent_issues.pop(key, None)
                elif issue["repo"] == repo:
                    issues[key] = issue
            r = await gh.get(f"/repos/{repo}/pulls", params={"state": "open", "per_page": 100})
            r.raise_for_status()
            for pr in r.json():
                key = f"{repo}#{pr['number']}"
                detail = {}
                try:
                    d = await gh.get(f"/repos/{repo}/pulls/{pr['number']}")
                    d.raise_for_status()
                    detail = d.json()
                except httpx.HTTPError:
                    pass
                ci = "unknown"
                try:
                    c = await gh.get(f"/repos/{repo}/commits/{pr['head']['sha']}/check-runs", params={"per_page": 100})
                    c.raise_for_status()
                    runs = c.json().get("check_runs", [])
                    if not runs:
                        ci = "none"
                    elif any(x["conclusion"] in ("failure", "timed_out", "cancelled") for x in runs if x["conclusion"]):
                        ci = "failing"
                    elif all(x["status"] == "completed" for x in runs):
                        ci = "passing"
                    else:
                        ci = "running"
                except httpx.HTTPError:
                    try:
                        s = await gh.get(f"/repos/{repo}/commits/{pr['head']['sha']}/status")
                        s.raise_for_status()
                        combined = s.json()
                        ci = {"success": "passing", "failure": "failing", "pending": "running"}.get(combined.get("state"), "unknown")
                        if not combined.get("statuses"):
                            ci = "none"
                    except httpx.HTTPError:
                        pass
                review = "none"
                try:
                    rv = await gh.get(f"/repos/{repo}/pulls/{pr['number']}/reviews", params={"per_page": 100})
                    rv.raise_for_status()
                    states = [x["state"] for x in rv.json()]
                    if "CHANGES_REQUESTED" in states:
                        review = "changes_requested"
                    elif "APPROVED" in states:
                        review = "approved"
                except httpx.HTTPError:
                    pass
                prs[key] = {
                    "repo": repo, "number": pr["number"], "title": pr["title"],
                    "body": pr.get("body") or "", "url": pr["html_url"],
                    "branch": pr["head"]["ref"], "created_at": pr["created_at"],
                    "ci": ci, "review": review,
                    "mergeable_state": detail.get("mergeable_state", "unknown"),
                    "draft": pr.get("draft", False),
                }
    state["issues"], state["prs"] = issues, prs
    state["github_ok"] = True

# ---------------------------------------------------------------- devin fetch

ACTIVE_STATUSES = {"running", "working", "resumed", "resume_requested", "resume_requested_frontend", "suspend_requested", "suspend_requested_frontend"}

async def fetch_devin():
    async with devin_client() as dv:
        r = await dv.get("/sessions", params={"limit": 100})
        r.raise_for_status()
        sessions = [s for s in r.json().get("items", []) if not s.get("is_archived")]
    state["sessions"] = sessions
    state["devin_ok"] = True

def _clean_text(text: str) -> str:
    if text.startswith("SYSTEM:"):
        m = re.search(r"<latest_message>\n?(.*?)\n?</latest_message>", text, re.S)
        if m:
            text = re.sub(r"^\S+ \(U[\w]+\) \[ts=[\d.]+\]: ", "", m.group(1))
    return text

async def fetch_session_messages(session_id: str) -> list[dict]:
    cached = session_msgs_cache.get(session_id) or {"msgs": [], "cursor": None, "seen": set()}
    msgs, cursor, seen = list(cached["msgs"]), cached["cursor"], set(cached["seen"])
    async with devin_client() as dv:
        for _ in range(20):
            params = {"after": cursor} if cursor else {}
            r = await dv.get(f"/sessions/{session_id}/messages", params=params)
            r.raise_for_status()
            page = r.json()
            for m in page.get("items", []):
                eid = m.get("event_id") or f"{m.get('created_at')}:{m.get('message')}"
                if eid in seen:
                    continue
                seen.add(eid)
                msgs.append({
                    "who": "user" if m.get("source") == "user" else "devin",
                    "ts": m.get("created_at", ""),
                    "text": _clean_text(m.get("message") or ""),
                })
            # the final page carries no end_cursor, so it is re-read on the next
            # call; `seen` keeps those items from being appended twice
            cursor = page.get("end_cursor") or cursor
            if not page.get("has_next_page"):
                break
    session_msgs_cache[session_id] = {"msgs": msgs, "cursor": cursor, "seen": seen}
    return msgs

# ---------------------------------------------------------------- extractor

EXTRACT_PROMPT = """You are given the chat transcript between a user and Devin (an AI software agent) working on a task.
Return STRICT JSON (no markdown) with this shape:
{"todos":[{"text":"...","owner":"agent|you","state":"done|active|open"}],
 "current_activity":"one short line: what the agent is doing right now, present tense",
 "ask":"if the agent is waiting on the user, the user's next action in one short imperative line (e.g. 'Approve the blog PR' / 'Choose between A and B'), else null",
 "options":["short option labels if the agent offered numbered/discrete choices, max 3, else empty"],
 "progress_pct":0-100}
Keep todo texts short (<70 chars). Derive todos from the plan/steps discussed. Mark items the user must do as owner "you".
If the last message is the agent asking the user something or reporting completion, current_activity MUST say it is waiting (e.g. "Waiting for you to ...") — never invent in-progress work."""

async def extract_session(session_id: str, messages: list[dict]) -> dict | None:
    if not OPENROUTER_API_KEY or not messages:
        return None
    key = f"{len(messages)}:{messages[-1]['ts']}"
    cached = extract_cache.get(session_id)
    if cached and (cached["key"] == key or cached.get("count") == len(messages)):
        return cached["data"]
    transcript = "\n".join(f"[{m['who']}] {m['text']}" for m in messages)[-24000:]
    try:
        async with httpx.AsyncClient(timeout=60) as cl:
            r = await cl.post(
                f"{OR_BASE}/chat/completions",
                headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}"},
                json={
                    "model": OPENROUTER_MODEL,
                    "messages": [
                        {"role": "system", "content": EXTRACT_PROMPT},
                        {"role": "user", "content": transcript},
                    ],
                    "response_format": {"type": "json_object"},
                    "temperature": 0,
                },
            )
            r.raise_for_status()
            content = r.json()["choices"][0]["message"]["content"]
            data = json.loads(content)
    except Exception as e:  # noqa: BLE001 — extractor is best-effort by design
        log.warning("extractor failed for %s: %s", session_id, e)
        return None
    extract_cache[session_id] = {"key": key, "count": len(messages), "data": data}
    return data

# ---------------------------------------------------------------- board assembly

def humanize_age(ts: str | int | float) -> str:
    from datetime import datetime, timezone
    if isinstance(ts, (int, float)):
        secs = time.time() - ts
    else:
        try:
            t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            secs = (datetime.now(timezone.utc) - t).total_seconds()
        except (ValueError, AttributeError):
            return ""
    if secs < 3600:
        return f"{max(1, int(secs // 60))}m"
    if secs < 86400:
        return f"{int(secs // 3600)}h"
    return f"{int(secs // 86400)}d"

def session_issue_key(sess: dict) -> str | None:
    for tag in sess.get("tags") or []:
        m = ISSUE_TAG_RE.search(tag)
        if m:
            return f"{m.group(1)}#{m.group(2)}"
    return None

def pr_issue_key(pr: dict) -> str | None:
    for m in PR_ISSUE_RE.finditer(pr.get("body") or ""):
        key = f"{pr['repo']}#{m.group(1)}"
        if key in state["issues"]:
            return key
    return None

def session_status(sess: dict) -> str:
    return (sess.get("status") or "").lower()

def session_needs_user(sess: dict) -> bool:
    detail = (sess.get("status_detail") or "").lower()
    return detail == "waiting_for_user" or session_status(sess) == "blocked"

def session_pr_urls(sess: dict) -> list[str]:
    return [p["pr_url"] for p in sess.get("pull_requests") or [] if p.get("pr_url")]

async def assemble_board():
    issues, prs, sessions = state["issues"], state["prs"], state["sessions"]
    cards = {}

    for key, issue in issues.items():
        cards[key] = {
            "id": key, "kind": "issue", "title": issue["title"],
            "repo": issue["repo"], "number": issue["number"], "url": issue["url"],
            "body": issue["body"], "labels": issue["labels"],
            "sessions": [], "prs": [], "created_at": issue["created_at"],
        }
    for key, pr in prs.items():
        ik = pr_issue_key(pr)
        if ik and ik in cards:
            cards[ik]["prs"].append(pr)
        else:
            cid = f"pr:{key}"
            cards[cid] = {
                "id": cid, "kind": "pr", "title": pr["title"], "repo": pr["repo"],
                "number": pr["number"], "url": pr["url"], "sessions": [],
                "prs": [pr], "created_at": pr["created_at"],
            }
    for sess in sessions:
        st = session_status(sess)
        if st in ("finished", "expired", "suspended") and not session_pr_urls(sess):
            continue
        ik = session_issue_key(sess)
        if ik and ik in cards:
            cards[ik]["sessions"].append(sess)
        else:
            attached = None
            for url in session_pr_urls(sess):
                for cid, c in cards.items():
                    if any(p["url"] == url for p in c["prs"]):
                        attached = cid
                        break
                if attached:
                    break
            if attached:
                cards[attached]["sessions"].append(sess)
            elif st in ACTIVE_STATUSES or st == "blocked":
                cid = f"session:{sess['session_id']}"
                cards[cid] = {
                    "id": cid, "kind": "session",
                    "title": sess.get("title") or sess["session_id"],
                    "repo": None, "number": None,
                    "url": sess.get("url") or f"https://app.devin.ai/sessions/{sess['session_id']}",
                    "sessions": [sess], "prs": [], "created_at": sess.get("created_at", ""),
                }

    out = []
    for c in cards.values():
        if c["id"] in dismissed:
            continue
        sess = c["sessions"][0] if c["sessions"] else None
        st = session_status(sess) if sess else None
        pr = c["prs"][0] if c["prs"] else None
        extract = None
        if sess:
            sid = sess["session_id"]
            msgs = (session_msgs_cache.get(sid) or {}).get("msgs") or []
            extract = await extract_session(sid, msgs) if msgs else None

        if (sess and session_needs_user(sess)) or (pr and pr["ci"] == "failing") or (pr and pr["review"] == "changes_requested"):
            col, tone = "needs-you", ("red" if pr and pr["ci"] == "failing" else "amber")
        elif pr and not pr["draft"] and pr["ci"] == "passing" and pr["review"] == "approved" and pr["mergeable_state"] == "clean":
            col, tone = "ready", "green"
        elif pr and not pr["draft"]:
            col, tone = "review", "purple"
        elif st in ACTIVE_STATUSES:
            col, tone = "working", "blue"
        elif c["kind"] == "issue":
            col, tone = "issues", "grey"
        else:
            col, tone = "working", "blue"

        now_text = None
        ask = None
        options = []
        if extract:
            now_text = extract.get("current_activity")
            ask = extract.get("ask")
            options = extract.get("options") or []
        if col == "needs-you" and not ask:
            if pr and pr["ci"] == "failing":
                ask = "CI failed — take a look"
            elif st == "blocked":
                ask = "Devin is blocked and waiting on you"
            elif sess and session_needs_user(sess):
                ask = "Devin asked you a question — reply"
            elif pr and pr["review"] == "changes_requested":
                ask = "Review requested changes"
        if col == "review" and not ask and pr and pr["ci"] == "passing":
            now_text = f"You: review & merge PR #{pr['number']}"

        out.append({
            **{k: c[k] for k in ("id", "kind", "title", "repo", "number", "url")},
            "col": col, "tone": tone,
            "session_id": sess["session_id"] if sess else None,
            "session_url": (sess.get("url") or f"https://app.devin.ai/sessions/{sess['session_id']}") if sess else None,
            "pr": {k: pr[k] for k in ("repo", "number", "url", "ci", "review", "mergeable_state")} if pr else None,
            "now": ask if col == "needs-you" else (f"You: {ask}" if ask else now_text),
            "options": options if col == "needs-you" else [],
            "todos": (extract or {}).get("todos", []),
            "progress_pct": (extract or {}).get("progress_pct"),
            "age": humanize_age(sess.get("created_at", "") if sess else c["created_at"]),
            "acus": sess.get("acus_consumed") if sess else None,
            "body": c.get("body", ""),
            "labels": c.get("labels", []),
            "created_at": c["created_at"],
        })

    # newest issues first; everything else oldest first
    state["board"] = {
        "columns": [
            {"id": cid, "cards": sorted((c for c in out if c["col"] == cid),
                                        key=lambda c: str(c["created_at"]), reverse=(cid == "issues"))}
            for cid in ("issues", "working", "needs-you", "review", "ready")
        ],
        "devin_ok": state["devin_ok"],
        "github_ok": state["github_ok"],
        "generated_at": time.time(),
    }
    state["generated_at"] = time.time()

# ---------------------------------------------------------------- pollers

async def poll_loop():
    last_gh = 0.0
    while True:
        try:
            if time.time() - last_gh >= GITHUB_POLL_SECS:
                await fetch_github()
                last_gh = time.time()
        except Exception as e:  # noqa: BLE001
            state["github_ok"] = False
            log.warning("github poll failed: %s", e)
        try:
            await fetch_devin()
            # refresh messages for sessions that appear on the board
            for sess in state["sessions"]:
                st = session_status(sess)
                if st in ACTIVE_STATUSES or st == "blocked" or session_issue_key(sess):
                    try:
                        await fetch_session_messages(sess["session_id"])
                    except httpx.HTTPError:
                        pass
        except Exception as e:  # noqa: BLE001
            state["devin_ok"] = False
            log.warning("devin poll failed: %s", e)
        try:
            await assemble_board()
        except Exception as e:  # noqa: BLE001
            log.exception("board assembly failed: %s", e)
        await asyncio.sleep(DEVIN_POLL_SECS)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(poll_loop())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

# ---------------------------------------------------------------- auth

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if request.url.path.startswith("/api/") and BOARD_TOKEN:
        token = request.headers.get("x-board-token")
        if token is None and request.url.path.startswith("/api/attachment/"):
            token = request.query_params.get("t")
        if token != BOARD_TOKEN:
            return JSONResponse({"error": "unauthorized"}, status_code=401)
    return await call_next(request)

# ---------------------------------------------------------------- api

@app.get("/api/board")
async def get_board():
    if state["board"] is None:
        return {"columns": [{"id": c, "cards": []} for c in ("issues", "working", "needs-you", "review", "ready")],
                "loading": True, "devin_ok": state["devin_ok"], "github_ok": state["github_ok"], "generated_at": 0}
    return state["board"]

def find_card(card_id: str) -> dict:
    board = state["board"] or {"columns": []}
    for col in board["columns"]:
        for c in col["cards"]:
            if c["id"] == card_id:
                return c
    raise HTTPException(404, "card not found")

@app.get("/api/card/{card_id:path}/messages")
async def get_messages(card_id: str):
    card = find_card(card_id)
    if not card["session_id"]:
        return {"messages": []}
    return {"messages": await fetch_session_messages(card["session_id"])}

@app.get("/api/attachment/{uuid}/{name}")
async def get_attachment(uuid: str, name: str):
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", uuid) or name in (".", ".."):
        raise HTTPException(400, "bad attachment path")
    async with devin_client() as dv:
        r = await dv.get(f"/attachments/{uuid}/{name}", follow_redirects=True, timeout=60)
    if r.status_code >= 400:
        raise HTTPException(r.status_code, "attachment unavailable")
    return Response(
        content=r.content,
        media_type=r.headers.get("content-type", "application/octet-stream"),
        headers={"Cache-Control": "private, max-age=86400", "Content-Disposition": "inline"},
    )

class MessageIn(BaseModel):
    text: str

@app.post("/api/card/{card_id:path}/message")
async def post_message(card_id: str, body: MessageIn):
    card = find_card(card_id)
    if not card["session_id"]:
        raise HTTPException(400, "card has no session")
    async with devin_client() as dv:
        r = await dv.post(f"/sessions/{card['session_id']}/messages", json={"message": body.text})
        if r.status_code >= 400:
            raise HTTPException(r.status_code, f"Devin API: {r.text[:200]}")
    return {"ok": True}

@app.post("/api/card/{card_id:path}/archive")
async def archive_card(card_id: str):
    card = find_card(card_id)
    if card["kind"] == "session" and card["session_id"]:
        async with devin_client() as dv:
            r = await dv.post(f"/sessions/{card['session_id']}/archive")
            if r.status_code >= 400:
                raise HTTPException(r.status_code, f"Devin API: {r.text[:200]}")
    else:
        dismissed.add(card_id)
        save_dismissed(dismissed)
    try:
        await fetch_devin()
        await assemble_board()
    except Exception:  # noqa: BLE001
        pass
    return {"ok": True}

class StartIn(BaseModel):
    repo: str
    number: int

@app.post("/api/issue/start")
async def start_session(body: StartIn):
    key = f"{body.repo}#{body.number}"
    issue = state["issues"].get(key)
    if not issue:
        raise HTTPException(404, "issue not found")
    tag = f"issue:{key}"
    for sess in state["sessions"]:
        if session_issue_key(sess) == key and session_status(sess) in ACTIVE_STATUSES | {"blocked"}:
            raise HTTPException(409, "issue already has an active session")
    prompt = (
        f"Work on this GitHub issue in {body.repo}:\n\n"
        f"# {issue['title']}\n\n{issue['body']}\n\n{issue['url']}\n\n"
        f"Implement it and open a PR that references the issue with 'Fixes #{body.number}'."
    )
    async with devin_client() as dv:
        r = await dv.post("/sessions", json={"prompt": prompt, "tags": [tag], "title": issue["title"]})
        if r.status_code >= 400:
            raise HTTPException(r.status_code, f"Devin API: {r.text[:200]}")
        data = r.json()
    try:
        await fetch_devin()
        await assemble_board()
    except Exception:  # noqa: BLE001
        pass
    return {"ok": True, "session_id": data.get("session_id"), "url": data.get("url")}

class IssueIn(BaseModel):
    title: str
    repo: str | None = None
    body: str = ""

@app.post("/api/issues")
async def create_issue(body: IssueIn):
    repo = body.repo or REPOS[0]
    async with gh_client() as gh:
        r = await gh.post(f"/repos/{repo}/issues", json={"title": body.title, "body": body.body})
        if r.status_code >= 400:
            raise HTTPException(r.status_code, f"GitHub: {r.text[:200]}")
        data = r.json()
    key = f"{repo}#{data['number']}"
    recent_issues[key] = (time.time(), issue_from_gh(repo, data))
    try:
        await fetch_github()
    except Exception:  # noqa: BLE001
        state["issues"][key] = recent_issues[key][1]
    try:
        await assemble_board()
    except Exception:  # noqa: BLE001
        pass
    return {"ok": True, "repo": repo, "number": data["number"], "url": data["html_url"]}

class MergeIn(BaseModel):
    repo: str
    number: int

@app.post("/api/pr/merge")
async def merge_pr(body: MergeIn):
    async with gh_client() as gh:
        r = await gh.put(f"/repos/{body.repo}/pulls/{body.number}/merge", json={"merge_method": "squash"})
        if r.status_code >= 400:
            detail = r.json().get("message", r.text[:200]) if r.headers.get("content-type", "").startswith("application/json") else r.text[:200]
            raise HTTPException(r.status_code, f"GitHub: {detail}")
    try:
        await fetch_github()
        await assemble_board()
    except Exception:  # noqa: BLE001
        pass
    return {"ok": True}

# ---------------------------------------------------------------- static

@app.get("/")
async def index():
    return FileResponse("static/index.html")

app.mount("/static", StaticFiles(directory="static"), name="static")
