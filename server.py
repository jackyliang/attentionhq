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
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
import psycopg
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
REPOS = [r.strip() for r in os.environ.get("REPOS", "jackyliang/answer-hq,jackyliang/answerhq-web,jackyliang/attentionhq").split(",") if r.strip()]
RENDER_API_KEY = os.environ.get("RENDER_API_KEY", "")
RENDER_SERVICE_TYPES = {t.strip() for t in os.environ.get("RENDER_SERVICE_TYPES", "web_service,static_site").split(",") if t.strip()}
DEVIN_POLL_SECS = int(os.environ.get("DEVIN_POLL_SECS", "15"))
GITHUB_POLL_SECS = int(os.environ.get("GITHUB_POLL_SECS", "60"))
RENDER_POLL_SECS = int(os.environ.get("RENDER_POLL_SECS", "15"))
# Automation-origin sessions (merged-PR review bots and the like) are background
# chatter, not work the board tracks.
SHOW_AUTOMATION_SESSIONS = os.environ.get("SHOW_AUTOMATION_SESSIONS", "").lower() in ("1", "true", "yes")

DEVIN_BASE = f"https://api.devin.ai/v3/organizations/{DEVIN_ORG_ID}"
GH_BASE = "https://api.github.com"
OR_BASE = "https://openrouter.ai/api/v1"
RENDER_BASE = "https://api.render.com/v1"

ISSUE_TAG_RE = re.compile(r"issue:([\w.-]+/[\w.-]+)#(\d+)")
PR_ISSUE_RE = re.compile(r"(?:#|issues/)(\d+)")
# A session started from the prompt box is tagged prompt:<id>; Devin writes the
# same id into the issue it files so the board can pair them up.
PROMPT_TAG_RE = re.compile(r"^prompt:([0-9a-f]{12})$")
PROMPT_MODE_TAG_RE = re.compile(r"^prompt-mode:(file|work)$")
PROMPT_MARK_RE = re.compile(r"\s*<!--\s*attention:prompt:([0-9a-f]{12})\s*-->")

# ---------------------------------------------------------------- state

state: dict = {
    "issues": {},      # "owner/repo#n" -> issue dict
    "prs": {},         # "owner/repo#n" -> pr dict (enriched with ci/review)
    "sessions": [],    # devin session list
    "deploys": [],     # render deploy status per service
    "board": None,     # assembled board payload
    "devin_ok": True,
    "github_ok": True,
    "render_ok": True,
    "generated_at": 0,
    "gh_refresh": False,  # pull GitHub on the next tick instead of waiting out the interval
}
DISMISSED_FILE = os.environ.get("DISMISSED_FILE", "dismissed.json")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

def _db():
    return psycopg.connect(DATABASE_URL, connect_timeout=10)

def load_dismissed() -> set:
    if DATABASE_URL:
        try:
            with _db() as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS dismissed (card_id text PRIMARY KEY, created_at timestamptz DEFAULT now())")
                return {r[0] for r in conn.execute("SELECT card_id FROM dismissed")}
        except psycopg.Error:
            log.warning("could not load dismissed list from db", exc_info=True)
            return set()
    try:
        with open(DISMISSED_FILE) as f:
            return set(json.load(f))
    except (OSError, ValueError):
        return set()

def save_dismissed(d: set):
    if DATABASE_URL:
        try:
            with _db() as conn:
                conn.execute("DELETE FROM dismissed WHERE card_id <> ALL(%s)", (list(d),))
                for cid in d:
                    conn.execute("INSERT INTO dismissed (card_id) VALUES (%s) ON CONFLICT DO NOTHING", (cid,))
        except psycopg.Error:
            log.warning("could not persist dismissed list to db", exc_info=True)
        return
    try:
        with open(DISMISSED_FILE, "w") as f:
            json.dump(sorted(d), f)
    except OSError:
        log.warning("could not persist dismissed list")

dismissed: set = load_dismissed()
extract_cache: dict = {}  # session_id -> {"key": last_msg_key, "data": {...}}
session_msgs_cache: dict = {}  # session_id -> {"msgs": [...], "cursor": str|None, "seen": {event_id}}
pr_meta_cache: dict = {}  # "owner/repo#n" -> {"at": epoch, "data": {title, state, draft}}
PR_META_TTL = 300

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

def render_client():
    return httpx.AsyncClient(base_url=RENDER_BASE, headers={"Authorization": f"Bearer {RENDER_API_KEY}", "Accept": "application/json"}, timeout=30)

# ---------------------------------------------------------------- github fetch

def issue_from_gh(repo: str, it: dict) -> dict:
    body = it.get("body") or ""
    mark = PROMPT_MARK_RE.search(body)
    return {
        "repo": repo, "number": it["number"], "title": it["title"],
        "body": PROMPT_MARK_RE.sub("", body), "url": it["html_url"],
        "prompt_id": mark.group(1) if mark else None,
        "labels": [l["name"] for l in it.get("labels", [])],
        "created_at": it["created_at"], "updated_at": it["updated_at"],
    }

# GitHub's list endpoint lags behind creates; keep issues we just created visible
# until the listing catches up (or they get closed).
RECENT_ISSUE_TTL = 300
recent_issues: dict[str, tuple[float, dict]] = {}
# ...and hide issues we just closed until the listing stops returning them.
recently_closed: dict[str, float] = {}

async def commit_ci(gh: httpx.AsyncClient, repo: str, sha: str) -> str:
    try:
        c = await gh.get(f"/repos/{repo}/commits/{sha}/check-runs", params={"per_page": 100})
        c.raise_for_status()
        runs = c.json().get("check_runs", [])
        if not runs:
            return "none"
        if any(x["conclusion"] in ("failure", "timed_out", "cancelled") for x in runs if x["conclusion"]):
            return "failing"
        if all(x["status"] == "completed" for x in runs):
            return "passing"
        return "running"
    except httpx.HTTPError:
        try:
            s = await gh.get(f"/repos/{repo}/commits/{sha}/status")
            s.raise_for_status()
            combined = s.json()
            if not combined.get("statuses"):
                return "none"
            return {"success": "passing", "failure": "failing", "pending": "running"}.get(combined.get("state"), "unknown")
        except httpx.HTTPError:
            return "unknown"

async def fetch_github():
    issues, prs = {}, {}
    async with gh_client() as gh:
        for repo in REPOS:
            r = await gh.get(f"/repos/{repo}/issues", params={"state": "open", "per_page": 100})
            r.raise_for_status()
            now = time.time()
            for key, ts in list(recently_closed.items()):
                if now - ts > RECENT_ISSUE_TTL:
                    recently_closed.pop(key, None)
            for it in r.json():
                if "pull_request" in it:
                    continue
                key = f"{repo}#{it['number']}"
                if key in recently_closed:
                    continue
                issues[key] = issue_from_gh(repo, it)
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
                ci = await commit_ci(gh, repo, pr["head"]["sha"])
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

# ---------------------------------------------------------------- render fetch

RENDER_IN_PROGRESS = {"created", "queued", "build_in_progress", "pre_deploy_in_progress", "update_in_progress"}
RENDER_FAILED = {"build_failed", "pre_deploy_failed", "update_failed"}

def repo_from_url(url: str) -> str:
    return re.sub(r"^https?://github\.com/", "", url or "").removesuffix(".git").strip("/")

def deploy_commit(d: dict | None) -> dict | None:
    c = (d or {}).get("commit") or {}
    if not c:
        return None
    lines = (c.get("message") or "").splitlines()
    return {"sha": (c.get("id") or "")[:7], "message": lines[0] if lines else ""}

def deploy_summary(service: dict, deploys: list[dict]) -> dict:
    latest = deploys[0] if deploys else None
    live = next((d for d in deploys if d.get("status") == "live"), None)
    if latest and latest.get("status") in RENDER_IN_PROGRESS:
        status = "deploying"
    elif latest and latest.get("status") in RENDER_FAILED:
        status = "failed"
    elif latest and latest.get("status") == "canceled":
        status = "canceled"
    elif live:
        status = "live"
    else:
        status = "unknown"
    return {
        "id": service["id"], "name": service["name"], "type": service["type"],
        "repo": repo_from_url(service.get("repo", "")),
        "url": service.get("dashboardUrl") or f"https://dashboard.render.com/web/{service['id']}",
        "status": status,
        "deploying_since": latest.get("createdAt") if status == "deploying" else None,
        "deploying_commit": deploy_commit(latest) if status == "deploying" else None,
        "failed_at": (latest.get("updatedAt") or latest.get("createdAt")) if status == "failed" else None,
        "last_deployed_at": (live.get("finishedAt") or live.get("updatedAt")) if live else None,
        "last_deployed_commit": deploy_commit(live),
    }

async def fetch_render():
    if not RENDER_API_KEY:
        state["deploys"] = []
        return
    out = []
    async with render_client() as rc:
        r = await rc.get("/services", params={"limit": 100})
        r.raise_for_status()
        services = [x.get("service", x) for x in r.json()]
        for svc in services:
            if svc.get("type") not in RENDER_SERVICE_TYPES or svc.get("suspended") == "suspended":
                continue
            d = await rc.get(f"/services/{svc['id']}/deploys", params={"limit": 10})
            d.raise_for_status()
            deploys = [x.get("deploy", x) for x in d.json()]
            out.append(deploy_summary(svc, deploys))
    def latest(s):
        return s["deploying_since"] or s["failed_at"] or s["last_deployed_at"] or ""
    out.sort(key=lambda s: (s["status"] == "deploying", latest(s)), reverse=True)
    state["deploys"] = out
    state["render_ok"] = True

# ---------------------------------------------------------------- devin fetch

ACTIVE_STATUSES = {"running", "working", "resumed", "resume_requested", "resume_requested_frontend", "suspend_requested", "suspend_requested_frontend"}

async def fetch_devin():
    async with devin_client() as dv:
        r = await dv.get("/sessions", params={"limit": 100})
        r.raise_for_status()
        sessions = [
            s for s in r.json().get("items", [])
            if not s.get("is_archived") and (SHOW_AUTOMATION_SESSIONS or not is_automation_session(s))
        ]
    state["sessions"] = sessions
    state["devin_ok"] = True
    live = {s["session_id"] for s in sessions}
    for cache in (session_msgs_cache, extract_cache):
        for sid in [sid for sid in cache if sid not in live]:
            del cache[sid]

def is_automation_session(sess: dict) -> bool:
    return sess.get("origin") == "automation" or bool(sess.get("automation_id"))

def _clean_text(text: str) -> str:
    if text.startswith("SYSTEM:"):
        m = re.search(r"<latest_message>\n?(.*?)\n?</latest_message>", text, re.S)
        if m:
            text = re.sub(r"^\S+ \(U[\w]+\) \[ts=[\d.]+\]: ", "", m.group(1))
    return text

async def fetch_session_messages(session_id: str, updated_at: int | None = None) -> list[dict]:
    cached = session_msgs_cache.get(session_id) or {"msgs": [], "cursor": None, "seen": set()}
    # The session's updated_at tracks its latest message, so skip the round-trip
    # (which always re-downloads the final page) when nothing has changed.
    if updated_at is not None and cached.get("updated_at") == updated_at:
        return cached["msgs"]
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
                    "origin": m.get("origin") or None,
                    "name": m.get("username") or None,
                })
            # the final page carries no end_cursor, so it is re-read on the next
            # call; `seen` keeps those items from being appended twice
            cursor = page.get("end_cursor") or cursor
            if not page.get("has_next_page"):
                break
    session_msgs_cache[session_id] = {
        "msgs": msgs, "cursor": cursor, "seen": seen,
        "updated_at": updated_at if updated_at is not None else cached.get("updated_at"),
    }
    return msgs

# ---------------------------------------------------------------- extractor

EXTRACT_PROMPT = """You are given the chat transcript between a user and Devin (an AI software agent) working on a task.
Return STRICT JSON (no markdown) with this shape:
{"todos":[{"text":"...","owner":"agent|you","state":"done|active|open"}],
 "current_activity":"one short line: what the agent is doing right now, present tense",
 "ask":"if the agent is waiting on the user, the user's next action in one short imperative line (e.g. 'Approve the blog PR' / 'Choose between A and B'), else null",
 "last_said":"the agent's most recent message to the user compressed to one line (<80 chars): the question it asked, or the answer/result it reported (e.g. 'Asked: keep Jinja or switch to Next?' / 'Reviewed #378: no dead code found')",
 "options":["short option labels if the agent offered numbered/discrete choices, max 3, else empty"],
 "blocked":true|false,
 "progress_pct":0-100}
Keep todo texts short (<70 chars). Derive todos from the plan/steps discussed. Mark items the user must do as owner "you".
If the last message is the agent asking the user something or reporting completion, current_activity MUST say it is waiting (e.g. "Waiting for you to ...") — never invent in-progress work.
"blocked": true only if the agent explicitly says it cannot continue until the user supplies something (a credential/token, a decision between alternatives, an approval). Examples of blocked=true: "blocked on you for the token", "which approach should I take?", "waiting for your approval to run X". Examples of blocked=false: "PR is up — want me to record a test?", "done; anything else?", "want me to also do X?" (delivered work + optional offer). If one of the offered choices is to skip / do nothing / proceed without it, blocked=false.
A trailing [prs] line lists the session's pull requests and their current state. A PR that is already merged or closed needs nothing from the user: do not ask them to review or merge it, and set ask to null if that was the only pending action."""

EXTRACT_VERSION = 4

async def extract_session(session_id: str, messages: list[dict], context: str = "") -> dict | None:
    if not OPENROUTER_API_KEY or not messages:
        return None
    key = f"{EXTRACT_VERSION}:{len(messages)}:{messages[-1]['ts']}:{context}"
    cached = extract_cache.get(session_id)
    if cached and (cached["key"] == key or (cached.get("count") == len(messages) and cached.get("v") == EXTRACT_VERSION and cached.get("ctx") == context)):
        return cached["data"]
    transcript = "\n".join(f"[{m['who']}] {m['text']}" for m in messages)[-24000:]
    if context:
        transcript += f"\n[prs] {context}"
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
    extract_cache[session_id] = {"key": key, "count": len(messages), "v": EXTRACT_VERSION, "ctx": context, "data": data}
    return data

# ---------------------------------------------------------------- board assembly

def _epoch(ts: str | int | float | None) -> int:
    if isinstance(ts, (int, float)):
        return int(ts)
    try:
        return int(datetime.fromisoformat((ts or "").replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0

def humanize_age(ts: str | int | float) -> str:
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

def session_prompt(sess: dict) -> tuple[str, str] | None:
    """(prompt id, mode) for sessions started from the prompt box."""
    pid = mode = None
    for tag in sess.get("tags") or []:
        m = PROMPT_TAG_RE.match(tag)
        if m:
            pid = m.group(1)
        m = PROMPT_MODE_TAG_RE.match(tag)
        if m:
            mode = m.group(1)
    return (pid, mode or "file") if pid else None

def issues_by_prompt() -> dict[str, str]:
    return {issue["prompt_id"]: key for key, issue in state["issues"].items() if issue.get("prompt_id")}

def pr_issue_key(pr: dict) -> str | None:
    for m in PR_ISSUE_RE.finditer(pr.get("body") or ""):
        key = f"{pr['repo']}#{m.group(1)}"
        if key in state["issues"]:
            return key
    return None

def _short(text, limit: int = 90) -> str | None:
    if not isinstance(text, str) or not text.strip():
        return None
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "\u2026"

def session_status(sess: dict) -> str:
    return (sess.get("status") or "").lower()

def session_needs_user(sess: dict) -> bool:
    detail = (sess.get("status_detail") or "").lower()
    return detail == "waiting_for_user" or session_status(sess) == "blocked"

def session_pr_urls(sess: dict) -> list[str]:
    return [p["pr_url"] for p in sess.get("pull_requests") or [] if p.get("pr_url")]

GH_PR_URL_RE = re.compile(r"github\.com/([^/]+/[^/]+)/pull/(\d+)")

def session_prs(sess: dict) -> list[dict]:
    """PRs Devin reports for the session, in any repo (not only the tracked ones)."""
    out = []
    for p in sess.get("pull_requests") or []:
        m = GH_PR_URL_RE.search(p.get("pr_url") or "")
        if m:
            out.append({"repo": m.group(1), "number": int(m.group(2)), "url": p["pr_url"],
                        "state": (p.get("pr_state") or "open").lower()})
    return out

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
    by_prompt = issues_by_prompt()
    for sess in sessions:
        st = session_status(sess)
        if st in ("finished", "expired", "suspended") and not session_pr_urls(sess):
            continue
        ik = session_issue_key(sess)
        prompt = session_prompt(sess)
        if prompt and not ik:
            ik = by_prompt.get(prompt[0])
            if not ik and st not in ACTIVE_STATUSES and time.time() - _epoch(sess.get("created_at")) < 1800:
                state["gh_refresh"] = True  # Devin just filed the issue; pick it up now
            # a file-only session is done once the issue exists; don't keep it on the card
            if prompt[1] == "file" and st not in ACTIVE_STATUSES and (ik or not session_pr_urls(sess)):
                continue
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
        s_prs = session_prs(sess) if sess else []
        if sess:
            sid = sess["session_id"]
            msgs = (session_msgs_cache.get(sid) or {}).get("msgs") or []
            ctx = ", ".join(f"{p['repo']}#{p['number']} {p['state']}" for p in s_prs)
            extract = await extract_session(sid, msgs, ctx) if msgs else None

        waiting = bool(sess) and session_needs_user(sess)
        busy = st in ACTIVE_STATUSES and not waiting
        blocked = waiting and (st == "blocked" or bool((extract or {}).get("blocked")))
        # Devin is done and the ball is a PR, not a question: file under the PR
        handed_off = waiting and not blocked and (bool(pr) or any(p["state"] == "open" for p in s_prs))
        if pr is None and handed_off:
            ext = next(p for p in s_prs if p["state"] == "open")
            pr = {**ext, "ci": "unknown", "review": "none", "mergeable_state": "unknown", "draft": False}
        # everything it shipped is merged/closed and it isn't asking anything: nothing left for you
        if waiting and not blocked and s_prs and not handed_off and extract is not None and not extract.get("ask"):
            continue

        pr_conflict = bool(pr) and pr["mergeable_state"] == "dirty" and not busy
        if (waiting and not handed_off) or (pr and pr["ci"] == "failing") or (pr and pr["review"] == "changes_requested") or pr_conflict:
            col, tone = "needs-you", ("red" if pr and pr["ci"] == "failing" else "amber")
        elif pr and not pr["draft"] and pr["ci"] in ("passing", "none") and pr["mergeable_state"] == "clean" and not busy:
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
            elif sess and session_needs_user(sess):
                ask = _short((extract or {}).get("last_said")) or ("Devin is blocked and waiting on you" if st == "blocked" else "Devin is waiting on your reply")
            elif pr and pr["review"] == "changes_requested":
                ask = "Review requested changes"
            elif pr_conflict:
                ask = f"Resolve merge conflicts on PR #{pr['number']}"
        if col == "ready" and not ask:
            now_text = f"You: merge PR #{pr['number']}"
        if col == "review" and not ask and pr and (pr["ci"] == "passing" or handed_off) and not busy:
            now_text = f"You: review PR #{pr['number']}"

        out.append({
            **{k: c[k] for k in ("id", "kind", "title", "repo", "number", "url")},
            "col": col, "tone": tone,
            "session_id": sess["session_id"] if sess else None,
            "session_url": (sess.get("url") or f"https://app.devin.ai/sessions/{sess['session_id']}") if sess else None,
            "pr": {k: pr.get(k) for k in ("repo", "number", "url", "ci", "review", "mergeable_state", "draft", "title", "branch", "created_at")} if pr else None,
            "now": ask if col == "needs-you" else (f"You: {ask}" if ask and not busy else now_text),
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
        "deploys": state["deploys"],
        "render": {"configured": bool(RENDER_API_KEY), "ok": state["render_ok"]},
        "devin_ok": state["devin_ok"],
        "github_ok": state["github_ok"],
        "generated_at": time.time(),
    }
    state["generated_at"] = time.time()

# ---------------------------------------------------------------- pollers

async def poll_loop():
    last_gh = 0.0
    last_render = 0.0
    while True:
        try:
            if time.time() - last_gh >= GITHUB_POLL_SECS or state["gh_refresh"]:
                state["gh_refresh"] = False
                await fetch_github()
                last_gh = time.time()
        except Exception as e:  # noqa: BLE001
            state["github_ok"] = False
            log.warning("github poll failed: %s", e)
        try:
            if time.time() - last_render >= RENDER_POLL_SECS:
                await fetch_render()
                last_render = time.time()
        except Exception as e:  # noqa: BLE001
            state["render_ok"] = False
            log.warning("render poll failed: %s", e)
        try:
            await fetch_devin()
            # refresh messages for sessions that appear on the board
            for sess in state["sessions"]:
                st = session_status(sess)
                if st in ACTIVE_STATUSES or st == "blocked" or session_issue_key(sess) or session_prompt(sess):
                    try:
                        await fetch_session_messages(sess["session_id"], sess.get("updated_at"))
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

@app.get("/healthz")
async def healthz():
    if state["board"] is None:
        return JSONResponse({"status": "warming"}, status_code=503)
    return {"status": "ok"}

@app.get("/api/board")
async def get_board():
    if state["board"] is None:
        return {"columns": [{"id": c, "cards": []} for c in ("issues", "working", "needs-you", "review", "ready")],
                "loading": True, "deploys": state["deploys"], "render": {"configured": bool(RENDER_API_KEY), "ok": state["render_ok"]},
                "devin_ok": state["devin_ok"], "github_ok": state["github_ok"], "generated_at": 0}
    return state["board"]

def find_card(card_id: str) -> dict:
    board = state["board"] or {"columns": []}
    for col in board["columns"]:
        for c in col["cards"]:
            if c["id"] == card_id:
                return c
    raise HTTPException(404, "card not found")

PR_META_EMPTY = {"branch": "", "base": "", "additions": 0, "deletions": 0, "changed_files": 0,
                 "ci": "none", "review": "none", "mergeable_state": "unknown"}

async def pr_meta(repo: str, number: int, fallback_state: str) -> dict:
    """Title/state/CI/diff stats for any PR Devin opened, tracked repo or not. Merged/closed PRs cache forever."""
    key = f"{repo}#{number}"
    hit = pr_meta_cache.get(key)
    if hit and (hit["data"]["state"] != "open" or time.time() - hit["at"] < PR_META_TTL):
        return hit["data"]
    tracked = state["prs"].get(key)
    try:
        async with gh_client() as gh:
            r = await gh.get(f"/repos/{repo}/pulls/{number}")
            r.raise_for_status()
            p = r.json()
            merged = bool(p.get("merged"))
            pr_state = "merged" if merged else (p.get("state") or "open")
            data = {
                "title": p.get("title") or f"PR #{number}",
                "state": pr_state,
                "draft": bool(p.get("draft")),
                "created": _epoch(p.get("created_at")),
                "branch": p.get("head", {}).get("ref", ""),
                "base": p.get("base", {}).get("ref", ""),
                "additions": p.get("additions", 0),
                "deletions": p.get("deletions", 0),
                "changed_files": p.get("changed_files", 0),
                "mergeable_state": p.get("mergeable_state", "unknown"),
                "review": tracked["review"] if tracked else "none",
                "ci": (tracked["ci"] if tracked else await commit_ci(gh, repo, p["head"]["sha"])) if pr_state == "open" else "none",
            }
    except httpx.HTTPError:
        if hit:
            return hit["data"]
        if tracked:
            return {**PR_META_EMPTY, "title": tracked["title"], "state": "open", "draft": tracked["draft"],
                    "created": _epoch(tracked["created_at"]), "branch": tracked["branch"],
                    "ci": tracked["ci"], "review": tracked["review"], "mergeable_state": tracked["mergeable_state"]}
        return {**PR_META_EMPTY, "title": f"PR #{number}", "state": fallback_state, "draft": False, "created": 0}
    pr_meta_cache[key] = {"at": time.time(), "data": data}
    return data

async def with_pr_cards(sess: dict, msgs: list[dict]) -> list[dict]:
    """Insert a card for each of the session's PRs where it entered the conversation:
    after the first Devin message linking it, else at the PR's creation time."""
    prs = session_prs(sess)
    if not prs:
        return msgs
    metas = await asyncio.gather(*(pr_meta(p["repo"], p["number"], p["state"]) for p in prs))
    cards = []
    for p, meta in zip(prs, metas):
        needle = re.escape(f"{p['repo']}/pull/{p['number']}") + r"(?!\d)"
        anchor = next((i for i, m in enumerate(msgs) if m["who"] == "devin" and re.search(needle, m["text"])), None)
        if anchor is None:
            anchor = next((i for i, m in enumerate(msgs) if isinstance(m["ts"], (int, float)) and m["ts"] >= meta["created"]), len(msgs)) - 1
        cards.append((anchor, {
            "who": "pr",
            "ts": msgs[anchor]["ts"] if 0 <= anchor < len(msgs) else meta["created"],
            "text": "",
            "pr": {
                "repo": p["repo"], "number": p["number"], "url": p["url"],
                "review_url": f"https://app.devin.ai/review/{p['repo']}/pull/{p['number']}",
                **{k: meta[k] for k in ("title", "state", "draft", "branch", "base", "additions", "deletions",
                                         "changed_files", "ci", "review", "mergeable_state")},
            },
        }))
    out = list(msgs)
    for anchor, card in sorted(cards, key=lambda ac: (ac[0], ac[1]["pr"]["number"]), reverse=True):
        out.insert(anchor + 1, card)
    return out

@app.get("/api/card/{card_id:path}/messages")
async def get_messages(card_id: str):
    card = find_card(card_id)
    if not card["session_id"]:
        return {"messages": [], "status": None}
    msgs = await fetch_session_messages(card["session_id"])
    sess = next((s for s in state["sessions"] if s["session_id"] == card["session_id"]), None)
    return {
        "messages": await with_pr_cards(sess, msgs) if sess else msgs,
        "status": {
            "state": session_status(sess),
            "detail": (sess.get("status_detail") or "").lower(),
            "waiting": session_needs_user(sess),
        } if sess else None,
    }

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

@app.exception_handler(httpx.HTTPError)
async def _upstream_error(_req, exc: httpx.HTTPError):
    log.warning("upstream request failed: %r", exc)
    return JSONResponse({"detail": f"upstream error: {exc.__class__.__name__}"}, status_code=502)

@app.post("/api/card/{card_id:path}/archive")
async def archive_card(card_id: str):
    """Archive the attached Devin session (if any), close the GitHub issue (if
    the card is one), and drop the card from the board."""
    card = find_card_or_stale(card_id)
    actions = []
    if card["session_id"]:
        async with devin_client() as dv:
            r = await dv.post(f"/sessions/{card['session_id']}/archive")
            if r.status_code >= 400:
                raise HTTPException(r.status_code, f"Devin API: {r.text[:200]}")
        actions.append("archived")
    if card["kind"] == "issue":
        async with gh_client() as gh:
            r = await gh.patch(f"/repos/{card['repo']}/issues/{card['number']}", json={"state": "closed"})
            if r.status_code >= 400:
                raise HTTPException(r.status_code, f"GitHub: {r.text[:200]}")
        recently_closed[card_id] = time.time()
        recent_issues.pop(card_id, None)
        state["issues"].pop(card_id, None)
        actions.append("closed")
    elif card["kind"] != "session":
        dismissed.add(card_id)
        await asyncio.to_thread(save_dismissed, dismissed)
    if card["session_id"]:
        state["sessions"] = [s for s in state["sessions"] if s["session_id"] != card["session_id"]]
    for col in (state["board"] or {"columns": []})["columns"]:
        col["cards"] = [c for c in col["cards"] if c["id"] != card_id]
    asyncio.create_task(_refresh_after_archive(bool(card["session_id"])))
    return {"ok": True, "actions": actions}

@app.post("/api/card/{card_id:path}/hide")
async def hide_card(card_id: str):
    """Drop the card from the board without touching the Devin session or the
    GitHub issue."""
    dismissed.add(card_id)
    await asyncio.to_thread(save_dismissed, dismissed)
    for col in (state["board"] or {"columns": []})["columns"]:
        col["cards"] = [c for c in col["cards"] if c["id"] != card_id]
    return {"ok": True, "actions": ["hidden"]}

def find_card_or_stale(card_id: str) -> dict:
    """Like find_card, but tolerates a card the client still shows that has
    already left the server board (e.g. the session went idle between polls)."""
    try:
        return find_card(card_id)
    except HTTPException:
        pass
    if card_id.startswith("session:"):
        return {"id": card_id, "kind": "session", "session_id": card_id[len("session:"):], "repo": None, "number": None}
    issue = state["issues"].get(card_id)
    if not issue and card_id in recent_issues:
        issue = recent_issues[card_id][1]
    if issue:
        sess = next((s for s in state["sessions"] if session_issue_key(s) == card_id), None)
        return {"id": card_id, "kind": "issue", "session_id": sess["session_id"] if sess else None,
                "repo": issue["repo"], "number": issue["number"]}
    raise HTTPException(404, "card not found")

async def _refresh_after_archive(had_session: bool):
    try:
        if had_session:
            await fetch_devin()
        await assemble_board()
    except Exception:  # noqa: BLE001
        log.warning("post-archive refresh failed", exc_info=True)

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

class PromptIn(BaseModel):
    prompt: str
    start: bool = False

@app.post("/api/prompt")
async def prompt_devin(body: PromptIn):
    """The '+' box is a prompt: Devin picks the repo, writes and files the issue
    (and, with start=True, goes on to implement it)."""
    text = body.prompt.strip()
    if not text:
        raise HTTPException(400, "empty prompt")
    pid = uuid.uuid4().hex[:12]
    mode = "work" if body.start else "file"
    repos = "\n".join(f"- {r}" for r in REPOS)
    prompt = (
        "A user typed this task into their Attention board's new-issue box:\n\n"
        f"\"\"\"\n{text}\n\"\"\"\n\n"
        "Turn it into a GitHub issue:\n"
        f"1. Pick the right repository from these tracked repos (choose by what the task is about):\n{repos}\n"
        "2. Write a clear title and description that preserves the user's intent. Follow that repo's issue "
        "conventions and any knowledge you have (title prefixes, labels, sections). Do not ask clarifying "
        "questions; make reasonable assumptions and note them in the issue.\n"
        f"3. The issue body MUST end with this exact line, unchanged: <!-- attention:prompt:{pid} -->\n"
        "4. Create the issue in that repo and reply with just its URL.\n"
        + ("5. Then implement the issue and open a PR that references it with 'Fixes #<number>'.\n" if body.start else
           "Do not start implementing it; filing the issue is the whole task.\n")
    )
    title = _short(text.splitlines()[0] if text.splitlines() else text, 70) or "New task"
    async with devin_client() as dv:
        r = await dv.post("/sessions", json={"prompt": prompt, "tags": [f"prompt:{pid}", f"prompt-mode:{mode}"], "title": title})
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
    issue = issue_from_gh(repo, data)
    recent_issues[key] = (time.time(), issue)
    state["issues"][key] = issue
    try:
        await assemble_board()
    except Exception:  # noqa: BLE001
        pass

    async def refresh():
        try:
            await fetch_github()
            await assemble_board()
        except Exception:  # noqa: BLE001
            pass
    asyncio.create_task(refresh())
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
    pr_meta_cache.pop(f"{body.repo}#{body.number}", None)
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
