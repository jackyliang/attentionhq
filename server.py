"""Attention server — FastAPI app serving the board UI and JSON API.

Reads GitHub issues/PRs and Devin sessions, derives Kanban columns, and
exposes mutations: start session, chat, create issue, merge PR.
"""

import asyncio
import ctypes
import ctypes.util
import hashlib
import hmac
import json
import logging
import os
import re
import time
import uuid
import contextvars
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
import psycopg
from fastapi import FastAPI, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
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
DEVIN_POLL_SECS = int(os.environ.get("DEVIN_POLL_SECS", "5"))
DEVIN_POLL_MAX_SECS = int(os.environ.get("DEVIN_POLL_MAX_SECS", "60"))
# GitHub pushes changes to /api/github/webhook when a secret is configured; the
# poll then only reconciles missed deliveries, so it runs far less often.
GITHUB_WEBHOOK_SECRET = os.environ.get("GITHUB_WEBHOOK_SECRET", "")
GITHUB_POLL_SECS = int(os.environ.get("GITHUB_POLL_SECS", "300" if GITHUB_WEBHOOK_SECRET else "20"))
GITHUB_POLL_MAX_SECS = int(os.environ.get("GITHUB_POLL_MAX_SECS", "3600"))
# Extra request budget the poll keeps in reserve so user actions (merge, edit,
# create) never hit the primary limit.
GITHUB_RATE_RESERVE = int(os.environ.get("GITHUB_RATE_RESERVE", "200"))
RENDER_POLL_SECS = int(os.environ.get("RENDER_POLL_SECS", "15"))
# The sessions list is newest-first, 100 per page; keep paging until sessions are older than this.
DEVIN_LOOKBACK_DAYS = float(os.environ.get("DEVIN_LOOKBACK_DAYS", "14"))
DEVIN_MAX_PAGES = int(os.environ.get("DEVIN_MAX_PAGES", "10"))
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
    "github_rate": {"limit": None, "remaining": None, "reset": None, "retry_at": None},
    "github_synced_at": 0,
    "devin_synced_at": 0,
    "webhook": {"configured": bool(GITHUB_WEBHOOK_SECRET), "last_at": 0, "count": 0},
}
# Set by webhooks and user actions to cut the poll loop's sleep short.
wake = asyncio.Event()

def request_refresh(github: bool = True):
    if github:
        state["gh_refresh"] = True
    wake.set()
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

# Titles the user gave standalone session cards. The Devin API has no rename
# call, so these live on the board only and shadow the session's own title.
SESSION_TITLES_FILE = os.environ.get("SESSION_TITLES_FILE", "session_titles.json")

def load_session_titles() -> dict[str, str]:
    if DATABASE_URL:
        try:
            with _db() as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS session_titles (session_id text PRIMARY KEY, title text NOT NULL, updated_at timestamptz DEFAULT now())")
                return {r[0]: r[1] for r in conn.execute("SELECT session_id, title FROM session_titles")}
        except psycopg.Error:
            log.warning("could not load session titles from db", exc_info=True)
            return {}
    try:
        with open(SESSION_TITLES_FILE) as f:
            return {str(k): str(v) for k, v in json.load(f).items()}
    except (OSError, ValueError, AttributeError):
        return {}

def save_session_titles(titles: dict[str, str], session_id: str):
    """Durably store titles[session_id]; raises on failure so the caller can roll back."""
    if DATABASE_URL:
        with _db() as conn:
            conn.execute(
                "INSERT INTO session_titles (session_id, title) VALUES (%s, %s) "
                "ON CONFLICT (session_id) DO UPDATE SET title = EXCLUDED.title, updated_at = now()",
                (session_id, titles[session_id]),
            )
        return
    with open(SESSION_TITLES_FILE, "w") as f:
        json.dump(titles, f, indent=1, sort_keys=True)

session_titles: dict[str, str] = load_session_titles()
session_titles_lock = asyncio.Lock()
extract_cache: dict = {}  # session_id -> {"key": last_msg_key, "data": {...}}
session_msgs_cache: dict = {}  # session_id -> {"msgs": [...], "cursor": str|None, "seen": {event_id}}
pr_meta_cache: dict = {}  # "owner/repo#n" -> {"at": epoch, "data": {title, state, draft}}
PR_META_TTL = 300

# ---------------------------------------------------------------- clients

# One client per upstream, kept for the process lifetime: a client per call
# builds a fresh TLS context and connection pool every few seconds, and the
# native buffers behind those never come back to the allocator.
_clients: dict[str, httpx.AsyncClient] = {}

def _client(name: str, **kwargs) -> httpx.AsyncClient:
    cl = _clients.get(name)
    if cl is None or cl.is_closed:
        cl = _clients[name] = httpx.AsyncClient(**kwargs)
    return cl

def devin_client() -> httpx.AsyncClient:
    return _client("devin", base_url=DEVIN_BASE, headers={"Authorization": f"Bearer {DEVIN_API_KEY}"}, timeout=30)

def gh_client() -> httpx.AsyncClient:
    return _client(
        "github",
        base_url=GH_BASE,
        headers={
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        timeout=30,
    )

def render_client() -> httpx.AsyncClient:
    return _client("render", base_url=RENDER_BASE, headers={"Authorization": f"Bearer {RENDER_API_KEY}", "Accept": "application/json"}, timeout=30)

def openrouter_client() -> httpx.AsyncClient:
    return _client("openrouter", base_url=OR_BASE, headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}"}, timeout=60)

async def close_clients():
    for cl in list(_clients.values()):
        await cl.aclose()
    _clients.clear()

_libc = None
if hasattr(ctypes, "CDLL"):
    try:
        _libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6")
        _libc.malloc_trim(0)
    except (OSError, AttributeError):
        _libc = None

def release_free_memory():
    """Hand memory freed by the poll cycle back to the OS.

    Each cycle parses megabytes of JSON; glibc keeps the freed arenas, so RSS
    only ever climbs and the instance is eventually OOM-killed."""
    if _libc is not None:
        _libc.malloc_trim(0)

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
# Devin's session list lags behind creates too; keep sessions we just started on the
# board until the listing catches up.
RECENT_SESSION_TTL = 300
recent_sessions: dict[str, tuple[float, dict]] = {}
# first line of the prompt, shown until Devin assigns the session its own title
provisional_titles: dict[str, tuple[float, str]] = {}
# ...and treat a session we just replied to as running until Devin's status catches up.
RECENT_REPLY_TTL = 120
recent_replies: dict[str, float] = {}

def _devin_replied_since(session_id: str, ts: float) -> bool:
    msgs = (session_msgs_cache.get(session_id) or {}).get("msgs") or []
    return any(m["who"] == "devin" and _epoch_f(m.get("ts")) > ts for m in msgs)

def mark_running(sess: dict):
    """A message we just sent means Devin is (about to be) working on it again."""
    sess["status_detail"] = ""
    if session_status(sess) not in ACTIVE_STATUSES:
        sess["status"] = "running"

def remember_session(data: dict, tags: list[str], title: str) -> dict:
    """Build a placeholder for a session the create call just returned, so the
    board can show it before the sessions listing includes it."""
    sid = data.get("session_id") or ""
    sess = {
        "session_id": sid,
        "url": data.get("url") or f"https://app.devin.ai/sessions/{sid}",
        "title": title, "tags": tags, "status": "running", "status_detail": "",
        "created_at": datetime.now(timezone.utc).isoformat(), "pull_requests": [],
    }
    recent_sessions[sid] = (time.time(), sess)
    provisional_titles[sid] = (time.time(), title)
    return sess

# Conditional requests: GitHub answers an unchanged resource with 304, which does
# not count against the rate limit. Cache body + ETag per URL and always revalidate.
ETAG_TTL = 3600
etag_cache: dict[str, dict] = {}  # url -> {"etag": str, "data": Any, "at": epoch}

def _etag_key(path: str, params: dict | None) -> str:
    return path + ("?" + "&".join(f"{k}={v}" for k, v in sorted((params or {}).items())) if params else "")

def note_rate_limit(headers) -> None:
    rate = state["github_rate"]
    try:
        if "x-ratelimit-limit" in headers:
            rate["limit"] = int(headers["x-ratelimit-limit"])
        if "x-ratelimit-remaining" in headers:
            rate["remaining"] = int(headers["x-ratelimit-remaining"])
        if "x-ratelimit-reset" in headers:
            rate["reset"] = int(headers["x-ratelimit-reset"])
    except ValueError:
        pass

class RateLimited(Exception):
    """GitHub (or Devin) asked us to stop; `retry_at` is when to try again."""
    def __init__(self, retry_at: float, response: httpx.Response | None = None):
        super().__init__(f"rate limited until {int(retry_at)}")
        self.retry_at = retry_at
        self.response = response

def _retry_at(r: httpx.Response, default_secs: float = 60) -> float:
    now = time.time()
    if r.headers.get("retry-after", "").isdigit():
        return now + int(r.headers["retry-after"])
    if r.headers.get("x-ratelimit-remaining") == "0" and r.headers.get("x-ratelimit-reset", "").isdigit():
        return max(now + 5, int(r.headers["x-ratelimit-reset"]) + 1)
    return now + default_secs

def _is_rate_limited(e: Exception) -> bool:
    if isinstance(e, RateLimited):
        return True
    if not isinstance(e, httpx.HTTPStatusError):
        return False
    r = e.response
    # GitHub signals primary-limit exhaustion with 403 + X-RateLimit-Remaining: 0,
    # secondary limits with 403/429 + Retry-After.
    return r.status_code == 429 or (r.status_code == 403 and (r.headers.get("x-ratelimit-remaining") == "0" or "retry-after" in r.headers))

# set while a background sync (reconcile / webhook re-read) runs so every read it
# makes stops at the reserve; user-triggered requests are never budget-gated
_background_sync: contextvars.ContextVar[bool] = contextvars.ContextVar("gh_background", default=False)

async def gh_get(gh: httpx.AsyncClient, path: str, params: dict | None = None):
    """GET a GitHub resource with If-None-Match; returns the parsed body."""
    if _background_sync.get():
        _check_budget()
    key = _etag_key(path, params)
    hit = etag_cache.get(key)
    headers = {"If-None-Match": hit["etag"]} if hit else {}
    r = await gh.get(path, params=params, headers=headers)
    note_rate_limit(r.headers)
    if r.status_code == 304 and hit:
        hit["at"] = time.time()
        return hit["data"]
    if r.status_code in (403, 429) and _is_rate_limited(httpx.HTTPStatusError("rate limited", request=r.request, response=r)):
        raise RateLimited(_retry_at(r), r)
    r.raise_for_status()
    data = r.json()
    if r.headers.get("etag"):
        etag_cache[key] = {"etag": r.headers["etag"], "data": data, "at": time.time()}
    return data

def prune_etag_cache():
    cutoff = time.time() - ETAG_TTL
    for k in [k for k, v in etag_cache.items() if v["at"] < cutoff]:
        del etag_cache[k]

async def commit_ci(gh: httpx.AsyncClient, repo: str, sha: str) -> str:
    try:
        runs = (await gh_get(gh, f"/repos/{repo}/commits/{sha}/check-runs", {"per_page": 100})).get("check_runs", [])
        if not runs:
            return "none"
        if any(x["conclusion"] in ("failure", "timed_out", "cancelled") for x in runs if x["conclusion"]):
            return "failing"
        if all(x["status"] == "completed" for x in runs):
            return "passing"
        return "running"
    except RateLimited:
        raise
    except httpx.HTTPError:
        try:
            combined = await gh_get(gh, f"/repos/{repo}/commits/{sha}/status")
            if not combined.get("statuses"):
                return "none"
            return {"success": "passing", "failure": "failing", "pending": "running"}.get(combined.get("state"), "unknown")
        except RateLimited:
            raise
        except httpx.HTTPError:
            return "unknown"

def review_state(reviews: list[dict]) -> str:
    # latest review per reviewer wins; a re-approval clears an earlier request for changes
    latest: dict[str, str] = {}
    for x in reviews:
        st = x.get("state") or ""
        if st in ("APPROVED", "CHANGES_REQUESTED", "DISMISSED"):
            latest[(x.get("user") or {}).get("login") or str(x.get("id"))] = st
    states = set(latest.values())
    if "CHANGES_REQUESTED" in states:
        return "changes_requested"
    if "APPROVED" in states:
        return "approved"
    return "none"

async def enrich_pr(gh: httpx.AsyncClient, repo: str, pr: dict) -> dict:
    """Board PR record from a list/webhook PR object plus its CI and review state."""
    key = f"{repo}#{pr['number']}"
    prev = state["prs"].get(key) or {}
    mergeable_state = pr.get("mergeable_state")
    if mergeable_state is None or mergeable_state == "unknown":
        try:
            mergeable_state = (await gh_get(gh, f"/repos/{repo}/pulls/{pr['number']}")).get("mergeable_state", "unknown")
        except RateLimited:
            raise
        except httpx.HTTPError:
            mergeable_state = prev.get("mergeable_state", "unknown")
    try:
        ci = await commit_ci(gh, repo, pr["head"]["sha"])
    except RateLimited:
        raise
    if ci == "unknown" and prev.get("ci"):
        ci = prev["ci"]
    try:
        review = review_state(await gh_get(gh, f"/repos/{repo}/pulls/{pr['number']}/reviews", {"per_page": 100}))
    except RateLimited:
        raise
    except httpx.HTTPError:
        review = prev.get("review", "none")
    return {
        "repo": repo, "number": pr["number"], "title": pr["title"],
        "body": pr.get("body") or "", "url": pr["html_url"],
        "branch": pr["head"]["ref"], "head_sha": pr["head"]["sha"], "created_at": pr["created_at"],
        "ci": ci, "review": review,
        "mergeable_state": mergeable_state or "unknown",
        "draft": pr.get("draft", False),
    }

def _budget_exhausted() -> bool:
    rate = state["github_rate"]
    return rate["remaining"] is not None and rate["remaining"] < GITHUB_RATE_RESERVE and (rate["reset"] or 0) > time.time()

def _check_budget() -> None:
    """Background reads stop at the reserve so create/merge/edit keep working."""
    if _budget_exhausted():
        raise RateLimited(state["github_rate"]["reset"] + 1)

# keys apply_webhook touched; a reconcile that started before the webhook must not undo it
_webhook_touched: set[str] = set()
_reconcile_lock = asyncio.Lock()

async def fetch_github_repo(gh: httpx.AsyncClient, repo: str) -> tuple[dict, dict]:
    issues, prs = {}, {}
    now = time.time()
    for it in await gh_get(gh, f"/repos/{repo}/issues", {"state": "open", "per_page": 100}):
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
    for pr in await gh_get(gh, f"/repos/{repo}/pulls", {"state": "open", "per_page": 100}):
        prs[f"{repo}#{pr['number']}"] = await enrich_pr(gh, repo, pr)
    return issues, prs

async def fetch_github():
    """Full reconciliation of every tracked repo. Webhooks keep the board current
    between runs; this catches deliveries that were missed. Serialized: two
    overlapping runs would each clear the other's webhook protection."""
    async with _reconcile_lock:
        tok = _background_sync.set(True)
        try:
            await _fetch_github_locked()
        finally:
            _background_sync.reset(tok)

async def _fetch_github_locked():
    _check_budget()
    gh = gh_client()
    now = time.time()
    for key, ts in list(recently_closed.items()):
        if now - ts > RECENT_ISSUE_TTL:
            recently_closed.pop(key, None)
    failed = None
    for repo in REPOS:
        prefix = f"{repo}#"
        _webhook_touched.difference_update({k for k in _webhook_touched if k.startswith(prefix)})
        try:
            ri, rp = await fetch_github_repo(gh, repo)
        except RateLimited:
            raise
        except Exception as e:  # noqa: BLE001 — one repo failing must not blank the others
            failed = e
            log.warning("github fetch failed for %s: %s", repo, e)
            continue
        # merge against *current* state: keys a webhook changed while we were
        # fetching are newer than what GitHub gave us, keep them as they are
        for name, fresh in (("issues", ri), ("prs", rp)):
            store = dict(state[name])
            for k in [k for k in store if k.startswith(prefix) and k not in _webhook_touched]:
                del store[k]
            store.update({k: v for k, v in fresh.items() if k not in _webhook_touched})
            state[name] = store
    prune_etag_cache()
    if failed is not None:
        raise failed
    state["github_ok"] = True
    state["github_synced_at"] = time.time()

async def refresh_pr(repo: str, number: int):
    """Re-read one PR (details, CI, reviews) after a webhook said it changed."""
    gh = gh_client()
    key = f"{repo}#{number}"
    tok = _background_sync.set(True)
    try:
        try:
            pr = await gh_get(gh, f"/repos/{repo}/pulls/{number}")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                _webhook_touched.add(key)
                state["prs"].pop(key, None)
                return
            raise
        if pr.get("state") != "open":
            _webhook_touched.add(key)
            state["prs"].pop(key, None)
            pr_meta_cache.pop(key, None)
            return
        fresh = await enrich_pr(gh, repo, pr)
        # marked at commit time so a reconcile that started earlier can't undo this write
        _webhook_touched.add(key)
        state["prs"][key] = fresh
        pr_meta_cache.pop(key, None)
    finally:
        _background_sync.reset(tok)

# ---------------------------------------------------------------- github webhook

def verify_webhook_signature(secret: str, body: bytes, signature: str | None) -> bool:
    if not secret or not signature or not signature.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)

WEBHOOK_PR_EVENTS = {"pull_request", "pull_request_review", "pull_request_review_comment", "check_run", "check_suite", "status", "workflow_run"}

def apply_webhook(event: str, payload: dict) -> set[tuple[str, int]]:
    """Fold what the payload already tells us into state and return the PRs that
    need a targeted re-read (CI/reviews/mergeability aren't in the payload)."""
    repo = (payload.get("repository") or {}).get("full_name") or ""
    if repo not in REPOS:
        return set()
    dirty: set[tuple[str, int]] = set()
    if event == "issues":
        it = payload.get("issue") or {}
        if not it.get("number"):
            return set()
        key = f"{repo}#{it['number']}"
        _webhook_touched.add(key)
        action = payload.get("action")
        if action == "reopened":
            recently_closed.pop(key, None)
        # deleted/transferred payloads still carry state=open; the issue is gone for us
        gone = action in ("deleted", "transferred")
        if not gone and it.get("state") == "open" and "pull_request" not in it:
            if key not in recently_closed:
                state["issues"][key] = issue_from_gh(repo, it)
                recent_issues.pop(key, None)
        else:
            state["issues"].pop(key, None)
            recent_issues.pop(key, None)
    elif event == "pull_request":
        pr = payload.get("pull_request") or {}
        if not pr.get("number"):
            return set()
        key = f"{repo}#{pr['number']}"
        _webhook_touched.add(key)
        if pr.get("state") == "open":
            prev = state["prs"].get(key) or {}
            state["prs"][key] = {
                "repo": repo, "number": pr["number"], "title": pr["title"],
                "body": pr.get("body") or "", "url": pr["html_url"],
                "branch": pr["head"]["ref"], "head_sha": pr["head"]["sha"], "created_at": pr["created_at"],
                "ci": prev.get("ci", "unknown") if prev.get("head_sha") == pr["head"]["sha"] else "unknown",
                "review": prev.get("review", "none"),
                "mergeable_state": pr.get("mergeable_state") or prev.get("mergeable_state", "unknown"),
                "draft": pr.get("draft", False),
            }
            dirty.add((repo, pr["number"]))
        else:
            state["prs"].pop(key, None)
        pr_meta_cache.pop(key, None)
    elif event in ("pull_request_review", "pull_request_review_comment"):
        pr = payload.get("pull_request") or {}
        if pr.get("number"):
            dirty.add((repo, pr["number"]))
    elif event in ("check_run", "check_suite", "workflow_run"):
        obj = payload.get(event) or {}
        for pr in obj.get("pull_requests") or []:
            if pr.get("number"):
                dirty.add((repo, pr["number"]))
        sha = obj.get("head_sha")
        if sha:
            for p in state["prs"].values():
                if p["repo"] == repo and p.get("head_sha") == sha:
                    dirty.add((repo, p["number"]))
    elif event == "status":
        sha = payload.get("sha")
        for p in state["prs"].values():
            if p["repo"] == repo and p.get("head_sha") == sha:
                dirty.add((repo, p["number"]))
    return dirty

async def _after_webhook(dirty: set[tuple[str, int]]):
    try:
        for repo, number in dirty:
            try:
                await refresh_pr(repo, number)
            except RateLimited as e:
                state["github_rate"]["retry_at"] = e.retry_at
                break
            except httpx.HTTPError as e:
                log.warning("webhook refresh of %s#%s failed: %s", repo, number, e)
        await assemble_board()
    except Exception:  # noqa: BLE001
        log.warning("post-webhook refresh failed", exc_info=True)

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
    rc = render_client()
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
    cutoff = time.time() - DEVIN_LOOKBACK_DAYS * 86400
    sessions, cursor = [], None
    dv = devin_client()
    for _ in range(DEVIN_MAX_PAGES):
        params = {"limit": 100}
        if cursor:
            params["after"] = cursor
        r = await dv.get("/sessions", params=params)
        r.raise_for_status()
        data = r.json()
        items = data.get("items", [])
        sessions.extend(
            s for s in items
            if not s.get("is_archived") and (SHOW_AUTOMATION_SESSIONS or not is_automation_session(s))
        )
        cursor = data.get("end_cursor")
        if not items or not data.get("has_next_page") or not cursor:
            break
        ages = [_epoch(s.get("created_at")) for s in items if s.get("created_at")]
        if ages and min(ages) < cutoff:
            break
    now = time.time()
    live = {s["session_id"] for s in sessions}
    for sid, (ts, sess) in list(recent_sessions.items()):
        if now - ts > RECENT_SESSION_TTL or sid in live:
            recent_sessions.pop(sid, None)
        else:
            sessions.insert(0, sess)
    for sid, ts in list(recent_replies.items()):
        if now - ts > RECENT_REPLY_TTL or _devin_replied_since(sid, ts):
            recent_replies.pop(sid, None)
    for s in sessions:
        if s["session_id"] in recent_replies:
            mark_running(s)
        if s["session_id"] in provisional_titles:
            if s.get("title"):
                provisional_titles.pop(s["session_id"], None)
            else:
                s["title"] = provisional_titles[s["session_id"]][1]
        if s["session_id"] in session_titles:
            s["title"] = session_titles[s["session_id"]]
    for sid, (ts, _) in list(provisional_titles.items()):
        if now - ts > RECENT_SESSION_TTL and sid not in live:
            provisional_titles.pop(sid, None)
    state["sessions"] = sessions
    state["devin_ok"] = True
    # Transcripts are only needed for sessions that can hold a card; holding one
    # per session in the whole lookback window is most of the process's memory.
    # A finished session only keeps a card while one of its PRs is still open.
    open_prs = {pr["url"] for pr in state["prs"].values()}
    live = {
        s["session_id"] for s in sessions
        if tracked_session(s) or open_prs.intersection(session_pr_urls(s))
    }
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
    dv = devin_client()
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
            if m.get("source") == "user":
                _drop_local_echo(msgs, m.get("message") or "")
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
    # post_message may have echoed a message while this poll was in flight; keep it
    live = (session_msgs_cache.get(session_id) or {}).get("msgs") or []
    for m in live:
        if m.get("local") and m not in msgs and not _has_real_user_msg(msgs, m["text"], m["local"]):
            msgs.append(m)
    _expire_local_echoes(msgs)
    session_msgs_cache[session_id] = {
        "msgs": msgs, "cursor": cursor, "seen": seen,
        "updated_at": updated_at if updated_at is not None else cached.get("updated_at"),
    }
    return msgs

# A message the board just sent is echoed into the transcript straight away and
# swapped for the real one once Devin's message list returns it.
LOCAL_ECHO_TTL = 300

def _drop_local_echo(msgs: list[dict], text: str):
    for i, m in enumerate(msgs):
        if m.get("local") and m["text"].strip() == text.strip():
            del msgs[i]
            return

def _has_real_user_msg(msgs: list[dict], text: str, since: float) -> bool:
    """True if Devin's list already holds this user message (sent within the last minute)."""
    return any(
        m["who"] == "user" and not m.get("local") and m["text"].strip() == _clean_text(text).strip()
        and _epoch_f(m.get("ts")) >= since - 60
        for m in msgs
    )

def _expire_local_echoes(msgs: list[dict]):
    now = time.time()
    msgs[:] = [m for m in msgs if not (m.get("local") and now - m["local"] > LOCAL_ECHO_TTL)]

def echo_user_message(session_id: str, text: str):
    cached = session_msgs_cache.setdefault(session_id, {"msgs": [], "cursor": None, "seen": set()})
    now = time.time()
    if not _has_real_user_msg(cached["msgs"], text, now):  # a poll may already have it
        cached["msgs"].append({
            "who": "user", "ts": datetime.now(timezone.utc).isoformat(), "text": text,
            "origin": "web", "name": None, "local": now,
        })
    cached["updated_at"] = None
    recent_replies[session_id] = time.time()
    for s in state["sessions"]:
        if s["session_id"] == session_id:
            mark_running(s)

# ---------------------------------------------------------------- extractor

EXTRACT_PROMPT = """You are given the chat transcript between a user and Devin (an AI software agent) working on a task.
Return STRICT JSON (no markdown) with this shape:
{"todos":[{"text":"...","owner":"agent|you","state":"done|active|open"}],
 "current_activity":"one short line: what the agent is doing right now, present tense",
 "ask":"if the agent is waiting on the user, the user's next action in one short imperative line (e.g. 'Approve the blog PR' / 'Choose between A and B'), else null",
 "last_said":"the agent's most recent message to the user compressed to one line (<80 chars): the question it asked, or the answer/result it reported (e.g. 'Asked: keep Jinja or switch to Next?' / 'Reviewed #378: no dead code found')",
 "question":"if the agent's last message asks the user to choose or decide, that question copied verbatim in one line (<120 chars), else null",
 "options":["the choices the agent offered, if any, each copied verbatim from the agent's own wording (the label/heading of each numbered or bulleted choice, without its explanation), in the agent's order, max 5, else empty"],
 "blocked":true|false,
 "activity":"browser_test|coding|review|deploy|waiting|other",
 "progress_pct":0-100}
"activity": "browser_test" only while the agent says it is currently running a browser / end-to-end / UI test (e.g. "starting the browser test run", "recording a test of the send flow") and has not yet reported the result; once it reports results or asks something, use "waiting".
Keep todo texts short (<70 chars). Derive todos from the plan/steps discussed. Mark items the user must do as owner "you".
If the last message is the agent asking the user something or reporting completion, current_activity MUST say it is waiting (e.g. "Waiting for you to ...") — never invent in-progress work.
"blocked": true only if the agent explicitly says it cannot continue until the user supplies something (a credential/token, a decision between alternatives, an approval). Examples of blocked=true: "blocked on you for the token", "which approach should I take?", "waiting for your approval to run X". Examples of blocked=false: "PR is up — want me to record a test?", "done; anything else?", "want me to also do X?" (delivered work + optional offer). If one of the offered choices is to skip / do nothing / proceed without it, blocked=false.
A trailing [prs] line lists the session's pull requests and their current state. A PR that is already merged or closed needs nothing from the user: do not ask them to review or merge it, and set ask to null if that was the only pending action."""

EXTRACT_VERSION = 6

BROWSER_TEST_RE = re.compile(
    r"\b(?:(?:starting|running|kicking off|beginning|launching)\b[^.\n]{0,60}\b(?:browser|e2e|end-to-end|ui)\b[^.\n]{0,30}\btest|"
    r"testing agent (?:is|will be) (?:now )?(?:running|testing|recording)|record(?:ing)? (?:myself )?testing)",
    re.I,
)

def session_activity(sess: dict | None, msgs: list[dict], extract: dict | None) -> str | None:
    """What the agent is doing right now, when the board can tell. "browser_test" is
    the case the UI surfaces specially; anything else is None."""
    if not sess or session_status(sess) not in ACTIVE_STATUSES or session_needs_user(sess):
        return None
    if extract and extract.get("activity") == "browser_test":
        return "browser_test"
    last = next((m for m in reversed(msgs) if m["who"] in ("devin", "user")), None)
    if last and last["who"] == "devin" and BROWSER_TEST_RE.search(last["text"]):
        return "browser_test"
    return None

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
        r = await openrouter_client().post(
            "/chat/completions",
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

def _epoch_f(ts: str | int | float | None) -> float:
    if isinstance(ts, (int, float)):
        return float(ts)
    try:
        return datetime.fromisoformat((ts or "").replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0

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

def tracked_session(sess: dict) -> bool:
    """Whether the board reads this session's transcript."""
    st = session_status(sess)
    return bool(st in ACTIVE_STATUSES or st == "blocked" or session_issue_key(sess) or session_prompt(sess))

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

# bumped whenever a card is removed out-of-band (archive/hide) so an assembly that
# started before the removal cannot commit a board that still shows the card
board_gen = 0

async def assemble_board():
    gen = board_gen
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
        if prompt and not ik and prompt[1] == "file":
            ik = by_prompt.get(prompt[0])
            if not ik and (st not in ACTIVE_STATUSES or session_needs_user(sess)) and time.time() - _epoch(sess.get("created_at")) < 1800:
                # Devin just filed the issue; pick it up now. With webhooks the
                # `issues` delivery does this, so only nudge a stale reconcile.
                if time.time() - state["github_synced_at"] > (GITHUB_POLL_SECS // 2 if state["webhook"]["configured"] else 30):
                    request_refresh()
            # a file-only session only exists to create the issue: show it just while it is
            # busy filing, never once the issue exists or it stops and waits on you
            if ik or st not in ACTIVE_STATUSES or session_needs_user(sess):
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
                    "filing": bool(prompt) and prompt[1] == "file",
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

        activity = session_activity(sess, msgs if sess else [], extract)
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
        elif c.get("filing"):
            col, tone = "issues", "grey"  # Devin is only filing the issue, not working on it
        elif busy:
            col, tone = "working", "blue"  # Devin is actively on it, even if a PR is already up
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
        question = None
        if extract:
            now_text = extract.get("current_activity")
            ask = extract.get("ask")
            options = [str(o).strip() for o in (extract.get("options") or []) if str(o).strip()][:5]
            question = extract.get("question") or None
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
            "col": col, "tone": tone, "filing": bool(c.get("filing")), "activity": activity,
            "session_id": sess["session_id"] if sess else None,
            "session_url": (sess.get("url") or f"https://app.devin.ai/sessions/{sess['session_id']}") if sess else None,
            "pr": {k: pr.get(k) for k in ("repo", "number", "url", "ci", "review", "mergeable_state", "draft", "title", "branch", "created_at")} if pr else None,
            "now": ask if col == "needs-you" else (f"You: {ask}" if ask and not busy else now_text),
            "options": options,
            "question": question,
            "todos": (extract or {}).get("todos", []),
            "progress_pct": (extract or {}).get("progress_pct"),
            "age": humanize_age(sess.get("created_at", "") if sess else c["created_at"]),
            "acus": sess.get("acus_consumed") if sess else None,
            "body": c.get("body", ""),
            "labels": c.get("labels", []),
            "created_at": c["created_at"],
        })

    if gen != board_gen:
        return
    # newest issues first (with anything still being filed on top); everything else oldest first
    state["board"] = {
        "columns": [
            {"id": cid, "cards": sorted((c for c in out if c["col"] == cid),
                                        key=lambda c: (c["filing"], _epoch_f(c["created_at"])), reverse=(cid == "issues"))}
            for cid in ("issues", "working", "needs-you", "review", "ready")
        ],
        "deploys": state["deploys"],
        "render": {"configured": bool(RENDER_API_KEY), "ok": state["render_ok"]},
        "devin_ok": state["devin_ok"],
        "github_ok": state["github_ok"],
        "generated_at": time.time(),
    }
    state["generated_at"] = time.time()
    state["board"]["sync"] = sync_status()
    _note_board_changed(state["board"])

# ---------------------------------------------------------------- pollers

def _backoff_until(e: Exception, current: float, max_secs: float) -> float:
    """Sleep target after a rate-limit error: what the server said, else doubled."""
    if isinstance(e, RateLimited):
        return min(e.retry_at, time.time() + max_secs)
    if isinstance(e, httpx.HTTPStatusError):
        return min(_retry_at(e.response, current * 2), time.time() + max_secs)
    return time.time() + min(current * 2, max_secs)

async def poll_loop():
    last_gh = 0.0
    last_render = 0.0
    devin_sleep = DEVIN_POLL_SECS
    gh_next = 0.0
    gh_sleep = float(GITHUB_POLL_SECS)
    while True:
        try:
            now = time.time()
            if (now >= gh_next or state["gh_refresh"]) and now >= (state["github_rate"]["retry_at"] or 0):
                state["gh_refresh"] = False
                await fetch_github()
                last_gh = time.time()
                gh_sleep = float(GITHUB_POLL_SECS)
                gh_next = last_gh + gh_sleep
                state["github_rate"]["retry_at"] = None
        except Exception as e:  # noqa: BLE001
            state["github_ok"] = False
            if _is_rate_limited(e):
                gh_next = _backoff_until(e, gh_sleep, GITHUB_POLL_MAX_SECS)
                gh_sleep = min(gh_sleep * 2, GITHUB_POLL_MAX_SECS)
                state["github_rate"]["retry_at"] = gh_next
                log.warning("github rate limited; next attempt in %ds", gh_next - time.time())
            else:
                gh_next = time.time() + gh_sleep
                log.warning("github poll failed: %s", e)
        try:
            if time.time() - last_render >= RENDER_POLL_SECS:
                await fetch_render()
                last_render = time.time()
        except Exception as e:  # noqa: BLE001
            state["render_ok"] = False
            log.warning("render poll failed: %s", e)
        devin_next = time.time() + devin_sleep
        try:
            await fetch_devin()
            # refresh messages for sessions that appear on the board
            for sess in state["sessions"]:
                if tracked_session(sess):
                    try:
                        await fetch_session_messages(sess["session_id"], sess.get("updated_at"))
                    except httpx.HTTPStatusError as e:
                        if _is_rate_limited(e):
                            raise
                    except httpx.HTTPError:
                        pass
            devin_sleep = DEVIN_POLL_SECS
            devin_next = time.time() + devin_sleep
            state["devin_synced_at"] = time.time()
        except Exception as e:  # noqa: BLE001
            state["devin_ok"] = False
            if _is_rate_limited(e):
                devin_next = _backoff_until(e, devin_sleep, DEVIN_POLL_MAX_SECS)
                devin_sleep = min(devin_sleep * 2, DEVIN_POLL_MAX_SECS)
                log.warning("devin rate limited (429); next attempt in %ds", devin_next - time.time())
            else:
                log.warning("devin poll failed: %s", e)
        try:
            await assemble_board()
        except Exception as e:  # noqa: BLE001
            log.exception("board assembly failed: %s", e)
        release_free_memory()
        # sleep until the next Devin tick, unless a webhook / user action wakes us
        wake.clear()
        try:
            await asyncio.wait_for(wake.wait(), timeout=max(0.5, devin_next - time.time()))
        except asyncio.TimeoutError:
            pass

# ---------------------------------------------------------------- live updates

# Every connected browser holds a queue; assemble_board() drops a notice in each
# when the board content changes, so clients re-fetch instantly instead of polling.
subscribers: set[asyncio.Queue] = set()
board_version = 0
_board_digest = ""
SSE_HEARTBEAT_SECS = 15

def sync_status() -> dict:
    now = time.time()
    rate = state["github_rate"]
    return {
        "generated_at": state["generated_at"],
        "version": board_version,
        "devin_ok": state["devin_ok"],
        "github_ok": state["github_ok"],
        "devin_synced_at": state["devin_synced_at"],
        "github_synced_at": state["github_synced_at"],
        "github_rate": {**rate, "retry_in": max(0, int(rate["retry_at"] - now)) if rate["retry_at"] else None},
        "webhook": dict(state["webhook"]),
    }

def publish(kind: str):
    payload = {"type": kind, **sync_status()}
    for q in list(subscribers):
        if q.full():
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
        q.put_nowait(payload)

_DIGEST_SKIP_CARD_KEYS = ("age",)  # display-only, ticks every minute

def _digest_view(board: dict) -> dict:
    out = {k: v for k, v in board.items() if k not in ("generated_at", "sync", "columns")}
    out["columns"] = [
        {**col, "cards": [{k: v for k, v in c.items() if k not in _DIGEST_SKIP_CARD_KEYS} for c in col.get("cards", [])]}
        for col in board.get("columns", [])
    ]
    return out

def _note_board_changed(board: dict):
    global board_version, _board_digest
    digest = hashlib.sha1(json.dumps(_digest_view(board), sort_keys=True, default=str).encode()).hexdigest()
    if digest != _board_digest:
        _board_digest = digest
        board_version += 1
        publish("board")
    else:
        publish("sync")

async def sse_events(request: Request):
    q: asyncio.Queue = asyncio.Queue(maxsize=8)
    subscribers.add(q)
    try:
        yield f"event: hello\ndata: {json.dumps({'type': 'hello', **sync_status()})}\n\n"
        while True:
            if await request.is_disconnected():
                break
            try:
                item = await asyncio.wait_for(q.get(), timeout=SSE_HEARTBEAT_SECS)
                yield f"event: {item['type']}\ndata: {json.dumps(item)}\n\n"
            except asyncio.TimeoutError:
                yield ": ping\n\n"
    finally:
        subscribers.discard(q)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(poll_loop())
    yield
    task.cancel()
    await close_clients()

app = FastAPI(lifespan=lifespan)

# ---------------------------------------------------------------- auth

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if request.url.path.startswith("/api/") and BOARD_TOKEN and request.url.path != "/api/github/webhook":
        token = request.headers.get("x-board-token")
        if token is None and request.url.path.startswith(("/api/attachment/", "/api/events")):
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
                "devin_ok": state["devin_ok"], "github_ok": state["github_ok"], "generated_at": 0, "sync": sync_status()}
    state["board"]["sync"] = sync_status()
    return state["board"]

@app.get("/api/events")
async def events(request: Request):
    """Server-sent events: `board` when the board content changed, `sync` after
    every poll cycle, `hello` on connect. Clients re-fetch /api/board on `board`."""
    return StreamingResponse(sse_events(request), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.post("/api/refresh")
async def refresh_now():
    """Pull GitHub and Devin on the next tick instead of waiting out the interval."""
    rate = state["github_rate"]
    if rate["retry_at"] and rate["retry_at"] > time.time():
        request_refresh(github=False)
        return {"ok": True, "github": "rate_limited", "retry_in": int(rate["retry_at"] - time.time())}
    request_refresh()
    return {"ok": True, "github": "scheduled"}

@app.post("/api/github/webhook")
async def github_webhook(request: Request):
    """GitHub → board push. Verified with X-Hub-Signature-256 (GITHUB_WEBHOOK_SECRET),
    not the board token, since GitHub is the caller."""
    if not GITHUB_WEBHOOK_SECRET:
        raise HTTPException(404, "webhooks not configured")
    body = await request.body()
    if not verify_webhook_signature(GITHUB_WEBHOOK_SECRET, body, request.headers.get("x-hub-signature-256")):
        raise HTTPException(401, "bad signature")
    event = request.headers.get("x-github-event", "")
    try:
        payload = json.loads(body or b"{}")
    except ValueError:
        raise HTTPException(400, "bad json")
    state["webhook"]["last_at"] = time.time()
    state["webhook"]["count"] += 1
    if event == "ping":
        return {"ok": True, "pong": True}
    dirty = apply_webhook(event, payload)
    handled = event in WEBHOOK_PR_EVENTS or event == "issues"
    if handled:
        asyncio.create_task(_after_webhook(dirty))
    return {"ok": True, "handled": handled, "refresh": [f"{r}#{n}" for r, n in sorted(dirty)]}

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
        gh = gh_client()
        p = await gh_get(gh, f"/repos/{repo}/pulls/{number}")
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
    except (httpx.HTTPError, RateLimited):
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
            "activity": session_activity(sess, msgs, (extract_cache.get(sess["session_id"]) or {}).get("data")),
        } if sess else None,
    }

# aiter_raw() forwards the body still encoded, so content-encoding has to travel with it.
ATT_PASS_THROUGH = ("content-type", "content-length", "content-range", "content-encoding", "accept-ranges", "etag", "last-modified", "vary")

@app.get("/api/attachment/{uuid}/{name}")
async def get_attachment(uuid: str, name: str, request: Request):
    """Stream a Devin attachment, passing Range through.

    Video players fetch a recording in dozens of ranged pieces per minute;
    buffering each whole response here is what fills the instance's memory."""
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", uuid) or name in (".", ".."):
        raise HTTPException(400, "bad attachment path")
    fwd = {k: v for k, v in request.headers.items() if k.lower() in ("range", "if-range", "if-none-match", "if-modified-since", "accept-encoding")}
    dv = devin_client()
    req = dv.build_request("GET", f"/attachments/{uuid}/{name}", headers=fwd, timeout=60)
    r = await dv.send(req, stream=True, follow_redirects=True)
    if r.status_code >= 400:
        await r.aclose()
        raise HTTPException(r.status_code, "attachment unavailable")

    async def body():
        try:
            async for chunk in r.aiter_raw():
                yield chunk
        finally:
            await r.aclose()

    headers = {k: v for k, v in r.headers.items() if k.lower() in ATT_PASS_THROUGH}
    headers["Cache-Control"] = "private, max-age=86400"
    headers["Content-Disposition"] = "inline"
    headers.setdefault("accept-ranges", "bytes")
    return StreamingResponse(body(), status_code=r.status_code, headers=headers)

MAX_UPLOAD_BYTES = 10 * 1024 * 1024
DEVIN_ATT_RE = re.compile(r"^https://app\.devin\.ai/attachments/[0-9a-fA-F-]{36}/[^/?#]+$")

def clean_attachments(urls: list[str] | None) -> list[str]:
    out = [u for u in (urls or []) if isinstance(u, str) and DEVIN_ATT_RE.match(u)]
    if len(out) != len(urls or []):
        raise HTTPException(400, "attachments must be Devin attachment URLs")
    return out[:10]

@app.post("/api/upload")
async def upload_attachment(file: UploadFile):
    """Proxy a pasted/dropped file to Devin's attachment store so the browser
    never holds the Devin API key. Returns the app.devin.ai attachment URL."""
    data = await file.read(MAX_UPLOAD_BYTES + 1)
    if not data:
        raise HTTPException(400, "empty file")
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, f"file exceeds {MAX_UPLOAD_BYTES // (1024 * 1024)} MB")
    name = re.sub(r"[^\w.\-]+", "_", file.filename or "") or f"paste-{int(time.time())}.bin"
    dv = devin_client()
    r = await dv.post("/attachments", files={"file": (name, data, file.content_type or "application/octet-stream")}, timeout=60)
    if r.status_code >= 400:
        raise HTTPException(r.status_code, f"Devin API: {r.text[:200]}")
    j = r.json()
    return {"url": j.get("url"), "name": j.get("name") or name}

class MessageIn(BaseModel):
    text: str = ""
    attachments: list[str] = []

@app.post("/api/card/{card_id:path}/message")
async def post_message(card_id: str, body: MessageIn):
    card = find_card(card_id)
    if not card["session_id"]:
        raise HTTPException(400, "card has no session")
    atts = clean_attachments(body.attachments)
    if not body.text.strip() and not atts:
        raise HTTPException(400, "empty message")
    payload = {"message": body.text}
    if atts:
        payload["attachment_urls"] = atts
    dv = devin_client()
    r = await dv.post(f"/sessions/{card['session_id']}/messages", json=payload)
    if r.status_code >= 400:
        raise HTTPException(r.status_code, f"Devin API: {r.text[:200]}")
    echo_user_message(card["session_id"], "\n".join([body.text, *(f'ATTACHMENT:"{u}"' for u in atts)]).strip())
    # the echo also flips the served board card to WORKING right away; a full
    # assembly (which may call the extractor) happens in the background
    cols = {col["id"]: col for col in (state["board"] or {"columns": []})["columns"]}
    if "needs-you" in cols and "working" in cols:
        # same guard as the client: a PR with failing CI / requested changes stays put
        def stuck(c):
            pr = c.get("pr") or {}
            return pr.get("ci") == "failing" or pr.get("review") == "changes_requested"
        moved = [c for c in cols["needs-you"]["cards"] if c["id"] == card_id and not stuck(c)]
        cols["needs-you"]["cards"] = [c for c in cols["needs-you"]["cards"] if c not in moved]
        for c in moved:
            c.update({"col": "working", "tone": "blue", "now": None, "options": [], "question": None})
            cols["working"]["cards"].append(c)
    asyncio.create_task(_assemble_quietly())
    return {"ok": True}

async def _assemble_quietly():
    try:
        await assemble_board()
    except Exception:  # noqa: BLE001
        log.warning("post-message assembly failed", exc_info=True)

class EditIn(BaseModel):
    title: str | None = None
    body: str | None = None

@app.post("/api/card/{card_id:path}/edit")
async def edit_card(card_id: str, body: EditIn):
    """Edit the title and/or body of the GitHub issue or PR behind a card.
    Standalone session cards have no GitHub object; their title is stored on
    the board and shadows the Devin session's own title."""
    patch: dict = {}
    if body.title is not None:
        title = " ".join(body.title.split())
        if not title:
            raise HTTPException(400, "empty title")
        if len(title) > 256:
            raise HTTPException(400, "title too long")
        patch["title"] = title
    card = find_card(card_id)
    if card["kind"] == "session" and card.get("session_id"):
        if body.body is not None:
            raise HTTPException(400, "session cards have no description to edit")
        if "title" not in patch:
            raise HTTPException(400, "nothing to change")
        sid = card["session_id"]
        async with session_titles_lock:
            titles = dict(session_titles)
            titles[sid] = patch["title"]
            try:
                await asyncio.to_thread(save_session_titles, titles, sid)
            except (psycopg.Error, OSError) as e:
                log.warning("could not persist session title", exc_info=True)
                raise HTTPException(500, f"could not save title: {e.__class__.__name__}")
            session_titles[sid] = patch["title"]
        for s in state["sessions"]:
            if s["session_id"] == sid:
                s["title"] = patch["title"]
        if sid in recent_sessions:
            recent_sessions[sid][1]["title"] = patch["title"]
        for col in (state["board"] or {"columns": []})["columns"]:
            for c in col["cards"]:
                if c["id"] == card_id:
                    c["title"] = patch["title"]
        global board_gen
        board_gen += 1
        try:
            await assemble_board()
        except Exception:  # noqa: BLE001
            pass
        return {"ok": True, **patch}
    if card["kind"] not in ("issue", "pr") or not card["repo"]:
        raise HTTPException(400, "only issue, PR and session cards can be edited")
    key = f"{card['repo']}#{card['number']}"
    issue = state["issues"].get(key) or recent_issues.get(key, (0, None))[1] if card["kind"] == "issue" else None
    if body.body is not None:
        text = body.body.replace("\r\n", "\n").strip()
        if len(text) > 60000:
            raise HTTPException(400, "body too long")
        patch["body"] = text
        # keep the prompt marker Devin wrote so the issue stays paired with its session
        if issue and issue.get("prompt_id"):
            patch["body"] = f"{text}\n\n<!-- attention:prompt:{issue['prompt_id']} -->".lstrip()
    if not patch:
        raise HTTPException(400, "nothing to change")
    gh = gh_client()
    path = f"/repos/{card['repo']}/" + ("issues" if card["kind"] == "issue" else "pulls") + f"/{card['number']}"
    r = await gh.patch(path, json=patch)
    if r.status_code >= 400:
        raise HTTPException(r.status_code, f"GitHub: {r.text[:200]}")
    shown = {k: v for k, v in patch.items()}
    if "body" in shown:
        shown["body"] = PROMPT_MARK_RE.sub("", shown["body"])
    if card["kind"] == "issue":
        for store in (state["issues"].get(key), recent_issues.get(key, (0, None))[1]):
            if store:
                store.update(shown)
    else:
        if key in state["prs"]:
            state["prs"][key].update(shown)
        pr_meta_cache.pop(key, None)
    for col in (state["board"] or {"columns": []})["columns"]:
        for c in col["cards"]:
            if c["id"] == card_id:
                c.update({k: v for k, v in shown.items() if k in c})
    board_gen += 1
    try:
        await assemble_board()
    except Exception:  # noqa: BLE001
        pass
    return {"ok": True, **shown}

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
        dv = devin_client()
        r = await dv.post(f"/sessions/{card['session_id']}/archive")
        if r.status_code >= 400:
            raise HTTPException(r.status_code, f"Devin API: {r.text[:200]}")
        actions.append("archived")
    if card["kind"] == "issue":
        gh = gh_client()
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
    global board_gen
    board_gen += 1
    if card["session_id"]:
        recent_sessions.pop(card["session_id"], None)
        recent_replies.pop(card["session_id"], None)
        provisional_titles.pop(card["session_id"], None)
        state["sessions"] = [s for s in state["sessions"] if s["session_id"] != card["session_id"]]
    for col in (state["board"] or {"columns": []})["columns"]:
        col["cards"] = [c for c in col["cards"] if c["id"] != card_id]
    if state["board"]:
        _note_board_changed(state["board"])
    asyncio.create_task(_refresh_after_archive(bool(card["session_id"])))
    return {"ok": True, "actions": actions}

@app.post("/api/card/{card_id:path}/hide")
async def hide_card(card_id: str):
    """Drop the card from the board without touching the Devin session or the
    GitHub issue."""
    global board_gen
    board_gen += 1
    dismissed.add(card_id)
    await asyncio.to_thread(save_dismissed, dismissed)
    for col in (state["board"] or {"columns": []})["columns"]:
        col["cards"] = [c for c in col["cards"] if c["id"] != card_id]
    if state["board"]:
        _note_board_changed(state["board"])
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
    dv = devin_client()
    r = await dv.post("/sessions", json={"prompt": prompt, "tags": [tag], "title": issue["title"]})
    if r.status_code >= 400:
        raise HTTPException(r.status_code, f"Devin API: {r.text[:200]}")
    data = r.json()
    remember_session(data, [tag], issue["title"])
    try:
        await fetch_devin()
        await assemble_board()
    except Exception:  # noqa: BLE001
        pass
    return {"ok": True, "session_id": data.get("session_id"), "url": data.get("url")}

class PromptIn(BaseModel):
    prompt: str
    start: bool = False
    attachments: list[str] = []

@app.post("/api/prompt")
async def prompt_devin(body: PromptIn):
    """The '+' box is a prompt: Devin picks the repo and either files it as a
    GitHub issue (Enter) or just starts working on it, no issue (start=True)."""
    text = body.prompt.strip()
    atts = clean_attachments(body.attachments)
    if not text and not atts:
        raise HTTPException(400, "empty prompt")
    typed = bool(text)
    text = text or "(see attached files)"
    pid = uuid.uuid4().hex[:12]
    mode = "work" if body.start else "file"
    repos = "\n".join(f"- {r}" for r in REPOS)
    attached = ("The user attached files to this task (see the session attachments)." if atts else "")
    if body.start:
        prompt = (
            "A user typed this task into their Attention board and asked you to start on it right away:\n\n"
            f"\"\"\"\n{text}\n\"\"\"\n\n"
            f"1. Pick the right repository from these tracked repos (choose by what the task is about):\n{repos}\n"
            "2. Implement it and open a PR. Do not file a GitHub issue for it; the PR is the tracking artifact. "
            "Do not ask clarifying questions up front; make reasonable assumptions and note them in the PR.\n"
            + ("   Quote the user's request above verbatim in the PR description under `## Original request`.\n" if typed else "")
            + (attached + " Use them as part of the task.\n" if atts else "")
        )
    else:
        prompt = (
            "A user typed this task into their Attention board's new-issue box:\n\n"
            f"\"\"\"\n{text}\n\"\"\"\n\n"
            "Turn it into a GitHub issue:\n"
            f"1. Pick the right repository from these tracked repos (choose by what the task is about):\n{repos}\n"
            "2. Write a clear title and description that preserves the user's intent. Follow that repo's issue "
            "conventions and any knowledge you have (title prefixes, labels, sections). Do not ask clarifying "
            "questions; make reasonable assumptions and note them in the issue.\n"
            + ("   The issue body MUST also include the user's original prompt above, verbatim and unedited, under a "
               "heading `## Original request` (quoted). Your summary may miss details; the verbatim prompt is the "
               "source of truth for whoever implements the issue.\n" if typed else "")
            + f"3. The issue body MUST end with this exact line, unchanged: <!-- attention:prompt:{pid} -->\n"
            "4. Create the issue in that repo and reply with just its URL.\n"
            + (attached + " Embed the image attachments in the issue body and link any others, so they are visible on GitHub.\n" if atts else "")
            + "Do not start implementing it; filing the issue is the whole task.\n"
        )
    title = _short(text.splitlines()[0] if text.splitlines() else text, 70) or "New task"
    dv = devin_client()
    payload = {"prompt": prompt, "tags": [f"prompt:{pid}", f"prompt-mode:{mode}"]}
    if not body.start:
        payload["title"] = title  # work sessions keep Devin's own title once it assigns one
    if atts:
        payload["attachment_urls"] = atts
    r = await dv.post("/sessions", json=payload)
    if r.status_code >= 400:
        raise HTTPException(r.status_code, f"Devin API: {r.text[:200]}")
    data = r.json()
    remember_session(data, payload["tags"], title)
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
    gh = gh_client()
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
    gh = gh_client()
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
