import asyncio
import hashlib
import hmac
import json
import os
import time

os.environ.update(
    BOARD_TOKEN="tok",
    GITHUB_WEBHOOK_SECRET="whsec",
    REPOS="acme/one,acme/two",
    DATABASE_URL="",
    DISMISSED_FILE="/tmp/attentionhq-test-dismissed.json",
    BOARDS_FILE="/tmp/attentionhq-test-boards.json",
    PROMPTS_FILE="/tmp/attentionhq-test-prompts.json",
)

import httpx  # noqa: E402
import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import server  # noqa: E402


def sign(body: bytes, secret: str = "whsec") -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def pr_payload(number=7, state="open", sha="abc", repo="acme/one", action="opened"):
    return {
        "action": action,
        "number": number,
        "repository": {"full_name": repo},
        "pull_request": {
            "number": number, "state": state, "title": f"PR {number}", "body": "", "draft": False,
            "html_url": f"https://github.com/{repo}/pull/{number}",
            "head": {"ref": "feat", "sha": sha}, "created_at": "2026-01-01T00:00:00Z",
        },
    }


def issue_payload(number=3, state="open", repo="acme/one"):
    return {
        "action": "opened",
        "repository": {"full_name": repo},
        "issue": {
            "number": number, "state": state, "title": f"Issue {number}", "body": "hi",
            "html_url": f"https://github.com/{repo}/issues/{number}",
            "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z",
            "labels": [], "assignees": [], "comments": 0, "user": {"login": "me"},
        },
    }


@pytest.fixture(autouse=True)
def clean_state(monkeypatch):
    server.state["issues"].clear()
    server.state["prs"].clear()
    server.state["github_rate"].update(limit=None, remaining=None, reset=None, retry_at=None)
    server.state["webhook"].update(last_at=0, count=0)
    server.etag_cache.clear()
    server.recent_issues.clear()
    server.recently_closed.clear()
    server.subscribers.clear()
    # webhooks would otherwise hit GitHub for the targeted PR re-read
    async def noop(dirty):
        return None
    monkeypatch.setattr(server, "_after_webhook", noop)
    yield


@pytest.fixture
def client():
    return TestClient(server.app)


# ------------------------------------------------------------------ webhook auth

def test_webhook_signature_roundtrip():
    body = b'{"a":1}'
    assert server.verify_webhook_signature("s", body, sign(body, "s"))
    assert not server.verify_webhook_signature("s", body, sign(body, "other"))
    assert not server.verify_webhook_signature("s", body, None)
    assert not server.verify_webhook_signature("s", body, "sha1=deadbeef")
    assert not server.verify_webhook_signature("", body, sign(body, ""))


def test_webhook_rejects_bad_or_missing_signature(client):
    body = json.dumps(pr_payload()).encode()
    assert client.post("/api/github/webhook", content=body, headers={"x-github-event": "pull_request"}).status_code == 401
    r = client.post("/api/github/webhook", content=body,
                    headers={"x-github-event": "pull_request", "x-hub-signature-256": sign(body, "nope")})
    assert r.status_code == 401
    # the board token is not a substitute for the GitHub signature
    r = client.post("/api/github/webhook", content=body,
                    headers={"x-github-event": "pull_request", "x-board-token": "tok"})
    assert r.status_code == 401
    assert server.state["webhook"]["count"] == 0


def test_webhook_ping(client):
    body = b'{"zen":"keep it logically awesome"}'
    r = client.post("/api/github/webhook", content=body,
                    headers={"x-github-event": "ping", "x-hub-signature-256": sign(body)})
    assert r.json() == {"ok": True, "pong": True}
    assert server.state["webhook"]["count"] == 1


def test_webhook_requires_board_token_elsewhere(client):
    assert client.get("/api/board").status_code == 401
    assert client.get("/api/board", headers={"x-board-token": "tok"}).status_code == 200
    assert client.get("/api/events").status_code == 401
    assert client.post("/api/refresh").status_code == 401


# ------------------------------------------------------------------ webhook events

def test_pull_request_event_updates_board_and_marks_dirty(client):
    body = json.dumps(pr_payload()).encode()
    r = client.post("/api/github/webhook", content=body,
                    headers={"x-github-event": "pull_request", "x-hub-signature-256": sign(body)})
    assert r.status_code == 200
    assert r.json()["handled"] is True
    assert r.json()["refresh"] == ["acme/one#7"]
    pr = server.state["prs"]["acme/one#7"]
    assert pr["title"] == "PR 7" and pr["head_sha"] == "abc" and pr["ci"] == "unknown"

    # closing removes it
    body = json.dumps(pr_payload(state="closed", action="closed")).encode()
    client.post("/api/github/webhook", content=body,
                headers={"x-github-event": "pull_request", "x-hub-signature-256": sign(body)})
    assert "acme/one#7" not in server.state["prs"]


def test_pull_request_synchronize_resets_ci_only_on_new_sha():
    server.state["prs"]["acme/one#7"] = {"repo": "acme/one", "number": 7, "head_sha": "abc", "ci": "passing", "review": "approved"}
    server.apply_webhook("pull_request", pr_payload(sha="abc", action="edited"))
    assert server.state["prs"]["acme/one#7"]["ci"] == "passing"
    assert server.state["prs"]["acme/one#7"]["review"] == "approved"
    server.apply_webhook("pull_request", pr_payload(sha="def", action="synchronize"))
    assert server.state["prs"]["acme/one#7"]["ci"] == "unknown"


def test_issue_event_adds_and_removes():
    assert server.apply_webhook("issues", issue_payload()) == set()
    assert server.state["issues"]["acme/one#3"]["title"] == "Issue 3"
    server.apply_webhook("issues", issue_payload(state="closed"))
    assert "acme/one#3" not in server.state["issues"]


def test_issue_event_skips_prs_and_recently_closed():
    p = issue_payload()
    p["issue"]["pull_request"] = {"url": "x"}
    server.apply_webhook("issues", p)
    assert server.state["issues"] == {}
    server.recently_closed["acme/one#3"] = time.time()
    server.apply_webhook("issues", issue_payload())
    assert server.state["issues"] == {}


def test_untracked_repo_ignored():
    assert server.apply_webhook("pull_request", pr_payload(repo="someone/else")) == set()
    assert server.state["prs"] == {}


def test_check_and_status_events_map_sha_to_prs():
    server.state["prs"]["acme/one#7"] = {"repo": "acme/one", "number": 7, "head_sha": "abc"}
    server.state["prs"]["acme/two#8"] = {"repo": "acme/two", "number": 8, "head_sha": "abc"}
    dirty = server.apply_webhook("check_run", {"repository": {"full_name": "acme/one"}, "check_run": {"head_sha": "abc", "pull_requests": []}})
    assert dirty == {("acme/one", 7)}
    dirty = server.apply_webhook("status", {"repository": {"full_name": "acme/two"}, "sha": "abc"})
    assert dirty == {("acme/two", 8)}
    dirty = server.apply_webhook("workflow_run", {"repository": {"full_name": "acme/one"}, "workflow_run": {"head_sha": "zzz", "pull_requests": [{"number": 7}]}})
    assert dirty == {("acme/one", 7)}
    dirty = server.apply_webhook("pull_request_review", {"repository": {"full_name": "acme/one"}, "pull_request": {"number": 7}})
    assert dirty == {("acme/one", 7)}


def test_unknown_event_not_handled(client):
    body = b'{"repository":{"full_name":"acme/one"}}'
    r = client.post("/api/github/webhook", content=body,
                    headers={"x-github-event": "star", "x-hub-signature-256": sign(body)})
    assert r.json()["handled"] is False


# ------------------------------------------------------------------ etag + rate limit

def make_gh(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://api.github.com")


def test_gh_get_uses_etag_and_304():
    calls = []

    def handler(req):
        calls.append(dict(req.headers))
        if req.headers.get("if-none-match") == '"v1"':
            return httpx.Response(304, headers={"etag": '"v1"', "x-ratelimit-remaining": "99"})
        return httpx.Response(200, json={"n": 1}, headers={"etag": '"v1"', "x-ratelimit-limit": "5000", "x-ratelimit-remaining": "100", "x-ratelimit-reset": "123"})

    async def run():
        async with make_gh(handler) as gh:
            a = await server.gh_get(gh, "/repos/acme/one/pulls", {"state": "open"})
            b = await server.gh_get(gh, "/repos/acme/one/pulls", {"state": "open"})
            return a, b

    a, b = asyncio.run(run())
    assert a == b == {"n": 1}
    assert "if-none-match" not in calls[0]
    assert calls[1]["if-none-match"] == '"v1"'
    assert server.state["github_rate"]["remaining"] == 99
    assert server.state["github_rate"]["limit"] == 5000


def test_gh_get_raises_rate_limited_with_reset():
    reset = int(time.time()) + 900

    def handler(req):
        return httpx.Response(403, json={"message": "API rate limit exceeded"},
                              headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(reset)})

    async def run():
        async with make_gh(handler) as gh:
            await server.gh_get(gh, "/x")

    with pytest.raises(server.RateLimited) as ei:
        asyncio.run(run())
    assert ei.value.retry_at == reset + 1


def test_gh_get_honours_retry_after():
    def handler(req):
        return httpx.Response(429, headers={"retry-after": "42"})

    async def run():
        async with make_gh(handler) as gh:
            await server.gh_get(gh, "/x")

    before = time.time()
    with pytest.raises(server.RateLimited) as ei:
        asyncio.run(run())
    assert 41 <= ei.value.retry_at - before <= 44


def test_plain_403_is_not_rate_limit():
    def handler(req):
        return httpx.Response(403, json={"message": "forbidden"})

    async def run():
        async with make_gh(handler) as gh:
            await server.gh_get(gh, "/x")

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(run())


def test_budget_reserve_blocks_polling():
    server.state["github_rate"].update(remaining=server.GITHUB_RATE_RESERVE - 1, reset=int(time.time()) + 600)
    assert server._budget_exhausted()
    server.state["github_rate"].update(reset=int(time.time()) - 1)
    assert not server._budget_exhausted()
    server.state["github_rate"].update(remaining=server.GITHUB_RATE_RESERVE + 1, reset=int(time.time()) + 600)
    assert not server._budget_exhausted()


def test_refresh_endpoint_reports_rate_limit(client):
    r = client.post("/api/refresh", headers={"x-board-token": "tok"})
    assert r.json()["github"] == "scheduled"
    assert server.state["gh_refresh"] is True
    server.state["github_rate"]["retry_at"] = time.time() + 120
    r = client.post("/api/refresh", headers={"x-board-token": "tok"})
    assert r.json()["github"] == "rate_limited"
    assert 118 <= r.json()["retry_in"] <= 120


def test_review_state():
    assert server.review_state([]) == "none"
    assert server.review_state([{"user": {"login": "a"}, "state": "APPROVED"}]) == "approved"
    assert server.review_state([
        {"user": {"login": "a"}, "state": "APPROVED"},
        {"user": {"login": "b"}, "state": "CHANGES_REQUESTED"},
    ]) == "changes_requested"
    # later approval by the same reviewer supersedes their earlier request
    assert server.review_state([
        {"user": {"login": "b"}, "state": "CHANGES_REQUESTED"},
        {"user": {"login": "b"}, "state": "APPROVED"},
    ]) == "approved"
    assert server.review_state([{"user": {"login": "a"}, "state": "COMMENTED"}]) == "none"


# ------------------------------------------------------------------ board / sse

def test_board_carries_sync_and_version_bumps_on_change(client):
    server.state["github_rate"].update(limit=5000, remaining=10, reset=int(time.time()) + 60)
    v0 = server.board_version
    asyncio.run(server.assemble_board())
    b = client.get("/api/board", headers={"x-board-token": "tok"}).json()
    assert b["sync"]["github_rate"]["remaining"] == 10
    assert b["sync"]["webhook"]["configured"] is True
    # same content: version unchanged, a 'sync' event still goes out
    q: asyncio.Queue = asyncio.Queue(maxsize=8)
    server.subscribers.add(q)
    asyncio.run(server.assemble_board())
    assert server.board_version == v0 + 1  # bumped once by the first assembly above
    assert q.get_nowait()["type"] == "sync"
    server.apply_webhook("issues", issue_payload())
    asyncio.run(server.assemble_board())
    assert server.board_version == v0 + 2
    assert q.get_nowait()["type"] == "board"
    assert client.get("/api/board", headers={"x-board-token": "tok"}).json()["sync"]["version"] == server.board_version


def test_publish_drops_oldest_when_subscriber_is_slow():
    q: asyncio.Queue = asyncio.Queue(maxsize=2)
    server.subscribers.add(q)
    for _ in range(5):
        server.publish("sync")
    assert q.qsize() == 2


def test_sse_stream_sends_hello_then_events_then_heartbeat(monkeypatch):
    monkeypatch.setattr(server, "SSE_HEARTBEAT_SECS", 0.05)

    class Req:
        n = 0
        async def is_disconnected(self):
            self.n += 1
            return self.n > 3

    async def run():
        frames = []
        gen = server.sse_events(Req())
        frames.append(await gen.__anext__())
        assert len(server.subscribers) == 1
        server.publish("board")
        frames.append(await gen.__anext__())
        frames.append(await gen.__anext__())  # nothing queued -> heartbeat
        async for f in gen:
            frames.append(f)
        return frames

    frames = asyncio.run(run())
    assert frames[0].startswith("event: hello\ndata: ")
    hello = json.loads(frames[0].split("data: ", 1)[1])
    assert hello["type"] == "hello" and "github_rate" in hello and "version" in hello
    assert frames[1].startswith("event: board\n")
    assert frames[2] == ": ping\n\n"
    assert server.subscribers == set()


def test_webhook_schedules_targeted_refresh_not_full_poll(client, monkeypatch):
    seen = []

    async def record(dirty):
        seen.append(dirty)
    monkeypatch.setattr(server, "_after_webhook", record)
    server.wake.clear()
    server.state["gh_refresh"] = False
    body = json.dumps(pr_payload()).encode()
    client.post("/api/github/webhook", content=body,
                headers={"x-github-event": "pull_request", "x-hub-signature-256": sign(body)})
    assert seen == [{("acme/one", 7)}]
    assert server.state["gh_refresh"] is False  # no full reconcile burned on a webhook


def test_manual_refresh_wakes_poll_loop(client):
    server.wake.clear()
    client.post("/api/refresh", headers={"x-board-token": "tok"})
    assert server.wake.is_set() and server.state["gh_refresh"] is True


def test_reopened_issue_clears_recently_closed():
    server.recently_closed["acme/one#3"] = time.time()
    p = issue_payload()
    p["action"] = "reopened"
    server.apply_webhook("issues", p)
    assert "acme/one#3" in server.state["issues"]
    assert "acme/one#3" not in server.recently_closed


def test_reconcile_does_not_clobber_webhook_applied_during_fetch(monkeypatch):
    server.state["issues"]["acme/one#1"] = {"repo": "acme/one", "number": 1, "title": "stale"}
    server._webhook_touched.clear()

    async def fake_repo(gh, repo):
        if repo == "acme/one":
            # webhook lands mid-fetch: closes #1, opens #3
            server.apply_webhook("issues", issue_payload(number=1, state="closed"))
            server.apply_webhook("issues", issue_payload(number=3))
            return {"acme/one#1": {"repo": "acme/one", "number": 1, "title": "from github (old)"}}, {}
        return {}, {}

    monkeypatch.setattr(server, "fetch_github_repo", fake_repo)
    monkeypatch.setattr(server, "gh_client", lambda: None)
    asyncio.run(server.fetch_github())
    assert "acme/one#1" not in server.state["issues"]
    assert server.state["issues"]["acme/one#3"]["title"] == "Issue 3"


def test_reconcile_stops_at_reserve_mid_run(monkeypatch):
    server.state["github_rate"].update(remaining=server.GITHUB_RATE_RESERVE + 5, reset=int(time.time()) + 600)
    calls = []

    def handler(req):
        calls.append(req.url.path)
        server.state["github_rate"]["remaining"] = server.GITHUB_RATE_RESERVE - 1
        if req.url.path.endswith("/issues"):
            return httpx.Response(200, json=[])
        return httpx.Response(200, json=[{"number": 1, "title": "p", "html_url": "u", "head": {"ref": "b", "sha": "s"}, "created_at": "2026-01-01T00:00:00Z"}])

    async def run():
        server._background_sync.set(True)
        async with make_gh(handler) as gh:
            await server.fetch_github_repo(gh, "acme/one")

    with pytest.raises(server.RateLimited):
        asyncio.run(run())
    # the first response dropped us under the reserve; nothing else was spent
    assert calls == ["/repos/acme/one/issues"]


def test_user_requests_are_not_budget_gated():
    server.state["github_rate"].update(remaining=1, reset=int(time.time()) + 600)

    async def run():
        async with make_gh(lambda req: httpx.Response(200, json={"ok": 1})) as gh:
            return await server.gh_get(gh, "/repos/acme/one/pulls/1")

    assert asyncio.run(run()) == {"ok": 1}


def test_fetch_github_is_serialized(monkeypatch):
    active = {"n": 0, "max": 0}

    async def fake_repo(gh, repo):
        active["n"] += 1
        active["max"] = max(active["max"], active["n"])
        await asyncio.sleep(0.01)
        active["n"] -= 1
        return {}, {}

    monkeypatch.setattr(server, "fetch_github_repo", fake_repo)
    monkeypatch.setattr(server, "gh_client", lambda: None)

    async def run():
        await asyncio.gather(server.fetch_github(), server.fetch_github())

    asyncio.run(run())
    assert active["max"] == 1


def test_board_digest_ignores_card_age():
    board = {"columns": [{"id": "issues", "cards": [{"id": "x", "title": "t", "age": "3m"}]}], "generated_at": 1, "sync": {}}
    server._note_board_changed(board)
    v = server.board_version
    board["columns"][0]["cards"][0]["age"] = "4m"
    board["generated_at"] = 2
    server._note_board_changed(board)
    assert server.board_version == v
    board["columns"][0]["cards"][0]["title"] = "changed"
    server._note_board_changed(board)
    assert server.board_version == v + 1


def test_deleted_issue_removed_even_if_payload_says_open():
    server.apply_webhook("issues", issue_payload())
    assert "acme/one#3" in server.state["issues"]
    p = issue_payload()
    p["action"] = "deleted"
    server.apply_webhook("issues", p)
    assert "acme/one#3" not in server.state["issues"]


def test_refresh_pr_write_survives_overlapping_reconcile(monkeypatch):
    server.state["prs"]["acme/one#1"] = {"repo": "acme/one", "number": 1, "review": "none"}
    server._webhook_touched.clear()

    async def fake_refresh():
        # a review webhook's targeted re-read commits while the reconcile is mid-fetch
        server._webhook_touched.add("acme/one#1")
        server.state["prs"]["acme/one#1"] = {"repo": "acme/one", "number": 1, "review": "approved"}

    async def fake_repo(gh, repo):
        if repo == "acme/one":
            await fake_refresh()
            return {}, {"acme/one#1": {"repo": "acme/one", "number": 1, "review": "none"}}
        return {}, {}

    monkeypatch.setattr(server, "fetch_github_repo", fake_repo)
    monkeypatch.setattr(server, "gh_client", lambda: None)
    asyncio.run(server.fetch_github())
    assert server.state["prs"]["acme/one#1"]["review"] == "approved"


def test_refresh_pr_marks_key_touched(monkeypatch):
    server._webhook_touched.clear()
    server.state["prs"].clear()

    def handler(req):
        p = req.url.path
        if p == "/repos/acme/one/pulls/7":
            return httpx.Response(200, json={"number": 7, "state": "open", "title": "t", "html_url": "u", "mergeable_state": "clean",
                                             "head": {"ref": "b", "sha": "s"}, "created_at": "2026-01-01T00:00:00Z"})
        if p.endswith("/check-runs"):
            return httpx.Response(200, json={"check_runs": []})
        return httpx.Response(200, json=[])

    monkeypatch.setattr(server, "gh_client", lambda: make_gh(handler))
    asyncio.run(server.refresh_pr("acme/one", 7))
    assert "acme/one#7" in server._webhook_touched
    assert server.state["prs"]["acme/one#7"]["review"] == "none"


def test_archive_publishes_board_change_before_devin_refresh(monkeypatch, client):
    server.state["board"] = {"columns": [{"id": "c", "cards": [{"id": "session:abc", "kind": "session", "session_id": "abc", "repo": None, "number": None}]}]}
    server._note_board_changed(server.state["board"])
    before = server.board_version

    class FakeDevin:
        async def post(self, path):
            return httpx.Response(200, json={})

    async def failing_refresh(had_session):
        raise RuntimeError("devin down")

    monkeypatch.setattr(server, "devin_client", lambda: FakeDevin())
    monkeypatch.setattr(server, "_refresh_after_archive", failing_refresh)
    r = client.post("/api/card/session:abc/archive", headers={"x-board-token": "tok"})
    assert r.status_code == 200
    assert server.board_version == before + 1
    assert server.state["board"]["columns"][0]["cards"] == []


def test_board_color_roundtrip_and_validation(monkeypatch, client):
    async def noop():
        return None
    monkeypatch.setattr(server, "_refresh_after_board_change", noop)
    h = {"x-board-token": "tok"}
    r = client.post("/api/boards", json={"name": "Tinted", "repos": ["acme/one"], "color": "teal"}, headers=h)
    assert r.status_code == 200
    bid = r.json()["board"]["id"]
    assert r.json()["board"]["color"] == "teal"
    assert client.put(f"/api/boards/{bid}", json={"name": "Tinted", "repos": ["acme/one"], "color": "neon"}, headers=h).status_code == 400
    assert server.board_by_id(bid)["color"] == "teal"
    # omitted color keeps the stored one; "" clears it
    r = client.put(f"/api/boards/{bid}", json={"name": "Tinted!", "repos": ["acme/one"]}, headers=h)
    assert r.json()["board"]["color"] == "teal" and r.json()["board"]["name"] == "Tinted!"
    r = client.put(f"/api/boards/{bid}", json={"name": "Tinted", "repos": ["acme/one"], "color": ""}, headers=h)
    assert r.json()["board"]["color"] == ""
    assert server.load_boards()[-1]["color"] == ""
    client.delete(f"/api/boards/{bid}", headers=h)
