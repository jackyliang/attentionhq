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
    SETTINGS_FILE="/tmp/attentionhq-test-settings.json",
)

import httpx  # noqa: E402
import psycopg  # noqa: E402
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


def test_board_reorder(monkeypatch, client):
    async def noop():
        return None
    monkeypatch.setattr(server, "_refresh_after_board_change", noop)
    h = {"x-board-token": "tok"}
    ids = [client.post("/api/boards", json={"name": n, "repos": ["acme/x"]}, headers=h).json()["board"]["id"] for n in ("Ra", "Rb", "Rc")]
    before = [b["id"] for b in server.state["boards"]]
    rest = [i for i in before if i not in ids]
    want = rest + ids[::-1]
    r = client.put("/api/board-order", json={"order": want}, headers=h)
    assert r.status_code == 200
    assert [b["id"] for b in r.json()["boards"]] == want
    assert [b["id"] for b in server.load_boards()] == want
    # must be a permutation: missing, duplicated or unknown ids are rejected and nothing moves
    for bad in (want[:-1], want + [want[0]], want[:-1] + ["nope"]):
        assert client.put("/api/board-order", json={"order": bad}, headers=h).status_code == 400
    assert [b["id"] for b in server.state["boards"]] == want
    # a storage failure rolls the in-memory order back
    def boom(*_):
        raise server.BoardStoreError("disk full")
    monkeypatch.setattr(server, "persist_boards", boom)
    assert client.put("/api/board-order", json={"order": before}, headers=h).status_code == 503
    assert [b["id"] for b in server.state["boards"]] == want
    monkeypatch.undo()
    monkeypatch.setattr(server, "_refresh_after_board_change", noop)
    # a board that happens to be called "order" is still editable at /api/boards/order
    assert client.post("/api/boards", json={"name": "Order", "repos": []}, headers=h).json()["board"]["id"] == "order"
    assert client.put("/api/boards/order", json={"name": "Ordered", "repos": []}, headers=h).json()["board"]["name"] == "Ordered"
    for i in ids + ["order"]:
        client.delete(f"/api/boards/{i}", headers=h)


def test_settings_roundtrip(client):
    h = {"x-board-token": "tok"}
    server.settings.clear()
    server.settings.update(server.DEFAULT_SETTINGS)
    assert client.get("/api/settings", headers=h).json() == {"settings": {"show_all": False}}
    assert client.put("/api/settings", json={"show_all": True}, headers=h).json()["settings"]["show_all"] is True
    assert server.load_settings() == {"show_all": True}
    server.state["board"] = None
    assert client.get("/api/board", headers=h).json()["settings"] == {"show_all": True}
    # partial / unknown keys leave the rest alone
    assert client.put("/api/settings", json={"bogus": 1}, headers=h).json()["settings"] == {"show_all": True}
    assert client.put("/api/settings", json={"show_all": False}, headers=h).json()["settings"]["show_all"] is False


def test_settings_recover_after_storage_outage(monkeypatch, client):
    h = {"x-board-token": "tok"}
    monkeypatch.setattr(server, "settings_loaded", False)
    server.settings.clear()
    server.settings.update(server.DEFAULT_SETTINGS)

    def down():
        raise psycopg.OperationalError("db down")
    monkeypatch.setattr(server, "load_settings", down)
    # defaults are served, writes are refused rather than overwriting an unseen stored choice
    assert client.get("/api/settings", headers=h).json()["settings"]["show_all"] is False
    assert client.put("/api/settings", json={"show_all": True}, headers=h).status_code == 503
    assert asyncio.run(server.reload_settings()) is False
    assert server.settings_loaded is False

    # the poll loop's retry adopts the stored value once the db answers
    monkeypatch.setattr(server, "load_settings", lambda: {"show_all": True})
    assert asyncio.run(server.reload_settings()) is True
    assert server.settings_loaded is True
    assert client.get("/api/settings", headers=h).json()["settings"]["show_all"] is True


# ------------------------------------------------------------------ ACP stream

@pytest.fixture
def acp_session():
    sess = {"session_id": "s1", "title": "t", "tags": [], "status": "running", "status_detail": "",
            "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z", "pull_requests": []}
    server.state["sessions"][:] = [sess]
    server.session_msgs_cache.pop("s1", None)
    server.state["acp"].update(connected=False, last_at=0, error=None, attached=0, events=0)
    server.state["devin_refresh"] = False
    server._acp_listed.clear()
    yield sess
    server.state["sessions"].clear()
    server.session_msgs_cache.pop("s1", None)


def test_acp_endpoints_require_token(client):
    assert client.post("/api/acp/hello", json={"connected": True}).status_code == 401
    assert client.post("/api/acp/events", json={"sessions": {}}).status_code == 401


def test_acp_hello_marks_stream_live(client, acp_session):
    h = {"x-board-token": "tok"}
    assert server.acp_live() is False
    r = client.post("/api/acp/hello", json={"connected": True, "attached": 3}, headers=h)
    assert r.status_code == 200
    assert server.acp_live() is True
    assert server.state["acp"]["attached"] == 3
    assert server.sync_status()["acp"]["live"] is True
    client.post("/api/acp/hello", json={"connected": False, "error": "HTTP 403"}, headers=h)
    assert server.acp_live() is False
    assert server.sync_status()["acp"]["error"] == "HTTP 403"


def test_acp_streamed_message_coalesces_and_finalizes(client, acp_session):
    h = {"x-board-token": "tok"}
    q = asyncio.Queue()
    server.subscribers.add(q)
    body = {"sessions": {"s1": [
        {"type": "message", "message_id": "m1", "text": "Hel", "ts": "2026-01-01T00:00:01Z", "event_id": "e1"},
        {"type": "message", "message_id": "m1", "text": "lo", "ts": "2026-01-01T00:00:02Z", "event_id": "e1"},
    ]}}
    assert client.post("/api/acp/events", json=body, headers=h).status_code == 200
    th = server.session_thread("s1")
    assert [(m["who"], m["text"], m["streaming"]) for m in th] == [("devin", "Hello", True)]
    # a streaming message is not fed to extraction / board assembly yet
    assert server.session_thread("s1", include_streaming=False) == []
    ev = q.get_nowait()
    assert ev["type"] == "thread" and ev["session_id"] == "s1"
    # whole-message overwrite replaces the text; typing=false finalizes it
    body = {"sessions": {"s1": [
        {"type": "message", "message_id": "m1", "text": "Hello world", "overwrite": True, "event_id": "e1"},
        {"type": "typing", "typing": False},
    ]}}
    client.post("/api/acp/events", json=body, headers=h)
    th = server.session_thread("s1", include_streaming=False)
    assert [(m["text"], m["streaming"]) for m in th] == [("Hello world", False)]
    assert "event_id" not in th[0] and "at" not in th[0]
    # duplicate chunks for an already-final message id do not duplicate the entry
    client.post("/api/acp/events", json={"sessions": {"s1": [{"type": "message", "message_id": "m1", "text": "!", "event_id": "e1"}]}}, headers=h)
    assert len(server.session_thread("s1")) == 1
    # an aborted message disappears
    client.post("/api/acp/events", json={"sessions": {"s1": [
        {"type": "message", "message_id": "m2", "text": "oops"},
        {"type": "message", "message_id": "m2", "aborted": True},
    ]}}, headers=h)
    assert len(server.session_thread("s1")) == 1


def test_acp_status_finalizes_stream_and_notifies_thread(client, acp_session):
    h = {"x-board-token": "tok"}
    q = asyncio.Queue()
    server.subscribers.add(q)
    client.post("/api/acp/events", json={"sessions": {"s1": [{"type": "message", "message_id": "m1", "text": "Done."}]}}, headers=h)
    q.get_nowait()
    # a status change with no board effect still has to clear the caret on open cards
    client.post("/api/acp/events", json={"sessions": {"s1": [{"type": "status", "status": "working"}]}}, headers=h)
    assert server.session_thread("s1")[0]["streaming"] is False
    assert q.get_nowait()["type"] == "thread"


def test_acp_retried_batch_is_applied_once(client, acp_session):
    h = {"x-board-token": "tok"}
    batch = {"run": "r1", "seq": 7, "sessions": {"s1": [{"type": "message", "message_id": "m1", "text": "Hi"}]}}
    assert client.post("/api/acp/events", json=batch, headers=h).json() == {"ok": True}
    assert client.post("/api/acp/events", json=batch, headers=h).json() == {"ok": True, "duplicate": True}
    assert server.session_thread("s1")[0]["text"] == "Hi"
    # same seq from a restarted bridge is a different batch
    batch["run"] = "r2"
    client.post("/api/acp/events", json=batch, headers=h)
    assert server.session_thread("s1")[0]["text"] == "HiHi"
    # the bridge dropping events forces a REST reconcile
    assert server.state["devin_refresh"] is False
    client.post("/api/acp/hello", json={"connected": True, "lost": True}, headers=h)
    assert server.state["devin_refresh"] is True


def test_acp_live_messages_yield_to_rest_transcript(acp_session):
    cached = server.session_msgs_cache.setdefault("s1", {"msgs": [], "cursor": None, "seen": set()})
    server.apply_acp_events("s1", [
        {"type": "message", "message_id": "m1", "text": "Done.", "event_id": "e-devin"},
        {"type": "user_message", "text": "thanks", "event_id": "e-user"},
    ])
    assert len(server.session_thread("s1")) == 2
    # REST returned the devin message (same event id) and the user one (same text)
    cached["msgs"].append({"who": "devin", "ts": "2026-01-01T00:00:03Z", "text": "Done.", "origin": None, "name": None})
    cached["seen"].add("e-devin")
    cached["msgs"].append({"who": "user", "ts": "2026-01-01T00:00:04Z", "text": "thanks", "origin": "slack", "name": "j"})
    server._reconcile_live(cached)
    th = server.session_thread("s1")
    assert [(m["who"], m["text"]) for m in th] == [("devin", "Done."), ("user", "thanks")]
    assert cached["live"] == {}


def test_acp_user_message_replaces_local_echo(acp_session):
    server.echo_user_message("s1", "fix it")
    assert [m.get("local") is not None for m in server.session_thread("s1")] == [True]
    server.apply_acp_events("s1", [{"type": "user_message", "text": "fix it", "event_id": "u1", "ts": "2026-01-01T00:00:05Z"}])
    th = server.session_thread("s1")
    assert len(th) == 1 and th[0]["who"] == "user" and "local" not in th[0]


def test_acp_status_events_update_session_and_schedule_reconcile(client, acp_session):
    h = {"x-board-token": "tok"}
    client.post("/api/acp/events", json={"sessions": {"s1": [{"type": "status", "status": "blocked", "snapshot": True}]}}, headers=h)
    assert acp_session["status"] == "running" and acp_session["status_detail"] == "waiting_for_user"
    assert server.session_needs_user(acp_session)
    assert server.state["devin_refresh"] is False  # the attach snapshot alone does not trigger REST
    client.post("/api/acp/events", json={"sessions": {"s1": [{"type": "status", "status": "working"}]}}, headers=h)
    assert acp_session["status"] == "running" and acp_session["status_detail"] == ""
    assert server.state["devin_refresh"] is True
    client.post("/api/acp/events", json={"sessions": {"s1": [{"type": "status", "status": "finished", "outcome": "suspended"}]}}, headers=h)
    assert acp_session["status"] == "suspended"
    client.post("/api/acp/events", json={"sessions": {"s1": [{"type": "lifecycle", "lifecycle": "finished", "status": "finished"}]}}, headers=h)
    assert acp_session["status"] == "finished"


def test_acp_session_list_selects_watch_and_flags_changes(client, acp_session):
    h = {"x-board-token": "tok"}
    listed = {"sessions": [{"id": "s1", "updated_at": "2026-01-01T00:00:00Z", "created_at": "2026-01-01T00:00:00Z"}]}
    r = client.post("/api/acp/sessions", json=listed, headers=h)
    assert r.status_code == 200 and r.json()["watch"] == ["s1"]
    assert server.state["devin_refresh"] is False  # nothing moved since our REST read
    assert server.acp_live() is True
    listed["sessions"][0]["updated_at"] = "2026-01-01T00:05:00Z"
    client.post("/api/acp/sessions", json=listed, headers=h)
    assert server.state["devin_refresh"] is True
    server.state["devin_refresh"] = False
    client.post("/api/acp/sessions", json=listed, headers=h)
    assert server.state["devin_refresh"] is False  # same updated_at again: reported once
    # a session we have never seen is a reason to re-read REST; an archived one is not
    listed["sessions"].append({"id": "s2", "created_at": server._now_iso(), "archived": True})
    client.post("/api/acp/sessions", json=listed, headers=h)
    assert server.state["devin_refresh"] is False
    listed["sessions"][-1]["archived"] = False
    client.post("/api/acp/sessions", json=listed, headers=h)
    assert server.state["devin_refresh"] is True
    # finished sessions without open work are not watched
    acp_session["status"] = "finished"
    assert client.post("/api/acp/sessions", json=listed, headers=h).json()["watch"] == []


def test_messages_endpoint_includes_streamed_messages(monkeypatch, client, acp_session):
    h = {"x-board-token": "tok"}
    server.state["board"] = {"columns": [{"id": "c", "cards": [{"id": "session:s1", "kind": "session", "session_id": "s1", "repo": None, "number": None}]}]}
    server.session_msgs_cache["s1"] = {"msgs": [{"who": "user", "ts": "2026-01-01T00:00:00Z", "text": "go", "origin": None, "name": None}],
                                       "cursor": None, "seen": set(), "updated_at": "2026-01-01T00:00:00Z"}
    server.apply_acp_events("s1", [{"type": "message", "message_id": "m1", "text": "on it"}])
    calls = []

    class FakeDevin:
        async def get(self, path, params=None):
            calls.append(path)
            return httpx.Response(200, json={"items": [], "has_next_page": False}, request=httpx.Request("GET", path))
    monkeypatch.setattr(server, "devin_client", lambda: FakeDevin())
    monkeypatch.setattr(server, "tracked_session", lambda s: True)
    server.state["acp"].update(connected=True, last_at=time.time())
    r = client.get("/api/card/session:s1/messages", headers=h)
    assert r.status_code == 200
    assert [(m["who"], m["text"]) for m in r.json()["messages"]] == [("user", "go"), ("devin", "on it")]
    assert calls == []  # stream is live and updated_at unchanged: no REST round-trip
    server.state["acp"]["connected"] = False
    client.get("/api/card/session:s1/messages", headers=h)
    assert calls == ["/sessions/s1/messages"]



def test_local_echo_dropped_when_devin_decorates_attachment_message():
    local = 'its still showing the 2061\nATTACHMENT:"https://app.devin.ai/attachments/abc/image.png"'
    real = (
        'its still showing the 2061\n\nATTACHMENT:"https://app.devin.ai/attachments/abc/image.png"\n'
        "<!-- This file was provided by the user and is automatically downloaded to: /home/x/image.png -->"
    )
    msgs = [{"who": "user", "text": local, "local": time.time(), "ts": ""}]
    server._drop_local_echo(msgs, real)
    assert msgs == []
    now = time.time()
    real_msgs = [{"who": "user", "text": real, "ts": server.datetime.now(server.timezone.utc).isoformat()}]
    assert server._has_real_user_msg(real_msgs, local, now)
    assert not server._has_real_user_msg(real_msgs, "something else", now)
    # a distinct reply that merely prefixes an earlier one is not its echo
    assert not server._has_real_user_msg(real_msgs, "its still showing", now)
    # user-authored HTML comments are kept when comparing
    only_comment = "<!-- just a note -->"
    assert server._echoes(only_comment, only_comment)
    assert not server._echoes(only_comment, "<!-- other -->")


def test_issue_filed_from_a_board_shows_only_there(monkeypatch, client):
    """A repo on two boards: an issue filed from one board's prompt box stays on that
    board; an issue created on GitHub itself still shows on every board tracking the repo."""
    async def noop():
        return None
    monkeypatch.setattr(server, "_refresh_after_board_change", noop)
    h = {"x-board-token": "tok"}
    a = client.post("/api/boards", json={"name": "Alpha", "repos": ["acme/shared"]}, headers=h).json()["board"]["id"]
    b = client.post("/api/boards", json={"name": "Beta", "repos": ["acme/shared"]}, headers=h).json()["board"]["id"]
    try:
        marker = server.prompt_marker("abcdef012345", a)
        assert marker == "<!-- attention:prompt:abcdef012345 board:alpha -->"
        gh = {"number": 1, "title": "from alpha", "body": f"do it\n\n{marker}", "html_url": "u", "labels": [],
              "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"}
        filed = server.issue_from_gh("acme/shared", gh)
        assert (filed["prompt_id"], filed["board"], filed["body"]) == ("abcdef012345", a, "do it")
        # the older marker (no board) still parses
        legacy = server.issue_from_gh("acme/shared", {**gh, "number": 2, "body": "x <!-- attention:prompt:abcdef012345 -->"})
        assert (legacy["prompt_id"], legacy["board"]) == ("abcdef012345", None)
        plain = server.issue_from_gh("acme/shared", {**gh, "number": 3, "body": "on github"})
        server.state["issues"].update({"acme/shared#1": filed, "acme/shared#2": legacy, "acme/shared#3": plain})
        asyncio.run(server.assemble_board())

        def issues_on(bid):
            return sorted(c["id"] for col in server.board_view(bid)["columns"] for c in col["cards"])
        assert issues_on(a) == ["acme/shared#1", "acme/shared#2", "acme/shared#3"]
        assert issues_on(b) == ["acme/shared#2", "acme/shared#3"]
        assert issues_on(None) == ["acme/shared#1", "acme/shared#2", "acme/shared#3"]
        # editing the body keeps the board in the marker
        assert server.PROMPT_MARK_RE.search(server.prompt_marker("abcdef012345", a)).group(2) == a
        # once the filing board no longer tracks the repo, the issue falls back to the repo's boards
        client.put(f"/api/boards/{a}", json={"name": "Alpha", "repos": ["acme/other"]}, headers=h)
        asyncio.run(server.assemble_board())
        assert issues_on(b) == ["acme/shared#1", "acme/shared#2", "acme/shared#3"]
    finally:
        for i in (a, b):
            client.delete(f"/api/boards/{i}", headers=h)
