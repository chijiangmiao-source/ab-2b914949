#!/usr/bin/env python3
"""End-to-end API smoke + rule exercise for the handoff coordinator.

Runs either against an already-running service (``SMOKE_BASE_URL``) or spawns
``python -m app`` locally on an ephemeral port with a throwaway database.

The script exercises the full externally-visible contract:

  * health endpoint
  * snapshot driven target allocation / member replacement -> revocation
  * stale, unauthorized and extra-partition confirmations rejected
  * current-generation confirmation publishes the next generation
  * idempotent retry replays the first result; changed reuse conflicts
  * process crash after the durable release intent but before publication,
    restart, recovery and replay -> never double-owned

Exits 0 only when every assertion passes.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from typing import Any

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

PARTITION_COUNT = 16


def members(*ids: str) -> list[dict[str, str]]:
    return [{"member_id": m} for m in ids]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Client:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            self.base_url + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def get(self, path: str) -> tuple[int, dict[str, Any]]:
        return self.request("GET", path)

    def post(self, path: str, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        return self.request("POST", path, body)


def wait_for_health(client: Client, attempts: int = 50) -> None:
    for _ in range(attempts):
        try:
            status, body = client.get("/healthz")
            if status == 200 and body.get("status") == "ok":
                return
        except Exception:
            pass
        time.sleep(0.2)
    raise RuntimeError("service did not become healthy")


def assert_invariant(view: dict[str, Any]) -> None:
    owned: dict[int, str] = {}
    for owner, plist in view["assignments"].items():
        for p in plist:
            assert p not in owned, f"partition {p} double-owned by {owned[p]} and {owner}"
            owned[p] = owner
    assert sorted(owned) == list(range(view["partition_count"])), "every partition allocated"
    for key, item in view["partitions"].items():
        assert owned[int(key)] == item["owner"]
        if item["revoking"]:
            assert item["owner"] != item["target"], key
        else:
            assert item["owner"] == item["target"], key
        assert view["handoff"][key] == item["generation"]


def run_scenario(client: Client) -> None:
    print("  health ...")
    status, body = client.get("/healthz")
    assert status == 200 and body == {"status": "ok"}, (status, body)

    print("  initial snapshot ...")
    status, body = client.post(
        "/snapshots", {"request_id": "smoke-init", "members": members("A", "B")}
    )
    assert status == 200, (status, body)
    assert not body["replayed"]
    assert_invariant(client.get("/")[1])

    print("  retry replays first result ...")
    status, again = client.post(
        "/snapshots", {"request_id": "smoke-init", "members": members("A", "B")}
    )
    assert status == 200 and again["replayed"] is True, (status, again)
    assert again["assignments"] == body["assignments"]

    print("  same request id with changed snapshot conflicts ...")
    status, body = client.post(
        "/snapshots", {"request_id": "smoke-init", "members": members("A", "C")}
    )
    assert status == 409 and body["error"] == "request_id_conflict", (status, body)

    print("  member replacement enters revocation ...")
    status, body = client.post(
        "/snapshots", {"request_id": "smoke-replace", "members": members("B", "C")}
    )
    assert status == 200 and body["newly_revoked"], (status, body)
    view = client.get("/")[1]
    assert_invariant(view)
    revoked = view["revocations"]
    assert revoked, "replacement must create revocations"
    # The brand-new member C must not yet own any partition.
    assert "C" not in view["assignments"], view["assignments"]

    print("  stale / unauthorized / extra confirmations rejected ...")
    by_owner: dict[tuple[str, int], list[int]] = {}
    for r in revoked:
        by_owner.setdefault((r["owner"], r["generation"]), []).append(r["partition"])
    (owner_a, gen_a), a_parts = sorted(by_owner.items())[0]
    a_parts = sorted(a_parts)

    status, body = client.post(
        "/confirmations",
        {"request_id": "smoke-stale", "member_id": owner_a,
         "generation": gen_a - 1, "partitions": a_parts},
    )
    assert status == 409 and body["error"] == "stale_confirmation", (status, body)

    status, body = client.post(
        "/confirmations",
        {"request_id": "smoke-unauth", "member_id": "C",
         "generation": gen_a, "partitions": a_parts},
    )
    assert status == 403 and body["error"] == "unauthorized_confirmation", (status, body)

    # A stable partition of this owner turns the batch into an "extra" one.
    stable = next(
        int(p) for p, it in view["partitions"].items()
        if it["owner"] == owner_a and not it["revoking"]
    ) if any(
        it["owner"] == owner_a and not it["revoking"]
        for it in view["partitions"].values()
    ) else None
    if stable is not None:
        status, body = client.post(
            "/confirmations",
            {"request_id": "smoke-extra", "member_id": owner_a,
             "generation": gen_a, "partitions": sorted(set(a_parts + [stable]))},
        )
        assert status == 409 and body["error"] == "unexpected_partition", (status, body)

    print("  valid confirmation publishes next generation ...")
    status, body = client.post(
        "/confirmations",
        {"request_id": "smoke-ack-a", "member_id": owner_a,
         "generation": gen_a, "partitions": a_parts},
    )
    assert status == 200, (status, body)
    assert {r["partition"] for r in body["released"]} == set(a_parts)
    assert all(r["generation"] == gen_a + 1 for r in body["released"])
    view = client.get("/")[1]
    assert_invariant(view)

    print("  confirmation retry replays, no double advance ...")
    status, replay = client.post(
        "/confirmations",
        {"request_id": "smoke-ack-a", "member_id": owner_a,
         "generation": gen_a, "partitions": a_parts},
    )
    assert status == 200 and replay["replayed"] is True, (status, replay)
    assert replay["released"] == body["released"]
    assert_invariant(client.get("/")[1])

    # Release every remaining owner so the system converges.
    view = client.get("/")[1]
    remaining: dict[tuple[str, int], list[int]] = {}
    for r in view["revocations"]:
        remaining.setdefault((r["owner"], r["generation"]), []).append(r["partition"])
    for i, ((owner, gen), plist) in enumerate(remaining.items()):
        status, body = client.post(
            "/confirmations",
            {"request_id": f"smoke-ack-rest-{i}", "member_id": owner,
             "generation": gen, "partitions": sorted(plist)},
        )
        assert status == 200, (status, body)
    assert_invariant(client.get("/")[1])


def run_crash_recovery(base_url: str, env: dict[str, str], tmpdir: str) -> None:
    """Fresh service: crash in the release window, restart, verify recovery."""
    print("  crash-recovery: fresh service ...")
    proc, client = start_server(base_url, env)
    try:
        wait_for_health(client)
        client.post(
            "/snapshots", {"request_id": "cr-init", "members": members("A", "B")}
        )
        client.post(
            "/snapshots", {"request_id": "cr-replace", "members": members("B", "C")}
        )
        view = client.get("/")[1]
        revoked_a = [r for r in view["revocations"] if r["owner"] == "A"]
        assert revoked_a
        gen = revoked_a[0]["generation"]
        parts = sorted(r["partition"] for r in revoked_a)

        print("  crash after durable release intent, before publish ...")
        # The test hook kills the whole server process abruptly, so the
        # connection dies with no response; either outcome is acceptable.
        try:
            status, _ = client.post(
                "/confirmations",
                {"request_id": "cr-ack", "member_id": "A", "generation": gen,
                 "partitions": parts, "_crash_after_intent": True},
            )
            assert status >= 500, status
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
    finally:
        proc.wait(timeout=10)

    print("  restart and verify no double ownership ...")
    proc, client = start_server(base_url, env)
    try:
        wait_for_health(client)
        view = client.get("/")[1]
        # Until the durable intent resumes, ownership is still entirely A's.
        # Startup recovery resumes it atomically: the post-recovery view is
        # consistent with confirmed releases only.
        assert_invariant(view)
        for p in parts:
            item = view["partitions"][str(p)]
            assert item["owner"] != "A", item
            assert item["owner"] == item["target"]
            assert item["generation"] == gen + 1

        print("  post-crash retry replays first result ...")
        status, replay = client.post(
            "/confirmations",
            {"request_id": "cr-ack", "member_id": "A", "generation": gen,
             "partitions": parts},
        )
        assert status == 200 and replay["replayed"] is True, (status, replay)
        assert {r["partition"] for r in replay["released"]} == set(parts)
        gens_before = {p: it["generation"] for p, it in view["partitions"].items()}
        assert_invariant(client.get("/")[1])
        gens_after = {
            p: it["generation"] for p, it in client.get("/")[1]["partitions"].items()
        }
        assert gens_before == gens_after, "replay must not advance generations again"
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def start_server(base_url: str, env: dict[str, str]):
    proc = subprocess.Popen(
        [sys.executable, "-m", "app"],
        cwd=ROOT,
        env={**os.environ, **env},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return proc, Client(base_url)


def main() -> int:
    existing = os.environ.get("SMOKE_BASE_URL")
    if existing:
        print(f"smoke against existing service {existing}")
        client = Client(existing.rstrip("/"))
        wait_for_health(client)
        try:
            run_scenario(client)
        except AssertionError:
            # Crash recovery requires the ability to kill the server; it is
            # only executed when this script owns the lifecycle.
            raise
        print("SMOKE OK")
        return 0

    tmpdir = tempfile.mkdtemp(prefix="handoff-smoke-")
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    env = {
        "HOST": "127.0.0.1",
        "PORT": str(port),
        "HANDOFF_DB": os.path.join(tmpdir, "handoff.db"),
        "PARTITION_COUNT": str(PARTITION_COUNT),
    }
    print("smoke: starting service")
    proc, client = start_server(base_url, env)
    try:
        wait_for_health(client)
        run_scenario(client)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

    crash_dir = tempfile.mkdtemp(prefix="handoff-crash-")
    crash_port = free_port()
    crash_url = f"http://127.0.0.1:{crash_port}"
    crash_env = {
        "HOST": "127.0.0.1",
        "PORT": str(crash_port),
        "HANDOFF_DB": os.path.join(crash_dir, "handoff.db"),
        "PARTITION_COUNT": str(PARTITION_COUNT),
    }
    try:
        run_crash_recovery(crash_url, crash_env, crash_dir)
    except Exception:
        print("SMOKE FAILED")
        raise

    print("SMOKE OK")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as exc:
        print(f"SMOKE FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
