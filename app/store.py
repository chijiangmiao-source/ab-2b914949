"""Persistent handoff coordination core.

Invariant
---------
For every partition p and generation g, at most one receiving instance owns p.
A partition whose target changes while still held by its old owner enters
``revoking``: the ownership record keeps pointing at the old owner (so the new
instance can never consume in parallel) until the old owner acknowledges the
revocation at the partition's *current* generation.  The acknowledgment then,
in a single durable commit,

  1. deletes the old owner's ownership,
  2. advances the handoff-ready collection, and
  3. publishes the next generation pointing at the persisted target.

Reads therefore only ever observe either the complete old allocation or an
intermediate allocation consistent with already confirmed releases.

A durable intent row is written before the release is applied.  If the process
dies between recording the release decision and publishing it, the ownership is
still wholly with the old owner; on recovery the intent is resumed against the
latest persisted target, so double ownership is impossible.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
from typing import Any

log = logging.getLogger("handoff.store")

SCHEMA = """
CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS requests (
    request_id  TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,
    signature   TEXT NOT NULL,
    status      TEXT NOT NULL,            -- 'pending' | 'done'
    result_code INTEGER,
    result_body TEXT,
    intent      TEXT,
    created_at  REAL NOT NULL
);
"""

STATE_KEY = "state"


class RequestError(Exception):
    """An error that maps to an HTTP response."""

    def __init__(self, status_code: int, code: str, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


def _stable_index(partition: int, member_id: str) -> int:
    digest = hashlib.sha256(f"{partition}:{member_id}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


class Store:
    def __init__(self, db_path: str, partition_count: int) -> None:
        self.db_path = db_path
        self.partition_count = partition_count
        # Writers are serialized in-process; SQLite serializes across processes
        # via its write lock (BEGIN IMMEDIATE + busy timeout).
        self._lock = threading.RLock()
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute(f"PRAGMA busy_timeout={5000}")
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)
        # Pending release intents from a previous process are resumed by
        # recover_pending(); callers decide when (e.g. at HTTP startup).

    # ------------------------------------------------------------------ utils
    def _begin(self) -> None:
        self._conn.execute("BEGIN IMMEDIATE")

    def _commit(self) -> None:
        self._conn.execute("COMMIT")

    def _rollback(self) -> None:
        try:
            self._conn.execute("ROLLBACK")
        except sqlite3.OperationalError as exc:
            if "no transaction is active" not in str(exc):
                raise

    def _load_state(self) -> dict[str, Any]:
        row = self._conn.execute(
            "SELECT value FROM kv WHERE key = ?", (STATE_KEY,)
        ).fetchone()
        if row is None:
            return {"generations": {}, "handoff": {}}
        return json.loads(row["value"])

    def _save_state(self, state: dict[str, Any]) -> None:
        payload = json.dumps(state, sort_keys=True)
        self._conn.execute(
            "INSERT INTO kv(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (STATE_KEY, payload),
        )

    def partitions(self) -> list[int]:
        return list(range(self.partition_count))

    # ------------------------------------------------------------- validation
    @staticmethod
    def _validate_request_id(body: dict[str, Any]) -> str:
        rid = body.get("request_id")
        if not isinstance(rid, str) or not rid.strip():
            raise RequestError(400, "invalid_request", "request_id is required")
        if len(rid) > 200:
            raise RequestError(400, "invalid_request", "request_id too long")
        return rid

    def _validate_members(self, body: dict[str, Any]) -> list[str]:
        members = body.get("members")
        if not isinstance(members, list) or not members:
            raise RequestError(
                400, "invalid_members", "members must be a non-empty list"
            )
        ids: list[str] = []
        for m in members:
            if not isinstance(m, dict):
                raise RequestError(400, "invalid_members", "each member must be an object")
            mid = m.get("member_id")
            if not isinstance(mid, str) or not mid.strip():
                raise RequestError(
                    400, "invalid_members", "member_id is required for each member"
                )
            ids.append(mid)
        if len(set(ids)) != len(ids):
            raise RequestError(400, "invalid_members", "member_id values must be unique")
        return ids

    def _validate_confirmation(
        self, body: dict[str, Any]
    ) -> tuple[str, int, list[int]]:
        member_id = body.get("member_id")
        if not isinstance(member_id, str) or not member_id.strip():
            raise RequestError(400, "invalid_member", "member_id is required")
        generation = body.get("generation")
        if not isinstance(generation, int) or isinstance(generation, bool) or generation < 0:
            raise RequestError(400, "invalid_generation", "generation must be a non-negative integer")
        partitions = body.get("partitions")
        if not isinstance(partitions, list):
            raise RequestError(400, "invalid_partitions", "partitions must be a list")
        if not partitions:
            raise RequestError(400, "invalid_partitions", "partitions must not be empty")
        result: list[int] = []
        for p in partitions:
            if not isinstance(p, int) or isinstance(p, bool):
                raise RequestError(400, "invalid_partitions", "partition ids must be integers")
            if not 0 <= p < self.partition_count:
                raise RequestError(404, "unknown_partition", f"partition {p} does not exist")
            result.append(p)
        if len(set(result)) != len(result):
            raise RequestError(400, "invalid_partitions", "partition ids must be unique")
        return member_id, generation, result

    @staticmethod
    def _snapshot_signature(member_ids: list[str]) -> str:
        # Membership is a set: only identity content participates in request
        # identity, not submission order.
        return "snapshot:" + json.dumps(sorted(member_ids))

    @staticmethod
    def _confirm_signature(member_id: str, generation: int, partitions: list[int]) -> str:
        return "confirm:" + json.dumps(
            [member_id, generation, sorted(partitions)], separators=(",", ":")
        )

    # ---------------------------------------------------------- target mapping
    def _targets_for(self, member_ids: list[str]) -> dict[str, str]:
        """Determine the target owner of every partition.

        The decision is a pure function of member identities and partition
        numbers (stable hash ring without vnodes), so re-submitting the same
        member snapshot converges to the same persisted target.
        """
        targets: dict[str, str] = {}
        for p in self.partitions():
            best = min(member_ids, key=lambda mid: (_stable_index(p, mid), mid))
            targets[str(p)] = best
        return targets

    # ----------------------------------------------------------------- views
    def describe(self, state: dict[str, Any] | None = None) -> dict[str, Any]:
        if state is None:
            with self._lock:
                state = self._load_state()
        gens = state["generations"]
        partitions: dict[str, Any] = {}
        assignments: dict[str, list[int]] = {}
        target_view: dict[str, list[int]] = {}
        revocations: list[dict[str, Any]] = []
        for p in map(str, self.partitions()):
            entry = gens.get(p)
            if entry is None:
                continue
            owner = entry["owner"]
            target = entry["target"]
            assignments.setdefault(owner, []).append(int(p))
            target_view.setdefault(target, []).append(int(p))
            item = {
                "partition": int(p),
                "generation": entry["gen"],
                "owner": owner,
                "target": target,
                "revoking": bool(entry["revoked"]),
            }
            partitions[p] = item
            if entry["revoked"]:
                revocations.append(
                    {
                        "partition": int(p),
                        "generation": entry["gen"],
                        "owner": owner,
                        "target": target,
                    }
                )
        for view in (assignments, target_view):
            for lst in view.values():
                lst.sort()
        return {
            "partition_count": self.partition_count,
            "partitions": partitions,
            "assignments": {k: v for k, v in sorted(assignments.items())},
            "targets": {k: v for k, v in sorted(target_view.items())},
            "revocations": sorted(revocations, key=lambda r: r["partition"]),
            "handoff": {str(k): v for k, v in sorted(state["handoff"].items(), key=lambda kv: int(kv[0]))},
        }

    # ------------------------------------------------------------ idempotency
    def _lookup_request(self, request_id: str) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM requests WHERE request_id = ?", (request_id,)
        ).fetchone()

    @staticmethod
    def _replay(row: sqlite3.Row) -> dict[str, Any]:
        body = json.loads(row["result_body"]) if row["result_body"] else {}
        body["request_id"] = row["request_id"]
        body["replayed"] = True
        if row["result_code"] is not None and row["result_code"] >= 400:
            raise RequestError(
                row["result_code"], body.get("error", "error"), body.get("message", "")
            )
        return body

    def _finish_pending(self, row: sqlite3.Row) -> tuple[int, dict[str, Any]]:
        """Resume a durable release intent inside an open txn.

        Raises ``RequestError`` if the intent went stale while the process was
        down; the rejection is then persisted terminally.
        """
        intent = json.loads(row["intent"])
        state = self._load_state()
        try:
            self._validate_intent_against_state(
                state, intent["member_id"], intent["generation"], intent["partitions"]
            )
        except RequestError as err:
            # The intent never released anything: record the rejection
            # terminally so a retry observes the same outcome.
            self._terminalize_error(row["request_id"], err)
            raise
        code, result = self._apply_release(
            row["request_id"],
            intent["member_id"],
            intent["generation"],
            intent["partitions"],
        )
        result["request_id"] = row["request_id"]
        result["resumed"] = True
        return code, result

    # ------------------------------------------------------------ public API
    def submit_snapshot(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        request_id = self._validate_request_id(body)
        member_ids = self._validate_members(body)
        signature = self._snapshot_signature(member_ids)
        with self._lock:
            self._begin()
            try:
                row = self._lookup_request(request_id)
                if row is not None:
                    if row["kind"] != "snapshot":
                        raise RequestError(
                            409, "request_id_conflict",
                            "request_id already used by a different request kind",
                        )
                    if row["signature"] != signature:
                        raise RequestError(
                            409, "request_id_conflict",
                            "request_id reused with a different member snapshot",
                        )
                    if row["status"] == "done":
                        code = row["result_code"]
                        result = self._replay(row)
                        self._commit()
                        return code, result
                    # A pending snapshot cannot exist: snapshots are atomic.
                    raise RequestError(409, "request_pending", "request is still in progress")

                targets = self._targets_for(member_ids)
                state = self._load_state()
                gens = state["generations"]
                changed: list[dict[str, Any]] = []
                for p, new_target in targets.items():
                    entry = gens.get(p)
                    if entry is None:
                        # First ever publication of this partition.
                        gens[p] = {
                            "gen": 1,
                            "owner": new_target,
                            "target": new_target,
                            "revoked": False,
                        }
                        state["handoff"][p] = 1
                        continue
                    entry["target"] = new_target
                    if entry["owner"] == new_target:
                        # Desired holder already owns it: nothing to revoke.
                        entry["revoked"] = False
                    else:
                        # Still held by the old owner: enter revocation instead
                        # of handing the partition straight to the new target.
                        if not entry["revoked"]:
                            changed.append(
                                {
                                    "partition": int(p),
                                    "generation": entry["gen"],
                                    "owner": entry["owner"],
                                    "target": new_target,
                                }
                            )
                        entry["revoked"] = True
                self._save_state(state)
                view = self.describe(state)
                result = {
                    "replayed": False,
                    "assignments": view["assignments"],
                    "targets": view["targets"],
                    "revoked": [r for r in view["revocations"]],
                    "newly_revoked": changed,
                    "handoff": view["handoff"],
                }
                self._conn.execute(
                    "INSERT INTO requests(request_id, kind, signature, status, "
                    "result_code, result_body, intent, created_at) "
                    "VALUES(?, 'snapshot', ?, 'done', 200, ?, NULL, ?)",
                    (request_id, signature, json.dumps(result), time.time()),
                )
                self._commit()
            except Exception:
                self._rollback()
                raise
        result["request_id"] = request_id
        return 200, result

    def _validate_intent_against_state(
        self, state: dict[str, Any], member_id: str, generation: int, partitions: list[int]
    ) -> None:
        gens = state["generations"]
        for p in partitions:
            key = str(p)
            entry = gens.get(key)
            if entry is None:
                raise RequestError(404, "unknown_partition", f"partition {p} is not allocated")
            if entry["gen"] != generation:
                raise RequestError(
                    409, "stale_confirmation",
                    f"partition {p} is at generation {entry['gen']}, not {generation}",
                )
            if entry["owner"] != member_id:
                raise RequestError(
                    403, "unauthorized_confirmation",
                    f"partition {p} is owned by {entry['owner']}",
                )
            if not entry["revoked"]:
                raise RequestError(
                    409, "unexpected_partition",
                    f"partition {p} is not under revocation for {member_id}",
                )

    def _terminalize_error(self, request_id: str, err: RequestError) -> None:
        """Persist a terminal error result for a pending request."""
        body = {
            "error": err.code,
            "message": err.message,
            "replayed": False,
        }
        self._conn.execute(
            "UPDATE requests SET status='done', result_code=?, result_body=?, "
            "intent=NULL WHERE request_id=?",
            (err.status_code, json.dumps(body), request_id),
        )

    def _terminalize_pending_insert(
        self, request_id: str, signature: str, err: RequestError
    ) -> None:
        body = {"error": err.code, "message": err.message, "replayed": False}
        self._conn.execute(
            "INSERT INTO requests(request_id, kind, signature, status, "
            "result_code, result_body, intent, created_at) "
            "VALUES(?, 'confirm', ?, 'done', ?, ?, NULL, ?)",
            (request_id, signature, err.status_code, json.dumps(body), time.time()),
        )

    def _apply_release(
        self,
        request_id: str,
        member_id: str,
        generation: int,
        partitions: list[int],
    ) -> tuple[int, dict[str, Any]]:
        """Validate and apply a release.

        Must be called inside ``self._lock`` with an open immediate transaction.
        Any stale, unauthorized or extra partition rejects the entire
        confirmation without advancing anything.
        """
        state = self._load_state()
        self._validate_intent_against_state(state, member_id, generation, partitions)
        gens = state["generations"]
        released: list[dict[str, Any]] = []
        for p in partitions:
            key = str(p)
            entry = gens[key]
            new_owner = entry["target"]  # converge on the latest persisted target
            new_gen = entry["gen"] + 1
            # Single atomic publication below: old ownership deleted, handoff
            # collection advanced and new generation published together.
            entry["owner"] = new_owner
            entry["gen"] = new_gen
            entry["revoked"] = False
            state["handoff"][key] = new_gen
            released.append(
                {"partition": p, "generation": new_gen, "owner": new_owner}
            )

        self._save_state(state)
        view = self.describe(state)
        result = {
            "replayed": False,
            "released": sorted(released, key=lambda r: r["partition"]),
            "assignments": view["assignments"],
            "revoked": view["revocations"],
            "handoff": view["handoff"],
        }
        self._conn.execute(
            "UPDATE requests SET status='done', result_code=200, result_body=?, "
            "intent=NULL WHERE request_id=?",
            (json.dumps(result), request_id),
        )
        return 200, result

    def confirm(
        self,
        body: dict[str, Any],
        crash_after_intent: bool = False,
        crash_after_commit: bool = False,
    ) -> tuple[int, dict[str, Any]]:
        request_id = self._validate_request_id(body)
        member_id, generation, partitions = self._validate_confirmation(body)
        signature = self._confirm_signature(member_id, generation, partitions)
        with self._lock:
            self._begin()
            row = self._lookup_request(request_id)
            try:
                if row is not None:
                    if row["kind"] != "confirm":
                        raise RequestError(
                            409, "request_id_conflict",
                            "request_id already used by a different request kind",
                        )
                    if row["signature"] != signature:
                        raise RequestError(
                            409, "request_id_conflict",
                            "request_id reused with a different confirmation",
                        )
                    if row["status"] == "done":
                        # Replays the stored result; raises the stored error
                        # when the first attempt was rejected.
                        result = self._replay(row)
                        self._commit()
                        return row["result_code"], result
                    # A crash left a durable intent: finish it now. Any
                    # rejection is terminalized inside the same transaction.
                    try:
                        code, result = self._finish_pending(row)
                    except RequestError:
                        self._commit()
                        raise
                    self._commit()
                    return code, result

                # Validate against the current generation *before* persisting
                # the release intent: stale, unauthorized and extra partitions
                # are rejected without advancing anything. The rejection is
                # still recorded terminally so an identical retry sees the same
                # first outcome.
                state = self._load_state()
                try:
                    self._validate_intent_against_state(
                        state, member_id, generation, partitions
                    )
                except RequestError as err:
                    self._terminalize_pending_insert(
                        request_id, signature, err
                    )
                    self._commit()
                    raise

                # Durable release decision. Until it is published the ownership
                # is still wholly with the old owner, so readers see the
                # complete old allocation.
                intent = {
                    "member_id": member_id,
                    "generation": generation,
                    "partitions": partitions,
                }
                self._conn.execute(
                    "INSERT INTO requests(request_id, kind, signature, status, "
                    "result_code, result_body, intent, created_at) "
                    "VALUES(?, 'confirm', ?, 'pending', NULL, NULL, ?, ?)",
                    (request_id, signature, json.dumps(intent), time.time()),
                )
                self._commit()
            except RequestError:
                self._rollback()
                raise

            if crash_after_intent:
                # Simulate the process dying after the release decision was
                # persisted but before it was published.
                os._exit(7)

            self._begin()
            try:
                row = self._lookup_request(request_id)
                if row is not None and row["status"] == "done":
                    # Another process resumed the durable intent while this
                    # thread was between phases: replay its first result.
                    result = self._replay(row)
                    self._commit()
                    return row["result_code"], result
                code, result = self._finish_pending(row)
                self._commit()
            except RequestError:
                # The rejection was terminalized inside the transaction.
                self._commit()
                raise
            except Exception:
                self._rollback()
                raise

            if crash_after_commit:
                # The durable commit landed but the response was lost: a retry
                # must replay the first result.
                os._exit(8)

        return code, result

    def recover_pending(self) -> list[str]:
        """Resume release intents left by a crashed process.

        Ownership while an intent is pending still belongs entirely to the old
        owner, so resuming (and re-converging on the latest persisted target)
        can never double-allocate.
        """
        resumed: list[str] = []
        rejected: list[str] = []
        with self._lock:
            while True:
                self._begin()
                row = self._conn.execute(
                    "SELECT * FROM requests "
                    "WHERE status='pending' AND intent IS NOT NULL "
                    "ORDER BY created_at LIMIT 1"
                ).fetchone()
                if row is None:
                    self._commit()
                    break
                try:
                    self._finish_pending(row)
                    resumed.append(row["request_id"])
                except RequestError:
                    # Lost a release race before the crash: the rejection was
                    # terminalized in this same transaction.
                    rejected.append(row["request_id"])
                self._commit()
        if rejected:
            log.warning("discarded %d stale pending intent(s): %s", len(rejected), rejected)
        return resumed

    def reset(self) -> None:
        with self._lock:
            self._begin()
            try:
                self._conn.execute("DELETE FROM kv")
                self._conn.execute("DELETE FROM requests")
                self._commit()
            except Exception:
                self._rollback()
                raise

    def close(self) -> None:
        with self._lock:
            self._conn.close()
