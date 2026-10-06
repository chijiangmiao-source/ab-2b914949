"""Rule tests for the satellite downlink partition handoff coordinator.

Covers:
  * at most one owner per partition per generation
  * member replacement moves still-held partitions into revocation
  * partitions are never handed straight to the new instance
  * current-generation confirmation releases and publishes in one commit
  * stale / unauthorized / extra-partition confirmations are rejected entirely
  * idempotent retries replay the first result; same id + changed snapshot conflicts
  * snapshots keep changing during handoff -> convergence on persisted targets
  * reads see only the old complete allocation or confirmed-release-consistent
    intermediate allocations
  * crash after release intent / after publish commit => no double ownership,
    retry replays the first result
  * concurrent snapshots and confirmations preserve every invariant

Run with:  python -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.store import RequestError, Store  # noqa: E402

PARTITION_COUNT = 16


def members(*ids: str) -> list[dict[str, str]]:
    return [{"member_id": m} for m in ids]


class StoreTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "handoff.db")
        self.store = Store(self.db, PARTITION_COUNT)

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def snapshot(self, rid: str, ids: tuple[str, ...]):
        return self.store.submit_snapshot({"request_id": rid, "members": members(*ids)})

    def confirm(self, rid: str, member: str, generation: int, partitions: list[int]):
        return self.store.confirm(
            {
                "request_id": rid,
                "member_id": member,
                "generation": generation,
                "partitions": partitions,
            }
        )

    def assert_single_owner_invariant(self, state=None) -> None:
        """Every partition has exactly one owner; generations are coherent."""
        view = self.store.describe(state)
        owned: dict[int, str] = {}
        for owner, plist in view["assignments"].items():
            for p in plist:
                self.assertNotIn(p, owned, f"partition {p} double-owned")
                owned[p] = owner
        self.assertEqual(
            sorted(owned),
            list(range(PARTITION_COUNT)),
            "every partition must be allocated",
        )
        for key, item in view["partitions"].items():
            p = int(key)
            self.assertEqual(owned[p], item["owner"])
            self.assertGreaterEqual(item["generation"], 1)
            if item["revoking"]:
                self.assertNotEqual(item["owner"], item["target"])
            else:
                self.assertEqual(item["owner"], item["target"])
            # Handoff collection always reflects the published generation.
            self.assertEqual(view["handoff"][key], item["generation"])
        return view


class InitialAllocationTests(StoreTestBase):
    def test_first_snapshot_publishes_full_allocation(self) -> None:
        _, res = self.snapshot("r1", ("A", "B", "C"))
        self.assertFalse(res["replayed"])
        view = self.assert_single_owner_invariant()
        self.assertEqual(set(view["assignments"]), {"A", "B", "C"})
        self.assertEqual(res["newly_revoked"], [])
        for item in view["partitions"].values():
            self.assertEqual(item["generation"], 1)

    def test_target_is_deterministic_from_member_id_and_partition(self) -> None:
        self.snapshot("r1", ("A", "B", "C"))
        first = self.store.describe()["targets"]
        # Recompute with a brand new store, same member identities and order.
        other = Store(os.path.join(self.tmp.name, "other.db"), PARTITION_COUNT)
        try:
            other.submit_snapshot({"request_id": "x", "members": members("A", "B", "C")})
            self.assertEqual(other.describe()["targets"], first)
        finally:
            other.close()
        # Order-independent: the mapping is over the member *set*.
        other2 = Store(os.path.join(self.tmp.name, "o2.db"), PARTITION_COUNT)
        try:
            other2.submit_snapshot({"request_id": "y", "members": members("C", "A", "B")})
            self.assertEqual(other2.describe()["targets"], first)
        finally:
            other2.close()


class MemberReplacementTests(StoreTestBase):
    def _setup_replacement(self):
        self.snapshot("init", ("A", "B"))
        _, res = self.snapshot("replace", ("B", "C"))
        return res

    def test_replacement_enters_revocation_instead_of_direct_handoff(self) -> None:
        res = self._setup_replacement()
        view = self.assert_single_owner_invariant()
        # Every partition retargeted away from its current holder is newly
        # revoked and stays with the old owner until that owner confirms.
        revoked = {r["partition"]: r for r in res["newly_revoked"]}
        self.assertTrue(revoked)
        for p, r in revoked.items():
            item = view["partitions"][str(p)]
            self.assertEqual(item["owner"], r["owner"])  # still old owner
            self.assertEqual(item["target"], r["target"])
            self.assertNotEqual(item["owner"], item["target"])
            self.assertTrue(item["revoking"])
        # New instance C owns nothing yet: no parallel consumption possible,
        # even for partitions whose persisted target is already C.
        self.assertTrue(
            [p for p, it in view["partitions"].items() if it["target"] == "C"]
        )
        self.assertNotIn("C", view["assignments"])

    def test_unchanged_partitions_are_not_revoked(self) -> None:
        self._setup_replacement()
        view = self.store.describe()
        stable = [item for item in view["partitions"].values() if not item["revoking"]]
        self.assertTrue(stable)
        for item in stable:
            self.assertEqual(item["owner"], item["target"])
            self.assertEqual(item["owner"], "B")

    def test_confirmation_releases_and_publishes(self) -> None:
        self._setup_replacement()
        before = self.store.describe()
        # The expected new owner of each revoked partition is its persisted
        # target; confirmations are issued per current (old) owner.
        expected_target = {r["partition"]: r["target"] for r in before["revocations"]}
        by_owner: dict[tuple[str, int], list[int]] = {}
        for r in before["revocations"]:
            by_owner.setdefault((r["owner"], r["generation"]), []).append(r["partition"])
        released_all: dict[int, dict] = {}
        for i, ((owner, gen), plist) in enumerate(by_owner.items()):
            _, res = self.confirm(f"ack-{i}", owner, gen, sorted(plist))
            for r in res["released"]:
                released_all[r["partition"]] = r
        self.assertEqual(set(released_all), set(expected_target))
        for p, r in released_all.items():
            self.assertEqual(r["generation"], 2)
            self.assertEqual(r["owner"], expected_target[p])
        view = self.assert_single_owner_invariant()
        self.assertNotIn("A", view["assignments"])
        for p, target in expected_target.items():
            item = view["partitions"][str(p)]
            self.assertEqual(item["owner"], target)
            self.assertFalse(item["revoking"])
            self.assertEqual(item["generation"], 2)

    def test_partial_confirmation(self) -> None:
        self._setup_replacement()
        revoked = [r for r in self.store.describe()["revocations"] if r["owner"] == "A"]
        partitions = [r["partition"] for r in revoked]
        self.assertGreaterEqual(len(partitions), 2)
        gen = revoked[0]["generation"]
        first = partitions[:1]
        rest = partitions[1:]
        target_first = revoked[0]["target"]
        self.confirm("ack-a", "A", gen, first)
        view = self.assert_single_owner_invariant()
        self.assertEqual(view["partitions"][str(first[0])]["owner"], target_first)
        for p in rest:
            self.assertEqual(view["partitions"][str(p)]["owner"], "A")
            self.assertTrue(view["partitions"][str(p)]["revoking"])
        self.confirm("ack-b", "A", gen, rest)
        self.assert_single_owner_invariant()

    def test_revocation_not_visible_to_new_owner_for_consumption(self) -> None:
        """While unconfirmed, the target member's assignment list stays empty."""
        self._setup_replacement()
        view = self.assert_single_owner_invariant()
        # targets mention C, assignments do not.
        self.assertIn("C", view["targets"])
        self.assertNotIn("C", view["assignments"])


class InvalidConfirmationTests(StoreTestBase):
    def setUp(self) -> None:
        super().setUp()
        self.snapshot("init", ("A", "B"))
        self.snapshot("replace", ("B", "C"))
        revoked = [r for r in self.store.describe()["revocations"] if r["owner"] == "A"]
        self.gen = revoked[0]["generation"]
        self.partitions = [r["partition"] for r in revoked]
        self.assertTrue(self.partitions)

    def test_stale_generation_rejected_without_advance(self) -> None:
        before = self.store.describe()["partitions"]
        with self.assertRaises(RequestError) as ctx:
            self.confirm("stale", "A", self.gen - 1, self.partitions)
        self.assertEqual(ctx.exception.code, "stale_confirmation")
        self.assertEqual(self.store.describe()["partitions"], before)

    def test_future_generation_rejected(self) -> None:
        with self.assertRaises(RequestError) as ctx:
            self.confirm("future", "A", self.gen + 5, self.partitions)
        self.assertEqual(ctx.exception.code, "stale_confirmation")

    def test_unauthorized_member_rejected(self) -> None:
        with self.assertRaises(RequestError) as ctx:
            self.confirm("wrong", "C", self.gen, self.partitions)
        self.assertEqual(ctx.exception.code, "unauthorized_confirmation")

    def test_extra_non_revoked_partition_rejects_whole_batch(self) -> None:
        # Build a scenario in which A has BOTH revoked and still-stable
        # partitions by adding a new member without removing anyone.
        self.store.submit_snapshot(
            {"request_id": "grow", "members": members("A", "B", "D")}
        )
        view = self.store.describe()
        a_revoked = [
            int(p)
            for p, item in view["partitions"].items()
            if item["owner"] == "A" and item["revoking"]
        ]
        a_stable = next(
            int(p)
            for p, item in view["partitions"].items()
            if item["owner"] == "A" and not item["revoking"]
        )
        self.assertTrue(a_revoked)
        gen = view["partitions"][str(a_revoked[0])]["generation"]
        before = view["partitions"]
        # Including a partition that A still owns but which is not under
        # revocation must reject the entire batch and release nothing.
        with self.assertRaises(RequestError) as ctx:
            self.confirm("extra", "A", gen, a_revoked + [a_stable])
        self.assertEqual(ctx.exception.code, "unexpected_partition")
        self.assertEqual(self.store.describe()["partitions"], before)
        for p in a_revoked:
            self.assertEqual(
                self.store.describe()["partitions"][str(p)]["owner"], "A"
            )

    def test_unknown_partition_rejected(self) -> None:
        with self.assertRaises(RequestError) as ctx:
            self.confirm("oob", "A", self.gen, [PARTITION_COUNT + 1])
        self.assertEqual(ctx.exception.status_code, 404)

    def test_empty_confirmation_rejected(self) -> None:
        with self.assertRaises(RequestError) as ctx:
            self.confirm("empty", "A", self.gen, [])
        self.assertEqual(ctx.exception.status_code, 400)

    def test_confirming_already_released_partition_rejected(self) -> None:
        self.confirm("ack-1", "A", self.gen, self.partitions)
        # Re-confirming the same set at the old generation is now stale.
        with self.assertRaises(RequestError) as ctx:
            self.confirm("ack-again", "A", self.gen, self.partitions)
        self.assertEqual(ctx.exception.code, "stale_confirmation")


class IdempotencyTests(StoreTestBase):
    def test_snapshot_retry_returns_first_result(self) -> None:
        _, first = self.snapshot("dup", ("A", "B"))
        _, second = self.snapshot("dup", ("A", "B"))
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(
            {k: v for k, v in second.items() if k != "replayed"},
            {k: v for k, v in first.items() if k != "replayed"},
        )

    def test_same_request_id_with_different_snapshot_conflicts(self) -> None:
        self.snapshot("dup", ("A", "B"))
        with self.assertRaises(RequestError) as ctx:
            self.store.submit_snapshot(
                {"request_id": "dup", "members": members("A", "C")}
            )
        self.assertEqual(ctx.exception.code, "request_id_conflict")

    def test_request_id_cannot_cross_kinds(self) -> None:
        self.snapshot("init", ("A", "B"))
        self.snapshot("replace", ("B", "C"))
        revoked = self.store.describe()["revocations"]
        r = revoked[0]
        # Reuse a snapshot id on a confirmation, and vice versa.
        with self.assertRaises(RequestError) as ctx:
            self.confirm("replace", r["owner"], r["generation"], [r["partition"]])
        self.assertEqual(ctx.exception.code, "request_id_conflict")
        self.confirm("ack", r["owner"], r["generation"], [r["partition"]])
        with self.assertRaises(RequestError) as ctx:
            self.snapshot("ack", ("A", "B"))
        self.assertEqual(ctx.exception.code, "request_id_conflict")

    def test_failed_confirmation_replayed_with_same_id_and_body(self) -> None:
        self.snapshot("init", ("A", "B"))
        self.snapshot("replace", ("B", "C"))
        revoked = [r for r in self.store.describe()["revocations"] if r["owner"] == "A"]
        partitions = [r["partition"] for r in revoked]
        gen = revoked[0]["generation"]
        body = {
            "request_id": "bad",
            "member_id": "A",
            "generation": gen - 1,
            "partitions": partitions,
        }
        with self.assertRaises(RequestError):
            self.store.confirm(body)
        # Identical retry yields the same terminal failure, still no advance.
        with self.assertRaises(RequestError) as ctx:
            self.store.confirm(dict(body))
        self.assertEqual(ctx.exception.code, "stale_confirmation")
        for p in partitions:
            self.assertEqual(
                self.store.describe()["partitions"][str(p)]["owner"], "A"
            )
        # Same id but a corrected body is a conflict, never silently accepted.
        with self.assertRaises(RequestError) as ctx:
            self.store.confirm(
                {
                    "request_id": "bad",
                    "member_id": "A",
                    "generation": gen,
                    "partitions": partitions,
                }
            )
        self.assertEqual(ctx.exception.code, "request_id_conflict")

    def test_confirmation_retry_replays_first_result(self) -> None:
        self.snapshot("init", ("A", "B"))
        self.snapshot("replace", ("B", "C"))
        revoked = [r for r in self.store.describe()["revocations"] if r["owner"] == "A"]
        partitions = [r["partition"] for r in revoked]
        gen = revoked[0]["generation"]
        _, first = self.confirm("ack", "A", gen, partitions)
        _, second = self.confirm("ack", "A", gen, partitions)
        self.assertFalse(first.get("replayed"))
        self.assertTrue(second["replayed"])
        self.assertEqual(second["released"], first["released"])
        self.assertNotIn("resumed", second)


class ReconvergenceTests(StoreTestBase):
    def test_changing_snapshot_during_handoff_converges_on_persisted_target(self) -> None:
        self.snapshot("s1", ("A", "B"))
        self.snapshot("s2", ("B", "C"))  # A's partitions -> C pending revocation
        view = self.store.describe()
        a_pending = [p for p, it in view["partitions"].items() if it["owner"] == "A"]
        self.assertTrue(a_pending)

        # Membership changes again before A acknowledges: A drops out, D joins.
        self.snapshot("s3", ("C", "D"))
        view = self.store.describe()
        for key in a_pending:
            item = view["partitions"][key]
            self.assertEqual(item["owner"], "A")  # old owner until confirmation
            self.assertTrue(item["revoking"])
            self.assertIn(item["target"], ("C", "D"))

        # A acknowledges at the current generation. Release converges on the
        # *latest persisted* target, not the one it was first told about.
        revoked = [r for r in view["revocations"] if r["owner"] == "A"]
        gen = revoked[0]["generation"]
        partitions = [r["partition"] for r in revoked]
        self.confirm("ack", "A", gen, partitions)
        view = self.assert_single_owner_invariant()
        for p in partitions:
            item = view["partitions"][str(p)]
            self.assertNotEqual(item["owner"], "A")
            self.assertEqual(item["owner"], item["target"])
        self.assertNotIn("A", view["assignments"])

    def test_snapshot_after_release_revokes_again_if_target_changes(self) -> None:
        self.snapshot("s1", ("A", "B"))
        self.snapshot("s2", ("B", "C"))
        revoked = self.store.describe()["revocations"]
        gen = revoked[0]["generation"]
        partitions = [r["partition"] for r in revoked if r["owner"] == "A"]
        self.confirm("ack", "A", gen, partitions)
        # New membership moves a now-C-owned partition toward B.
        self.snapshot("s3", ("B", "D"))
        view = self.assert_single_owner_invariant()
        c_pending = [p for p, it in view["partitions"].items() if it["owner"] == "C"]
        self.assertTrue(c_pending, "C must hold partitions pending its own release")
        for p in c_pending:
            self.assertTrue(view["partitions"][str(p)]["revoking"])

    def test_reads_always_coherent_under_churn(self) -> None:
        sequences = [
            ("A", "B"), ("B", "C"), ("A", "C"),
            ("A", "B", "C"), ("C",), ("D", "E"), ("A", "E"),
        ]
        for i, snap in enumerate(sequences):
            self.snapshot(f"s{i}", snap)
            self.assert_single_owner_invariant()
            # Drain every outstanding revocation where possible.
            view = self.store.describe()
            owners = {}
            for r in view["revocations"]:
                owners.setdefault((r["owner"], r["generation"]), []).append(r["partition"])
            j = 0
            for (owner, gen), plist in owners.items():
                self.confirm(f"ack-{i}-{j}", owner, gen, sorted(plist))
                j += 1
            self.assert_single_owner_invariant()


class CrashRecoveryTests(StoreTestBase):
    DRIVER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crash_driver.py")

    def _run_crash(self, mode: str) -> list[int]:
        proc = subprocess.run(
            [sys.executable, self.DRIVER, self.db, str(PARTITION_COUNT), mode],
            capture_output=True,
            text=True,
        )
        lines = proc.stdout.strip().splitlines()
        revoked_a = json.loads(lines[0])
        return revoked_a

    def _reopen(self) -> Store:
        self.store.close()
        self.store = Store(self.db, PARTITION_COUNT)
        return self.store

    def test_crash_between_release_intent_and_publish(self) -> None:
        revoked_a = self._run_crash("intent")
        self.assertTrue(revoked_a)

        # Before any recovery call, reopening must expose the *complete old
        # allocation*: A still owns every not-yet-released partition.
        store = self._reopen()
        view = store.describe()
        for p in revoked_a:
            self.assertEqual(view["partitions"][str(p)]["owner"], "A")
            self.assertTrue(view["partitions"][str(p)]["revoking"])

        # Recovery resumes the durable intent: one atomic release per partition.
        resumed = store.recover_pending()
        self.assertEqual(resumed, ["confirm-crash"])
        view = self.assert_single_owner_invariant()
        for p in revoked_a:
            item = view["partitions"][str(p)]
            self.assertEqual(item["owner"], item["target"])
            self.assertNotEqual(item["owner"], "A")
            self.assertEqual(item["generation"], 2)

        # The client's retry with the same id replays the first (resumed) result
        # and must not release anything a second time.
        _, replay = store.confirm(
            {
                "request_id": "confirm-crash",
                "member_id": "A",
                "generation": 1,
                "partitions": revoked_a,
            }
        )
        self.assertTrue(replay["replayed"])
        self.assertEqual({r["partition"] for r in replay["released"]}, set(revoked_a))
        view2 = store.describe()
        self.assertEqual(
            {p: it["generation"] for p, it in view2["partitions"].items()},
            {p: it["generation"] for p, it in view["partitions"].items()},
        )

    def test_crash_after_commit_retry_replays(self) -> None:
        revoked_a = self._run_crash("commit")
        store = self._reopen()
        # Recovery finds nothing pending: the commit had already landed.
        self.assertEqual(store.recover_pending(), [])
        view = store.describe()
        for p in revoked_a:
            item = view["partitions"][str(p)]
            self.assertEqual(item["owner"], item["target"])
            self.assertNotEqual(item["owner"], "A")
        _, replay = store.confirm(
            {
                "request_id": "confirm-crash",
                "member_id": "A",
                "generation": 1,
                "partitions": revoked_a,
            }
        )
        self.assertTrue(replay["replayed"])
        self.assertNotIn("resumed", replay)

    def test_no_double_ownership_ever_on_disk(self) -> None:
        # Scan the SQLite state at every step of an in-process intent window:
        # while the intent row is pending, ownership is unchanged.
        self.snapshot("init", ("A", "B"))
        self.snapshot("replace", ("B", "C"))
        revoked = [r for r in self.store.describe()["revocations"] if r["owner"] == "A"]
        partitions = [r["partition"] for r in revoked]
        gen = revoked[0]["generation"]
        # Emulate the durable intent commit without applying the release.
        import sqlite3 as sql
        conn = sql.connect(self.db)
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO requests(request_id, kind, signature, status, result_code, "
            "result_body, intent, created_at) VALUES(?, 'confirm', ?, 'pending', "
            "NULL, NULL, ?, 0)",
            (
                "manual-intent",
                Store._confirm_signature("A", gen, partitions),
                json.dumps({"member_id": "A", "generation": gen, "partitions": partitions}),
            ),
        )
        conn.commit()
        conn.close()

        fresh = Store(self.db, PARTITION_COUNT)
        try:
            resumed = fresh.recover_pending()
            self.assertEqual(resumed, ["manual-intent"])
        finally:
            fresh.close()


class ConcurrencyTests(StoreTestBase):
    def test_concurrent_confirmations_and_snapshots_keep_invariant(self) -> None:
        self.snapshot("init", ("A", "B", "C"))

        errors: list[BaseException] = []

        def worker(i: int) -> None:
            try:
                if i % 2 == 0:
                    self.store.submit_snapshot(
                        {
                            "request_id": f"snap-{i}",
                            "members": members("A", "B", "C"),
                        }
                    )
                else:
                    # A concurrent member replacement.
                    self.store.submit_snapshot(
                        {
                            "request_id": f"snap-{i}",
                            "members": members("B", "C", "D"),
                        }
                    )
                    view = self.store.describe()
                    by_owner: dict[tuple[str, int], list[int]] = {}
                    for r in view["revocations"]:
                        by_owner.setdefault((r["owner"], r["generation"]), []).append(
                            r["partition"]
                        )
                    for j, ((owner, g), plist) in enumerate(by_owner.items()):
                        try:
                            self.store.confirm(
                                {
                                    "request_id": f"ack-{i}-{j}",
                                    "member_id": owner,
                                    "generation": g,
                                    "partitions": sorted(plist),
                                }
                            )
                        except RequestError:
                            # Lost a race against another worker: the
                            # confirmation went stale; nothing may advance for it.
                            pass
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assert_single_owner_invariant()

    def test_concurrent_duplicate_request_serialized(self) -> None:
        results: list[tuple[int, dict]] = []
        lock = threading.Lock()

        def fire() -> None:
            r = self.store.submit_snapshot(
                {"request_id": "same-id", "members": members("A", "B")}
            )
            with lock:
                results.append(r)

        threads = [threading.Thread(target=fire) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(results), 8)
        replays = sum(1 for _, body in results if body.get("replayed"))
        self.assertEqual(replays, 7)


if __name__ == "__main__":
    unittest.main(verbosity=2)
