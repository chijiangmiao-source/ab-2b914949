"""Subprocess driver for crash-recovery tests.

Performs a fixed scenario and exits abruptly (``os._exit``) at the chosen
point, without flushing any Python-level buffers:

  1. initial snapshot [A, B]
  2. replacement snapshot [B, C]  (partitions leave A -> revocation)
  3. A confirms the revoked partitions it still owns, then:
       mode=intent: die after the durable release intent is committed,
                    before ownership is deleted / new generation published
       mode=commit: die after the release commit landed, before responding
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.store import Store


def main() -> None:
    db_path = sys.argv[1]
    partition_count = int(sys.argv[2])
    mode = sys.argv[3]

    store = Store(db_path, partition_count)
    store.submit_snapshot(
        {"request_id": "seed-1", "members": [{"member_id": "A"}, {"member_id": "B"}]}
    )
    store.submit_snapshot(
        {"request_id": "seed-2", "members": [{"member_id": "B"}, {"member_id": "C"}]}
    )
    view = store.describe()
    revoked_a = [r["partition"] for r in view["revocations"] if r["owner"] == "A"]
    assert revoked_a, "scenario expects at least one revoked partition still owned by A"
    gen = view["partitions"][str(revoked_a[0])]["generation"]

    confirmation = {
        "request_id": "confirm-crash",
        "member_id": "A",
        "generation": gen,
        "partitions": revoked_a,
    }
    print(json.dumps(revoked_a))
    sys.stdout.flush()

    store.confirm(
        confirmation,
        crash_after_intent=(mode == "intent"),
        crash_after_commit=(mode == "commit"),
    )
    # Only reached when no crash mode was selected.
    print(json.dumps({"status": "completed"}))


if __name__ == "__main__":
    main()
