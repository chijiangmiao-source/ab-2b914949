"""HTTP API for partition handoff coordination.

Endpoints
---------
GET  /healthz                 liveness/readiness probe
GET  /                        current allocation view (old-complete or
                              confirmed-release-consistent)
POST /snapshots               submit a complete member snapshot with a stable
                              request id
POST /confirmations           old owner acknowledges revoked partitions at the
                              current generation

The service only depends on the Python standard library.
"""

from __future__ import annotations

import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .store import RequestError, Store

log = logging.getLogger("handoff")

_MAX_BODY = 1 << 20  # 1 MiB


def build_store() -> Store:
    db_path = os.environ.get("HANDOFF_DB", "/data/handoff.db")
    partition_count = int(os.environ.get("PARTITION_COUNT", "32"))
    store = Store(db_path, partition_count)
    resumed = store.recover_pending()
    if resumed:
        log.warning("resumed %d pending release(s) after restart: %s", len(resumed), resumed)
    return store


class Handler(BaseHTTPRequestHandler):
    server_version = "PartitionHandoff/1.0"

    # Silence the default noisy logging; route through logging instead.
    def log_message(self, fmt: str, *args: Any) -> None:
        log.info("%s - %s", self.address_string(), fmt % args)

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            raise RequestError(400, "invalid_body", "a JSON request body is required")
        if length > _MAX_BODY:
            raise RequestError(413, "body_too_large", "request body exceeds 1 MiB")
        raw = self.rfile.read(length)
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise RequestError(400, "invalid_body", "request body must be valid JSON")
        if not isinstance(parsed, dict):
            raise RequestError(400, "invalid_body", "request body must be a JSON object")
        return parsed

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path == "/healthz":
            # A simple durable probe: the database must answer.
            try:
                self.server.store.describe()  # type: ignore[attr-defined]
            except Exception:
                self._send_json(503, {"status": "unhealthy"})
                return
            self._send_json(200, {"status": "ok"})
            return
        if path == "/":
            try:
                view = self.server.store.describe()  # type: ignore[attr-defined]
                self._send_json(200, view)
            except Exception:
                log.exception("view failed")
                self._send_json(500, {"error": "internal_error"})
            return
        self._send_json(404, {"error": "not_found", "message": path})

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        store: Store = self.server.store  # type: ignore[attr-defined]
        try:
            body = self._read_json()
            if path == "/snapshots":
                status, result = store.submit_snapshot(body)
            elif path == "/confirmations":
                # Test hooks, never used by production clients.
                status, result = store.confirm(
                    body,
                    crash_after_intent=bool(body.pop("_crash_after_intent", False)),
                    crash_after_commit=bool(body.pop("_crash_after_commit", False)),
                )
            else:
                self._send_json(404, {"error": "not_found", "message": path})
                return
            self._send_json(status, result)
        except RequestError as err:
            self._send_json(
                err.status_code, {"error": err.code, "message": err.message}
            )
        except Exception:
            log.exception("request failed")
            self._send_json(500, {"error": "internal_error"})


def serve(host: str, port: int) -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    store = build_store()
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.store = store  # type: ignore[attr-defined]
    log.info("partition handoff coordinator listening on %s:%d", host, port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        store.close()
