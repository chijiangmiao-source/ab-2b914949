#!/usr/bin/env bash
#
# One-shot verification container entrypoint.
#
#   1. rule tests (member replacement, invalid/stale/unauthorized/extra
#      confirmations, idempotency, reconvergence and crash recovery)
#   2. wait for the app service health endpoint
#   3. API smoke against the running service
#
# Exits non-zero on the first failure; Compose therefore reports the run's
# status via the container exit code.
set -euo pipefail

cd "$(dirname "$0")/.."

APP_URL="${APP_URL:-http://app:8080}"

echo "==> [1/3] rule tests (replacement, invalid acks, interruption recovery)"
python -m unittest discover -s tests -v

echo "==> [2/3] wait for ${APP_URL}/healthz"
for i in $(seq 1 60); do
  if python - "$APP_URL" <<'PY'
import json, sys, urllib.request
url = sys.argv[1].rstrip("/") + "/healthz"
try:
    with urllib.request.urlopen(url, timeout=2) as resp:
        body = json.load(resp)
    sys.exit(0 if resp.status == 200 and body.get("status") == "ok" else 1)
except Exception:
    sys.exit(1)
PY
  then
    echo "    service healthy"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "    service did not become healthy" >&2
    exit 1
  fi
  sleep 1
done

echo "==> [3/3] API smoke against ${APP_URL}"
SMOKE_BASE_URL="${APP_URL}" python scripts/smoke.py

echo "==> VERIFY OK"
