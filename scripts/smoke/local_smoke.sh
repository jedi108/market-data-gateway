#!/usr/bin/env bash
# Local single-node smoke: real binary, fake provider, loopback-only listeners.
#
# Runs the gateway with config/gateway.local.example.yaml (single-node cluster
# mdg-local, node-local; client 127.0.0.1:8088, peer 127.0.0.1:9443). Because
# the config is single-node, both listeners are loopback and no private-
# interface discovery is needed. The node token is a synthetic value; no real
# credential exists in this flow. No root, no VPN, no non-loopback bind.
#
# Self-contained: builds build/gateway itself and cleans up the sqlite state
# it created (build/mdg-local.sqlite) on exit. Exits non-zero on any violation.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

BIN="build/gateway"
CONFIG="config/gateway.local.example.yaml"
STATE="build/mdg-local.sqlite"
PORT=8088
URL="http://127.0.0.1:${PORT}"
NODE_TOKEN="local-smoke-token"   # synthetic value; never a real credential

WORK="$(mktemp -d)"
GW_PID=""
cleanup() {
  if [ -n "$GW_PID" ]; then
    kill -TERM "$GW_PID" 2>/dev/null || true
    for _ in $(seq 1 30); do
      if ! kill -0 "$GW_PID" 2>/dev/null; then break; fi
      sleep 0.1
    done
    kill -KILL "$GW_PID" 2>/dev/null || true
  fi
  rm -f "$STATE" "$STATE-shm" "$STATE-wal"
  rm -rf "$WORK"
}
trap cleanup EXIT

command -v go >/dev/null 2>&1 || { echo "FAIL: go not found on PATH"; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "FAIL: curl is required"; exit 1; }
[ -f "$CONFIG" ] || { echo "FAIL: missing $CONFIG (single-node loopback example config)"; exit 1; }
mkdir -p build

echo "== 1. build =="
CGO_ENABLED=0 go build -trimpath -o "$BIN" ./cmd/gateway

echo "== 2. start (fake provider, loopback 127.0.0.1:${PORT}) =="
GATEWAY_NODE_TOKEN="$NODE_TOKEN" "$BIN" -config "$CONFIG" -provider=fake -log-level DEBUG \
  > "$WORK/gateway.log" 2>&1 &
GW_PID=$!
for _ in $(seq 1 100); do
  if curl -sf "$URL/v1/health" > /dev/null 2>&1; then break; fi
  sleep 0.1
done
curl -sf "$URL/v1/health" | grep -q '"status":"ok"' \
  || { echo "FAIL: /v1/health"; cat "$WORK/gateway.log"; exit 1; }
curl -sf "$URL/v1/ready" | grep -q '"status":"ready"' \
  || { echo "FAIL: /v1/ready"; cat "$WORK/gateway.log"; exit 1; }

echo "== 3. synthetic candles request =="
curl -sf "$URL/v1/candles?venue=tbank&symbol=SBER&timeframe=1m&from_utc_ms=0&to_utc_ms=180000&limit=500&include_incomplete=false" \
  > "$WORK/candles.json" \
  || { echo "FAIL: candles request"; cat "$WORK/gateway.log"; exit 1; }
grep -q '"cache_status"' "$WORK/candles.json" \
  || { echo "FAIL: candles envelope missing cache_status"; cat "$WORK/candles.json"; exit 1; }

echo "== 4. metrics endpoint exposes gateway families =="
curl -sf "$URL/metrics" | grep -q '^# TYPE gateway_' \
  || { echo "FAIL: metrics families missing"; exit 1; }

echo "== 5. graceful stop =="
kill -TERM "$GW_PID" 2>/dev/null || true
if ! wait "$GW_PID"; then
  echo "FAIL: gateway exited non-zero on SIGTERM"
  tail -5 "$WORK/gateway.log"
  exit 1
fi
GW_PID=""
grep -q '"gateway stopped"' "$WORK/gateway.log" \
  || { echo "FAIL: no graceful stop log"; tail -5 "$WORK/gateway.log"; exit 1; }

echo "== 6. state cleanup =="
rm -f "$STATE" "$STATE-shm" "$STATE-wal"
if [ -e "$STATE" ]; then
  echo "FAIL: state file still present: $STATE"
  exit 1
fi

echo "LOCAL SMOKE PASSED"
