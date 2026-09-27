#!/usr/bin/env bash
# Verifies the "no cache hit on INFO" log-level contract with the real binary.
# Single-node loopback config, fake provider, synthetic token; self-contained
# (builds build/gateway itself, temp sqlite state removed on exit).
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

BIN="build/gateway"
WORK="$(mktemp -d)"
PORT=18089
URL="http://127.0.0.1:$PORT"
NODE_TOKEN="loglevel-smoke-token"   # synthetic value; never a real credential
PID=""

cleanup() {
  if [ -n "$PID" ]; then
    kill -TERM "$PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

command -v go >/dev/null 2>&1 || { echo "FAIL: go not found on PATH"; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "FAIL: curl is required"; exit 1; }

# Host private address for the peer-bind stand-in (the config loader requires
# a private non-loopback peer; the client listener stays on loopback).
discover_private_ipv4() {
  local candidates="" addr
  if candidates="$(hostname -I 2>/dev/null)"; then :; fi   # Linux
  if [ -z "$candidates" ] && command -v ipconfig >/dev/null 2>&1; then
    for addr in $(ifconfig -l 2>/dev/null); do
      local found=""
      found="$(ipconfig getifaddr "$addr" 2>/dev/null || true)"
      if [ -n "$found" ]; then candidates="$candidates $found"; fi
    done
  fi
  if [ -z "$candidates" ] && command -v ifconfig >/dev/null 2>&1; then
    candidates="$(ifconfig 2>/dev/null | awk '$1 == "inet" {print $2}')"
  fi
  for addr in $candidates; do
    case "$addr" in
      10.*|172.1[6-9].*|172.2[0-9].*|172.3[0-1].*|192.168.*) echo "$addr"; return 0 ;;
    esac
  done
  return 1
}
if ! PEER_HOST="$(discover_private_ipv4)"; then
  echo "FAIL: no private host address found for the smoke peer bind (got none)"
  exit 1
fi

mkdir -p build
CGO_ENABLED=0 go build -trimpath -o "$BIN" ./cmd/gateway

cat > "$WORK/gateway.yaml" <<EOF
cluster:
  cluster_id: smoke-cluster
  node_id: local
  routing_version: 1
  nodes:
    - {id: local, weight: 1, peer_url: "http://$PEER_HOST:19444"}
listeners:
  client: "127.0.0.1:$PORT"
  peer: "$PEER_HOST:19444"
auth:
  node_token_env: GATEWAY_NODE_TOKEN
providers:
  tbank:
    credential_group: smoke
    token_env: TBANK_MARKET_DATA_TOKEN
    safe_budget_per_minute: 200
    node_hard_budget_per_minute: 100
limits:
  request_timeout_ms: 1000
  max_request_limit: 500
  max_batch_items: 64
  batch_fan_out: 8
  cache_retention_ms: 3600000
  storage_path: $WORK/cache.sqlite
  queue_capacity: 64
  worker_count: 2
  max_retries: 1
  retry_base_delay_ms: 10
  per_client_quota_per_minute: 100
  max_tracked_clients: 16
  drain_timeout_ms: 2000
EOF

GATEWAY_NODE_TOKEN="$NODE_TOKEN" "$BIN" -config "$WORK/gateway.yaml" -provider fake -log-level INFO > "$WORK/log.jsonl" 2>&1 &
PID=$!
for _ in $(seq 1 50); do curl -sf "$URL/v1/health" > /dev/null 2>&1 && break; sleep 0.1; done
curl -sf "$URL/v1/health" > /dev/null 2>&1 \
  || { echo "FAIL: gateway did not become healthy"; cat "$WORK/log.jsonl"; exit 1; }
# 1 cold refresh + 4 memory hits
for _ in 1 2 3 4 5; do
  curl -sf "$URL/v1/candles?venue=tbank&symbol=SBER&timeframe=1m&from_utc_ms=0&to_utc_ms=180000&limit=500&include_incomplete=false" > /dev/null \
    || { echo "FAIL: candles request"; cat "$WORK/log.jsonl"; exit 1; }
done
kill -TERM "$PID" 2>/dev/null || true
wait "$PID" || true
PID=""
echo "--- full INFO log:"
cat "$WORK/log.jsonl"
if grep -qE '"memory coverage complete"|"gateway candle decision"|"persistent coverage complete"' "$WORK/log.jsonl"; then
  echo "FAIL: cache-hit events leaked to INFO"; exit 1
fi
if ! grep -q '"series refreshed from provider"' "$WORK/log.jsonl"; then
  echo "FAIL: expected lifecycle INFO for the cold refresh"; exit 1
fi
echo "PASS: no cache-hit events at INFO; lifecycle INFO present"
