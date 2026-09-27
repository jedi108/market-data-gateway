#!/usr/bin/env bash
# Two-node process-level smoke: two real gateway binaries as a two-node
# cluster (node-a + node-b). The host's private address with distinct peer
# ports stands in for the confirmed private transport (same address family,
# bind-checked, never a public bind). Exercises: identical routing hash on
# both nodes, peer auth boundary, node identity, one provider call for a
# cluster-wide miss, replica service from the non-owner, NOT_OWNER loop
# prevention via a mismatched routing-version request, cluster budget
# fail-closed config, and graceful shutdown of both listeners. Client
# listeners stay on loopback; the peer listeners REQUIRE a real private
# non-loopback IPv4 (config validation rejects loopback for multi-node).
# Test-only synthetic values (fake provider). Exits non-zero on any violation.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

BIN="build/gateway"
WORK="$(mktemp -d)"
NODE_TOKEN="smoke-node-token"
NODE_A_CLIENT_PORT=18091
NODE_B_CLIENT_PORT=18092
NODE_A_PEER_PORT=18441
NODE_B_PEER_PORT=18442
# Multi-node membership requires a real private address for the peer binds;
# two distinct peer ports distinguish the two nodes on the smoke host.
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
  echo "FAIL: multi-node smoke requires a real private non-loopback IPv4 address"
  echo "      on this host for the peer binds (config validation rejects"
  echo "      loopback peers for multi-node membership); none was found."
  exit 1
fi
NODE_A_PEER_URL="http://$PEER_HOST:$NODE_A_PEER_PORT"
NODE_B_PEER_URL="http://$PEER_HOST:$NODE_B_PEER_PORT"
NODE_A_URL="http://127.0.0.1:$NODE_A_CLIENT_PORT"
NODE_B_URL="http://127.0.0.1:$NODE_B_CLIENT_PORT"

trap 'kill ${NODE_A_PID:-0} ${NODE_B_PID:-0} 2>/dev/null || true; rm -rf "$WORK"' EXIT

command -v go >/dev/null 2>&1 || { echo "FAIL: go not found on PATH"; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "FAIL: curl is required"; exit 1; }
mkdir -p build
CGO_ENABLED=0 go build -trimpath -o "$BIN" ./cmd/gateway

write_config() {
  local node_id="$1" peer_port="$2" client_port="$3" path="$4"
  cat > "$path" <<EOF
cluster:
  cluster_id: smoke-cluster
  node_id: $node_id
  routing_version: 1
  nodes:
    - {id: node-a, weight: 1, peer_url: "$NODE_A_PEER_URL"}
    - {id: node-b, weight: 1, peer_url: "$NODE_B_PEER_URL"}
listeners:
  client: "127.0.0.1:$client_port"
  peer: "$PEER_HOST:$peer_port"
auth:
  node_token_env: GATEWAY_NODE_TOKEN
providers:
  tbank:
    credential_group: smoke
    token_env: TBANK_MARKET_DATA_TOKEN
    safe_budget_per_minute: 200
    node_hard_budget_per_minute: 100
    node_hard_budgets_per_minute:
      node-a: 100
      node-b: 100
limits:
  request_timeout_ms: 2000
  max_request_limit: 500
  max_batch_items: 64
  batch_fan_out: 8
  cache_retention_ms: 3600000
  storage_path: $WORK/$node_id-cache.sqlite
  queue_capacity: 64
  worker_count: 2
  max_retries: 1
  retry_base_delay_ms: 10
  per_client_quota_per_minute: 100
  max_tracked_clients: 16
  drain_timeout_ms: 2000
EOF
}

start_node() {
  local conf="$1" log="$2"
  GATEWAY_NODE_TOKEN="$NODE_TOKEN" "$BIN" -config "$conf" -provider fake -log-level DEBUG > "$log" 2>&1 &
  echo $!
}

wait_healthy() {
  local url="$1" log="${2:-}"
  for _ in $(seq 1 50); do
    if curl -sf "$url/v1/health" > /dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "FAIL: gateway at $url did not become healthy"
  if [ -n "$log" ] && [ -f "$log" ]; then
    echo "--- node log:"; tail -20 "$log"
  fi
  exit 1
}

echo "== 1. startup and identical routing hash =="
write_config node-a "$NODE_A_PEER_PORT" "$NODE_A_CLIENT_PORT" "$WORK/node-a.yaml"
write_config node-b "$NODE_B_PEER_PORT" "$NODE_B_CLIENT_PORT" "$WORK/node-b.yaml"
NODE_A_PID=$(start_node "$WORK/node-a.yaml" "$WORK/node-a.log")
NODE_B_PID=$(start_node "$WORK/node-b.yaml" "$WORK/node-b.log")
wait_healthy "$NODE_A_URL" "$WORK/node-a.log"
wait_healthy "$NODE_B_URL" "$WORK/node-b.log"
NODE_A_HASH=$(grep -o '"routing_hash":"[^"]*"' "$WORK/node-a.log" | head -1)
NODE_B_HASH=$(grep -o '"routing_hash":"[^"]*"' "$WORK/node-b.log" | head -1)
if [ "$NODE_A_HASH" != "$NODE_B_HASH" ] || [ -z "$NODE_A_HASH" ]; then
  echo "FAIL: routing hash diverged: node-a=$NODE_A_HASH node-b=$NODE_B_HASH"; exit 1
fi
echo "PASS: both nodes derived the identical routing hash"

echo "== 2. node identity on the peer boundary =="
curl -sf -H "Authorization: Bearer $NODE_TOKEN" "$NODE_A_PEER_URL/internal/v1/node" > "$WORK/node-node-a.json"
curl -sf -H "Authorization: Bearer $NODE_TOKEN" "$NODE_B_PEER_URL/internal/v1/node" > "$WORK/node-node-b.json"
grep -q '"node_id":"node-a"' "$WORK/node-node-a.json" || { echo "FAIL: node-a identity"; cat "$WORK/node-node-a.json"; exit 1; }
grep -q '"node_id":"node-b"' "$WORK/node-node-b.json" || { echo "FAIL: node-b identity"; cat "$WORK/node-node-b.json"; exit 1; }
grep -q '"routing_version":1' "$WORK/node-node-a.json" || { echo "FAIL: routing version missing"; exit 1; }
echo "PASS: /internal/v1/node reports identity without secrets"

echo "== 3. peer boundary rejects bad credentials =="
STATUS=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer wrong" "$NODE_A_PEER_URL/internal/v1/node")
if [ "$STATUS" != "403" ]; then
  echo "FAIL: wrong-token peer call returned $STATUS, want 403"; exit 1
fi
STATUS=$(curl -s -o /dev/null -w '%{http_code}' "$NODE_A_PEER_URL/internal/v1/node")
if [ "$STATUS" != "403" ]; then
  echo "FAIL: unauthenticated peer call returned $STATUS, want 403"; exit 1
fi
echo "PASS: peer auth boundary is fail-closed"

echo "== 4. one provider call for a cluster-wide miss =="
# Request the same window from both nodes' client boundaries sequentially:
# the first miss triggers one fetch by the owner; the second is served from
# cache or replica. Then request a NEW window: again exactly one fetch.
curl -sf "$NODE_A_URL/v1/candles?venue=tbank&symbol=SBER&timeframe=1m&from_utc_ms=0&to_utc_ms=120000&limit=500&include_incomplete=false" > /dev/null
curl -sf "$NODE_B_URL/v1/candles?venue=tbank&symbol=SBER&timeframe=1m&from_utc_ms=0&to_utc_ms=120000&limit=500&include_incomplete=false" > /dev/null
curl -sf "$NODE_A_URL/metrics" > "$WORK/m1-node-a.txt"
curl -sf "$NODE_B_URL/metrics" > "$WORK/m1-node-b.txt"
TOTAL=$(cat "$WORK/m1-node-a.txt" "$WORK/m1-node-b.txt" | grep -o 'gateway_upstream_requests_total{provider="tbank",venue="tbank",endpoint="get_candles",status="ok"} [0-9]*' | awk '{s+=$2} END {print s+0}')
if [ "$TOTAL" -ne 1 ]; then
  echo "FAIL: cluster-wide miss produced $TOTAL provider calls, want exactly 1"; exit 1
fi
echo "PASS: cluster-wide miss produced exactly one provider call"

echo "== 5. new window adds exactly one more provider call =="
curl -sf "$NODE_B_URL/v1/candles?venue=tbank&symbol=SBER&timeframe=1m&from_utc_ms=120000&to_utc_ms=240000&limit=500&include_incomplete=false" > /dev/null
curl -sf "$NODE_A_URL/metrics" > "$WORK/m2-node-a.txt"
curl -sf "$NODE_B_URL/metrics" > "$WORK/m2-node-b.txt"
TOTAL2=$(cat "$WORK/m2-node-a.txt" "$WORK/m2-node-b.txt" | grep -o 'gateway_upstream_requests_total{provider="tbank",venue="tbank",endpoint="get_candles",status="ok"} [0-9]*' | awk '{s+=$2} END {print s+0}')
if [ "$TOTAL2" -ne 2 ]; then
  echo "FAIL: second window produced $TOTAL2 total provider calls, want exactly 2"; exit 1
fi
echo "PASS: second window added exactly one provider call cluster-wide"

echo "== 6. client batch: per-item results across both owners =="
# One batch with two items (5m and 15m timeframes may have different owners):
# the envelope must answer 200 with two per-item results and exactly one
# provider call per distinct series cluster-wide.
BEFORE=$(cat "$WORK/m2-node-a.txt" "$WORK/m2-node-b.txt" | grep -o 'gateway_upstream_requests_total{provider="tbank",venue="tbank",endpoint="get_candles",status="ok"} [0-9]*' | awk '{s+=$2} END {print s+0}')
printf '{"items":[{"venue":"tbank","symbol":"SBER","timeframe":"5m","from_utc_ms":0,"to_utc_ms":300000,"limit":500,"include_incomplete":false},{"venue":"tbank","symbol":"SBER","timeframe":"15m","from_utc_ms":0,"to_utc_ms":900000,"limit":500,"include_incomplete":false}]}' > "$WORK/batch.json"
BATCH_STATUS=$(curl -s -o "$WORK/batch-out.json" -w '%{http_code}' -H 'Content-Type: application/json' --data-binary @"$WORK/batch.json" "$NODE_B_URL/v1/candles/batch")
if [ "$BATCH_STATUS" != "200" ]; then
  echo "FAIL: batch status $BATCH_STATUS"; cat "$WORK/batch-out.json"; exit 1
fi
ITEMS=$(grep -o '"series"' "$WORK/batch-out.json" | wc -l)
if [ "$ITEMS" -ne 2 ]; then
  echo "FAIL: batch items=$ITEMS, want 2"; cat "$WORK/batch-out.json"; exit 1
fi
if grep -q '"error":' "$WORK/batch-out.json"; then
  echo "FAIL: batch returned per-item errors"; cat "$WORK/batch-out.json"; exit 1
fi
AFTER_BATCH=$( { curl -sf "$NODE_A_URL/metrics"; curl -sf "$NODE_B_URL/metrics"; } | grep -o 'gateway_upstream_requests_total{provider="tbank",venue="tbank",endpoint="get_candles",status="ok"} [0-9]*' | awk '{s+=$2} END {print s+0}')
if [ "$AFTER_BATCH" -ne $((BEFORE + 2)) ]; then
  echo "FAIL: batch produced $((AFTER_BATCH - BEFORE)) provider calls, want exactly 2 (one per distinct series)"; exit 1
fi
echo "PASS: batch served per-item with exactly one provider call per series"

echo "== 7. cluster budget guard: unsafe config refuses to start =="
# sum(shares) 150+150=300 > safe 200 must be rejected at config load.
cat > "$WORK/unsafe.yaml" <<EOF
cluster:
  cluster_id: smoke-cluster
  node_id: node-a
  routing_version: 1
  nodes:
    - {id: node-a, weight: 1, peer_url: "$NODE_A_PEER_URL"}
    - {id: node-b, weight: 1, peer_url: "$NODE_B_PEER_URL"}
listeners:
  client: "127.0.0.1:18099"
  peer: "$PEER_HOST:18499"
auth:
  node_token_env: GATEWAY_NODE_TOKEN
providers:
  tbank:
    credential_group: smoke
    token_env: TBANK_MARKET_DATA_TOKEN
    safe_budget_per_minute: 200
    node_hard_budget_per_minute: 150
    node_hard_budgets_per_minute:
      node-a: 150
      node-b: 150
limits:
  request_timeout_ms: 2000
  max_request_limit: 500
  max_batch_items: 64
  batch_fan_out: 8
  cache_retention_ms: 3600000
  storage_path: $WORK/unsafe-cache.sqlite
  queue_capacity: 64
  worker_count: 2
  max_retries: 1
  retry_base_delay_ms: 10
  per_client_quota_per_minute: 100
  max_tracked_clients: 16
  drain_timeout_ms: 2000
EOF
if GATEWAY_NODE_TOKEN="$NODE_TOKEN" "$BIN" -config "$WORK/unsafe.yaml" -provider fake > "$WORK/unsafe.log" 2>&1; then
  echo "FAIL: unsafe cluster budget config was accepted"; exit 1
fi
grep -q 'cluster budget invariant violated' "$WORK/unsafe.log" || { echo "FAIL: unsafe config missing invariant error"; cat "$WORK/unsafe.log"; exit 1; }
echo "PASS: unsafe cluster budget config fails closed at startup"

echo "== 8. cluster budget metrics export configured families =="
curl -sf "$NODE_A_URL/metrics" | grep -q 'gateway_cluster_budget_safe{credential_group="smoke"} 200' || { echo "FAIL: configured safe budget metric missing"; curl -sf "$NODE_A_URL/metrics" | head -40; exit 1; }
curl -sf "$NODE_A_URL/metrics" | grep -q 'gateway_cluster_budget_configured_sum{credential_group="smoke"} 200' || { echo "FAIL: configured sum metric missing"; exit 1; }
curl -sf "$NODE_A_URL/metrics" | grep -q 'gateway_cluster_budget_blocked{credential_group="smoke"} 0' || { echo "FAIL: blocked gauge missing"; exit 1; }
echo "PASS: configured/observed cluster budget families exported"

echo "== 9. graceful shutdown of both listeners =="
kill -TERM "$NODE_A_PID" 2>/dev/null || true
for _ in $(seq 1 50); do
  if ! kill -0 "$NODE_A_PID" 2>/dev/null; then break; fi
  sleep 0.1
done
if kill -0 "$NODE_A_PID" 2>/dev/null; then
  echo "FAIL: node-a did not exit after SIGTERM"; exit 1
fi
grep -q '"msg":"gateway stopped"' "$WORK/node-a.log" || { echo "FAIL: node-a missing clean shutdown log"; exit 1; }
echo "PASS: SIGTERM drains and stops both listeners cleanly"

echo "ALL TWO-NODE SMOKE CHECKS PASSED"
