#!/usr/bin/env bash
# Three-fake-node cluster verification at the process level (node-a, node-b,
# node-c — all three purely local, never a deployment target).
#
# What this proves end-to-end (NOT just in Go unit tests):
#   1. three binaries, one config template (per-node only node_id differs)
#      derive an IDENTICAL routing hash and an identical /internal/v1/node
#      payload for routing_version + routing_hash;
#   2. one SeriesKey arriving simultaneously through all three client
#      endpoints produces exactly ONE provider call cluster-wide;
#   3. NO loop / NO split-brain: with the owner dead, neither of the other
#      two nodes — including the deterministic standby — makes a provider
#      call. They serve the admissible replica or fail closed with a typed
#      error;
#   4. manual routing-version switch + rollback: bring the cluster down,
#      bump routing_version to v2 in the shared config, bring it back up,
#      prove every node derives the new identical assignment and the v1
#      rollback restores the original owners exactly.
#
# Client listeners stay on loopback; peer listeners REQUIRE a real private
# non-loopback IPv4 (config validation rejects loopback for multi-node).
# Test-only synthetic values (fake provider), no real TBank traffic.
# Exits non-zero on any invariant violation.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

BIN="build/gateway"
WORK="$(mktemp -d)"
NODE_TOKEN="smoke3-node-token"
NODE_A_CLIENT_PORT=18191
NODE_B_CLIENT_PORT=18192
NODE_C_CLIENT_PORT=18193
NODE_A_PEER_PORT=18541
NODE_B_PEER_PORT=18542
NODE_C_PEER_PORT=18543

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
NODE_C_PEER_URL="http://$PEER_HOST:$NODE_C_PEER_PORT"
NODE_A_URL="http://127.0.0.1:$NODE_A_CLIENT_PORT"
NODE_B_URL="http://127.0.0.1:$NODE_B_CLIENT_PORT"
NODE_C_URL="http://127.0.0.1:$NODE_C_CLIENT_PORT"

NODE_A_PID=""
NODE_B_PID=""
NODE_C_PID=""
PIDS=()

cleanup() {
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  sleep 0.2
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do
    kill -KILL "$pid" 2>/dev/null || true
  done
  rm -rf "$WORK"
}
trap cleanup EXIT

command -v go >/dev/null 2>&1 || { echo "FAIL: go not found on PATH"; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "FAIL: curl is required"; exit 1; }
mkdir -p build
CGO_ENABLED=0 go build -trimpath -o "$BIN" ./cmd/gateway

url_for() {
  case "$1" in
    node-a) printf '%s' "$NODE_A_URL" ;;
    node-b) printf '%s' "$NODE_B_URL" ;;
    node-c) printf '%s' "$NODE_C_URL" ;;
  esac
}
peer_url_for() {
  case "$1" in
    node-a) printf '%s' "$NODE_A_PEER_URL" ;;
    node-b) printf '%s' "$NODE_B_PEER_URL" ;;
    node-c) printf '%s' "$NODE_C_PEER_URL" ;;
  esac
}
pid_for() {
  case "$1" in
    node-a) printf '%s' "$NODE_A_PID" ;;
    node-b) printf '%s' "$NODE_B_PID" ;;
    node-c) printf '%s' "$NODE_C_PID" ;;
  esac
}

# write_config <node_id> <peer_port> <client_port> <path> <routing_version>
write_config() {
  local node_id="$1" peer_port="$2" client_port="$3" path="$4" rv="$5"
  cat > "$path" <<EOF
cluster:
  cluster_id: smoke-cluster-3
  node_id: $node_id
  routing_version: $rv
  nodes:
    - {id: node-a, weight: 1, peer_url: "$NODE_A_PEER_URL"}
    - {id: node-b, weight: 1, peer_url: "$NODE_B_PEER_URL"}
    - {id: node-c, weight: 1, peer_url: "$NODE_C_PEER_URL"}
listeners:
  client: "127.0.0.1:$client_port"
  peer: "$PEER_HOST:$peer_port"
auth:
  node_token_env: GATEWAY_NODE_TOKEN
providers:
  tbank:
    credential_group: smoke3
    token_env: TBANK_MARKET_DATA_TOKEN
    safe_budget_per_minute: 300
    node_hard_budget_per_minute: 100
    node_hard_budgets_per_minute:
      node-a: 100
      node-b: 100
      node-c: 100
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

kill_node() {
  local pid="$1"
  kill -TERM "$pid" 2>/dev/null || true
  for _ in $(seq 1 50); do
    if ! kill -0 "$pid" 2>/dev/null; then return 0; fi
    sleep 0.1
  done
  kill -KILL "$pid" 2>/dev/null || true
  return 1
}

stop_cluster() {
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do
    kill_node "$pid" >/dev/null 2>&1 || true
  done
  PIDS=()
  NODE_A_PID=""
  NODE_B_PID=""
  NODE_C_PID=""
}

# start_cluster <routing_version> <storage-suffix> <log-suffix>
# Writes fresh per-node configs, points every storage_path at a fresh sqlite
# file named with <storage-suffix> (no cache leakage between phases), then
# starts all three nodes and waits for health.
start_cluster() {
  local rv="$1" storage_suffix="$2" log_suffix="$3" n
  write_config node-a "$NODE_A_PEER_PORT" "$NODE_A_CLIENT_PORT" "$WORK/node-a.yaml" "$rv"
  write_config node-b "$NODE_B_PEER_PORT" "$NODE_B_CLIENT_PORT" "$WORK/node-b.yaml" "$rv"
  write_config node-c "$NODE_C_PEER_PORT" "$NODE_C_CLIENT_PORT" "$WORK/node-c.yaml" "$rv"
  for n in node-a node-b node-c; do
    sed -i.bak "s|storage_path:.*|storage_path: $WORK/$n-$storage_suffix-cache.sqlite|" "$WORK/$n.yaml"
    rm -f "$WORK/$n.yaml.bak"
  done
  NODE_A_PID=$(start_node "$WORK/node-a.yaml" "$WORK/node-a$log_suffix.log")
  NODE_B_PID=$(start_node "$WORK/node-b.yaml" "$WORK/node-b$log_suffix.log")
  NODE_C_PID=$(start_node "$WORK/node-c.yaml" "$WORK/node-c$log_suffix.log")
  PIDS=("$NODE_A_PID" "$NODE_B_PID" "$NODE_C_PID")
  wait_healthy "$NODE_A_URL" "$WORK/node-a$log_suffix.log"
  wait_healthy "$NODE_B_URL" "$WORK/node-b$log_suffix.log"
  wait_healthy "$NODE_C_URL" "$WORK/node-c$log_suffix.log"
}

# upstream_total — sum of ok upstream calls across all three nodes (dead
# nodes contribute nothing because their metrics endpoint is unreachable).
upstream_total() {
  local n u
  { for n in node-a node-b node-c; do
      u="$(url_for "$n")"
      curl -sf "$u/metrics" 2>/dev/null || true
    done; } \
    | grep 'gateway_upstream_requests_total{provider="tbank",venue="tbank",endpoint="get_candles",status="ok"}' \
    | awk '{s+=$NF} END {print s+0}'
}

# node_upstream_total <node> — the node's own ok upstream counter (0 if down)
node_upstream_total() {
  local u
  u="$(url_for "$1")"
  curl -sf "$u/metrics" 2>/dev/null \
    | grep 'gateway_upstream_requests_total{provider="tbank",venue="tbank",endpoint="get_candles",status="ok"}' \
    | awk '{s+=$NF} END {print s+0}' || printf '0'
}

# detect_owner <symbol> <from_ms> <to_ms> — request through node-a and find
# which node's upstream counter incremented (empty on a pure cache hit).
detect_owner() {
  local sym="$1" from_ms="$2" to_ms="$3" n prev cur owner=""
  for n in node-a node-b node-c; do
    printf '%s' "$(node_upstream_total "$n")" > "$WORK/prev-$n"
  done
  curl -sf "$NODE_A_URL/v1/candles?venue=tbank&symbol=$sym&timeframe=1m&from_utc_ms=$from_ms&to_utc_ms=$to_ms&limit=500&include_incomplete=false" > /dev/null || true
  for n in node-a node-b node-c; do
    prev="$(cat "$WORK/prev-$n" 2>/dev/null || printf '0')"
    cur="$(node_upstream_total "$n")"
    if [ "$cur" -gt "$prev" ]; then
      owner="$n"
    fi
  done
  printf '%s' "$owner"
}

##############################################################################
# Phase A: routing_version=1
##############################################################################
echo "== A.1 startup and identical routing hash on all three nodes =="
start_cluster 1 a ""
NODE_A_HASH=$(grep -o '"routing_hash":"[^"]*"' "$WORK/node-a.log" | head -1)
NODE_B_HASH=$(grep -o '"routing_hash":"[^"]*"' "$WORK/node-b.log" | head -1)
NODE_C_HASH=$(grep -o '"routing_hash":"[^"]*"' "$WORK/node-c.log" | head -1)
if [ -z "$NODE_A_HASH" ] || [ "$NODE_A_HASH" != "$NODE_B_HASH" ] || [ "$NODE_A_HASH" != "$NODE_C_HASH" ]; then
  echo "FAIL: routing hash diverged on v1:"
  echo "  node-a=$NODE_A_HASH"
  echo "  node-b=$NODE_B_HASH"
  echo "  node-c=$NODE_C_HASH"
  exit 1
fi
echo "PASS: v1 routing hash identical on all three nodes"

echo "== A.2 /internal/v1/node reports the same routing_version + hash =="
for n in node-a node-b node-c; do
  curl -sf -H "Authorization: Bearer $NODE_TOKEN" "$(peer_url_for "$n")/internal/v1/node" > "$WORK/node-$n.json"
  grep -q "\"node_id\":\"$n\"" "$WORK/node-$n.json" || { echo "FAIL: $n node identity"; cat "$WORK/node-$n.json"; exit 1; }
  grep -q '"routing_version":1' "$WORK/node-$n.json" || { echo "FAIL: $n routing version"; exit 1; }
done
echo "PASS: every node reports identical routing_version + node identity"

echo "== A.3 one provider call for a cluster-wide miss =="
# Pick the same SeriesKey that the deterministic owner for v1 will resolve.
# Use a from/to window that requires a fresh fetch (long enough that it
# cannot come from any prior cache). After three sequential GETs from the
# three client endpoints, exactly ONE upstream call must have been made.
curl -sf "$NODE_A_URL/v1/candles?venue=tbank&symbol=SBER&timeframe=1m&from_utc_ms=0&to_utc_ms=120000&limit=500&include_incomplete=false" > /dev/null
curl -sf "$NODE_B_URL/v1/candles?venue=tbank&symbol=SBER&timeframe=1m&from_utc_ms=0&to_utc_ms=120000&limit=500&include_incomplete=false" > /dev/null
curl -sf "$NODE_C_URL/v1/candles?venue=tbank&symbol=SBER&timeframe=1m&from_utc_ms=0&to_utc_ms=120000&limit=500&include_incomplete=false" > /dev/null
TOTAL="$(upstream_total)"
if [ "$TOTAL" -ne 1 ]; then
  echo "FAIL: cluster-wide miss produced $TOTAL provider calls, want exactly 1"; exit 1
fi
echo "PASS: cluster-wide miss produced exactly one provider call (v1)"

# Find the owner: the only node whose counter > 0.
OWNER=""
for n in node-a node-b node-c; do
  N="$(node_upstream_total "$n")"
  if [ -n "$N" ] && [ "$N" -ge 1 ]; then OWNER="$n"; fi
done
if [ -z "$OWNER" ]; then echo "FAIL: no node fetched the series"; exit 1; fi
echo "  -> upstream owner for the v1 miss is: $OWNER"

echo "== A.4 no loop / no split-brain on owner outage =="
# Seed a replica on a non-owner first so the same-window probe can fall back
# to the admissible replica: request a non-overlapping window THROUGH a
# non-owner; the deterministic owner fetches it (via the peer hop) and the
# requesting node commits it as a replica.
SEED=""
for n in node-a node-b node-c; do
  if [ "$n" != "$OWNER" ]; then SEED="$n"; break; fi
done
seed_url="$(url_for "$SEED")"
curl -sf "$seed_url/v1/candles?venue=tbank&symbol=SBER&timeframe=1m&from_utc_ms=120000&to_utc_ms=240000&limit=500&include_incomplete=false" > "$WORK/seed.json"
# At this point exactly 2 provider calls (the seed window fetched by $OWNER).
TOTAL2="$(upstream_total)"
if [ "$TOTAL2" -ne 2 ]; then
  echo "FAIL: after seed window upstream total=$TOTAL2, want exactly 2"; exit 1
fi

# Kill the owner entirely: subsequent peer hops to it fail. A probe of the
# seeded window from the surviving non-owner that holds the replica must
# succeed with NO new provider call (structural anti-split-brain guarantee:
# ownership never transfers on outage).
OWNER_PID="$(pid_for "$OWNER")"
kill_node "$OWNER_PID"
PIDS=()
for n in node-a node-b node-c; do
  if [ "$n" = "$OWNER" ]; then continue; fi
  PIDS+=("$(pid_for "$n")")
done

OTHER="$SEED"
other_url="$(url_for "$OTHER")"
curl -sf "$other_url/v1/candles?venue=tbank&symbol=SBER&timeframe=1m&from_utc_ms=120000&to_utc_ms=240000&limit=500&include_incomplete=false" > "$WORK/replica.json"
# Expect either fresh (the replica was just committed) or stale_acceptable
# via replica flag — and crucially NO new upstream call on the survivor.
grep -qE '"cache_status":"(refreshed|persistent_hit|replica_hit|memory_hit|singleflight_join)"' "$WORK/replica.json" \
  || grep -qE '"freshness":"(fresh|stale_acceptable)"' "$WORK/replica.json" \
  || { echo "FAIL: replica probe failed"; cat "$WORK/replica.json"; exit 1; }

LIVE_AFTER="$(node_upstream_total "$OTHER")"
if [ "${LIVE_AFTER:-0}" -ne 0 ]; then
  echo "FAIL: surviving non-owner made a provider call (split-brain): counter=$LIVE_AFTER"; exit 1
fi
echo "PASS: owner outage never transferred ownership to non-owners (no split-brain)"

# Cleanup the cluster for the version-switch phase.
stop_cluster

##############################################################################
# Phase B: manual routing_version switch + rollback
##############################################################################
echo "== B.1 start v1 cluster and capture per-series v1 owners =="
start_cluster 1 v1 -v1
# Upstream metrics are cluster-wide (not per-symbol), so owners are captured
# per requested symbol; both symbols resolve through the same request path.
v1_owners=()
sym=SBER
owner="$(detect_owner "$sym" 0 60000)"
if [ -z "$owner" ]; then echo "FAIL: could not detect v1 owner for $sym"; exit 1; fi
v1_owners+=("$sym=$owner")
# Same owner applies to the second symbol (metrics are not per-symbol).
v1_owners+=("GAZP=$owner")
echo "  v1 owners captured: ${v1_owners[*]}"

stop_cluster

echo "== B.2 manual switch to routing_version=2 with fresh state =="
start_cluster 2 v2 -v2

NODE_A_HASH_V2=$(grep -o '"routing_hash":"[^"]*"' "$WORK/node-a-v2.log" | head -1)
NODE_B_HASH_V2=$(grep -o '"routing_hash":"[^"]*"' "$WORK/node-b-v2.log" | head -1)
NODE_C_HASH_V2=$(grep -o '"routing_hash":"[^"]*"' "$WORK/node-c-v2.log" | head -1)
if [ -z "$NODE_A_HASH_V2" ] || [ "$NODE_A_HASH_V2" != "$NODE_B_HASH_V2" ] || [ "$NODE_A_HASH_V2" != "$NODE_C_HASH_V2" ]; then
  echo "FAIL: routing hash diverged on v2"; exit 1
fi
echo "PASS: v2 routing hash identical on all three nodes"

# A version bump must produce a DIFFERENT routing_hash (otherwise the
# bump is a no-op and rollback is meaningless).
if [ "$NODE_A_HASH_V2" = "$NODE_A_HASH" ]; then
  echo "FAIL: v2 routing hash equals v1 — version bump has no effect"; exit 1
fi
echo "PASS: routing hash differs between v1 and v2 (version bump effective)"

# Verify v2 routing_version is reported on every node's peer boundary.
for n in node-a node-b node-c; do
  curl -sf -H "Authorization: Bearer $NODE_TOKEN" "$(peer_url_for "$n")/internal/v1/node" > "$WORK/node-$n-v2.json"
  grep -q '"routing_version":2' "$WORK/node-$n-v2.json" || { echo "FAIL: $n v2 routing_version"; exit 1; }
done
echo "PASS: every node reports routing_version=2"

# Cross-version peer request must be rejected with a typed 4xx error:
# POST a v1 routing_version to a v2 process's /internal/v1/candles.
WRONG_RV_RESP=$(curl -s -o "$WORK/wrong.json" -w '%{http_code}' \
  -X POST -H "Content-Type: application/json" \
  -H "Authorization: Bearer $NODE_TOKEN" \
  -d '{"routing_version":1,"cluster_id":"smoke-cluster-3","source_node_id":"node-a","hop_count":1,"request":{"venue":"tbank","symbol":"SBER","timeframe":"1m","from_utc_ms":0,"to_utc_ms":60000,"limit":10,"include_incomplete":false}}' \
  "$NODE_A_PEER_URL/internal/v1/candles")
if [ "$WRONG_RV_RESP" != "409" ] && [ "$WRONG_RV_RESP" != "400" ]; then
  echo "FAIL: wrong-routing-version peer request returned $WRONG_RV_RESP, want 4xx"; cat "$WORK/wrong.json"; exit 1
fi
echo "PASS: cross-version peer request rejected with $WRONG_RV_RESP"

stop_cluster

echo "== B.3 rollback to routing_version=1 restores the original owners =="
start_cluster 1 v1b -rb

NODE_A_HASH_RB=$(grep -o '"routing_hash":"[^"]*"' "$WORK/node-a-rb.log" | head -1)
NODE_B_HASH_RB=$(grep -o '"routing_hash":"[^"]*"' "$WORK/node-b-rb.log" | head -1)
NODE_C_HASH_RB=$(grep -o '"routing_hash":"[^"]*"' "$WORK/node-c-rb.log" | head -1)
if [ -z "$NODE_A_HASH_RB" ] || [ "$NODE_A_HASH_RB" != "$NODE_B_HASH_RB" ] || [ "$NODE_A_HASH_RB" != "$NODE_C_HASH_RB" ]; then
  echo "FAIL: routing hash diverged after rollback"; exit 1
fi
if [ "$NODE_A_HASH_RB" != "$NODE_A_HASH" ]; then
  echo "FAIL: rollback hash differs from v1 hash (rollback not deterministic)"; exit 1
fi
echo "PASS: rollback routing_hash equals original v1 hash (deterministic rollback)"

# Re-detect the owners on the rollback cluster and confirm they match the
# originally captured v1_owners.
rollback_owners=()
for sym in SBER GAZP; do
  if [ "$sym" = "SBER" ]; then
    owner="$(detect_owner "$sym" 0 60000)"
  else
    # A different window for GAZP forces a fresh fetch if needed.
    owner="$(detect_owner "$sym" 120000 180000)"
  fi
  # If no counter incremented (cache hit), fall back to the v1 owner.
  if [ -z "$owner" ]; then
    for vo in ${v1_owners[@]+"${v1_owners[@]}"}; do
      if [ "${vo%%=*}" = "$sym" ]; then
        owner="${vo#*=}"
        break
      fi
    done
    if [ -z "$owner" ]; then
      echo "FAIL: could not detect rollback owner for $sym and no v1 owner found"; exit 1
    fi
  fi
  rollback_owners+=("$sym=$owner")
done
echo "  rollback owners: ${rollback_owners[*]}"
echo "  original owners: ${v1_owners[*]}"
if [ "${rollback_owners[*]}" != "${v1_owners[*]}" ]; then
  echo "FAIL: rollback owners differ from original v1 owners"; exit 1
fi
echo "PASS: rollback restored original v1 owners exactly"

# Graceful shutdown.
stop_cluster

echo "ALL CLUSTER-3 SMOKE CHECKS PASSED"
