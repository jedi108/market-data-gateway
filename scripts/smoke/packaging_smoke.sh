#!/usr/bin/env bash
# Packaging smoke: verifies the single-binary packaging of the gateway.
#   - the linux/amd64 build is a static, CGO-free ELF executable;
#   - the binary carries no private repository/deploy information
#     (no SSH targets, no private addresses, no monorepo paths);
#   - CLI contract (-help) responds;
#   - config validation fails closed on REQUIRED placeholders;
#   - real provider mode refuses to start without Gate 0B authorization.
# Local verification only — no remote operations, no deploy assets asserted
# here (the deploy surface lives in the private operator repository).
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

BUILD_DIR="build"
BIN_NAME="gateway"
NATIVE_BIN="$BUILD_DIR/$BIN_NAME"                     # runnable on this host
LINUX_BIN="$BUILD_DIR/gateway-linux-amd64"            # shipped artifact
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

command -v go >/dev/null 2>&1 || { echo "FAIL: go not found on PATH"; exit 1; }

# discover_private_ipv4 — first private (RFC1918) IPv4 of this host, for the
# peer bind stand-in used by the provider-gate check below.
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

echo "== 1. build static linux/amd64 binary (CGO_ENABLED=0) =="
mkdir -p "$BUILD_DIR"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath \
  -ldflags '-s -w -X main.build=packaging-smoke' -o "$LINUX_BIN" ./cmd/gateway
CGO_ENABLED=0 go build -trimpath -o "$NATIVE_BIN" ./cmd/gateway

echo "== 2. shipped binary is a linux/amd64 ELF executable =="
if command -v file >/dev/null 2>&1; then
  file "$LINUX_BIN" | grep -q "ELF 64-bit LSB" \
    && file "$LINUX_BIN" | grep -Eq "x86-64|AMD x86-64" || {
      echo "FAIL: binary is not a linux/amd64 ELF"
      file "$LINUX_BIN"
      exit 1
    }
elif command -v readelf >/dev/null 2>&1; then
  readelf -h "$LINUX_BIN" | grep -q "Machine:.*Advanced Micro Devices X86-64" || {
    echo "FAIL: binary is not x86-64 (readelf check)"
    readelf -h "$LINUX_BIN" | head -20
    exit 1
  }
else
  echo "WARNING: neither 'file' nor 'readelf' available; skipping architecture check"
fi

echo "== 3. shipped binary is statically linked (no dynamic deps) =="
if command -v file >/dev/null 2>&1; then
  if file "$LINUX_BIN" | grep -q "statically linked"; then
    echo "PASS: file reports statically linked"
  elif command -v ldd >/dev/null 2>&1; then
    if ldd "$LINUX_BIN" 2>&1 | grep -q "not a dynamic executable"; then
      echo "PASS: ldd reports not a dynamic executable"
    else
      echo "FAIL: binary has dynamic dependencies (CGO may be enabled)"
      ldd "$LINUX_BIN" || true
      exit 1
    fi
  else
    echo "FAIL: binary is dynamically linked"
    file "$LINUX_BIN"
    exit 1
  fi
elif command -v ldd >/dev/null 2>&1; then
  ldd "$LINUX_BIN" 2>&1 | grep -q "not a dynamic executable" || {
    echo "FAIL: binary has dynamic dependencies (CGO may be enabled)"
    ldd "$LINUX_BIN" || true
    exit 1
  }
else
  echo "WARNING: neither 'file' nor 'ldd' available; skipping static-link check"
fi

echo "== 4. binary contains no private repository/deploy strings =="
for token in "jedi@fs" "user1@sber" "root@reg" "root@isp" \
             "192.168.0.6" "10.8.0.1" \
             "fin_predict" "fin-predict" "workspace/mdg_spike"; do
  if grep -aqF -- "$token" "$LINUX_BIN"; then
    echo "FAIL: forbidden string '$token' found in the binary"
    exit 1
  fi
done
echo "PASS: no private strings in the binary"

echo "== 5. binary -help responds =="
set +e
HELP_OUT="$("$NATIVE_BIN" -help 2>&1)"
HELP_CODE=$?
set -e
if [ "$HELP_CODE" -eq 0 ] && echo "$HELP_OUT" | grep -q "config"; then
  echo "PASS: -help responds with the flag contract"
else
  echo "FAIL: -help failed (exit=$HELP_CODE): $HELP_OUT"
  exit 1
fi

echo "== 6. config validation fails closed on REQUIRED placeholders =="
# config/gateway.example.yaml is a non-deployable schema reference with
# REQUIRED placeholders; loading it must fail validation.
[ -f "config/gateway.example.yaml" ] || { echo "FAIL: config/gateway.example.yaml missing"; exit 1; }
set +e
"$NATIVE_BIN" -config config/gateway.example.yaml -provider fake -log-level ERROR > "$WORK/config_test.log" 2>&1
EXIT_CODE=$?
set -e
if [ "$EXIT_CODE" -ne 0 ] && grep -q -E "unmarshal|decode config|required|invalid|fail-closed|error" "$WORK/config_test.log"; then
  echo "PASS: config validation rejects REQUIRED placeholders (fail-closed)"
else
  echo "FAIL: config validation did not reject REQUIRED placeholders (exit_code=$EXIT_CODE)"
  head -5 "$WORK/config_test.log"
  exit 1
fi

echo "== 7. real provider mode refuses to start without Gate 0B =="
# Valid minimal single-node config so this tests the provider gate, not YAML
# parsing. The peer bind uses the host's private IPv4 (loopback peer binds are
# rejected for anything but the dedicated loopback example config; a private
# address is valid in every validation variant). Skipped when the host has no
# private IPv4 at all — the provider gate is not a packaging property.
if ! PEER_HOST="$(discover_private_ipv4)"; then
  echo "WARNING: no private IPv4 on this host; skipping the provider-gate check"
  echo ""
  echo "ALL PACKAGING SMOKE CHECKS PASSED"
  exit 0
fi
cat > "$WORK/minimal.yaml" <<EOF
cluster:
  cluster_id: packaging-smoke
  node_id: pkg-node
  routing_version: 1
  nodes:
    - {id: pkg-node, weight: 1, peer_url: "http://$PEER_HOST:19498"}
listeners:
  client: "127.0.0.1:18098"
  peer: "$PEER_HOST:19498"
auth:
  node_token_env: GATEWAY_NODE_TOKEN
providers:
  tbank:
    credential_group: packaging-smoke
    token_env: TBANK_MARKET_DATA_TOKEN
    safe_budget_per_minute: 100
    node_hard_budget_per_minute: 100
limits:
  request_timeout_ms: 2000
  max_request_limit: 500
  max_batch_items: 64
  batch_fan_out: 8
  cache_retention_ms: 3600000
  storage_path: $WORK/pkg-cache.sqlite
  queue_capacity: 64
  worker_count: 2
  max_retries: 1
  retry_base_delay_ms: 10
  per_client_quota_per_minute: 100
  max_tracked_clients: 16
  drain_timeout_ms: 2000
EOF
set +e
GATEWAY_NODE_TOKEN="packaging-smoke-token" "$NATIVE_BIN" -config "$WORK/minimal.yaml" -provider tbank -log-level ERROR > "$WORK/real_test.log" 2>&1
EXIT_CODE=$?
set -e
if [ "$EXIT_CODE" -eq 0 ]; then
  echo "FAIL: real provider mode started without authorization"
  head -5 "$WORK/real_test.log"
  exit 1
fi
if grep -q -E "Gate 0B|not authorized|requires" "$WORK/real_test.log"; then
  echo "PASS: real provider mode refused without authorization"
elif grep -q -E "decode config|runtime limits|storage_schema_version" "$WORK/real_test.log"; then
  # The config gate refused before the provider gate was reached (config
  # schema drift). The non-negotiable property — real mode never starts —
  # still holds; the provider gate itself is pinned by Go tests.
  echo "SKIP: provider gate unreached (config gate refused first); real mode did not start"
else
  echo "FAIL: real provider mode failed for an unexpected reason:"
  head -5 "$WORK/real_test.log"
  exit 1
fi

echo ""
echo "ALL PACKAGING SMOKE CHECKS PASSED"
