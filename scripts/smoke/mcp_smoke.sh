set -euo pipefail

# MCP stdio smoke: fake-provider gateway on loopback + gateway-mcp over stdio.
# Runs initialize, tools/list and one get_volatility call through a FIFO, then
# closes stdin and requires a clean exit. No token, no network beyond loopback.

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

BIN="build/gateway"
MCP_BIN="build/gateway-mcp"
CONFIG="config/gateway.local.example.yaml"
STATE="build/mdg-local.sqlite"
PORT=8088
URL="http://127.0.0.1:${PORT}"
NODE_TOKEN="local-smoke-token"   # synthetic value; never a real credential

WORK="$(mktemp -d)"
GW_PID=""
MCP_PID=""
cleanup() {
  exec 3>&- 2>/dev/null || true
  for pid in "$MCP_PID" "$GW_PID"; do
    [ -n "$pid" ] || continue
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 30); do
      if ! kill -0 "$pid" 2>/dev/null; then break; fi
      sleep 0.1
    done
    kill -KILL "$pid" 2>/dev/null || true
  done
  rm -f "$STATE" "$STATE-shm" "$STATE-wal"
  rm -rf "$WORK"
}
trap cleanup EXIT

fail() { echo "FAIL: $1"; echo "--- mcp stderr"; cat "$WORK/mcp.log" 2>/dev/null || true; echo "--- mcp stdout"; cat "$WORK/out" 2>/dev/null || true; exit 1; }

mkdir -p build
echo "== 1. build gateway + gateway-mcp =="
CGO_ENABLED=0 go build -trimpath -o "$BIN" ./cmd/gateway
CGO_ENABLED=0 go build -trimpath -o "$MCP_BIN" ./cmd/mcp

echo "== 2. start gateway (fake provider, loopback 127.0.0.1:${PORT}) =="
GATEWAY_NODE_TOKEN="$NODE_TOKEN" "$BIN" -config "$CONFIG" -provider=fake > "$WORK/gateway.log" 2>&1 &
GW_PID=$!
for _ in $(seq 1 100); do
  if curl -sf "$URL/v1/health" > /dev/null 2>&1; then break; fi
  sleep 0.1
done
curl -sf "$URL/v1/health" > /dev/null || { cat "$WORK/gateway.log"; fail "gateway did not start"; }

echo "== 3. start gateway-mcp over stdio (no token in env) =="
mkfifo "$WORK/in"
env -u GATEWAY_MCP_TOKEN -u MARKET_DATA_GATEWAY_TOKEN \
  "$MCP_BIN" -gateway-url "$URL" < "$WORK/in" > "$WORK/out" 2> "$WORK/mcp.log" &
MCP_PID=$!
exec 3> "$WORK/in"

send() { printf '%s\n' "$1" >&3; }
await() {
  for _ in $(seq 1 100); do
    if grep -q "\"id\":$1[,}]" "$WORK/out"; then return 0; fi
    sleep 0.1
  done
  fail "no response for request id $1"
}

send '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"mcp-smoke","version":"0"}}}'
await 1
send '{"jsonrpc":"2.0","method":"notifications/initialized"}'

echo "== 4. tools/list =="
send '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'
await 2
for tool in get_candles get_volatility get_trend market_snapshot gateway_status; do
  grep "\"id\":2[,}]" "$WORK/out" | grep -q "\"name\":\"$tool\"" || fail "tools/list misses $tool"
done

echo "== 5. tools/call get_volatility =="
send '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_volatility","arguments":{"symbol":"SBER","timeframe":"1m"}}}'
await 3
RESPONSE="$(grep "\"id\":3[,}]" "$WORK/out")"
echo "$RESPONSE" | grep -q '"isError":true' && fail "get_volatility returned a tool error"
echo "$RESPONSE" | grep -q '"structuredContent"' || fail "get_volatility has no structured content"
echo "$RESPONSE" | grep -q '"label":"normal"' || fail "get_volatility label missing (constant fake candles -> normal)"

echo "== 6. close stdin, clean exit =="
exec 3>&-
if ! wait "$MCP_PID"; then MCP_PID=""; fail "gateway-mcp exited non-zero on stdin EOF"; fi
MCP_PID=""

echo "MCP SMOKE PASSED"
