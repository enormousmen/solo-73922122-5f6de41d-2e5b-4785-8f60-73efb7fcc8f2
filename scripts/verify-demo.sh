#!/usr/bin/env bash
# 一键核验演示：启动 compose → 产生流量 → 可控轮转（含旧 inode 延迟写入）
# → 采集 → 对每个已追踪物理文件调用 GET /verify/{dev}/{inode}。
#
# 核验本身只由 collector 的 API 完成；本脚本仅编排样例，方便在终端复现。
# 也可以在 http://localhost:8000/docs 里直接调用 /verify/{dev}/{inode}。
#
# 用法:
#   ./scripts/verify-demo.sh            # 启动并演示
#   DEMO_ROTATE=0 ./scripts/verify-demo.sh   # 只核验，不轮转
set -euo pipefail

cd "$(dirname "$0")/.."
API="${API:-http://localhost:8000}"
NGINX_SERVICE="${NGINX_SERVICE:-nginx}"

log() { printf '%s\n' "$*"; }
fail() { log "ERROR: $*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || fail "docker not found"
command -v curl >/dev/null 2>&1 || fail "curl not found"

log "== starting compose environment =="
docker compose up -d --build

log "== waiting for collector API =="
for _ in $(seq 1 120); do
  curl -fsS "$API/health" >/dev/null 2>&1 && break
  sleep 0.5
done
curl -fsS "$API/health" >/dev/null 2>&1 || fail "collector API not ready at $API"

# Wait until the traffic generator has produced the first lines.
for _ in $(seq 1 60); do
  n=$(curl -fsS "$API/health" | python3 -c 'import json,sys;print(json.load(sys.stdin)["counts"]["events"])')
  [ "$n" -gt 0 ] && break
  sleep 0.5
done

if [ "${DEMO_ROTATE:-1}" = "1" ]; then
  log "== controlled rotation (rename + USR1 reopen) =="
  docker compose exec -e ROTATE_NAME="verify-demo-$(date -u +%Y%m%dT%H%M%SZ)" \
    "$NGINX_SERVICE" /usr/local/bin/rotate-nginx-log.sh

  # 轮转后旧 inode 仍可能接收在途请求：主动补写一行模拟“延迟写入”，
  # 它应继续由归档来源采集，而不是被算作新文件的内容。
  log "== late write into the rotated archive + new lines into access.log =="
  docker compose exec "$NGINX_SERVICE" sh -c '
    set -eu
    dir=/var/log/nginx/json
    arch=$(ls -1t "$dir"/access.log.verify-demo-*.log | head -1)
    # 归档：轮转后延迟写入一行（应被继续采集）+ 一条坏行 + 一条未换行尾部。
    printf "{\"time\":\"%s\",\"path\":\"/late/after-rotate\",\"status\":502,\"method\":\"GET\"}\n" \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$arch"
    printf "{\"time\":\"%s\",\"path\":\"/bad/json\",\"status\":500,\"method\":\"GET\"} trailing\n" \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$arch"
    # 归档不再接收真实流量，这条尾部会稳定保持未换行，不参与核验。
    printf "{\"time\":\"now\",\"path\":\"/dangling\"" >> "$arch"
    # 新文件：一条正常行，之后由 traffic 容器继续写入。
    printf "{\"time\":\"%s\",\"path\":\"/new/after-rotate\",\"status\":200,\"method\":\"GET\"}\n" \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$dir/access.log"
  '
  sleep 1
fi

log "== forcing one collection pass =="
curl -fsS -X POST "$API/ingest/poll-now" >/dev/null
sleep 0.5
curl -fsS -X POST "$API/ingest/poll-now" >/dev/null

log "== verifying every tracked physical file (read-only) =="
progress=$(curl -fsS "$API/ingest/progress")
API="$API" python3 ./scripts/verify_summary.py <<<"$progress"
rc=$?
log "== demo done (rc=$rc) =="
exit "$rc"
