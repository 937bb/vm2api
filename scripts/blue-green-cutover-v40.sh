#!/usr/bin/env bash
set -Eeuo pipefail

OLD_CONTAINER="${OLD_CONTAINER:-vm2api-canary-v36}"
NEW_CONTAINER="${NEW_CONTAINER:-vm2api-canary-v40}"
OLD_INSTANCE="${OLD_INSTANCE:-v36}"
OLD_PORT="${OLD_PORT:-8793}"
NEW_PORT="${NEW_PORT:-8797}"
EXPECTED_VERSION="${EXPECTED_VERSION:-1.3.153-hotfix-v40-bounded-lease-poll}"
NGINX_CONFIG="${NGINX_CONFIG:-/www/server/panel/vhost/nginx/vm2api.937ai.chat.conf}"
NGINX_BIN="${NGINX_BIN:-/www/server/nginx/sbin/nginx}"
PUBLIC_HOST="${PUBLIC_HOST:-vm2api.937ai.chat}"
LEASE_PREFIX="${KIN_OPENAI_REDIS_PREFIX:-vm2api:openai}"
LEASE_CAP="${LEASE_CAP:-40}"
GUARD_TTL_MS="${GUARD_TTL_MS:-600000}"
QUEUE_DRAIN_TIMEOUT_SEC="${QUEUE_DRAIN_TIMEOUT_SEC:-110}"
GUARD_MEMBER_PREFIX="migration:${OLD_CONTAINER}:${NEW_CONTAINER}:"
VM_ROOT="${VM_ROOT:-/opt/vm2api/vms}"

full_guard_lua=$(cat <<'LUA'
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
for _, member in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
  if string.sub(member, 1, string.len(ARGV[3])) == ARGV[3] then
    redis.call('ZREM', KEYS[1], member)
  end
end
local active = redis.call('ZCARD', KEYS[1])
local needed = math.max(0, tonumber(ARGV[1]) - active)
for i = 1, needed do
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[3] .. i)
end
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]) * 2)
return {redis.call('ZCARD', KEYS[1]), needed}
LUA
)

sync_guard_lua=$(cat <<'LUA'
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
for _, member in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
  if string.sub(member, 1, string.len(ARGV[3])) == ARGV[3] then
    redis.call('ZREM', KEYS[1], member)
  end
end
for i = 1, tonumber(ARGV[1]) do
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[3] .. i)
end
if redis.call('ZCARD', KEYS[1]) == 0 then
  redis.call('DEL', KEYS[1])
else
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]) * 2)
end
return {redis.call('ZCARD', KEYS[1]), tonumber(ARGV[1])}
LUA
)

clear_guard_lua=$(cat <<'LUA'
local removed = 0
for _, member in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
  if string.sub(member, 1, string.len(ARGV[1])) == ARGV[1] then
    removed = removed + redis.call('ZREM', KEYS[1], member)
  end
end
if redis.call('ZCARD', KEYS[1]) == 0 then
  redis.call('DEL', KEYS[1])
end
return {removed, redis.call('ZCARD', KEYS[1])}
LUA
)

mapfile -t account_ids < <(find "$VM_ROOT" -mindepth 2 -maxdepth 2 -type f -name codex-credentials.json -printf '%h\n' | xargs -r -n1 basename | sort -u)
if ((${#account_ids[@]} == 0)); then
  echo "no Codex accounts found under $VM_ROOT" >&2
  exit 1
fi

lease_key() {
  local encoded
  encoded=$(jq -rn --arg id "$1" '$id|@uri')
  printf '%s:{%s}:leases' "$LEASE_PREFIX" "$encoded"
}

old_socket_count() {
  local socket="$VM_ROOT/$1/run/codex-kernel-${OLD_INSTANCE}.sock"
  if [[ ! -S "$socket" ]]; then
    echo 0
    return
  fi
  ss -xanp 2>/dev/null | grep -F "$socket" | grep -c ESTAB || true
}

old_active_inference() {
  local value
  if ! value=$(curl --max-time 3 -fsS "http://127.0.0.1:${OLD_PORT}/health" 2>/dev/null | jq -r '.runtime.active_inference // 0'); then
    echo 999999
    return
  fi
  echo "$value"
}

clear_guards() {
  local id key result
  for id in "${account_ids[@]}"; do
    key=$(lease_key "$id")
    result=$(redis-cli --raw EVAL "$clear_guard_lua" 1 "$key" "$GUARD_MEMBER_PREFIX")
    echo "guard-clear account=$id result=$(tr '\n' ',' <<<"$result" | sed 's/,$//')"
  done
}

rollback_before_handoff() {
  local backup=$1
  cp -a "$backup" "$NGINX_CONFIG"
  "$NGINX_BIN" -t
  "$NGINX_BIN" -s reload
  clear_guards
}

new_version=$(curl --max-time 5 -fsS "http://127.0.0.1:${NEW_PORT}/health" | jq -r '.version // empty')
if [[ "$new_version" != "$EXPECTED_VERSION" ]]; then
  echo "new gateway version mismatch: expected=$EXPECTED_VERSION actual=$new_version" >&2
  exit 1
fi
if [[ $(docker inspect -f '{{.State.Running}}' "$NEW_CONTAINER") != true ]]; then
  echo "new gateway is not running: $NEW_CONTAINER" >&2
  exit 1
fi
if ! grep -qF "proxy_pass http://127.0.0.1:${OLD_PORT};" "$NGINX_CONFIG"; then
  echo "nginx does not currently target port $OLD_PORT" >&2
  exit 1
fi

for id in "${account_ids[@]}"; do
  key=$(lease_key "$id")
  result=$(redis-cli --raw EVAL "$full_guard_lua" 1 "$key" "$LEASE_CAP" "$GUARD_TTL_MS" "$GUARD_MEMBER_PREFIX")
  total=$(sed -n '1p' <<<"$result")
  if [[ "$total" != "$LEASE_CAP" ]]; then
    echo "failed to fill migration guard for $id: total=$total" >&2
    clear_guards
    exit 1
  fi
  echo "guard-seed account=$id total=$total"
done

timestamp=$(date -u +%Y%m%dT%H%M%SZ)
backup="${NGINX_CONFIG}.pre-v40-${timestamp}"
cp -a "$NGINX_CONFIG" "$backup"
sed -i "s#proxy_pass http://127.0.0.1:${OLD_PORT};#proxy_pass http://127.0.0.1:${NEW_PORT};#" "$NGINX_CONFIG"
if ! "$NGINX_BIN" -t; then
  rollback_before_handoff "$backup"
  exit 1
fi
"$NGINX_BIN" -s reload

public_version=''
for _ in $(seq 1 20); do
  public_version=$(curl --max-time 5 -fsS --resolve "${PUBLIC_HOST}:443:127.0.0.1" "https://${PUBLIC_HOST}/health" | jq -r '.version // empty' || true)
  [[ "$public_version" == "$EXPECTED_VERSION" ]] && break
  sleep 1
done
if [[ "$public_version" != "$EXPECTED_VERSION" ]]; then
  echo "public health did not switch to $EXPECTED_VERSION; rolling nginx back" >&2
  rollback_before_handoff "$backup"
  exit 1
fi
echo "nginx-switched version=$public_version backup=$backup"

# Full guards keep the new gateway from overlapping v36 while its already
# accepted admission queue drains. With no new traffic, that queue is bounded
# by the old gateway's 90 second wait timeout.
queue_deadline=$((SECONDS + QUEUE_DRAIN_TIMEOUT_SEC))
stable=0
while ((SECONDS < queue_deadline)); do
  active=$(old_active_inference)
  unix_total=0
  for id in "${account_ids[@]}"; do
    unix_total=$((unix_total + $(old_socket_count "$id")))
  done
  queued=$((active - unix_total))
  ((queued < 0)) && queued=0
  echo "old-queue active=$active unix=$unix_total queued=$queued elapsed=${SECONDS}s"
  if ((queued <= 2)); then
    stable=$((stable + 1))
    ((stable >= 3)) && break
  else
    stable=0
  fi
  sleep 2
done
if ((stable < 3)); then
  echo "old admission queue did not drain safely; rolling nginx back" >&2
  rollback_before_handoff "$backup"
  exit 1
fi

# Mirror only v36's real per-account Unix work into Redis. Every old request
# that completes removes one guard seat, so v40 takes over the same seat while
# the combined account concurrency remains capped at 40.
while true; do
  active=$(old_active_inference)
  unix_total=0
  for id in "${account_ids[@]}"; do
    count=$(old_socket_count "$id")
    unix_total=$((unix_total + count))
    redis-cli --raw EVAL "$sync_guard_lua" 1 "$(lease_key "$id")" "$count" 120000 "$GUARD_MEMBER_PREFIX" >/dev/null
  done
  echo "old-handoff active=$active unix=$unix_total elapsed=${SECONDS}s"
  ((active == 0 && unix_total == 0)) && break
  sleep 1
done

docker update --restart=no "$OLD_CONTAINER" >/dev/null
docker stop --time 30 "$OLD_CONTAINER" >/dev/null
clear_guards
curl --max-time 5 -fsS "http://127.0.0.1:${NEW_PORT}/health" | jq '{version,runtime}'
