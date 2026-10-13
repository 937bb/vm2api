#!/usr/bin/env bash
set -Eeuo pipefail

OLD_CONTAINER="${OLD_CONTAINER:-vm2api-canary-v36}"
NEW_CONTAINER="${NEW_CONTAINER:-vm2api-canary-v39}"
OLD_PORT="${OLD_PORT:-8793}"
NEW_PORT="${NEW_PORT:-8796}"
EXPECTED_VERSION="${EXPECTED_VERSION:-1.3.153-hotfix-v39-awaited-lease-release}"
NGINX_CONFIG="${NGINX_CONFIG:-/www/server/panel/vhost/nginx/vm2api.937ai.chat.conf}"
NGINX_BIN="${NGINX_BIN:-/www/server/nginx/sbin/nginx}"
PUBLIC_HOST="${PUBLIC_HOST:-vm2api.937ai.chat}"
LEASE_PREFIX="${KIN_OPENAI_REDIS_PREFIX:-vm2api:openai}"
LEASE_CAP="${LEASE_CAP:-40}"
GUARD_TTL_MS="${GUARD_TTL_MS:-600000}"
GUARD_MEMBER_PREFIX="migration:${OLD_CONTAINER}:${NEW_CONTAINER}:"
VM_ROOT="${VM_ROOT:-/opt/vm2api/vms}"

seed_lua=$(cat <<'LUA'
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
local members = redis.call('ZRANGE', KEYS[1], 0, -1)
for _, member in ipairs(members) do
  if string.sub(member, 1, string.len(ARGV[3])) == ARGV[3] then
    redis.call('ZREM', KEYS[1], member)
  end
end
local active = redis.call('ZCARD', KEYS[1])
local cap = tonumber(ARGV[1])
local needed = math.max(0, cap - active)
for i = 1, needed do
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[3] .. i)
end
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]) * 2)
return {redis.call('ZCARD', KEYS[1]), needed}
LUA
)

clear_lua=$(cat <<'LUA'
local removed = 0
local members = redis.call('ZRANGE', KEYS[1], 0, -1)
for _, member in ipairs(members) do
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

clear_guards() {
  local id key result
  for id in "${account_ids[@]}"; do
    key=$(lease_key "$id")
    result=$(redis-cli --raw EVAL "$clear_lua" 1 "$key" "$GUARD_MEMBER_PREFIX")
    echo "guard-clear account=$id result=$(tr '\n' ',' <<<"$result" | sed 's/,$//')"
  done
}

rollback_before_drain() {
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
  result=$(redis-cli --raw EVAL "$seed_lua" 1 "$key" "$LEASE_CAP" "$GUARD_TTL_MS" "$GUARD_MEMBER_PREFIX")
  total=$(sed -n '1p' <<<"$result")
  if [[ "$total" != "$LEASE_CAP" ]]; then
    echo "failed to fill migration guard for $id: total=$total" >&2
    clear_guards
    exit 1
  fi
  echo "guard-seed account=$id total=$total"
done

timestamp=$(date -u +%Y%m%dT%H%M%SZ)
backup="${NGINX_CONFIG}.pre-v39-${timestamp}"
cp -a "$NGINX_CONFIG" "$backup"
sed -i "s#proxy_pass http://127.0.0.1:${OLD_PORT};#proxy_pass http://127.0.0.1:${NEW_PORT};#" "$NGINX_CONFIG"
if ! "$NGINX_BIN" -t; then
  rollback_before_drain "$backup"
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
  echo "public health did not switch to v39; rolling nginx back" >&2
  rollback_before_drain "$backup"
  exit 1
fi
echo "nginx-switched version=$public_version backup=$backup"

docker update --restart=no "$OLD_CONTAINER" >/dev/null
docker kill --signal=TERM "$OLD_CONTAINER" >/dev/null
echo "old-drain-start container=$OLD_CONTAINER"

deadline=$((SECONDS + 330))
while [[ $(docker inspect -f '{{.State.Running}}' "$OLD_CONTAINER" 2>/dev/null || echo false) == true ]]; do
  if ((SECONDS >= deadline)); then
    echo "old gateway did not exit; guards remain in place" >&2
    exit 1
  fi
  active=0
  for socket in "$VM_ROOT"/*/run/codex-kernel-v36.sock; do
    [[ -S "$socket" ]] || continue
    count=$(ss -xanp 2>/dev/null | grep -F "$socket" | grep -c ESTAB || true)
    active=$((active + count))
  done
  echo "old-draining unix_active=$active elapsed=${SECONDS}s"
  sleep 5
done

echo "old-drained exit=$(docker inspect -f '{{.State.ExitCode}}' "$OLD_CONTAINER")"
clear_guards
curl --max-time 5 -fsS "http://127.0.0.1:${NEW_PORT}/health" | jq '{version,runtime}'
