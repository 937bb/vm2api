import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { ApiKeyStore, generateApiKey, maskApiKey } from '../../src/lib/admin/api-keys.mjs'

function tmpStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-keys-'))
  return new ApiKeyStore({ dataDir: dir })
}

test('generateApiKey uses sk-vm- prefix and is long enough', () => {
  const k = generateApiKey()
  assert.match(k, /^sk-vm-[a-f0-9]{64}$/)
  assert.notEqual(k, generateApiKey())
})

test('maskApiKey hides middle', () => {
  const k = 'sk-vm-' + 'a'.repeat(64)
  const m = maskApiKey(k)
  assert.ok(m.includes('…'))
  assert.ok(!m.includes('a'.repeat(20)))
})

test('create + authenticate + list mask', () => {
  const store = tmpStore()
  const rec = store.create({ name: 'demo', max_concurrency: 3, quota_requests: 10, rpm: 60 })
  assert.equal(rec.name, 'demo')
  assert.equal(rec.max_concurrency, 3)
  assert.equal(rec.quota_requests, 10)
  const auth = store.authenticate(rec.key)
  assert.equal(auth.ok, true)
  assert.equal(auth.record.id, rec.id)
  const stored = store.getById(rec.id)
  assert.notEqual(stored.key, rec.key)
  assert.match(stored.key, /^hmac:/)
  assert.ok(stored.key_hash)
  const listed = store.list()
  assert.equal(listed.length, 1)
  assert.ok(listed[0].key.includes('…'))
  assert.notEqual(listed[0].key, rec.key)
})

test('reveal returns the plaintext for copy; rotate swaps it in place', () => {
  const store = tmpStore()
  const rec = store.create({ name: 'copyable', max_concurrency: 4 })
  const listed = store.list()[0]
  assert.equal(listed.revealable, true)
  assert.ok(listed.key.includes('…'), 'list stays masked')

  const shown = store.reveal(rec.id)
  assert.equal(shown.ok, true)
  assert.equal(shown.key, rec.key)

  const rotated = store.rotate(rec.id)
  assert.notEqual(rotated.key, rec.key)
  assert.equal(store.authenticate(rec.key).ok, false, 'old value dies')
  assert.equal(store.authenticate(rotated.key).ok, true)
  assert.equal(store.reveal(rec.id).key, rotated.key)
  const kept = store.getById(rec.id)
  assert.equal(kept.name, 'copyable')
  assert.equal(kept.max_concurrency, 4)
})

test('reveal reports hash-only rows as unrecoverable', () => {
  const store = tmpStore()
  const rec = store.create({ name: 'legacy' })
  const row = store.getById(rec.id)
  row.key_secret = null
  store.repo.update(row)

  const out = store.reveal(rec.id)
  assert.equal(out.ok, false)
  assert.equal(out.code, 'key_not_recoverable')
  assert.equal(store.list()[0].revealable, false)
  assert.equal(store.reveal('key_missing'), null)
  assert.equal(store.authenticate(rec.key).ok, true, 'auth still works off the hash')
})

test('key_secret is encrypted at rest when KIN_DB_SECRET is set', () => {
  const prev = process.env.KIN_DB_SECRET
  process.env.KIN_DB_SECRET = 'unit-test-secret'
  try {
    const store = tmpStore()
    const rec = store.create({ name: 'enc' })
    const row = store.getById(rec.id)
    assert.match(row.key_secret, /^enc:v1:/)
    assert.ok(!row.key_secret.includes(rec.key))
    assert.equal(store.reveal(rec.id).key, rec.key)
  } finally {
    if (prev === undefined) delete process.env.KIN_DB_SECRET
    else process.env.KIN_DB_SECRET = prev
  }
})

test('canAccept rejects disabled / expired / quota / concurrency', () => {
  const store = tmpStore()
  const rec = store.create({ name: 'lim', max_concurrency: 1, quota_requests: 2, rpm: 0 })
  assert.equal(store.canAccept(rec).ok, true)

  store.update(rec.id, { status: 'disabled' })
  assert.equal(store.canAccept(store.getById(rec.id)).code, 'api_key_disabled')
  store.update(rec.id, { status: 'active' })

  store.update(rec.id, { expires_at: new Date(Date.now() - 1000).toISOString() })
  assert.equal(store.canAccept(store.getById(rec.id)).code, 'api_key_expired')
  store.update(rec.id, { expires_at: null })

  const a = store.acquire(store.getById(rec.id))
  assert.equal(a.ok, true)
  const blocked = store.canAccept(store.getById(rec.id))
  assert.equal(blocked.code, 'api_key_concurrency_limit')
  store.release(rec.id)

  store.recordUsage(rec.id)
  store.recordUsage(rec.id)
  assert.equal(store.canAccept(store.getById(rec.id)).code, 'api_key_quota_exhausted')
})

test('rpm window blocks then recovers', () => {
  const store = tmpStore()
  const rec = store.create({ name: 'rpm', max_concurrency: 0, quota_requests: 0, rpm: 2 })
  assert.equal(store.acquire(rec).ok, true)
  store.release(rec)
  assert.equal(store.acquire(rec).ok, true)
  store.release(rec)
  const gate = store.canAccept(store.getById(rec.id))
  assert.equal(gate.code, 'api_key_rate_limit')
})

test('user concurrency caps regular users but never the admin operator', () => {
  const store = tmpStore()
  const now = new Date().toISOString()
  for (const [id, role] of [
    ['u-admin', 'admin'],
    ['u-user', 'user'],
  ]) {
    store.users.insert({ id, username: id, password_hash: 'x', role, concurrency: 1, created_at: now, updated_at: now })
  }
  const admin = store.create({ name: 'admin-key', max_concurrency: 0, user_id: 'u-admin' })
  const user = store.create({ name: 'user-key', max_concurrency: 0, user_id: 'u-user' })

  for (let i = 0; i < 3; i++) assert.equal(store.acquire(admin).ok, true)

  assert.equal(store.acquire(user).ok, true)
  const blocked = store.canAccept(user)
  assert.equal(blocked.code, 'user_concurrency_limit')
  assert.deepEqual(blocked.detail, { inflight: 1, max: 1 })
})

test('update reset_quota and remove', () => {
  const store = tmpStore()
  const rec = store.create({ name: 'x', quota_requests: 5 })
  store.recordUsage(rec.id)
  store.recordUsage(rec.id)
  assert.equal(store.getById(rec.id).quota_requests_used, 2)
  store.update(rec.id, { reset_quota: true })
  assert.equal(store.getById(rec.id).quota_requests_used, 0)
  assert.equal(store.remove(rec.id), true)
  assert.equal(store.getById(rec.id), null)
  assert.equal(store.authenticate(rec.key).ok, false)
})

test('recordUsage accumulates cache token columns', () => {
  const store = tmpStore()
  const rec = store.create({ name: 'cache' })
  store.recordUsage(rec.id, {
    input_tokens: 10,
    output_tokens: 5,
    cache_read_input_tokens: 3,
    cache_creation_input_tokens: 7,
  })
  store.recordUsage(rec.id, { input_tokens: 1, output_tokens: 1, cache_read_tokens: 2 })
  const saved = store.getById(rec.id)
  assert.equal(saved.tokens_in, 11)
  assert.equal(saved.tokens_out, 6)
  assert.equal(saved.cache_read_tokens, 5)
  assert.equal(saved.cache_creation_tokens, 7)
})

test('recordUsage atomically marks USD quota exhausted at the exact boundary', () => {
  const store = tmpStore()
  const rec = store.create({ name: 'usd-quota', quota: 5 })

  const below = store.recordUsage(rec.id, { cost: 4 })
  assert.equal(below.quota_used, 4)
  assert.equal(below.status, 'active')

  const exhausted = store.recordUsage(rec.id, { cost: 1 })
  assert.equal(exhausted.quota_used, 5)
  assert.equal(exhausted.status, 'quota_exhausted')
  assert.equal(store.canAccept(exhausted).code, 'api_key_quota_exhausted')
})

test('recordUsage resets expired spend windows inside the atomic update', () => {
  const store = tmpStore()
  const rec = store.create({
    name: 'spend-windows',
    rate_limit_5h: 100,
    rate_limit_1d: 100,
    rate_limit_7d: 100,
  })
  store.recordUsage(rec.id, { cost: 9 })
  const expired = new Date(Date.now() - 8 * 24 * 3600_000).toISOString()
  store.db
    .prepare(`
      UPDATE api_keys
      SET usage_5h = 90, usage_1d = 91, usage_7d = 92,
          window_5h_start = ?, window_1d_start = ?, window_7d_start = ?
      WHERE id = ?
    `)
    .run(expired, expired, expired, rec.id)

  const reset = store.recordUsage(rec.id, { cost: 3 })
  assert.equal(reset.usage_5h, 3)
  assert.equal(reset.usage_1d, 3)
  assert.equal(reset.usage_7d, 3)
  assert.ok(Date.parse(reset.window_5h_start) > Date.parse(expired))
  assert.ok(Date.parse(reset.window_1d_start) > Date.parse(expired))
  assert.ok(Date.parse(reset.window_7d_start) > Date.parse(expired))
})

test('custom key rejected when duplicate', () => {
  const store = tmpStore()
  store.create({ name: 'a', key: 'sk-kin-customkey0001' })
  assert.throws(() => store.create({ name: 'b', key: 'sk-kin-customkey0001' }), /exists/)
})

test('keys persist in sqlite across store re-open (same dataDir)', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-keys-'))
  const store = new ApiKeyStore({ dataDir: dir })
  const rec = store.create({ name: 'persist', quota_requests: 9 })
  store.recordUsage(rec.id, { input_tokens: 11, output_tokens: 22 })

  const store2 = new ApiKeyStore({ dataDir: dir })
  const again = store2.getById(rec.id)
  assert.ok(again, 'record should survive re-open')
  assert.equal(again.name, 'persist')
  assert.equal(again.quota_requests_used, 1)
  assert.equal(again.tokens_in, 11)
  assert.equal(again.tokens_out, 22)
  assert.equal(store2.authenticate(rec.key).ok, true)
  assert.ok(fs.existsSync(path.join(dir, 'kin.db')), 'kin.db file exists')
})

test('group_type all is the default and a platform group keeps its VM list', () => {
  const store = tmpStore()
  const open = store.create({ name: 'global' })
  assert.equal(store.list()[0].group_type, 'all')
  assert.deepEqual(store.list()[0].allowed_vms, [])
  const scoped = store.create({ name: 'claude', group_type: 'anthropic', allowed_vms: ['vm-01', 'vm-03'] })
  assert.equal(store.getById(scoped.id).group_type, 'anthropic')
  assert.equal(
    store
      .list()
      .find((k) => k.id === scoped.id)
      .allowed_vms.join(','),
    'vm-01,vm-03',
  )
  const widened = store.update(scoped.id, { group_type: 'all' })
  assert.equal(widened.group_type, 'all')
  assert.equal(widened.allowed_vms, '[]')
  assert.throws(() => store.create({ name: 'bare', group_type: 'openai' }), /至少勾选一台 VM/)
  store.update(open.id, { status: 'disabled' })
  assert.equal(store.getById(open.id).group_type, 'all')
})
