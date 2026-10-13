import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import {
  collectLivePanelCredentials,
  invalidateLiveCredentialCache,
  publicLiveCredential,
} from '../../src/lib/admin/panel-live-credentials.mjs'

function tmpDir(prefix = 'kin-live-cred-') {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix))
}

test('live credential prefers newer file TTL over stale worker snapshot', () => {
  const now = Date.now()
  const live = publicLiveCredential(
    {
      ok: true,
      credential: {
        has_access: true,
        has_refresh: true,
        needs_refresh: true,
        expires_at: now - 3600_000,
        credential_state: 'expired_refreshable',
      },
    },
    {
      has_access: true,
      has_refresh: true,
      expires_at: now + 8 * 3600_000,
    },
    now,
  )
  assert.equal(live.credential_state, 'fresh')
  assert.equal(live.needs_refresh, false)
  assert.ok(live.expires_at > now)
  assert.equal(live.source, 'slot-credentials')
})

test('fresh TTL drops leftover invalid_grant last_error', () => {
  const now = Date.now()
  const live = publicLiveCredential(
    {
      ok: true,
      last_error: 'OAuth refresh failed (invalid_grant): Refresh token not found or invalid',
      last_error_class: 'fatal',
      credential: {
        has_access: true,
        has_refresh: true,
        needs_refresh: false,
        expires_at: now + 8 * 3600_000,
      },
    },
    {
      has_access: true,
      has_refresh: true,
      expires_at: now + 8 * 3600_000,
    },
    now,
  )
  assert.equal(live.credential_state, 'fresh')
  assert.equal(live.last_error, null)
  assert.equal(live.last_error_class, null)
})

test('live credential keeps expired_refreshable when both TTLs are past', () => {
  const now = Date.now()
  const live = publicLiveCredential(
    null,
    {
      has_access: true,
      has_refresh: true,
      expires_at: now - 60_000,
    },
    now,
  )
  assert.equal(live.credential_state, 'expired_refreshable')
  assert.equal(live.needs_refresh, true)
  assert.equal(live.source, 'slot-credentials')
})

test('live collect reads slot credentials.json when vm.json TTL is stale', async () => {
  invalidateLiveCredentialCache()
  const project = tmpDir()
  const vms = path.join(project, 'vms')
  const home = path.join(vms, 'vm-06', 'cli-home', '.claude')
  fs.mkdirSync(home, { recursive: true })
  const freshMs = Date.now() + 7 * 3600_000
  fs.writeFileSync(
    path.join(vms, 'vm-06.json'),
    JSON.stringify({
      id: 'vm-06',
      name: '06',
      status: 'running',
      claude: {
        has_access: true,
        has_refresh: true,
        expires_at: Math.floor((Date.now() - 3600_000) / 1000),
        refreshed_at: '2026-08-20T00:00:00.000Z',
      },
      policy: { maxConcurrency: 4, weight: 1 },
    }),
  )
  fs.writeFileSync(
    path.join(home, 'credentials.json'),
    JSON.stringify({
      claudeAiOauth: {
        accessToken: 'test-access',
        refreshToken: 'test-refresh',
        expiresAt: freshMs,
      },
    }),
  )
  const live = await collectLivePanelCredentials(
    project,
    [{ id: 'vm-06', expires_at: Math.floor((Date.now() - 3600_000) / 1000) }],
    { cacheMs: 0 },
  )
  const cred = live.get('vm-06')
  assert.ok(cred)
  assert.equal(cred.has_access, true)
  assert.ok(cred.expires_at >= freshMs - 1000)
  assert.equal(cred.credential_state, 'fresh')
  assert.ok(!JSON.stringify(cred).includes('test-access'))
  assert.ok(!JSON.stringify(cred).includes('test-refresh'))
  const mirrored = JSON.parse(fs.readFileSync(path.join(vms, 'vm-06.json'), 'utf8'))
  assert.ok(Number(mirrored.claude.expires_at) > Date.now() / 1000 - 10)
  assert.ok(!JSON.stringify(mirrored).includes('test-access'))
})

test('concurrent cached collects share one in-flight credential map', async () => {
  invalidateLiveCredentialCache()
  const project = tmpDir()
  const calls = Array.from({ length: 40 }, () =>
    collectLivePanelCredentials(project, [{ id: 'missing-runtime' }], { cacheMs: 3000 }),
  )
  const maps = await Promise.all(calls)
  assert.ok(maps.every((map) => map === maps[0]))
})
