import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { createPanelHandler } from '../../src/lib/admin/panel-routes.mjs'
import { reloadActiveVm } from '../../src/lib/core/config.mjs'
import { getVm } from '../../src/lib/vm/vm-registry.mjs'
import { readCodexAccounts, writeCodexAccounts } from '../../src/lib/vm/codex-slot.mjs'
import { invalidateReadCache } from '../../src/lib/vm/vm-file.mjs'

function seed(root, id, { active = false, codex = false } = {}) {
  const dir = path.join(root, 'vms')
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(
    path.join(dir, `${id}.json`),
    JSON.stringify({
      id,
      name: id,
      status: 'stopped',
      schedulable: true,
      policy: { maxConcurrency: 2 },
      ...(codex ? { platform: 'openai', family: 'codex', codex_kernel: true, codex: {} } : {}),
    }),
  )
  if (active) fs.writeFileSync(path.join(dir, 'active.json'), JSON.stringify({ active_vm: id }))
}

function handler(project, overrides = {}) {
  const response = {}
  const cfg = { paths: { project }, vm: { id: 'stale' } }
  const handlePanel = createPanelHandler({
    cfg,
    requireAuth(req) {
      req.apiKeyKind = 'master'
      req.panelRole = 'admin'
      return true
    },
    json(_res, status, payload) {
      response.status = status
      response.body = payload
      return true
    },
    readBody: async () => ({}),
    proxyPool: { unbindVm() {} },
    ...overrides,
  })
  return { handlePanel, response, cfg }
}

test('fresh deploy can delete its only active VM', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-del-only-'))
  try {
    seed(root, 'vm-01', { active: true })
    const { handlePanel, response, cfg } = handler(root)
    await handlePanel({ method: 'DELETE' }, {}, new URL('http://localhost/api/panel/vms/vm-01'))
    assert.equal(response.status, 200, JSON.stringify(response.body))
    assert.equal(response.body?.data?.deleted, 'vm-01')
    assert.equal(fs.existsSync(path.join(root, 'vms', 'vm-01.json')), false)
    assert.equal(fs.existsSync(path.join(root, 'vms', 'active.json')), false)
    reloadActiveVm(cfg)
    assert.equal(cfg.vm.id, null)
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('Codex deletion retires the account and preserves credentials while a request is active', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-del-codex-busy-'))
  const previousRedis = process.env.KIN_OPENAI_REDIS_URL
  delete process.env.KIN_OPENAI_REDIS_URL
  rt.resetOpenAIAccountRuntime()
  try {
    seed(root, 'vm-gpt-01', { codex: true })
    fs.mkdirSync(path.join(root, 'vms', 'vm-gpt-01'), { recursive: true })
    writeCodexAccounts(root, 'vm-gpt-01', [{ access_token: 'active-secret' }])
    const held = rt.tryAcquireOpenAISlot('vm-gpt-01', { concurrency: 2 })
    assert.ok(held)
    const { handlePanel, response } = handler(root, { destroySlot: async () => ({ ok: true }) })

    await handlePanel({ method: 'DELETE' }, {}, new URL('http://localhost/api/panel/vms/vm-gpt-01'))

    assert.equal(response.status, 409, JSON.stringify(response.body))
    assert.equal(response.body?.error?.code, 'vm_in_flight')
    assert.equal(response.body?.error?.details?.in_flight, 1)
    assert.equal(readCodexAccounts(root, 'vm-gpt-01')[0].access_token, 'active-secret')
    const retiring = getVm(root, 'vm-gpt-01')
    assert.equal(retiring.schedulable, false)
    assert.equal(retiring.schedule_disabled_reason, 'account_retiring')
    assert.equal(rt.openAIRuntimeSignals('vm-gpt-01').retirementBlocked, true)

    held.release()
    const resumed = handler(root, {
      destroySlot: async () => ({ ok: true }),
      readBody: async () => ({ schedulable: true }),
    })
    await resumed.handlePanel({ method: 'POST' }, {}, new URL('http://localhost/api/panel/vms/vm-gpt-01/schedulable'))
    assert.equal(resumed.response.status, 200, JSON.stringify(resumed.response.body))
    assert.equal(getVm(root, 'vm-gpt-01').schedulable, true)
    assert.equal(rt.openAIRuntimeSignals('vm-gpt-01').retirementBlocked, false)

    await resumed.handlePanel({ method: 'DELETE' }, {}, new URL('http://localhost/api/panel/vms/vm-gpt-01'))
    assert.equal(resumed.response.status, 200, JSON.stringify(resumed.response.body))
    assert.equal(getVm(root, 'vm-gpt-01'), null)
    assert.deepEqual(readCodexAccounts(root, 'vm-gpt-01'), [])
  } finally {
    rt.resetOpenAIAccountRuntime()
    if (previousRedis == null) delete process.env.KIN_OPENAI_REDIS_URL
    else process.env.KIN_OPENAI_REDIS_URL = previousRedis
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('slot destroy failure never deletes the VM record or credential directory', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-del-destroy-failed-'))
  try {
    seed(root, 'vm-02')
    fs.mkdirSync(path.join(root, 'vms', 'vm-02'), { recursive: true })
    fs.writeFileSync(path.join(root, 'vms', 'vm-02', 'credential'), 'keep')
    const { handlePanel, response } = handler(root, {
      destroySlot: async () => ({ ok: false, code: 'runtime_busy', error: 'runtime still active' }),
    })

    await handlePanel({ method: 'DELETE' }, {}, new URL('http://localhost/api/panel/vms/vm-02'))

    assert.equal(response.status, 409, JSON.stringify(response.body))
    assert.equal(response.body?.error?.code, 'runtime_busy')
    assert.equal(fs.existsSync(path.join(root, 'vms', 'vm-02.json')), true)
    assert.equal(fs.readFileSync(path.join(root, 'vms', 'vm-02', 'credential'), 'utf8'), 'keep')
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('active VM stays when another slot exists', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-del-active-'))
  try {
    seed(root, 'vm-01', { active: true })
    seed(root, 'vm-02')
    const { handlePanel, response } = handler(root)
    await handlePanel({ method: 'DELETE' }, {}, new URL('http://localhost/api/panel/vms/vm-01'))
    assert.equal(response.status, 409)
    assert.match(response.body?.error?.message, /switch active first/)
    assert.equal(fs.existsSync(path.join(root, 'vms', 'vm-01.json')), true)
    assert.equal(JSON.parse(fs.readFileSync(path.join(root, 'vms', 'active.json'), 'utf8')).active_vm, 'vm-01')
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('non-active VM deletes while the active pointer stays', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-del-other-'))
  try {
    seed(root, 'vm-01', { active: true })
    seed(root, 'vm-02')
    const { handlePanel, response } = handler(root)
    await handlePanel({ method: 'DELETE' }, {}, new URL('http://localhost/api/panel/vms/vm-02'))
    assert.equal(response.status, 200, JSON.stringify(response.body))
    assert.equal(fs.existsSync(path.join(root, 'vms', 'vm-02.json')), false)
    assert.equal(fs.existsSync(path.join(root, 'vms', 'vm-01.json')), true)
    assert.equal(JSON.parse(fs.readFileSync(path.join(root, 'vms', 'active.json'), 'utf8')).active_vm, 'vm-01')
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('startup reconcile does not resurrect a deleted sole active VM', async () => {
  const { openDatabase, closeDatabase } = await import('../../src/lib/db/database.mjs')
  const { initVmDbSync, stopVmWatch, reconcileVms } = await import('../../src/lib/vm/vm-db-sync.mjs')
  const { setVmWriteHook } = await import('../../src/lib/vm/vm-file.mjs')
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-del-db-'))
  try {
    openDatabase({ dataDir: path.join(root, 'data') })
    seed(root, 'vm-01', { active: true })
    initVmDbSync(root, { watch: false })
    const { handlePanel, response } = handler(root)
    await handlePanel({ method: 'DELETE' }, {}, new URL('http://localhost/api/panel/vms/vm-01'))
    assert.equal(response.status, 200, JSON.stringify(response.body))
    const again = reconcileVms(root)
    assert.equal(again.rebuilt, 0)
    assert.equal(again.active, null)
    assert.equal(fs.existsSync(path.join(root, 'vms', 'vm-01.json')), false)
    assert.equal(fs.existsSync(path.join(root, 'vms', 'active.json')), false)
  } finally {
    stopVmWatch()
    setVmWriteHook(null)
    closeDatabase()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('deleting a slot drops cached record and credential entries', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-del-cache-'))
  try {
    seed(root, 'vm-02')
    fs.mkdirSync(path.join(root, 'vms', 'vm-02'), { recursive: true })
    writeCodexAccounts(root, 'vm-02', [{ access_token: 'stay-secret' }])
    const recPath = path.join(root, 'vms', 'vm-02.json')
    const credPath = path.join(root, 'vms', 'vm-02', 'codex-credentials.json')
    assert.equal(getVm(root, 'vm-02').id, 'vm-02')
    assert.equal(readCodexAccounts(root, 'vm-02')[0].access_token, 'stay-secret')
    const { handlePanel, response } = handler(root)
    await handlePanel({ method: 'DELETE' }, {}, new URL('http://localhost/api/panel/vms/vm-02'))
    assert.equal(response.status, 200, JSON.stringify(response.body))
    assert.equal(invalidateReadCache(recPath), false)
    assert.equal(invalidateReadCache(credPath), false)
    assert.equal(getVm(root, 'vm-02'), null)
    assert.deepEqual(readCodexAccounts(root, 'vm-02'), [])
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})
