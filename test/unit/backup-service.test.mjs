import { test, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { spawnSync } from 'node:child_process'
import { openDatabase, closeDatabase, getDb } from '../../src/lib/db/database.mjs'
import { SettingsRepo } from '../../src/lib/db/repos/settings-repo.mjs'
import { VmsRepo } from '../../src/lib/db/repos/vms-repo.mjs'
import { BackupService, tarAvailable } from '../../src/lib/admin/backup-service.mjs'
import { ApiKeyStore } from '../../src/lib/admin/api-keys.mjs'
import { RequestLogStore } from '../../src/lib/admin/request-log.mjs'
import { stopVmWatch, initVmDbSync } from '../../src/lib/vm/vm-db-sync.mjs'
import { setVmWriteHook } from '../../src/lib/vm/vm-file.mjs'

let root, dataDir, configDir, svc

function seedVm(id = 'vm-b1') {
  const vmsDir = path.join(root, 'vms')
  fs.mkdirSync(vmsDir, { recursive: true })
  fs.writeFileSync(
    path.join(vmsDir, `${id}.json`),
    JSON.stringify(
      {
        id,
        name: 'b',
        status: 'running',
        claude: { access_token: 'sk-ant-oat01-BAK', refresh_token: 'sk-ant-ort01-BAK', account_uuid: 'acct-b' },
      },
      null,
      2,
    ),
  )
  fs.writeFileSync(path.join(vmsDir, 'active.json'), JSON.stringify({ active_vm: id }))
}

beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-bak-'))
  dataDir = path.join(root, 'data')
  configDir = path.join(root, 'config')
  fs.mkdirSync(configDir, { recursive: true })
  fs.writeFileSync(path.join(configDir, 'routing.json'), JSON.stringify({ sticky: { enabled: true } }))
  openDatabase({ dataDir })
  svc = new BackupService({ dataDir, projectRoot: root, configDir })
})

afterEach(() => {
  svc.stopScheduler()
  stopVmWatch()
  setVmWriteHook(null)
  closeDatabase()
  fs.rmSync(root, { recursive: true, force: true })
  delete process.env.KIN_BACKUP_DISABLED
  delete process.env.KIN_BACKUP_INTERVAL_HOURS
  delete process.env.KIN_BACKUP_RETENTION
})

test('tar is available in this environment', () => {
  assert.equal(tarAvailable(), true)
})

test('createBackup produces tar.gz with manifest + db + vms + config, records ok', () => {
  seedVm()
  initVmDbSync(root, { watch: false })
  new SettingsRepo(getDb()).set('marker', 'before-backup')

  const rec = svc.createBackup({ kind: 'manual' })
  assert.equal(rec.status, 'ok')
  assert.equal(rec.kind, 'manual')
  assert.ok(rec.file_path.endsWith('.tar.gz'))
  assert.ok(fs.existsSync(rec.file_path))
  assert.ok(rec.size_bytes > 0)
  assert.match(rec.sha256, /^[0-9a-f]{64}$/)
  assert.equal(rec.includes.vms, 2) // vm-b1.json + active.json
  assert.equal(rec.includes.config, 1)

  // archive contents sane
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-bak-x-'))
  spawnSync('tar', ['-xzf', rec.file_path, '-C', tmp])
  assert.ok(fs.existsSync(path.join(tmp, 'manifest.json')))
  assert.ok(fs.existsSync(path.join(tmp, 'db', 'kin.db')))
  assert.ok(fs.existsSync(path.join(tmp, 'vms', 'vm-b1.json')))
  assert.ok(fs.existsSync(path.join(tmp, 'config', 'routing.json')))
  const manifest = JSON.parse(fs.readFileSync(path.join(tmp, 'manifest.json'), 'utf8'))
  assert.equal(manifest.version, 1)
  assert.ok(manifest.sha256s['db/kin.db'])
  fs.rmSync(tmp, { recursive: true, force: true })
})

test('restoreBackup rolls data back and creates pre_restore record', () => {
  seedVm()
  initVmDbSync(root, { watch: false })
  const keys = new ApiKeyStore({ db: getDb() })
  const kept = keys.create({ name: 'kept-key' })

  const rec = svc.createBackup({ kind: 'manual' })

  // mutate after the backup
  const lost = keys.create({ name: 'lost-key' })
  new SettingsRepo(getDb()).set('marker', 'after-backup')
  // wire the rebind like the server does
  let reboundDb = null
  svc.onRestored((db) => {
    reboundDb = db
    keys.rebind(db)
  })

  const out = svc.restoreBackup(rec.id)
  assert.equal(out.ok, true)
  assert.ok(reboundDb, 'rebind callback should fire')

  // state rolled back
  assert.ok(keys.getById(kept.id), 'pre-backup key survives')
  assert.equal(keys.getById(lost.id), null, 'post-backup key rolled back')
  assert.equal(new SettingsRepo(getDb()).get('marker'), null)

  // old db file kept on disk as safety net
  assert.ok(fs.existsSync(path.join(dataDir, 'kin.db.pre-restore')))
  // pre_restore archive file kept on disk for manual recovery
  const preFiles = fs.readdirSync(path.join(dataDir, 'backups'))
  assert.ok(preFiles.length >= 2, 'manual + pre_restore archives on disk')
  // ledger re-registered after restore: pre_restore (and restored manual) visible
  const ledger = svc.list()
  assert.ok(
    ledger.some((r) => r.id === out.pre_restore && r.kind === 'pre_restore'),
    'pre_restore record re-registered in restored DB',
  )
  assert.ok(
    ledger.some((r) => r.id === rec.id),
    'restored manual record present',
  )
  assert.ok(
    ledger.every((r) => r.file_exists),
    'all ledger rows point at real files',
  )
})

test('restore drains buffered writes before closing the live database', () => {
  const logs = new RequestLogStore({
    db: getDb(),
    mode: 'normal',
    bufferedWrites: true,
    writeFlushIntervalMs: 60_000,
  })
  const rec = svc.createBackup({ kind: 'manual' })
  const ctx = logs.start({ method: 'POST', headers: {}, socket: {} }, { pathName: '/v1/responses' })
  logs.finish(ctx, { status: 200, model: 'gpt-5.4' })
  assert.equal(logs.snapshot().write_queue.pending, 1)

  let drained = false
  svc.onBeforeRestore(() => {
    assert.equal(logs.flushPending(), 1)
    drained = true
  })
  svc.onRestored((db) => logs.rebind(db))
  svc.restoreBackup(rec.id)

  assert.equal(drained, true)
  assert.equal(logs.snapshot().write_queue.pending, 0)
  assert.equal(logs.queryNormal({ include_muted: true }).total, 0, 'restored snapshot must not replay newer rows')
  logs.stop()
})

test('restore fails when stores cannot rebind to the reopened database', () => {
  seedVm()
  const rec = svc.createBackup({ kind: 'manual' })
  svc.onRestored(() => {
    throw new Error('store rebind failed')
  })

  assert.throws(() => svc.restoreBackup(rec.id), /store rebind failed/)
  assert.equal(svc.isRestoring, false)
  assert.doesNotThrow(() => getDb().prepare('SELECT 1').get())
})

test('restore rebuilds vm files from DB mirror for db-only path', () => {
  seedVm('vm-b2')
  initVmDbSync(root, { watch: false })
  const rec = svc.createBackup({ kind: 'manual' })

  // delete vm file + its row is still in backup db; also delete local file post-backup
  fs.rmSync(path.join(root, 'vms', 'vm-b2.json'))
  const out = svc.restoreBackup(rec.id)
  assert.equal(out.ok, true)
  const back = JSON.parse(fs.readFileSync(path.join(root, 'vms', 'vm-b2.json'), 'utf8'))
  // tar snapshot still has the on-disk file from backup time; DB is metadata only.
  assert.equal(back.claude.access_token, 'sk-ant-oat01-BAK')
  const row = new VmsRepo(getDb()).get('vm-b2')
  assert.equal(row.access_token, null)
  assert.equal(row.vm.claude.has_access, true)
  assert.equal(row.vm.claude.access_token, undefined)
})

test('sha256 tamper detection blocks restore', () => {
  seedVm()
  const rec = svc.createBackup({ kind: 'manual' })
  fs.appendFileSync(rec.file_path, 'tamper')
  assert.throws(() => svc.restoreBackup(rec.id), /sha256 mismatch/)
})

test('retention prunes old backups', () => {
  process.env.KIN_BACKUP_RETENTION = '2'
  seedVm()
  const recs = []
  for (let i = 0; i < 4; i++) recs.push(svc.createBackup({ kind: 'manual' }))
  const remaining = svc.list().filter((r) => r.kind === 'manual')
  assert.equal(remaining.length, 2)
  // newest two kept
  assert.ok(remaining.some((r) => r.id === recs[3].id))
  assert.ok(remaining.some((r) => r.id === recs[2].id))
  // pruned files removed from disk
  assert.equal(fs.existsSync(recs[0].file_path), false)
  assert.equal(fs.existsSync(recs[1].file_path), false)
})

test('schedule config defaults on, env override off, updateSchedule persists', () => {
  const cfg = svc.getSchedule()
  assert.equal(cfg.enabled, true)
  assert.equal(cfg.interval_hours, 24)
  assert.equal(cfg.retention, 7)

  svc.updateSchedule({ interval_hours: 6, retention: 3 })
  assert.equal(svc.getSchedule().interval_hours, 6)
  assert.equal(svc.getSchedule().retention, 3)
  assert.throws(() => svc.updateSchedule({ interval_hours: 0 }), /interval_hours/)

  process.env.KIN_BACKUP_DISABLED = '1'
  assert.equal(svc.getSchedule().enabled, false)
})

test('shouldRunScheduled: true when never run, false right after a backup', () => {
  seedVm()
  assert.equal(svc.shouldRunScheduled(), true)
  svc.createBackup({ kind: 'scheduled' })
  assert.equal(svc.shouldRunScheduled(), false)
  // pretend 25h elapsed
  assert.equal(svc.shouldRunScheduled(Date.now() + 25 * 3600_000), true)
})

test('remove deletes record + file', () => {
  seedVm()
  const rec = svc.createBackup({ kind: 'manual' })
  assert.equal(svc.remove(rec.id), true)
  assert.equal(fs.existsSync(rec.file_path), false)
  assert.equal(svc.get(rec.id), null)
})
