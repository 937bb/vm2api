import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import {
  parseCodexCredentialInput,
  upsertCodexAccount,
  readCodexAccounts,
  syncCodexAccountScheduling,
} from '../../src/lib/vm/codex-slot.mjs'

test('parseCodexCredentialInput accepts Codex CLI auth.json', () => {
  const parsed = parseCodexCredentialInput({
    tokens: {
      access_token: 'eyJhbGciOiaccess',
      refresh_token: 'rt-1',
      id_token: 'id-1',
    },
    account_id: 'acc-uuid',
  })
  assert.equal(parsed.ok, true)
  assert.equal(parsed.account.access_token, 'eyJhbGciOiaccess')
  assert.equal(parsed.account.refresh_token, 'rt-1')
  assert.equal(parsed.account.chatgpt_account_id, 'acc-uuid')
})

test('parseCodexCredentialInput accepts kernel accounts file', () => {
  const parsed = parseCodexCredentialInput(
    JSON.stringify({
      accounts: [
        {
          id: 'user@example.com',
          access_token: 'at',
          refresh_token: 'rt',
          chatgpt_account_id: 'org-1',
        },
      ],
    }),
  )
  assert.equal(parsed.ok, true)
  assert.equal(parsed.account.email, 'user@example.com')
  assert.equal(parsed.account.chatgpt_account_id, 'org-1')
})

test('parseCodexCredentialInput rejects Claude sessionKey JSON', () => {
  const parsed = parseCodexCredentialInput({ session_key: 'sk-ant-sid01-xxx' })
  assert.equal(parsed.ok, false)
  assert.equal(parsed.error, 'missing_token')
})

test('parseCodexCredentialInput accepts AT-only paste', () => {
  const parsed = parseCodexCredentialInput('eyJhbGciOiaccess', { mode: 'access_token' })
  assert.equal(parsed.ok, true)
  assert.equal(parsed.account.access_token, 'eyJhbGciOiaccess')
  assert.equal(parsed.account.refresh_token, '')
})

test('parseCodexCredentialInput accepts RT-only paste', () => {
  const parsed = parseCodexCredentialInput('rt-only-1\nrt-ignored', { mode: 'refresh_token' })
  assert.equal(parsed.ok, true)
  assert.equal(parsed.account.refresh_token, 'rt-only-1')
  assert.equal(parsed.account.access_token, '')
  assert.equal(parsed.extra_count, 1)
})

test('parseCodexCredentialInput accepts camelCase credentials object', () => {
  const parsed = parseCodexCredentialInput({
    credentials: { accessToken: 'at-2', refreshToken: 'rt-2', idToken: 'id-2' },
  })
  assert.equal(parsed.ok, true)
  assert.equal(parsed.account.access_token, 'at-2')
  assert.equal(parsed.account.refresh_token, 'rt-2')
})

test('parseCodexCredentialInput accepts CPR openai document', () => {
  const parsed = parseCodexCredentialInput({
    documents: [
      { provider: 'xai', document: { refreshToken: 'xai-rt' } },
      { provider: 'openai', document: { accounts: [{ accessToken: 'cpr-at', refreshToken: 'cpr-rt' }] } },
    ],
  })
  assert.equal(parsed.ok, true)
  assert.equal(parsed.account.access_token, 'cpr-at')
  assert.equal(parsed.account.refresh_token, 'cpr-rt')
})

test('parseCodexCredentialInput rejects OPENAI_API_KEY-only client config', () => {
  const parsed = parseCodexCredentialInput({ OPENAI_API_KEY: 'sk-proj-not-oauth' })
  assert.equal(parsed.ok, false)
  assert.equal(parsed.error, 'not_oauth_account')
})

test('upsertCodexAccount writes credential file', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-slot-'))
  fs.mkdirSync(path.join(root, 'vms', 'vm-codex-01'), { recursive: true })
  upsertCodexAccount(root, 'vm-codex-01', {
    access_token: 'at',
    refresh_token: 'rt',
    chatgpt_account_id: 'org-9',
  })
  const accounts = readCodexAccounts(root, 'vm-codex-01')
  assert.equal(accounts.length, 1)
  assert.equal(accounts[0].access_token, 'at')
  assert.equal(accounts[0].chatgpt_account_id, 'org-9')
  fs.rmSync(root, { recursive: true, force: true })
})

test('imported Codex account defaults to unlimited kernel concurrency', () => {
  const parsed = parseCodexCredentialInput({
    tokens: { access_token: 'at', refresh_token: 'rt' },
    account_id: 'acc-uuid',
  })
  assert.equal(parsed.ok, true)
  assert.equal(parsed.account.max_concurrency, 0)
  assert.equal(parsed.account.rpm, 0)
})

test('import flow synchronizes the stored kernel limits from the VM policy', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-import-schedule-'))
  const id = 'vm-codex-import'
  fs.mkdirSync(path.join(root, 'vms', id), { recursive: true })
  try {
    const parsed = parseCodexCredentialInput({
      tokens: { access_token: 'at', refresh_token: 'rt' },
      account_id: 'acc-uuid',
    })
    upsertCodexAccount(root, id, parsed.account)
    const synced = syncCodexAccountScheduling(root, {
      id,
      platform: 'openai',
      family: 'codex',
      policy: { maxConcurrency: 40, maxRpm: 0 },
    })
    assert.deepEqual(synced, { updated: true, account_count: 1, max_concurrency: 40, rpm: 0 })
    const [stored] = readCodexAccounts(root, id)
    assert.equal(stored.max_concurrency, 40)
    assert.equal(stored.rpm, 0)
    assert.equal(stored.refresh_token, 'rt')
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('scheduling sync is idempotent and never creates an empty credential file', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-empty-schedule-'))
  const id = 'vm-codex-empty'
  const dir = path.join(root, 'vms', id)
  fs.mkdirSync(dir, { recursive: true })
  const vm = { id, platform: 'openai', family: 'codex', policy: { maxConcurrency: 40, maxRpm: 0 } }
  try {
    assert.equal(syncCodexAccountScheduling(root, vm).updated, false)
    assert.equal(fs.existsSync(path.join(dir, 'codex-credentials.json')), false)
    upsertCodexAccount(root, id, { access_token: 'at', max_concurrency: 40, rpm: 0 })
    const before = fs.readFileSync(path.join(dir, 'codex-credentials.json'), 'utf8')
    assert.equal(syncCodexAccountScheduling(root, vm).updated, false)
    assert.equal(fs.readFileSync(path.join(dir, 'codex-credentials.json'), 'utf8'), before)
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('scheduling sync sees an external kernel rewrite with an unchanged cache signature', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-external-schedule-'))
  const id = 'vm-codex-external'
  const dir = path.join(root, 'vms', id)
  const file = path.join(dir, 'codex-credentials.json')
  fs.mkdirSync(dir, { recursive: true })
  const vm = { id, platform: 'openai', family: 'codex', policy: { maxConcurrency: 40, maxRpm: 0 } }
  try {
    upsertCodexAccount(root, id, { access_token: 'at', max_concurrency: 40, rpm: 0 })
    assert.equal(readCodexAccounts(root, id)[0].max_concurrency, 40)
    const before = fs.statSync(file)
    const original = fs.readFileSync(file, 'utf8')
    const rewritten = original.replace('"max_concurrency": 40', '"max_concurrency": 0 ')
    assert.equal(Buffer.byteLength(rewritten), Buffer.byteLength(original))
    fs.writeFileSync(file, rewritten)
    fs.utimesSync(file, before.atime, before.mtime)

    const synced = syncCodexAccountScheduling(root, vm)
    assert.equal(synced.updated, true)
    assert.equal(JSON.parse(fs.readFileSync(file, 'utf8')).accounts[0].max_concurrency, 40)
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})
