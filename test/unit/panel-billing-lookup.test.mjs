import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { buildUsage, indexBillingAccounts, lookupBilling } from '../../src/lib/admin/panel-api.mjs'

test('billing index keeps UUID row when leftover vm-id account is empty', () => {
  const leftover = {
    account_id: 'vm-50',
    vm_id: 'vm-50',
    today_requests: 0,
    today_cache_read_tokens: 0,
    requests: 0,
    today_cost: 0,
  }
  const real = {
    account_id: '7667b68b-3351-4bc9-b06e-39d6795b3019',
    vm_id: 'vm-50',
    today_requests: 243,
    today_cache_read_tokens: 24757802,
    requests: 245,
    today_cost: 24.45,
  }
  const index = indexBillingAccounts({ accounts: [real, leftover] })
  const vm = { id: 'vm-50', account_uuid: real.account_id }
  const leftoverAcc = { account_id: 'vm-50', vm_id: 'vm-50' }
  assert.equal(lookupBilling(index, vm), real)
  assert.equal(lookupBilling(index, leftoverAcc), real)
  assert.equal(lookupBilling(index, { id: 'vm-50' }), real)
})

test('billing rows of an older account on a reused slot keep their own email', (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-billing-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const billing = {
    accounts: [
      { account_id: 'old-uuid', vm_id: 'vm-02', total_cost: 9, email: 'old@example.com' },
      { account_id: 'new-uuid', vm_id: 'vm-02', total_cost: 1 },
      { vm_id: 'vm-03', total_cost: 1 },
    ],
  }
  const accountQuota = {
    snapshot: () => ({
      accounts: [
        { account_id: 'new-uuid', vm_id: 'vm-02', email: 'new@example.com' },
        { account_id: 'vm3-uuid', vm_id: 'vm-03', email: 'three@example.com' },
      ],
    }),
  }
  const usage = buildUsage({
    accountQuota,
    cfg: { paths: { project: root } },
    requestLog: { billingStats: () => billing },
  })
  const emails = usage.data.billing.accounts.map((row) => row.email)
  assert.deepEqual(emails, ['old@example.com', 'new@example.com', 'three@example.com'])
})

test('usage rows of a deleted VM are not listed', (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-usage-deleted-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  fs.mkdirSync(path.join(root, 'vms'), { recursive: true })
  fs.writeFileSync(path.join(root, 'vms', 'vm-02.json'), JSON.stringify({ id: 'vm-02' }))
  const usage = buildUsage({
    accountQuota: {
      snapshot: () => ({
        accounts: [
          { account_id: 'two-uuid', vm_id: 'vm-02', email: 'two@example.com', requests: 1 },
          { account_id: 'three-uuid', vm_id: 'vm-03', email: 'three@example.com', requests: 5 },
        ],
      }),
    },
    cfg: { paths: { project: root } },
    requestLog: {
      billingStats: () => ({ accounts: [{ account_id: 'three-uuid', vm_id: 'vm-03', total_cost: 3 }] }),
    },
  })
  assert.deepEqual(
    usage.data.accounts.map((a) => a.vm_id),
    ['vm-02'],
  )
  assert.equal(usage.data.totals.requests, 1)
  assert.equal(usage.data.billing.accounts[0].email, 'three@example.com')
})

test('GPT usage row without pool email takes slot email', (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-gpt-usage-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  fs.mkdirSync(path.join(root, 'vms'), { recursive: true })
  fs.writeFileSync(
    path.join(root, 'vms', 'vm-codex-01.json'),
    JSON.stringify({
      id: 'vm-codex-01',
      platform: 'openai',
      family: 'codex',
      codex_kernel: true,
      email: 'lemeryvorhees@gmail.com',
      codex: { email: 'lemeryvorhees@gmail.com' },
    }),
  )
  const usage = buildUsage({
    accountQuota: {
      snapshot: () => ({
        accounts: [{ account_id: 'vm-codex-01', vm_id: 'vm-codex-01', email: null }],
      }),
    },
    cfg: { paths: { project: root } },
    requestLog: {
      billingStats: () => ({
        accounts: [{ account_id: 'vm-codex-01', vm_id: 'vm-codex-01', total_cost: 139.87, today_cost: 24.86 }],
      }),
    },
  })
  const row = usage.data.accounts.find((a) => a.vm_id === 'vm-codex-01')
  assert.equal(row?.email, 'lemeryvorhees@gmail.com')
  assert.equal(row?.total_cost, 139.87)
})

test('GPT usage totals do not count cached tokens twice in the denominator', (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-gpt-cache-rate-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  fs.mkdirSync(path.join(root, 'vms'), { recursive: true })
  fs.writeFileSync(
    path.join(root, 'vms', 'vm-codex-01.json'),
    JSON.stringify({ id: 'vm-codex-01', platform: 'openai', family: 'codex', codex_kernel: true }),
  )
  const usage = buildUsage({
    accountQuota: {
      snapshot: () => ({
        accounts: [
          {
            account_id: 'vm-codex-01',
            vm_id: 'vm-codex-01',
            tokens_in: 100,
            cache_read_tokens: 80,
            cache_creation_tokens: 0,
          },
        ],
      }),
    },
    cfg: { paths: { project: root } },
    requestLog: { billingStats: () => ({ accounts: [] }) },
  })
  assert.equal(usage.data.totals.prompt_tokens, 100)
  assert.equal(usage.data.totals.cache_hit_rate, 0.8)
})
