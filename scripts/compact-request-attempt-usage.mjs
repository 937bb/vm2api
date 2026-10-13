#!/usr/bin/env node

import path from 'node:path'
import process from 'node:process'
import { DatabaseSync } from 'node:sqlite'
import { compactAttemptUsage } from '../src/lib/db/repos/request-attempts-repo.mjs'

function arg(name, fallback = null) {
  const index = process.argv.indexOf(name)
  return index >= 0 ? process.argv[index + 1] : fallback
}

const dbPath = path.resolve(arg('--db', process.env.KIN_DB_PATH || path.join(process.cwd(), 'data', 'kin.db')))
const batchSize = Math.max(10, Math.min(1000, Number(arg('--batch', 100)) || 100))
const apply = process.argv.includes('--apply')
const db = new DatabaseSync(dbPath)
db.exec('PRAGMA busy_timeout = 5000')

const select = db.prepare(`
  SELECT id, usage_json
  FROM request_attempts
  WHERE id > ? AND usage_json IS NOT NULL
    AND instr(usage_json, '"attribution"') > 0
  ORDER BY id
  LIMIT ?
`)
const update = db.prepare('UPDATE request_attempts SET usage_json = ? WHERE id = ?')

let cursor = 0
let rows = 0
let beforeBytes = 0
let afterBytes = 0
try {
  for (;;) {
    const batch = select.all(cursor, batchSize)
    if (!batch.length) break
    if (apply) db.exec('BEGIN IMMEDIATE')
    try {
      for (const row of batch) {
        cursor = Number(row.id)
        const raw = String(row.usage_json || '')
        let usage
        try {
          usage = JSON.parse(raw)
        } catch {
          continue
        }
        const compact = JSON.stringify(compactAttemptUsage(usage))
        rows += 1
        beforeBytes += Buffer.byteLength(raw)
        afterBytes += Buffer.byteLength(compact)
        if (apply) update.run(compact, row.id)
      }
      if (apply) db.exec('COMMIT')
    } catch (error) {
      if (apply) db.exec('ROLLBACK')
      throw error
    }
    if (rows % 1000 < batchSize) {
      console.log(
        `rows=${rows} before_mb=${(beforeBytes / 1048576).toFixed(1)} after_mb=${(afterBytes / 1048576).toFixed(1)}`,
      )
    }
  }
  console.log(
    JSON.stringify({
      apply,
      rows,
      before_bytes: beforeBytes,
      after_bytes: afterBytes,
      saved_bytes: beforeBytes - afterBytes,
    }),
  )
} finally {
  db.close()
}
