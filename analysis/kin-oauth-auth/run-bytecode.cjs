'use strict'

const fs = require('node:fs')
const path = require('node:path')
const vm = require('node:vm')

if (process.env.KIN_OAUTH_TRACE_PRELOAD) require(process.env.KIN_OAUTH_TRACE_PRELOAD)

const blobPath = process.argv[2]
if (!blobPath) {
  process.stderr.write('usage: node run-bytecode.cjs <service-entry.cjs.blob>\n')
  process.exitCode = 2
} else {
  const blob = fs.readFileSync(blobPath)
  const script = new vm.Script(undefined, { cachedData: blob, sourceless: true })
  if (script.cachedDataRejected) throw new Error('cached bytecode rejected by this V8 runtime')

  const wrapper = script.runInThisContext()
  const filename = '/snapshot/oauth-auth/service-entry.cjs'
  const moduleRecord = { exports: {}, filename, paths: [] }
  wrapper.call(
    moduleRecord.exports,
    moduleRecord.exports,
    require,
    moduleRecord,
    filename,
    path.dirname(filename),
  )
}
