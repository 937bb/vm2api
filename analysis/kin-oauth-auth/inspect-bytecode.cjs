'use strict'

const fs = require('node:fs')
const vm = require('node:vm')

const blobPath = process.argv[2]
if (!blobPath) {
  process.stderr.write('usage: node inspect-bytecode.cjs <service-entry.cjs.blob>\n')
  process.exitCode = 2
} else {
  const blob = fs.readFileSync(blobPath)
  const script = new vm.Script('', { cachedData: blob, sourceless: true })
  const result = {
    runtime: {
      node: process.version,
      v8: process.versions.v8,
      pkg: process.versions.pkg || '',
    },
    blob_bytes: blob.length,
    cached_data_rejected: script.cachedDataRejected,
  }

  if (!script.cachedDataRejected) {
    const wrapper = script.runInThisContext()
    result.wrapper = {
      type: typeof wrapper,
      name: wrapper?.name || '',
      arity: wrapper?.length,
      preview: String(wrapper).slice(0, 500),
    }
  }

  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`)
}
