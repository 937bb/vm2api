import { createHash } from 'node:crypto'
import fs from 'node:fs'

const file = process.argv[2]
if (!file) throw new Error('usage: node scripts/patch-codex-kernel-retry.mjs <kin-codex-kernel>')

const expectedHash = '11c8ec08d2660a3d8beef4a3b55ed795c4b550a77ee6c8ff50d9b4af9c92d895'
const expectedPatchedHash = '1b116301d94daed14ae3231d98b06a4a874b8465b31866fcec8c4f1912490a86'
const offset = 0x1a1874
// Force every hop error terminal; the original cmove re-enabled retry for enum variant 8.
const original = Buffer.from('bb010000000f44da', 'hex')
const patched = Buffer.from('bb00000000909090', 'hex')
const signatureOffset = offset - 6
const signature = Buffer.from('83f8080fb6d2bb010000000f44daeb0580e10131db', 'hex')
const binary = fs.readFileSync(file)
const hash = createHash('sha256').update(binary).digest('hex')

if (hash !== expectedHash) throw new Error(`unsupported kin-codex-kernel sha256: ${hash}`)
if (!binary.subarray(offset, offset + original.length).equals(original)) {
  throw new Error(`kin-codex-kernel retry branch mismatch at 0x${offset.toString(16)}`)
}
if (
  !binary.subarray(signatureOffset, signatureOffset + signature.length).equals(signature) ||
  binary.indexOf(signature) !== signatureOffset ||
  binary.lastIndexOf(signature) !== signatureOffset
) {
  throw new Error('kin-codex-kernel retry branch signature is not unique')
}

patched.copy(binary, offset)
const patchedHash = createHash('sha256').update(binary).digest('hex')
if (patchedHash !== expectedPatchedHash) {
  throw new Error(`unexpected patched kin-codex-kernel sha256: ${patchedHash}`)
}
fs.writeFileSync(file, binary, { mode: 0o755 })
console.log(`patched kin-codex-kernel internal retry branch: ${expectedHash} -> ${patchedHash}`)
