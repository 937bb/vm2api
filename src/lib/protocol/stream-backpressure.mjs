function responseClosed(res) {
  return !!(res?.destroyed || res?.writableEnded || res?.closed)
}

/**
 * Write one downstream chunk and stop consuming upstream data until Node's
 * HTTP buffer drains. Resolves false when the client disappears.
 */
export async function writeWithBackpressure(res, chunk, signal) {
  if (!res || responseClosed(res) || signal?.aborted) return false

  let accepted
  try {
    accepted = res.write(chunk)
  } catch {
    return false
  }
  if (accepted !== false) return true
  if (responseClosed(res) || signal?.aborted) return false

  return await new Promise((resolve) => {
    let settled = false
    const cleanup = () => {
      res.off?.('drain', onDrain)
      res.off?.('close', onClose)
      res.off?.('error', onError)
      signal?.removeEventListener?.('abort', onAbort)
    }
    const finish = (ok) => {
      if (settled) return
      settled = true
      cleanup()
      resolve(ok)
    }
    const onDrain = () => finish(!responseClosed(res) && !signal?.aborted)
    const onClose = () => finish(false)
    const onError = () => finish(false)
    const onAbort = () => finish(false)

    res.once?.('drain', onDrain)
    res.once?.('close', onClose)
    res.once?.('error', onError)
    signal?.addEventListener?.('abort', onAbort, { once: true })

    // Close/abort may race with listener installation.
    if (responseClosed(res) || signal?.aborted) finish(false)
  })
}

/** Keepalive timers must never add bytes while a real response is backpressured. */
export function writeKeepaliveIfWritable(res, chunk, writeHeaders) {
  if (!res || responseClosed(res) || res.writableNeedDrain) return false
  if (!res.headersSent) writeHeaders?.(res)
  if (responseClosed(res) || res.writableNeedDrain) return false
  try {
    // Even when this flips the stream into backpressure, the one small
    // keepalive was queued; later timer beats will observe writableNeedDrain.
    res.write(chunk)
    return true
  } catch {
    return false
  }
}
