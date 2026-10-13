const CLOSED_SOCKET_ERRORS = new Set(['ECONNRESET', 'EPIPE', 'ERR_STREAM_DESTROYED'])

export function handleHttpClientError(err, socket, { log = console.error } = {}) {
  try {
    log(
      JSON.stringify({
        event: 'http_client_error',
        code: err?.code || null,
        message: String(err?.message || err).slice(0, 200),
      }),
    )
  } catch {}

  const code = String(err?.code || '')
  if (!socket || socket.destroyed || !socket.writable || CLOSED_SOCKET_ERRORS.has(code)) {
    try {
      socket?.destroy()
    } catch {}
    return
  }

  try {
    socket.end(
      'HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{"error":{"type":"invalid_request_error","message":"malformed request"}}\n',
    )
  } catch {
    try {
      socket.destroy()
    } catch {}
  }
}
