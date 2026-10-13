/**
 * Downstream SSE keepalive aligned with sub2api:
 *   - default: event: ping
 *   - official Claude Code ≥ 2.1.193: empty content_block_delta so the CLI
 *     parser does not treat comment/ping as a dead stream
 * Used only toward the client. Upstream idle is the worker interval timeout.
 */

export const CLAUDE_CODE_NOOP_DELTA_KEEPALIVE_MIN_VERSION = '2.1.193'
export const DEFAULT_STREAM_KEEPALIVE_MS = 10_000

const DELTA_FOR_BLOCK = Object.freeze({
  text: 'text_delta',
  tool_use: 'input_json_delta',
  thinking: 'thinking_delta',
})

const FIELD_FOR_DELTA = Object.freeze({
  text_delta: 'text',
  input_json_delta: 'partial_json',
  thinking_delta: 'thinking',
})

export function compareSemver(a, b) {
  const left = String(a || '')
    .split('.')
    .map((part) => parseInt(part, 10) || 0)
  const right = String(b || '')
    .split('.')
    .map((part) => parseInt(part, 10) || 0)
  for (let i = 0; i < 3; i++) {
    const delta = (left[i] || 0) - (right[i] || 0)
    if (delta) return delta
  }
  return 0
}

export function extractCliVersion(ua = '') {
  const m = String(ua || '').match(/claude-cli\/(\d+\.\d+\.\d+)/i)
  return m ? m[1] : ''
}

export function shouldUseClaudeCodeNoopDeltaKeepalive(ua = '') {
  const version = extractCliVersion(ua)
  if (!version) return false
  return compareSemver(version, CLAUDE_CODE_NOOP_DELTA_KEEPALIVE_MIN_VERSION) >= 0
}

export function keepaliveDeltaTypeForContentBlock(blockType = '') {
  return DELTA_FOR_BLOCK[String(blockType || '')] || ''
}

export function buildPingKeepalive() {
  return 'event: ping\ndata: {"type":"ping"}\n\n'
}

export function buildCommentKeepalive() {
  return ': keepalive\n\n'
}

export function buildClaudeCodeNoopDeltaKeepalive(index, deltaType) {
  const field = FIELD_FOR_DELTA[deltaType]
  if (field == null || !Number.isInteger(index) || index < 0) return ''
  return `event: content_block_delta\ndata: {"type":"content_block_delta","index":${index},"delta":{"type":"${deltaType}","${field}":""}}\n\n`
}

export function observeKeepaliveEvent(state, event) {
  if (!event || typeof event !== 'object') return state
  const next = { ...state }
  const type = String(event.type || '')
  const index = Number(event.index)
  if (type === 'content_block_start') {
    const deltaType = keepaliveDeltaTypeForContentBlock(event.content_block?.type)
    if (Number.isInteger(index) && deltaType) {
      next.blockIndex = index
      next.deltaType = deltaType
    } else {
      next.blockIndex = -1
      next.deltaType = ''
    }
    return next
  }
  if (type === 'content_block_delta') {
    const deltaType = String(event.delta?.type || '')
    if (Number.isInteger(index) && FIELD_FOR_DELTA[deltaType]) {
      next.blockIndex = index
      next.deltaType = deltaType
    }
    return next
  }
  if (type === 'content_block_stop' && Number.isInteger(index) && index === next.blockIndex) {
    next.blockIndex = -1
    next.deltaType = ''
    return next
  }
  if (type === 'message_stop' || type === 'error') {
    next.blockIndex = -1
    next.deltaType = ''
    next.done = true
    return next
  }
  return next
}

export function parseSseDataLine(line) {
  const raw = String(line || '')
  if (!raw.startsWith('data:')) return null
  const piece = raw.slice(5).trim()
  if (!piece || piece === '[DONE]') return null
  try {
    const event = JSON.parse(piece)
    return event && typeof event === 'object' ? event : null
  } catch {
    return null
  }
}

export function resolveKeepaliveChunk({ protocol = 'anthropic.messages', userAgent = '', state = {} } = {}) {
  if (String(protocol || '') !== 'anthropic.messages') return buildCommentKeepalive()
  if (
    shouldUseClaudeCodeNoopDeltaKeepalive(userAgent) &&
    Number.isInteger(state.blockIndex) &&
    state.blockIndex >= 0 &&
    state.deltaType
  ) {
    const block = buildClaudeCodeNoopDeltaKeepalive(state.blockIndex, state.deltaType)
    if (block) return block
  }
  return buildPingKeepalive()
}

/**
 * @param {{ intervalMs?: number, userAgent?: string, protocol?: string, write: (chunk: string) => void | boolean }} opts
 */
export function createDownstreamKeepalive({
  intervalMs = DEFAULT_STREAM_KEEPALIVE_MS,
  userAgent = '',
  protocol = 'anthropic.messages',
  write,
} = {}) {
  const interval = Math.max(0, Number(intervalMs) || 0)
  let state = { blockIndex: -1, deltaType: '', done: false }
  let lastWriteAt = 0
  let timer = null
  let armed = false

  const markWrite = () => {
    lastWriteAt = Date.now()
  }

  const beat = () => {
    if (!armed || state.done || interval <= 0) return
    if (Date.now() - lastWriteAt < interval) return
    if (typeof write !== 'function') return
    try {
      const written = write(resolveKeepaliveChunk({ protocol, userAgent, state }))
      if (written !== false) markWrite()
    } catch {
      stop()
    }
  }

  const start = () => {
    if (timer || interval <= 0) return
    timer = setInterval(beat, Math.min(interval, 1000))
    timer.unref?.()
  }

  const stop = () => {
    if (timer) {
      clearInterval(timer)
      timer = null
    }
    state = { ...state, done: true }
  }

  const arm = () => {
    if (state.done || interval <= 0) return
    armed = true
    markWrite()
    start()
  }

  const observeActivity = () => {
    armed = true
    markWrite()
  }

  const observeLine = (line) => {
    observeActivity()
    const event = parseSseDataLine(line)
    if (event) {
      state = observeKeepaliveEvent(state, event)
      if (state.done) stop()
    }
  }

  return { start, stop, arm, observeActivity, observeLine, beat, getState: () => ({ ...state, armed, lastWriteAt }) }
}
