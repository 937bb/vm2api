import { officialMessagesBody } from '../protocol/anthropic-messages.mjs'
import { prepareClassifierBody } from '../protocol/request-purpose.mjs'
import { consumeClaudeSSEData } from '../protocol/convert.mjs'
import { mapUpstreamError } from '../core/errors.mjs'
import { isContentPolicyErrorCode } from '../core/refusal-guard.mjs'
import { forwardApi, readApiJson } from '../transport/api-kernel-client.mjs'
import { messagesUrl, normalizeProtocol, resolvePreset, responsesUrl, upstreamAuthHeaders } from './api-presets.mjs'
import { claudeToOpenAIResponsesRequest } from './api-openai.mjs'
import {
  assembleCodexBodyFromSse,
  codexBodyToAnthropicMessage,
  createAnthropicSseState,
  createChatSseState,
  responsesSseToAnthropicEvents,
  isToolArgumentsError,
  responsesSseToChatChunk,
} from '../protocol/codex-convert.mjs'
import { usageFromSseLine } from '../protocol/handle-codex.mjs'
import { writeWithBackpressure } from '../protocol/stream-backpressure.mjs'

export function resolveInferenceBackend(req) {
  if (req?.apiKeyKind === 'managed') {
    return String(req.apiKeyRecord?.category || 'oauth').toLowerCase() === 'api' ? 'api' : 'oauth'
  }
  if (req?.apiKeyKind === 'master') {
    return String(req.headers?.['x-kin-backend'] || '').toLowerCase() === 'api' ? 'api' : 'oauth'
  }
  return 'oauth'
}

export { messagesUrl, responsesUrl } from './api-presets.mjs'

function joinHeaderMap(headers = {}) {
  const out = {}
  for (const [k, v] of Object.entries(headers)) {
    if (v == null || v === '') continue
    out[k] = String(v)
  }
  return out
}

async function readLines(stream, onLine) {
  let buf = ''
  for await (const chunk of stream) {
    buf += chunk.toString('utf8')
    let idx
    while ((idx = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, idx)
      buf = buf.slice(idx + 1)
      await onLine(line)
    }
  }
  if (buf) await onLine(buf)
}

export async function runApiInference({
  req,
  res,
  cfg,
  scheduler,
  store,
  protocol,
  inbound,
  convertedBody,
  clientStream,
  deliveryMode,
  signal,
  timeoutMs,
  personaHideTokens,
  cacheTtl,
  converters,
  requestContext = null,
} = {}) {
  const canonical = officialMessagesBody(convertedBody)
  const model = canonical.model
  const picked = scheduler.pick(model)
  if (!picked.ok) {
    return {
      ok: false,
      status: picked.code === 'model_not_found' ? 404 : 503,
      via: 'api-kernel',
      body: { type: 'error', error: { type: 'api_error', code: picked.code, message: picked.message } },
      terminalState: 'exhausted',
    }
  }

  const preset = resolvePreset(picked.endpoint.kind, picked.endpoint)
  const protocolKind = normalizeProtocol(picked.endpoint.protocol || preset.protocol, preset.kind)
  const extraHeaders = joinHeaderMap(picked.endpoint.headers)
  const isOpenAI = protocolKind === 'openai'
  if (requestContext && (isOpenAI || picked.upstream_model !== canonical.model)) {
    return {
      ok: false,
      status: 400,
      terminalState: 'rejected',
      body: {
        type: 'error',
        error: {
          type: 'invalid_request_error',
          code: 'classifier_model_incompatible',
          message: 'Classifier requests require the selected Anthropic model without endpoint model substitution.',
        },
      },
    }
  }
  const body = isOpenAI
    ? { ...claudeToOpenAIResponsesRequest({ ...canonical, model: picked.upstream_model }), stream: true }
    : { ...canonical, model: picked.upstream_model, stream: true }
  if (requestContext) Object.assign(body, prepareClassifierBody(body))
  const headers = {
    'content-type': 'application/json',
    accept: 'text/event-stream',
    ...upstreamAuthHeaders(protocolKind, picked.key.api_key, {
      ...extraHeaders,
      auth_scheme: picked.endpoint.auth_scheme || extraHeaders.auth_scheme || extraHeaders.anthropic_apikey_auth_scheme,
    }),
  }

  let upstream
  try {
    upstream = await forwardApi({
      cfg,
      url: isOpenAI ? responsesUrl(preset.base_url) : messagesUrl(preset.base_url),
      headers,
      body,
      proxyUrl: picked.key.proxy_url,
      signal,
      timeoutMs,
    })
  } catch (error) {
    return {
      ok: false,
      status: 502,
      via: 'api-kernel',
      body: {
        type: 'error',
        error: {
          type: 'api_error',
          code: error.code || 'api_kernel_transport',
          message: String(error.message || error).slice(0, 300),
        },
      },
      terminalState: 'transport_error',
      transportError: true,
    }
  }

  const status = Number(upstream.statusCode) || 502
  if (status === 429 && !picked.endpoint.disable_cooling) {
    const retry = Number(upstream.headers['retry-after'] || 30)
    const until = new Date(Date.now() + Math.max(1, retry) * 1000).toISOString()
    try {
      store.setKeyCooldown(picked.key.id, until)
    } catch {}
    scheduler.reload(store.listRaw())
  }

  const resultBase = {
    ok: status >= 200 && status < 300,
    status,
    via: 'api-kernel',
    vmId: null,
    accountId: picked.key.id,
    endpointId: picked.endpoint.id,
    model: picked.upstream_model,
    headers: { ...upstream.headers },
  }

  if (status >= 400) {
    const payload = await readApiJson(upstream).catch(() => ({}))
    const message = payload?.error?.message || payload?.message || `upstream ${status}`
    const upstreamCode = String(payload?.error?.code || payload?.code || '').trim()
    const contentCode = isContentPolicyErrorCode(upstreamCode) ? upstreamCode.toLowerCase() : ''
    return {
      ...resultBase,
      ok: false,
      body: {
        type: 'error',
        error: {
          ...(requestContext ? payload?.error : {}),
          ...(requestContext && payload?.error?.code ? { upstream_code: payload.error.code } : {}),
          ...(!requestContext && contentCode ? { code: contentCode } : {}),
          type: requestContext ? payload?.error?.type || 'api_error' : 'api_error',
          message: String(message).slice(0, 300),
        },
      },
      terminalState: 'rejected',
    }
  }

  if (isOpenAI) {
    return await runOpenAIResponsesUpstream({
      upstream,
      resultBase,
      protocol,
      inbound,
      clientStream,
      deliveryMode,
      res,
      converters,
      body,
      signal,
    })
  }

  const classifierSseState = { dataBuf: '' }
  let classifierError = null
  let classifierStopped = false
  const readUpstreamLines = async (onLine) => {
    try {
      await readLines(upstream, async (line) => {
        if (requestContext) {
          const event = consumeClaudeSSEData(line, classifierSseState)
          if (event?.type === 'error')
            classifierError = {
              ...event,
              error: { ...event.error, ...(event.error?.code ? { upstream_code: event.error.code } : {}) },
            }
          if (event?.type === 'message_stop') classifierStopped = true
        }
        await onLine(line)
      })
    } catch (error) {
      if (!requestContext) throw error
      classifierError = {
        type: 'error',
        error: {
          type: 'api_error',
          code: 'upstream_stream_interrupted',
          status: 502,
          message: String(error.message || 'Classifier stream interrupted').slice(0, 300),
        },
      }
    }
  }
  const classifierFailure = () => {
    if (!requestContext || (!classifierError && classifierStopped)) return null
    const body = classifierError || {
      type: 'error',
      error: {
        type: 'api_error',
        code: 'upstream_stream_interrupted',
        status: 502,
        message: 'Classifier stream ended before message_stop',
      },
    }
    const declared = Number(body.error?.status)
    const status = declared >= 400 && declared < 600 ? declared : mapUpstreamError(200, body).status
    return {
      ...resultBase,
      ok: false,
      status,
      body,
      headers: {
        ...resultBase.headers,
        ...(body.error?.retry_after ? { 'retry-after': String(body.error.retry_after) } : {}),
      },
      terminalState: 'rejected',
    }
  }

  if (!clientStream) {
    const assembler = converters.createClaudeMessageAssembler()
    let committed = false
    await readUpstreamLines(async (line) => {
      applyMaybeUsageHide(line, personaHideTokens, cacheTtl, converters)
      converters.applyClaudeSSELineToMessage(line, assembler)
      if (!committed && String(line).startsWith('data:')) committed = true
    })
    const failure = classifierFailure()
    if (failure) return { ...failure, committed: false }
    return {
      ...resultBase,
      ok: resultBase.ok && !!assembler.message,
      body: assembler.message || assembler.error || { type: 'error', error: { message: 'empty api upstream' } },
      usage: assembler.message?.usage || null,
      terminalState: resultBase.ok ? 'verified' : 'rejected',
      committed,
    }
  }

  let state
  if (protocol === 'openai.chat')
    state = converters.createOpenAIChatStreamState(inbound.model || body.model, picked.endpoint.id)
  else if (protocol === 'openai.completions')
    state = converters.createOpenAICompletionStreamState(inbound.model || body.model, picked.endpoint.id)
  else if (protocol === 'openai.responses')
    state = converters.createResponsesStreamState(inbound.model || body.model, picked.endpoint.id)

  let committed = false
  let sawStop = false
  const started = Date.now()
  let ttftMs = null
  await readUpstreamLines(async (line) => {
    if (personaHideTokens) line = converters.hidePersonaUsageInSseLine(line, personaHideTokens, cacheTtl) || line
    if (String(line).startsWith('data:') && ttftMs == null) ttftMs = Date.now() - started
    if (String(line).includes('message_stop')) sawStop = true
    if (!committed && String(line).startsWith('data:')) {
      committed = true
      if (protocol === 'anthropic.messages' && !res.headersSent) converters.writeSSEHeaders(res)
    }
    if (protocol === 'anthropic.messages') {
      if (!res.headersSent) converters.writeSSEHeaders(res)
      await writeWithBackpressure(res, String(line).endsWith('\n') ? String(line) : `${line}\n`, signal)
      return
    }
    const writeChunks = async (chunks) => {
      if (!chunks.length) return
      if (!res.headersSent) converters.writeSSEHeaders(res)
      for (const chunk of chunks) {
        if (!(await writeWithBackpressure(res, `data: ${JSON.stringify(chunk)}\n\n`, signal))) break
      }
    }
    if (protocol === 'openai.chat') await writeChunks(converters.claudeSSELineToOpenAIChatChunks(line, state))
    else if (protocol === 'openai.completions')
      await writeChunks(converters.claudeSSELineToOpenAICompletionChunks(line, state))
    else await writeChunks(converters.claudeSSELineToResponsesEvents(line, state))
  })

  const failure = classifierFailure()
  if (failure) return { ...failure, committed, ttftMs }

  return {
    ...resultBase,
    ok: resultBase.ok && (deliveryMode !== 'verified' || sawStop),
    terminalState: sawStop ? 'verified' : committed ? 'incomplete' : 'rejected',
    committed,
    ttftMs,
  }
}

async function runOpenAIResponsesUpstream({
  upstream,
  resultBase,
  protocol,
  inbound,
  clientStream,
  deliveryMode,
  res,
  converters,
  body,
  signal,
}) {
  const chunks = []
  const anthropicSse = protocol === 'anthropic.messages' ? createAnthropicSseState() : null
  const chatSse = createChatSseState()
  let conversionError = null
  let committed = false
  let completed = false
  let ttftMs = null
  let streamedUsage = null
  const started = Date.now()

  await readLines(upstream, async (line) => {
    if (conversionError) return
    const raw = String(line || '')
    const seen = usageFromSseLine(raw)
    if (seen) streamedUsage = seen
    if (raw.startsWith('data:') && ttftMs == null) ttftMs = Date.now() - started
    if (!committed && raw.startsWith('data:')) committed = true
    if (!clientStream) {
      chunks.push(raw)
      return
    }
    if (!res.headersSent) converters.writeSSEHeaders(res)
    if (protocol === 'openai.responses') {
      await writeWithBackpressure(res, raw.endsWith('\n') ? raw : `${raw}\n`, signal)
      if (/response\.(completed|done)/.test(raw)) completed = true
      return
    }
    try {
      const mapped =
        protocol === 'anthropic.messages'
          ? responsesSseToAnthropicEvents(raw, anthropicSse)
          : responsesSseToChatChunk(raw, 'codex', chatSse)
      if (mapped) {
        await writeWithBackpressure(res, mapped, signal)
        if (mapped.includes('[DONE]') || mapped.includes('event: message_stop\n')) completed = true
      }
    } catch (error) {
      if (error.code !== 'tool_arguments_mismatch') throw error
      conversionError = { type: 'upstream_protocol_error', code: error.code, message: error.message }
      await writeWithBackpressure(
        res,
        protocol === 'anthropic.messages'
          ? `event: error\ndata: ${JSON.stringify({ type: 'error', error: conversionError })}\n\n`
          : `data: ${JSON.stringify({ error: conversionError })}\n\ndata: [DONE]\n\n`,
        signal,
      )
    }
  })

  if (!clientStream) {
    let assembled
    let outBody
    try {
      assembled = assembleCodexBodyFromSse(chunks, {})
      outBody =
        protocol === 'anthropic.messages'
          ? codexBodyToAnthropicMessage(assembled, inbound?.model || body?.model)
          : assembled
    } catch (error) {
      if (!isToolArgumentsError(error)) throw error
      return {
        ...resultBase,
        ok: false,
        status: 502,
        body: { error: { type: 'upstream_protocol_error', code: error.code, message: error.message } },
        terminalState: 'incomplete',
        committed: true,
        usage: streamedUsage,
      }
    }
    return {
      ...resultBase,
      ok: resultBase.ok && !!(assembled && (assembled.output || assembled.id || outBody)),
      body: outBody,
      usage: assembled?.usage || streamedUsage || outBody?.usage || null,
      terminalState: resultBase.ok ? 'verified' : 'rejected',
      committed: true,
    }
  }
  if (conversionError)
    return {
      ...resultBase,
      ok: false,
      status: 502,
      body: { error: conversionError },
      terminalState: 'incomplete',
      committed,
      usage: streamedUsage,
      ttftMs,
    }

  return {
    ...resultBase,
    ok: resultBase.ok && (deliveryMode !== 'verified' || completed || committed),
    usage: streamedUsage,
    terminalState: completed ? 'verified' : committed ? 'incomplete' : 'rejected',
    committed,
    ttftMs,
  }
}

function applyMaybeUsageHide() {}
