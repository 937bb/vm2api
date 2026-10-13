# kin-oauth-auth reverse analysis

Analysis date: 2026-10-13

## Verdict

`kin-oauth-auth` is recoverable at the behavioral level. It is not encrypted with
a cryptographic scheme. It is an x86-64 Linux-static Node.js executable built by
`pkg 5.8.1`; the application source was bundled into V8 sourceless cached
bytecode and the ELF section table was removed.

The exact original source text, comments, formatting and original local variable
names cannot be guaranteed. The module boundary, operations, constants, network
destinations, error paths and a behavior-compatible implementation can be
recovered and tested.

This binary is not in the normal inference request path. Repository-wide call
site analysis finds it only in the control-plane OAuth import/exchange path:

- `session_key`: panel sessionKey import.
- `exchange_code`: pasted OAuth authorization-code exchange.

It can break account import, authorization-code exchange, OAuth identity
bootstrap, or return bad credentials. It does not directly explain sustained
503 responses or stream disconnects from ordinary inference traffic.

## Sample identity

Production sample:

```text
path: /opt/vm2api/bin/kin-oauth-auth
size: 14430374 bytes
sha256: fe64b4ea20ca694863f9041fd02fd2c0c4e6d30cce99e273ee693d7d730a8095
build-id: e8e9fc449943f935d469333b1101f224a4c0164f
format: ELF 64-bit LSB, x86-64, statically linked, stripped, no section header
runtime: Node.js v18.5.0, V8 10.2.154.4-node.8
packer: pkg 5.8.1
entrypoint: /snapshot/oauth-auth/service-entry.cjs
recorded original source size: 636155 bytes
```

Extracted V8 blob:

```text
payload position: 13250000
blob size: 1105600 bytes
sha256: 9804a3582dc34321de7d8fa568fe66987661513558e52aa4105c0d2a37a48837
V8 cachedDataRejected on matching runtime: false
wrapper: anonymous CommonJS function, arity 5
```

## Recovered protocol

The process accepts one JSON object on stdin and emits one JSON object on
stdout. Successful output has the form:

```json
{"ok":true,"credential":{}}
```

Failure output has the form:

```json
{"ok":false,"error":{"code":"...","message":"..."}}
```

Known input operations:

```json
{
  "operation": "session_key",
  "session_key": "sk-ant-sid...",
  "scope": "user:profile user:inference ...",
  "runtime_mode": "oauth|setup-token",
  "proxy_url": "socks5h://host:port",
  "timeout_ms": 15000
}
```

```json
{
  "operation": "exchange_code",
  "code": "authorization-code",
  "code_verifier": "pkce-verifier",
  "state": "optional-provider-state",
  "redirect_uri": "https://platform.claude.com/oauth/code/callback",
  "proxy_url": "socks5h://host:port",
  "timeout_ms": 15000
}
```

## Recovered network flow

The `session_key` operation performs:

1. `GET https://claude.ai/api/organizations`
2. `POST https://platform.claude.com/v1/oauth/{organization_uuid}/authorize`
3. `POST https://platform.claude.com/v1/oauth/token`
4. `GET https://api.anthropic.com/api/claude_cli/bootstrap?entrypoint=claude-code&model=claude-opus-5-5`

The session cookie is used for the first two browser-style calls. The OAuth
access token is used for the official Claude Code bootstrap call.

The `exchange_code` operation starts at step 3 and then performs step 4.

Runtime interception confirmed the first requests and header names without
logging any header values or request bodies:

```text
GET  claude.ai/api/organizations
headers: accept, accept-encoding, cookie, origin, referer, user-agent

POST platform.claude.com/v1/oauth/token
headers: accept, accept-encoding, content-length, content-type, user-agent,
         x-app, x-stainless-*
```

Static extraction found one additional
`https://chatgpt.com/backend-api/codex/models` constant next to shared runtime
configuration constants. It was not observed in either operation during dynamic
tracing or the full local TLS/SOCKS integration test. No webhook, Telegram,
Discord, paste, analytics, Sentry, or other credential-collection endpoint was
found or observed.

## 503 boundary

`src/lib/oauth/cookie-auth.mjs` is the sole process-spawn wrapper. Its callers
are the panel sessionKey import route and OAuth authorization-code exchange.
`panelImportErrorPayload()` maps helper failures to 400, 500, or 502; it does
not map them to 503.

There is no import or spawn call from the inference protocol handlers, account
pool, Codex kernel supervisor, Go worker transport, or stream relay. Therefore:

- an OAuth import/exchange failure can be caused by this binary;
- a malformed credential produced by it can later cause authentication errors;
- ordinary high-concurrency inference 503/stream failures must be investigated
  in the kernel, worker transport, pool admission, reverse proxy, or upstream
  response path instead.

## Verification

The repository's real binary integration test was run in an isolated container.
It generated a temporary local CA, redirected every SOCKS destination to a local
TLS server, and used synthetic tokens only.

```text
test/unit/oauth-binary.test.mjs
tests: 5
pass: 5
fail: 0
```

Covered variants:

- CAI code with provider state.
- Claude Code code with provider state.
- CAI code without a state suffix.
- Setup Token code with session state.
- sessionKey authorize rejection mapping.

## Tools in this directory

- `inspect-bytecode.cjs`: validates the extracted blob against a matching
  pkg-patched V8 runtime and reports the recovered wrapper.
- `run-bytecode.cjs`: executes the extracted CommonJS bytecode wrapper for
  isolated behavioral testing.
- `trace-preload.cjs`: logs request destinations and header names only; it does
  not log cookie values, Authorization values, bodies, or tokens.
