# Transport reliability

The proxy can recover from some temporary upstream failures. It cannot guarantee network availability or exactly-once model execution.

## Recovery rules

- Responses and translated Chat requests use one outer retry budget. The transport helper sends one attempt per outer iteration.
- Generic API forwarding permits two transport attempts, including the initial attempt.
- Retries preserve the existing timeout, retry-count, and recovery-budget settings.
- Known authentication, billing, quota, invalid-input, and content-policy errors do not receive automatic retries.
- Read and write failures can occur after AgentRouter accepts work. Automatic retries can produce duplicate work or additional charges.
- `PROXY_RETRY_AMBIGUOUS_ERRORS=0` disables retries for ambiguous transport errors, empty streams, and initial SSE timeouts.
- `PROXY_RETRY_ON_5XX=0` disables retries for upstream server errors.
- Initial Server-Sent Events (SSE) use a bounded inspection buffer before the proxy sends response headers downstream.
- After downstream response headers, the proxy does not replay the request, partial output, or tool calls.

These options do not provide an exactly-once guarantee. TCP keep-alive and downstream SSE heartbeats cannot repair an upstream outage.

## Disconnects and connection ownership

The proxy watches downstream disconnects during connection setup, header waits, buffered body reads, and retry delays.
A downstream disconnect cancels pending upstream work. It does not count as a transport failure that needs another attempt.
The application returns status `499` internally. A disconnected client usually cannot receive that response.
Cancellation does not prove that AgentRouter stopped generation or cancelled billing.

Each streaming HTTP response owns its upstream response and producer task.
Cleanup closes the iterator, cancels and joins the producer, and closes the upstream response.
Cleanup also runs if downstream transmission fails before the first output byte or while the iterator is suspended.
Cleanup uses a cancellation shield so a cancelled application scope cannot skip connection release.
A cleanup exception does not replace the original failure or trigger a retry of completed work.

The proxy does not close the entire shared client after an individual transport failure.
HTTPcore already removes failed, closed, and expired connections.
A global client reset can interrupt unrelated streams. A failure on a new connection also does not establish stale-pool trouble.

## Error diagnostics

An attempt log can include:

```text
ReadError (caused by BrokenResourceError -> ConnectionResetError: [Errno 10054] Connection reset by peer) (phase=response_headers elapsed=42.30s connection=new)
```

This is an example, not proof that every read failure has this cause.

| Field | Meaning |
| --- | --- |
| `ConnectError` | TCP connection or TLS setup failed. The cause can identify DNS, socket, or certificate trouble. |
| `ReadError` | A network read failed. This is different from a read timeout. |
| `phase` | The latest relevant HTTPcore operation, such as `connect_tcp`, `tls_handshake`, `response_headers`, or `response_body`. |
| `elapsed` | Time since the attempt started, not time in the reported phase alone. |
| `connection=new` | The trace recorded TCP or TLS setup for this attempt. |
| `connection=reused` | An HTTP operation started without a connection-setup trace. |
| `http_status` | The upstream status, when response headers arrived. |

HTTPcore can clear an explicit exception cause when it re-raises an error during pool or stream cleanup.
The proxy captures explicit causes from supported trace `.failed` events before that cleanup.
It stores bounded, redacted strings on the affected exception. It does not retain trace headers, bodies, or additional exception objects.
These strings remain available after HTTPX wraps the error and during later stream reads.
An unavailable cause remains unavailable. The proxy does not infer one from an exception name.

Several retry lines can belong to concurrent requests. Each request has its own counter.
`[Success] Request recovered` means the proxy obtained a usable response. It does not prove the stream or client task completed.

## Source comparison

The comparison uses fixed source revisions inspected on November 14–15, 2026.
The table describes relevant implementation behavior, not every feature or deployment of each project.

| Source | Relevant behavior | Application here |
| --- | --- | --- |
| [LiteLLM HTTP handler](https://github.com/BerriAI/litellm/blob/ec48e2491c5132951cd2710671506019ce93f0a0/litellm/llms/custom_httpx/http_handler.py#L996-L1050) | Some `ConnectError` paths retry with a new client. The shown path closes that client in `finally`. | Do not copy this stream lifetime into the proxy. Keep a streamed response open until its consumer finishes. |
| [CLIProxyAPI Codex stream](https://github.com/router-for-me/CLIProxyAPI/blob/bd9c766f34840b050b01cbdafad7cf29b3cb151f/internal/runtime/executor/codex_executor_stream.go#L164-L229) | Optional bootstrap inspection has time, frame, and byte limits. [Cancellation guards](https://github.com/router-for-me/CLIProxyAPI/blob/bd9c766f34840b050b01cbdafad7cf29b3cb151f/internal/runtime/executor/codex_executor_stream.go#L387-L400) separate downstream cancellation from upstream failure. | Retain bounded initial inspection. Cancel abandoned operations without another upstream attempt. |
| [CLIProxyAPI transport cache](https://github.com/router-for-me/CLIProxyAPI/blob/bd9c766f34840b050b01cbdafad7cf29b3cb151f/internal/runtime/executor/helps/transport_cache.go#L155-L159) | Cache removal closes idle connections without closing active requests. | Do not reset the whole shared client to address one failure. |
| [New API HTTP client](https://github.com/QuantumNous/new-api/blob/b388140acfd80369ec6243b16869daff1b4051f4/service/http_client.go#L79-L125) | Configurable connection limits, idle expiry, and response-header timeout bound transport use. | Existing HTTPX connect, read, write, and pool timeouts remain in effect. Do not increase timeouts as a proposed cure for `ReadError`. |
| [Portkey retry handler](https://github.com/Portkey-AI/gateway/blob/669825cbe89ee51569918b8f78a9db486fd69dd4/src/handlers/retryHandler.ts) | Uses bounded retries and a timeout-driven abort controller. | Preserve bounded retries and release abandoned upstream operations. Do not add another retry layer. |
| [OpenAI Python client](https://github.com/openai/openai-python/blob/3dae6090cc31cc64a437fe0bcee740b6d81ed2cc/src/openai/_base_client.py) | Uses retry classification, backoff, jitter, and retry-delay hints. | Retain existing permanent-error classification and provider delay hints. |
| [HTTPX transport documentation](https://github.com/encode/httpx/blob/b5addb64f0161ff6bfe94c124ef76f6a1fba5254/docs/advanced/transports.md) | Built-in transport retries cover connection errors and connection timeouts, not general read failures. | The application recovery loop owns read-failure policy and the total retry count. |
| [HTTPcore pool cleanup](https://github.com/encode/httpcore/blob/10a658221deb38a4c5b16db55ab554b0bf731707/httpcore/_async/connection_pool.py#L248-L299) and [trace events](https://github.com/encode/httpcore/blob/10a658221deb38a4c5b16db55ab554b0bf731707/httpcore/_trace.py#L95-L107) | Pool cleanup uses `raise exc from None`. Trace failure events expose the error earlier. Stream cleanup has the same cause-loss mechanism. | Capture safe cause strings before cleanup. Use the supported trace extension, not an HTTPcore patch. |

Static IP changes, disabled TLS verification, unlimited retries, and global client resets are not general remedies.
Endpoint failover needs evidence that an endpoint is consistently unavailable. A successful small request does not exclude intermittent failures.

## Tests

Run all tests from the repository root:

```powershell
python -m unittest discover -s tests -v
```

Run transport fault tests only:

```powershell
python -m unittest discover -s tests -p test_transport_reliability.py -v
```

These tests use mocked transports and temporary loopback servers. They do not send model requests or require upstream credentials.
Fault cases include cause suppression, truncated bodies, stalled header waits, cancellation races, downstream transmission failures, and concurrent streams.
Existing recovery tests cover retry limits, permanent errors, and the no-replay rule after partial output or tool activity.

## Activation

After an update, wait for active requests to finish before you stop and restart the proxy.
The launcher does not replace an already healthy worker. Starting it again does not automatically load edited Python files.
Keep TLS verification enabled. Do not include API keys or request bodies in diagnostic reports.
