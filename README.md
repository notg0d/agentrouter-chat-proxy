# AgentRouter Compatibility Proxy

This local proxy connects **OpenCode** and **Codex** to **AgentRouter**.
It converts Chat Completions requests to Responses requests.
It uses supplied conversation history for Azure session recovery.
It can send requests again after temporary upstream errors.

## API routes

OpenCode and Codex are clients. AgentRouter is the upstream server.

| Program | Default port | Client endpoints |
| --- | --- | --- |
| `chat_proxy.py` | `8789` | `/v1/responses`, `/responses`, `/v1/chat/completions`, `/chat/completions` |
| `proxy.py` | `8788` | `/v1/responses`, `/responses` |

Both programs send model requests to `/v1/responses` on the upstream server.
The default upstream URL is `https://agentrouter.org`.
The proxy sends requests for other API routes without session recovery or API conversion.

The proxy uses HTTP and server-sent events (SSE).
It does not use Responses WebSockets.

### Chat Completions conversion

The proxy converts supplied messages to a Responses request without server-state references.
It then converts Responses output to Chat Completions output.

- The input conversion includes text, images, function calls, and function outputs.
- The output conversion includes text, reasoning text, and function calls.
- Stream chunks use `reasoning_content` and `tool_calls` when applicable.
- The conversion includes function tools, tool selection, output token limits, and JSON output formats.

**A Chat Completions endpoint on the upstream server is not necessary.**
This conversion does not include all OpenAI API features.

## Installation

Use **Python 3.10 or a higher version**.
These instructions use PowerShell.

1. Get the source files.

   ```powershell
   git clone https://github.com/notg0d/agentrouter-chat-proxy.git
   ```

2. Go to the repository directory.

   ```powershell
   cd agentrouter-chat-proxy
   ```

3. Make a Python virtual environment.

   ```powershell
   python -m venv .venv
   ```

4. Activate the virtual environment.

   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```

5. Install the packages.

   ```powershell
   python -m pip install -r requirements.txt
   ```

6. Start the proxy for both client APIs.

   ```powershell
   python chat_proxy.py
   ```

On macOS or Linux, use this command for step 4:

```bash
source .venv/bin/activate
```

To use only Responses, start `proxy.py` as an alternative.
Both programs can operate at the same time in different terminals.
Use `--host` to set the listen address.
Use `--port` to set the port.

**CAUTION:** Keep the listen address at `127.0.0.1` unless a firewall controls access.
Other users can send requests with your fallback API key if they can connect to the proxy.
Do not make this proxy available as a public service.

## API key

Set `CODEX_GATEWAY_API_KEY` in the client environment.

```powershell
$env:CODEX_GATEWAY_API_KEY = "your-agentrouter-api-key"
```

The proxy sends the client `Authorization` header to the upstream server.
Without that header, the proxy uses a fallback key from its environment or `.env`.
It tries `CODEX_GATEWAY_API_KEY` before `AGENTROUTER_API_KEY`.
The proxy loads `.env` values only for missing environment variables.

## Client configuration

The examples use `gpt-6.1-sol`.
Select a model available to your AgentRouter account.

### OpenCode V2

1. Add this provider to `opencode.json`.

   ```json
   {
     "$schema": "https://opencode.ai/config.json",
     "providers": {
       "agentrouter": {
         "package": "@opencode/ai/providers/openai-compatible/responses",
         "env": ["CODEX_GATEWAY_API_KEY"],
         "settings": {
           "baseURL": "http://127.0.0.1:8789/v1",
           "apiKey": "{env:CODEX_GATEWAY_API_KEY}",
           "transport": "http"
         },
         "models": {
           "gpt-6.1-sol": { "name": "gpt-6.1-sol" }
         }
       }
     }
   }
   ```

2. Select `agentrouter/gpt-6.1-sol` in OpenCode.

For Chat Completions, replace the package value with `@opencode/ai/providers/openai-compatible`.
Keep the same `baseURL`.

If your client uses this proxy, keep the same provider ID and model ID.
A different provider is not necessary for session recovery.

### Codex

Use this configuration in `~/.codex/config.toml`:

```toml
model = "gpt-6.1-sol"
model_provider = "agentrouter"

[model_providers.agentrouter]
name = "AgentRouter"
base_url = "http://127.0.0.1:8788/v1"
wire_api = "responses"
env_key = "CODEX_GATEWAY_API_KEY"
```

This configuration uses `proxy.py` on port `8788`.
Codex can also use the Responses endpoint on port `8789`.

## Session recovery

Responses requests can contain resource-specific state, such as response IDs, item IDs, and encrypted reasoning.
If a subsequent request uses a different resource, the upstream server can send this error:

```text
The requested item was created under a different Azure OpenAI resource.
Use the same resource that created the item to access it.
```

The Responses handler applies the image limit before it sends the supplied request.
After it identifies a cross-resource error, the handler:

- Removes response references, such as `previous_response_id`.
- Removes server item IDs, reasoning items, and `item_reference` items.
- Removes encrypted content and summary content from input parts.
- Sends the remaining conversation history again.

It keeps function `call_id` values to connect function calls with their outputs.

**The request must contain conversation history.**
The proxy cannot get conversation history from IDs without the message content.
It cannot decrypt a compaction checkpoint from a different Azure resource.

Session recovery uses the supplied history, not a record of previous requests.
The proxy does not read, change, or delete local session files.

## Rate limits and timeouts

An Azure token rate limit is not a session error.
The proxy can wait and send the request again.
**It cannot increase Azure token capacity.**
It cannot select the Azure region or resource that AgentRouter uses.

For HTTP `429` and initial SSE or JSON rate-limit errors, the proxy uses a shared retry delay.
It uses upstream delay information from `Retry-After`, `retry-after-ms`, reset headers, and error text.
It does not decrease a delay that the upstream server gives.

The proxy also sends requests again after connection, read, or connection-pool timeouts.
It uses the same recovery limits for HTTP `408`, `500`, `502`, `503`, and `504`.
Connection pooling, TCP keep-alive, and DNS fallback help with transport errors.

Each Python process keeps retry-delay state in its memory.
Requests in different processes do not use the same retry-delay state.

If `rate_limit_exceeded` errors continue:

1. Decrease the number of concurrent requests.
2. Use a shorter conversation history or a client-side summary.
3. Decrease the output token limit or reasoning effort.
4. Write to AgentRouter for more token capacity.

The API conversion does not remove token rate limits.

### Interrupted streams

Before it sends HTTP `200` to the client, the proxy examines initial SSE events.
This lets the proxy send the request again after some initial errors.

After it sends HTTP headers to the client, the proxy does not send the request again.
It does not send partial text or function calls again.
It sends subsequent upstream errors to the client.
If the stream ends without a terminal event, the proxy sends `upstream_stream_interrupted`.

SSE heartbeats help keep the client connection active.
They cannot make the upstream server supply output.

## Proxy configuration

1. Copy `.env.example` to `.env`.

   ```powershell
   Copy-Item .env.example .env
   ```

2. Change the applicable values in `.env`.
3. Stop the proxy process.
4. Start the proxy process again.

**CAUTION:** Do not add `.env` or API keys to Git.
Other users can read keys in repository files.

| Variable | Default value | Function |
| --- | --- | --- |
| `AGENTROUTER_UPSTREAM` | `https://agentrouter.org` | Upstream server URL |
| `CHAT_PROXY_PORT` | `8789` | Port for `chat_proxy.py` |
| `PROXY_PORT` | `8788` | Port for `proxy.py` |
| `PROXY_MAX_RATE_LIMIT_WAIT_S` | `90` | Recovery budget in seconds |
| `PROXY_MAX_TRANSIENT_RETRIES` | `6` | Maximum transient retries, not the initial request |
| `PROXY_RATE_LIMIT_BASE_S` | `2` | Base value for calculated rate-limit delays in seconds |
| `PROXY_RATE_LIMIT_MAX_DELAY_S` | `15` | Maximum calculated retry delay in seconds, not an upstream-delay limit |
| `PROXY_RECOVERY_STAGGER_S` | `0.6` | Interval between concurrent recovery requests in seconds |
| `PROXY_CONNECT_TIMEOUT_S` | `20` | Upstream connection timeout in seconds |
| `PROXY_READ_TIMEOUT_S` | `600` | Upstream read timeout in seconds |
| `PROXY_SSE_PRIME_FIRST_TIMEOUT_S` | `45` | Initial SSE wait timeout in seconds |
| `PROXY_SSE_PRIME_CREATED_GRACE_S` | `3` | Initial inspection period after an SSE setup event, in seconds |
| `PROXY_MAX_IMAGES` | `35` | Maximum images in a Responses request |

The recovery budget is the time available for retries and shared retry delays.
It is not a total request timeout.
Connection, read, and client timeouts have different limits.
If the upstream delay is longer than the remaining recovery budget, the proxy sends HTTP `429` with `Retry-After`.
Make sure that the client timeout is longer than the recovery delay.

The Responses handler keeps the newest images up to the image limit.
It replaces older images with text placeholders.
The translated Chat Completions route does not apply this image limit.

The listen address is not an `.env` setting.
The `--host` option controls the listen address.
The `--port` option overrides the port environment variable.
The `.env.example` file includes log and automatic-update options.

## Windows background service

The Windows service uses **`proxy.py`**, not `chat_proxy.py`.
The default port is `8788`.

| Command | Function |
| --- | --- |
| `.\start-proxy.ps1 -Background` | Start a background process without a console window |
| `.\install-autostart.ps1` | Start at user logon with automatic updates |
| `.\install-autostart.ps1 -NoAutoUpdate` | Start at user logon without automatic updates |
| `.\uninstall-autostart.ps1` | Remove the user-logon task |
| `python updater.py --check` | Do a check for Git updates |
| `python updater.py --apply` | Apply available fast-forward Git updates |

The service writes logs to `logs/proxy.log` with automatic log rotation.
The console shows foreground logs.

## Diagnostics

1. Get the health data.

   ```powershell
   Invoke-RestMethod http://127.0.0.1:8789/_health
   ```

2. Get the request counters.

   ```powershell
   Invoke-RestMethod http://127.0.0.1:8789/_stats
   ```

For port `8788`, replace `8789` in these commands.

The health data on port `8789` include these fields:

| Field | Meaning |
| --- | --- |
| `version` | Program version: `1.1.0` |
| `supported_apis` | `chat.completions` and `responses` |
| `stats.responses_requests` | Number of native Responses requests |
| `stats.chat_requests` | Number of translated Chat Completions requests |
| `responses_stats` | Counters from the Responses recovery handler |

Version `1.0.0` of `chat_proxy.py` did not use session recovery for native Responses requests.
After you update the code, stop the proxy process.
Start the updated program again.
Make sure that `/_health` shows version `1.1.0` and `responses` in `supported_apis`.

## Tests

Do the automated tests:

```powershell
python -m unittest discover -s tests -v
```

The tests use simulated upstream responses.
An API key is not necessary for these tests.
