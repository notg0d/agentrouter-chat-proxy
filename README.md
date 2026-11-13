# AgentRouter Codex Responses Compatibility Proxy

A lightweight local compatibility proxy for **Codex** and **OpenCode** using **AgentRouter**, with cross-resource session recovery and bounded capacity retries.

---

## The Problem

When running Codex through AgentRouter with `wire_api = "responses"` (the OpenAI Responses API), multi-turn sessions frequently fail with:

```text
OpenAI Responses bad request: The requested item was created under a different **** OpenAI resource.
Use the same resource that created the item to access it. [trace_id=...]
```

### Why does this happen?
1. **Responses API is Stateful**: The OpenAI Responses API (`/v1/responses`) relies on server-stored conversation state (`previous_response_id`, server-minted item IDs, and encrypted reasoning content).
2. **Upstream Load Balancing**: AgentRouter load-balances traffic across multiple Azure OpenAI instances.
3. **Resource Isolation in Azure**: When Turn 1 runs on Azure Resource A, all response IDs and items are stored inside Resource A. When Turn 2 is routed by AgentRouter to Resource B, Resource B checks the IDs against its own store, fails to locate or decrypt them, and rejects the request with HTTP 400.

---

## How This Proxy Solves It

```
┌─────────────────┐       ┌───────────────────────────────┐       ┌───────────────────────┐
│  Codex Desktop  │  -->  │  agentrouter-codex-proxy      │  -->  │  AgentRouter Upstream │
│  or Codex CLI   │  <--  │  (127.0.0.1:8788)             │  <--  │  (https://...)        │
└─────────────────┘       └───────────────────────────────┘       └───────────────────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    │ 1. Transparent Fast-Path        │
                    │    Attempts request as-is to    │
                    │    preserve server KV cache.    │
                    │                                 │
                    │ 2. Smart Fallback on 400:       │
                    │    Detects resource mismatch,   │
                    │    sanitizes payload:           │
                    │    - Strips previous_response_id│
                    │    - Strips server item IDs     │
                    │    - Drops un-decryptable CoT   │
                    │                                 │
                    │ 3. Instant Recovery:            │
                    │    Retries immediately with     │
                    │    sanitized conversation.      │
                    │    ANY upstream instance can    │
                    │    process it without error!    │
                    └─────────────────────────────────┘
```

1. **Zero Latency on Normal Turns**: As long as requests hit the same backend, requests pass through unmodified with maximum performance and KV cache hits.
2. **Stateless Fallback**: When Azure OpenAI returns a 400 Resource Mismatch (or an early SSE `response.failed` mismatch), the proxy catches the error, sanitizes the supplied history while pruning excess historical screenshots past 35 images, and resubmits. Recovery does not depend on the proxy remembering yesterday's session.
3. **Pre-Commit SSE Priming & Rate-Limit Recovery**: Buffers initial `200 OK` `text/event-stream` setup frames long enough to catch early `rate_limit_exceeded` / `response.failed` / empty streams before committing headers to Codex, coordinates a shared cooldown + stagger across concurrent subagent calls, and honors `Retry-After` / reset hints before retrying.
4. **Windows & DNS Resilience**: Provides resilient DNS fallback (`8.214.160.125`, `8.214.161.192`), TCP keep-alive hooks, and a Python 3.12 Windows `ProactorEventLoop` `_call_connection_lost` guard (`WinError 10054`).
5. **Codex Keeps Streaming**: Codex receives clean `text/event-stream` tokens and `: ping` heartbeats with far fewer interruptions.

> [!IMPORTANT]
> Stateless recovery needs conversation history in the request. The proxy cannot reconstruct history supplied only as foreign `item_reference` IDs, a `previous_response_id`, or an encrypted native-compaction checkpoint, nor decrypt foreign-resource state. It does not read, modify, or delete your local sessions. Such cases require client-side full-history replay/local summaries or resource affinity from AgentRouter.

---

## Setup & Usage

### 1. Requirements
- Python 3.10+
- Dependencies: `fastapi`, `uvicorn`, `httpx` (installed automatically by `start-proxy.ps1`)

### 2. Quick Start (Interactive or Headless Background)
```powershell
cd D:\Projects\agentrouter-codex-proxy
.\start-proxy.ps1                    # interactive foreground
.\start-proxy.ps1 -Background        # headless background service (no console window)
.\start-proxy.ps1 -Update            # check/apply fast-forward git updates before starting
.\start-proxy.ps1 -BindHost 0.0.0.0  # bind non-loopback (Docker/LAN; restrict with firewall!)
```

Direct Python / Linux / Docker usage with optional `--host` and `--port`:
```bash
python proxy.py --host 0.0.0.0 --port 8788
# or via headless supervisor / uvicorn:
python service.py --host 0.0.0.0 --port 8788
uvicorn proxy:app --host 0.0.0.0 --port 8788
```
> [!IMPORTANT]
> By default the listener binds `127.0.0.1` (loopback only) and cannot be changed via `.env`. Only pass `--host 0.0.0.0` (or `-BindHost 0.0.0.0`) inside isolated Docker/Proxmox or firewalled LAN setups, and avoid setting a shared fallback API key if other machines can reach the port.

### 3. Autostart as a Headless Background Task (Recommended)
Registers a Windows Task Scheduler logon task running `service.py` via GUI-subsystem `pythonw.exe` (no console or Windows Terminal window opens) with automatic rotating logs (`logs/proxy.log`) and a safe fast-forward auto-updater (`updater.py`):
```powershell
cd D:\Projects\agentrouter-codex-proxy
.\install-autostart.ps1
```
To pass `--no-auto-update` or check/apply updates manually:
```powershell
.\install-autostart.ps1 -NoAutoUpdate
python updater.py --check
python updater.py --apply
```
To remove the background task:
```powershell
.\uninstall-autostart.ps1
```

---

## Configuring Codex

In `C:\Users\<username>\.codex\config.toml`:

```toml
model = "gpt-6-astra"
model_provider = "Agentrouter"
model_reasoning_effort = "high"

[model_providers.Agentrouter]
name = "Agentrouter"
base_url = "http://127.0.0.1:8788/v1"
wire_api = "responses"
env_key = "CODEX_GATEWAY_API_KEY"
```

> [!NOTE]
> Ensure your `CODEX_GATEWAY_API_KEY` (or `AGENTROUTER_API_KEY`) is configured in your environment or in `.codex/.env`.

---

## Configuring OpenCode (Responses or Chat Completions)

OpenCode's provider package determines which API it uses. The proxy on port **8789** supports **both**:

- `/v1/responses` and `/responses`: native Responses requests go through the same session sanitization, SSE inspection and retry handler as `proxy.py`.
- `/v1/chat/completions` and `/chat/completions`: chat history is converted to a stateless Responses request, then upstream output is translated back into chat chunks with text, reasoning and tool calls.

Keep your existing provider/model selection if already using `http://127.0.0.1:8789/v1`; switching to a different provider is not needed to resume a session.

```bash
python chat_proxy.py --host 127.0.0.1 --port 8789
# or: uvicorn chat_proxy:app --host 127.0.0.1 --port 8789
```

For a new custom provider, this **OpenCode V2** `opencode.json` example uses Chat Completions:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "providers": {
    "agentrouter": {
      "package": "@opencode/ai/providers/openai-compatible",
      "name": "AgentRouter",
      "env": ["CODEX_GATEWAY_API_KEY"],
      "settings": {
        "baseURL": "http://127.0.0.1:8789/v1",
        "apiKey": "{env:CODEX_GATEWAY_API_KEY}"
      },
      "models": {
        "gpt-6.1-sol": { "name": "gpt-6.1-sol" }
      }
    }
  }
}
```

For native Responses, use `@opencode/ai/providers/openai-compatible/responses` instead. The same base URL applies. This proxy supports HTTP/SSE, not Responses WebSockets.

> [!NOTE]
> Both proxies can run at the same time: Codex can keep using `8788` (`wire_api = "responses"`), and OpenCode can use `8789` with either API. Shared cooldown state is in-memory and per Python process, not shared between separate proxies/workers.

### Resumed-session errors on an older proxy

Version **1.0.0** of `chat_proxy.py` handled only Chat Completions. Native `/v1/responses` requests fell into generic passthrough, bypassing recovery entirely. This explains upstream 400 logs without `[Resource Mismatch]` / `[Recovery]` messages and a `chat_requests` counter of zero.

Stop that proxy process (Ctrl+C in its terminal), then start the updated code:

```powershell
python chat_proxy.py --host 127.0.0.1 --port 8789
Invoke-RestMethod http://127.0.0.1:8789/_health
```

Health should show version **1.1.0**, `supported_apis` containing `responses`, and a `responses_requests` counter that increases when resuming your existing session. Detailed native recovery counters are under `responses_stats`. Re-running the launcher without stopping the old process does not update code already loaded in memory.

### Token rate limits and timeouts under high traffic

`rate_limit_exceeded ... eastus2 ... token rate limit` is an **upstream Azure token-rate limit**, not a session-state error. Long history, requested output budgets and concurrent agents can all contribute.

- HTTP 429 and early SSE/JSON rate-limit errors use a shared cooldown and staggered retries. Provider `Retry-After`, `retry-after-ms`, reset headers and retry-delay text are honored in full, even above the 15-second generated-backoff cap.
- The default recovery budget is **90 seconds**, with at most **6 retries**. A delay longer than the remaining budget returns 429 with `Retry-After`, without retrying prematurely. An already-in-flight success cannot cancel another request's active cooldown.
- Connection/read/pool timeouts and HTTP 408/500/502/503/504 use the same bounded retry loop, not nested groups of five attempts. Connection pools are released after failed body reads.
- Once output has been streamed, the proxy does **not** replay partial text or tool calls. Interrupted streams report `upstream_stream_interrupted`, rather than falsely reporting successful completion. Late errors after stream commitment are forwarded to the client.
- `PROXY_MAX_RATE_LIMIT_WAIT_S` bounds recovery/waiting; `PROXY_CONNECT_TIMEOUT_S` and `PROXY_READ_TIMEOUT_S` separately control transport timeouts. Heartbeats keep the downstream connection active after commitment; they cannot force a busy upstream to respond. OpenCode's client timeout must also allow the chosen wait budget.

The proxy cannot increase Azure TPM, select a different hidden Azure region/resource, or guarantee recovery during sustained outages. For persistent limits, reduce concurrent requests, compact long sessions using client-side summaries, reduce requested output/reasoning budgets, or ask AgentRouter for more capacity/an alternative upstream.

---

## Health, Diagnostics & Tests

- **Health check**: `http://127.0.0.1:8788/_health`
- **OpenCode proxy health**: `http://127.0.0.1:8789/_health` (stats at `/_stats`)
- **Proxy stats**: `http://127.0.0.1:8788/_stats`
  - Returns counts for total requests, mismatches caught, fallbacks applied, rate limits caught, pre-commit stream retries, and successful recoveries.
- **Logs**: When running as a service (`-Service` or via scheduled task), logs are stored in `logs/proxy.log` (automatically rotates at 1MB).
- **Tests**:
  ```powershell
  .\.venv\Scripts\python.exe -m unittest discover -s tests -v
  ```
