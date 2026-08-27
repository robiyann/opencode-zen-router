# Repository Guidelines

## Project Overview
`opencode-zen-router` is a high-concurrency, self-contained AI gateway and reverse proxy written in Go (Go 1.25.0). It routes OpenAI-compatible chat completion (`/v1/chat/completions`) and model discovery (`/v1/models`) requests across a dynamic pool of Vercel Edge relay nodes forwarding to OpenCode Zen AI (`https://opencode.ai/zen/v1`).

The service is distributed as a single standalone executable with zero CGO dependencies and includes an embedded single-page monitoring dashboard (`/dashboard`), SQLite database with Write-Ahead Logging (WAL), Bcrypt-authenticated admin sessions, API key lifecycle management (`sk-zen-...`), and real-time Server-Sent Events (SSE) telemetry.

---

## Architecture & Data Flow

### High-Level Architecture
```
                         +-----------------------------------+
                         |      AI Clients / IDE Agents      |
                         | (Cursor, Cline, Roo Code, Python) |
                         +-----------------------------------+
                                           |
                                           | Authorization: Bearer sk-zen-...
                                           v
+-------------------------------------------------------------------------------------------------+
|                                 OpenCode Zen Go Router (:8080)                                  |
|                                                                                                 |
|  [Security & Rate Limiting]                                                                     |
|   - HTTP Security Headers (X-Frame-Options: DENY, X-Content-Type-Options: nosniff)              |
|   - In-memory LoginLimiter (IP-based lockout: 5 failures -> 5-min ban)                          |
|   - Session validation via SQLite (`sessions` table)                                            |
|   - API Key validation & token usage tracking (`api_keys` table)                                |
|                                                                                                 |
|  [Routing & Reverse Proxy Engine]                                                               |
|   - ProxyPool: Round-Robin, Random, or Sticky-Session distribution                              |
|   - Circuit Breaker: 45s cooldown on 429/5xx/timeout; up to 3 failover retries                  |
|   - In-flight request counting & real-time latency measurement                                  |
|                                                                                                 |
|  [Real-Time Event Hub (SSE)]                                                                    |
|   - EventHub: Broadcasts `request_start`, `request_done`, `proxy_cooldown` to `/api/events`     |
|                                                                                                 |
|  [Persistence Engine (modernc.org/sqlite in WAL mode)]                                          |
|   - Tables: `proxies`, `api_keys`, `sessions`, `usage_logs`, `settings`                         |
+-------------------------------------------------------------------------------------------------+
                                           |
                                           | Forwards OpenAI payload with Bearer public
                                           v
                         +-----------------------------------+
                         |     Vercel Edge Relay Network     |
                         |  (20+ serverless edge instances)  |
                         +-----------------------------------+
                                           |
                                           v
                         +-----------------------------------+
                         |      Target Upstream Gateway      |
                         |    https://opencode.ai/zen/v1     |
                         +-----------------------------------+
```

### Data Flow
1. **Ingress**: Client sends an OpenAI-compatible request to `/v1/chat/completions` or `/v1/models` with `Authorization: Bearer sk-zen-...`.
2. **Authentication**: `Database.ValidateAPIKey()` checks the database for active key status. Rejects invalid requests with HTTP `401 Unauthorized`.
3. **Node Selection**: `ProxyPool.Pick(sessionID)` selects an available `ProxyNode` based on the configured strategy (`round-robin`, `random`, or `sticky`).
4. **Relay Forwarding**: Request body is buffered and piped to the chosen node. Upstream authentication (`Authorization: Bearer public`, `x-opencode-client: desktop`) is attached.
5. **Failover & Circuit Breaking**: If upstream returns `429`, `401`, `>=500`, or times out, the node enters cooldown (`45s`), triggers an SSE `proxy_cooldown` event, and the request fails over to the next healthy node (up to 3 retries).
6. **Streaming & Accounting**: For `text/event-stream` SSE streaming, data is flushed chunk-by-chunk. Total tokens, prompt tokens, completion tokens, latency, and status are recorded asynchronously in `usage_logs` via `db.LogUsage()`.

---

## Key Directories

```
.
├── web/                    # Frontend UI assets compiled into binary via //go:embed
│   └── dashboard.html      # Single-page responsive monitoring & management interface
├── router.db               # SQLite database file (created on runtime startup)
├── router.db-wal           # SQLite Write-Ahead Log (WAL)
├── router.db-shm           # SQLite shared-memory index
```

---

## Important Files

| File | Purpose | Key Symbols / Responsibilities |
|---|---|---|
| `main.go` | Core entry point & HTTP router | `ProxyPool`, `ProxyNode`, `EventHub`, `LoginLimiter`, `proxyHandler`, `dashboardHTML` (`//go:embed web/dashboard.html`) |
| `db.go` | Pure-Go SQLite persistence layer | `Database`, `InitDB`, `DBProxy`, `APIKey`, `UsageLog`, `GlobalStats`, Bcrypt password hashing |
| `go.mod` / `go.sum` | Go module definitions | Declares Go `1.25.0` toolchain and dependencies (`modernc.org/sqlite`, `golang.org/x/crypto`) |
| `web/dashboard.html` | Embedded admin dashboard | Real-time SSE metric charts, proxy CRUD, API key generator, Vercel batch deployment UI |
| `batch-deploy-20.js` | Edge deployment automation | Standalone Node.js/Bun script deploying 20 randomized Vercel Edge relays and registering into `router.db` |
| `security-audit.js` | Automated penetration suite | Node.js integration testing: admin auth barriers, security headers, SQLi rejection, brute-force rate limits |
| `proxies.txt` | Seed relay list | Reference list of Vercel Edge proxy URLs (supports `#` comments) |
| `build-linux.bat` | Linux cross-compilation | Builds stripped Linux amd64 binary (`opencode-router-linux`) without CGO dependencies |
| `build-windows.bat` | Windows compilation | Builds stripped Windows binary (`opencode-router.exe`) |
| `start-router.bat` | Local launcher | Executes `opencode-router.exe` on Windows |
| `DEPLOY-UBUNTU.md` | Server deployment guide | Systemd unit setup (`opencode-router.service`) for Ubuntu/Debian production environments |

---

## Development Commands

### Building

```bash
# Build for current OS (local development)
go build -o opencode-router.exe .

# Production Windows build (stripped debug info & symbol tables)
go build -ldflags="-s -w" -o opencode-router.exe .

# Cross-compile for Linux (amd64) from Windows/macOS (Zero CGO required)
# Windows cmd/bat:
set GOOS=linux&& set GOARCH=amd64&& go build -ldflags="-s -w" -o opencode-router-linux .
# Linux/macOS bash:
GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o opencode-router-linux .
```

### Running

```bash
# Direct Go run (default port 8080, db router.db)
go run .

# Run with custom port or host
PORT=9000 HOST=0.0.0.0 go run .

# Windows batch script
start-router.bat
```

### Testing & QA

```bash
# 1. Start the router in background / separate terminal
go run .

# 2. Run automated security audit and penetration test suite (Node.js / Bun)
node security-audit.js
# or: bun security-audit.js

# 3. Standard Go tests (when adding unit tests)
go test -v ./...
go test -race ./...
```

---

## Code Conventions & Common Patterns

### 1. Zero-CGO SQLite Database Layer
- **Driver**: Uses `modernc.org/sqlite`, a pure Go transpilation of SQLite. Never introduce packages requiring `CGO_ENABLED=1` (like `mattn/go-sqlite3`) to preserve instant cross-compilation.
- **WAL Pragma**: Always execute `PRAGMA journal_mode=WAL;` on database connection initialization.
- **Parameterized SQL**: All database queries must use positional placeholders (`?`) to prevent SQL injection vulnerabilities.

### 2. Concurrency & State Management
- **In-Memory Thread Safety**: Shared structures (`ProxyPool`, `ProxyNode`, `EventHub`, `LoginLimiter`) use `sync.RWMutex` or `sync.Mutex` with deferred unlocks (`defer mu.Unlock()`).
- **Atomic Counters**: Use `sync/atomic` (`atomic.Uint64`, `atomic.Int64`) for high-frequency counters (e.g. `counter`, `inFlight`, `totalRouted`, `totalRetries`).
- **SSE Non-blocking Dispatch**: When broadcasting events across connected client channels, use non-blocking `select` with `default:` to prevent slow or dropped dashboard clients from blocking the proxy engine:
  ```go
  select {
  case ch <- msg:
  default:
  }
  ```

### 3. Routing & Circuit Breaker Logic
- **Failover Loop**: `proxyHandler` retries up to `MaxRetries = 3` across healthy nodes.
- **Dynamic Cooldown**: Set `node.SetCooldown(CooldownPeriod)` (45 seconds) whenever an upstream node returns HTTP 429, 401, >=500, or a network dial timeout.
- **Request Cloning**: Because HTTP request bodies can only be read once, read into a byte slice (`io.ReadAll(r.Body)`) and reconstruct `io.NopCloser(bytes.NewReader(bodyBytes))` before each proxy attempt.

### 4. API Key & Security Patterns
- **API Key Format**: Keys are prefixed with `sk-zen-` followed by 32 hex characters (e.g. `sk-zen-4a9b2c...`).
- **Key Masking**: In API responses, keys are masked to show only prefix and suffix: `sk-zen-...4a9b`.
- **Admin Password**: Hashed using Bcrypt (`golang.org/x/crypto/bcrypt`) with `bcrypt.DefaultCost`. Default password on fresh database is `admin123`.
- **Security Headers**: Admin and API handlers must enforce `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, and `X-XSS-Protection: 1; mode=block`.

---

## Runtime & Tooling Preferences

- **Go Toolchain**: Go `1.25.0` or newer.
- **CGO**: `CGO_ENABLED=0` (pure Go compilation). No MinGW, GCC, or Clang required.
- **Script Runtime**: Node.js (`v18+`) or Bun for running operational scripts (`batch-deploy-20.js`, `security-audit.js`). Scripts use standard `fetch` with zero `node_modules` dependencies.
- **Package Manager**: Go Modules (`go mod tidy`, `go mod download`).

---

## Testing & QA

### Existing QA Infrastructure
- **Security Penetration Suite (`security-audit.js`)**:
  - Validates unauthorized blocks (HTTP 401) on `/api/keys`, `/api/proxies`, `/api/strategy`, `/api/logs`, `/api/deploy-vercel`.
  - Verifies HTTP security headers.
  - Tests `/v1/chat/completions` key verification and SQL injection payloads (`' OR '1'='1' --`, `UNION SELECT`, `DROP TABLE`).
  - Tests `LoginLimiter` brute-force lockout (triggers HTTP 429 after 5 failed attempts).

### Unit Testing Guidelines for New Code
When writing Go unit tests (`*_test.go`):
- **Database Unit Tests (`db_test.go`)**: Use an in-memory SQLite connection (`InitDB(":memory:")`) or `t.TempDir()` for complete test isolation.
- **HTTP Reverse Proxy Tests (`main_test.go`)**: Use `net/http/httptest` (`httptest.NewServer` and `httptest.NewRecorder`) to simulate mock upstream Vercel Edge relays and test timeout, retry, and SSE streaming mechanisms.
