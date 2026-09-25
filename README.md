# VoidDB

A lightweight, high-performance in-memory database written in Go with persistent transaction logging, TTL support, priority-based eviction, and a modern RESTful API.

## Features

- **In-Memory Storage**: Ultra-fast key-value storage with thread-safe `RWMutex` protection
- **Persistent Transaction Log (WAL)**: Automatic recovery on startup with atomic log compaction (`/compact`)
- **TTL & Dual Expiration**: Background janitor cleanup combined with lazy expiration on read to never serve stale data
- **Priority System & Capacity Eviction**: Store items with priority levels; automatically evict lowest-priority items when capacity (`-max-keys`) is reached
- **Modern RESTful API**: Idiomatic Go 1.22+ endpoints (`/keys/{key}`) with strict HTTP method matching and backward-compatible legacy routes
- **Server Metrics**: Built-in `/stats` endpoint for key counts, uptime, and operation counters
- **Graceful Shutdown**: Intercepts `SIGINT`/`SIGTERM` to safely flush WAL files and stop background workers

## Quick Start

### Prerequisites

- Go 1.22 or higher

### Installation

```bash
git clone https://github.com/MrWaifu24/vdb.git
cd vdb
go mod tidy
```

### Running the Server

```bash
go run main.go
```

Configurable command-line flags:
```bash
go run main.go -port 6969 -log transaction.log -max-keys 10000 -cleanup 1s
```

| Flag | Default | Description |
| :--- | :--- | :--- |
| `-port` | `6969` | HTTP server listening port |
| `-log` | `transaction.log` | Path to persistent transaction log |
| `-max-keys` | `0` | Max in-memory keys (0 for unlimited; lowest priority evicted when full) |
| `-cleanup` | `1s` | Background janitor cleanup interval |

### Running Tests

```bash
go test -v -race ./...
```

---

## API Endpoints

### Modern REST API

#### `GET /keys/{key}`
Retrieve a value and its metadata by key.

**Query Parameters (optional):**
- `raw=true`: Returns only the stored value directly instead of the metadata wrapper.

**Example:**
```bash
curl "http://localhost:6969/keys/mykey"
```
```json
{
  "key": "mykey",
  "value": "myvalue",
  "priority": 1,
  "ttl": 285,
  "expiration": 1751320077508959000
}
```

#### `POST /keys` or `PUT /keys/{key}`
Store a key-value pair with optional TTL and priority.

**Request Body:**
```json
{
  "key": "mykey",
  "value": "myvalue",
  "ttl": 300,
  "priority": 1
}
```

**Fields:**
- `key` (string, required): Key to store (can be provided in URL for `PUT /keys/{key}`)
- `value` (any, required): Value to store (string, number, array, or JSON object)
- `ttl` (int64, optional): Time-to-live in seconds (`0` or omitted for no expiration)
- `priority` (int, optional): Priority level for capacity eviction (higher numbers retained longer)

**Example:**
```bash
curl -X POST http://localhost:6969/keys \
  -H "Content-Type: application/json" \
  -d '{"key":"mykey","value":"myvalue","ttl":300,"priority":1}'
```

#### `DELETE /keys/{key}`
Remove a key-value pair. Returns `200 OK` on success, or `404 Not Found` if the key does not exist.

**Example:**
```bash
curl -X DELETE "http://localhost:6969/keys/mykey"
```

#### `GET /keys`
List active, non-expired keys.

**Query Parameters (optional):**
- `prefix`: Filter keys by prefix (e.g. `?prefix=user:`)
- `min_priority`: Filter keys with priority greater than or equal to this value

**Example:**
```bash
curl "http://localhost:6969/keys?prefix=user:&min_priority=1"
```
```json
{
  "keys": ["user:1", "user:2"],
  "count": 2
}
```

#### `GET /stats`
Inspect real-time server metrics.

**Example:**
```bash
curl "http://localhost:6969/stats"
```
```json
{
  "keys": 42,
  "max_keys": 10000,
  "uptime_seconds": 360,
  "total_reads": 1500,
  "total_writes": 300,
  "total_deletes": 50,
  "expired_cleaned": 12
}
```

#### `POST /compact`
Trigger an online compaction of the transaction log. Active unexpired keys are rewritten to a clean log file atomically.

**Example:**
```bash
curl -X POST "http://localhost:6969/compact"
```
```json
{
  "status": "compacted",
  "keys_retained": 42
}
```

---

### Legacy Endpoints (Backward Compatible)

VoidDB maintains backward compatibility with earlier versions:

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/get?key={key}` | `GET` | Returns stored value directly as JSON |
| `/set` | `POST` | Accepts JSON body `{key, value, ttl, priority}` |
| `/delete?key={key}` | `DELETE` | Removes key and returns `204 No Content` |

---

## Architecture

VoidDB is designed to remain simple and self-contained in a single file without external dependencies:

- **`Store`**: Encapsulates the in-memory map protected by `sync.RWMutex`, atomic counters, and dedicated WAL file mutex (`logMu`).
- **`Server`**: REST routing layer using Go 1.22+ pattern matching with strict HTTP method checking.
- **Dual-Phase Expiration**:
  - **Lazy expiration**: On every read, expired keys are detected, deleted, and logged immediately.
  - **Background janitor**: Periodic sweep (`-cleanup` interval) cleans unread expired keys.
- **Priority Eviction**: When configured with `-max-keys`, inserts beyond capacity automatically evict the lowest priority item.
- **Atomic Compaction**: Rewrites only active, unexpired state to a temporary file before atomically renaming it, preventing unbounded log growth.

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
