# VoidDB

<a href="https://www.namtao.com/0rwx/"><picture><source media="(prefers-color-scheme: dark)" srcset=".github/ai-w-dark.svg"><img src=".github/ai-w-light.svg" alt="AI use: Level W (Write), with help from an AI co-writer" height="28"></picture></a>

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

### Running Tests & Benchmarks

```bash
# Run unit & integration tests with race detection
go test -v -race ./...

# Run performance benchmarks
go test -bench=. -benchmem -run=^$ ./...
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

## Benchmarks

Benchmarked on Apple Silicon (8 cores, Go 1.24+):

```bash
go test -bench=. -benchmem -run=^$ ./...
```

### Store Operations (In-Memory Engine)

| Benchmark | Throughput | Latency | Memory | Allocations | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `Store.Get` | **~20.3M ops/sec** | **~49 ns/op** | 0 B/op | 0 allocs/op | Direct in-memory lookup with `RWMutex` |
| `Store.ParallelGet` | **~4.6M ops/sec** | **~216 ns/op** | 0 B/op | 0 allocs/op | Highly concurrent multi-goroutine reads |
| `Store.Set` | **~335K ops/sec** | **~2.98 µs/op** | 256 B/op | 4 allocs/op | Memory insert + JSON encoding + WAL disk write |
| `Store.ParallelSet` | **~215K ops/sec** | **~4.64 µs/op** | 270 B/op | 5 allocs/op | Concurrent writes across worker goroutines |

### HTTP Endpoints (Full HTTP Pipeline)

| Endpoint | Throughput | Latency | Memory | Allocations | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `GET /keys/{key}` | **~530K req/sec** | **~1.88 µs/op** | 1,689 B/op | 32 allocs/op | Full routing, lookup, and JSON serialization |
| `POST /keys` | **~132K req/sec** | **~7.58 µs/op** | 7,798 B/op | 43 allocs/op | JSON decoding, store write, WAL append, response |

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
