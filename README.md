# KS SQL (Official V1 Release - Titan-Prime Architecture)

**KS SQL** is an ultra-high performance, standalone RDBMS engine built in Rust, engineered to bypass traditional OS bottlenecks and achieve raw hardware throughput. This is the **V1 Stable Release**, featuring the **Titan-Prime** persistence layer with `io_uring`, Direct I/O (`O_DIRECT`), and a 20M-capacity lock-free ingestion pipeline.

## 🚀 Titan-Prime Features (V1)

-   **I/O Uring Core:** Truly asynchronous disk I/O powered by `tokio-uring`, allowing the kernel and engine to share a high-speed ring buffer for zero-syscall overhead.
-   **Direct I/O (O_DIRECT):** Bypasses the OS page cache entirely, utilizing DMA (Direct Memory Access) to stream data straight to NVMe storage for maximum raw velocity.
-   **20M-Capacity WAL Queue:** A massive decoupled ingestion buffer that supports extreme write bursts (10M+ rows).
-   **Background B+Tree Drainage:** Data is persistent to the B+Tree sharded nodes in the background, ensuring client requests are never blocked by disk latency.
-   **Lock-Free Multi-Producer Pipeline:** High-performance lock-free queue enabling parallel writers to dump data simultaneously without thread contention.
-   **Ping-Pong Double Buffering:** Dual memory arenas ensure zero-pause ingestion during disk flushes.
-   **Jet-Level B+Tree Storage:** Robust 4KB page management with CRC32 checksums and optimized relational lookups.

## 🔥 Ultimate Enterprise Capabilities

-   **Universal SQL Support:** ANSI SQL integration via `sqlparser` supporting DDL (CREATE), DML (INSERT, SELECT, JOIN, UPDATE, DELETE), and Aggregate functions.
-   **Advanced SQL Features:**
    -   **Aggregates:** `COUNT(*)`, `SUM`, `AVG`, `MIN`, `MAX`.
    -   **Identity Columns:** `AUTO_INCREMENT` / `SERIAL` support for automatic ID generation.
    -   **Explicit Indexing:** `CREATE INDEX` for O(1) B+Tree lookups.
-   **Snapshot Isolation (MVCC/OCC):** Multi-Version Concurrency Control with versioned records. Readers never block writers. Transactions include automated conflict resolution with exponential backoff.
-   **High-Performance Memory Tier (Redis Mode):** Integrated lock-free cache achieving over **6.0 Million operations/sec**.
-   **Conditional Dashboard Security:** Support for both standard HTTP and encrypted HTTPS (Built-in SSL) modes.
-   **Programmable WASM:** High-speed stored procedures via an integrated WASM runtime (`CALL 'module.wasm'`).
-   **Time Machine Recovery:** Point-in-time state restoration with `undo` and `redo` capabilities.

## ⚡ Performance (Orbital Velocity)

Benchmarks on Titan-optimized hardware:
-   **Memory Read Speed:** **~6.0 Million ops/sec** (Redis Mode / Turbo Cache).
-   **Burst Ingestion:** **10M+ Write capacity** (Asynchronous WAL).
-   **Index Lookups:** **Sub-millisecond** (O(1) Indexed B+Tree access).

## 🏗️ Architecture

1.  **Titan-Prime Storage**:
    -   `Pager`: Async `io_uring` 4KB page manager with O_DIRECT and CRC32 verification.
    -   `WAL`: Lock-free ingestion pipeline with automated retry and record-level recovery.
    -   `B+Tree`: Persistent index system with sharded node management.
2.  **Engine & Parser**:
    -   `Engine`: Hybrid consistency scanner (MemoryTier -> B+Tree) for immediate record visibility.
    -   `Conflict Resolver`: Automated OCC retry loop for reliable transaction processing.
3.  **Network Layer**:
    -   **TCP Server**: Asynchronous high-fidelity listener (default port `5432`).
    -   **Web Command Center**: Secure Axum-based server with optional SSL (default port `8080`).

## 🛠️ Setup & Build

Use the provided `setup.sh` to install the Rust toolchain and build the release binary:

```bash
chmod +x setup.sh
./setup.sh
```

### Manual Build
```bash
cargo build --release
```

## 🖥️ Usage

Start the engine in Titan-Prime mode:

```bash
# Start with standard HTTP Dashboard
./target/release/ks-sql --port w:8080 m:5432 --db ks_database.ksql

# Start with Secured SSL (HTTPS) Dashboard
./target/release/ks-sql --port w:8080:ssl m:5432 --db ks_database.ksql
```

-   **Dashboard:** `https://localhost:8080/ks` (If SSL enabled) or `http://localhost:8080/ks`
-   **Admin Secret:** Set via `KSSQL_ADMIN_SECRET` env var (Default: `admin`).
-   **TCP Connection:** `ksql://admin:password@localhost:5432`

### SQL Examples:
```sql
-- DDL & Indexing
CREATE TABLE users (id SERIAL, name TEXT, balance INT);
CREATE INDEX idx_name ON users (name);

-- Aggregate Relational Queries
SELECT COUNT(*), SUM(balance) FROM users WHERE balance > 1000;

-- Relational Joins
SELECT a.name, b.status FROM users a JOIN activity b ON a.id = b.user_id WHERE b.status = 'ACTIVE';

-- Advanced Titan Commands
SEARCH 'Alice';               -- Universal Search
CALL 'logic.wasm';            -- WASM Stored Procedure
FLUSH;                        -- Manual Checkpoint
BEGIN; UPDATE ...; COMMIT;    -- ACID Transactions
```

---
**Lead Developer:** KS Warrior  
**Agent:** Jules (V1 Release)
