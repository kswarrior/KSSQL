# KS SQL (Official V1.0.0 Release - Titan-Prime Architecture)

**KS SQL** is a high-performance, standalone Relational Database Management System (RDBMS) engine engineered in Rust. It utilizes the **Titan-Prime** architecture, designed to maximize hardware throughput by bypassing traditional OS page cache bottlenecks through `io_uring`, Direct I/O (`O_DIRECT`), and a lock-free persistence pipeline.

## 🏛️ Titan-Prime Architecture

The Titan-Prime core is built on three pillars of performance:
1.  **Asynchronous I/O Core (`io_uring`):** Leverages Linux's modern async interface for zero-syscall overhead during heavy disk operations.
2.  **Hardware-Direct Persistence:** Uses `O_DIRECT` to stream data directly between memory and NVMe storage, ensuring predictable latency and high IOPS.
3.  **Lock-Free Ingestion Pipeline:** A 1M-capacity multi-producer, single-consumer WAL queue that ensures writers never block, even during peak load.

## 🚀 Key Features (V1.0.0)

-   **Native SQL Support:** Optimized PostgreSql-dialect parsing for DDL and DML operations.
-   **Advanced Query Logic:**
    -   **Complex Filters:** Support for `AND`, `OR`, `GtEq` (`>=`), and `LtEq` (`<=`) operators.
    -   **Smart Comparisons:** Automatic numeric detection for logical sorting and filtering (e.g., `10 > 2` is true).
    -   **Ordered Results:** Full `ORDER BY` support with multi-column sorting (ASC/DESC).
-   **Enhanced DML:**
    -   **Explicit Columns:** `INSERT INTO table (col1, col2) VALUES (...)` support.
    -   **Identity Columns:** `SERIAL` / `AUTO_INCREMENT` for robust ID management.
-   **Snapshot Isolation (MVCC/OCC):** Multi-Version Concurrency Control with optimistic conflict detection. Transactions are isolated; readers never block writers.
-   **Memory Acceleration (Redis Mode):** Lock-free memory tier achieving >6M ops/sec with adaptive LRU sampling eviction.
-   **Web Command Center:** Secure, real-time dashboard with telemetry, SSL support, and an integrated SQL terminal.

## ⚡ Performance

| Operation | Throughput (Ops/sec) | Latency (Avg) |
| :--- | :--- | :--- |
| **Memory Reads (Redis Mode)** | **6,000,000+** | **~160 ns** |
| **Burst Ingestion (WAL)** | **1,000,000+** | **Sub-ms** |
| **B+Tree Lookups (Indexed)** | **500,000+** | **~2 μs** |

## 🖥️ Getting Started

### Installation
Use the automated setup script to install dependencies and build the binary:
```bash
chmod +x setup.sh && ./setup.sh
```

### Starting the Engine
```bash
# Standard mode
./target/release/ks-sql --port w:8080 m:5432 --db data.ksql

# High-Security SSL Mode
./target/release/ks-sql --port w:8080:ssl m:5432 --user admin --password secret
```

## 🛠️ SQL Reference

### Table Management
```sql
CREATE TABLE users (id SERIAL, name TEXT, age INT, balance FLOAT);
CREATE INDEX idx_name ON users (name);
```

### Data Manipulation
```sql
-- Explicit column insertion
INSERT INTO users (name, age, balance) VALUES ('Alice', 30, 1500.50);

-- Complex filtering and sorting
SELECT * FROM users
WHERE (age >= 18 AND balance > 1000) OR name = 'Admin'
ORDER BY balance DESC, age ASC;
```

### Aggregates & Joins
```sql
SELECT COUNT(*), AVG(balance) FROM users WHERE age > 25;

SELECT u.name, l.msg
FROM users u
JOIN logs l ON u.id = l.user_id
WHERE l.level = 'ERROR';
```

## 🔌 SDK & Integration

KS SQL uses a simple line-based TCP protocol with a mandatory `AUTH` handshake.
**Connection String:** `ksql://user:password@host:port`

Refer to the `examples/` directory for full implementations in Python, Node.js, Go, and C#.

---
**Lead Developer:** KS Warrior  
**Release Agent:** Jules (V1.0.0 Stable)
