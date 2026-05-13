# KS SQL Engine

KS SQL is a standalone RDBMS engine written in Rust with a modular architecture: storage (B+Tree), parser (SQL logic), and network (TCP server).

## 🚀 Features & Powers

- **Persistent B+Tree Storage**: Implements a robust B+Tree index with 4KB pages for efficient data management.
- **ACID Durability (WAL)**: Every transaction is recorded in a Write-Ahead Log (WAL) to ensure data integrity even in the event of a crash.
- **SQL Support**:
    - **DDL**: `CREATE TABLE` with schema definitions.
    - **CRUD**: Full support for `INSERT`, `SELECT`, `UPDATE`, and `DELETE`.
    - **Filtering**: Support for `WHERE` clauses with equality and inequality operators.
- **High-Performance Networking**: Integrated Tokio-based TCP server capable of handling multiple concurrent connections.
- **Cyberpunk Aesthetic Dashboard**: Future-ready integration for high-end web monitoring.

## ⚡ Performance

Benchmarks performed on standard hardware:
- **Write Speed**: ~280 rows/sec (with synchronous WAL durability).
- **Read Speed**: ~8,000 queries/sec (index-based lookups).

## 🛠️ Architecture

1.  **Storage Layer**:
    - `Pager`: Manages raw 4KB pages on disk.
    - `WAL`: Ensures durability via pre-write logging.
    - `B+Tree`: Provides an ordered index for key-value storage of table rows.
2.  **Parser & Engine**:
    - Leverages `sqlparser` for SQL translation.
    - `Engine`: Manages table schemas and maps SQL statements to B+Tree operations.
3.  **Network Layer**:
    - `Server`: Asynchronous TCP listener on port `5432`.

## 📖 How to Use

### Installation
Ensure you have the Rust toolchain installed. Run the setup script to build the engine:
```bash
./setup.sh
```

### Running the Server
Start the KS SQL engine:
```bash
cargo run --release
```
The server will start listening on `0.0.0.0:5432`.

### Connecting
You can connect to the server via any TCP client using the following format:
`ksql://<user>:<password>@<host>:<port>/<db_name>`

Example SQL commands:
```sql
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users VALUES (1, 'Alice');
SELECT * FROM users WHERE name = 'Alice';
UPDATE users SET name = 'Bob' WHERE id = 1;
DELETE FROM users WHERE id = 1;
```

---
*Developed by KS Warrior.*
