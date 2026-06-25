# KS SQL Integration Guide (Titan-Prime Evolution)

**KS SQL** is designed for extreme scale and seamless integration. Following the **Titan-Prime Evolution (V1.1.0)** update, the engine now supports the **PostgreSQL Wire Protocol (v3.0)**, allowing you to use standard PostgreSQL client libraries across almost every major programming language.

---

## 🚀 Integration via PostgreSQL Protocol
The engine listens on port `5432` (default) for SQL queries. Authentication is required by default.

### 🟢 Node.js
Use the standard `pg` package.

```javascript
const { Client } = require('pg');

const client = new Client({
  user: 'admin',
  host: 'localhost',
  database: 'ksql',
  password: 'admin',
  port: 5432,
});

async function run() {
  await client.connect();
  const res = await client.query('SELECT * FROM users LIMIT 10');
  console.log('Query Result:', res.rows);
  await client.end();
}
run();
```

### ⚛️ Next.js (Server Actions)
You can integrate KS SQL directly into your Next.js application using `pg`.

```typescript
// app/actions.ts
'use server'
import { Client } from 'pg';

export async function getUsers() {
  const client = new Client("postgres://admin:admin@localhost:5432/ksql");
  await client.connect();
  const { rows } = await client.query('SELECT * FROM users');
  await client.end();
  return rows;
}
```

### 🐍 Python
Use `psycopg2` for synchronous or `asyncpg` for high-performance asynchronous workloads.

```python
import psycopg2

conn = psycopg2.connect(
    dbname="ksql",
    user="admin",
    password="admin",
    host="localhost",
    port="5432"
)

cur = conn.cursor()
cur.execute("SELECT name, email FROM customers WHERE active = 'true'")
for row in cur.fetchall():
    print(f"Customer: {row[0]} ({row[1]})")

cur.close()
conn.close()
```

### 🦀 Rust
Leverage the native performance of Rust with `tokio-postgres`.

```rust
use tokio_postgres::{NoTls, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Connect to the Titan-Prime engine
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=admin password=admin dbname=ksql", NoTls).await?;

    // The connection object performs the actual communication
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let rows = client.query("SELECT id, val FROM metrics", &[]).await?;
    for row in rows {
        let val: &str = row.get(1);
        println!("Metric: {}", val);
    }
    Ok(())
}
```

### 🐹 Go
Use `pgx`, the most popular PostgreSQL driver for Go.

```go
package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"os"
)

func main() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://admin:admin@localhost:5432/ksql")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	var count int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM logs").Scan(&count)
	fmt.Printf("Total Logs: %d\n", count)
}
```

### ☕ Java
Use standard JDBC for Spring Boot or standalone Java applications.

```java
import java.sql.*;

public class KSQLTest {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/ksql";
        try (Connection conn = DriverManager.getConnection(url, "admin", "admin")) {
            PreparedStatement st = conn.prepareStatement("SELECT * FROM inventory WHERE stock < ?");
            st.setInt(1, 10);
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                System.out.println("Low Stock: " + rs.getString("item_name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
```

### 🐘 PHP
Integrate into Laravel or vanilla PHP using the PDO driver.

```php
<?php
$dsn = "pgsql:host=localhost;port=5432;dbname=ksql;";
try {
    $pdo = new PDO($dsn, "admin", "admin");
    $stmt = $pdo->prepare("SELECT * FROM sensor_data ORDER BY timestamp DESC LIMIT 5");
    $stmt->execute();

    foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $row) {
        echo "Reading: " . $row['value'] . "\n";
    }
} catch (PDOException $e) {
    echo "Connection failed: " . $e->getMessage();
}
?>
```

---

## ⚡ Native KS-SQL Protocol (Legacy/Lightweight)
If you require an extremely lightweight integration without the overhead of the PostgreSQL protocol, you can use the raw line-based TCP protocol.

1.  **Handshake:** Send `AUTH <user>:<pass>\n`
2.  **Receive:** `AUTHENTICATED\n` or `ERROR...\n`
3.  **Command:** Send `<SQL_QUERY>\n`
4.  **Response:** Receive the result set string followed by `\n`

**Example using `nc` (Netcat):**
```bash
echo -e "AUTH admin:admin\nSELECT * FROM users\n" | nc localhost 5432
```

---
**Lead Developer:** KS Warrior
**Architecture:** Titan-Prime Evolution
