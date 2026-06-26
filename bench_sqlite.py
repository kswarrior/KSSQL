import sqlite3
import time

def benchmark_sqlite():
    conn = sqlite3.connect('bench.db')
    c = conn.cursor()
    c.execute('CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, age TEXT)')

    start = time.time()
    rows = 500000
    batch_size = 5000

    for i in range(0, rows, batch_size):
        data = []
        for j in range(batch_size):
            data.append((i+j, f"User_{i+j}", f"{20 + (i+j)%50}"))
        c.executemany('INSERT INTO test VALUES (?,?,?)', data)
        conn.commit()

    end = time.time()
    total = end - start
    print(f"SQLite Throughput: {rows / total:.2f} Rows/sec")
    print(f"Total Time: {total:.2f}s")
    conn.close()

if __name__ == "__main__":
    benchmark_sqlite()
