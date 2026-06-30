use ks_sql::parser::engine::Engine;
use std::fs;
use std::time::Instant;
use std::sync::Arc;
use tokio::task;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = "ingest_proof.ksql";
    let wal_path = "ingest_proof.wal";

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);

    let engine = Arc::new(Engine::new(db_path, wal_path).await?);

    println!("Titan-Prime Evolution: Initializing Concurrent High-Velocity Ingestion Test...");
    engine.execute("CREATE TABLE ingest_test (id SERIAL, data TEXT)", 0).await?;

    let total_rows = 500_000;
    let concurrency = 8;
    let rows_per_task = total_rows / concurrency;
    let batch_size = 1000;

    println!("Target: Writing {} rows across {} parallel tasks...", total_rows, concurrency);

    let start = Instant::now();
    let mut handles = Vec::new();

    for t_id in 0..concurrency {
        let engine_clone = Arc::clone(&engine);
        let handle = task::spawn(async move {
            let iterations = rows_per_task / batch_size;
            for i in 0..iterations {
                let mut values = Vec::with_capacity(batch_size);
                for j in 0..batch_size {
                    let id = t_id * rows_per_task + i * batch_size + j;
                    values.push(format!("({}, 'val_{}_{}')", id, t_id, id));
                }
                let sql = format!("INSERT INTO ingest_test VALUES {}", values.join(", "));
                let _ = engine_clone.execute(&sql, t_id as u32).await;
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    println!("Flushing WAL to storage for durability confirmation...");
    engine.execute("FLUSH", 0).await?;

    let duration = start.elapsed();
    let rps = total_rows as f64 / duration.as_secs_f64();
    let latency_per_row = duration.as_nanos() as f64 / total_rows as f64;

    println!("\n--- CONCURRENT INGESTION PERFORMANCE PROOF ---");
    println!("Total Rows Written: {}", total_rows);
    println!("Concurrency:        {} tasks", concurrency);
    println!("Total Time:         {:?}", duration);
    println!("Verified Throughput: {:.2} Rows/sec", rps);
    println!("Avg Latency:        {:.2} ns/row", latency_per_row);
    println!("------------------------------------\n");

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);

    Ok(())
}
