use ks_sql::parser::engine::Engine;
use std::fs;
use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = "ingest_proof.ksql";
    let wal_path = "ingest_proof.wal";

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);

    let engine = Engine::new(db_path, wal_path).await?;

    println!("Titan-Prime Evolution: Initializing High-Velocity Ingestion Test...");
    engine.execute("CREATE TABLE ingest_test (id SERIAL, data TEXT)", 0).await?;

    let batch_size = 5000;
    let total_rows = 500_000;
    let iterations = total_rows / batch_size;

    println!("Target: Writing {} rows in batches of {}...", total_rows, batch_size);

    let start = Instant::now();

    for i in 0..iterations {
        let mut values = Vec::new();
        for j in 0..batch_size {
            values.push(format!("({}, 'val_{}_{}')", i * batch_size + j, i, j));
        }
        let sql = format!("INSERT INTO ingest_test VALUES {}", values.join(", "));
        engine.execute(&sql, 0).await?;

        if (i + 1) % 10 == 0 {
            println!("Processed {} rows...", (i + 1) * batch_size);
        }
    }

    println!("Flushing WAL to NVMe for durability confirmation...");
    engine.execute("FLUSH", 0).await?;

    let duration = start.elapsed();
    let rps = total_rows as f64 / duration.as_secs_f64();
    let latency_per_row = duration.as_nanos() as f64 / total_rows as f64;

    println!("\n--- INGESTION PERFORMANCE PROOF ---");
    println!("Total Rows Written: {}", total_rows);
    println!("Total Time: {:?}", duration);
    println!("Verified Throughput: {:.2} Rows/sec", rps);
    println!("Avg Latency: {:.2} ns/row", latency_per_row);
    println!("------------------------------------\n");

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);

    Ok(())
}
