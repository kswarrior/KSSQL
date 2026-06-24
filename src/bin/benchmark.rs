use ks_sql::parser::engine::Engine;
use std::fs;
use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = "bench.ksql";
    let wal_path = "bench.wal";

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);

    let engine = Engine::new(db_path, wal_path).await?;

    println!("KS SQL Benchmark: Starting Data Load...");
    engine
        .execute("CREATE TABLE bench (id INT, val TEXT)", 0)
        .await?;

    for i in 0..100 {
        engine
            .execute(
                &format!("INSERT INTO bench VALUES ({}, 'data_{}')", i, i),
                0,
            )
            .await?;
    }

    println!("Data Load Complete. Enabling Redis Mode & Warming up RAM Tier...");
    {
        let state = engine.state.clone();
        state
            .btree
            .memory_tier
            .turbo_mode
            .store(1, std::sync::atomic::Ordering::Relaxed);
    }
    engine.execute("SELECT * FROM bench LIMIT 100", 0).await?;

    println!("Starting Throughput Test (1,000,000 Reads)...");
    let start = Instant::now();
    let iterations = 1000000;

    {
        let state = engine.state.clone();
        let mem = &state.btree.memory_tier;
        let key = mem.turbo_cache.iter().next().unwrap().key().clone();
        println!("Benchmarking with key: {}", String::from_utf8_lossy(&key));
        for _ in 0..iterations {
            let _ = mem.get(&key).unwrap();
        }
    }

    let duration = start.elapsed();
    let rps = iterations as f64 / duration.as_secs_f64();
    let latency = duration.as_nanos() as f64 / iterations as f64;

    println!("Benchmark Results:");
    println!("Total Time: {:?}", duration);
    println!("Throughput: {:.2} Reads/sec", rps);
    println!(
        "Avg Latency: {:.2} ns ({:.2} μs)",
        latency,
        latency / 1000.0
    );

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);

    Ok(())
}
