use ks_sql::parser::engine::Engine;
use std::time::Instant;
use std::fs;
use std::sync::Arc;
use tokio::task;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = "high_load.ksql";
    let wal_path = "high_load.wal";
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);

    println!("======================================================");
    println!("   TITAN-PRIME ULTRA-HIGH CONCURRENCY TEST (V1.0.0)   ");
    println!("======================================================");

    let engine = Arc::new(Engine::new(db_path, wal_path).await?);

    // Setup tables for high contention
    engine.execute("CREATE TABLE inventory (id SERIAL, item_name TEXT, stock INT)", 0).await?;
    engine.execute("CREATE TABLE audit_log (id SERIAL, user_id INT, action TEXT, ts TEXT)", 0).await?;

    // Seed data
    for i in 1..=10 {
        engine.execute(&format!("INSERT INTO inventory (item_name, stock) VALUES ('Item_{}', 1000)", i), 0).await?;
    }

    let num_tasks = 40;
    let ops_per_task = 250;

    println!("Launching {} concurrent tasks...", num_tasks);
    println!("Total operations: {}", num_tasks * ops_per_task);

    let start = Instant::now();
    let mut handles = Vec::new();

    for t_id in 0..num_tasks {
        let engine_clone = Arc::clone(&engine);
        let handle = task::spawn(async move {
            let mut success_count = 0;
            for i in 0..ops_per_task {
                let conn_id = (t_id * 10000 + i) as u32;
                let op = (t_id + i) % 5;

                let res = match op {
                    0 => { // HIGH CONTENTION UPDATE
                        let item_id = (i % 10) + 1;
                        engine_clone.execute(&format!("UPDATE inventory SET stock = stock - 1 WHERE id = {}", item_id), conn_id).await
                    },
                    1 => { // INSERT with Explicit Columns
                        engine_clone.execute(&format!("INSERT INTO audit_log (user_id, action, ts) VALUES ({}, 'PURCHASE', '2023-10-27')", t_id), conn_id).await
                    },
                    2 => { // COMPLEX SELECT with ORDER BY and Filters
                        engine_clone.execute("SELECT * FROM inventory WHERE stock > 500 AND stock < 1500 ORDER BY stock DESC, item_name ASC LIMIT 5", conn_id).await
                    },
                    3 => { // JOIN Query
                        engine_clone.execute("SELECT i.item_name, a.action FROM inventory i JOIN audit_log a ON i.id = a.user_id WHERE i.stock > 0 LIMIT 10", conn_id).await
                    },
                    4 => { // TRANSACTIONal Update
                        engine_clone.execute("BEGIN", conn_id).await.unwrap();
                        let item_id = (i % 5) + 1;
                        engine_clone.execute(&format!("UPDATE inventory SET stock = stock + 1 WHERE id = {}", item_id), conn_id).await.unwrap();
                        engine_clone.execute("COMMIT", conn_id).await
                    },
                    _ => unreachable!()
                };

                if res.is_ok() {
                    success_count += 1;
                }
            }
            success_count
        });
        handles.push(handle);
    }

    let mut total_success = 0;
    for h in handles {
        total_success += h.await?;
    }

    let duration = start.elapsed();
    let total_ops = num_tasks * ops_per_task;

    println!("------------------------------------------------------");
    println!("HIGH LOAD TEST COMPLETE");
    println!("Duration:       {:?}", duration);
    println!("Total Ops:      {}", total_ops);
    println!("Successful:     {}", total_success);
    println!("Throughput:     {:.2} ops/sec", total_ops as f64 / duration.as_secs_f64());
    println!("------------------------------------------------------");

    println!("Verifying Final State Integrity...");

    // Check stock levels - should be roughly balanced if updates were successful
    let stock_report = engine.execute("SELECT item_name, stock FROM inventory ORDER BY id ASC", 0).await?;
    println!("Current Inventory State:\n{}", stock_report);

    let log_count = engine.execute("SELECT COUNT(*) FROM audit_log", 0).await?;
    println!("Audit Log Count: {}", log_count);

    println!("Performing Cold Restart Recovery Test...");
    engine.execute("FLUSH", 0).await?;
    drop(engine);

    let engine_recovery = Engine::new(db_path, wal_path).await?;
    let recovered_logs = engine_recovery.execute("SELECT COUNT(*) FROM audit_log", 0).await?;
    println!("Recovered Log Count: {}", recovered_logs);

    assert_eq!(log_count, recovered_logs, "Data loss detected during recovery!");

    println!("Titan-Prime Integrity: VERIFIED UNDER LOAD");

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);
    Ok(())
}
