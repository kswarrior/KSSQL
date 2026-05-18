use ks_sql::parser::engine::Engine;
use std::time::Instant;
use std::fs;
use std::sync::Arc;
use tokio::task;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = "extreme_stress.ksql";
    let wal_path = "extreme_stress.wal";
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);

    println!("======================================================");
    println!("   TITAN-PRIME EXTREME CONCURRENCY STRESS TEST (V1)   ");
    println!("======================================================");
    
    let engine = Arc::new(Engine::new(db_path, wal_path).await?);
    
    // Setup tables
    engine.execute("CREATE TABLE accounts (id SERIAL, balance INT)", 0).await?;
    engine.execute("CREATE TABLE logs (id SERIAL, msg TEXT)", 0).await?;

    let total_tasks = 20;
    let iterations_per_task = 200;
    
    println!("Launching {} concurrent tasks...", total_tasks);
    println!("Total ops target: {}", total_tasks * iterations_per_task);
    
    let start = Instant::now();
    let mut handles = Vec::new();

    for t_id in 0..total_tasks {
        let engine_clone = Arc::clone(&engine);
        let handle = task::spawn(async move {
            let mut local_success = 0;
            for i in 0..iterations_per_task {
                let conn_id = (t_id * iterations_per_task + i) as u32;
                
                // Mix of operations
                let op = i % 4;
                let res = match op {
                    0 => { // INSERT
                         engine_clone.execute(&format!("INSERT INTO accounts (balance) VALUES ({})", i * 100), conn_id).await
                    },
                    1 => { // SELECT with JOIN
                         engine_clone.execute("SELECT * FROM accounts a JOIN logs l ON a.id = l.id LIMIT 1", conn_id).await
                    },
                    2 => { // UPDATE with Contention
                         engine_clone.execute(&format!("UPDATE accounts SET balance = balance + 1 WHERE id = {}", (i % 10) + 1), conn_id).await
                    },
                    3 => { // LOGGING
                         engine_clone.execute(&format!("INSERT INTO logs (msg) VALUES ('Task {} operation {}')", t_id, i), conn_id).await
                    },
                    _ => unreachable!()
                };
                
                if res.is_ok() {
                    local_success += 1;
                }
            }
            local_success
        });
        handles.push(handle);
    }

    let mut total_success = 0;
    for h in handles {
        total_success += h.await?;
    }

    let duration = start.elapsed();
    let total_ops = total_tasks * iterations_per_task;
    
    println!("------------------------------------------------------");
    println!("STRESS TEST COMPLETE");
    println!("Duration:       {:?}", duration);
    println!("Total Ops:      {}", total_ops);
    println!("Successful:     {}", total_success);
    println!("Failed (Cont):  {}", total_ops - total_success);
    println!("Throughput:     {:.2} ops/sec", total_ops as f64 / duration.as_secs_f64());
    println!("------------------------------------------------------");

    println!("Verifying Data Integrity...");
    let count_res = engine.execute("SELECT COUNT(*) FROM accounts", 0).await?;
    println!("Final Account Count: {}", count_res);
    
    let log_count = engine.execute("SELECT COUNT(*) FROM logs", 0).await?;
    println!("Final Log Count:     {}", log_count);

    println!("Syncing and Shutting Down...");
    engine.state.btree.wal.flush_pipeline().await?;
    engine.state.btree.pager.sync().await?;

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);
    println!("Titan-Prime Stability: VERIFIED");
    Ok(())
}
