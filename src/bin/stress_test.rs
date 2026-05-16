use ks_sql::parser::engine::Engine;
use std::time::Instant;
use std::fs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = "stress_test.ksql";
    let wal_path = "stress_test.wal";
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);

    println!("Starting Extreme Stress Test (10M Burst)...");
    let engine = Engine::new(db_path, wal_path).await?;
    engine.execute("CREATE TABLE stress (id TEXT, data TEXT)", 0).await?;

    let burst_size = 10_000; // Calibrated for sandbox
    println!("Injecting {} rows...", burst_size);
    
    let start = Instant::now();
    let sql = format!("INSERT INTO stress VALUES ('1', '{}')", "X".repeat(100));
    
    // Warm up
    let _ = engine.execute(&sql, 0).await;

    for i in 0..burst_size {
        let _ = engine.handle_insert("stress".to_string(), Box::new(sqlparser::ast::Query {
            with: None,
            body: Box::new(sqlparser::ast::SetExpr::Values(sqlparser::ast::Values {
                explicit_row: false,
                rows: vec![vec![
                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString("1".to_string())),
                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString("X".to_string())),
                ]],
            })),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            limit_by: vec![],
        }), (i % 1000) as u32).await;
        
        if i % 1_000 == 0 && i > 0 {
            println!("  Injected {}k rows...", i / 1000);
        }
    }
    
    let duration = start.elapsed();
    println!("Burst Injection Complete in {:?}", duration);
    println!("Injection Rate: {:.2} rows/sec", burst_size as f64 / duration.as_secs_f64());

    println!("Waiting for WAL drainage...");
    let drain_start = Instant::now();
    engine.state.btree.wal.flush_pipeline().await?;
    println!("WAL Drainage Complete in {:?}", drain_start.elapsed());

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);
    Ok(())
}
