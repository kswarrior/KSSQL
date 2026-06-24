use crate::parser::engine::Engine;
use std::fs;

fn run_test<F>(f: F)
where F: std::future::Future<Output = ()> + Send + 'static
{
    #[cfg(target_os = "linux")]
    {
        tokio_uring::start(f);
    }
    #[cfg(not(target_os = "linux"))]
    {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(f);
    }
}

#[test]
fn test_serial_auto_increment() {
    let db_path = "test_serial.ksql";
    let wal_path = "test_serial.wal";
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);

    run_test(async move {
        let engine = Engine::new(db_path, wal_path).await.unwrap();
        engine.execute("CREATE TABLE items (id SERIAL, name TEXT)", 0).await.unwrap();
        engine.execute("INSERT INTO items (name) VALUES ('Item A')", 0).await.unwrap();
        engine.execute("INSERT INTO items (name) VALUES ('Item B')", 0).await.unwrap();

        let res = engine.execute("SELECT * FROM items", 0).await.unwrap();
        assert!(res.contains("1 | Item A"));
        assert!(res.contains("2 | Item B"));
    });

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);
}

#[test]
fn test_aggregates() {
    let db_path = "test_agg.ksql";
    let wal_path = "test_agg.wal";
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);

    run_test(async move {
        let engine = Engine::new(db_path, wal_path).await.unwrap();
        engine.execute("CREATE TABLE sales (amount INT)", 0).await.unwrap();
        engine.execute("INSERT INTO sales VALUES (10)", 0).await.unwrap();
        engine.execute("INSERT INTO sales VALUES (20)", 0).await.unwrap();
        engine.execute("INSERT INTO sales VALUES (30)", 0).await.unwrap();

        let res = engine.execute("SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM sales", 0).await.unwrap();
        assert!(res.contains("3 | 60 | 20 | 10 | 30"));
    });

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);
}

#[test]
fn test_wal_recovery() {
    let db_path = "test_recovery.ksql";
    let wal_path = "test_recovery.wal";
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);

    run_test(async move {
        {
            let engine = Engine::new(db_path, wal_path).await.unwrap();
            engine.execute("CREATE TABLE logs (msg TEXT)", 0).await.unwrap();
            engine.execute("INSERT INTO logs VALUES ('Entry 1')", 0).await.unwrap();
            engine.execute("FLUSH", 0).await.unwrap();
            // Data is in WAL and Pager
        }

        // Re-open engine
        let engine = Engine::new(db_path, wal_path).await.unwrap();
        let res = engine.execute("SELECT * FROM logs", 0).await.unwrap();
        assert!(res.contains("Entry 1"));
    });

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);
}
