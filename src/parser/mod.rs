pub mod engine;
pub mod scheduler;

#[cfg(test)]
mod tests {
    use super::engine::Engine;
    use std::fs;

    #[test]
    fn test_engine_sql() {
        let db_path = "test_engine.ksql";
        let wal_path = "test_engine.wal";
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);
        
        tokio_uring::start(async move {
            let engine = Engine::new(db_path, wal_path).await.unwrap();
            engine.execute("CREATE TABLE users (id INT, name TEXT)", 0).await.unwrap();
            engine.execute("INSERT INTO users VALUES (1, 'Alice')", 0).await.unwrap();
            let res = engine.execute("SELECT * FROM users", 0).await.unwrap();
            assert!(res.contains("Alice"));
        });
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);
    }

    #[test]
    fn test_mvcc_occ() {
        let db_path = "test_mvcc.ksql";
        let wal_path = "test_mvcc.wal";
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);

        tokio_uring::start(async move {
            let engine = Engine::new(db_path, wal_path).await.unwrap();
            engine.execute("CREATE TABLE accounts (id INT, balance TEXT)", 0).await.unwrap();
            engine.execute("INSERT INTO accounts VALUES (1, '100')", 0).await.unwrap();

            engine.execute("BEGIN", 1).await.unwrap();
            engine.execute("BEGIN", 2).await.unwrap();

            engine.execute("UPDATE accounts SET balance = '200' WHERE id = 1", 1).await.unwrap();
            engine.execute("UPDATE accounts SET balance = '300' WHERE id = 1", 2).await.unwrap();

            engine.execute("COMMIT", 1).await.unwrap();

            let res = engine.execute("COMMIT", 2).await;
            assert!(res.is_err());
            assert!(res.unwrap_err().to_string().contains("conflict"));
        });
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);
    }
}

#[cfg(test)]
mod tests_extended {
    use super::engine::Engine;
    use std::fs;

    #[test]
    fn test_where_clause_dml() {
        let db_path = "test_dml_ext.ksql";
        let wal_path = "test_dml_ext.wal";
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);

        tokio_uring::start(async move {
            let engine = Engine::new(db_path, wal_path).await.unwrap();
            
            engine.execute("CREATE TABLE users (id TEXT, name TEXT)", 1).await.unwrap();
            engine.execute("INSERT INTO users VALUES ('1', 'Alice')", 1).await.unwrap();
            engine.execute("INSERT INTO users VALUES ('2', 'Bob')", 1).await.unwrap();
            
            engine.execute("UPDATE users SET name = 'Alicia' WHERE id = '1'", 1).await.unwrap();
            
            let res = engine.execute("SELECT name FROM users WHERE id = '1'", 1).await.unwrap();
            assert!(res.contains("Alicia"));
            
            let res2 = engine.execute("SELECT name FROM users WHERE id = '2'", 1).await.unwrap();
            assert!(res2.contains("Bob"));
            assert!(!res2.contains("Alicia"));

            engine.execute("DELETE FROM users WHERE id = '2'", 1).await.unwrap();
            let res3 = engine.execute("SELECT * FROM users", 1).await.unwrap();
            assert!(!res3.contains("Bob"));
            assert!(res3.contains("Alicia"));
        });

        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);
    }

    #[test]
    fn test_join_constraints() {
        let db_path = "test_join_ext.ksql";
        let wal_path = "test_join_ext.wal";
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);

        tokio_uring::start(async move {
            let engine = Engine::new(db_path, wal_path).await.unwrap();
            
            engine.execute("CREATE TABLE a (id TEXT, val TEXT)", 1).await.unwrap();
            engine.execute("CREATE TABLE b (id TEXT, info TEXT)", 1).await.unwrap();
            
            engine.execute("INSERT INTO a VALUES ('1', 'A1')", 1).await.unwrap();
            engine.execute("INSERT INTO a VALUES ('2', 'A2')", 1).await.unwrap();
            engine.execute("INSERT INTO b VALUES ('1', 'B1')", 1).await.unwrap();
            engine.execute("INSERT INTO b VALUES ('3', 'B3')", 1).await.unwrap();
            
            let res = engine.execute("SELECT a.id, a.val, b.info FROM a JOIN b ON a.id = b.id", 1).await.unwrap();
            assert!(res.contains("A1"));
            assert!(res.contains("B1"));
            assert!(!res.contains("A2"));
            assert!(!res.contains("B3"));
        });

        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);
    }
}
