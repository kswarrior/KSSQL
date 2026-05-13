pub mod engine;

#[cfg(test)]
mod tests {
    use super::engine::Engine;
    use std::fs;

    #[test]
    fn test_engine_sql() {
        let db_path = "test_engine.ksql";
        let wal_path = "test_engine.wal";
        {
            let mut engine = Engine::new(db_path, wal_path).unwrap();
            engine.execute("CREATE TABLE users (id INT, name TEXT)").unwrap();
            engine.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
            engine.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();

            let res = engine.execute("SELECT * FROM users WHERE name = 'Alice'").unwrap();
            assert!(res.contains("Alice"));
            assert!(!res.contains("Bob"));

            engine.execute("UPDATE users SET name = 'Charlie' WHERE name = 'Alice'").unwrap();
            let res2 = engine.execute("SELECT * FROM users WHERE name = 'Charlie'").unwrap();
            assert!(res2.contains("Charlie"));

            engine.execute("DELETE FROM users WHERE name = 'Bob'").unwrap();
            let res3 = engine.execute("SELECT * FROM users").unwrap();
            assert!(!res3.contains("Bob"));
        }
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);
    }
}
