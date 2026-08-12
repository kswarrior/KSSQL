use ks_sql::parser::engine::Engine;
use std::fs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = "v1_verify.ksql";
    let wal_path = "v1_verify.wal";
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);

    println!("--- KS SQL V1 Verification ---");
    let engine = Engine::new(db_path, wal_path).await?;

    // 1. Test Explicit Column Insert
    println!("Testing explicit column INSERT...");
    engine.execute("CREATE TABLE users (id SERIAL, name TEXT, age INT)", 0).await?;
    engine.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)", 0).await?;
    engine.execute("INSERT INTO users (age, name) VALUES (25, 'Bob')", 0).await?;
    let res = engine.execute("SELECT * FROM users", 0).await?;
    println!("Users:\n{}", res);
    assert!(res.contains("Alice | 30"));
    assert!(res.contains("Bob | 25"));

    // 2. Test AND/OR Logic and Numeric Comparison
    println!("Testing AND/OR logic and numeric comparisons...");
    engine.execute("INSERT INTO users (name, age) VALUES ('Charlie', 15)", 0).await?;
    let res = engine.execute("SELECT name FROM users WHERE age >= 18 AND age < 100", 0).await?;
    println!("Adults:\n{}", res);
    assert!(res.contains("Alice"));
    assert!(res.contains("Bob"));
    assert!(!res.contains("Charlie"));

    let res = engine.execute("SELECT name FROM users WHERE age < 18 OR age > 25", 0).await?;
    println!("Special selection:\n{}", res);
    assert!(res.contains("Alice"));
    assert!(res.contains("Charlie"));
    assert!(!res.contains("Bob"));

    // 3. Test ORDER BY
    println!("Testing ORDER BY...");
    let res = engine.execute("SELECT name, age FROM users ORDER BY age ASC", 0).await?;
    println!("Ordered by Age ASC:\n{}", res);
    // Charlie (15), Bob (25), Alice (30)
    let lines: Vec<&str> = res.lines().collect();
    assert!(lines[2].contains("Charlie"));
    assert!(lines[3].contains("Bob"));
    assert!(lines[4].contains("Alice"));

    // 4. Test numeric sorting (10 > 2)
    println!("Testing numeric sorting...");
    engine.execute("CREATE TABLE scores (val TEXT)", 0).await?;
    engine.execute("INSERT INTO scores VALUES ('2')", 0).await?;
    engine.execute("INSERT INTO scores VALUES ('10')", 0).await?;
    engine.execute("INSERT INTO scores VALUES ('1')", 0).await?;
    let res = engine.execute("SELECT val FROM scores ORDER BY val ASC", 0).await?;
    println!("Scores ASC:\n{}", res);
    let lines: Vec<&str> = res.lines().collect();
    assert!(lines[2].contains("1"));
    assert!(lines[3].contains("2"));
    assert!(lines[4].contains("10"));

    let res = engine.execute("SELECT name, age FROM users ORDER BY name DESC", 0).await?;
    println!("Ordered by Name DESC:\n{}", res);
    // Charlie, Bob, Alice
    let lines: Vec<&str> = res.lines().collect();
    assert!(lines[2].contains("Charlie"));
    assert!(lines[3].contains("Bob"));
    assert!(lines[4].contains("Alice"));

    println!("V1 Verification: SUCCESS");

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(wal_path);
    Ok(())
}
