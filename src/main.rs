use clap::Parser;
use ks_sql::network::server::Server;
use ks_sql::parser::engine::Engine;

#[derive(Parser, Debug)]
#[command(name = "ks-sql")]
#[command(author = "KS Warrior")]
#[command(version = "0.4.0")]
#[command(about = "High-performance RDBMS engine", long_about = None)]
struct Args {
    /// Port configurations (e.g., w:8080 m:5432)
    #[arg(long, num_args = 1..)]
    port: Vec<String>,

    /// Database file path
    #[arg(long, default_value = "ks_database.ksql")]
    db: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut web_port = 8080;
    let mut main_port = 5432;

    for p in args.port {
        if p.starts_with("w:") {
            web_port = p[2..].parse()?;
        } else if p.starts_with("m:") {
            main_port = p[2..].parse()?;
        }
    }

    println!("KS SQL Engine starting (Titan-Prime Mode)...");
    println!("Database: {}", args.db);

    let wal_path = format!("{}.wal", args.db.strip_suffix(".ksql").unwrap_or(&args.db));

    let engine = Engine::new(&args.db, &wal_path).await?;
    let server = Server::new(engine);

    server.run(main_port, web_port).await?;

    Ok(())
}
