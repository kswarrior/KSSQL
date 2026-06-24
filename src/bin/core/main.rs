use clap::Parser;
use ks_sql::network::server::Server;
use ks_sql::parser::engine::Engine;
use std::process::Command;

#[derive(Parser, Debug)]
#[command(name = "ks-core")]
#[command(author = "KS Warrior")]
#[command(version = "1.1.0")]
#[command(about = "Ultra-Scale Core Storage Engine", long_about = None)]
struct Args {
    #[arg(long, num_args = 1..)]
    port: Vec<String>,
    #[arg(long, default_value = "ks_database.ksql")]
    db: String,
    #[arg(long, default_value = "admin")]
    user: String,
    #[arg(long, default_value = "admin")]
    password: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut web_port = 8080;
    let mut use_ssl = false;
    let mut main_port = 5432;

    for p in args.port {
        if p.starts_with("w:") {
            let parts: Vec<&str> = p[2..].split(':').collect();
            web_port = parts[0].parse()?;
            if parts.len() > 1 && parts[1].to_lowercase() == "ssl" {
                use_ssl = true;
            }
        } else if p.starts_with("m:") {
            main_port = p[2..].parse()?;
        }
    }

    println!("\x1b[38;5;45mInitializing Titan-Prime Evolution Core...\x1b[0m");

    // Spawn Worker Process using current executable path logic
    let exe_path = std::env::current_exe()?;
    let bin_dir = exe_path.parent().unwrap();
    let worker_bin = bin_dir.join("ks-worker");

    let mut _worker = Command::new(worker_bin)
        .spawn()
        .expect("Failed to start ks-worker process");

    let wal_path = format!("{}.wal", args.db.strip_suffix(".ksql").unwrap_or(&args.db));
    let engine = Engine::new(&args.db, &wal_path).await?;
    let server = Server::new(engine, args.user, args.password);

    server.run(main_port, web_port, use_ssl).await?;

    Ok(())
}
