use clap::Parser;
use ks_sql::network::server::Server;
use ks_sql::parser::engine::Engine;

#[derive(Parser, Debug)]
#[command(name = "ks-sql")]
#[command(author = "KS Warrior")]
#[command(version = "1.0.0")]
#[command(about = "Ultimate Enterprise RDBMS Engine", long_about = None)]
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

    let banner = r#"
    __  ___  ____   ____   ____   _
   / / / / |/ / /  / __ \ / __ \ / /
  / /_/ /|   / /  / / / // / / // /
 / __  //   / /__/ /_/ // /_/ // /___
/_/ /_//_/|_\___/\____/ \____//_____/
    "#;
    println!("\x1b[38;5;45m{}\x1b[0m", banner);
    println!("\x1b[38;5;198m[CORE]\x1b[0m Initializing Titan-Prime Runtime...");
    println!("\x1b[38;5;198m[DATA]\x1b[0m Persistence Layer: \x1b[38;5;220m{}\x1b[0m", args.db);

    let wal_path = format!("{}.wal", args.db.strip_suffix(".ksql").unwrap_or(&args.db));

    let engine = Engine::new(&args.db, &wal_path).await?;
    let server = Server::new(engine);

    server.run(main_port, web_port, use_ssl).await?;

    Ok(())
}
