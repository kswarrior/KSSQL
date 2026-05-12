pub mod storage;
pub mod parser;
pub mod network;

use crate::parser::engine::Engine;
use crate::network::server::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("KS SQL Engine starting...");

    let db_path = "ks_database.ksql";
    let wal_path = "ks_database.wal";

    let engine = Engine::new(db_path, wal_path)?;
    let server = Server::new(engine);

    let addr = "0.0.0.0:5432";
    server.run(addr).await?;

    Ok(())
}
