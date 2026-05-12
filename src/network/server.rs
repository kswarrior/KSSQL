use anyhow::{Result, anyhow};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::parser::engine::Engine;
use std::sync::{Arc, Mutex};

#[derive(Debug, PartialEq)]
pub struct Config {
    pub user: String,
    pub password: Option<String>,
    pub host: String,
    pub port: u16,
    pub db_name: String,
}

pub fn parse_connection_string(s: &str) -> Result<Config> {
    if !s.starts_with("ksql://") {
        return Err(anyhow!("Invalid protocol. Expected ksql://"));
    }

    let trimmed = &s[7..];
    let (auth, rest) = if let Some(idx) = trimmed.find('@') {
        (&trimmed[..idx], &trimmed[idx+1..])
    } else {
        return Err(anyhow!("Missing @ in connection string"));
    };

    let (user, password) = if let Some(idx) = auth.find(':') {
        (auth[..idx].to_string(), Some(auth[idx+1..].to_string()))
    } else {
        (auth.to_string(), None)
    };

    let (host_port, db_name) = if let Some(idx) = rest.find('/') {
        (&rest[..idx], rest[idx+1..].to_string())
    } else {
        return Err(anyhow!("Missing database name"));
    };

    let (host, port) = if let Some(idx) = host_port.find(':') {
        (host_port[..idx].to_string(), host_port[idx+1..].parse::<u16>()?)
    } else {
        (host_port.to_string(), 5432)
    };

    Ok(Config {
        user,
        password,
        host,
        port,
        db_name,
    })
}

pub struct Server {
    engine: Arc<Mutex<Engine>>,
}

impl Server {
    pub fn new(engine: Engine) -> Self {
        Server {
            engine: Arc::new(Mutex::new(engine)),
        }
    }

    pub async fn run(self, addr: &str) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        println!("KSSQL Server listening on {}", addr);

        loop {
            let (socket, _) = listener.accept().await?;
            let engine = Arc::clone(&self.engine);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, engine).await {
                    eprintln!("Error handling connection: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(mut socket: TcpStream, engine: Arc<Mutex<Engine>>) -> Result<()> {
    let mut buf = [0u8; 1024];
    loop {
        let n = socket.read(&mut buf).await?;
        if n == 0 { return Ok(()); }

        let query = String::from_utf8_lossy(&buf[..n]).trim().to_string();
        let result = {
            let mut engine = engine.lock().map_err(|_| anyhow!("Failed to lock engine"))?;
            match engine.execute(&query) {
                Ok(res) => res,
                Err(e) => format!("Error: {}", e),
            }
        };

        socket.write_all(result.as_bytes()).await?;
        socket.write_all(b"\n").await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_conn() {
        let s = "ksql://admin:password@127.0.0.1:8080/testdb";
        let config = parse_connection_string(s).unwrap();
        assert_eq!(config.user, "admin");
        assert_eq!(config.password, Some("password".to_string()));
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 8080);
        assert_eq!(config.db_name, "testdb");
    }
}
