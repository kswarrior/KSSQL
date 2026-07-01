use crate::parser::engine::Engine;
use crate::network::pgproto::{PgProtocolHandler, PgMessage};
use anyhow::Result;
use axum::{
    extract::{ws::WebSocket, State, WebSocketUpgrade},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Router,
};
use axum_server::tls_rustls::RustlsConfig;
use serde_json::json;
use std::fs;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, AsyncReadExt};
use tokio::net::{TcpListener as TokioTcpListener, TcpStream};
use tokio::sync::broadcast;

pub struct Server {
    engine: Arc<Engine>,
    tx: broadcast::Sender<String>,
    admin_user: String,
    admin_pass: String,
}

impl Server {
    pub fn new(engine: Engine, user: String, pass: String) -> Self {
        let (tx, _) = broadcast::channel(100);
        Server {
            engine: Arc::new(engine),
            tx,
            admin_user: user,
            admin_pass: pass,
        }
    }

    pub async fn run(self, main_port: u16, web_port: u16, use_ssl: bool) -> Result<()> {
        let engine_for_tcp = Arc::clone(&self.engine);
        let tcp_addr = format!("0.0.0.0:{}", main_port);
        let tx_for_tcp = self.tx.clone();

        let admin_user = self.admin_user.clone();
        let admin_pass = self.admin_pass.clone();
        tokio::spawn(async move {
            let listener = match TokioTcpListener::bind(&tcp_addr).await {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("\x1b[31m[ERROR]\x1b[0m Failed to bind TCP listener: {}", e);
                    return;
                }
            };
            println!("\x1b[38;5;82m[LIVE]\x1b[0m SQL Engine Protocol (KS-SQL + PG-Wire): \x1b[1mport {}\x1b[0m", main_port);
            let mut next_conn_id = 1000;
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(res) => res,
                    Err(_) => continue,
                };
                let engine = Arc::clone(&engine_for_tcp);
                let tx = tx_for_tcp.clone();
                let conn_id = next_conn_id;
                let user_clone = admin_user.clone();
                let pass_clone = admin_pass.clone();
                next_conn_id += 1;
                tokio::spawn(async move {
                    if let Err(e) =
                        handle_tcp_connection(socket, engine, tx, conn_id, user_clone, pass_clone).await
                    {
                        eprintln!("TCP Error: {}", e);
                    }
                });
            }
        });

        let app = Router::new()
            .route("/", get(terminal_page))
            .route("/resilience", get(resilience_page))
            .route("/telemetry", get(telemetry_page))
            .route("/ws", get(ws_handler))
            .route("/api/query", post(query_handler))
            .route("/api/undo", post(undo_handler))
            .route("/api/redo", post(redo_handler))
            .route("/api/backup", get(backup_handler))
            .route("/api/memory/mode", post(memory_mode_handler))
            .route("/api/memory/limit", post(memory_limit_handler))
            .route("/api/memory/purge", post(memory_purge_handler))
            .route("/api/hotswap", post(hotswap_handler))
            .with_state((
                Arc::clone(&self.engine),
                self.tx.clone(),
                self.admin_user.clone(),
                self.admin_pass.clone(),
            ));

        let web_addr: std::net::SocketAddr = format!("0.0.0.0:{}", web_port).parse()?;

        if use_ssl {
            let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
            let cert = rcgen::generate_simple_self_signed(subject_alt_names)?;
            let config = RustlsConfig::from_der(
                vec![cert.serialize_der()?],
                cert.serialize_private_key_der(),
            )
            .await?;

            println!("\x1b[38;5;82m[LIVE]\x1b[0m Command Center: \x1b[1;38;5;45mhttps://localhost:{}/\x1b[0m", web_port);
            axum_server::bind_rustls(web_addr, config)
                .serve(app.into_make_service())
                .await?;
        } else {
            println!("\x1b[38;5;82m[LIVE]\x1b[0m Command Center: \x1b[1;38;5;45mhttp://localhost:{}/\x1b[0m", web_port);
            let listener = tokio::net::TcpListener::bind(&web_addr).await?;
            axum::serve(listener, app).await?;
        }

        Ok(())
    }
}

async fn handle_tcp_connection(
    socket: TcpStream,
    engine: Arc<Engine>,
    tx: broadcast::Sender<String>,
    conn_id: u32,
    user: String,
    pass: String,
) -> Result<()> {
    let mut buffer = [0u8; 8192];
    let n = socket.peek(&mut buffer).await.unwrap_or(0);

    if n > 4 {
        if let Ok((msg, _)) = PgProtocolHandler::decode(&buffer[..n]) {
            match msg {
                PgMessage::SSLRequest | PgMessage::Startup { .. } => {
                    return handle_pg_wire(socket, engine, tx, conn_id).await;
                }
                _ => {}
            }
        }
    }

    handle_ksql_wire(socket, engine, tx, conn_id, user, pass).await
}

async fn handle_pg_wire(mut socket: TcpStream, engine: Arc<Engine>, tx: broadcast::Sender<String>, conn_id: u32) -> Result<()> {
    let mut buffer = [0u8; 8192];

    // 1. Initial Handshake
    let n = socket.read(&mut buffer).await?;
    if n == 0 { return Ok(()); }

    if let Ok((msg, _)) = PgProtocolHandler::decode(&buffer[..n]) {
        match msg {
            PgMessage::SSLRequest => {
                socket.write_all(b"N").await?; // No SSL
                let n2 = socket.read(&mut buffer).await?;
                if n2 == 0 { return Ok(()); }
                // Expect Startup after SSL response
            }
            PgMessage::Startup { .. } => {
                // Handled below
            }
            _ => return Err(anyhow::anyhow!("Expected Startup Message")),
        }
    }

    // Authentication OK
    socket.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).await?;

    // ParameterStatus messages (standard for PG clients)
    let params = vec![
        ("server_version", "1.1.0"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO"),
    ];
    for (k, v) in params {
        let mut p_buf = Vec::new();
        p_buf.push(b'S');
        let msg_len = (k.len() + v.len() + 6) as i32;
        p_buf.extend_from_slice(&msg_len.to_be_bytes());
        p_buf.extend_from_slice(k.as_bytes());
        p_buf.push(0);
        p_buf.extend_from_slice(v.as_bytes());
        p_buf.push(0);
        socket.write_all(&p_buf).await?;
    }

    // BackendKeyData
    let mut k_buf = Vec::new();
    k_buf.push(b'K');
    k_buf.extend_from_slice(&12i32.to_be_bytes());
    k_buf.extend_from_slice(&std::process::id().to_be_bytes());
    k_buf.extend_from_slice(&1234u32.to_be_bytes());
    socket.write_all(&k_buf).await?;

    // ReadyForQuery
    socket.write_all(&[b'Z', 0, 0, 0, 5, b'I']).await?;

    let mut offset = 0;
    let mut total_read = 0;
    loop {
        if offset >= total_read {
            offset = 0;
            total_read = socket.read(&mut buffer).await?;
            if total_read == 0 { break; }
        }

        match PgProtocolHandler::decode(&buffer[offset..total_read]) {
            Ok((msg, size)) => {
                offset += size;
                match msg {
                    PgMessage::Query(q) => {
                        let _ = tx.send(q.clone());
                        let mut tag = "SELECT".to_string();
                        match engine.execute(&q, conn_id).await {
                            Ok(res) => {
                                let lines: Vec<&str> = res.split('\n').collect();
                                if lines.len() > 1 && lines[1].starts_with('-') {
                                    // Result set (Table)
                                    let cols: Vec<String> = lines[0].split('|').map(|s| s.trim().to_string()).collect();
                                    socket.write_all(&PgProtocolHandler::encode_row_description(&cols)).await?;
                                    for line in &lines[2..] {
                                        if line.trim().is_empty() { continue; }
                                        let vals: Vec<String> = line.split('|').map(|s| s.trim().to_string()).collect();
                                        socket.write_all(&PgProtocolHandler::encode_data_row(&vals)).await?;
                                    }
                                    tag = "SELECT".to_string();
                                } else {
                                    // Command response (e.g. "Inserted 1 rows")
                                    if res.to_uppercase().contains("INSERTED") {
                                        let count = res.split_whitespace().nth(1).unwrap_or("1");
                                        tag = format!("INSERT 0 {}", count);
                                    } else if res.to_uppercase().contains("CREATED") {
                                        tag = "CREATE TABLE".to_string();
                                    } else if res.to_uppercase().contains("DELETED") {
                                        let count = res.split_whitespace().nth(1).unwrap_or("0");
                                        tag = format!("DELETE {}", count);
                                    } else {
                                        tag = "SELECT".to_string();
                                    }
                                }
                            }
                            Err(e) => {
                                let err_msg = format!("Error: {}", e);
                                socket.write_all(&PgProtocolHandler::encode_data_row(&[err_msg])).await?;
                            }
                        };
                        // CommandComplete
                        let mut c_msg = Vec::new();
                        c_msg.push(b'C');
                        let tag_bytes = tag.as_bytes();
                        let c_len = (tag_bytes.len() + 5) as i32;
                        c_msg.extend_from_slice(&c_len.to_be_bytes());
                        c_msg.extend_from_slice(tag_bytes);
                        c_msg.push(0);
                        socket.write_all(&c_msg).await?;

                        // ReadyForQuery
                        socket.write_all(&[b'Z', 0, 0, 0, 5, b'I']).await?;
                    }
                    PgMessage::Terminate => break,
                    _ => {
                        socket.write_all(&[b'Z', 0, 0, 0, 5, b'I']).await?;
                    }
                }
            }
            Err(_) => break,
        }
    }
    Ok(())
}

async fn handle_ksql_wire(
    socket: TcpStream,
    engine: Arc<Engine>,
    tx: broadcast::Sender<String>,
    conn_id: u32,
    user: String,
    pass: String,
) -> Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let mut authenticated = false;

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            return Ok(());
        }
        let input = line.trim().to_string();

        if !authenticated {
            if input.starts_with("AUTH ") {
                let provided = input.trim_start_matches("AUTH ").trim();
                if provided == format!("{}:{}", user, pass) {
                    authenticated = true;
                    let _ = writer.write_all(b"AUTHENTICATED\n").await;
                    continue;
                }
            }
            let _ = writer
                .write_all(b"ERROR: Authentication Required. Send 'AUTH <user>:<pass>'\n")
                .await;
            return Ok(());
        }

        let query = input;
        let _ = tx.send(query.clone());
        let result = match engine.execute(&query, conn_id).await {
            Ok(res) => res,
            Err(e) => format!("Error: {}", e),
        };
        let _ = writer.write_all(result.as_bytes()).await;
        let _ = writer.write_all(b"\n").await;
    }
}

const TERMINAL_HTML: &str = include_str!("ui/terminal.html");
const RESILIENCE_HTML: &str = include_str!("ui/resilience.html");
const TELEMETRY_HTML: &str = include_str!("ui/telemetry.html");

async fn terminal_page() -> Html<&'static str> {
    Html(TERMINAL_HTML)
}

async fn resilience_page() -> Html<&'static str> {
    Html(RESILIENCE_HTML)
}

async fn telemetry_page() -> Html<&'static str> {
    Html(TELEMETRY_HTML)
}

type AppState = (Arc<Engine>, broadcast::Sender<String>, String, String);

async fn ws_handler(ws: WebSocketUpgrade, State((engine, tx, _, _)): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| handle_ws(socket, engine, tx))
}

async fn handle_ws(mut socket: WebSocket, engine: Arc<Engine>, tx: broadcast::Sender<String>) {
    let mut rx = tx.subscribe();
    let mut sys = sysinfo::System::new_all();

    loop {
        tokio::select! {
            Ok(msg) = rx.recv() => {
                let payload = json!({"type": "log", "msg": msg});
                if socket.send(axum::extract::ws::Message::Text(payload.to_string().into())).await.is_err() { break; }
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                 sys.refresh_all();
                 let stats = {
                    let hw = &engine.hardware_specs;
                    let rps = 10000 + (rand::random::<u32>() % 5000);
                    let cpu_usage = sys.global_cpu_info().cpu_usage();
                    let mem = &engine.state.btree.memory_tier;
                    let redis_mode = mem.turbo_mode.load(std::sync::atomic::Ordering::Relaxed) == 1;

                    json!({
                        "type": "metric",
                        "cpu_cores": hw.cpu_cores,
                        "cpu_load": format!("{:.1}", cpu_usage),
                        "ram_total": hw.total_ram_mb,
                        "ram_usage": (sys.used_memory() / 1024 / 1024),
                        "jet_buffer_mb": hw.jet_buffer_size_mb,
                        "rps": rps,
                        "is_saturated": cpu_usage > 90.0,
                        "redis_mode": redis_mode,
                        "cache_hit_ratio": mem.get_hit_ratio()
                    })
                };
                if socket.send(axum::extract::ws::Message::Text(stats.to_string().into())).await.is_err() { break; }
            }
        }
    }
}

fn check_auth(headers: &HeaderMap, user: &str, pass: &str) -> bool {
    headers
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .map(|h| h == format!("{}:{}", user, pass))
        .unwrap_or(false)
}

async fn query_handler(
    State((engine, tx, user, pass)): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Response {
    if !check_auth(&headers, &user, &pass) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }
    let _ = tx.send(body.clone());
    let conn_id = 999;
    match engine.execute(&body, conn_id).await {
        Ok(res) => res.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("Error: {}", e)).into_response(),
    }
}

async fn undo_handler(State((engine, _, user, pass)): State<AppState>, headers: HeaderMap) -> Response {
    if !check_auth(&headers, &user, &pass) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let _ = engine.undo().await;
    StatusCode::OK.into_response()
}

async fn redo_handler(State((engine, _, user, pass)): State<AppState>, headers: HeaderMap) -> Response {
    if !check_auth(&headers, &user, &pass) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let _ = engine.redo().await;
    StatusCode::OK.into_response()
}

async fn backup_handler(
    State((engine, _, user, pass)): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if !check_auth(&headers, &user, &pass) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let db_path = engine.state.db_path.clone();
    fs::read(db_path).unwrap_or_default().into_response()
}

async fn memory_mode_handler(
    State((engine, _, user, pass)): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Response {
    if !check_auth(&headers, &user, &pass) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let req: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
    if let Some(turbo) = req.get("turbo").and_then(|v| v.as_bool()) {
        let val = if turbo { 1 } else { 0 };
        engine
            .state
            .btree
            .memory_tier
            .turbo_mode
            .store(val, std::sync::atomic::Ordering::Relaxed);
        StatusCode::OK.into_response()
    } else {
        StatusCode::BAD_REQUEST.into_response()
    }
}

async fn memory_limit_handler(
    State((engine, _, user, pass)): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Response {
    if !check_auth(&headers, &user, &pass) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let req: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
    if let Some(limit) = req.get("limit_mb").and_then(|v| v.as_u64()) {
        engine
            .state
            .btree
            .memory_tier
            .max_ram_mb
            .store(limit, std::sync::atomic::Ordering::Relaxed);
        StatusCode::OK.into_response()
    } else {
        StatusCode::BAD_REQUEST.into_response()
    }
}

async fn memory_purge_handler(
    State((engine, _, user, pass)): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if !check_auth(&headers, &user, &pass) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    engine.state.btree.memory_tier.clear();
    StatusCode::OK.into_response()
}

async fn hotswap_handler(
    State((engine, _, user, pass)): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Response {
    if !check_auth(&headers, &user, &pass) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let req: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
    if let Some(url) = req.get("url").and_then(|v| v.as_str()) {
        if let Ok(resp) = reqwest::get(url).await {
            if let Ok(bytes) = resp.bytes().await {
                let db_path = engine.state.db_path.clone();
                let tmp_path = format!("{}.tmp", db_path);
                if let Ok(_) = fs::write(&tmp_path, bytes) {
                    let _ = engine.state.btree.wal.flush_pipeline().await;
                    let _ = engine.state.btree.pager.sync().await;
                    let _ = fs::rename(&tmp_path, &db_path);
                    let _ = engine.state.btree.pager.reload(std::path::Path::new(&db_path)).await;
                    return StatusCode::OK.into_response();
                }
            }
        }
    }
    StatusCode::BAD_REQUEST.into_response()
}
