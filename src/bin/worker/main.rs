use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::thread;

fn main() {
    println!("\x1b[38;5;198m[WORKER]\x1b[0m KS-Worker Sandbox Initializing...");
    let socket_path = "/tmp/ks-worker.sock";
    let _ = std::fs::remove_file(socket_path);

    let listener = UnixListener::bind(socket_path).expect("Failed to bind unix socket");
    println!("\x1b[38;5;198m[WORKER]\x1b[0m Listening on {} for IPC...", socket_path);

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                thread::spawn(move || {
                    let mut buffer = [0u8; 1024];
                    while let Ok(n) = stream.read(&mut buffer) {
                        if n == 0 { break; }
                        let msg = String::from_utf8_lossy(&buffer[..n]);
                        if msg.trim() == "PING" {
                            let _ = stream.write_all(b"PONG\n");
                        } else if msg.starts_with("CALL") {
                             // Placeholder for WASM execution logic moved to worker
                             let _ = stream.write_all(b"EXECUTED\n");
                        }
                    }
                });
            }
            Err(_) => break,
        }
    }
}
