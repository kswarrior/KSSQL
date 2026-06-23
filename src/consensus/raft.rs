use std::sync::Arc;
use tokio::sync::mpsc;
use anyhow::Result;
use crate::storage::io::AsyncIoBackend;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftLogEntry {
    Command { sql: String, tx_id: u64 },
    Noop,
}

pub struct RaftNode {
    pub id: u64,
    pub term: u64,
    pub log: Vec<RaftLogEntry>,
    pub io_backend: Arc<dyn AsyncIoBackend>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub last_heartbeat: Instant,
}

impl RaftNode {
    pub fn new(id: u64, io_backend: Arc<dyn AsyncIoBackend>) -> Self {
        Self {
            id,
            term: 0,
            log: Vec::new(),
            io_backend,
            commit_index: 0,
            last_applied: 0,
            last_heartbeat: Instant::now(),
        }
    }

    /// High-throughput zero-copy log append via io_uring
    pub async fn append_entry(&mut self, entry: RaftLogEntry) -> Result<()> {
        self.log.push(entry.clone());
        let encoded = bincode::serialize(&entry)?;
        let offset = self.commit_index * 4096; // 4KB sector aligned log
        self.io_backend.write_at(offset, encoded).await?;
        self.commit_index += 1;
        Ok(())
    }

    /// Garbage collect old log entries after they have been applied to state
    pub fn truncate_log(&mut self, index: u64) {
        if index < self.log.len() as u64 {
            self.log.drain(0..index as usize);
        }
    }

    pub fn send_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
        // Broadcast logic stubs for evolution foundation
    }

    pub fn check_timeout(&self, timeout: Duration) -> bool {
        self.last_heartbeat.elapsed() > timeout
    }
}

pub struct ConsensusManager {
    pub node: Arc<tokio::sync::Mutex<RaftNode>>,
}

impl ConsensusManager {
    pub fn new(id: u64, io_backend: Arc<dyn AsyncIoBackend>) -> Self {
        Self {
            node: Arc::new(tokio::sync::Mutex::new(RaftNode::new(id, io_backend))),
        }
    }
}
