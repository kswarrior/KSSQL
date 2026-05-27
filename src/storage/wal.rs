use crate::storage::pager::AlignedBuf;
use anyhow::Result;
use crossbeam::queue::ArrayQueue;
use serde::{Deserialize, Serialize};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WalEntry {
    PageUpdate {
        page_id: u32,
        data: Vec<u8>,
    },
    RecordUpdate {
        key: Vec<u8>,
        data: Vec<u8>,
    },
    TransactionStart {
        tx_id: u64,
    },
    TransactionCommit {
        tx_id: u64,
        version: u64,
        timestamp: i64,
    },
    TransactionRollback {
        tx_id: u64,
    },
}

pub enum WalRequest {
    Flush {
        batch: Vec<u8>,
        resp: tokio::sync::oneshot::Sender<()>,
    },
}

pub struct Wal {
    queue: Arc<ArrayQueue<WalEntry>>,
    pub drain_queue: Arc<ArrayQueue<WalEntry>>,
    tx: mpsc::Sender<WalRequest>,
    buffer_a: Mutex<Vec<u8>>,
    buffer_b: Mutex<Vec<u8>>,
    active_is_a: Arc<std::sync::atomic::AtomicBool>,
    batch_limit: std::sync::atomic::AtomicUsize,
    recovered_entries: Mutex<Vec<WalEntry>>,
}

impl Wal {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_owned = path.as_ref().to_owned();

        let mut recovered = Vec::new();
        if path_owned.exists() {
            if let Ok(mut file) = std::fs::File::open(&path_owned) {
                let mut buffer = Vec::new();
                use std::io::Read;
                let _ = file.read_to_end(&mut buffer);

                let mut cursor = 0;
                while cursor + 4 <= buffer.len() {
                    let len_bytes: [u8; 4] = buffer[cursor..cursor + 4].try_into().unwrap_or([0; 4]);
                    let len = u32::from_le_bytes(len_bytes) as usize;
                    if len == 0 || len > 10 * 1024 * 1024 {
                        cursor = ((cursor + 4095) / 4096) * 4096;
                        if cursor >= buffer.len() { break; }
                        continue;
                    }
                    cursor += 4;
                    if cursor + len <= buffer.len() {
                        if let Ok(entry) = bincode::deserialize::<WalEntry>(&buffer[cursor..cursor + len]) {
                            recovered.push(entry);
                        }
                        cursor += len;
                    } else {
                        cursor = ((cursor + 4095) / 4096) * 4096;
                        if cursor >= buffer.len() { break; }
                    }
                }
            }
        }

        let (tx, mut rx) = mpsc::channel::<WalRequest>(1024);

        let path_for_thread = path_owned.clone();
        std::thread::spawn(move || {
            tokio_uring::start(async move {
                let mut opts = std::fs::OpenOptions::new();
                opts.read(true).write(true).create(true);
                #[cfg(target_os = "linux")]
                opts.custom_flags(libc::O_DIRECT | libc::O_DSYNC);

                let std_file = opts
                    .open(&path_for_thread)
                    .expect("Failed to open WAL file (O_DIRECT)");
                let file = tokio_uring::fs::File::from_std(std_file);
                let mut offset = std::fs::metadata(&path_for_thread).map(|m| m.len()).unwrap_or(0);

                while let Some(req) = rx.recv().await {
                    match req {
                        WalRequest::Flush { batch, resp } => {
                            let size = batch.len();
                            let padded_size = (size + 4095) & !4095;
                            let mut aligned_buf = AlignedBuf::new(padded_size);
                            {
                                let slice = aligned_buf.as_mut_slice();
                                slice[..size].copy_from_slice(&batch);
                                if padded_size > size {
                                    slice[size..padded_size].fill(0);
                                }
                            }

                            let (res, _) = file.write_at(aligned_buf, offset).await;
                            match res {
                                Ok(n) => {
                                    offset += n as u64;
                                }
                                Err(e) => {
                                    eprintln!("[WAL] CRITICAL: Write failed at offset {}: {}", offset, e);
                                    // Even on error, we should probably move forward or at least not stuck
                                    offset += padded_size as u64;
                                }
                            }
                            let _ = file.sync_all().await;
                            let _ = resp.send(());
                        }
                    }
                }
            });
        });

        Ok(Wal {
            queue: Arc::new(ArrayQueue::new(1_000_000)),
            drain_queue: Arc::new(ArrayQueue::new(1_000_000)),
            tx,
            buffer_a: Mutex::new(Vec::with_capacity(128 * 1024 * 1024)),
            buffer_b: Mutex::new(Vec::with_capacity(128 * 1024 * 1024)),
            active_is_a: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            batch_limit: std::sync::atomic::AtomicUsize::new(32 * 1024 * 1024),
            recovered_entries: Mutex::new(recovered),
        })
    }

    pub fn set_batch_limit(&self, limit_bytes: usize) {
        self.batch_limit
            .store(limit_bytes, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn enqueue(&self, entry: WalEntry) -> Result<()> {
        let mut retries = 0;
        loop {
            match self.queue.push(entry.clone()) {
                Ok(_) => return Ok(()),
                Err(_) => {
                    if retries > 1000 {
                         return Err(anyhow::anyhow!("WAL Queue Saturated after 1000 retries"));
                    }
                    std::thread::sleep(std::time::Duration::from_micros(100));
                    retries += 1;
                }
            }
        }
    }

    pub async fn flush_pipeline(&self) -> Result<()> {
        loop {
            let q_len = self.queue.len();
            if q_len == 0 {
                break;
            }
            self.flush_once().await?;
        }
        Ok(())
    }

    async fn flush_once(&self) -> Result<()> {
        let q_len = self.queue.len();
        if q_len == 0 {
            return Ok(());
        }
        let is_a = self.active_is_a.load(std::sync::atomic::Ordering::Relaxed);
        let mut active = if is_a {
            self.buffer_a.lock().await
        } else {
            self.buffer_b.lock().await
        };

        let limit = self.batch_limit.load(std::sync::atomic::Ordering::Relaxed);
        let adaptive_limit = if q_len > 1_000_000 { limit * 4 } else { limit };

        let mut entries_to_drain = Vec::new();
        let mut count = 0;
        while let Some(entry) = self.queue.pop() {
            if let Ok(encoded) = bincode::serialize(&entry) {
                let len = encoded.len() as u32;
                active.extend_from_slice(&len.to_le_bytes());
                active.extend_from_slice(&encoded);
                entries_to_drain.push(entry);
                count += 1;
            }
            if active.len() >= adaptive_limit || count >= 1000 {
                break;
            }
        }

        if active.is_empty() {
            return Ok(());
        }

        let batch = std::mem::replace(&mut *active, Vec::with_capacity(limit));
        self.active_is_a
            .store(!is_a, std::sync::atomic::Ordering::Relaxed);

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let _ = self.tx.send(WalRequest::Flush { batch, resp: resp_tx }).await;
        let _ = resp_rx.await;

        for entry in entries_to_drain {
            let _ = self.drain_queue.push(entry);
        }

        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty() && self.drain_queue.is_empty()
    }

    pub async fn read_all(&self) -> Result<Vec<WalEntry>> {
        let mut recovered = self.recovered_entries.lock().await;
        Ok(std::mem::take(&mut *recovered))
    }

    pub fn pop_entry(&self) -> Option<WalEntry> {
        self.queue.pop()
    }
}
