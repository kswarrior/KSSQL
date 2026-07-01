#[cfg(target_os = "linux")]
use crate::storage::pager::AlignedBuf;
use anyhow::Result;
use crossbeam::queue::ArrayQueue;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WalEntry {
    PageUpdate {
        page_id: u64,
        data: Vec<u8>,
    },
    RecordUpdate {
        key: Vec<u8>,
        data: Vec<u8>,
    },
    RecordBatch {
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    },
    BinaryBatch {
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

pub struct Wal {
    queue: Arc<ArrayQueue<WalEntry>>,
    drain_queue: Arc<ArrayQueue<WalEntry>>,
    flush_tx: mpsc::Sender<()>,
    recovered_entries: Vec<WalEntry>,
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
                while cursor + 8 <= buffer.len() {
                    let checksum_bytes: [u8; 4] = buffer[cursor..cursor + 4].try_into().unwrap_or([0; 4]);
                    let stored_checksum = u32::from_le_bytes(checksum_bytes);

                    let len_bytes: [u8; 4] = buffer[cursor + 4..cursor + 8].try_into().unwrap_or([0; 4]);
                    let len = u32::from_le_bytes(len_bytes) as usize;

                    if len == 0 && stored_checksum == 0 {
                        // Skip zeros (O_DIRECT padding)
                        cursor = (cursor / 4096 + 1) * 4096;
                        if cursor >= buffer.len() { break; }
                        continue;
                    }

                    cursor += 8;
                    if cursor + len <= buffer.len() {
                        let data = &buffer[cursor..cursor + len];

                        // Verify checksum
                        let mut hasher = crc32fast::Hasher::new();
                        hasher.update(data);
                        if hasher.finalize() == stored_checksum {
                            if let Ok(entry) = bincode::deserialize::<WalEntry>(data) {
                                recovered.push(entry);
                            }
                        } else {
                            eprintln!("[WAL] Checksum mismatch at offset {}, skipping entry", cursor - 8);
                        }
                        cursor += len;
                    } else {
                        break;
                    }
                }
            }
        }

        let queue = Arc::new(ArrayQueue::new(5_000_000));
        let drain_queue = Arc::new(ArrayQueue::new(5_000_000));
        let (flush_tx, mut flush_rx) = mpsc::channel::<()>(1024);

        let q_clone = Arc::clone(&queue);
        let dq_clone = Arc::clone(&drain_queue);

        std::thread::spawn(move || {
            #[cfg(target_os = "linux")]
            {
                tokio_uring::start(async move {
                    let mut opts = std::fs::OpenOptions::new();
                    opts.read(true).write(true).create(true).append(true);
                    {
                        use std::os::unix::fs::OpenOptionsExt;
                        opts.custom_flags(libc::O_DIRECT);
                    }

                    let std_file = opts.open(&path_owned).expect("Failed to open WAL (O_DIRECT)");
                    let file = tokio_uring::fs::File::from_std(std_file);
                    let mut offset = std::fs::metadata(&path_owned).map(|m| m.len()).unwrap_or(0);

                    let mut batch_buf = Vec::with_capacity(64 * 1024 * 1024);
                    let mut aligned_buf = AlignedBuf::new(64 * 1024 * 1024);

                    loop {
                        tokio::select! {
                            _ = flush_rx.recv() => {},
                            _ = tokio::time::sleep(tokio::time::Duration::from_micros(10)) => {}
                        }

                        batch_buf.clear();
                        let mut entries = Vec::with_capacity(100_000);
                        while let Some(entry) = q_clone.pop() {
                            if let Ok(encoded) = bincode::serialize(&entry) {
                                let len = encoded.len() as u32;
                                let mut hasher = crc32fast::Hasher::new();
                                hasher.update(&encoded);
                                let checksum = hasher.finalize();

                                batch_buf.extend_from_slice(&checksum.to_le_bytes());
                                batch_buf.extend_from_slice(&len.to_le_bytes());
                                batch_buf.extend_from_slice(&encoded);
                                entries.push(entry);
                            }
                            if batch_buf.len() > 32 * 1024 * 1024 { break; }
                        }

                        if batch_buf.is_empty() { continue; }

                        let size = batch_buf.len();
                        let padded_size = (size + 4095) & !4095;
                        {
                            let slice = aligned_buf.as_mut_slice();
                            slice[..size].copy_from_slice(&batch_buf);
                            if padded_size > size { slice[size..padded_size].fill(0); }
                        }

                        let (res, buf_ret) = file.write_at(aligned_buf, offset).await;
                        aligned_buf = buf_ret;
                        if let Ok(n) = res {
                            offset += n as u64;
                            for entry in entries {
                                let _ = dq_clone.push(entry);
                            }
                        }
                    }
                });
            }

            #[cfg(not(target_os = "linux"))]
            {
                let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                rt.block_on(async move {
                    use std::io::Write;
                    let mut file = std::fs::OpenOptions::new().write(true).create(true).append(true).open(&path_owned).expect("Failed to open WAL");
                    let mut batch_buf = Vec::with_capacity(32 * 1024 * 1024);

                    loop {
                        tokio::select! {
                            _ = flush_rx.recv() => {},
                            _ = tokio::time::sleep(tokio::time::Duration::from_millis(1)) => {}
                        }

                        batch_buf.clear();
                        let mut entries = Vec::with_capacity(50_000);
                        while let Some(entry) = q_clone.pop() {
                            if let Ok(encoded) = bincode::serialize(&entry) {
                                let len = encoded.len() as u32;
                                let mut hasher = crc32fast::Hasher::new();
                                hasher.update(&encoded);
                                let checksum = hasher.finalize();

                                batch_buf.extend_from_slice(&checksum.to_le_bytes());
                                batch_buf.extend_from_slice(&len.to_le_bytes());
                                batch_buf.extend_from_slice(&encoded);
                                entries.push(entry);
                            }
                            if batch_buf.len() > 16 * 1024 * 1024 { break; }
                        }

                        if batch_buf.is_empty() { continue; }

                        if file.write_all(&batch_buf).is_ok() && file.sync_all().is_ok() {
                            for entry in entries {
                                let _ = dq_clone.push(entry);
                            }
                        }
                    }
                });
            }
        });

        Ok(Wal {
            queue,
            drain_queue,
            flush_tx,
            recovered_entries: recovered,
        })
    }

    pub fn enqueue(&self, mut entry: WalEntry) -> Result<()> {
        let mut retries = 0;
        loop {
            match self.queue.push(entry) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    entry = e;
                    if retries > 10000 {
                         return Err(anyhow::anyhow!("WAL Ingestion Queue Saturated (Titan-Prime High Backpressure)"));
                    }
                    std::thread::yield_now();
                    retries += 1;
                }
            }
        }
    }

    pub async fn flush_pipeline(&self) -> Result<()> {
        let _ = self.flush_tx.send(()).await;
        Ok(())
    }

    pub async fn read_all(&self) -> Result<Vec<WalEntry>> {
        Ok(self.recovered_entries.clone())
    }

    pub fn pop_entry(&self) -> Option<WalEntry> {
        self.drain_queue.pop()
    }

    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    pub fn drain_queue_len(&self) -> usize {
        self.drain_queue.len()
    }
}
