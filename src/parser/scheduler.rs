use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, oneshot};
use anyhow::Result;

pub enum SchedulerRequest {
    Acquire {
        tx_id: u64,
        read_set: HashSet<Vec<u8>>,
        write_set: HashSet<Vec<u8>>,
        resp: oneshot::Sender<Result<()>>,
    },
    Release {
        tx_id: u64,
        write_set: HashSet<Vec<u8>>,
    },
}

pub struct TransactionRequest {
    pub tx_id: u64,
    pub read_set: HashSet<Vec<u8>>,
    pub write_set: HashSet<Vec<u8>>,
    pub commit_tx: Option<oneshot::Sender<Result<()>>>,
}

pub struct DeterministicScheduler {
    tx: mpsc::Sender<SchedulerRequest>,
}

impl DeterministicScheduler {
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::channel::<SchedulerRequest>(10000);

        tokio::spawn(async move {
            let mut active_writes: HashMap<Vec<u8>, u64> = HashMap::new();
            let mut pending_queue: Vec<(u64, HashSet<Vec<u8>>, HashSet<Vec<u8>>, oneshot::Sender<Result<()>>)> = Vec::new();

            while let Some(req) = rx.recv().await {
                match req {
                    SchedulerRequest::Acquire { tx_id, read_set, write_set, resp } => {
                        let mut has_conflict = false;
                        for key in &read_set { if active_writes.contains_key(key) { has_conflict = true; break; } }
                        if !has_conflict {
                            for key in &write_set { if active_writes.contains_key(key) { has_conflict = true; break; } }
                        }

                        if has_conflict {
                            pending_queue.push((tx_id, read_set, write_set, resp));
                        } else {
                            for key in &write_set {
                                active_writes.insert(key.clone(), tx_id);
                            }
                            let _ = resp.send(Ok(()));
                        }
                    }
                    SchedulerRequest::Release { tx_id, write_set } => {
                        for key in &write_set {
                            if let Some(&owner) = active_writes.get(key) {
                                if owner == tx_id {
                                    active_writes.remove(key);
                                }
                            }
                        }

                        // After release, try to drain pending queue
                        let mut i = 0;
                        while i < pending_queue.len() {
                            let (p_tx_id, p_read, p_write, _) = &pending_queue[i];
                            let mut p_conflict = false;
                            for k in p_read { if active_writes.contains_key(k) { p_conflict = true; break; } }
                            if !p_conflict {
                                for k in p_write { if active_writes.contains_key(k) { p_conflict = true; break; } }
                            }

                            if !p_conflict {
                                let (tx_id, _, write_set, resp) = pending_queue.remove(i);
                                for k in &write_set { active_writes.insert(k.clone(), tx_id); }
                                let _ = resp.send(Ok(()));
                                // Reset drain loop to check all after new locks
                                i = 0;
                            } else {
                                i += 1;
                            }
                        }
                    }
                }
            }
        });

        Self { tx }
    }

    pub async fn acquire(&self, tx_id: u64, read_set: HashSet<Vec<u8>>, write_set: HashSet<Vec<u8>>) -> Result<oneshot::Receiver<Result<()>>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(SchedulerRequest::Acquire { tx_id, read_set, write_set, resp: tx }).await?;
        Ok(rx)
    }

    pub async fn release(&self, tx_id: u64, write_set: HashSet<Vec<u8>>) -> Result<()> {
        self.tx.send(SchedulerRequest::Release { tx_id, write_set }).await?;
        Ok(())
    }
}
