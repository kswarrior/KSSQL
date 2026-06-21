use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, oneshot};
use anyhow::Result;

pub struct TransactionRequest {
    pub tx_id: u64,
    pub read_set: HashSet<Vec<u8>>,
    pub write_set: HashSet<Vec<u8>>,
    pub commit_tx: Option<oneshot::Sender<Result<()>>>,
}

pub struct DeterministicScheduler {
    tx: mpsc::Sender<TransactionRequest>,
}

impl DeterministicScheduler {
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::channel::<TransactionRequest>(10000);

        tokio::spawn(async move {
            let mut active_writes: HashMap<Vec<u8>, u64> = HashMap::new();
            let mut pending_queue: Vec<TransactionRequest> = Vec::new();

            while let Some(mut req) = rx.recv().await {
                let mut has_conflict = false;
                for key in &req.read_set {
                    if active_writes.contains_key(key) {
                        has_conflict = true;
                        break;
                    }
                }
                if !has_conflict {
                    for key in &req.write_set {
                        if active_writes.contains_key(key) {
                            has_conflict = true;
                            break;
                        }
                    }
                }

                if has_conflict {
                    pending_queue.push(req);
                } else {
                    for key in &req.write_set {
                        active_writes.insert(key.clone(), req.tx_id);
                    }
                    if let Some(chan) = req.commit_tx.take() {
                        let _ = chan.send(Ok(()));
                    }
                }

                if pending_queue.len() > 10 {
                    let mut i = 0;
                    while i < pending_queue.len() {
                        let mut p_conflict = false;
                        for k in &pending_queue[i].read_set { if active_writes.contains_key(k) { p_conflict = true; break; } }
                        if !p_conflict {
                            for k in &pending_queue[i].write_set { if active_writes.contains_key(k) { p_conflict = true; break; } }
                        }

                        if !p_conflict {
                            let mut p = pending_queue.remove(i);
                            for k in &p.write_set { active_writes.insert(k.clone(), p.tx_id); }
                            if let Some(chan) = p.commit_tx.take() {
                                let _ = chan.send(Ok(()));
                            }
                        } else {
                            i += 1;
                        }
                    }
                }

                if active_writes.len() > 5000 {
                    active_writes.clear();
                }
            }
        });

        Self { tx }
    }

    pub async fn schedule(&self, req: TransactionRequest) -> Result<()> {
        self.tx.send(req).await?;
        Ok(())
    }
}
