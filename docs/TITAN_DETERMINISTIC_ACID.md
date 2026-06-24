# Titan-Prime Evolution: Deterministic ACID & Consensus

To ensure linear scalability across distributed nodes with zero lock contention, Titan-Prime is transitioning to a Calvin-inspired deterministic sequencing model.

## ⚖️ 1. Deterministic Sequencing
Incoming transactions are globally sequenced before they are allowed to touch the storage layer.

- **Lock-Ahead Protocol:** Analyze the transaction read/write sets and acquire all necessary locks deterministically in sequence ID order.
- **Deadlock Elimination:** Since locks are acquired in a strict, global sequence, circular wait conditions are impossible.
- **Disjoint Parallelism:** Transactions with non-overlapping key ranges execute concurrently without coordination.

## 🤝 2. Zero-Copy Raft Consensus
Replication is handled via a specialized Raft implementation optimized for `io_uring`.

- **Direct-to-Disk Log:** Raft log entries are streamed directly to NVMe via O_DIRECT, bypassing the core engine for replication durability.
- **Batching:** 5,000-entry batches from the WAL drainage pipeline are replicated as a single Raft unit.
- **Pipelining:** Overlap consensus agreement with local execution to maintain 6.0M+ ops/sec throughput.

---
**Component:** `src/consensus/`
**Standard:** Beyond 2PC (Zero-Wait Transactions)
