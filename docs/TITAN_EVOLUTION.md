# Titan-Prime Evolution: Ultra-Scale Architecture (5+ Trillion Rows)

This document outlines the next-generation architectural overhaul for KS SQL, transitioning from a monolithic B+Tree engine to a distributed, isolated, and multi-tiered storage powerhouse.

## 🏛️ 1. Storage: Transition to Hybrid LSM-Tree
To resolve the trillions-row indexing paradox, Titan-Prime is evolving into a hybrid **LSM-Tree** (Log-Structured Merge-Tree) architecture.

- **MemTable (Write-Front):** All writes hit a lock-free SkipList MemTable, immediately persisted to the WAL.
- **SSTables (Disk-Back):** Cold data is flushed into immutable SSTables (Sorted String Tables) using a Leveled Compaction Strategy.
- **Sparse Indexing:** Instead of dense B+Tree pointers, we use sparse block-level indexing.
- **Bloom Filters:** Probabilistic data structures to skip unnecessary disk I/O for point lookups.

## ⚡ 2. Concurrency: Deterministic Transaction Sequencing
Smashing the OCC collision wall by moving toward deterministic execution for high-contention keys.

- **Sequencer Layer:** Incoming transactions are assigned a global sequence ID.
- **Locking Protocol:** Localized Pessimistic/Intent locking for overlapping key ranges; disjoint keys remain lock-free.
- **MVCC Isolation:** Readers utilize snapshot isolation, ensuring zero-block reads.

## 🧠 3. Tiered Memory Management
Strict segmentation of RAM to eliminate thrashing and maximize hardware cache lines.

- **Turbo Cache Pool (KV):** High-speed, lock-free DashMap for active records.
- **Index Cache Pool (ARC):** Adaptive Replacement Cache for SSTable metadata and Bloom filters.
- **Direct I/O Arena:** 4096-byte aligned transient buffers for O_DIRECT operations.
- **Predictive Prefetching:** User-space algorithm to warm the cache based on access patterns.

## 🌍 4. Portability Abstract Layer (PAL)
Breaking the Linux cage with a unified I/O trait.

| Platform | Backend |
| :--- | :--- |
| **Modern Linux** | `io_uring` (DMA / O_DIRECT) |
| **macOS** | `kqueue` (Standard Async) |
| **Windows** | `IOCP` (Standard Async) |
| **Fallback** | `Tokio` (Epoll / Standard I/O) |

## 🛡️ 5. Process Isolation (Decoupled Model)
Shrinking the blast radius by separating core components into distinct OS processes.

- **`ks-core`:** Raw storage, I/O, and transaction management (Privileged).
- **`ks-worker`:** WASM sandbox for stored procedures (Isolated, Memory-Capped).
- **`ks-dash`:** Unprivileged dashboard agent communicating via IPC.

---
**Status:** Evolution In Progress
**Target Rowcount:** 5,000,000,000,000+
