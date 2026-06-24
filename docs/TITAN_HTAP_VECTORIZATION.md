# Titan-Prime Evolution: Hybrid LSM-Columnar Engine (HTAP)

This document defines the storage and execution transition into a High-Performance Transactional/Analytical Processing (HTAP) engine.

## 📊 1. PAX Storage Format
Data is stored in SSTables using the **PAX (Partition Attributes Across)** layout.

- **RowGroups:** Large contiguous blocks of data (e.g., 64MB).
- **Columnar Pages:** Inside each RowGroup, attributes for the same column are stored contiguously.
- **Benefits:** Maximizes CPU cache locality for analytical scans while maintaining transaction-friendly ingestion via the LSM MemTable.

## ⚡ 2. Vectorized Execution
The execution engine is being re-engineered to operate on chunks of data (vectors) rather than individual rows.

- **Batch Processing:** Operators process batches of ~1024 values at a time.
- **SIMD Optimization:** Utilize compiler intrinsics (AVX-512, NEON) to parallelize primitive operations like filtering and aggregation.
- **Lazy Materialization:** Delay row reconstruction until the final output stage to minimize memory bandwidth usage.

## 🧠 3. Resource Allocation (RAM)
Strict segmentation ensures analytical scans do not evict transactional hot-spots.

- **Turbo Cache (40%):** KV records and active MemTable.
- **Index Cache (20%):** SSTable metadata, Bloom filters, and Sparse indexes.
- **Columnar Chunk Cache (30%):** Cached vectorized RowGroups for analytical dominance.
- **Direct I/O Arena (10%):** Aligned buffers for zero-copy NVMe streaming.

---
**Component:** `src/storage/columnar/`
**Goal:** Surpass ClickHouse Analytical Throughput
