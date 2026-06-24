pub mod pager;
pub mod wal;
pub mod btree;
pub mod io;
pub mod columnar;

use sysinfo::System;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use chrono::Utc;
use rand::seq::IteratorRandom;
use std::collections::BTreeMap;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct MemoryMetrics {
    pub hits: Arc<AtomicU64>,
    pub misses: Arc<AtomicU64>,
}

#[derive(Clone, Copy)]
pub struct LruEntry {
    pub timestamp: i64,
    pub priority: u32,
}

/// LSM-Tree MemTable Foundation
pub struct MemTable {
    pub table: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
    pub size: AtomicU64,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            table: RwLock::new(BTreeMap::new()),
            size: AtomicU64::new(0),
        }
    }

    pub async fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut t = self.table.write().await;
        self.size.fetch_add((key.len() + value.len()) as u64, Ordering::Relaxed);
        t.insert(key, value);
    }

    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let t = self.table.read().await;
        t.get(key).cloned()
    }
}

/// A tiered memory management system for ultra-scale HTAP workloads
pub struct TieredMemory {
    pub turbo_cache: DashMap<Vec<u8>, Vec<u8>>,      // KV records (40%)
    pub index_cache: DashMap<Vec<u8>, Vec<u8>>,      // SSTable / Index metadata (20%)
    pub columnar_cache: DashMap<Vec<u8>, Vec<u8>>,   // HTAP Vectorized Chunks (30%)
    pub lru: DashMap<Vec<u8>, LruEntry>,
    pub metrics: MemoryMetrics,
    pub turbo_mode: Arc<AtomicU64>,
    pub max_ram_mb: Arc<AtomicU64>,
    pub memtable: Arc<MemTable>,
}

impl TieredMemory {
    pub fn new(max_ram_mb: u64) -> Self {
        Self {
            turbo_cache: DashMap::new(),
            index_cache: DashMap::new(),
            columnar_cache: DashMap::new(),
            lru: DashMap::new(),
            metrics: MemoryMetrics {
                hits: Arc::new(AtomicU64::new(0)),
                misses: Arc::new(AtomicU64::new(0)),
            },
            turbo_mode: Arc::new(AtomicU64::new(0)),
            max_ram_mb: Arc::new(AtomicU64::new(max_ram_mb)),
            memtable: Arc::new(MemTable::new()),
        }
    }

    /// Autopilot PID-based memory rebalancing logic foundation
    pub fn autopilot_rebalance(&self) {
        let hits = self.metrics.hits.load(Ordering::Relaxed);
        let misses = self.metrics.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total > 1000 {
             // Dynamic adjustment of cache targets would happen here
             // e.g., if index misses are high, increase index_cache budget
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(val) = self.turbo_cache.get(key) {
             self.hit(key);
             return Some(val.clone());
        }
        if let Some(val) = self.index_cache.get(key) {
             self.hit(key);
             return Some(val.clone());
        }
        if let Some(val) = self.columnar_cache.get(key) {
             self.hit(key);
             return Some(val.clone());
        }
        self.metrics.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    pub fn get_kv(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get(key)
    }

    fn hit(&self, key: &[u8]) {
        self.metrics.hits.fetch_add(1, Ordering::Relaxed);
        if let Some(mut entry) = self.lru.get_mut(key) {
            entry.timestamp = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        }
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        self.insert_with_priority(key, value, 0);
    }

    pub fn insert_kv(&self, key: Vec<u8>, value: Vec<u8>) {
        self.insert(key, value);
    }

    pub fn insert_with_priority(&self, key: Vec<u8>, value: Vec<u8>, priority: u32) {
        // Namespace mapping:
        // 0xFF: Index Cache
        // 0xFE: Columnar Chunk Cache
        // others: Turbo Cache (KV)
        let pool = if key.starts_with(&[0xFF]) {
            &self.index_cache
        } else if key.starts_with(&[0xFE]) {
            &self.columnar_cache
        } else {
            &self.turbo_cache
        };
        
        let max_bytes = self.max_ram_mb.load(Ordering::Relaxed) * 1024 * 1024;
        let current_entries = self.turbo_cache.len() + self.index_cache.len() + self.columnar_cache.len();
        if current_entries as u64 * 256 > max_bytes {
            self.evict_lru(current_entries / 10);
        }

        self.lru.insert(key.clone(), LruEntry {
            timestamp: Utc::now().timestamp_nanos_opt().unwrap_or(0),
            priority,
        });
        pool.insert(key, value);
    }

    fn evict_lru(&self, count: usize) {
        let mut rng = rand::thread_rng();
        let sample_size = (count * 3).max(100).min(self.lru.len());

        let mut items: Vec<(Vec<u8>, LruEntry)> = self.lru.iter()
            .choose_multiple(&mut rng, sample_size)
            .into_iter()
            .map(|r| (r.key().clone(), *r.value()))
            .collect();

        items.sort_by(|a, b| {
            match a.1.priority.cmp(&b.1.priority) {
                std::cmp::Ordering::Equal => a.1.timestamp.cmp(&b.1.timestamp),
                other => other,
            }
        });

        for i in 0..count.min(items.len()) {
            let key = &items[i].0;
            self.turbo_cache.remove(key);
            self.index_cache.remove(key);
            self.columnar_cache.remove(key);
            self.lru.remove(key);
        }
    }

    pub fn remove(&self, key: &[u8]) {
        self.turbo_cache.remove(key);
        self.index_cache.remove(key);
        self.columnar_cache.remove(key);
        self.lru.remove(key);
    }

    pub fn clear(&self) {
        self.turbo_cache.clear();
        self.index_cache.clear();
        self.columnar_cache.clear();
        self.lru.clear();
    }

    pub fn get_hit_ratio(&self) -> f64 {
        let hits = self.metrics.hits.load(Ordering::Relaxed);
        let misses = self.metrics.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 { 0.0 } else { (hits as f64 / total as f64) * 100.0 }
    }
}

// Keeping MemoryTier as a compatibility alias
pub type MemoryTier = TieredMemory;

pub struct HardwareSpecs {
    pub cpu_cores: usize,
    pub total_ram_mb: u64,
    pub available_ram_mb: u64,
    pub jet_buffer_size_mb: u64,
    pub writers: usize,
    pub readers: usize,
}

pub struct HardwareManager;

impl HardwareManager {
    pub fn scan() -> HardwareSpecs {
        let mut sys = System::new_all();
        sys.refresh_all();

        let cpu_cores = sys.cpus().len();
        let total_ram_mb = sys.total_memory() / 1024 / 1024;
        let available_ram_mb = sys.available_memory() / 1024 / 1024;
        
        let jet_buffer_size_mb = (available_ram_mb as f64 * 0.1) as u64;
        
        let writers = cpu_cores;
        let readers = cpu_cores * 4;

        HardwareSpecs {
            cpu_cores,
            total_ram_mb,
            available_ram_mb,
            jet_buffer_size_mb,
            writers,
            readers,
        }
    }
}
