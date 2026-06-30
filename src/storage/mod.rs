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
use crossbeam_skiplist::SkipMap;

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

pub struct MemTable {
    pub map: SkipMap<Vec<u8>, Vec<u8>>,
    pub size: AtomicU64,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
            size: AtomicU64::new(0),
        }
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        self.size.fetch_add((key.len() + value.len()) as u64, Ordering::Relaxed);
        self.map.insert(key, value);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.get(key).map(|entry| entry.value().clone())
    }
}

pub struct TieredMemory {
    pub turbo_cache: Vec<DashMap<Vec<u8>, Vec<u8>>>, // 64-way sharded
    pub index_cache: DashMap<Vec<u8>, Vec<u8>>,
    pub columnar_cache: DashMap<Vec<u8>, Vec<u8>>,
    pub lru: DashMap<Vec<u8>, LruEntry>,
    pub metrics: MemoryMetrics,
    pub turbo_mode: Arc<AtomicU64>,
    pub max_ram_mb: Arc<AtomicU64>,
    pub memtable: Arc<MemTable>,
    pub dirty_pages: DashMap<u64, Vec<u8>>,
    pub turbo_len: AtomicU64,
}

impl TieredMemory {
    pub fn new(max_ram_mb: u64) -> Self {
        let mut turbo = Vec::with_capacity(64);
        for _ in 0..64 { turbo.push(DashMap::with_capacity(16384)); }
        Self {
            turbo_cache: turbo,
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
            dirty_pages: DashMap::new(),
            turbo_len: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    fn get_shard(&self, key: &[u8]) -> usize {
        let mut h = 0u64;
        for &b in key { h = h.wrapping_mul(31).wrapping_add(b as u64); }
        (h % 64) as usize
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Fast MemTable Check
        if let Some(val) = self.memtable.get(key) {
            return Some(val);
        }

        let shard = self.get_shard(key);
        if let Some(val) = self.turbo_cache[shard].get(key) {
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
        // Extremely Sampled LRU
        if self.metrics.hits.load(Ordering::Relaxed) % 1024 == 0 {
            if let Some(mut entry) = self.lru.get_mut(key) {
                entry.timestamp = Utc::now().timestamp_nanos_opt().unwrap_or(0);
            }
        }
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        let shard = self.get_shard(&key);
        if self.turbo_cache[shard].insert(key, value).is_none() {
            self.turbo_len.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn insert_kv(&self, key: Vec<u8>, value: Vec<u8>) {
        self.insert(key, value);
    }

    pub fn insert_batch(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) {
        for (key, value) in entries {
            self.insert(key, value);
        }
    }

    pub fn insert_with_priority(&self, key: Vec<u8>, value: Vec<u8>, priority: u32) {
        let total_ops = self.metrics.hits.load(Ordering::Relaxed) + self.metrics.misses.load(Ordering::Relaxed);

        if total_ops % 8192 == 0 {
            let max_bytes = self.max_ram_mb.load(Ordering::Relaxed) * 1024 * 1024;
            let total_len = self.turbo_len.load(Ordering::Relaxed) + self.index_cache.len() as u64 + self.columnar_cache.len() as u64;
            if total_len * 256 > max_bytes {
                self.evict_lru((total_len / 10) as usize);
            }
        }

        if total_ops % 128 == 0 {
            self.lru.insert(key.clone(), LruEntry {
                timestamp: Utc::now().timestamp_nanos_opt().unwrap_or(0),
                priority,
            });
        }

        if key.starts_with(&[0xFF]) {
            self.index_cache.insert(key, value);
        } else if key.starts_with(&[0xFE]) {
            self.columnar_cache.insert(key, value);
        } else {
            self.insert(key, value);
        }
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
            self.remove(key);
        }
    }

    pub fn remove(&self, key: &[u8]) {
        let shard = self.get_shard(key);
        if self.turbo_cache[shard].remove(key).is_some() {
            self.turbo_len.fetch_sub(1, Ordering::Relaxed);
        }
        self.index_cache.remove(key);
        self.columnar_cache.remove(key);
        self.lru.remove(key);
    }

    pub fn clear(&self) {
        for s in &self.turbo_cache { s.clear(); }
        self.turbo_len.store(0, Ordering::Relaxed);
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

#[cfg(test)]
mod tiered_tests;

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
