pub mod pager;
pub mod wal;
pub mod btree;

use sysinfo::System;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use chrono::Utc;

#[derive(Clone)]
pub struct MemoryMetrics {
    pub hits: Arc<AtomicU64>,
    pub misses: Arc<AtomicU64>,
}

pub struct MemoryTier {
    pub cache: DashMap<Vec<u8>, Vec<u8>>,
    pub lru: DashMap<Vec<u8>, i64>,
    pub metrics: MemoryMetrics,
    pub turbo_mode: Arc<AtomicU64>, // 0 for OFF, 1 for ON
    pub max_ram_mb: Arc<AtomicU64>,
}

impl MemoryTier {
    pub fn new(max_ram_mb: u64) -> Self {
        Self {
            cache: DashMap::new(),
            lru: DashMap::new(),
            metrics: MemoryMetrics {
                hits: Arc::new(AtomicU64::new(0)),
                misses: Arc::new(AtomicU64::new(0)),
            },
            turbo_mode: Arc::new(AtomicU64::new(0)),
            max_ram_mb: Arc::new(AtomicU64::new(max_ram_mb)),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(val) = self.cache.get(key) {
            if self.turbo_mode.load(Ordering::Relaxed) == 1 {
                return Some(val.clone());
            }
            self.metrics.hits.fetch_add(1, Ordering::Relaxed);
            Some(val.clone())
        } else {
            if self.turbo_mode.load(Ordering::Relaxed) == 1 {
                return None;
            }
            self.metrics.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        let max_bytes = self.max_ram_mb.load(Ordering::Relaxed) * 1024 * 1024;
        let current_entries = self.cache.len();
        let estimated_size = current_entries as u64 * 256;
        
        if estimated_size > max_bytes && !self.cache.is_empty() {
            self.evict_lru(current_entries / 10);
        }

        self.lru.insert(key.clone(), Utc::now().timestamp_nanos_opt().unwrap_or(0));
        self.cache.insert(key, value);
    }

    fn evict_lru(&self, count: usize) {
        // Optimization: Use a sampling approach to avoid full sort on large caches
        let mut sample: Vec<(Vec<u8>, i64)> = Vec::with_capacity(count * 3);
        let mut iter = self.lru.iter();

        // Take a small sample of the cache
        for _ in 0..(count * 3) {
            if let Some(r) = iter.next() {
                sample.push((r.key().clone(), *r.value()));
            } else {
                break;
            }
        }

        // Sort only the sample
        sample.sort_by_key(|k| k.1);

        // Evict the oldest items from the sample
        for i in 0..count.min(sample.len()) {
            self.cache.remove(&sample[i].0);
            self.lru.remove(&sample[i].0);
        }
    }

    pub fn remove(&self, key: &[u8]) {
        self.cache.remove(key);
        self.lru.remove(key);
    }

    pub fn clear(&self) {
        self.cache.clear();
        self.lru.clear();
    }

    pub fn get_hit_ratio(&self) -> f64 {
        let hits = self.metrics.hits.load(Ordering::Relaxed);
        let misses = self.metrics.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 { 0.0 } else { (hits as f64 / total as f64) * 100.0 }
    }
}

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

    pub fn check_alerts(specs: &HardwareSpecs) {
        let mut sys = System::new_all();
        sys.refresh_all();
        let cpu_usage = sys.global_cpu_info().cpu_usage();
        let used_ram = (sys.total_memory() - sys.available_memory()) / 1024 / 1024;

        if cpu_usage > 90.0 {
            eprintln!("\x1b[31m[ALERT]\x1b[0m CPU CRITICAL: {:.2}%! Resource saturation imminent.", cpu_usage);
        }
        if used_ram > (specs.total_ram_mb * 90 / 100) {
            eprintln!("\x1b[31m[ALERT]\x1b[0m RAM CRITICAL: {}MB used! Automatic purging suggested.", used_ram);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::pager::Pager;
    use super::btree::BPlusTree;
    use std::fs;

    #[test]
    fn test_pager() {
        let path = "test_pager.ksql";
        let _ = fs::remove_file(path);
        tokio_uring::start(async move {
            {
                let pager = Pager::open(path).await.unwrap();
                let data = [0u8; 4096];
                pager.write_page(0, &data).await.unwrap();
            }
            {
                let pager = Pager::open(path).await.unwrap();
                let data = pager.read_page(0).await.unwrap();
                assert_eq!(data.len(), 4096);
            }
        });
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_btree_basic() {
        let db_path = "test_btree.ksql";
        let wal_path = "test_btree.wal";
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);
        let memory_tier = std::sync::Arc::new(super::MemoryTier::new(100));
        tokio_uring::start(async move {
            {
                let btree = BPlusTree::open(db_path, wal_path, memory_tier).await.unwrap();
                btree.insert(b"key1".to_vec(), b"value1".to_vec()).await.unwrap();
                assert_eq!(btree.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
            }
        });
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);
    }
}
