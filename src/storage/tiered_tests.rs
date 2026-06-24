use crate::storage::TieredMemory;
use std::sync::atomic::Ordering;

#[test]
fn test_tiered_memory_eviction() {
    // Small limit to trigger eviction quickly
    let mem = TieredMemory::new(1); // 1 MB
    mem.max_ram_mb.store(1, Ordering::SeqCst);

    // Insert many entries to exceed "estimated" size
    // Current logic: current_entries * 256 > max_bytes
    // 1 MB = 1048576 bytes. 1048576 / 256 = 4096 entries.

    for i in 0..5000 {
        let key = format!("key{}", i).into_bytes();
        let val = vec![0u8; 100];
        mem.insert(key, val);
    }

    // Should have evicted some
    assert!(mem.turbo_cache.len() < 5000);
    assert!(mem.lru.len() < 5000);
}

#[test]
fn test_tiered_memory_priority() {
    let mem = TieredMemory::new(1);

    // High priority key (index node)
    let index_key = vec![0xFF, 1, 2, 3];
    mem.insert_with_priority(index_key.clone(), vec![1, 2, 3], 10);

    // Low priority keys
    for i in 0..5000 {
        let key = format!("low{}", i).into_bytes();
        mem.insert_with_priority(key, vec![0], 0);
    }

    // Index key should ideally still be there because it has higher priority
    assert!(mem.index_cache.contains_key(&index_key));
}
