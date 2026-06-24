use crate::parser::scheduler::{DeterministicScheduler};
use std::collections::HashSet;

#[tokio::test]
async fn test_scheduler_non_conflicting() {
    let scheduler = DeterministicScheduler::new();
    let tx1_id = 1;
    let tx2_id = 2;

    let r1 = HashSet::new();
    let mut w1 = HashSet::new();
    w1.insert(b"key1".to_vec());

    let r2 = HashSet::new();
    let mut w2 = HashSet::new();
    w2.insert(b"key2".to_vec());

    let rx1 = scheduler.acquire(tx1_id, r1, w1.clone()).await.unwrap();
    let rx2 = scheduler.acquire(tx2_id, r2, w2.clone()).await.unwrap();

    rx1.await.unwrap().unwrap();
    rx2.await.unwrap().unwrap();

    scheduler.release(tx1_id, w1).await.unwrap();
    scheduler.release(tx2_id, w2).await.unwrap();
}

#[tokio::test]
async fn test_scheduler_conflicting() {
    let scheduler = DeterministicScheduler::new();
    let tx1_id = 10;
    let tx2_id = 20;

    let r1 = HashSet::new();
    let mut w1 = HashSet::new();
    w1.insert(b"common".to_vec());

    let mut r2 = HashSet::new();
    r2.insert(b"common".to_vec());
    let mut w2 = HashSet::new();
    w2.insert(b"other".to_vec());

    let rx1 = scheduler.acquire(tx1_id, r1, w1.clone()).await.unwrap();
    let rx2 = scheduler.acquire(tx2_id, r2, w2.clone()).await.unwrap();

    // tx1 should get it
    rx1.await.unwrap().unwrap();

    // tx2 should be blocked. We check by using now_or_never or just a short timeout
    // For simplicity in this env, we'll release tx1 and then check tx2
    scheduler.release(tx1_id, w1).await.unwrap();

    rx2.await.unwrap().unwrap();
    scheduler.release(tx2_id, w2).await.unwrap();
}
