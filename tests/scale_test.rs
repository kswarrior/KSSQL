use ks_sql::storage::pager::Pager;
use std::fs;

#[test]
fn test_pager_u64_interface() {
    let path = "test_interface.ksql";
    let _ = fs::remove_file(path);

    // We use a reasonable page_id but use u64 type explicitly
    let page_id: u64 = 1000;

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async move {
        let pager = Pager::open(path).await.expect("Failed to open pager");

        let mut data = [0u8; 4096];
        data[4000..4004].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());

        pager.write_page(page_id, &data).await.expect("Failed to write page");

        let read_data = pager.read_page(page_id).await.expect("Failed to read page");

        assert_eq!(&read_data[4000..4004], &0xDEADBEEFu32.to_le_bytes());

        // Verify num_pages returns u64
        let n: u64 = pager.num_pages();
        assert!(n > 0);
    });

    let _ = fs::remove_file(path);
}
