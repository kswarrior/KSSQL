pub mod pager;
pub mod wal;
pub mod btree;

#[cfg(test)]
mod tests {
    use super::pager::Pager;
    use super::wal::Wal;
    use super::btree::BPlusTree;
    use std::fs;

    #[test]
    fn test_pager() {
        let path = "test_pager.ksql";
        {
            let mut pager = Pager::open(path).unwrap();
            let mut data = [0u8; 4096];
            data[0] = 1;
            data[4095] = 2;
            pager.write_page(0, &data).unwrap();
        }
        {
            let mut pager = Pager::open(path).unwrap();
            let data = pager.read_page(0).unwrap();
            assert_eq!(data[0], 1);
            assert_eq!(data[4095], 2);
        }
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_btree_basic() {
        let db_path = "test_btree.ksql";
        let wal_path = "test_btree.wal";
        {
            let mut btree = BPlusTree::open(db_path, wal_path).unwrap();
            btree.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
            btree.insert(b"key2".to_vec(), b"value2".to_vec()).unwrap();

            assert_eq!(btree.get(b"key1").unwrap(), Some(b"value1".to_vec()));
            assert_eq!(btree.get(b"key2").unwrap(), Some(b"value2".to_vec()));
            assert_eq!(btree.get(b"key3").unwrap(), None);
        }
        fs::remove_file(db_path).unwrap();
        fs::remove_file(wal_path).unwrap();
    }
}
