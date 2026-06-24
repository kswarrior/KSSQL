use crate::storage::columnar::{ColumnChunk, DataType};
use crate::storage::columnar::executor::VectorExecutor;

#[test]
fn test_columnar_simd_sum() {
    let mut data = Vec::new();
    for i in 0..100 {
        data.extend_from_slice(&(i as i64).to_le_bytes());
    }

    let chunk = ColumnChunk {
        column_id: 1,
        data_type: DataType::Int64,
        data,
        null_bitmap: Vec::new(),
    };

    let sum = VectorExecutor::sum_int64(&chunk).unwrap();
    // Sum of 0..99 = (99 * 100) / 2 = 4950
    assert_eq!(sum, 4950);
}

#[test]
fn test_columnar_filter() {
    let mut data = Vec::new();
    for i in 0..100 {
        data.extend_from_slice(&(i as i64).to_le_bytes());
    }

    let chunk = ColumnChunk {
        column_id: 1,
        data_type: DataType::Int64,
        data,
        null_bitmap: Vec::new(),
    };

    let indices = VectorExecutor::filter_gt_int64(&chunk, 90).unwrap();
    assert_eq!(indices.len(), 9); // 91, 92, 93, 94, 95, 96, 97, 98, 99
    assert_eq!(indices[0], 91);
}
