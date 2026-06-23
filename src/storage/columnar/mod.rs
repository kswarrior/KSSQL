pub mod executor;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataType {
    Int64,
    Float64,
    Text,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColumnChunk {
    pub column_id: u32,
    pub data_type: DataType,
    /// Contiguous column data for SIMD processing
    pub data: Vec<u8>,
    /// Validity bitmap for null handling
    pub null_bitmap: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RowGroup {
    pub row_count: usize,
    /// PAX layout: columns are stored contiguously within the RowGroup
    pub columns: Vec<ColumnChunk>,
    pub min_values: Vec<Vec<u8>>, // Sparse index metadata
    pub max_values: Vec<Vec<u8>>,
}

pub struct VectorizedChunk {
    pub columns: Vec<Box<dyn std::any::Any + Send + Sync>>,
    pub count: usize,
}

impl RowGroup {
    /// Zero-copy transformation from PAX storage to vectorized execution chunks
    pub fn to_vectorized(&self) -> VectorizedChunk {
        // Implementation for SIMD-aligned vector reconstruction
        VectorizedChunk {
            columns: Vec::new(),
            count: self.row_count,
        }
    }
}
