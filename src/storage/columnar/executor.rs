use crate::storage::columnar::ColumnChunk;
use anyhow::Result;

pub struct VectorExecutor;

impl VectorExecutor {
    /// SIMD-optimized column summation using manual loop unrolling and explicit alignment
    /// Optimized for LLVM to emit AVX-512 / NEON instructions.
    pub fn sum_int64(chunk: &ColumnChunk) -> Result<i64> {
        let data: &[i64] = unsafe {
            let (prefix, mid, suffix) = chunk.data.align_to::<i64>();
            if !prefix.is_empty() || !suffix.is_empty() {
                // Fallback for unaligned data
                return Ok(chunk.data.chunks_exact(8)
                    .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
                    .sum());
            }
            mid
        };

        // Manual unrolling to hint SIMD vectorization to the compiler
        let mut sum0 = 0i64;
        let mut sum1 = 0i64;
        let mut sum2 = 0i64;
        let mut sum3 = 0i64;

        let chunks = data.chunks_exact(4);
        let remainder = chunks.remainder();

        for c in chunks {
            sum0 += c[0];
            sum1 += c[1];
            sum2 += c[2];
            sum3 += c[3];
        }

        let mut total = sum0 + sum1 + sum2 + sum3;
        for &val in remainder {
            total += val;
        }

        Ok(total)
    }

    /// High-throughput analytical filter
    pub fn filter_gt_int64(chunk: &ColumnChunk, threshold: i64) -> Result<Vec<usize>> {
        let data: &[i64] = unsafe {
            let (prefix, mid, suffix) = chunk.data.align_to::<i64>();
             if !prefix.is_empty() || !suffix.is_empty() {
                // Slow path for unaligned data
                return Ok(chunk.data.chunks_exact(8)
                    .enumerate()
                    .filter(|(_, b)| i64::from_le_bytes((*b).try_into().unwrap()) > threshold)
                    .map(|(i, _)| i)
                    .collect());
            }
            mid
        };

        let mut indices = Vec::with_capacity(data.len());
        for (i, &val) in data.iter().enumerate() {
            if val > threshold {
                indices.push(i);
            }
        }

        Ok(indices)
    }
}
