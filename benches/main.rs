use divan::{black_box, Bencher};
use lance_rle_benchmark::{data::*, lance, parquet};
use tokio::runtime::Runtime;

fn main() {
    divan::main();
}

macro_rules! bench_write {
    ($mod_name:ident, $group_name:expr, $size:expr, $pattern:expr) => {
        #[divan::bench_group(name = $group_name)]
        mod $mod_name {
            use super::*;
            const N: usize = $size;
            const PATTERN: DataPattern = $pattern;

            #[divan::bench]
            fn lance_bitpacking(bencher: Bencher) {
                let rt = Runtime::new().unwrap();
                let batch = generate_record_batch(&PATTERN, N);
                bencher
                    .counter(divan::counter::BytesCount::new(
                        N * std::mem::size_of::<i32>(),
                    ))
                    .bench_local(|| {
                        rt.block_on(async {
                            let bytes = lance::write_bytes(batch.clone(), false, false).await;
                            black_box(bytes)
                        })
                    });
            }

            #[divan::bench]
            fn lance_rle(bencher: Bencher) {
                let rt = Runtime::new().unwrap();
                let batch = generate_record_batch(&PATTERN, N);
                bencher
                    .counter(divan::counter::BytesCount::new(
                        N * std::mem::size_of::<i32>(),
                    ))
                    .bench_local(|| {
                        rt.block_on(async {
                            let bytes = lance::write_bytes(batch.clone(), false, true).await;
                            black_box(bytes)
                        })
                    });
            }

            #[divan::bench]
            fn parquet(bencher: Bencher) {
                let batch = generate_record_batch(&PATTERN, N);
                bencher
                    .counter(divan::counter::BytesCount::new(
                        N * std::mem::size_of::<i32>(),
                    ))
                    .bench_local(|| {
                        let bytes = parquet::write_bytes(batch.clone());
                        black_box(bytes)
                    });
            }
        }
    };
}

macro_rules! bench_read {
    ($mod_name:ident, $group_name:expr, $size:expr, $pattern:expr) => {
        #[divan::bench_group(name = $group_name)]
        mod $mod_name {
            use super::*;
            const N: usize = $size;
            const PATTERN: DataPattern = $pattern;

            #[divan::bench]
            fn lance_bitpacking(bencher: Bencher) {
                let rt = Runtime::new().unwrap();
                let batch = generate_record_batch(&PATTERN, N);
                let bytes = rt.block_on(lance::write_bytes(batch.clone(), false, false));
                bencher
                    .counter(divan::counter::BytesCount::new(
                        N * std::mem::size_of::<i32>(),
                    ))
                    .bench_local(|| {
                        rt.block_on(async {
                            let batches = lance::read_bytes(&bytes).await;
                            black_box(batches)
                        })
                    });
            }

            #[divan::bench]
            fn lance_rle(bencher: Bencher) {
                let rt = Runtime::new().unwrap();
                let batch = generate_record_batch(&PATTERN, N);
                let bytes = rt.block_on(lance::write_bytes(batch.clone(), false, true));
                bencher
                    .counter(divan::counter::BytesCount::new(
                        N * std::mem::size_of::<i32>(),
                    ))
                    .bench_local(|| {
                        rt.block_on(async {
                            let batches = lance::read_bytes(&bytes).await;
                            black_box(batches)
                        })
                    });
            }

            #[divan::bench]
            fn parquet(bencher: Bencher) {
                let batch = generate_record_batch(&PATTERN, N);
                let bytes = parquet::write_bytes(batch.clone());
                bencher
                    .counter(divan::counter::BytesCount::new(
                        N * std::mem::size_of::<i32>(),
                    ))
                    .bench_local(|| {
                        let batches = parquet::read_bytes(&bytes);
                        black_box(batches)
                    });
            }
        }
    };
}

macro_rules! bench_take {
    ($mod_name:ident, $group_name:expr, $size:expr, $pattern:expr, $ratio:expr) => {
        #[divan::bench_group(name = $group_name)]
        mod $mod_name {
            use super::*;
            const N: usize = $size;
            const PATTERN: DataPattern = $pattern;
            const RATIO: f64 = $ratio;

            #[divan::bench]
            fn lance_bitpacking(bencher: Bencher) {
                let rt = Runtime::new().unwrap();
                let batch = generate_record_batch(&PATTERN, N);
                let bytes = rt.block_on(lance::write_bytes(batch, false, false));
                let indices = generate_indices(N, RATIO);
                bencher.counter(indices.len() as u64).bench_local(|| {
                    rt.block_on(async {
                        let result = lance::take_rows_from_bytes(&bytes, &indices).await;
                        black_box(result)
                    })
                });
            }

            #[divan::bench]
            fn lance_rle(bencher: Bencher) {
                let rt = Runtime::new().unwrap();
                let batch = generate_record_batch(&PATTERN, N);
                let bytes = rt.block_on(lance::write_bytes(batch, false, true));
                let indices = generate_indices(N, RATIO);
                bencher.counter(indices.len() as u64).bench_local(|| {
                    rt.block_on(async {
                        let result = lance::take_rows_from_bytes(&bytes, &indices).await;
                        black_box(result)
                    })
                });
            }

            #[divan::bench]
            fn parquet(bencher: Bencher) {
                let batch = generate_record_batch(&PATTERN, N);
                let bytes = parquet::write_bytes(batch);
                let indices = generate_indices(N, RATIO);
                bencher.counter(indices.len() as u64).bench_local(|| {
                    let result = parquet::take_rows_from_bytes(&bytes, &indices);
                    black_box(result)
                });
            }
        }
    };
}

// High repetition patterns
const HIGH_REP_99: DataPattern = DataPattern::HighRepetition {
    repetition_rate: 0.99,
    run_length: 1000,
};

const HIGH_REP_95: DataPattern = DataPattern::HighRepetition {
    repetition_rate: 0.95,
    run_length: 100,
};

const HIGH_REP_90: DataPattern = DataPattern::HighRepetition {
    repetition_rate: 0.90,
    run_length: 10,
};

// Low repetition patterns
const LOW_REP_100: DataPattern = DataPattern::LowRepetition { unique_ratio: 1.0 };
const LOW_REP_90: DataPattern = DataPattern::LowRepetition { unique_ratio: 0.90 };
const LOW_REP_50: DataPattern = DataPattern::LowRepetition { unique_ratio: 0.50 };

// Write benchmarks - 100K elements
bench_write!(write_100k_high_rep_99, "write/100k/high_rep_99pct", 100_000, HIGH_REP_99);
bench_write!(write_100k_high_rep_95, "write/100k/high_rep_95pct", 100_000, HIGH_REP_95);
bench_write!(write_100k_high_rep_90, "write/100k/high_rep_90pct", 100_000, HIGH_REP_90);
bench_write!(write_100k_low_rep_100, "write/100k/low_rep_100pct", 100_000, LOW_REP_100);
bench_write!(write_100k_low_rep_90, "write/100k/low_rep_90pct", 100_000, LOW_REP_90);
bench_write!(write_100k_low_rep_50, "write/100k/low_rep_50pct", 100_000, LOW_REP_50);

// Write benchmarks - 1M elements
bench_write!(write_1m_high_rep_99, "write/1m/high_rep_99pct", 1_000_000, HIGH_REP_99);
bench_write!(write_1m_high_rep_95, "write/1m/high_rep_95pct", 1_000_000, HIGH_REP_95);
bench_write!(write_1m_high_rep_90, "write/1m/high_rep_90pct", 1_000_000, HIGH_REP_90);
bench_write!(write_1m_low_rep_100, "write/1m/low_rep_100pct", 1_000_000, LOW_REP_100);
bench_write!(write_1m_low_rep_90, "write/1m/low_rep_90pct", 1_000_000, LOW_REP_90);
bench_write!(write_1m_low_rep_50, "write/1m/low_rep_50pct", 1_000_000, LOW_REP_50);

// Read benchmarks - 100K elements
bench_read!(read_100k_high_rep_99, "read/100k/high_rep_99pct", 100_000, HIGH_REP_99);
bench_read!(read_100k_high_rep_95, "read/100k/high_rep_95pct", 100_000, HIGH_REP_95);
bench_read!(read_100k_high_rep_90, "read/100k/high_rep_90pct", 100_000, HIGH_REP_90);
bench_read!(read_100k_low_rep_100, "read/100k/low_rep_100pct", 100_000, LOW_REP_100);
bench_read!(read_100k_low_rep_90, "read/100k/low_rep_90pct", 100_000, LOW_REP_90);
bench_read!(read_100k_low_rep_50, "read/100k/low_rep_50pct", 100_000, LOW_REP_50);

// Read benchmarks - 1M elements
bench_read!(read_1m_high_rep_99, "read/1m/high_rep_99pct", 1_000_000, HIGH_REP_99);
bench_read!(read_1m_high_rep_95, "read/1m/high_rep_95pct", 1_000_000, HIGH_REP_95);
bench_read!(read_1m_high_rep_90, "read/1m/high_rep_90pct", 1_000_000, HIGH_REP_90);
bench_read!(read_1m_low_rep_100, "read/1m/low_rep_100pct", 1_000_000, LOW_REP_100);
bench_read!(read_1m_low_rep_90, "read/1m/low_rep_90pct", 1_000_000, LOW_REP_90);
bench_read!(read_1m_low_rep_50, "read/1m/low_rep_50pct", 1_000_000, LOW_REP_50);

// Take benchmarks - 100K elements, single row
bench_take!(take_100k_high_rep_99, "take/100k/high_rep_99pct/single", 100_000, HIGH_REP_99, 0.1);
bench_take!(take_100k_high_rep_95, "take/100k/high_rep_95pct/single", 100_000, HIGH_REP_95, 0.1);
bench_take!(take_100k_high_rep_90, "take/100k/high_rep_90pct/single", 100_000, HIGH_REP_90, 0.1);
bench_take!(take_100k_low_rep_100, "take/100k/low_rep_100pct/single", 100_000, LOW_REP_100, 0.1);
bench_take!(take_100k_low_rep_90, "take/100k/low_rep_90pct/single", 100_000, LOW_REP_90, 0.1);
bench_take!(take_100k_low_rep_50, "take/100k/low_rep_50pct/single", 100_000, LOW_REP_50, 0.1);

// Take benchmarks - 1M elements, single row
bench_take!(take_1m_high_rep_99, "take/1m/high_rep_99pct/single", 1_000_000, HIGH_REP_99, 0.1);
bench_take!(take_1m_high_rep_95, "take/1m/high_rep_95pct/single", 1_000_000, HIGH_REP_95, 0.1);
bench_take!(take_1m_high_rep_90, "take/1m/high_rep_90pct/single", 1_000_000, HIGH_REP_90, 0.1);
bench_take!(take_1m_low_rep_100, "take/1m/low_rep_100pct/single", 1_000_000, LOW_REP_100, 0.1);
bench_take!(take_1m_low_rep_90, "take/1m/low_rep_90pct/single", 1_000_000, LOW_REP_90, 0.1);
bench_take!(take_1m_low_rep_50, "take/1m/low_rep_50pct/single", 1_000_000, LOW_REP_50, 0.1);