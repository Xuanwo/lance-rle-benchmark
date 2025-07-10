use divan::{black_box, Bencher};
use lance_rle_benchmark::{data::*, lance, parquet};
use tokio::runtime::Runtime;

fn main() {
    divan::main();
}

macro_rules! bench_write {
    ($mod_name:ident, $group_name:expr, $size:expr) => {
        #[divan::bench_group(name = $group_name)]
        mod $mod_name {
            use super::*;
            const N: usize = $size;

            #[divan::bench]
            fn lance_bitpacking(bencher: Bencher) {
                let rt = Runtime::new().unwrap();
                let batch = generate_record_batch(N);
                bencher
                    .counter(divan::counter::BytesCount::new(
                        N * (8 + 8 * 3827), // uuid + 3827 double features
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
                let batch = generate_record_batch(N);
                bencher
                    .counter(divan::counter::BytesCount::new(
                        N * (8 + 8 * 3827), // uuid + 3827 double features
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
                let batch = generate_record_batch(N);
                bencher
                    .counter(divan::counter::BytesCount::new(
                        N * (8 + 8 * 3827), // uuid + 3827 double features
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
    ($mod_name:ident, $group_name:expr, $size:expr) => {
        #[divan::bench_group(name = $group_name)]
        mod $mod_name {
            use super::*;
            const N: usize = $size;

            #[divan::bench]
            fn lance_bitpacking(bencher: Bencher) {
                let rt = Runtime::new().unwrap();
                let batch = generate_record_batch(N);
                let bytes = rt.block_on(lance::write_bytes(batch.clone(), false, false));
                bencher
                    .counter(divan::counter::BytesCount::new(
                        N * (8 + 8 * 3827), // uuid + 3827 double features
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
                let batch = generate_record_batch(N);
                let bytes = rt.block_on(lance::write_bytes(batch.clone(), false, true));
                bencher
                    .counter(divan::counter::BytesCount::new(
                        N * (8 + 8 * 3827), // uuid + 3827 double features
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
                let batch = generate_record_batch(N);
                let bytes = parquet::write_bytes(batch.clone());
                bencher
                    .counter(divan::counter::BytesCount::new(
                        N * (8 + 8 * 3827), // uuid + 3827 double features
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
    ($mod_name:ident, $group_name:expr, $size:expr) => {
        #[divan::bench_group(name = $group_name)]
        mod $mod_name {
            use super::*;
            const N: usize = $size;

            #[divan::bench]
            fn lance_bitpacking(bencher: Bencher) {
                let rt = Runtime::new().unwrap();
                let batch = generate_record_batch(N);
                let bytes = rt.block_on(lance::write_bytes(batch, false, false));
                let indices = vec![N / 2]; // Take single row from middle
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
                let batch = generate_record_batch(N);
                let bytes = rt.block_on(lance::write_bytes(batch, false, true));
                let indices = vec![N / 2]; // Take single row from middle
                bencher.counter(indices.len() as u64).bench_local(|| {
                    rt.block_on(async {
                        let result = lance::take_rows_from_bytes(&bytes, &indices).await;
                        black_box(result)
                    })
                });
            }

            #[divan::bench]
            fn parquet(bencher: Bencher) {
                let batch = generate_record_batch(N);
                let bytes = parquet::write_bytes(batch);
                let indices = vec![N / 2]; // Take single row from middle
                bencher.counter(indices.len() as u64).bench_local(|| {
                    let result = parquet::take_rows_from_bytes(&bytes, &indices);
                    black_box(result)
                });
            }
        }
    };
}

// Write benchmarks with realistic data
bench_write!(write_1k, "write/1k_rows", 1_000);
bench_write!(write_10k, "write/10k_rows", 10_000);
bench_write!(write_100k, "write/100k_rows", 100_000);

// Read benchmarks with realistic data
bench_read!(read_1k, "read/1k_rows", 1_000);
bench_read!(read_10k, "read/10k_rows", 10_000);
bench_read!(read_100k, "read/100k_rows", 100_000);

// Take benchmarks with realistic data
bench_take!(take_1k, "take/1k_rows/single", 1_000);
bench_take!(take_10k, "take/10k_rows/single", 10_000);
bench_take!(take_100k, "take/100k_rows/single", 100_000);