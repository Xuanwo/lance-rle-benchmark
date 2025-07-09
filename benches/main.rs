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

// Write benchmarks
bench_write!(
    write_1k_runs,
    "write/1k/runs",
    1_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    }
);
bench_write!(
    write_1k_sparse,
    "write/1k/sparse",
    1_000,
    DataPattern::Sparse { density: 0.1 }
);
bench_write!(
    write_1k_periodic,
    "write/1k/periodic",
    1_000,
    DataPattern::Periodic {
        period: 100,
        amplitude: 1000
    }
);
bench_write!(
    write_1k_monotonic,
    "write/1k/monotonic",
    1_000,
    DataPattern::Monotonic { step: 5 }
);
bench_write!(
    write_1k_random,
    "write/1k/random",
    1_000,
    DataPattern::Random
);

bench_write!(
    write_10k_runs,
    "write/10k/runs",
    10_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    }
);
bench_write!(
    write_10k_sparse,
    "write/10k/sparse",
    10_000,
    DataPattern::Sparse { density: 0.1 }
);
bench_write!(
    write_10k_periodic,
    "write/10k/periodic",
    10_000,
    DataPattern::Periodic {
        period: 100,
        amplitude: 1000
    }
);
bench_write!(
    write_10k_monotonic,
    "write/10k/monotonic",
    10_000,
    DataPattern::Monotonic { step: 5 }
);
bench_write!(
    write_10k_random,
    "write/10k/random",
    10_000,
    DataPattern::Random
);

bench_write!(
    write_100k_runs,
    "write/100k/runs",
    100_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    }
);
bench_write!(
    write_100k_sparse,
    "write/100k/sparse",
    100_000,
    DataPattern::Sparse { density: 0.1 }
);
bench_write!(
    write_100k_periodic,
    "write/100k/periodic",
    100_000,
    DataPattern::Periodic {
        period: 100,
        amplitude: 1000
    }
);
bench_write!(
    write_100k_monotonic,
    "write/100k/monotonic",
    100_000,
    DataPattern::Monotonic { step: 5 }
);
bench_write!(
    write_100k_random,
    "write/100k/random",
    100_000,
    DataPattern::Random
);

bench_write!(
    write_1m_runs,
    "write/1m/runs",
    1_000_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    }
);
bench_write!(
    write_1m_sparse,
    "write/1m/sparse",
    1_000_000,
    DataPattern::Sparse { density: 0.1 }
);
bench_write!(
    write_1m_periodic,
    "write/1m/periodic",
    1_000_000,
    DataPattern::Periodic {
        period: 100,
        amplitude: 1000
    }
);
bench_write!(
    write_1m_monotonic,
    "write/1m/monotonic",
    1_000_000,
    DataPattern::Monotonic { step: 5 }
);
bench_write!(
    write_1m_random,
    "write/1m/random",
    1_000_000,
    DataPattern::Random
);

// Read benchmarks
bench_read!(
    read_1k_runs,
    "read/1k/runs",
    1_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    }
);
bench_read!(
    read_1k_sparse,
    "read/1k/sparse",
    1_000,
    DataPattern::Sparse { density: 0.1 }
);
bench_read!(
    read_1k_periodic,
    "read/1k/periodic",
    1_000,
    DataPattern::Periodic {
        period: 100,
        amplitude: 1000
    }
);
bench_read!(
    read_1k_monotonic,
    "read/1k/monotonic",
    1_000,
    DataPattern::Monotonic { step: 5 }
);
bench_read!(read_1k_random, "read/1k/random", 1_000, DataPattern::Random);

bench_read!(
    read_10k_runs,
    "read/10k/runs",
    10_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    }
);
bench_read!(
    read_10k_sparse,
    "read/10k/sparse",
    10_000,
    DataPattern::Sparse { density: 0.1 }
);
bench_read!(
    read_10k_periodic,
    "read/10k/periodic",
    10_000,
    DataPattern::Periodic {
        period: 100,
        amplitude: 1000
    }
);
bench_read!(
    read_10k_monotonic,
    "read/10k/monotonic",
    10_000,
    DataPattern::Monotonic { step: 5 }
);
bench_read!(
    read_10k_random,
    "read/10k/random",
    10_000,
    DataPattern::Random
);

bench_read!(
    read_100k_runs,
    "read/100k/runs",
    100_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    }
);
bench_read!(
    read_100k_sparse,
    "read/100k/sparse",
    100_000,
    DataPattern::Sparse { density: 0.1 }
);
bench_read!(
    read_100k_periodic,
    "read/100k/periodic",
    100_000,
    DataPattern::Periodic {
        period: 100,
        amplitude: 1000
    }
);
bench_read!(
    read_100k_monotonic,
    "read/100k/monotonic",
    100_000,
    DataPattern::Monotonic { step: 5 }
);
bench_read!(
    read_100k_random,
    "read/100k/random",
    100_000,
    DataPattern::Random
);

bench_read!(
    read_1m_runs,
    "read/1m/runs",
    1_000_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    }
);
bench_read!(
    read_1m_sparse,
    "read/1m/sparse",
    1_000_000,
    DataPattern::Sparse { density: 0.1 }
);
bench_read!(
    read_1m_periodic,
    "read/1m/periodic",
    1_000_000,
    DataPattern::Periodic {
        period: 100,
        amplitude: 1000
    }
);
bench_read!(
    read_1m_monotonic,
    "read/1m/monotonic",
    1_000_000,
    DataPattern::Monotonic { step: 5 }
);
bench_read!(
    read_1m_random,
    "read/1m/random",
    1_000_000,
    DataPattern::Random
);

// Take benchmarks
bench_take!(
    take_1k_runs_1pct,
    "take/1k/runs/1%",
    1_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    },
    0.01
);
bench_take!(
    take_1k_runs_10pct,
    "take/1k/runs/10%",
    1_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    },
    0.1
);
bench_take!(
    take_1k_runs_50pct,
    "take/1k/runs/50%",
    1_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    },
    0.5
);
bench_take!(
    take_1k_sparse_1pct,
    "take/1k/sparse/1%",
    1_000,
    DataPattern::Sparse { density: 0.1 },
    0.01
);
bench_take!(
    take_1k_sparse_10pct,
    "take/1k/sparse/10%",
    1_000,
    DataPattern::Sparse { density: 0.1 },
    0.1
);
bench_take!(
    take_1k_random_1pct,
    "take/1k/random/1%",
    1_000,
    DataPattern::Random,
    0.01
);
bench_take!(
    take_1k_random_10pct,
    "take/1k/random/10%",
    1_000,
    DataPattern::Random,
    0.1
);

bench_take!(
    take_10k_runs_1pct,
    "take/10k/runs/1%",
    10_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    },
    0.01
);
bench_take!(
    take_10k_runs_10pct,
    "take/10k/runs/10%",
    10_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    },
    0.1
);
bench_take!(
    take_10k_runs_50pct,
    "take/10k/runs/50%",
    10_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    },
    0.5
);
bench_take!(
    take_10k_sparse_1pct,
    "take/10k/sparse/1%",
    10_000,
    DataPattern::Sparse { density: 0.1 },
    0.01
);
bench_take!(
    take_10k_sparse_10pct,
    "take/10k/sparse/10%",
    10_000,
    DataPattern::Sparse { density: 0.1 },
    0.1
);
bench_take!(
    take_10k_random_1pct,
    "take/10k/random/1%",
    10_000,
    DataPattern::Random,
    0.01
);
bench_take!(
    take_10k_random_10pct,
    "take/10k/random/10%",
    10_000,
    DataPattern::Random,
    0.1
);

bench_take!(
    take_100k_runs_1pct,
    "take/100k/runs/1%",
    100_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    },
    0.01
);
bench_take!(
    take_100k_runs_10pct,
    "take/100k/runs/10%",
    100_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    },
    0.1
);
bench_take!(
    take_100k_runs_50pct,
    "take/100k/runs/50%",
    100_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    },
    0.5
);
bench_take!(
    take_100k_sparse_1pct,
    "take/100k/sparse/1%",
    100_000,
    DataPattern::Sparse { density: 0.1 },
    0.01
);
bench_take!(
    take_100k_sparse_10pct,
    "take/100k/sparse/10%",
    100_000,
    DataPattern::Sparse { density: 0.1 },
    0.1
);
bench_take!(
    take_100k_random_1pct,
    "take/100k/random/1%",
    100_000,
    DataPattern::Random,
    0.01
);
bench_take!(
    take_100k_random_10pct,
    "take/100k/random/10%",
    100_000,
    DataPattern::Random,
    0.1
);

bench_take!(
    take_1m_runs_1pct,
    "take/1m/runs/1%",
    1_000_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    },
    0.01
);
bench_take!(
    take_1m_runs_10pct,
    "take/1m/runs/10%",
    1_000_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    },
    0.1
);
bench_take!(
    take_1m_runs_50pct,
    "take/1m/runs/50%",
    1_000_000,
    DataPattern::Runs {
        run_length: 100,
        unique_values: 10
    },
    0.5
);
bench_take!(
    take_1m_sparse_1pct,
    "take/1m/sparse/1%",
    1_000_000,
    DataPattern::Sparse { density: 0.1 },
    0.01
);
bench_take!(
    take_1m_sparse_10pct,
    "take/1m/sparse/10%",
    1_000_000,
    DataPattern::Sparse { density: 0.1 },
    0.1
);
bench_take!(
    take_1m_random_1pct,
    "take/1m/random/1%",
    1_000_000,
    DataPattern::Random,
    0.01
);
bench_take!(
    take_1m_random_10pct,
    "take/1m/random/10%",
    1_000_000,
    DataPattern::Random,
    0.1
);
