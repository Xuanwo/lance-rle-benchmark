use divan::{black_box, Bencher};
use lance_rle_benchmark::{data::*, lance, parquet};
use tokio::runtime::Runtime;

fn main() {
    divan::main();
}

const SIZES: &[usize] = &[1_000, 10_000, 100_000, 1_000_000];
const PATTERNS: &[DataPattern] = &[
    DataPattern::Runs { run_length: 100, unique_values: 10 },
    DataPattern::Sparse { density: 0.1 },
    DataPattern::Periodic { period: 100, amplitude: 1000 },
    DataPattern::Monotonic { step: 5 },
    DataPattern::Random,
];

#[divan::bench_group]
mod write {
    use super::*;

    #[divan::bench(name = "lance_bitpacking", consts = SIZES, args = PATTERNS)]
    fn lance_bitpacking<const N: usize>(bencher: Bencher, pattern: &DataPattern) {
        let rt = Runtime::new().unwrap();
        let batch = generate_record_batch(pattern, N);

        bencher
            .counter(divan::counter::BytesCount::new(N * std::mem::size_of::<i32>()))
            .bench_local(|| {
                rt.block_on(async {
                    let (temp_dir, _) = lance::write_file(batch.clone(), false, false).await;
                    black_box(temp_dir)
                })
            });
    }

    #[divan::bench(name = "lance_rle", consts = SIZES, args = PATTERNS)]
    fn lance_rle<const N: usize>(bencher: Bencher, pattern: &DataPattern) {
        let rt = Runtime::new().unwrap();
        let batch = generate_record_batch(pattern, N);

        bencher
            .counter(divan::counter::BytesCount::new(N * std::mem::size_of::<i32>()))
            .bench_local(|| {
                rt.block_on(async {
                    let (temp_dir, _) = lance::write_file(batch.clone(), false, true).await;
                    black_box(temp_dir)
                })
            });
    }

    #[divan::bench(name = "parquet", consts = SIZES, args = PATTERNS)]
    fn parquet<const N: usize>(bencher: Bencher, pattern: &DataPattern) {
        let batch = generate_record_batch(pattern, N);

        bencher
            .counter(divan::counter::BytesCount::new(N * std::mem::size_of::<i32>()))
            .bench_local(|| {
                let (temp_dir, _) = parquet::write_file(batch.clone());
                black_box(temp_dir)
            });
    }
}

#[divan::bench_group]
mod read {
    use super::*;

    #[divan::bench(name = "lance_bitpacking", consts = SIZES, args = PATTERNS)]
    fn lance_bitpacking<const N: usize>(bencher: Bencher, pattern: &DataPattern) {
        let rt = Runtime::new().unwrap();
        let batch = generate_record_batch(pattern, N);
        let (_temp_dir, uri) = rt.block_on(lance::write_file(batch.clone(), false, false));

        bencher
            .counter(divan::counter::BytesCount::new(N * std::mem::size_of::<i32>()))
            .bench_local(|| {
                rt.block_on(async {
                    let batches = lance::read_file(&uri).await;
                    black_box(batches)
                })
            });
    }

    #[divan::bench(name = "lance_rle", consts = SIZES, args = PATTERNS)]
    fn lance_rle<const N: usize>(bencher: Bencher, pattern: &DataPattern) {
        let rt = Runtime::new().unwrap();
        let batch = generate_record_batch(pattern, N);
        let (_temp_dir, uri) = rt.block_on(lance::write_file(batch.clone(), false, true));

        bencher
            .counter(divan::counter::BytesCount::new(N * std::mem::size_of::<i32>()))
            .bench_local(|| {
                rt.block_on(async {
                    let batches = lance::read_file(&uri).await;
                    black_box(batches)
                })
            });
    }

    #[divan::bench(name = "parquet", consts = SIZES, args = PATTERNS)]
    fn parquet<const N: usize>(bencher: Bencher, pattern: &DataPattern) {
        let batch = generate_record_batch(pattern, N);
        let (_temp_dir, path) = parquet::write_file(batch.clone());

        bencher
            .counter(divan::counter::BytesCount::new(N * std::mem::size_of::<i32>()))
            .bench_local(|| {
                let batches = parquet::read_file(&path);
                black_box(batches)
            });
    }
}

#[divan::bench_group]
mod take {
    use super::*;

    const TAKE_PATTERNS_AND_RATIOS: &[(DataPattern, f64)] = &[
        (DataPattern::Runs { run_length: 100, unique_values: 10 }, 0.01),
        (DataPattern::Runs { run_length: 100, unique_values: 10 }, 0.1),
        (DataPattern::Runs { run_length: 100, unique_values: 10 }, 0.5),
        (DataPattern::Sparse { density: 0.1 }, 0.01),
        (DataPattern::Sparse { density: 0.1 }, 0.1),
        (DataPattern::Random, 0.01),
        (DataPattern::Random, 0.1),
    ];

    #[divan::bench(name = "lance_bitpacking", consts = SIZES, args = TAKE_PATTERNS_AND_RATIOS)]
    fn lance_bitpacking<const N: usize>(bencher: Bencher, (pattern, ratio): &(DataPattern, f64)) {
        let rt = Runtime::new().unwrap();
        let batch = generate_record_batch(pattern, N);
        let (_temp_dir, uri) = rt.block_on(lance::write_file(batch, false, false));
        let indices = generate_indices(N, *ratio);

        bencher.counter(indices.len() as u64).bench_local(|| {
            rt.block_on(async {
                let result = lance::take_rows(&uri, &indices).await;
                black_box(result)
            })
        });
    }

    #[divan::bench(name = "lance_rle", consts = SIZES, args = TAKE_PATTERNS_AND_RATIOS)]
    fn lance_rle<const N: usize>(bencher: Bencher, (pattern, ratio): &(DataPattern, f64)) {
        let rt = Runtime::new().unwrap();
        let batch = generate_record_batch(pattern, N);
        let (_temp_dir, uri) = rt.block_on(lance::write_file(batch, false, true));
        let indices = generate_indices(N, *ratio);

        bencher.counter(indices.len() as u64).bench_local(|| {
            rt.block_on(async {
                let result = lance::take_rows(&uri, &indices).await;
                black_box(result)
            })
        });
    }

    #[divan::bench(name = "parquet", consts = SIZES, args = TAKE_PATTERNS_AND_RATIOS)]
    fn parquet<const N: usize>(bencher: Bencher, (pattern, ratio): &(DataPattern, f64)) {
        let batch = generate_record_batch(pattern, N);
        let (_temp_dir, path) = parquet::write_file(batch);
        let indices = generate_indices(N, *ratio);

        bencher.counter(indices.len() as u64).bench_local(|| {
            let result = parquet::take_rows(&path, &indices);
            black_box(result)
        });
    }
}