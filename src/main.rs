use lance_rle_benchmark::{data::*, lance, parquet};
use tabled::{Table, Tabled};
use tokio::runtime::Runtime;

#[derive(Tabled)]
struct CompressionRow {
    pattern: String,
    size: usize,
    lance_size: usize,
    lance_ratio: f64,
    lance_rle_size: usize,
    lance_rle_ratio: f64,
    parquet_size: usize,
    parquet_ratio: f64,
}

fn main() {
    println!("\n=== Compression Ratio Summary ===");

    let rt = Runtime::new().unwrap();
    let mut rows = vec![];

    for pattern in [
        DataPattern::Runs {
            run_length: 100,
            unique_values: 10,
        },
        DataPattern::Sparse { density: 0.1 },
        DataPattern::Periodic {
            period: 100,
            amplitude: 1000,
        },
        DataPattern::Monotonic { step: 5 },
        DataPattern::Random,
    ] {
        for size in [1_000, 100_000, 1_000_000] {
            let batch = generate_record_batch(&pattern, size);
            let original_size = size * 4; // i32 = 4 bytes

            // Lance default (bitpacking)
            let lance_bytes = rt.block_on(lance::write_bytes(batch.clone(), false, false));
            let lance_size = lance_bytes.len();

            // Lance with RLE
            let lance_rle_bytes = rt.block_on(lance::write_bytes(batch.clone(), false, true));
            let lance_rle_size = lance_rle_bytes.len();

            // Parquet
            let parquet_bytes = parquet::write_bytes(batch);
            let parquet_size = parquet_bytes.len();

            rows.push(CompressionRow {
                pattern: pattern.name().to_string(),
                size,
                lance_size,
                lance_ratio: original_size as f64 / lance_size as f64,
                lance_rle_size,
                lance_rle_ratio: original_size as f64 / lance_rle_size as f64,
                parquet_size,
                parquet_ratio: original_size as f64 / parquet_size as f64,
            });
        }
    }

    let table = Table::new(rows);
    println!("{table}");
}
