use lance_rle_benchmark::{data::*, lance, parquet};
use std::fs;
use std::path::Path;
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

            // Lance default
            let (_temp_dir, uri) = rt.block_on(lance::write_file(batch.clone(), false, false));
            let lance_size = get_dir_size(&uri);

            // Lance with RLE
            let (_temp_dir2, uri2) = rt.block_on(lance::write_file(batch.clone(), false, true));
            let lance_rle_size = get_dir_size(&uri2);

            // Parquet
            let (_temp_dir3, path) = parquet::write_file(batch);
            let parquet_size = fs::metadata(&path).map(|m| m.len() as usize).unwrap_or(0);

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

fn get_dir_size(path: &str) -> usize {
    let path = path.replace("file://", "");
    let path = Path::new(&path);

    if path.is_file() {
        fs::metadata(path).map(|m| m.len() as usize).unwrap_or(0)
    } else if path.is_dir() {
        fs::read_dir(path)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .map(|e| {
                        fs::metadata(e.path())
                            .map(|m| m.len() as usize)
                            .unwrap_or(0)
                    })
                    .sum()
            })
            .unwrap_or(0)
    } else {
        0
    }
}
