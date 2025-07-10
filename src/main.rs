use arrow_array::RecordBatch;
use lance_rle_benchmark::{data::{generate_nested_record_batch, generate_flat_record_batch}, lance, parquet};
use tokio::runtime::Runtime;

struct CompressionRow {
    size: usize,
    lance: String,
    lance_rle: String,
    parquet: String,
}

fn test_schema(schema_name: &str, generate_fn: fn(usize) -> RecordBatch) {
    println!("\n### {}", schema_name);
    println!("Data pattern: 40% zeros, 40% common values, 20% random values");

    let rt = Runtime::new().unwrap();
    let mut rows = vec![];

    // Test with different row counts
    for num_rows in [1_000, 10_000, 100_000] {
        let batch = generate_fn(num_rows);
        // uuid: 8 bytes + features: 8 bytes * 3827
        let original_size = num_rows * (8 + 8 * 3827);

        // Lance default (bitpacking)
        let lance_bytes = rt.block_on(lance::write_bytes(batch.clone(), false, false));
        let lance_size = lance_bytes.len();

        // Lance with RLE
        let lance_rle_bytes = rt.block_on(lance::write_bytes(batch.clone(), false, true));
        let lance_rle_size = lance_rle_bytes.len();

        // Parquet
        let parquet_bytes = parquet::write_bytes(batch);
        let parquet_size = parquet_bytes.len();

        let lance_ratio_val = original_size as f64 / lance_size as f64;
        let lance_rle_ratio_val = original_size as f64 / lance_rle_size as f64;
        let parquet_ratio_val = original_size as f64 / parquet_size as f64;
        
        // Find the best compression ratio
        let best_ratio = lance_ratio_val.max(lance_rle_ratio_val).max(parquet_ratio_val);
        
        // Format size and ratio combined with best one marked
        let lance_str = if (lance_ratio_val - best_ratio).abs() < 0.0001 {
            format!("{} (**{:.2}x**)", lance_size, lance_ratio_val)
        } else {
            format!("{} ({:.2}x)", lance_size, lance_ratio_val)
        };
        
        let lance_rle_str = if (lance_rle_ratio_val - best_ratio).abs() < 0.0001 {
            format!("{} (**{:.2}x**)", lance_rle_size, lance_rle_ratio_val)
        } else {
            format!("{} ({:.2}x)", lance_rle_size, lance_rle_ratio_val)
        };
        
        let parquet_str = if (parquet_ratio_val - best_ratio).abs() < 0.0001 {
            format!("{} (**{:.2}x**)", parquet_size, parquet_ratio_val)
        } else {
            format!("{} ({:.2}x)", parquet_size, parquet_ratio_val)
        };
        
        rows.push(CompressionRow {
            size: num_rows,
            lance: lance_str,
            lance_rle: lance_rle_str,
            parquet: parquet_str,
        });
    }

    // Print markdown table header
    println!("\n| Rows | Lance (bitpacking) | Lance (RLE) | Parquet |");
    println!("|------|--------------------|-------------|---------|");
    
    // Print each row
    for row in rows {
        println!(
            "| {} | {} | {} | {} |",
            row.size,
            row.lance,
            row.lance_rle,
            row.parquet
        );
    }
}

fn main() {
    println!("\n=== RLE Compression Benchmark ===");
    
    // Test nested schema
    test_schema(
        "Nested Schema: uuid (int64) + features (struct with 3827 double fields)",
        generate_nested_record_batch
    );
    
    // Test flat schema
    test_schema(
        "Flat Schema: uuid (int64) + 3827 double columns",
        generate_flat_record_batch
    );
    
    println!("\n**Note**: Best compression ratio for each test is marked with **bold**.");
}
