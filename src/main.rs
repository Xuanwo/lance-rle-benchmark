use lance_rle_benchmark::{data::*, lance, parquet};
use tokio::runtime::Runtime;

struct CompressionRow {
    pattern: String,
    size: usize,
    lance: String,
    lance_rle: String,
    parquet: String,
}

fn main() {
    println!("\n=== Compression Ratio Summary ===");

    let rt = Runtime::new().unwrap();
    let mut rows = vec![];

    for pattern in [
        // High repetition cases - ideal for RLE
        DataPattern::HighRepetition {
            repetition_rate: 0.99,  // 99% repeated values
            run_length: 1000,       // Long runs
        },
        DataPattern::HighRepetition {
            repetition_rate: 0.95,  // 95% repeated values
            run_length: 100,        // Medium runs
        },
        DataPattern::HighRepetition {
            repetition_rate: 0.90,  // 90% repeated values
            run_length: 10,         // Short runs
        },
        
        // Low repetition cases - worst for RLE
        DataPattern::LowRepetition {
            unique_ratio: 1.0,      // 100% unique values
        },
        DataPattern::LowRepetition {
            unique_ratio: 0.90,     // 90% unique values
        },
        DataPattern::LowRepetition {
            unique_ratio: 0.50,     // 50% unique values
        },
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
                pattern: pattern.name().to_string(),
                size,
                lance: lance_str,
                lance_rle: lance_rle_str,
                parquet: parquet_str,
            });
        }
    }

    // Print markdown table header
    println!("\n| Pattern | Size | Lance (bitpacking) | Lance (RLE) | Parquet |");
    println!("|---------|------|--------------------|-------------|---------|");
    
    // Print each row
    for row in rows {
        println!(
            "| {} | {} | {} | {} | {} |",
            row.pattern,
            row.size,
            row.lance,
            row.lance_rle,
            row.parquet
        );
    }
    
    println!("\n**Note**: Best compression ratio for each test is marked with **bold**.");
}
