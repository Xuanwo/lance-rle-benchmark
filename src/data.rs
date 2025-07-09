use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub enum DataPattern {
    /// High repetition - ideal for RLE
    /// repetition_rate: percentage of values that repeat (0.0 to 1.0)
    /// run_length: average length of repeated runs
    HighRepetition { 
        repetition_rate: f64, 
        run_length: usize 
    },
    
    /// Low repetition - worst case for RLE
    /// unique_ratio: percentage of unique values (0.0 to 1.0)
    LowRepetition { 
        unique_ratio: f64 
    },
}

impl DataPattern {
    pub fn generate(&self, size: usize) -> Vec<i32> {
        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(size);
        
        match self {
            Self::HighRepetition { repetition_rate, run_length } => {
                // Generate data with high repetition
                // Strategy: create runs of repeated values
                let mut i = 0;
                while i < size {
                    if rng.gen::<f64>() < *repetition_rate {
                        // Generate a run of repeated values
                        let value = rng.gen_range(0..100); // Limited value range for more repetition
                        let actual_run_length = if *run_length > 1 {
                            rng.gen_range(1..=*run_length).min(size - i)
                        } else {
                            1
                        };
                        
                        for _ in 0..actual_run_length {
                            if i < size {
                                data.push(value);
                                i += 1;
                            }
                        }
                    } else {
                        // Generate a unique value
                        data.push(rng.gen_range(1000..10000)); // Different range to ensure uniqueness
                        i += 1;
                    }
                }
            }
            
            Self::LowRepetition { unique_ratio } => {
                // Generate data with low repetition (high entropy)
                let unique_count = (size as f64 * unique_ratio) as usize;
                
                if unique_count >= size {
                    // All unique values
                    for i in 0..size {
                        data.push(i as i32);
                    }
                } else {
                    // Mix of unique and some repeated values
                    let mut values: Vec<i32> = (0..unique_count).map(|i| i as i32).collect();
                    
                    // Fill the rest with random selections from existing values
                    while values.len() < size {
                        let idx = rng.gen_range(0..unique_count);
                        values.push(values[idx]);
                    }
                    
                    // Shuffle to distribute repeated values
                    use rand::seq::SliceRandom;
                    values.shuffle(&mut rng);
                    data = values;
                }
            }
        }
        
        data
    }

    pub fn name(&self) -> String {
        match self {
            Self::HighRepetition { repetition_rate, run_length } => {
                // Format as "high_rep_95pct_run100" for 95% repetition with run length 100
                format!("high_rep_{}pct_run{}", (repetition_rate * 100.0) as u32, run_length)
            }
            Self::LowRepetition { unique_ratio } => {
                // Format as "low_rep_90pct_unique" for 90% unique values
                format!("low_rep_{}pct_unique", (unique_ratio * 100.0) as u32)
            }
        }
    }
}

pub fn generate_indices(size: usize, _ratio: f64) -> Vec<usize> {
    // Always return just one random index in the middle of the data
    vec![size / 2]
}

pub fn generate_record_batch(pattern: &DataPattern, size: usize) -> RecordBatch {
    let data = pattern.generate(size);
    let array = Arc::new(Int32Array::from(data)) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));

    RecordBatch::try_new(schema, vec![array]).unwrap()
}