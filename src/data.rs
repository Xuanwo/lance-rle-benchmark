use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub enum DataPattern {
    Runs {
        run_length: usize,
        unique_values: usize,
    },
    Periodic {
        period: usize,
        amplitude: i32,
    },
    Sparse {
        density: f64,
    },
    Monotonic {
        step: i32,
    },
    Random,
}

impl DataPattern {
    pub fn generate(&self, size: usize) -> Vec<i32> {
        let mut rng = StdRng::seed_from_u64(42);

        match self {
            Self::Runs {
                run_length,
                unique_values,
            } => {
                let mut data = Vec::with_capacity(size);
                let mut current_value = 0;
                let mut remaining_in_run = 0;

                while data.len() < size {
                    if remaining_in_run == 0 {
                        current_value = rng.gen_range(0..*unique_values as i32);
                        remaining_in_run = rng.gen_range(1..=*run_length);
                    }

                    data.push(current_value);
                    remaining_in_run -= 1;
                }

                data.truncate(size);
                data
            }

            Self::Periodic { period, amplitude } => (0..size)
                .map(|i| ((i % period) as i32) * amplitude / (*period as i32))
                .collect(),

            Self::Sparse { density } => (0..size)
                .map(|_| {
                    if rng.gen::<f64>() < *density {
                        rng.gen_range(0..100)
                    } else {
                        0
                    }
                })
                .collect(),

            Self::Monotonic { step } => (0..size).map(|i| (i as i32) * step).collect(),

            Self::Random => (0..size).map(|_| rng.gen()).collect(),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Runs { .. } => "runs",
            Self::Periodic { .. } => "periodic",
            Self::Sparse { .. } => "sparse",
            Self::Monotonic { .. } => "monotonic",
            Self::Random => "random",
        }
    }
}

pub fn generate_indices(size: usize, ratio: f64) -> Vec<usize> {
    let mut rng = StdRng::seed_from_u64(42);
    let count = (size as f64 * ratio) as usize;

    let mut indices: Vec<usize> = (0..size).collect();

    for i in 0..count {
        let j = rng.gen_range(i..size);
        indices.swap(i, j);
    }

    indices.truncate(count);
    indices.sort_unstable();
    indices
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
