use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema};
use rand::distributions::{Distribution, WeightedIndex};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

const NUM_FEATURES: usize = 3827;

pub fn generate_record_batch(num_rows: usize) -> RecordBatch {
    generate_nested_record_batch(num_rows)
}

pub fn generate_nested_record_batch(num_rows: usize) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(42);
    
    // Generate UUID column
    let uuids: Vec<i64> = (0..num_rows as i64).collect();
    let uuid_array = Arc::new(Int64Array::from(uuids)) as ArrayRef;
    
    // Generate features struct
    let mut feature_arrays: Vec<ArrayRef> = Vec::with_capacity(NUM_FEATURES);
    let mut feature_fields: Vec<Field> = Vec::with_capacity(NUM_FEATURES);
    
    // Define common values with many zeros (simulating real data)
    let common_values = [0.0, 1.0, -1.0, 2.0, -2.0, 5.0, -5.0, 10.0, -10.0, 100.0];
    let weights = [40, 15, 15, 8, 8, 4, 4, 3, 2, 1]; // 40% zeros
    let dist = WeightedIndex::new(&weights).unwrap();
    
    // Generate data for each feature
    for i in 0..NUM_FEATURES {
        let mut feature_values = Vec::with_capacity(num_rows);
        
        for _ in 0..num_rows {
            // 80% chance of using common values (includes many zeros)
            let val = if rng.gen_bool(0.8) {
                common_values[dist.sample(&mut rng)]
            } else {
                // 20% chance of random value
                rng.gen_range(-1000.0..1000.0)
            };
            feature_values.push(val);
        }
        
        feature_arrays.push(Arc::new(Float64Array::from(feature_values)) as ArrayRef);
        feature_fields.push(Field::new(format!("feature{}", i), DataType::Float64, false));
    }
    
    // Clone fields for schema before creating struct array
    let schema_fields = feature_fields.clone();
    
    // Create the struct array for features
    let field_array_pairs: Vec<(Arc<Field>, ArrayRef)> = feature_fields
        .into_iter()
        .map(Arc::new)
        .zip(feature_arrays)
        .collect();
    let features_struct = Arc::new(StructArray::from(field_array_pairs)) as ArrayRef;
    
    // Create the final schema matching user's schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("uuid", DataType::Int64, false),
        Field::new(
            "features",
            DataType::Struct(Fields::from(schema_fields)),
            false,
        ),
    ]));
    
    RecordBatch::try_new(schema, vec![uuid_array, features_struct]).unwrap()
}

pub fn generate_flat_record_batch(num_rows: usize) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(42);
    
    // Generate UUID column
    let uuids: Vec<i64> = (0..num_rows as i64).collect();
    let uuid_array = Arc::new(Int64Array::from(uuids)) as ArrayRef;
    
    // Generate feature columns
    let mut columns: Vec<ArrayRef> = vec![uuid_array];
    let mut fields: Vec<Field> = vec![Field::new("uuid", DataType::Int64, false)];
    
    // Define common values with many zeros (simulating real data)
    let common_values = [0.0, 1.0, -1.0, 2.0, -2.0, 5.0, -5.0, 10.0, -10.0, 100.0];
    let weights = [40, 15, 15, 8, 8, 4, 4, 3, 2, 1]; // 40% zeros
    let dist = WeightedIndex::new(&weights).unwrap();
    
    // Generate data for each feature as a separate column
    for i in 0..NUM_FEATURES {
        let mut feature_values = Vec::with_capacity(num_rows);
        
        for _ in 0..num_rows {
            // 80% chance of using common values (includes many zeros)
            let val = if rng.gen_bool(0.8) {
                common_values[dist.sample(&mut rng)]
            } else {
                // 20% chance of random value
                rng.gen_range(-1000.0..1000.0)
            };
            feature_values.push(val);
        }
        
        columns.push(Arc::new(Float64Array::from(feature_values)) as ArrayRef);
        fields.push(Field::new(format!("feature{}", i), DataType::Float64, false));
    }
    
    // Create the final schema with flat structure
    let schema = Arc::new(Schema::new(fields));
    
    RecordBatch::try_new(schema, columns).unwrap()
}