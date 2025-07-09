use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::{Field, Schema};
use futures::TryStreamExt;
use lance::dataset::{ProjectionRequest, WriteMode, WriteParams};
use lance::Dataset;
use lance_core::datatypes::COMPRESSION_META_KEY;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

pub async fn write_file(batch: RecordBatch, _use_v2: bool, use_rle: bool) -> (TempDir, String) {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("data.lance");
    let uri = path.to_str().unwrap();

    // Create schema with compression metadata
    let mut metadata = HashMap::new();
    let compression = if use_rle { "rle" } else { "bitpacking" };
    metadata.insert(COMPRESSION_META_KEY.to_string(), compression.to_string());

    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| {
            Field::new(f.name(), f.data_type().clone(), f.is_nullable())
                .with_metadata(metadata.clone())
        })
        .collect();

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));

    let batches = vec![batch];
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    let params = WriteParams {
        mode: WriteMode::Create,
        ..Default::default()
    };

    let _dataset = Dataset::write(reader, uri, Some(params)).await.unwrap();

    (temp_dir, uri.to_string())
}

pub async fn read_file(uri: &str) -> Vec<RecordBatch> {
    let dataset = Dataset::open(uri).await.unwrap();
    dataset
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

pub async fn take_rows(uri: &str, indices: &[usize]) -> RecordBatch {
    let dataset = Dataset::open(uri).await.unwrap();
    let indices_u64: Vec<u64> = indices.iter().map(|&i| i as u64).collect();
    let schema = Arc::new(dataset.schema().clone());
    dataset
        .take_rows(&indices_u64, ProjectionRequest::Schema(schema))
        .await
        .unwrap()
}
