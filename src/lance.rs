use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use lance_core::datatypes::{Schema as LanceSchema, COMPRESSION_META_KEY};
use lance_file::reader::FileReader;
use lance_file::writer::{FileWriter, FileWriterOptions};
use lance_io::object_store::ObjectStore;
use lance_io::ReadBatchParams;
use lance_table::format::SelfDescribingFileReader;
use lance_table::io::manifest::ManifestDescribing;
use object_store::path::Path;
use std::collections::HashMap;
use std::sync::Arc;

pub async fn write_bytes(batch: RecordBatch, _use_v2: bool, use_rle: bool) -> Vec<u8> {
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

    let arrow_schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    let lance_schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();
    let batch_with_compression =
        RecordBatch::try_new(arrow_schema, batch.columns().to_vec()).unwrap();

    // Use memory object store
    let object_store = Arc::new(ObjectStore::memory());
    let path = Path::from("data.lance");

    // Write the file
    let options = FileWriterOptions::default();
    let mut writer =
        FileWriter::<ManifestDescribing>::try_new(&object_store, &path, lance_schema, &options)
            .await
            .unwrap();

    writer.write(&[batch_with_compression]).await.unwrap();
    writer.finish().await.unwrap();

    // Read back the bytes
    object_store
        .inner
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec()
}

pub async fn read_bytes(bytes: &[u8]) -> Vec<RecordBatch> {
    // Use memory object store
    let object_store = Arc::new(ObjectStore::memory());
    let path = Path::from("data.lance");

    // Write bytes to memory store
    object_store
        .inner
        .put(&path, bytes::Bytes::from(bytes.to_vec()).into())
        .await
        .unwrap();

    // Read the file
    let reader = FileReader::try_new_self_described(&object_store, &path, None)
        .await
        .unwrap();
    let schema = reader.schema();

    // Read all data
    let num_batches = reader.num_batches();
    let mut batches = Vec::new();

    for batch_id in 0..num_batches {
        let batch = reader
            .read_batch(batch_id as i32, ReadBatchParams::RangeFull, schema)
            .await
            .unwrap();
        batches.push(batch);
    }

    batches
}

pub async fn take_rows_from_bytes(bytes: &[u8], indices: &[usize]) -> RecordBatch {
    // Use memory object store
    let object_store = Arc::new(ObjectStore::memory());
    let path = Path::from("data.lance");

    // Write bytes to memory store
    object_store
        .inner
        .put(&path, bytes::Bytes::from(bytes.to_vec()).into())
        .await
        .unwrap();

    // Read the file
    let reader = FileReader::try_new_self_described(&object_store, &path, None)
        .await
        .unwrap();

    // Take specific rows
    let indices_u32: Vec<u32> = indices.iter().map(|&i| i as u32).collect();
    let schema = reader.schema();
    reader.take(&indices_u32, schema).await.unwrap()
}
