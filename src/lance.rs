use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use futures::StreamExt;
use lance_core::cache::LanceCache;
use lance_core::datatypes::{Schema as LanceSchema, COMPRESSION_META_KEY};
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_encoding::version::LanceFileVersion;
use lance_file::v2::reader::{FileReader, FileReaderOptions, ReaderProjection};
use lance_file::v2::writer::{FileWriter, FileWriterOptions};
use lance_io::object_store::ObjectStore;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use lance_io::ReadBatchParams;
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

    // Write the file using v2 writer
    let options = FileWriterOptions {
        format_version: Some(LanceFileVersion::V2_1),
        ..Default::default()
    };

    // Use custom encoding strategy for RLE

    let object_writer = object_store.create(&path).await.unwrap();
    let mut writer = FileWriter::try_new(object_writer, lance_schema, options).unwrap();

    writer.write_batch(&batch_with_compression).await.unwrap();
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

    // Create scheduler and open file
    let scheduler = ScanScheduler::new(
        object_store.clone(),
        SchedulerConfig::max_bandwidth(&object_store),
    );
    let file_scheduler = scheduler
        .open_file(&path, &CachedFileSize::unknown())
        .await
        .unwrap();

    // Open the file reader
    let cache = LanceCache::no_cache();
    let reader = FileReader::try_open(
        file_scheduler,
        None,
        Arc::<DecoderPlugins>::default(),
        &cache,
        FileReaderOptions::default(),
    )
    .await
    .unwrap();

    let num_rows = reader.num_rows();
    let projection =
        ReaderProjection::from_whole_schema(reader.schema(), reader.metadata().version());

    // Read all data
    let stream = reader
        .read_tasks(
            ReadBatchParams::Range(0..num_rows as usize),
            1024,
            Some(projection),
            FilterExpression::no_filter(),
        )
        .unwrap();

    let mut batches = Vec::new();
    futures::pin_mut!(stream);
    while let Some(batch_task) = stream.next().await {
        let batch = batch_task.task.await.unwrap();
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

    // Create scheduler and open file
    let scheduler = ScanScheduler::new(
        object_store.clone(),
        SchedulerConfig::max_bandwidth(&object_store),
    );
    let file_scheduler = scheduler
        .open_file(&path, &CachedFileSize::unknown())
        .await
        .unwrap();

    // Open the file reader
    let cache = LanceCache::no_cache();
    let reader = FileReader::try_open(
        file_scheduler,
        None,
        Arc::<DecoderPlugins>::default(),
        &cache,
        FileReaderOptions::default(),
    )
    .await
    .unwrap();

    let projection =
        ReaderProjection::from_whole_schema(reader.schema(), reader.metadata().version());

    // Take specific rows using indices
    let indices_u32: Vec<u32> = indices.iter().map(|&i| i as u32).collect();
    let indices_array = arrow_array::UInt32Array::from(indices_u32);
    let stream = reader
        .read_tasks(
            ReadBatchParams::Indices(indices_array),
            1024,
            Some(projection),
            FilterExpression::no_filter(),
        )
        .unwrap();

    let mut batches = Vec::new();
    futures::pin_mut!(stream);
    while let Some(batch_task) = stream.next().await {
        let batch = batch_task.task.await.unwrap();
        batches.push(batch);
    }

    // Should return a single batch for take operation
    if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        // Concatenate if multiple batches
        arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap()
    }
}
