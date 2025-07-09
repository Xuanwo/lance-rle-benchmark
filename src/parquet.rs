use arrow_array::{ArrayRef, RecordBatch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub fn write_bytes(batch: RecordBatch) -> Vec<u8> {
    // Use Parquet's default encoding selection which automatically chooses:
    // - RLE_DICTIONARY for columns with repeated values
    // - DELTA_BINARY_PACKED for sorted integer columns
    // - PLAIN for other cases
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    buffer
}

pub fn read_bytes(bytes: &[u8]) -> Vec<RecordBatch> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes.to_vec()))
        .unwrap()
        .build()
        .unwrap();

    reader.collect::<Result<Vec<_>, _>>().unwrap()
}

pub fn take_rows_from_bytes(bytes: &[u8], indices: &[usize]) -> RecordBatch {
    // Parquet doesn't have efficient random access, so we read all and filter
    let batches = read_bytes(bytes);

    // Concatenate all batches into a single batch
    let schema = batches[0].schema();
    let mut all_columns: Vec<Vec<ArrayRef>> = vec![vec![]; schema.fields().len()];

    for batch in &batches {
        for (i, column) in batch.columns().iter().enumerate() {
            all_columns[i].push(column.clone());
        }
    }

    let concatenated_columns: Vec<ArrayRef> = all_columns
        .into_iter()
        .map(|columns| {
            arrow::compute::concat(&columns.iter().map(|a| a.as_ref()).collect::<Vec<_>>()).unwrap()
        })
        .collect();

    let concatenated_batch = RecordBatch::try_new(schema.clone(), concatenated_columns).unwrap();

    // Now perform take on the concatenated batch
    let indices_array =
        arrow_array::UInt32Array::from(indices.iter().map(|&i| i as u32).collect::<Vec<_>>());

    let arrays = concatenated_batch
        .columns()
        .iter()
        .map(|col| arrow::compute::take(col, &indices_array, None).unwrap())
        .collect();

    RecordBatch::try_new(schema, arrays).unwrap()
}
