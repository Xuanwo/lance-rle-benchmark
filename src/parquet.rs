use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector};
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
    // Since we only read one row, optimize for this case
    assert_eq!(indices.len(), 1, "Expected exactly one index");
    let target_row = indices[0];

    let file = bytes::Bytes::from(bytes.to_vec());
    let mut reader_builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    // Get metadata to know total rows
    let metadata = reader_builder.metadata();
    let total_rows = metadata.file_metadata().num_rows() as usize;
    let schema = reader_builder.schema().clone();

    if target_row >= total_rows {
        return RecordBatch::new_empty(schema);
    }

    // Build RowSelection for single row
    let selectors = vec![
        RowSelector::skip(target_row),
        RowSelector::select(1),
        RowSelector::skip(total_rows - target_row - 1),
    ];
    let row_selection = RowSelection::from(selectors);

    // Apply row selection and build reader
    reader_builder = reader_builder.with_row_selection(row_selection);
    let mut reader = reader_builder.build().unwrap();

    // Read the single row
    reader.next().unwrap().unwrap()
}
