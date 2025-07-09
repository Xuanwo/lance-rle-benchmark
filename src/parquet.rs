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
    let file = bytes::Bytes::from(bytes.to_vec());
    let mut reader_builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    // Get metadata to know total rows
    let metadata = reader_builder.metadata();
    let total_rows = metadata.file_metadata().num_rows() as usize;
    let schema = reader_builder.schema().clone();

    // Convert indices to a sorted, deduplicated list (required for RowSelection)
    let mut sorted_indices = indices.to_vec();
    sorted_indices.sort_unstable();
    sorted_indices.dedup();

    // Build RowSelection from indices
    let mut selectors = Vec::new();
    let mut current_pos = 0;

    for &idx in &sorted_indices {
        if idx >= total_rows {
            continue;
        }

        // Skip rows before this index
        if idx > current_pos {
            selectors.push(RowSelector::skip(idx - current_pos));
            current_pos = idx;
        }

        // Select this row
        selectors.push(RowSelector::select(1));
        current_pos += 1;
    }

    // Skip any remaining rows
    if current_pos < total_rows {
        selectors.push(RowSelector::skip(total_rows - current_pos));
    }

    let row_selection = RowSelection::from(selectors);

    // Apply row selection and build reader
    reader_builder = reader_builder.with_row_selection(row_selection);
    let reader = reader_builder.build().unwrap();

    // Read the selected rows
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();

    // Concatenate if multiple batches
    if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else if batches.is_empty() {
        RecordBatch::new_empty(schema)
    } else {
        arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap()
    }
}
