use std::io::Cursor;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use deltalake::action::{Action, DeltaOperation, SaveMode};
use deltalake::arrow::error::Result as ArrowResult;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use deltalake::parquet::arrow::ProjectionMask;
use deltalake::DeltaTable;

use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use object_store::{DynObjectStore, ObjectStore};
use rocksdb::Options;

use crate::FileEvents;

pub struct EventProcessorOptions {
    pub poll_time: u64,
    pub checkpoint_location: String,
}

pub struct EventProcessor<F>
where
    F: FileEvents,
{
    events: F,
    storage: Arc<DynObjectStore>,
    checkpoints: rocksdb::DB,
    table: DeltaTable,
    opts: EventProcessorOptions,
}

impl<F> EventProcessor<F>
where
    F: FileEvents,
{
    pub fn new(
        events: F,
        storage: impl ObjectStore,
        table: DeltaTable,
        opts: EventProcessorOptions,
    ) -> Result<Self> {
        let storage = Arc::new(storage);
        let options = Options::default();
        let checkpoints = rocksdb::DB::open(&options, opts.checkpoint_location.clone())?;

        Ok(Self {
            events,
            storage,
            table,
            opts,
            checkpoints,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        let metadata = self.table.get_metadata()?.clone();
        let mut batch_writer = RecordBatchWriter::for_table(&self.table)?;
        let mut tx = self.table.create_transaction(None);

        for file in self.events.next_file().await? {
            let obj_stream = self.storage.get(&file).await?;
            let stream = create_parquet_reader(obj_stream.bytes().await?)?;
            for batch in stream {
                let batch = batch?;
                batch_writer.write(batch).await?;
            }
            let actions: Vec<Action> = batch_writer
                .flush()
                .await?
                .iter()
                .map(|add| Action::add(add.clone()))
                .collect();
            tx.add_actions(actions);
        }
        let app = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: Some(metadata.partition_columns),
            predicate: None,
        };
        tx.commit(Some(app), None).await?;
        dbg!(self.table.get_state());
        Ok(())
    }
}

fn create_parquet_reader(bytes: Bytes) -> Result<impl Iterator<Item = ArrowResult<RecordBatch>>> {
    let mask = ProjectionMask::all();
    ParquetRecordBatchReaderBuilder::try_new(bytes)?
        .with_projection(mask)
        .build()
        .map_err(Into::into)
}

fn create_csv_reader(bytes: Bytes) -> Result<impl Iterator<Item = ArrowResult<RecordBatch>>> {
    let reader = Cursor::new(bytes);
    deltalake::arrow::csv::ReaderBuilder::new()
        .infer_schema(Some(100))
        .build(reader)
        .map_err(Into::into)
}

fn create_json_reader(bytes: Bytes) -> Result<impl Iterator<Item = ArrowResult<RecordBatch>>> {
    let reader = Cursor::new(bytes);
    deltalake::arrow::json::ReaderBuilder::new()
        .infer_schema(Some(100))
        .build(reader)
        .map_err(Into::into)
}

#[cfg(test)]
pub mod test {
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    use crate::test_utils::{create_initialized_table, StaticFileEvents};

    use super::*;

    #[tokio::test]
    pub async fn test_processor() -> Result<()> {
        let table = create_initialized_table(&[]).await?;
        let test_file = Path::from_filesystem_path("./test_files/alltypes_dictionary.parquet")?;
        let events = StaticFileEvents(vec![
            test_file.clone(),
            test_file.clone(),
            test_file.clone(),
        ]);
        let mut processor = EventProcessor::new(
            events,
            LocalFileSystem::new(),
            table,
            EventProcessorOptions {
                poll_time: 20,
                checkpoint_location: "".to_string(),
            },
        )?;

        processor.run().await
    }
}
