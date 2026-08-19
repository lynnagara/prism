//! One place that decides how rows become an object, so a file ingest wrote
//! and a file compaction merged are encoded the same way.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::object_store::path::Path;
use datafusion::object_store::{ObjectStore, ObjectStoreExt};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::{Compression, ZstdLevel};
use datafusion::parquet::file::properties::WriterProperties;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Rows per row group — parquet records min/max per group, making it the
/// smallest unit a query can skip. Rows are written in primary key order, so
/// each group covers a contiguous slice of it.
const ROW_GROUP_ROWS: usize = 10_000;

/// A file being built up in memory: nothing is stored until
/// [`ObjectWriter::finish`], so memory grows with the file being written.
pub struct ObjectWriter {
    inner: ArrowWriter<Vec<u8>>,
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl ObjectWriter {
    pub fn new(store: Arc<dyn ObjectStore>, path: Path, schema: SchemaRef) -> Result<Self> {
        // Use zstd because query cost is dominated by bytes not CPU
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
            .build();

        Ok(Self {
            inner: ArrowWriter::try_new(Vec::new(), schema, Some(properties))?,
            store,
            path,
        })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner.write(batch)?;
        Ok(())
    }

    /// Appends the parquet footer and stores the result. Without it the bytes
    /// are row groups with no index, which no reader will open.
    pub async fn finish(self) -> Result<()> {
        let Self { inner, store, path } = self;
        store.put(&path, inner.into_inner()?.into()).await?;
        Ok(())
    }
}
