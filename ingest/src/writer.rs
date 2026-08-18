use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::object_store::path::Path;
use datafusion::object_store::{ObjectStore, ObjectStoreExt};
use datafusion::parquet::arrow::ArrowWriter;
use uuid::Uuid;

/// One object's rows and name. The name is set here so a retry overwrites
/// rather than duplicates.
pub struct Batch {
    pub directory: String,
    pub file_id: Uuid,
    pub rows: RecordBatch,
}

pub struct Writer {
    store: Arc<dyn ObjectStore>,
}

impl Writer {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Ids are written unhyphenated and time-ordered, so a listing reads oldest
    /// first and a name costs four bytes less.
    fn object_path(directory: &str, file_id: Uuid) -> Path {
        Path::from(format!("{}/{}.parquet", directory, file_id.simple()))
    }

    pub async fn write(
        &self,
        batch: &Batch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path = Self::object_path(&batch.directory, batch.file_id);

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.rows.schema(), None)?;
        writer.write(&batch.rows)?;
        writer.close()?;

        self.store.put(&path, buffer.into()).await?;
        Ok(())
    }
}
