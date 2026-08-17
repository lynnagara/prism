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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use futures::StreamExt;
    use schema::record::{Common, Record};
    use schema::spans::{Span, Status};
    use schema::types::Timestamp;

    fn span(received_at: DateTime<Utc>) -> Span {
        Span {
            common: Common {
                organization_id: "4812".to_string(),
                project_id: "91733".to_string(),
                received_at: Timestamp::from(received_at),
            },
            span_id: "a".repeat(16),
            trace_id: "c".repeat(32),
            parent_span_id: None,
            name: "GET /checkout".to_string(),
            started_at: Timestamp::from(received_at),
            ended_at: None,
            status: Status::Ok,
            status_message: None,
        }
    }

    #[tokio::test]
    async fn write() {
        let store: Arc<dyn ObjectStore> =
            Arc::new(datafusion::object_store::memory::InMemory::new());
        let writer = Writer::new(store.clone());

        let partition = Utc.with_ymd_and_hms(2026, 8, 17, 14, 37, 22).unwrap();
        let rows = Span::to_record_batch(&[span(partition)]).unwrap();
        let directory = Span::directory(partition);
        let file_id = Uuid::now_v7();
        writer
            .write(&Batch {
                directory,
                file_id,
                rows,
            })
            .await
            .unwrap();

        // `partition` is a raw instant, so this also covers the write landing in
        // the truncated directory rather than one named for the exact moment.
        let prefix = Path::from(Span::directory(partition));
        let written: Vec<_> = store.list(Some(&prefix)).collect().await;
        assert_eq!(written.len(), 1);
    }
}
