use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use schema::record::Record;
use schema::types::TimePartitioned;
use uuid::Uuid;

use crate::writer::{Batch, Writer};

/// One partition's rows, named when the partition is first seen so a retry
/// reuses the name.
struct Pending<T> {
    file_id: Uuid,
    rows: Vec<T>,
}

/// Rows held until they are written, grouped by the partition they fall in.
pub struct Buffer<T> {
    writer: Writer,
    partitions: BTreeMap<DateTime<Utc>, Pending<T>>,
}

impl<T: Record> Buffer<T> {
    pub fn new(writer: Writer) -> Self {
        Self {
            writer,
            partitions: BTreeMap::new(),
        }
    }

    pub fn push(&mut self, row: T) {
        let partition = row.common().received_at.partition_start(T::GRANULARITY);
        self.partitions
            .entry(partition)
            .or_insert_with(|| Pending {
                file_id: Uuid::now_v7(),
                rows: Vec::new(),
            })
            .rows
            .push(row);
    }

    /// Removes each partition only after its write succeeds, so a failure
    /// leaves it (and any after it) untouched for the next flush.
    pub async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let keys: Vec<DateTime<Utc>> = self.partitions.keys().copied().collect();

        for partition in keys {
            let pending = &self.partitions[&partition];
            let rows = T::to_record_batch(&pending.rows)?;
            let directory = T::directory(partition);

            self.writer
                .write(&Batch {
                    directory,
                    file_id: pending.file_id,
                    rows,
                })
                .await?;

            self.partitions.remove(&partition);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use datafusion::object_store::{ObjectStore, path::Path};
    use futures::StreamExt;
    use schema::record::Common;
    use schema::spans::{Span, Status};
    use schema::types::Timestamp;
    use std::sync::Arc;

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
            start_ts: Timestamp::from(received_at),
            end_ts: None,
            status: Status::Ok,
            status_message: None,
        }
    }

    #[tokio::test]
    async fn flush_writes_one_object_per_partition() {
        let store: Arc<dyn ObjectStore> =
            Arc::new(datafusion::object_store::memory::InMemory::new());
        let mut buffer: Buffer<Span> = Buffer::new(Writer::new(store.clone()));

        let day1 = Utc.with_ymd_and_hms(2026, 8, 17, 9, 0, 0).unwrap();
        let day2 = Utc.with_ymd_and_hms(2026, 8, 18, 9, 0, 0).unwrap();

        buffer.push(span(day1));
        buffer.push(span(day1));
        buffer.push(span(day2));

        buffer.flush().await.unwrap();

        let written: Vec<_> = store.list(Some(&Path::from(Span::TABLE))).collect().await;
        assert_eq!(written.len(), 2);
    }
}
