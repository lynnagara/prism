//! Merges a partition's files into one, so a query opens fewer of them.
//!
//! A merged file is named `<own id>_<replaced id>_<replaced id>...`, so a reader
//! can tell from a listing alone which files it replaced, without opening
//! anything. That name is what commits a merge: the upload is invisible until
//! it completes, and once visible it already declares its sources replaced.

use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::SortExpr;
use datafusion::object_store::path::Path;
use datafusion::object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext, col};
use futures::{StreamExt, TryStreamExt};
use schema::record::Record;
use uuid::Uuid;

/// Where the store is mounted while a merge reads it. Addressing only —
/// datafusion keys registered stores by url — so it never reaches a path.
const OBJECT_STORE_URL: &str = "prism://store";

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Delimits the ids packed into a merged filename. Ids are written
/// as unhypenated uuids.
const SEPARATOR: char = '_';

/// Files per merge. A merge names every source in one filename, and a store
/// backed by files bounds that at 255 bytes rather than s3's looser 1024 per
/// key. At unhyphenated uuids the budget is `40 + 33n`, which leaves room for 6.
const SOURCES_PER_MERGE: usize = 6;

pub struct Compactor {
    store: Arc<dyn ObjectStore>,
}

impl Compactor {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Merges `directory`'s files, at most [`SOURCES_PER_MERGE`] at a time,
    /// until one is left. Answers what it wrote, newest last.
    pub async fn compact<T: Record>(&self, directory: &Path) -> Result<Vec<Path>> {
        let mut sources: Vec<ObjectMeta> = self.store.list(Some(directory)).try_collect().await?;
        let mut written = Vec::new();

        while sources.len() > 1 {
            let batch: Vec<ObjectMeta> = sources
                .drain(..sources.len().min(SOURCES_PER_MERGE))
                .collect();
            let merged = self.merge::<T>(&batch).await?;

            // Merged files are themselves sources for the next pass, so a
            // partition of any size ends as one file rather than as batches.
            sources.push(self.store.head(&merged).await?);
            written.push(merged);
        }

        Ok(written)
    }

    /// One batch of sources into one file, deleting them once it lands.
    async fn merge<T: Record>(&self, sources: &[ObjectMeta]) -> Result<Path> {
        let path = directory_of(&sources[0].location).join(filename(sources));
        let mut stream = self
            .merge_sorted::<T>(sources)
            .await?
            .execute_stream()
            .await?;

        // Encoded a batch at a time, so what is held is one output file's bytes
        // rather than every row the partition holds.
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, T::schema(), None)?;
        while let Some(batch) = stream.next().await {
            writer.write(&batch?)?;
        }
        writer.close()?;

        self.store.put(&path, buffer.into()).await?;

        for source in sources {
            self.store.delete(&source.location).await?;
        }

        Ok(path)
    }

    /// The sources as one sorted stream.
    ///
    /// A group per file, since datafusion concatenates each group's files into
    /// one stream — and two sorted files end to end are not sorted, so it would
    /// re-sort in memory rather than interleave.
    async fn merge_sorted<T: Record>(&self, sources: &[ObjectMeta]) -> Result<DataFrame> {
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(sources.len()),
        );
        ctx.register_object_store(
            ObjectStoreUrl::parse(OBJECT_STORE_URL)?.as_ref(),
            self.store.clone(),
        );

        let paths: Vec<String> = sources
            .iter()
            .map(|source| format!("{OBJECT_STORE_URL}/{}", source.location))
            .collect();
        let order: Vec<SortExpr> = T::all_primary_key()
            .iter()
            .map(|name| col(*name).sort(true, false))
            .collect();

        let schema = T::schema();
        let options = ParquetReadOptions::default()
            .schema(&schema)
            .file_sort_order(vec![order.clone()]);

        Ok(ctx.read_parquet(paths, options).await?.sort(order)?)
    }
}

/// A new id, then each source's own — not the ids those in turn replaced, or
/// the name would grow at every level until it passed the key limit.
fn filename(sources: &[ObjectMeta]) -> String {
    let mut name = Uuid::now_v7().simple().to_string();
    for source in sources {
        name.push(SEPARATOR);
        name.push_str(own_id(
            source.location.filename().expect("sources are files"),
        ));
    }
    name.push_str(".parquet");
    name
}

/// The partition a file sits in.
fn directory_of(path: &Path) -> Path {
    let mut parts: Vec<_> = path.parts().collect();
    parts.pop();
    Path::from_iter(parts)
}

fn own_id(name: &str) -> &str {
    name.strip_suffix(".parquet")
        .unwrap_or(name)
        .split(SEPARATOR)
        .next()
        .expect("split yields at least one field")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::object_store::memory::InMemory;
    use datafusion::physical_plan::displayable;
    use schema::spans::Span;

    /// The interleave is the point: sorted sources merge, and a plan that sorts
    /// instead is holding a partition in memory to do it.
    #[tokio::test]
    async fn merges_rather_than_re_sorts() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let compactor = Compactor::new(store.clone());

        // More files than any plausible cpu count, so the default grouping
        // would put several in one group and force a re-sort.
        for id in 0..64 {
            let batch = empty_span_batch();
            let mut buffer = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            store
                .put(
                    &Path::from(format!("spans/p/{id:02}.parquet")),
                    buffer.into(),
                )
                .await
                .unwrap();
        }

        let sources: Vec<ObjectMeta> = store.list(None).try_collect().await.unwrap();
        let plan = compactor
            .merge_sorted::<Span>(&sources)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan = displayable(plan.as_ref()).indent(true).to_string();

        assert!(plan.contains("SortPreservingMergeExec"), "{plan}");
        assert!(!plan.contains("SortExec"), "{plan}");
    }

    fn empty_span_batch() -> RecordBatch {
        RecordBatch::new_empty(<Span as Record>::schema())
    }
}
