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
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext, col};
use futures::{StreamExt, TryStreamExt};
use schema::record::Record;
use store::merged;
use store::{OBJECT_STORE_URL, ObjectWriter};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// What a merge aims for. A file at this size is left alone: merging it again
/// would rewrite it for a fraction more.
const TARGET_FILE_BYTES: u64 = 128 * 1024 * 1024;

/// How far apart in size a batch's files may be. Bounds how much of a larger
/// file gets rewritten to absorb a smaller one — the byte budget alone would
/// not, since crumbs fit beside a nearly target-sized file.
const SIZE_SPREAD: u64 = 8;

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

    /// Merges `directory`'s files until no batch is left worth merging, and
    /// answers what it wrote.
    ///
    /// `closed` says no more files are coming, which is what makes a partial
    /// batch worth merging — while a partition is still being written to, a
    /// merge now is one the next file undoes.
    pub async fn compact<T: Record>(&self, directory: &Path, closed: bool) -> Result<Vec<Path>> {
        let mut sources: Vec<ObjectMeta> = self.store.list(Some(directory)).try_collect().await?;
        let mut written = Vec::new();

        while let Some(batch) = next_batch(&sources, closed) {
            let merged = self.merge::<T>(&batch).await?;

            // The merged file is a source for the next pass, so a partition
            // ends as one file rather than as a pile of batch outputs.
            sources.retain(|source| !batch.iter().any(|used| used.location == source.location));
            sources.push(self.store.head(&merged).await?);
            written.push(merged);
        }

        Ok(written)
    }

    /// One batch of sources into one file, deleting them once it lands.
    async fn merge<T: Record>(&self, sources: &[ObjectMeta]) -> Result<Path> {
        let path = directory_of(&sources[0].location).join(merged::filename(sources));
        let mut stream = self
            .merge_sorted::<T>(sources)
            .await?
            .execute_stream()
            .await?;

        let mut writer = ObjectWriter::new(self.store.clone(), path.clone(), T::schema())?;
        while let Some(batch) = stream.next().await {
            writer.write(&batch?)?;
        }
        writer.finish().await?;

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

/// The next batch worth merging, smallest files first.
///
/// Restarts the batch rather than stopping when a file is too large for it, or
/// one stray crumb anchors the spread for the whole listing and nothing ever
/// merges. Closed, there is no later batch to leave the crumbs for.
fn next_batch(sources: &[ObjectMeta], closed: bool) -> Option<Vec<ObjectMeta>> {
    let mut eligible: Vec<&ObjectMeta> = sources
        .iter()
        .filter(|source| source.size < TARGET_FILE_BYTES)
        .collect();
    eligible.sort_by_key(|source| source.size);

    let mut batch: Vec<ObjectMeta> = Vec::new();
    let mut overflowed = false;

    for source in eligible {
        if batch.len() == SOURCES_PER_MERGE {
            break;
        }

        let smallest = batch.first().map_or(source.size, |first| first.size);
        if !closed && source.size > smallest * SIZE_SPREAD {
            batch.clear();
        }

        let total: u64 = batch.iter().map(|source| source.size).sum();
        if total + source.size > TARGET_FILE_BYTES {
            overflowed = true;
            break;
        }
        batch.push(source.clone());
    }

    let full = overflowed || batch.len() == SOURCES_PER_MERGE || closed;
    (full && batch.len() >= 2).then_some(batch)
}

/// The partition a file sits in.
fn directory_of(path: &Path) -> Path {
    let mut parts: Vec<_> = path.parts().collect();
    parts.pop();
    Path::from_iter(parts)
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
            let mut writer = ObjectWriter::new(
                store.clone(),
                Path::from(format!("spans/p/{id:02}.parquet")),
                batch.schema(),
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.finish().await.unwrap();
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
