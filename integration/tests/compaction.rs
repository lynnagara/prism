use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use compact::Compactor;
use datafusion::arrow::array::{Array, FixedSizeBinaryArray};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::object_store::memory::InMemory;
use datafusion::object_store::path::Path;
use datafusion::object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use futures::TryStreamExt;
use ingest::buffer::Buffer;
use ingest::writer::Writer;
use query::Catalog;
use schema::record::{Common, Record};
use schema::spans::{Span, Status};
use schema::types::{SpanId, Tags, Timestamp, TraceId};

/// Ids are bytes, and a test wants to read them: a short label padded out is
/// legible in a fixture and comes back as itself.
fn span_id(label: &str) -> SpanId {
    let mut bytes = [0; 8];
    bytes[..label.len()].copy_from_slice(label.as_bytes());
    SpanId::from(bytes)
}

fn trace_id(label: &str) -> TraceId {
    let mut bytes = [0; 16];
    bytes[..label.len()].copy_from_slice(label.as_bytes());
    TraceId::from(bytes)
}

fn label(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .trim_end_matches('\0')
        .to_string()
}

fn at(hour: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 17, hour, 0, 0).unwrap()
}

fn tuesday(hour: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 18, hour, 0, 0).unwrap()
}

fn span(span_id_label: &str, received_at: DateTime<Utc>) -> Span {
    Span {
        common: Common {
            organization_id: "4812".to_string(),
            project_id: "91733".to_string(),
            received_at: Timestamp::from(received_at),
        },
        span_id: span_id(span_id_label),
        trace_id: trace_id("aaa"),
        parent_span_id: None,
        name: "GET /checkout".to_string(),
        started_at: Timestamp::from(received_at),
        ended_at: None,
        status: Status::Ok,
        status_message: None,
        tags: Tags::default(),
    }
}

/// A buffer writes what it holds when it shuts down, so one per span is what
/// gives a partition several files to merge.
async fn written(store: Arc<dyn ObjectStore>, spans: Vec<Span>) {
    for span in spans {
        file(&store, vec![span]).await;
    }
}

/// Everything given at once, so it lands in one file per partition.
async fn file(store: &Arc<dyn ObjectStore>, spans: Vec<Span>) {
    let buffer: Buffer<Span> = Buffer::new(Writer::new(store.clone()));

    buffer.push(spans).expect("room to queue");
    buffer.shutdown().await;
}

async fn listing(store: &Arc<dyn ObjectStore>) -> Vec<ObjectMeta> {
    store.list(None).try_collect().await.unwrap()
}

async fn span_ids(store: Arc<dyn ObjectStore>) -> Vec<String> {
    let catalog = Catalog::new(store).unwrap();
    catalog.register::<Span>().await.unwrap();

    let batches = catalog
        .sql_cross_org("select span_id from spans order by span_id")
        .await
        .unwrap();

    batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column_by_name("span_id")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            (0..ids.len())
                .map(|i| label(ids.value(i)))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// A partition's files become one, holding the same rows, named for the three
/// it replaced.
#[tokio::test]
async fn a_partition_merges_into_one_file() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    written(
        store.clone(),
        vec![span("a", at(9)), span("b", at(10)), span("c", at(11))],
    )
    .await;

    let before = listing(&store).await;
    assert_eq!(before.len(), 3, "one file per flush");

    let directory = Path::from(Span::directory(at(9)));
    let merged = Compactor::new(store.clone())
        .compact::<Span>(&directory, true)
        .await
        .unwrap();

    let after = listing(&store).await;
    assert_eq!(after.len(), 1, "the sources are gone");
    assert_eq!(after[0].location, *merged.last().unwrap());
    assert_eq!(span_ids(store).await, ["a", "b", "c"]);
}

/// Nothing to merge, and nothing rewritten for the sake of it.
#[tokio::test]
async fn one_file_is_left_alone() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    written(store.clone(), vec![span("a", at(9))]).await;

    let directory = Path::from(Span::directory(at(9)));
    let merged = Compactor::new(store.clone())
        .compact::<Span>(&directory, true)
        .await
        .unwrap();

    assert!(merged.is_empty());
    assert_eq!(listing(&store).await.len(), 1);
}

/// One file holding the rows of two, named for them, as a merge leaves things
/// until its deletes land.
async fn put(store: &Arc<dyn ObjectStore>, name: &str, spans: &[Span]) {
    let batch = Span::to_record_batch(spans).unwrap();
    let mut buffer = Vec::new();
    let mut writer =
        datafusion::parquet::arrow::ArrowWriter::try_new(&mut buffer, batch.schema(), None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let path = Path::from(format!("{}/{name}", Span::directory(at(9))));
    store.put(&path, buffer.into()).await.unwrap();
}

/// A merged file and the sources it replaced are both present until the deletes
/// land — and a failed delete leaves them indefinitely. Reading both counts
/// those rows twice.
#[tokio::test]
async fn a_replaced_file_is_not_read() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    put(&store, "aaa.parquet", &[span("a", at(9))]).await;
    put(&store, "bbb.parquet", &[span("b", at(10))]).await;
    put(
        &store,
        "ccc_aaa_bbb.parquet",
        &[span("a", at(9)), span("b", at(10))],
    )
    .await;

    assert_eq!(span_ids(store).await, ["a", "b"]);
}

/// More files than one merge can name, so the batches merge and then their
/// outputs merge, until the partition is one file.
#[tokio::test]
async fn more_files_than_one_merge_can_name() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let spans = (0..9).map(|i| span(&format!("s{i}"), at(9))).collect();
    written(store.clone(), spans).await;

    assert_eq!(listing(&store).await.len(), 9);

    Compactor::new(store.clone())
        .compact::<Span>(&Path::from(Span::directory(at(9))), true)
        .await
        .unwrap();

    assert_eq!(listing(&store).await.len(), 1, "merged down to one file");
    assert_eq!(
        span_ids(store).await,
        ["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]
    );
}

/// Row groups are the smallest unit a query can skip, so a merged file holding
/// more rows than one group has to split into several — otherwise its min/max
/// spans everything and merging bought only fewer files to open.
#[tokio::test]
async fn a_merged_file_splits_into_row_groups() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Three files of nine thousand, so the merge crosses the row group size
    // that no single ingest file reaches.
    for id in 0..3 {
        let spans = (0..9_000).map(|i| span(&format!("s{id}-{i:05}"), at(9)));
        file(&store, spans.collect()).await;
    }

    Compactor::new(store.clone())
        .compact::<Span>(&Path::from(Span::directory(at(9))), true)
        .await
        .unwrap();

    let merged = listing(&store).await;
    assert_eq!(merged.len(), 1);

    let bytes = store
        .get(&merged[0].location)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let groups =
        datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .metadata()
            .num_row_groups();

    assert!(
        groups > 1,
        "27000 rows should not be one row group, got {groups}"
    );
}

/// A partition still being written to waits for a full batch: merging two of
/// its files now is work the next flush undoes.
#[tokio::test]
async fn an_open_partition_waits_for_a_full_batch() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    written(store.clone(), vec![span("a", at(9)), span("b", at(10))]).await;

    let written_paths = Compactor::new(store.clone())
        .compact::<Span>(&Path::from(Span::directory(at(9))), false)
        .await
        .unwrap();

    assert!(written_paths.is_empty(), "two files is not a full batch");
    assert_eq!(listing(&store).await.len(), 2);
}

/// Closed, the same two files merge — there is no later batch to wait for.
#[tokio::test]
async fn a_closed_partition_merges_a_partial_batch() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    written(store.clone(), vec![span("a", at(9)), span("b", at(10))]).await;

    Compactor::new(store.clone())
        .compact::<Span>(&Path::from(Span::directory(at(9))), true)
        .await
        .unwrap();

    assert_eq!(listing(&store).await.len(), 1);
}

/// Five crumbs and one much larger file are a full batch by count, but merging
/// them would rewrite the large one to absorb a fraction of its size. Open,
/// that is work better left for when the crumbs have company.
#[tokio::test]
async fn a_large_file_is_not_rewritten_to_absorb_crumbs() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let big = (0..50_000).map(|i| span(&format!("big{i:05}"), at(9)));
    file(&store, big.collect()).await;

    let crumbs = (0..5).map(|i| span(&format!("crumb{i}"), at(9)));
    written(store.clone(), crumbs.collect()).await;

    // A one-row file still costs a parquet footer, so the large one has to be
    // genuinely large for the spread to separate them.
    let sizes = listing(&store).await;
    assert_eq!(sizes.len(), 6);

    let written_paths = Compactor::new(store.clone())
        .compact::<Span>(&Path::from(Span::directory(at(9))), false)
        .await
        .unwrap();

    assert!(written_paths.is_empty(), "the large file stays as it is");
    assert_eq!(listing(&store).await.len(), 6);
}

/// Closed, the same crumbs merge into the large file: rewriting it once costs
/// less than every later query opening five extra files.
#[tokio::test]
async fn a_closed_partition_absorbs_crumbs_into_a_large_file() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let big = (0..50_000).map(|i| span(&format!("big{i:05}"), at(9)));
    file(&store, big.collect()).await;

    let crumbs = (0..5).map(|i| span(&format!("crumb{i}"), at(9)));
    written(store.clone(), crumbs.collect()).await;

    Compactor::new(store.clone())
        .compact::<Span>(&Path::from(Span::directory(at(9))), true)
        .await
        .unwrap();

    assert_eq!(listing(&store).await.len(), 1);
}

/// A span inserted twice is one span: compaction keeps the newest and drops
/// the rest, so a plain read stops seeing both.
#[tokio::test]
async fn compaction_collapses_superseded_rows() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let mut checking_in = span("s1", at(9));
    let mut finished = span("s1", at(9));
    finished.common.received_at = Timestamp::from(at(10));
    finished.ended_at = Some(Timestamp::from(at(10)));
    checking_in.ended_at = None;

    written(store.clone(), vec![checking_in]).await;
    written(store.clone(), vec![finished]).await;

    assert_eq!(span_ids(store.clone()).await, ["s1", "s1"], "both stored");

    Compactor::new(store.clone())
        .compact::<Span>(&Path::from(Span::directory(at(9))), true)
        .await
        .unwrap();

    assert_eq!(span_ids(store.clone()).await, ["s1"], "one row survives");

    let catalog = Catalog::new(store).unwrap();
    catalog.register::<Span>().await.unwrap();
    let batches = catalog
        .sql_cross_org("select ended_at from spans")
        .await
        .unwrap();

    assert!(
        pretty_format_batches(&batches)
            .unwrap()
            .to_string()
            .contains("2026-08-17T10:00:00Z"),
        "the newest insert is the one kept"
    );
}

/// Compacts every partition it finds, leaving the newest alone: it is the only
/// one still being written to, so merging its two files is work the next flush
/// undoes.
#[tokio::test]
async fn compact_all_leaves_the_newest_partition() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    written(
        store.clone(),
        vec![span("mon1", at(9)), span("mon2", at(10))],
    )
    .await;
    written(
        store.clone(),
        vec![span("tue1", tuesday(9)), span("tue2", tuesday(10))],
    )
    .await;

    assert_eq!(listing(&store).await.len(), 4, "two files per partition");

    // Tuesday exists, so monday is closed; tuesday is the live one.
    Compactor::new(store.clone())
        .compact_all::<Span>()
        .await
        .unwrap();

    let after = listing(&store).await;
    assert_eq!(after.len(), 3, "monday merged, tuesday untouched");
    assert_eq!(span_ids(store).await, ["mon1", "mon2", "tue1", "tue2"]);
}
