use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use compact::Compactor;
use datafusion::arrow::array::Array;
use datafusion::object_store::memory::InMemory;
use datafusion::object_store::path::Path;
use datafusion::object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use futures::TryStreamExt;
use ingest::buffer::Buffer;
use ingest::writer::Writer;
use query::Catalog;
use schema::record::{Common, Record};
use schema::spans::{Span, Status};
use schema::types::{Tags, Timestamp};

fn at(hour: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 17, hour, 0, 0).unwrap()
}

fn span(span_id: &str, received_at: DateTime<Utc>) -> Span {
    Span {
        common: Common {
            organization_id: "4812".to_string(),
            project_id: "91733".to_string(),
            received_at: Timestamp::from(received_at),
        },
        span_id: span_id.to_string(),
        trace_id: "aaa".to_string(),
        parent_span_id: None,
        name: "GET /checkout".to_string(),
        started_at: Timestamp::from(received_at),
        ended_at: None,
        status: Status::Ok,
        status_message: None,
        tags: Tags::default(),
    }
}

/// One file per flush, which is what gives a partition several to merge.
async fn written(store: Arc<dyn ObjectStore>, spans: Vec<Span>) {
    let mut buffer: Buffer<Span> = Buffer::new(Writer::new(store));

    for span in spans {
        buffer.push(span);
        buffer.flush().await.expect("write to succeed");
    }
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
            let ids =
                datafusion::arrow::array::as_string_array(batch.column_by_name("span_id").unwrap());
            (0..ids.len())
                .map(|i| ids.value(i).to_string())
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
        .compact::<Span>(&directory)
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
        .compact::<Span>(&directory)
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
#[ignore = "a merged file's sources are still read until LiveStore hides them"]
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
        .compact::<Span>(&Path::from(Span::directory(at(9))))
        .await
        .unwrap();

    assert_eq!(listing(&store).await.len(), 1, "merged down to one file");
    assert_eq!(
        span_ids(store).await,
        ["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]
    );
}
