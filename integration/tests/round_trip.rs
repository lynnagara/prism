use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::array::{Array, as_string_array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::object_store::ObjectStore;
use datafusion::object_store::memory::InMemory;
use ingest::buffer::Buffer;
use ingest::writer::Writer;
use query::Catalog;
use schema::record::Common;
use schema::spans::{Span, Status};
use schema::types::{Tags, Timestamp};

fn at(day: u32, hour: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, day, hour, 0, 0).unwrap()
}

fn span(trace: &str, span_id: &str, started_at: DateTime<Utc>) -> Span {
    Span {
        common: Common {
            organization_id: "4812".to_string(),
            project_id: "91733".to_string(),
            received_at: Timestamp::from(started_at),
        },
        span_id: span_id.to_string(),
        trace_id: trace.to_string(),
        parent_span_id: None,
        name: "GET /checkout".to_string(),
        started_at: Timestamp::from(started_at),
        ended_at: None,
        status: Status::Ok,
        status_message: None,
        tags: Tags::default(),
    }
}

/// Uses the real writer, so a layout change the reader doesn't follow fails.
async fn written(spans: Vec<Span>) -> Arc<dyn ObjectStore> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut buffer: Buffer<Span> = Buffer::new(Writer::new(store.clone()));

    for span in spans {
        buffer.push(span);
    }
    buffer.flush().await.expect("write to succeed");

    store
}

async fn catalog(store: Arc<dyn ObjectStore>) -> Catalog {
    let catalog = Catalog::new(store).unwrap();
    catalog.register::<Span>().await.unwrap();
    catalog
}

fn span_ids(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let strings = as_string_array(batch.column_by_name("span_id").unwrap());
            (0..strings.len())
                .map(|i| strings.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn a_written_span_reads_back() {
    let store = written(vec![span(&"c".repeat(32), "a1", at(17, 9))]).await;

    let batches = catalog(store)
        .await
        .sql_cross_org("select span_id, name, status from spans")
        .await
        .unwrap();

    assert_eq!(
        pretty_format_batches(&batches).unwrap().to_string(),
        "+---------+---------------+--------+\n\
         | span_id | name          | status |\n\
         +---------+---------------+--------+\n\
         | a1      | GET /checkout | ok     |\n\
         +---------+---------------+--------+"
    );
}

/// Rows are written in primary key order — trace, then span — so one trace's
/// spans are contiguous and a span's own rows sit together.
#[tokio::test]
async fn rows_are_stored_in_sort_order() {
    let store = written(vec![
        span("bbb", "b2", at(17, 11)),
        span("aaa", "a1", at(17, 9)),
        span("bbb", "b1", at(17, 10)),
    ])
    .await;

    let batches = catalog(store)
        .await
        .sql_cross_org("select span_id from spans")
        .await
        .unwrap();

    assert_eq!(span_ids(&batches), ["a1", "b1", "b2"]);
}

/// The partition is recovered from the path, so it is a column a query can
/// filter on without it ever being written into a file.
#[tokio::test]
async fn the_partition_is_a_column() {
    let store = written(vec![
        span("aaa", "monday", at(17, 9)),
        span("aaa", "tuesday", at(18, 9)),
    ])
    .await;

    let batches = catalog(store)
        .await
        .sql_cross_org("select span_id from spans where partition = '2026-08-17T00:00:00Z'")
        .await
        .unwrap();

    assert_eq!(span_ids(&batches), ["monday"]);
}

async fn explain(catalog: &Catalog, query: &str) -> String {
    let batches = catalog
        .sql_cross_org(&format!("explain {query}"))
        .await
        .expect("explain to succeed");

    pretty_format_batches(&batches)
        .expect("plan to format")
        .to_string()
}

/// The point of partitioning: a query for one day must not open the other
/// days' files. Datafusion reads `partition` and `received_at` as unrelated
/// columns, so without `PartitionedTable` this lists every file ever written.
#[tokio::test]
async fn prunes_partitions_by_received_at() {
    let store = written(vec![
        span("aaa", "monday", at(17, 9)),
        span("aaa", "tuesday", at(18, 9)),
    ])
    .await;

    let plan = explain(
        &catalog(store).await,
        "select count(*) from spans where received_at >= timestamp '2026-08-18T00:00:00Z'",
    )
    .await;

    assert!(plan.contains("partition=2026-08-18"), "{plan}");
    assert!(!plan.contains("partition=2026-08-17"), "{plan}");
}

/// An exact `received_at` narrows to the one partition holding it, and still
/// returns the row — the case where pruning is easiest to get wrong.
#[tokio::test]
async fn prunes_an_exact_received_at_without_losing_it() {
    let store = written(vec![
        span("aaa", "monday", at(17, 9)),
        span("aaa", "tuesday", at(18, 9)),
    ])
    .await;
    let catalog = catalog(store).await;
    let query = "select span_id from spans where received_at = timestamp '2026-08-17T09:00:00Z'";

    let batches = catalog.sql_cross_org(query).await.unwrap();
    assert_eq!(span_ids(&batches), ["monday"]);

    let plan = explain(&catalog, query).await;
    assert!(plan.contains("partition=2026-08-17"), "{plan}");
    assert!(!plan.contains("partition=2026-08-18"), "{plan}");
}

/// Tags survive the round trip, and a bare one — set with no value — is still
/// distinguishable from a tag that was never set.
#[tokio::test]
async fn tags_read_back() {
    let mut tagged = span("aaa", "tagged", at(17, 9));
    tagged.tags = Tags::from_iter([
        ("env", Some("prod".to_string())),
        ("blank", Some(String::new())),
        ("production", None),
    ]);
    let store = written(vec![tagged]).await;

    let batches = catalog(store)
        .await
        .sql_cross_org(
            "select tags['env'] as env,
                    tags['blank'] is null as blank_is_null,
                    tags['production'] is null as bare_is_null,
                    'production' = any(map_keys(tags)) as bare_set,
                    'nope' = any(map_keys(tags)) as absent_set
             from spans",
        )
        .await
        .unwrap();

    assert_eq!(
        pretty_format_batches(&batches).unwrap().to_string(),
        "+------+---------------+--------------+----------+------------+\n\
         | env  | blank_is_null | bare_is_null | bare_set | absent_set |\n\
         +------+---------------+--------------+----------+------------+\n\
         | prod | false         | true         | true     | false      |\n\
         +------+---------------+--------------+----------+------------+"
    );
}

#[tokio::test]
async fn a_tag_filters() {
    let mut tagged = span("aaa", "tagged", at(17, 9));
    tagged.tags = Tags::from_iter([("env", Some("prod".to_string()))]);
    let store = written(vec![tagged, span("aaa", "untagged", at(17, 10))]).await;

    let batches = catalog(store)
        .await
        .sql_cross_org("select span_id from spans where tags['env'] = 'prod'")
        .await
        .unwrap();

    assert_eq!(span_ids(&batches), ["tagged"]);
}

/// A cron checking in as it goes inserts the same span more than once. It is
/// one span, and the newest insert wins.
#[tokio::test]
async fn the_newest_insert_wins() {
    let started = at(17, 9);

    let mut checking_in = span("aaa", "s1", started);
    checking_in.common.received_at = Timestamp::from(started);

    let mut finished = span("aaa", "s1", started);
    finished.common.received_at = Timestamp::from(at(17, 10));
    finished.ended_at = Some(Timestamp::from(at(17, 10)));

    let store = written(vec![checking_in, finished]).await;
    let catalog = catalog(store).await;

    let raw = catalog
        .sql_cross_org("select span_id from spans")
        .await
        .unwrap();
    assert_eq!(span_ids(&raw), ["s1", "s1"], "both inserts are stored");

    let merged = catalog
        .sql_cross_org("select span_id, ended_at from spans_merged")
        .await
        .unwrap();

    assert_eq!(
        pretty_format_batches(&merged).unwrap().to_string(),
        "+---------+----------------------+\n\
         | span_id | ended_at             |\n\
         +---------+----------------------+\n\
         | s1      | 2026-08-17T10:00:00Z |\n\
         +---------+----------------------+"
    );
}

/// The merged view ranks rows, and a filter cannot be pushed below a window
/// without changing which row ranks first — so a time filter on the merged
/// table reads every partition, where the stored table reads one.
#[tokio::test]
async fn a_merged_read_prunes_partitions() {
    let store = written(vec![
        span("aaa", "monday", at(17, 9)),
        span("aaa", "tuesday", at(18, 9)),
    ])
    .await;

    let plan = explain(
        &catalog(store).await,
        "select count(*) from spans where received_at >= timestamp '2026-08-18T00:00:00Z'",
    )
    .await;

    assert!(!plan.contains("partition=2026-08-17"), "{plan}");
}
