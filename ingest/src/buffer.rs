use std::collections::BTreeMap;
use std::fmt;
use std::time::Instant;

use chrono::{DateTime, Utc};
use schema::record::Record;
use schema::types::TimePartitioned;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::task::JoinHandle;
use tokio::time::{Duration, interval};
use uuid::Uuid;

use crate::writer::{Batch, Writer};

/// Rows held before a flush is worth doing. Bounds what is held in memory more
/// than what a file costs — compaction is what makes files big.
const FLUSH_MAX_ROWS: usize = 10_000;

/// How long a row waits when the buffer never fills — the lag between a span
/// arriving and a query being able to find it.
const FLUSH_MAX_AGE: Duration = Duration::from_secs(10);

/// How often the buffer is asked whether it has waited long enough, since
/// nothing arriving is exactly the case that threshold exists for.
const TICK: Duration = Duration::from_secs(1);

/// Rows waiting before the queue is left to fill instead. Several flushes' worth,
/// so only writes that keep failing reach it.
const PENDING_MAX_ROWS: usize = 100_000;

/// Batches queued before senders are turned away. Each is one request's worth
/// of rows, so this is how far ingest can run ahead of a slow store.
const QUEUE_CAPACITY: usize = 64;

/// The queue is full: the store is behind. Senders retry, so this is the one
/// answer that neither drops the rows nor pretends they were stored.
#[derive(Debug)]
pub struct Full;

impl fmt::Display for Full {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ingest queue is full")
    }
}

impl std::error::Error for Full {}

/// Takes rows and writes them as files, on its own rather than when asked: a
/// caller hands them over and says when it is done.
pub struct Buffer<T> {
    batches: mpsc::Sender<Vec<T>>,
    task: JoinHandle<()>,
}

impl<T: Record + Send + 'static> Buffer<T> {
    pub fn new(writer: Writer) -> Self {
        let (batches, incoming) = mpsc::channel(QUEUE_CAPACITY);

        Self {
            batches,
            task: tokio::spawn(Pending::new(writer).run(incoming)),
        }
    }

    /// Queues rows without waiting for them to be stored, so the cost of a
    /// write never lands on whoever sent them.
    pub fn push(&self, rows: Vec<T>) -> Result<(), Full> {
        match self.batches.try_send(rows) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_) | TrySendError::Closed(_)) => Err(Full),
        }
    }

    /// Stops accepting rows and waits for what was accepted to be written.
    /// Without it the process can exit owing whatever is still buffered.
    pub async fn shutdown(self) {
        let Self { batches, task } = self;

        drop(batches);
        let _ = task.await;
    }
}

/// One partition's rows and the name they will be written under, set when the
/// partition is first seen so a retry reuses it.
struct File<T> {
    file_id: Uuid,
    rows: Vec<T>,
}

/// Rows waiting to be written, grouped by the partition they fall in. Lives in
/// the task, so nothing outside it can reach them.
struct Pending<T> {
    writer: Writer,
    partitions: BTreeMap<DateTime<Utc>, File<T>>,
    first_pushed_at: Option<Instant>,
}

impl<T: Record> Pending<T> {
    fn new(writer: Writer) -> Self {
        Self {
            writer,
            partitions: BTreeMap::new(),
            first_pushed_at: None,
        }
    }

    /// Rows in, files out: hold what arrives, write when it is worth writing,
    /// and write what is left once the sender is gone.
    async fn run(mut self, mut incoming: mpsc::Receiver<Vec<T>>) {
        let mut tick = interval(TICK);

        loop {
            tokio::select! {
                // Holding rows a failing store won't take is how a queue that
                // is never full hides that nothing is being written. Leaving
                // them queued instead is what senders can see.
                batch = incoming.recv(), if !self.is_full() || incoming.is_closed() => match batch {
                    Some(rows) => rows.into_iter().for_each(|row| self.add(row)),
                    // Every sender is gone and the queue is drained.
                    None => break,
                },
                _ = tick.tick() => {}
            }

            // A failed write leaves the rows held, so the next flush tries
            // them again.
            if self.should_flush()
                && let Err(error) = self.flush().await
            {
                eprintln!("flush failed: {error}");
            }
        }

        if let Err(error) = self.flush().await {
            eprintln!("flush failed: {error}");
        }
    }

    fn add(&mut self, row: T) {
        self.first_pushed_at.get_or_insert_with(Instant::now);

        let partition = row.common().received_at.partition_start(T::GRANULARITY);
        self.partitions
            .entry(partition)
            .or_insert_with(|| File {
                file_id: Uuid::now_v7(),
                rows: Vec::new(),
            })
            .rows
            .push(row);
    }

    /// Whether it is worth writing what is held: enough rows to make a file, or
    /// old enough that waiting for more costs a reader more than the file size
    /// saves.
    fn should_flush(&self) -> bool {
        self.rows() >= FLUSH_MAX_ROWS
            || self
                .first_pushed_at
                .is_some_and(|since| since.elapsed() >= FLUSH_MAX_AGE)
    }

    fn rows(&self) -> usize {
        self.partitions.values().map(|p| p.rows.len()).sum()
    }

    /// More held than a flush is failing to clear, so taking more on is only
    /// spending memory to hide it.
    fn is_full(&self) -> bool {
        self.rows() >= PENDING_MAX_ROWS
    }

    /// Removes each partition only after its write succeeds, so a failure
    /// leaves it (and any after it) untouched for the next flush.
    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let keys: Vec<DateTime<Utc>> = self.partitions.keys().copied().collect();

        for partition in keys {
            let file = &self.partitions[&partition];
            let rows = T::sorted(T::to_record_batch(&file.rows)?)?;
            let directory = T::directory(partition);

            self.writer
                .write(&Batch {
                    directory,
                    file_id: file.file_id,
                    rows,
                })
                .await?;

            self.partitions.remove(&partition);
        }

        // Only once everything is written: a failure leaves the clock running
        // on what is still held.
        self.first_pushed_at = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use datafusion::object_store::local::LocalFileSystem;
    use datafusion::object_store::memory::InMemory;
    use datafusion::object_store::{ObjectStore, path::Path};
    use futures::StreamExt;
    use schema::record::Common;
    use schema::spans::{Span, Status};
    use schema::types::{SpanId, Tags, Timestamp, TraceId};
    use std::sync::Arc;

    fn span(received_at: DateTime<Utc>) -> Span {
        Span {
            common: Common {
                organization_id: "4812".to_string(),
                project_id: "91733".to_string(),
                received_at: Timestamp::from(received_at),
            },
            span_id: SpanId::from([0xaa; 8]),
            trace_id: TraceId::from([0xcc; 16]),
            parent_span_id: None,
            service: None,
            name: "GET /checkout".to_string(),
            started_at: Timestamp::from(received_at),
            ended_at: None,
            status: Status::Ok,
            status_message: None,
            tags: Tags::default(),
        }
    }

    fn at(day: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, day, 9, 0, 0).unwrap()
    }

    /// A store rooted at a file, so nothing can be written under it — which
    /// is what a store refusing writes looks like from here.
    fn failing() -> Arc<dyn ObjectStore> {
        Arc::new(LocalFileSystem::new_with_prefix("/dev/null").unwrap())
    }

    async fn stored(store: &Arc<dyn ObjectStore>) -> usize {
        store.list(Some(&Path::from(Span::TABLE))).count().await
    }

    /// Nothing is worth writing until there is enough of it.
    #[test]
    fn a_buffer_fills_before_it_is_worth_flushing() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut pending: Pending<Span> = Pending::new(Writer::new(store));

        assert!(!pending.should_flush(), "empty");

        pending.add(span(at(17)));
        assert!(!pending.should_flush(), "one row is not a file");
    }

    /// A trickle still has to reach a reader, so waiting is what ends the wait.
    #[test]
    fn a_row_waiting_long_enough_is_worth_flushing() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut pending: Pending<Span> = Pending::new(Writer::new(store));

        pending.add(span(at(17)));
        pending.first_pushed_at = Some(Instant::now() - FLUSH_MAX_AGE);

        assert!(pending.should_flush());
    }

    #[tokio::test]
    async fn flush_writes_one_object_per_partition() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut pending: Pending<Span> = Pending::new(Writer::new(store.clone()));

        pending.add(span(at(17)));
        pending.add(span(at(17)));
        pending.add(span(at(18)));

        pending.flush().await.unwrap();

        assert_eq!(stored(&store).await, 2);
    }

    /// Too few rows to be worth a file, and no wait long enough to force one —
    /// shutdown is what gets them stored.
    #[tokio::test]
    async fn shutdown_writes_what_is_still_pending() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer: Buffer<Span> = Buffer::new(Writer::new(store.clone()));

        buffer.push(vec![span(at(17)), span(at(17))]).unwrap();

        buffer.shutdown().await;
        assert_eq!(stored(&store).await, 1);
    }

    /// Writes that keep failing would otherwise pile up in memory while the
    /// queue drains, leaving senders with no sign that nothing is stored.
    #[tokio::test]
    async fn a_store_that_takes_nothing_turns_senders_away() {
        let buffer: Buffer<Span> = Buffer::new(Writer::new(failing()));

        // Enough to fill what waits, and then the queue behind it.
        let waiting = (0..PENDING_MAX_ROWS).map(|_| span(at(17))).collect();
        buffer.push(waiting).expect("room to queue");

        for _ in 0..QUEUE_CAPACITY + 1 {
            tokio::task::yield_now().await;

            if buffer.push(vec![span(at(17))]).is_err() {
                return;
            }
        }

        panic!("a store taking nothing was still accepting rows");
    }

    /// The store being behind has to reach the sender, because dropping rows
    /// and accepting rows there is no room for both lose them.
    #[tokio::test]
    async fn a_full_queue_turns_senders_away() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer: Buffer<Span> = Buffer::new(Writer::new(store));

        let refused = (0..QUEUE_CAPACITY + 1).filter(|_| buffer.push(vec![span(at(17))]).is_err());

        assert!(
            refused.count() > 0,
            "queued more batches than there is room for"
        );
    }
}
