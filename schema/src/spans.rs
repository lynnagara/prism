use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::DataType;

use crate::record::{Column, Common, Record};
use crate::types::{ArrowField, Granularity, Tags, Timestamp};

/// Span status codes. `Unset` is default.
pub enum Status {
    Unset,
    Ok,
    Error,
}

impl Status {
    fn as_str(&self) -> &'static str {
        match self {
            Status::Unset => "unset",
            Status::Ok => "ok",
            Status::Error => "error",
        }
    }
}

/// Three values across every row, so parquet dictionary-encodes it to nearly
/// nothing — and `where status = 'error'` is the query people write.
impl ArrowField for Status {
    fn data_type() -> DataType {
        DataType::Utf8
    }

    fn build_array<'a>(values: impl Iterator<Item = Option<&'a Self>>) -> ArrayRef {
        Arc::new(StringArray::from_iter(
            values.map(|value| value.map(Status::as_str)),
        ))
    }
}

/// One unit of work, written as a single row. A span that started and never
/// finished has no `ended_at`, which is why it is the one timestamp that can be
/// absent.
pub struct Span {
    pub common: Common,
    pub span_id: String,
    pub trace_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub started_at: Timestamp,
    pub ended_at: Option<Timestamp>,
    pub status: Status,
    /// Why it failed, in whatever words the caller used. Only set when
    /// `status` is `Error`.
    pub status_message: Option<String>,
    pub tags: Tags,
}

impl Record for Span {
    const TABLE: &'static str = "spans";
    const GRANULARITY: Granularity = Granularity::Day;

    fn common(&self) -> &Common {
        &self.common
    }

    fn columns() -> Vec<Column<Self>> {
        vec![
            Column::new("span_id", |s| &s.span_id),
            Column::new("trace_id", |s| &s.trace_id),
            Column::new("parent_span_id", |s| &s.parent_span_id),
            Column::new("name", |s| &s.name),
            Column::new("started_at", |s| &s.started_at),
            Column::new("ended_at", |s| &s.ended_at),
            Column::new("status", |s| &s.status),
            Column::new("status_message", |s| &s.status_message),
            Column::new("tags", |s| &s.tags),
        ]
    }

    /// A trace is read whole — every span sharing a `trace_id` — so clustering
    /// on it is what lets that read skip row groups. `started_at` second
    /// returns a trace already in the order a waterfall draws it.
    fn sort_columns() -> Vec<&'static str> {
        vec!["trace_id", "started_at"]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};

    fn at(hour: u32) -> Timestamp {
        Timestamp::from(Utc.with_ymd_and_hms(2026, 8, 17, hour, 0, 0).unwrap())
    }

    /// A finished root span, and an unfinished child of it.
    fn spans() -> Vec<Span> {
        vec![
            Span {
                common: Common {
                    organization_id: "4812".to_string(),
                    project_id: "91733".to_string(),
                    received_at: at(9),
                },
                span_id: "a".repeat(16),
                trace_id: "c".repeat(32),
                parent_span_id: None,
                name: "GET /checkout".to_string(),
                started_at: at(9),
                ended_at: Some(at(10)),
                status: Status::Ok,
                status_message: None,
                tags: Tags::from_iter([("env", Some("prod".to_string()))]),
            },
            Span {
                common: Common {
                    organization_id: "4812".to_string(),
                    project_id: "91733".to_string(),
                    received_at: at(9),
                },
                span_id: "b".repeat(16),
                trace_id: "c".repeat(32),
                parent_span_id: Some("a".repeat(16)),
                name: "charge card".to_string(),
                started_at: at(9),
                ended_at: None,
                status: Status::Unset,
                status_message: None,
                tags: Tags::default(),
            },
        ]
    }

    /// Every value in one column, as text, so a column reads the same however
    /// it is typed. Nulls come back as "null".
    fn column(batch: &RecordBatch, name: &str) -> Vec<String> {
        let array = batch.column_by_name(name).expect("column exists");
        let options = FormatOptions::default().with_null("null");
        let formatter = ArrayFormatter::try_new(array, &options).unwrap();

        (0..array.len())
            .map(|i| formatter.value(i).to_string())
            .collect()
    }

    #[test]
    fn a_span_becomes_a_row() {
        let batch = Span::to_record_batch(&spans()).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(column(&batch, "span_id"), ["a".repeat(16), "b".repeat(16)]);
        assert_eq!(column(&batch, "name"), ["GET /checkout", "charge card"]);
        assert_eq!(column(&batch, "project_id"), ["91733", "91733"]);
        assert_eq!(column(&batch, "status"), ["ok", "unset"]);
    }

    #[test]
    fn a_span_that_never_finished_has_no_ended_at() {
        let batch = Span::to_record_batch(&spans()).unwrap();

        assert_eq!(column(&batch, "ended_at"), ["2026-08-17T10:00:00Z", "null"]);
        assert_eq!(
            column(&batch, "parent_span_id"),
            ["null".to_string(), "a".repeat(16)]
        );
    }
}
