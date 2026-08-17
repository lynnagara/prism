use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, SecondsFormat, Utc};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::{SortColumn, lexsort_to_indices, take_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;

use crate::types::{ArrowField, Granularity, TimePartitioned, Timestamp};

type ColumnBuilder<T> = Box<dyn Fn(&[T]) -> ArrayRef>;

/// Names every partition directory, as `partition=<rfc3339>`. The `key=value`
/// shape is hive-style partitioning, the convention datafusion parses to
/// recover a column from the path, so a writer and a reader must spell the key
/// identically — a mismatch gives an empty table, not an error.
pub const PARTITION_COLUMN: &str = "partition";

/// Named because the columns [`Record::all_columns`] declares and the order
/// [`Record::sort_columns`] sorts by have to agree.
const ORGANIZATION_ID: &str = "organization_id";
const PROJECT_ID: &str = "project_id";
const RECEIVED_AT: &str = "received_at";

/// What the store needs from a record whatever else it holds: who the row
/// belongs to, and when the store saw it. Carried by every record so no record
/// declares these columns itself.
pub struct Common {
    pub organization_id: String,
    pub project_id: String,
    /// What rows are partitioned by. A span's `started_at` can precede the
    /// partition it lands in; this cannot.
    pub received_at: Timestamp,
}

/// One column of a [`Record`].
pub struct Column<T> {
    name: &'static str,
    data_type: DataType,
    nullable: bool,
    build: ColumnBuilder<T>,
}

impl<T: 'static> Column<T> {
    /// Declare one field as a column: its name, plus how to reach it on `T`.
    /// The Arrow type, nullability and conversion all come from `F`.
    pub fn new<F: ArrowField + 'static>(name: &'static str, accessor: fn(&T) -> &F) -> Self {
        Self {
            name,
            data_type: F::data_type(),
            nullable: F::NULLABLE,
            build: Box::new(move |rows: &[T]| F::build_array(rows.iter().map(accessor).map(Some))),
        }
    }
}

/// A record's columns, common first.
pub struct Columns<T>(Vec<Column<T>>);

impl<T> Columns<T> {
    /// Panics on a record declaring a name twice — including one `Common`
    /// already holds. A mistake in the declaration itself, so it fails as early
    /// as anything reads the columns.
    fn new(columns: Vec<Column<T>>) -> Self {
        let mut names = HashSet::new();
        for column in &columns {
            let name = column.name;
            assert!(names.insert(name), "two columns are declared as `{name}`");
        }

        Self(columns)
    }

    fn iter(&self) -> std::slice::Iter<'_, Column<T>> {
        self.0.iter()
    }

    fn arrow_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(
            self.iter()
                .map(|c| Field::new(c.name, c.data_type.clone(), c.nullable))
                .collect::<Vec<_>>(),
        ))
    }
}

/// Every type the store holds implements Record.
pub trait Record: Sized + 'static {
    /// Where this type is stored — must be unique across every `Record` type.
    const TABLE: &'static str;

    /// How finely this type's data is partitioned — a storage decision
    /// intrinsic to the type, not a per-deployment tuning knob.
    const GRANULARITY: Granularity;

    fn common(&self) -> &Common;

    /// The record's own columns, added to the ones every record shares.
    fn columns() -> Vec<Column<Self>>;

    /// RFC 3339 so the value is parseable as a timestamp rather than only a
    /// string, and spelled the same at every granularity so changing
    /// `GRANULARITY` later doesn't split the column into two formats.
    fn partition_dir(partition: DateTime<Utc>) -> String {
        format!(
            "{PARTITION_COLUMN}={}",
            partition.to_rfc3339_opts(SecondsFormat::Secs, true)
        )
    }

    /// Directory holding one partition's objects, from any instant inside it.
    fn directory(instant: DateTime<Utc>) -> String {
        let start = Timestamp::from(instant).partition_start(Self::GRANULARITY);
        format!("{}/{}", Self::TABLE, Self::partition_dir(start))
    }

    fn all_columns() -> Columns<Self> {
        let mut columns = vec![
            Column::new(ORGANIZATION_ID, |r: &Self| &r.common().organization_id),
            Column::new(PROJECT_ID, |r: &Self| &r.common().project_id),
            Column::new(RECEIVED_AT, |r: &Self| &r.common().received_at),
        ];
        columns.extend(Self::columns());

        Columns::new(columns)
    }

    /// What rows are clustered by within one project — the column this type's
    /// commonest lookup filters on, so a merged file's row groups each cover a
    /// contiguous run of it and the rest can be skipped. Which column that is
    /// depends entirely on how the type is read, so every type answers for
    /// itself.
    fn sort_columns() -> Vec<&'static str>;

    /// Tenancy first whatever the type: every user-facing query is scoped to
    /// one organization and project, so leading with them is what makes row
    /// groups selective for the reader that matters.
    fn all_sort_columns() -> Vec<&'static str> {
        let mut columns = vec![ORGANIZATION_ID, PROJECT_ID];
        columns.extend(Self::sort_columns());
        columns
    }

    fn sorted(batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
        let columns: Vec<SortColumn> = Self::all_sort_columns()
            .iter()
            .map(|name| SortColumn {
                values: batch
                    .column_by_name(name)
                    .expect("sort_columns names a column in the schema")
                    .clone(),
                options: None,
            })
            .collect();

        take_record_batch(&batch, &lexsort_to_indices(&columns, None)?)
    }

    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        let columns = Self::all_columns();
        let arrays = columns.iter().map(|c| (c.build)(rows)).collect();

        RecordBatch::try_new(columns.arrow_schema(), arrays)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Duplicated {
        common: Common,
    }

    impl Record for Duplicated {
        const TABLE: &'static str = "duplicated";
        const GRANULARITY: Granularity = Granularity::Day;

        fn common(&self) -> &Common {
            &self.common
        }

        fn columns() -> Vec<Column<Self>> {
            vec![Column::new("project_id", |d| &d.common.project_id)]
        }

        fn sort_columns() -> Vec<&'static str> {
            vec![]
        }
    }

    /// A schema with the same name twice resolves every lookup to the first, so
    /// the second column silently never reads — including a record redeclaring
    /// one `Common` already holds.
    #[test]
    #[should_panic(expected = "two columns are declared as `project_id`")]
    fn a_name_declared_twice_is_rejected() {
        Duplicated::all_columns();
    }
}
