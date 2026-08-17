use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;

use crate::types::{ArrowField, Timestamp};

type ColumnBuilder<T> = Box<dyn Fn(&[T]) -> ArrayRef>;

/// What the store needs from a record whatever else it holds: who the row
/// belongs to, and when the store saw it. Carried by every record so no record
/// declares these columns itself.
pub struct Common {
    pub organization_id: String,
    pub project_id: String,
    /// What rows are partitioned by. A span's `start_ts` can precede the
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
    fn common(&self) -> &Common;

    /// The record's own columns, added to the ones every record shares.
    fn columns() -> Vec<Column<Self>>;

    fn all_columns() -> Columns<Self> {
        let mut columns = vec![
            Column::new("organization_id", |r: &Self| &r.common().organization_id),
            Column::new("project_id", |r: &Self| &r.common().project_id),
            Column::new("received_at", |r: &Self| &r.common().received_at),
        ];
        columns.extend(Self::columns());

        Columns::new(columns)
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
        fn common(&self) -> &Common {
            &self.common
        }

        fn columns() -> Vec<Column<Self>> {
            vec![Column::new("project_id", |d| &d.common.project_id)]
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
