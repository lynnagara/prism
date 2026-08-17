use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};
use datafusion::arrow::array::{ArrayRef, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};

/// A field type that knows how to become an Arrow column
pub trait ArrowField {
    /// Whether a column of this type is declared nullable. Parquet enforces
    /// the declaration, so a null reaching a column declared `false` fails the
    /// write rather than passing silently.
    const NULLABLE: bool = false;

    fn data_type() -> DataType;

    /// Values arrive as `Option` whether or not the column can hold nulls, so
    /// there is one path to build on: a non-null column simply never sees a
    /// `None`.
    fn build_array<'a>(values: impl Iterator<Item = Option<&'a Self>>) -> ArrayRef
    where
        Self: 'a;
}

/// An absent value is the same Arrow type as a present one, only nullable
impl<T: ArrowField> ArrowField for Option<T> {
    const NULLABLE: bool = true;

    fn data_type() -> DataType {
        T::data_type()
    }

    fn build_array<'a>(values: impl Iterator<Item = Option<&'a Self>>) -> ArrayRef
    where
        Self: 'a,
    {
        T::build_array(values.map(|value| value.and_then(Option::as_ref)))
    }
}

/// How finely partitions are bucketed — each record type decides which one
/// applies; this is just the vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Granularity {
    Day,
}

impl Granularity {
    /// How long one partition covers. Partitions floor against this, so any
    /// fixed interval works.
    pub fn duration(&self) -> TimeDelta {
        match self {
            Granularity::Day => TimeDelta::days(1),
        }
    }
}

pub trait TimePartitioned {
    fn partition_start(&self, granularity: Granularity) -> DateTime<Utc>;
}

pub fn utc_timestamp() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

/// Tenancy ids and free text are stored as strings. The store carries no knowledge
/// of how org and project ids are minted or their format.
impl ArrowField for String {
    fn data_type() -> DataType {
        DataType::Utf8
    }

    fn build_array<'a>(values: impl Iterator<Item = Option<&'a Self>>) -> ArrayRef {
        Arc::new(StringArray::from_iter(
            values.map(|value| value.map(String::as_str)),
        ))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timestamp(DateTime<Utc>);

impl TimePartitioned for Timestamp {
    /// Floored against the unix epoch, which is midnight, so a granularity
    /// dividing a day lands on the boundary its name implies.
    fn partition_start(&self, granularity: Granularity) -> DateTime<Utc> {
        let step = granularity.duration().num_seconds();
        DateTime::from_timestamp(self.0.timestamp().div_euclid(step) * step, 0)
            .expect("a floored timestamp is in range")
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(value: DateTime<Utc>) -> Self {
        Timestamp(value)
    }
}

impl ArrowField for Timestamp {
    fn data_type() -> DataType {
        utc_timestamp()
    }

    fn build_array<'a>(values: impl Iterator<Item = Option<&'a Self>>) -> ArrayRef {
        Arc::new(
            TimestampMicrosecondArray::from_iter(
                values.map(|value| value.map(|value| value.0.timestamp_micros())),
            )
            .with_timezone("UTC"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn partition_start_truncates_to_granularity() {
        let ts = Timestamp::from(Utc.with_ymd_and_hms(2026, 8, 2, 14, 37, 22).unwrap());

        assert_eq!(
            ts.partition_start(Granularity::Day),
            Utc.with_ymd_and_hms(2026, 8, 2, 0, 0, 0).unwrap()
        );
    }

    /// Whether a field is optional decides only that its column is nullable,
    /// never what type it holds.
    #[test]
    fn option_keeps_the_inner_data_type() {
        assert_eq!(<Option<Timestamp>>::data_type(), Timestamp::data_type());
    }

    /// A `None` field — `ended_at` on a span that never finished — is a null in
    /// the column, at that row, leaving the values around it untouched.
    #[test]
    fn none_becomes_null() {
        let at = Utc::now();
        let values = [Some(Timestamp::from(at)), None];

        let array = <Option<Timestamp>>::build_array(values.iter().map(Some));

        assert_eq!(array.len(), 2);
        assert_eq!(array.null_count(), 1);
        assert!(array.is_null(1));

        let timestamps = array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("an optional timestamp column is still a timestamp column");
        assert_eq!(timestamps.value(0), at.timestamp_micros());
    }
}
