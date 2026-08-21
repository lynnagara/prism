use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};
use datafusion::arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, MapBuilder, MapFieldNames, StringArray, StringBuilder,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};

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

/// An OpenTelemetry id — 16 bytes for a trace, 8 for a span — stored as bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Id<const N: usize>([u8; N]);

pub type TraceId = Id<16>;
pub type SpanId = Id<8>;

impl<const N: usize> From<[u8; N]> for Id<N> {
    fn from(bytes: [u8; N]) -> Self {
        Id(bytes)
    }
}

/// Hex, which is how an id is written everywhere a person sees one — a log
/// line, a url, a filter someone pastes.
impl<const N: usize> fmt::Display for Id<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl<const N: usize> ArrowField for Id<N> {
    fn data_type() -> DataType {
        DataType::FixedSizeBinary(N as i32)
    }

    fn build_array<'a>(values: impl Iterator<Item = Option<&'a Self>>) -> ArrayRef {
        let mut builder = FixedSizeBinaryBuilder::new(N as i32);
        for value in values {
            match value {
                Some(id) => builder.append_value(id.0).expect("the width is fixed"),
                None => builder.append_null(),
            }
        }
        Arc::new(builder.finish())
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

/// A map is stored as a list of key-value structs, and the names of those
/// nested fields are part of its type — so the type declared for the column and
/// the array built for it must use the same ones, or the batch is a mismatch.
fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_string(),
        key: "keys".to_string(),
        value: "values".to_string(),
    }
}

/// Free-form key-value annotations on a record. Sorted, so the same tags always
/// encode identically. A value is optional — a bare tag is a key with none —
/// and `tags['x']` reads that as null, so use `'x' = any(map_keys(tags))`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tags(BTreeMap<String, Option<String>>);

impl<K: Into<String>, V: Into<Option<String>>> FromIterator<(K, V)> for Tags {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(tags: I) -> Self {
        Tags(
            tags.into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        )
    }
}

impl ArrowField for Tags {
    fn data_type() -> DataType {
        let names = map_field_names();

        DataType::Map(
            Arc::new(Field::new(
                names.entry,
                DataType::Struct(Fields::from(vec![
                    Field::new(names.key, DataType::Utf8, false),
                    Field::new(names.value, DataType::Utf8, true),
                ])),
                false,
            )),
            false,
        )
    }

    fn build_array<'a>(values: impl Iterator<Item = Option<&'a Self>>) -> ArrayRef {
        let mut builder = MapBuilder::new(
            Some(map_field_names()),
            StringBuilder::new(),
            StringBuilder::new(),
        );

        for row_tags in values {
            if let Some(tags) = row_tags {
                for (key, value) in &tags.0 {
                    builder.keys().append_value(key);
                    builder.values().append_option(value.as_deref());
                }
            }
            // Once per row, not per tag: this is what marks where a row's
            // entries end, so a row with none is an empty map.
            builder
                .append(true)
                .expect("keys and values are appended in pairs");
        }

        Arc::new(builder.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use datafusion::arrow::array::{Array, MapArray};

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

    fn map(tags: &[&Tags]) -> MapArray {
        Tags::build_array(tags.iter().copied().map(Some))
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("a map column")
            .clone()
    }

    /// Sorted on the way in, so two records with the same tags in a different
    /// order encode to the same bytes.
    #[test]
    fn tags_encode_in_key_order() {
        let tags = Tags::from_iter([("b", "2".to_string()), ("a", "1".to_string())]);
        let keys = map(&[&tags]);
        let keys = keys
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string keys");

        assert_eq!(keys.value(0), "a");
        assert_eq!(keys.value(1), "b");
    }

    /// A bare tag is a key with no value, which is not the same as the key
    /// being absent — `map_keys` finds it, `tags['x']` reads null.
    #[test]
    fn a_tag_can_have_no_value() {
        let tags = Tags::from_iter([("bare", None), ("set", Some("1".to_string()))]);
        let values = map(&[&tags]);
        let values = values
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert!(values.is_null(0), "the bare tag has no value");
        assert_eq!(values.value(1), "1");
    }

    /// One entry per row whatever its tag count, so a row with none is an
    /// empty map rather than one that borrowed the next row's entries.
    #[test]
    fn a_row_without_tags_is_an_empty_map() {
        let tagged = Tags::from_iter([("a", "1".to_string())]);
        let rows = map(&[&Tags::default(), &tagged]);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows.value_length(0), 0);
        assert_eq!(rows.value_length(1), 1);
    }
}
