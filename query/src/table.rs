//! Extends datafusion's `ListingTable` so a query filtered on the `received_at`
//! column only opens the relevant partitions.
//!
//! Datafusion reads `partition` and `received_at` as unrelated columns — it has
//! no notion of one being derived from another — so a filter on `received_at`
//! prunes nothing.
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::ListingTable;
use datafusion::error::Result;
use datafusion::logical_expr::{BinaryExpr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Expr, col, lit};
use schema::record::{PARTITION_COLUMN, RECEIVED_AT};
use schema::types::{Granularity, TimePartitioned, Timestamp};

#[derive(Debug)]
pub struct PartitionedTable {
    inner: Arc<ListingTable>,
    granularity: Granularity,
}

impl PartitionedTable {
    pub fn new(inner: ListingTable, granularity: Granularity) -> Self {
        Self {
            inner: Arc::new(inner),
            granularity,
        }
    }

    /// Pushes `filter` down to `partition`, to avoid opening every file.
    ///
    /// A row's partition is its `received_at` truncated to the granularity, and
    /// truncating preserves order — so every row matching `received_at >= t` is
    /// in a partition at or after `t` truncated, and the ones before it cannot
    /// hold a match.
    ///
    /// It narrows to whole partitions and no further, so rows inside a matching
    /// one can still fail the original filter. That filter has to keep running
    /// per row, which reporting it `Inexact` below is what ensures.
    fn partition_filter(&self, filter: &Expr) -> Option<Expr> {
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter else {
            return None;
        };

        let (op, at) = match (left.as_ref(), right.as_ref()) {
            (Expr::Column(column), Expr::Literal(at, _)) if column.name == RECEIVED_AT => (*op, at),
            (Expr::Literal(at, _), Expr::Column(column)) if column.name == RECEIVED_AT => {
                (op.swap()?, at)
            }
            _ => return None,
        };

        let partition = lit(partition_dir_value(
            Timestamp::from(datetime(at)?).partition_start(self.granularity),
        ));

        Some(match op {
            Operator::Gt | Operator::GtEq => col(PARTITION_COLUMN).gt_eq(partition),
            Operator::Lt | Operator::LtEq => col(PARTITION_COLUMN).lt_eq(partition),
            Operator::Eq => col(PARTITION_COLUMN).eq(partition),
            _ => return None,
        })
    }
}

/// The instant a timestamp literal holds, ready to truncate to a partition.
/// `None` for any other literal, which skips the pruning without changing
/// results; coercion runs before a provider sees a filter, so it rarely does.
fn datetime(at: &ScalarValue) -> Option<DateTime<Utc>> {
    match at {
        ScalarValue::TimestampMicrosecond(Some(at), _) => DateTime::from_timestamp_micros(*at),
        _ => None,
    }
}

/// Spelled exactly as the directory is, since that string is the column's
/// value. Rfc3339 orders the same either way, so comparing it as text still
/// selects the right partitions.
fn partition_dir_value(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[async_trait]
impl TableProvider for PartitionedTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    /// Unchanged from the inner table: what `scan` adds is weaker than the
    /// filters it derived them from, so those must still be applied to rows,
    /// which is what `Inexact` asks datafusion to do.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    /// Required by the trait, but every argument it takes is one `ScanArgs`
    /// carries, so it only repackages them and lets the real work happen once.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let args = ScanArgs::default()
            .with_projection(projection.map(|projection| projection.as_slice()))
            .with_filters(Some(filters))
            .with_limit(limit);

        Ok(self.scan_with_args(state, args).await?.into_inner())
    }

    /// Adds the derived filters here rather than in [`Self::scan`] so that
    /// anything else `ScanArgs` carries reaches the inner table untouched —
    /// flattening it to `scan`'s four arguments would silently drop any field
    /// datafusion adds later.
    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        let given = args.filters().unwrap_or(&[]);

        let mut pushed = given.to_vec();
        pushed.extend(
            given
                .iter()
                .filter_map(|filter| self.partition_filter(filter)),
        );

        self.inner
            .scan_with_args(state, args.with_filters(Some(&pushed)))
            .await
    }
}
