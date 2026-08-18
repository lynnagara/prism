mod table;

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::object_store::ObjectStore;
use datafusion::prelude::{SessionContext, col};
use schema::record::{MergeStrategy, PARTITION_COLUMN, Record};

use crate::table::PartitionedTable;

/// Backend-agnostic url the object store is mounted under, so table paths are
/// identical whatever object storage resolves them. Datafusion keys registered
/// stores by scheme *and* host, so both must appear here.
const OBJECT_STORE_URL: &str = "prism://store";

pub struct Catalog {
    ctx: SessionContext,
}

impl Catalog {
    pub fn new(store: Arc<dyn ObjectStore>) -> Result<Self> {
        let ctx = SessionContext::new();
        ctx.register_object_store(ObjectStoreUrl::parse(OBJECT_STORE_URL)?.as_ref(), store);

        Ok(Self { ctx })
    }

    /// Registers `T`'s files as the table `T::TABLE`, and — if `T` declares a
    /// merge — `<table>_merged` beside it, holding one row per primary key.
    ///
    /// The plain name is the cheap one: it prunes, and it can show a row a
    /// later insert superseded. The merged one is correct and pays a sort over
    /// everything it reads, so the caller chooses which they want.
    pub async fn register<T: Record>(&self) -> Result<()> {
        let url = ListingTableUrl::parse(format!("{OBJECT_STORE_URL}/{}/", T::TABLE))?;
        let sort_order = T::all_primary_key()
            .iter()
            .map(|name| col(*name).sort(true, false))
            .collect();

        let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_file_sort_order(vec![sort_order])
            // Utf8, not a timestamp: datafusion turns `partition = x` into a
            // listing prefix built from the literal's `Display`, which for a
            // timestamp is epoch micros and matches no directory.
            .with_table_partition_cols(vec![(PARTITION_COLUMN.to_string(), DataType::Utf8)]);
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(T::schema());

        // Wrapped so a filter on `received_at` also bounds which partitions can
        // match; a bare ListingTable reads the two as unrelated.
        let table = PartitionedTable::new(ListingTable::try_new(config)?, T::GRANULARITY);

        self.ctx.register_table(T::TABLE, Arc::new(table))?;

        if let Some(strategy) = T::merge() {
            self.create_merged_view::<T>(strategy).await?;
        }
        Ok(())
    }

    /// One row per primary key *within a partition* — the partition is part of
    /// the ranking key, so a record whose rows straddle two still shows both
    /// until compaction consolidates them.
    ///
    /// Ranking rather than grouping keeps every column without naming an
    /// aggregate for each, which no aggregate could do for `tags`.
    async fn create_merged_view<T: Record>(&self, strategy: MergeStrategy) -> Result<()> {
        let schema = T::schema();
        let mut names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        names.push(PARTITION_COLUMN);
        let columns = names.join(", ");

        // The partition joins the key so that ranking happens within one, which is
        // what lets a filter on it be pushed below the window: dropping whole
        // partitions cannot change the ranks inside those that remain.
        let mut key = T::all_primary_key();
        key.push(PARTITION_COLUMN);
        let key = key.join(", ");

        let MergeStrategy::Latest { version } = strategy;

        let sql = format!(
            "create view {table}_merged as \
             select {columns} from ( \
               select {columns}, \
                 row_number() over (partition by {key} order by {version} desc) as merge_rank \
               from {table} \
             ) where merge_rank = 1",
            table = T::TABLE,
        );

        self.ctx.sql(&sql).await?;
        Ok(())
    }

    /// Reads every organization's data. Anything user-facing needs a scoped
    /// variant that injects an `organization_id` filter into each table scan.
    pub async fn sql_cross_org(&self, query: &str) -> Result<Vec<RecordBatch>> {
        self.ctx.sql(query).await?.collect().await
    }
}
