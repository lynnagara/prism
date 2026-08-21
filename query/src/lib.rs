pub mod api;
mod table;

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::view::ViewTable;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::object_store::ObjectStore;
use datafusion::prelude::{SessionContext, col};
use schema::record::{MergeStrategy, PARTITION_COLUMN, Record};
use store::{LiveStore, OBJECT_STORE_URL};

use crate::table::PartitionedTable;

pub struct Catalog {
    ctx: SessionContext,
}

impl Catalog {
    pub fn new(store: Arc<dyn ObjectStore>) -> Result<Self> {
        let ctx = SessionContext::new();

        // Wrapped here rather than by the caller, so no query can be handed a
        // store that still lists the files a merge replaced.
        ctx.register_object_store(
            ObjectStoreUrl::parse(OBJECT_STORE_URL)?.as_ref(),
            Arc::new(LiveStore::new(store)),
        );

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
        let sort_order = T::sort_order()
            .iter()
            .map(|(name, ascending)| col(*name).sort(*ascending, false))
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
        let table = PartitionedTable::new(
            Arc::new(ListingTable::try_new(config)?),
            T::partitioned_by().column,
            T::GRANULARITY,
        );

        self.ctx.register_table(T::TABLE, Arc::new(table))?;

        if let Some(strategy) = T::merge() {
            self.register_merged::<T>(strategy).await?;
        }
        Ok(())
    }

    /// One row per primary key *within a partition* — the partition is part of
    /// the ranking key, so a record whose rows straddle two still shows both.
    async fn register_merged<T: Record>(&self, strategy: MergeStrategy) -> Result<()> {
        let schema = T::schema();
        let mut names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        names.push(PARTITION_COLUMN);
        let columns = names.join(", ");

        let mut key = T::all_primary_key();
        key.push(PARTITION_COLUMN);
        let key = key.join(", ");

        let MergeStrategy::Latest { version } = strategy;

        // Ranked rather than grouped: no aggregate could combine two `tags`.
        let sql = format!(
            "select {columns} from ( \
               select {columns}, \
                 row_number() over (partition by {key} order by {version} desc) as merge_rank \
               from {table} \
             ) where merge_rank = 1",
            table = T::TABLE,
        );

        // Wrapped like the stored rows are, so a `received_at` filter still
        // bounds the partitions: the view applies what it is handed above its
        // own plan, and the optimizer pushes it below the window from there.
        let plan = self.ctx.sql(&sql).await?.into_unoptimized_plan();
        let merged = PartitionedTable::new(
            Arc::new(ViewTable::new(plan, None)),
            T::partitioned_by().column,
            T::GRANULARITY,
        );

        self.ctx
            .register_table(format!("{}_merged", T::TABLE), Arc::new(merged))?;
        Ok(())
    }

    /// Reads every organization's data. Anything user-facing needs a scoped
    /// variant that injects an `organization_id` filter into each table scan.
    pub async fn sql_cross_org(&self, query: &str) -> Result<Vec<RecordBatch>> {
        self.ctx.sql(query).await?.collect().await
    }
}
