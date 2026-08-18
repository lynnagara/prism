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
use schema::record::{PARTITION_COLUMN, Record};

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

    /// Registers `T`'s directory as a table named after it.
    pub fn register<T: Record>(&self) -> Result<()> {
        let url = ListingTableUrl::parse(format!("{OBJECT_STORE_URL}/{}/", T::TABLE))?;
        let sort_order = T::all_sort_columns()
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
        Ok(())
    }

    /// Reads every organization's data. Anything user-facing needs a scoped
    /// variant that injects an `organization_id` filter into each table scan.
    pub async fn sql_cross_org(&self, query: &str) -> Result<Vec<RecordBatch>> {
        self.ctx.sql(query).await?.collect().await
    }
}
