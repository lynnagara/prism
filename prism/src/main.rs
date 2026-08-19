use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use compact::Compactor;
use datafusion::object_store::ObjectStore;
use datafusion::object_store::local::LocalFileSystem;
use schema::spans::Span;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
#[command(version, about = "A columnar store for telemetry")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Merge each partition's files, leaving the one still being written to
    Compact {
        /// The store's root directory
        path: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Compact { path } => compact(path).await,
    }
}

async fn compact(path: PathBuf) -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(path)?);
    let written = Compactor::new(store).compact_all::<Span>().await?;

    for path in &written {
        println!("{path}");
    }
    eprintln!("{} merged", written.len());
    Ok(())
}
