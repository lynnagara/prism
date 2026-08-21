use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

mod seed;

use clap::{Parser, Subcommand};
use compact::Compactor;
use datafusion::object_store::ObjectStore;
use datafusion::object_store::local::LocalFileSystem;
use datafusion::object_store::path::Path;
use futures::TryStreamExt;
use ingest::buffer::Buffer;
use ingest::writer::Writer;
use query::Catalog;
use schema::record::Record;
use schema::spans::Span;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
#[command(version, about = "A columnar store for telemetry")]
struct Cli {
    /// The store's root directory
    #[arg(long, env = "PRISM_STORE", default_value = ".", global = true)]
    path: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write spans across a range of days, so partitioning and compaction have
    /// more than today to work on
    Seed {
        /// How many days back to write, ending today
        #[arg(long, default_value_t = 30)]
        days: i64,

        /// Traces per day
        #[arg(long, default_value_t = 2500)]
        traces: u64,

        /// Where the generator starts. Left alone it differs every run, so
        /// seeding a store a second time cannot repeat the first run's ids.
        /// Give it a number to get the same store back.
        #[arg(long)]
        seed: Option<u64>,
    },

    /// Merge each partition's files, leaving the one still being written to
    Compact,

    /// Answer SQL over HTTP, for a UI to read
    Api {
        /// The address to listen on
        #[arg(long, default_value = "0.0.0.0:3000")]
        addr: SocketAddr,
    },

    /// Receive OpenTelemetry traces, writing them as they arrive
    Otlp {
        /// The address to listen on
        #[arg(long, default_value = "0.0.0.0:4318")]
        addr: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Seed { days, traces, seed } => {
            let seed = seed.unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|since| since.as_nanos() as u64)
                    .unwrap_or_default()
            });
            seed::run(store(cli.path)?, days, traces, seed).await
        }
        Command::Compact => compact(cli.path).await,
        Command::Api { addr } => api(cli.path, addr).await,
        Command::Otlp { addr } => otlp(cli.path, addr).await,
    }
}

async fn api(path: PathBuf, addr: SocketAddr) -> Result<()> {
    let catalog = Catalog::new(store(path)?)?;
    catalog.register::<Span>().await?;

    eprintln!("listening on http://{addr}/sql");
    query::api::serve(addr, catalog).await
}

async fn otlp(path: PathBuf, addr: SocketAddr) -> Result<()> {
    let buffer = Buffer::new(Writer::new(store(path)?));

    eprintln!("listening on http://{addr}/v1/traces");
    otlp::receiver::serve(addr, buffer).await
}

fn store(path: PathBuf) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(LocalFileSystem::new_with_prefix(path)?))
}

/// Reports what the store looks like either side, rather than what the run
/// wrote: a pass can merge the same partition more than once, so the files it
/// produced is not the number a person is asking about.
async fn compact(path: PathBuf) -> Result<()> {
    let store = store(path)?;
    let before = files(&store).await?;
    Compactor::new(store.clone()).compact_all::<Span>().await?;
    let after = files(&store).await?;

    println!("compacted {before} files into {after}");
    Ok(())
}

/// How many objects the store holds for this record type.
async fn files(store: &Arc<dyn ObjectStore>) -> Result<usize> {
    Ok(store
        .list(Some(&Path::from(Span::TABLE)))
        .try_fold(0, |n, _| async move { Ok(n + 1) })
        .await?)
}
