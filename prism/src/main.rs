use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use compact::Compactor;
use datafusion::object_store::ObjectStore;
use datafusion::object_store::local::LocalFileSystem;
use ingest::buffer::Buffer;
use ingest::writer::Writer;
use query::Catalog;
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

async fn compact(path: PathBuf) -> Result<()> {
    let written = Compactor::new(store(path)?).compact_all::<Span>().await?;

    for path in &written {
        println!("{path}");
    }
    eprintln!("{} merged", written.len());
    Ok(())
}
