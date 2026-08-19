use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use compact::Compactor;
use datafusion::object_store::ObjectStore;
use datafusion::object_store::local::LocalFileSystem;
use ingest::buffer::Buffer;
use ingest::writer::Writer;
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

    /// Receive OpenTelemetry traces, writing them as they arrive
    Otlp {
        /// The store's root directory
        path: PathBuf,

        /// The address to listen on
        #[arg(long, default_value = "0.0.0.0:4318")]
        addr: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Compact { path } => compact(path).await,
        Command::Otlp { path, addr } => otlp(path, addr).await,
    }
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
