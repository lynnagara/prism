//! SQL in, rows out. What builds the queries lives elsewhere — for now the UI,
//! later something between it and here.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::json::ArrayWriter;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::signal;

use crate::Catalog;

#[derive(Deserialize)]
pub struct Request {
    pub sql: String,
}

pub fn routes(catalog: Arc<Catalog>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/sql", post(sql))
        .with_state(catalog)
}

/// The UI ships in the binary and is served from here, so it shares an origin
/// with the endpoint it queries and needs no cross-origin setup.
async fn index() -> Html<&'static str> {
    Html(include_str!("../../web/index.html"))
}

pub async fn serve(
    address: SocketAddr,
    catalog: Catalog,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(address).await?;

    axum::serve(listener, routes(Arc::new(catalog)))
        .with_graceful_shutdown(async {
            let _ = signal::ctrl_c().await;
        })
        .await?;

    Ok(())
}

/// A query that will not run is the caller's mistake, and the message is what
/// they need to fix it — so it is the body rather than a log line.
async fn sql(State(catalog): State<Arc<Catalog>>, Json(request): Json<Request>) -> Response {
    match catalog.sql_cross_org(&request.sql).await {
        Ok(batches) => json(&batches),
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

/// Rows as the query selected them, so the columns are the contract.
fn json(batches: &[RecordBatch]) -> Response {
    let mut writer = ArrayWriter::new(Vec::new());

    for batch in batches {
        if let Err(error) = writer.write(batch) {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    }

    match writer.finish() {
        Ok(()) => (
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            writer.into_inner(),
        )
            .into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}
