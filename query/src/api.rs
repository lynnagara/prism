//! SQL in, rows out. What builds the queries lives elsewhere — for now the UI,
//! later something between it and here.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    // Around the query alone: planning through collect, and none of the
    // serialising that a caller's clock would otherwise include.
    let started = Instant::now();
    let batches = match catalog.sql_cross_org(&request.sql).await {
        Ok(batches) => batches,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };

    json(&batches, started.elapsed())
}

/// `rows` are the query's own columns, untouched — everything the store wants
/// to say about answering it goes beside them rather than among them.
fn json(batches: &[RecordBatch], elapsed: Duration) -> Response {
    let mut writer = ArrayWriter::new(Vec::new());

    for batch in batches {
        if let Err(error) = writer.write(batch) {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    }
    if let Err(error) = writer.finish() {
        return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
    }

    // A query matching nothing produces no batches, and so no array at all.
    let mut rows = writer.into_inner();
    if rows.is_empty() {
        rows = b"[]".to_vec();
    }

    let mut body = Vec::with_capacity(rows.len() + 48);
    body.extend_from_slice(b"{\"rows\":");
    body.append(&mut rows);
    body.extend_from_slice(
        format!(",\"elapsed_ms\":{:.1}}}", elapsed.as_secs_f64() * 1000.0).as_bytes(),
    );

    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}
