//! Where OpenTelemetry sends its spans: one route, speaking the protocol a
//! collector or an SDK exporter already speaks.

use std::borrow::Cow;
use std::io::{self, Read};
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use chrono::Utc;
use flate2::read::GzDecoder;
use ingest::buffer::Buffer;
use schema::spans::Span;
use tokio::net::TcpListener;
use tokio::signal;

/// The path OTLP over HTTP puts traces on
const TRACES: &str = "/v1/traces";

/// A full success is an empty `ExportTraceServiceResponse`, which encodes to no
/// bytes at all — only the content type says what it is.
const PROTOBUF: &str = "application/x-protobuf";

pub fn routes(buffer: Arc<Buffer<Span>>) -> Router {
    Router::new().route(TRACES, post(traces)).with_state(buffer)
}

/// Serves until interrupted, then writes what it is still holding — spans
/// answered for are owed to whoever sent them.
pub async fn serve(
    address: SocketAddr,
    buffer: Buffer<Span>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let buffer = Arc::new(buffer);
    let listener = TcpListener::bind(address).await?;

    axum::serve(listener, routes(buffer.clone()))
        .with_graceful_shutdown(async {
            let _ = signal::ctrl_c().await;
        })
        .await?;

    // Only the server held the other reference, and it has stopped.
    match Arc::try_unwrap(buffer) {
        Ok(buffer) => buffer.shutdown().await,
        Err(_) => unreachable!("the server is the only other holder"),
    }

    Ok(())
}

async fn traces(
    State(buffer): State<Arc<Buffer<Span>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let decoded = decompressed(&headers, &body)
        .map_err(|error| error.to_string())
        .and_then(|body| {
            crate::spans::from_bytes(&body, Utc::now()).map_err(|error| error.to_string())
        });

    let spans = match decoded {
        Ok(spans) => spans,
        Err(error) => {
            eprintln!("refused a request: {error}");
            return (StatusCode::BAD_REQUEST, error).into_response();
        }
    };

    match buffer.push(spans) {
        Ok(()) => (StatusCode::OK, [(header::CONTENT_TYPE, PROTOBUF)]).into_response(),
        // Senders retry, so saying no is what keeps the spans somewhere until
        // prism can take them.
        Err(full) => (StatusCode::SERVICE_UNAVAILABLE, full.to_string()).into_response(),
    }
}

/// Senders compress by default and say so in a header. Anything else arrives
/// as it was sent.
fn decompressed<'a>(headers: &HeaderMap, body: &'a Bytes) -> io::Result<Cow<'a, [u8]>> {
    if headers
        .get(header::CONTENT_ENCODING)
        .is_none_or(|value| value != "gzip")
    {
        return Ok(Cow::Borrowed(body));
    }

    let mut decompressed = Vec::new();
    GzDecoder::new(body.as_ref()).read_to_end(&mut decompressed)?;

    Ok(Cow::Owned(decompressed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use datafusion::object_store::memory::InMemory;
    use datafusion::object_store::{ObjectStore, path::Path};
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use futures::StreamExt;
    use ingest::writer::Writer;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan};
    use prost::Message;
    use schema::record::Record;
    use std::io::Write;
    use tower::ServiceExt;

    fn exported() -> Vec<u8> {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![OtlpSpan {
                        trace_id: vec![0xab; 16],
                        span_id: vec![0xcd; 8],
                        name: "GET /checkout".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec()
    }

    fn post(body: Vec<u8>) -> Request<Body> {
        Request::post(TRACES)
            .header(header::CONTENT_TYPE, PROTOBUF)
            .body(Body::from(body))
            .unwrap()
    }

    /// What a sender is told is the whole contract: accepted, or keep it.
    #[tokio::test]
    async fn a_request_becomes_a_file() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer = Arc::new(Buffer::new(Writer::new(store.clone())));

        let response = routes(buffer.clone()).oneshot(post(exported())).await;
        assert_eq!(response.unwrap().status(), StatusCode::OK);

        // Too few spans to be worth a file, so shutting down is what stores it.
        Arc::try_unwrap(buffer)
            .unwrap_or_else(|_| unreachable!())
            .shutdown()
            .await;

        let written = store.list(Some(&Path::from(Span::TABLE))).count().await;
        assert_eq!(written, 1);
    }

    /// Senders compress by default, so a receiver that only reads plain bodies
    /// refuses everything a collector sends it.
    #[tokio::test]
    async fn a_compressed_request_is_read() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer = Arc::new(Buffer::new(Writer::new(store)));

        let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
        gzip.write_all(&exported()).unwrap();

        let request = Request::post(TRACES)
            .header(header::CONTENT_TYPE, PROTOBUF)
            .header(header::CONTENT_ENCODING, "gzip")
            .body(Body::from(gzip.finish().unwrap()))
            .unwrap();

        let response = routes(buffer).oneshot(request).await;
        assert_eq!(response.unwrap().status(), StatusCode::OK);
    }

    /// A body prism cannot read is the sender's mistake, and no amount of
    /// retrying will fix it.
    #[tokio::test]
    async fn a_body_that_is_not_otlp_is_refused() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer = Arc::new(Buffer::new(Writer::new(store)));

        let response = routes(buffer).oneshot(post(b"not protobuf".to_vec())).await;
        assert_eq!(response.unwrap().status(), StatusCode::BAD_REQUEST);
    }
}
