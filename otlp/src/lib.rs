//! Turns what OpenTelemetry sends into what prism stores.

use chrono::{DateTime, Utc};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::trace::v1::{Span as OtlpSpan, status::StatusCode};
use schema::record::Common;
use schema::spans::{Span, Status};
use schema::types::{Id, Tags, Timestamp};

/// Hardcoded for demo.
const ORGANIZATION_ID: &str = "1";
const PROJECT_ID: &str = "1";

/// Every span in the request, whatever resource or scope it arrived under.
/// Those groupings say where a span came from, not what it is, and prism stores
/// one row per span.
pub fn spans(request: ExportTraceServiceRequest, received_at: DateTime<Utc>) -> Vec<Span> {
    request
        .resource_spans
        .into_iter()
        .flat_map(|resource| resource.scope_spans)
        .flat_map(|scope| scope.spans)
        .filter_map(|span| to_span(span, received_at))
        .collect()
}

fn to_span(span: OtlpSpan, received_at: DateTime<Utc>) -> Option<Span> {
    let (code, message) = match span.status {
        Some(status) => (status.code(), status.message),
        None => (StatusCode::Unset, String::new()),
    };

    Some(Span {
        common: Common {
            organization_id: ORGANIZATION_ID.to_string(),
            project_id: PROJECT_ID.to_string(),
            received_at: Timestamp::from(received_at),
        },
        span_id: id(&span.span_id)?,
        trace_id: id(&span.trace_id)?,
        // Empty rather than absent is how OTLP says a span has no parent.
        parent_span_id: (!span.parent_span_id.is_empty())
            .then(|| id(&span.parent_span_id))
            .flatten(),
        name: span.name,
        started_at: Timestamp::from(nanos(span.start_time_unix_nano)),
        // Zero means the span had not finished when it was exported, which is
        // the same as having no end.
        ended_at: (span.end_time_unix_nano != 0)
            .then(|| Timestamp::from(nanos(span.end_time_unix_nano))),
        status: match code {
            StatusCode::Ok => Status::Ok,
            StatusCode::Error => Status::Error,
            StatusCode::Unset => Status::Unset,
        },
        status_message: (!message.is_empty()).then_some(message),
        tags: tags(span.attributes),
    })
}

/// Ids are fixed width, and a sender that gets it wrong has sent something
/// prism cannot identify — so the span is dropped rather than stored under a
/// padded id that collides with another.
fn id<const N: usize>(bytes: &[u8]) -> Option<Id<N>> {
    <[u8; N]>::try_from(bytes).ok().map(Id::from)
}

fn nanos(unix_nano: u64) -> DateTime<Utc> {
    DateTime::from_timestamp_nanos(unix_nano as i64)
}

/// Keys pass through as they are; only values need converting.
fn tags(attributes: Vec<KeyValue>) -> Tags {
    attributes
        .into_iter()
        .map(|attribute| (attribute.key, attribute.value.and_then(tag_value)))
        .collect()
}

/// Tag values are text or none. Arrays and nested lists have no obvious text, so
/// they are mapped to none.
fn tag_value(value: AnyValue) -> Option<String> {
    match value.value? {
        any_value::Value::StringValue(value) => Some(value),
        any_value::Value::BoolValue(value) => Some(value.to_string()),
        any_value::Value::IntValue(value) => Some(value.to_string()),
        any_value::Value::DoubleValue(value) => Some(value.to_string()),
        any_value::Value::BytesValue(value) => {
            Some(value.iter().map(|byte| format!("{byte:02x}")).collect())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Status as OtlpStatus};

    fn at(hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 19, hour, 0, 0).unwrap()
    }

    fn request(spans: Vec<OtlpSpan>) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope::default()),
                    spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn finished() -> OtlpSpan {
        OtlpSpan {
            trace_id: vec![0xab; 16],
            span_id: vec![0xcd; 8],
            parent_span_id: vec![],
            name: "GET /checkout".to_string(),
            start_time_unix_nano: at(9).timestamp_nanos_opt().unwrap() as u64,
            end_time_unix_nano: at(10).timestamp_nanos_opt().unwrap() as u64,
            status: Some(OtlpStatus {
                code: StatusCode::Error as i32,
                message: "card declined".to_string(),
            }),
            attributes: vec![
                KeyValue {
                    key: "env".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("prod".to_string())),
                    }),
                    ..Default::default()
                },
                KeyValue {
                    key: "retries".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::IntValue(2)),
                    }),
                    ..Default::default()
                },
                KeyValue {
                    key: "sampled".to_string(),
                    value: None,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    #[test]
    fn a_span_becomes_a_record() {
        let spans = spans(request(vec![finished()]), at(11));
        let span = &spans[0];

        assert_eq!(span.trace_id.to_string(), "ab".repeat(16));
        assert_eq!(span.span_id.to_string(), "cd".repeat(8));
        assert_eq!(span.parent_span_id, None, "no parent is absent, not empty");
        assert_eq!(span.name, "GET /checkout");
        assert_eq!(span.status_message.as_deref(), Some("card declined"));
    }

    /// Exported before it finished, so it has a start and no end — the case
    /// `ended_at` is optional for.
    #[test]
    fn a_span_still_running_has_no_end() {
        let running = OtlpSpan {
            end_time_unix_nano: 0,
            ..finished()
        };

        let spans = spans(request(vec![running]), at(11));
        assert!(spans[0].ended_at.is_none());
    }

    /// Attribute values are typed on the wire and text in prism, and one with
    /// no value at all is a bare tag rather than a missing one.
    #[test]
    fn attributes_become_tags() {
        let spans = spans(request(vec![finished()]), at(11));
        let tags = Tags::from_iter([
            ("env", Some("prod".to_string())),
            ("retries", Some("2".to_string())),
            ("sampled", None),
        ]);

        assert_eq!(spans[0].tags, tags);
    }

    /// Resource and scope say where a span came from, not what it is, so spans
    /// under several of them are still just spans.
    #[test]
    fn every_span_in_the_request_is_returned() {
        let mut request = request(vec![finished(), finished()]);
        request
            .resource_spans
            .push(request.resource_spans[0].clone());

        assert_eq!(spans(request, at(11)).len(), 4);
    }
}
