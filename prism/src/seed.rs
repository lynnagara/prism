//! Writes spans across a range of days.
//!
//! A receiver stamps `received_at` on arrival, so real traffic only ever lands
//! in today's partition — there is nothing for partition pruning to skip and
//! nothing for compaction to walk. Seeding backdates the arrival instead, so
//! the rows in a partition are the rows that partition claims to hold.

use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};
use datafusion::object_store::ObjectStore;
use ingest::buffer::Buffer;
use ingest::writer::Writer;
use schema::record::Common;
use schema::spans::{Span, Status};
use schema::types::{SpanId, Tags, Timestamp, TraceId};

use crate::Result;

/// A span within a shape: which service ran it, what it is called, how much of
/// its parent's time it takes, and which earlier entry is its parent — 0 being
/// the root. Declared rather than inferred, so a query nests under the call
/// that issued it.
type Step = (&'static str, &'static str, u64, usize);

/// (root service, root operation, root milliseconds, the calls beneath it)
///
/// The services and operations are the OpenTelemetry demo's — the astronomy
/// shop — so a trace here reads like one from the shop it is standing in for.
/// The shapes are not: real ones nest through a proxy and a browser SDK and are
/// hard to follow at a glance, and this is a store being shown, not a webshop.
const SHAPES: &[(&str, &str, i64, &[Step])] = &[
    ("frontend", "GET /api/products", 180, &[
        ("product-catalog", "oteldemo.ProductCatalogService/ListProducts", 74, 0),
        ("product-catalog", "SELECT products", 68, 1),
        ("ad", "oteldemo.AdService/GetAds", 21, 0),
    ]),
    ("frontend", "POST /api/cart", 240, &[
        ("product-catalog", "oteldemo.ProductCatalogService/GetProduct", 26, 0),
        ("cart", "oteldemo.CartService/AddItem", 58, 0),
        ("cart", "HSET cart", 61, 2),
    ]),
    ("frontend", "POST /api/checkout", 1_450, &[
        ("cart", "oteldemo.CartService/GetCart", 8, 0),
        ("cart", "HGET cart", 55, 1),
        ("currency", "oteldemo.CurrencyService/Convert", 5, 0),
        ("checkout", "oteldemo.CheckoutService/PlaceOrder", 71, 0),
        ("payment", "oteldemo.PaymentService/Charge", 44, 4),
        ("shipping", "oteldemo.ShippingService/ShipOrder", 19, 4),
        ("email", "oteldemo.EmailService/SendOrderConfirmation", 12, 4),
        ("accounting", "consume order", 6, 0),
    ]),
    ("frontend", "GET /api/recommendations", 320, &[
        ("recommendation", "oteldemo.RecommendationService/ListRecommendations", 68, 0),
        ("product-catalog", "oteldemo.ProductCatalogService/ListProducts", 47, 1),
    ]),
    ("frontend", "GET /api/data", 95, &[
        ("ad", "oteldemo.AdService/GetAds", 72, 0),
    ]),
    ("frontend", "POST /api/currency", 70, &[
        ("currency", "oteldemo.CurrencyService/Convert", 61, 0),
    ]),
    ("frontend", "GET /api/shipping", 210, &[
        ("quote", "oteldemo.QuoteService/GetQuote", 63, 0),
        ("shipping", "oteldemo.ShippingService/GetQuote", 44, 1),
    ]),
    ("load-generator", "browser_checkout", 2_600, &[
        ("frontend", "GET /api/cart", 11, 0),
        ("cart", "oteldemo.CartService/GetCart", 57, 1),
        ("frontend", "POST /api/checkout", 74, 0),
        ("checkout", "oteldemo.CheckoutService/PlaceOrder", 82, 3),
        ("payment", "oteldemo.PaymentService/Charge", 37, 4),
    ]),
];

/// What the shop sells, so a span someone opens says something recognisable
/// rather than a placeholder. Borrowed from the OpenTelemetry demo's catalogue.
const PRODUCTS: &[(&str, &str)] = &[
    ("OLJCESPC7Z", "National Park Foundation Explorascope"),
    ("66VCHSJNUP", "Starsense Explorer Dobsonian"),
    ("1YMWWN1N4O", "Eclipsmart Travel Refractor"),
    ("L9ECAV7KIM", "Solar System Color Imager"),
    ("2ZYFJ3GM2N", "Roof Binoculars"),
    ("0PUK6V6EV0", "Solar Filter"),
    ("HQTGWGPNH4", "The Comet Book"),
    ("9SIQT8TOJO", "Lens Cleaning Kit"),
    ("6E92ZMYYFZ", "Red Flashlight"),
    ("LS4PSXUNUM", "Optical Tube Assembly"),
];

/// Deterministic, so seeding twice gives the same store and a demo can be
/// rehearsed against it.
///
/// The step is a linear congruential generator, whose low bits barely move —
/// so the output is mixed before it is used. Ids are built from these bytes,
/// and unmixed they collide often enough to fuse unrelated traces into one.
fn next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);

    let mut mixed = *state;
    mixed ^= mixed >> 30;
    mixed = mixed.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed ^= mixed >> 27;
    mixed = mixed.wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^ (mixed >> 31)
}

/// Eight bytes at a time, so every byte of an id comes from a different part
/// of a well-mixed word rather than the same weak corner of eight of them.
fn id<const N: usize>(state: &mut u64) -> [u8; N] {
    let mut bytes = [0u8; N];
    for chunk in bytes.chunks_mut(8) {
        let word = next(state).to_le_bytes();
        chunk.copy_from_slice(&word[..chunk.len()]);
    }
    bytes
}

/// Days are interleaved rather than written one after another. The buffer
/// flushes on how many rows it is holding, so writing a day at a time gives a
/// partition a single file and leaves compaction nothing to merge — which is
/// exactly the thing worth being able to show.
pub async fn run(store: Arc<dyn ObjectStore>, days: i64, per_day: u64, seed: u64) -> Result<()> {
    let buffer: Buffer<Span> = Buffer::new(Writer::new(store));
    // Measured back from now, never from midnight: anchoring to the start of
    // today puts a share of the newest day in the future.
    let now = Utc::now();
    let window = (days * 86_400_000) as u64;

    // Mixed once so neighbouring seeds do not start in neighbouring places.
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x5eed;
    let mut written = 0u64;
    let mut batch = Vec::new();

    for _ in 0..days as u64 * per_day {
        let at = now - TimeDelta::milliseconds((next(&mut state) % window) as i64);
        batch.extend(trace(&mut state, at));

        if batch.len() >= 4_000 {
            written += flush(&buffer, &mut batch).await;
        }
    }
    written += flush(&buffer, &mut batch).await;

    buffer.shutdown().await;
    eprintln!("{written} spans across {days} days");
    Ok(())
}

/// Waits for room rather than pushing: generating rows is far quicker than
/// writing them, so without backpressure the queue fills and the rows it was
/// handed are dropped.
async fn flush(buffer: &Buffer<Span>, batch: &mut Vec<Span>) -> u64 {
    let rows = std::mem::take(batch);
    let count = rows.len() as u64;

    buffer.send(rows).await;
    count
}

/// A root span and the calls beneath it, each laid out inside its own parent so
/// nothing outlives the work that spawned it.
fn trace(state: &mut u64, at: DateTime<Utc>) -> Vec<Span> {
    let (root_service, operation, base_ms, steps) = SHAPES[(next(state) % SHAPES.len() as u64) as usize];
    let total = base_ms * (55 + next(state) % 150) as i64 / 100;
    let trace_id = TraceId::from(id::<16>(state));
    let started = at - TimeDelta::milliseconds(total);

    // Roughly one trace in fourteen fails, in one of its calls.
    let failing = (next(state) % 14 == 0).then(|| next(state) % steps.len() as u64 + 1);
    // A status of Ok means someone asserted success, which almost nothing does.
    let root_status = if next(state) % 20 == 0 { Status::Ok } else { Status::Unset };

    let common = || Common {
        organization_id: "1".to_string(),
        project_id: "1".to_string(),
        received_at: Timestamp::from(at),
    };
    let span = |service: &str, name: &str, id: SpanId, parent, from: i64, to: i64,
                status, message: Option<String>, tags| Span {
        common: common(),
        span_id: id,
        trace_id,
        parent_span_id: parent,
        service: Some(service.to_string()),
        name: name.to_string(),
        started_at: Timestamp::from(started + TimeDelta::milliseconds(from)),
        ended_at: Some(Timestamp::from(started + TimeDelta::milliseconds(to))),
        status,
        status_message: message,
        tags,
    };

    let product = PRODUCTS[(next(state) % PRODUCTS.len() as u64) as usize];
    let root_id = SpanId::from(id::<8>(state));
    let mut ids = vec![root_id];
    // Where each span sits, and how far into each the next child should start.
    let mut window = vec![(0i64, total)];
    let mut cursor = vec![total / 40];
    let mut spans = vec![span(root_service, operation, root_id, None, 0, total, root_status, None, Tags::default())];

    for (index, (service, name, share, parent)) in steps.iter().enumerate() {
        let (parent_from, parent_to) = window[*parent];
        let room = parent_to - parent_from;
        let from = cursor[*parent].min(parent_to);
        let to = (from + (room * *share as i64 / 100).max(1)).min(parent_to);
        let failed = failing == Some(index as u64 + 1);

        let child = SpanId::from(id::<8>(state));
        spans.push(span(
            service,
            name,
            child,
            Some(ids[*parent]),
            from,
            to,
            if failed { Status::Error } else { Status::Unset },
            failed.then(|| {
                ["upstream timeout", "connection reset", "deadline exceeded", "429 rate limited"]
                    [(next(state) % 4) as usize]
                    .to_string()
            }),
            tags(name, product),
        ));

        ids.push(child);
        window.push((from, to));
        cursor.push(from + (to - from) / 25);
        cursor[*parent] = (to + room / 40).min(parent_to);
    }

    spans
}

/// The tags a caller would actually have set. Most spans carry none, which is
/// what the column looks like in practice — the ones that do are the ones
/// worth opening: a query says which engine, a catalogue call says which item.
fn tags(name: &str, (sku, title): (&str, &str)) -> Tags {
    let mut tags = match name.split(' ').next() {
        Some("SELECT" | "INSERT" | "UPDATE" | "COPY") => {
            vec![("db.system", Some("postgresql".to_string()))]
        }
        Some("POST" | "GET") if name.contains('.') => {
            vec![("http.status_code", Some("200".to_string()))]
        }
        _ => Vec::new(),
    };

    if name.contains("ProductCatalogService")
        || name.contains("CartService")
        || name.contains("RecommendationService")
    {
        tags.push(("product.sku", Some(sku.to_string())));
        tags.push(("product.name", Some(title.to_string())));
    }

    Tags::from_iter(tags)
}
