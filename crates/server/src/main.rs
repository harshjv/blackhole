use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use blackhole_core::{Authority, BhError, Ctx};
use blackhole_mcp::Blackhole;
use clap::Parser;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use serde::Deserialize;
use serde_json::json;

#[derive(Parser)]
#[command(name = "blackhole-server", version, about = "Blackhole in one process: S3 endpoint, MCP over HTTP, change feed, key API")]
struct Cli {
    /// Root directory of the authority (or set BLACKHOLE_ROOT)
    #[arg(long, env = "BLACKHOLE_ROOT", default_value = ".blackhole")]
    root: PathBuf,
    /// Address to listen on: S3 at /, MCP at /mcp, feed at /feed/<drive>, keys at /api/keys
    #[arg(long, env = "BLACKHOLE_ADDR", default_value = "127.0.0.1:9900")]
    addr: SocketAddr,
}

#[derive(Clone)]
struct App {
    auth: Authority,
    s3: s3s::service::S3Service,
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    headers.get(axum::http::header::AUTHORIZATION)?.to_str().ok()?.strip_prefix("Bearer ").map(|k| k.trim().to_string())
}

fn fail(e: anyhow::Error) -> Response {
    let status = match e.downcast_ref::<BhError>() {
        Some(BhError::Denied(_)) => StatusCode::FORBIDDEN,
        Some(BhError::NotFound(_)) => StatusCode::NOT_FOUND,
        Some(BhError::Conflict(_)) => StatusCode::CONFLICT,
        None => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({ "error": format!("{e:#}") }))).into_response()
}

async fn ctx_of(app: &App, headers: &HeaderMap) -> Result<Ctx, Response> {
    let key = bearer(headers).ok_or_else(|| (StatusCode::UNAUTHORIZED, Json(json!({ "error": "send Authorization: Bearer <key>" }))).into_response())?;
    app.auth.ctx(&key).await.map_err(fail)
}

#[derive(Deserialize)]
struct NewKey {
    drive: String,
    principal: String,
    prefix: Option<String>,
    #[serde(default)]
    read_only: bool,
    #[serde(default)]
    admin: bool,
    ttl_seconds: Option<i64>,
}

async fn create_key(State(app): State<App>, headers: HeaderMap, Json(req): Json<NewKey>) -> Response {
    let ctx = match ctx_of(&app, &headers).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    match app.auth.create_key(&ctx, &req.drive, &req.principal, req.prefix.as_deref(), req.read_only, req.admin, req.ttl_seconds.map(|s| s * 1000)).await {
        Ok(k) => Json(json!({ "id": k.id, "secret": k.secret, "principal": req.principal, "drive": req.drive, "expires_at": k.expires_at })).into_response(),
        Err(e) => fail(e),
    }
}

async fn attest(State(app): State<App>, headers: HeaderMap, Path(drive): Path<String>) -> Response {
    let ctx = match ctx_of(&app, &headers).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    match app.auth.attest(&ctx, &drive).await {
        Ok(a) => Json(json!({ "drive": a.drive, "head_seq": a.head_seq, "head_hash": a.head_hash, "at": a.at, "hold": a.hold })).into_response(),
        Err(e) => fail(e),
    }
}

async fn verify(State(app): State<App>, headers: HeaderMap, Path(drive): Path<String>) -> Response {
    let ctx = match ctx_of(&app, &headers).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    match app.auth.verify(&ctx, &drive).await {
        Ok(lines) => {
            let bad = lines.iter().filter(|l| !l.status.starts_with("ok")).count();
            Json(json!({ "drive": drive, "commits": lines.len(), "bad": bad, "lines": lines.iter().map(|l| json!({ "seq": l.seq, "hash": l.hash, "status": l.status })).collect::<Vec<_>>() })).into_response()
        }
        Err(e) => fail(e),
    }
}

#[derive(Deserialize)]
struct FeedQuery {
    after: Option<i64>,
}

async fn feed(State(app): State<App>, headers: HeaderMap, Path(drive): Path<String>, axum::extract::Query(q): axum::extract::Query<FeedQuery>) -> Response {
    let ctx = match ctx_of(&app, &headers).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    if let Err(e) = app.auth.changes_since(&ctx, &drive, i64::MAX, 1).await {
        return fail(e);
    }
    let cursor = q.after.unwrap_or(0);
    let stream = futures::stream::unfold((app, ctx, drive, cursor), |(app, ctx, drive, mut cursor)| async move {
        loop {
            match app.auth.changes_since(&ctx, &drive, cursor, 500).await {
                Ok(sets) if !sets.is_empty() => {
                    let events: Vec<Result<Event, Infallible>> = sets
                        .iter()
                        .map(|cs| {
                            cursor = cs.commit.seq;
                            let data = json!({
                                "seq": cs.commit.seq,
                                "at": cs.commit.committed_at,
                                "actor": cs.commit.actor,
                                "intent": cs.commit.intent,
                                "paths": cs.paths.iter().map(|(p, k)| json!({ "path": p, "kind": k })).collect::<Vec<_>>(),
                            });
                            Ok(Event::default().id(cs.commit.seq.to_string()).event("commit").data(data.to_string()))
                        })
                        .collect();
                    return Some((futures::stream::iter(events), (app, ctx, drive, cursor)));
                }
                Ok(_) => tokio::time::sleep(Duration::from_millis(250)).await,
                Err(_) => return None,
            }
        }
    })
    .flatten();
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))).into_response()
}

async fn s3_fallback(State(app): State<App>, req: Request<Body>) -> Response {
    let (parts, body) = req.into_parts();
    let req = axum::http::Request::from_parts(parts, s3s::Body::http_body_unsync(body));
    match app.s3.call(req).await {
        Ok(resp) => resp.map(Body::new),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
    }
}

async fn health() -> &'static str {
    "ok"
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let auth = Authority::open(&cli.root).await?;
    let (_, store_label) = blackhole_core::ShardStore::from_env(&cli.root)?;
    let resumed = auth.store.resume_uploads()?;
    if resumed > 0 {
        eprintln!("resuming {resumed} pending shard upload(s) from the last run");
    }
    let app = App { auth: auth.clone(), s3: blackhole_s3::service(auth.clone()) };
    let mcp_auth = auth.clone();
    let mcp = StreamableHttpService::new(move || Ok(Blackhole::new(mcp_auth.clone(), None)), Arc::new(LocalSessionManager::default()), StreamableHttpServerConfig::default());
    let router = Router::new()
        .route("/healthz", get(health))
        .route("/api/keys", post(create_key))
        .route("/api/attest/{drive}", get(attest))
        .route("/api/verify/{drive}", get(verify))
        .route("/feed/{drive}", get(feed))
        .nest_service("/mcp", mcp)
        .fallback(s3_fallback)
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(cli.addr).await?;
    eprintln!("blackhole-server on http://{}  (S3 at /, MCP at /mcp, feed at /feed/<drive>, keys at /api/keys; shards: {store_label})", cli.addr);
    axum::serve(listener, router).with_graceful_shutdown(async { tokio::signal::ctrl_c().await.ok(); }).await?;
    Ok(())
}
