//! Hyperliquid Agent - Data streaming agent for Hyperliquid blockchain.
//!
//! This agent reads blockchain data from local files,
//! parses it into standardized formats, and forwards to downstream systems.

use anyhow::{Context, Result};
use hl_agent::config::Config;
use hl_agent::parsers::hash_store::HashStore;
use hl_agent::runner;
use std::net::SocketAddr;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config_path =
        std::env::var("HL_AGENT_CONFIG").unwrap_or_else(|_| "config.toml".to_string());
    let config = Config::load(&config_path)
        .with_context(|| format!("failed to load config from {config_path}"))?;

    init_hash_store(&config).await?;

    // Start metrics HTTP server on port 9090
    tokio::spawn(start_metrics_server());

    runner::file_mode::run(&config).await
}

async fn init_hash_store(config: &Config) -> Result<()> {
    let redis_url = config.redis_url();

    HashStore::init(&redis_url)
        .await
        .context("failed to initialize hash store")?;

    Ok(())
}

fn init_tracing() {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);
}

/// Starts the Prometheus metrics HTTP server on port 9090.
/// Serves metrics at GET /metrics
/// Returns when a shutdown signal is received (Ctrl+C).
async fn start_metrics_server() {
    use http_body_util::Full;
    use hyper::body::Bytes;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response};
    use hyper_util::rt::TokioIo;
    use prometheus::Encoder;
    use tokio::net::TcpListener;

    let addr: SocketAddr = "0.0.0.0:9090".parse().unwrap();

    let listener = match TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(err) => {
            tracing::error!("Failed to bind metrics server to {}: {}", addr, err);
            return;
        }
    };

    info!(
        "Prometheus metrics server listening on http://{}/metrics",
        addr
    );

    // Listen for shutdown signal
    let shutdown_signal = async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Metrics server received shutdown signal");
    };

    tokio::pin!(shutdown_signal);

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown_signal => {
                tracing::info!("Metrics server shutting down");
                break;
            }
            accept_result = listener.accept() => {
                let (stream, _) = match accept_result {
                    Ok(conn) => conn,
                    Err(err) => {
                        tracing::warn!("Failed to accept metrics connection: {}", err);
                        continue;
                    }
                };

                tokio::spawn(async move {
                    let io = TokioIo::new(stream);

                    let service = service_fn(|req: Request<hyper::body::Incoming>| async move {
                        if req.uri().path() == "/metrics" {
                            let encoder = prometheus::TextEncoder::new();
                            // CRITICAL FIX: Use default registry where metrics are actually registered
                            let metric_families = prometheus::gather();
                            let mut buffer = Vec::new();
                            encoder.encode(&metric_families, &mut buffer).unwrap();

                            // Add Content-Type header for Prometheus compatibility
                            let response = Response::builder()
                                .header(hyper::header::CONTENT_TYPE, encoder.format_type())
                                .body(Full::new(Bytes::from(buffer)))
                                .unwrap();
                            Ok::<_, hyper::Error>(response)
                        } else {
                            let response = Response::builder()
                                .status(404)
                                .body(Full::new(Bytes::from("Not Found")))
                                .unwrap();
                            Ok::<_, hyper::Error>(response)
                        }
                    });

                    if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                        tracing::debug!("Error serving metrics connection: {}", err);
                    }
                });
            }
        }
    }
}
