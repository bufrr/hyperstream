use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sonic_rs::{json, Value};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

const DEFAULT_WS_URL: &str = "wss://rpc.hyperliquid.xyz/ws";
const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379";
const BLOCK_TTL_SECONDS: u64 = 86_400;
const MAX_BACKOFF_SECS: u64 = 60;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
enum ExplorerSubscription {
    ExplorerBlock,
}

#[derive(Debug, Clone, Serialize)]
struct SubscriptionMessage {
    method: String,
    subscription: ExplorerSubscription,
}

impl SubscriptionMessage {
    fn new(sub_type: ExplorerSubscription) -> Self {
        Self {
            method: "subscribe".to_string(),
            subscription: sub_type,
        }
    }
}

#[derive(Debug, Deserialize)]
struct SubscriptionResponse {
    channel: String,
    data: Value,
}

#[derive(Debug, Deserialize)]
struct ExplorerBlock {
    height: u64,
    #[serde(rename = "blockTime")]
    block_time: u64,
    hash: String,
    proposer: String,
    #[serde(rename = "numTxs")]
    num_txs: u32,
}

#[derive(Clone, Debug)]
struct Config {
    ws_url: String,
    redis_url: String,
}

impl Config {
    fn from_env() -> Self {
        let ws_url =
            std::env::var("WS_AGENT_WS_URL").unwrap_or_else(|_| DEFAULT_WS_URL.to_string());
        let redis_url =
            std::env::var("WS_AGENT_REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_string());

        Self { ws_url, redis_url }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let config = Config::from_env();
    info!(
        ws_url = %config.ws_url,
        redis_url = %config.redis_url,
        "Starting ws-agent"
    );

    let client = redis::Client::open(config.redis_url.clone())
        .context("Failed to construct Redis client")?;
    let conn = client
        .get_connection_manager()
        .await
        .context("Failed to connect to Redis")?;

    run_loop(config, conn).await
}

async fn run_loop(config: Config, mut redis_conn: ConnectionManager) -> Result<()> {
    let mut backoff = Duration::from_secs(1);

    loop {
        match stream_once(&config.ws_url, &mut redis_conn).await {
            Ok(()) => {
                info!("Stream closed gracefully; resetting backoff");
                backoff = Duration::from_secs(1);
            }
            Err(err) => {
                error!("Stream failed: {err:?}");
                backoff = std::cmp::min(backoff * 2, Duration::from_secs(MAX_BACKOFF_SECS));
            }
        }

        info!("Reconnecting after {:?}", backoff);
        sleep(backoff).await;
    }
}

async fn stream_once(ws_url: &str, redis_conn: &mut ConnectionManager) -> Result<()> {
    info!("Connecting to Explorer WebSocket at {ws_url}");
    let (ws_stream, _) = connect_async(ws_url)
        .await
        .with_context(|| format!("Failed to connect to WebSocket {ws_url}"))?;

    info!("Connected to Explorer WebSocket");
    let (mut write, mut read) = ws_stream.split();

    let sub = SubscriptionMessage::new(ExplorerSubscription::ExplorerBlock);
    let sub_msg = sonic_rs::to_string(&sub).context("Failed to serialize subscription")?;
    write
        .send(Message::Text(sub_msg.into()))
        .await
        .context("Failed to send subscription request")?;
    info!("Subscribed to explorerBlock channel");

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Err(err) = handle_text(&text, redis_conn).await {
                    warn!("Failed to process WS payload: {err:?}");
                }
            }
            Ok(Message::Ping(payload)) => {
                write
                    .send(Message::Pong(payload))
                    .await
                    .context("Failed to respond to ping")?;
            }
            Ok(Message::Pong(_)) => {}
            Ok(Message::Frame(_)) => {
                debug!("Ignored frame message from WebSocket");
            }
            Ok(Message::Close(frame)) => {
                warn!("WebSocket closed by server: {:?}", frame);
                return Ok(());
            }
            Ok(Message::Binary(_)) => {
                debug!("Ignored binary payload from WebSocket");
            }
            Err(err) => {
                return Err(err.into());
            }
        }
    }

    warn!("WebSocket read stream ended unexpectedly");
    Ok(())
}

async fn handle_text(text: &str, redis_conn: &mut ConnectionManager) -> Result<()> {
    if let Ok(resp) = sonic_rs::from_str::<SubscriptionResponse>(text) {
        if resp.channel == "subscriptionResponse" {
            debug!("Subscription acknowledged: {:?}", resp.data);
            return Ok(());
        }
    }

    if let Ok(blocks) = sonic_rs::from_str::<Vec<ExplorerBlock>>(text) {
        for block in blocks {
            store_block(redis_conn, &block).await?;
        }
        return Ok(());
    }

    debug!(
        "Unhandled WebSocket message: {}",
        &text[..text.len().min(200)]
    );
    Ok(())
}

async fn store_block(conn: &mut ConnectionManager, block: &ExplorerBlock) -> Result<()> {
    let key = format!("block:{}", block.height);
    let payload = json!({
        "hash": block.hash,
        "proposer": block.proposer,
        "blockTime": block.block_time,
        "numTxs": block.num_txs,
    });

    let payload = sonic_rs::to_string(&payload)?;

    conn.set_ex::<_, _, ()>(&key, payload, BLOCK_TTL_SECONDS)
        .await
        .with_context(|| format!("Failed to store block {}", block.height))?;

    info!(
        height = block.height,
        hash = %block.hash,
        ttl = BLOCK_TTL_SECONDS,
        "Stored block in Redis"
    );
    Ok(())
}
