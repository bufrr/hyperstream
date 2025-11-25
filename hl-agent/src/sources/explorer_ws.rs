//! WebSocket source for Hyperliquid Explorer API
//!
//! This module provides real-time data streaming from the Hyperliquid Explorer
//! WebSocket API, offering processed blocks and transactions data.

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

/// Explorer WebSocket subscription types
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum ExplorerSubscription {
    ExplorerBlock,
    ExplorerTxs,
}

/// Subscription message format
#[derive(Debug, Clone, Serialize)]
pub struct SubscriptionMessage {
    method: String,
    subscription: ExplorerSubscription,
}

impl SubscriptionMessage {
    pub fn new(sub_type: ExplorerSubscription) -> Self {
        Self {
            method: "subscribe".to_string(),
            subscription: sub_type,
        }
    }
}

/// Subscription response from server
#[derive(Debug, Deserialize)]
pub struct SubscriptionResponse {
    pub channel: String,
    pub data: Value,
}

/// Block data from explorerBlock subscription
#[derive(Debug, Deserialize)]
pub struct ExplorerBlock {
    pub height: u64,
    #[serde(rename = "blockTime")]
    pub block_time: u64, // milliseconds
    pub hash: String,
    pub proposer: String,
    #[serde(rename = "numTxs")]
    pub num_txs: u32,
}

/// Transaction data from explorerTxs subscription
#[derive(Debug, Deserialize)]
pub struct ExplorerTx {
    pub time: u64, // milliseconds
    pub user: String,
    pub action: Value, // Dynamic action object
    pub block: u64,
    pub hash: String,
    pub error: Option<String>,
}

/// Explorer WebSocket client
pub struct ExplorerWsClient {
    url: String,
    block_tx: Option<mpsc::Sender<ExplorerBlock>>,
    tx_tx: Option<mpsc::Sender<ExplorerTx>>,
}

impl ExplorerWsClient {
    /// Create new Explorer WebSocket client
    pub fn new(url: String) -> Self {
        Self {
            url,
            block_tx: None,
            tx_tx: None,
        }
    }

    /// Enable block subscription with channel
    pub fn with_blocks(mut self, tx: mpsc::Sender<ExplorerBlock>) -> Self {
        self.block_tx = Some(tx);
        self
    }

    /// Enable transaction subscription with channel
    #[allow(dead_code)]
    pub fn with_transactions(mut self, tx: mpsc::Sender<ExplorerTx>) -> Self {
        self.tx_tx = Some(tx);
        self
    }

    /// Start the WebSocket client and begin streaming
    pub async fn run(self) -> Result<()> {
        info!("Connecting to Explorer WebSocket: {}", self.url);

        let (ws_stream, _) = connect_async(&self.url)
            .await
            .context("Failed to connect to WebSocket")?;

        info!("Connected to Explorer WebSocket");

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to blocks if enabled
        if self.block_tx.is_some() {
            let sub = SubscriptionMessage::new(ExplorerSubscription::ExplorerBlock);
            let msg = serde_json::to_string(&sub)?;
            write.send(Message::Text(msg)).await?;
            info!("Subscribed to explorerBlock");
        }

        // Subscribe to transactions if enabled
        if self.tx_tx.is_some() {
            let sub = SubscriptionMessage::new(ExplorerSubscription::ExplorerTxs);
            let msg = serde_json::to_string(&sub)?;
            write.send(Message::Text(msg)).await?;
            info!("Subscribed to explorerTxs");
        }

        // Process incoming messages
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Err(e) = self.handle_message(&text).await {
                        error!("Error handling message: {}", e);
                    }
                }
                Ok(Message::Close(_)) => {
                    warn!("WebSocket closed by server");
                    break;
                }
                Ok(_) => {
                    // Ignore other message types (ping/pong/binary)
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
            }
        }

        info!("Explorer WebSocket client stopped");
        Ok(())
    }

    async fn handle_message(&self, text: &str) -> Result<()> {
        // Try to parse as subscription response first
        if let Ok(resp) = serde_json::from_str::<SubscriptionResponse>(text) {
            if resp.channel == "subscriptionResponse" {
                debug!("Subscription confirmed: {:?}", resp.data);
                return Ok(());
            }
        }

        // Try to parse as block array
        if let Some(ref block_tx) = self.block_tx {
            if let Ok(blocks) = serde_json::from_str::<Vec<ExplorerBlock>>(text) {
                for block in blocks {
                    debug!(
                        "Received block: height={}, hash={}",
                        block.height, block.hash
                    );
                    if let Err(e) = block_tx.send(block).await {
                        warn!("Failed to send block to channel: {}", e);
                    }
                }
                return Ok(());
            }
        }

        // Try to parse as transaction array
        if let Some(ref tx_tx) = self.tx_tx {
            if let Ok(txs) = serde_json::from_str::<Vec<ExplorerTx>>(text) {
                for tx in txs {
                    debug!(
                        "Received transaction: hash={}, user={}, block={}",
                        tx.hash, tx.user, tx.block
                    );
                    if let Err(e) = tx_tx.send(tx).await {
                        warn!("Failed to send transaction to channel: {}", e);
                    }
                }
                return Ok(());
            }
        }

        // Log unknown message format
        debug!("Unknown message format: {}", &text[..text.len().min(200)]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_serialization() {
        let sub = SubscriptionMessage::new(ExplorerSubscription::ExplorerBlock);
        let json = serde_json::to_string(&sub).unwrap();
        assert_eq!(
            json,
            r#"{"method":"subscribe","subscription":{"type":"explorerBlock"}}"#
        );
    }

    #[test]
    fn test_block_deserialization() {
        let json = r#"{
            "height": 467380970,
            "blockTime": 1763973059106,
            "hash": "0xabc123",
            "proposer": "0xdef456",
            "numTxs": 5
        }"#;
        let block: ExplorerBlock = serde_json::from_str(json).unwrap();
        assert_eq!(block.height, 467380970);
        assert_eq!(block.hash, "0xabc123");
    }

    #[test]
    fn test_tx_deserialization() {
        let json = r#"{
            "time": 1763973059106,
            "user": "0xc64cc00b46101bd40aa1c3121195e85c0b0918d8",
            "action": {"type": "order", "orders": []},
            "block": 467380970,
            "hash": "0x043dbdb186dd8fb105b7041bdbaaea010101229721d0ae83a806690445d1699b",
            "error": null
        }"#;
        let tx: ExplorerTx = serde_json::from_str(json).unwrap();
        assert_eq!(tx.user, "0xc64cc00b46101bd40aa1c3121195e85c0b0918d8");
        assert_eq!(tx.block, 467380970);
        assert!(tx.error.is_none());
    }
}
