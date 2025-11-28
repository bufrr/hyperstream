use anyhow::{anyhow, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic::Request;
use tracing::{error, warn};

pub mod proto {
    tonic::include_proto!("hyperstream");
}

use proto::sorter_service_client::SorterServiceClient;
use proto::{DataBatch, DataRecord, SourceMetadata};

#[derive(Clone)]
pub struct BatchSender {
    inner: Arc<BatchSenderInner>,
}

struct BatchSenderInner {
    tx: Mutex<mpsc::Sender<DataBatch>>,
    client: SorterServiceClient<Channel>,
    node_id: String,
    agent_id: String,
    max_retries: usize,
    base_backoff: Duration,
    cancel_token: CancellationToken,
}

impl BatchSender {
    /// Queue a batch for transmission.
    pub async fn send_batch(
        &self,
        file_path: String,
        byte_offset: u64,
        records: Vec<DataRecord>,
    ) -> Result<()> {
        if records.is_empty() {
            return Err(anyhow!("cannot send empty batch"));
        }

        let batch = DataBatch {
            source: Some(SourceMetadata {
                node_id: self.inner.node_id.clone(),
                agent_id: self.inner.agent_id.clone(),
                file_path,
                byte_offset,
            }),
            records,
        };

        let mut attempt: usize = 0;
        let mut backoff = self.inner.base_backoff.max(Duration::from_millis(1));

        loop {
            let sender = { self.inner.tx.lock().await.clone() };
            match sender.send(batch.clone()).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    attempt += 1;
                    if attempt > self.inner.max_retries {
                        return Err(anyhow!(
                            "failed to enqueue batch after {attempt} attempts: {err}"
                        ));
                    }

                    warn!(
                        attempt,
                        max_attempts = self.inner.max_retries,
                        error = %err,
                        "sorter channel send failed; attempting reconnect with backoff"
                    );

                    self.reconnect_stream().await?;
                    tokio::select! {
                        biased;
                        _ = self.inner.cancel_token.cancelled() => {
                            return Err(anyhow!("shutdown"));
                        }
                        _ = sleep(backoff) => {}
                    }
                    backoff = std::cmp::min(backoff.saturating_mul(2), Duration::from_secs(30));
                }
            }
        }
    }

    async fn reconnect_stream(&self) -> Result<()> {
        let mut client = self.inner.client.clone();
        let (tx, rx) = mpsc::channel(1000);
        let request = Request::new(ReceiverStream::new(rx));

        tokio::spawn(async move {
            if let Err(err) = client.stream_data(request).await {
                error!(error = %err, "sorter stream terminated");
            }
        });

        let mut guard = self.inner.tx.lock().await;
        *guard = tx;
        Ok(())
    }
}

#[derive(Clone)]
pub struct SorterClient {
    client: SorterServiceClient<Channel>,
    node_id: String,
    agent_id: String,
}

impl SorterClient {
    pub async fn connect(endpoint: String, node_id: String) -> Result<Self> {
        // Increase gRPC message size limits to handle large batches
        // Default: 4MB, Increased to: 100MB
        const MAX_MESSAGE_SIZE: usize = 100 * 1024 * 1024; // 100 MB

        let client = SorterServiceClient::connect(endpoint)
            .await?
            .max_encoding_message_size(MAX_MESSAGE_SIZE)
            .max_decoding_message_size(MAX_MESSAGE_SIZE);

        Ok(Self {
            client,
            node_id,
            agent_id: uuid::Uuid::new_v4().to_string(),
        })
    }

    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    /// Start unidirectional streaming session
    /// Returns: batch sender handle
    pub async fn start_stream(
        &mut self,
        max_retries: usize,
        base_backoff: Duration,
        cancel_token: CancellationToken,
    ) -> Result<BatchSender> {
        // Limit in-flight batches so a slow gRPC sink can't grow memory without bound.
        let (tx, rx) = mpsc::channel(1000);

        let mut client = self.client.clone();
        let request = Request::new(ReceiverStream::new(rx));

        tokio::spawn(async move {
            if let Err(err) = client.stream_data(request).await {
                tracing::error!(error = %err, "sorter stream terminated");
            }
        });

        Ok(BatchSender {
            inner: Arc::new(BatchSenderInner {
                tx: Mutex::new(tx),
                client: self.client.clone(),
                node_id: self.node_id.clone(),
                agent_id: self.agent_id.clone(),
                max_retries,
                base_backoff,
                cancel_token,
            }),
        })
    }
}
