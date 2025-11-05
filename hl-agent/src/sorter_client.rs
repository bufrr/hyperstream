use anyhow::{anyhow, Result};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;
use tonic::Request;

pub mod proto {
    tonic::include_proto!("hyperstream");
}

use proto::sorter_service_client::SorterServiceClient;
use proto::{DataBatch, DataRecord, SourceMetadata};

#[derive(Clone)]
pub struct BatchSender {
    tx: mpsc::UnboundedSender<DataBatch>,
    node_id: String,
    agent_id: String,
}

impl BatchSender {
    /// Queue a batch for transmission.
    pub fn send_batch(
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
                node_id: self.node_id.clone(),
                agent_id: self.agent_id.clone(),
                file_path,
                byte_offset,
            }),
            records,
        };

        self.tx
            .send(batch)
            .map_err(|err| anyhow!("failed to enqueue batch for sorter stream: {err}"))
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
        let client = SorterServiceClient::connect(endpoint).await?;

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
    pub async fn start_stream(&mut self) -> Result<BatchSender> {
        let (tx, rx) = mpsc::unbounded_channel();

        let mut client = self.client.clone();
        let request = Request::new(UnboundedReceiverStream::new(rx));

        tokio::spawn(async move {
            if let Err(err) = client.stream_data(request).await {
                tracing::error!(error = %err, "sorter stream terminated");
            }
        });

        Ok(BatchSender {
            tx,
            node_id: self.node_id.clone(),
            agent_id: self.agent_id.clone(),
        })
    }
}
