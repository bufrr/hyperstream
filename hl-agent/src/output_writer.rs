use crate::sorter_client::proto::DataRecord;
use crate::sorter_client::BatchSender;
use anyhow::{anyhow, Context, Result};
use futures::{stream, StreamExt, TryStreamExt};
use serde::Serialize;
use serde_json::{self, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;
use tonic::async_trait;
use tracing::{debug, trace};
use uuid::Uuid;

const MAX_CONCURRENT_FILE_WRITES: usize = 256;

#[async_trait]
pub trait RecordSink: Send + Sync {
    async fn send_batch(
        &self,
        file_path: String,
        offset: u64,
        records: Vec<DataRecord>,
    ) -> Result<()>;
}

#[async_trait]
impl RecordSink for BatchSender {
    async fn send_batch(
        &self,
        file_path: String,
        offset: u64,
        records: Vec<DataRecord>,
    ) -> Result<()> {
        BatchSender::send_batch(self, file_path, offset, records).await
    }
}

#[derive(Debug, Clone)]
pub struct FileWriter {
    output_dir: PathBuf,
}

impl FileWriter {
    pub fn new(output_dir: PathBuf) -> Self {
        Self { output_dir }
    }

    fn sanitize_component(component: &str) -> String {
        component
            .chars()
            .map(|ch| match ch {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
                _ => '_',
            })
            .collect()
    }

    fn payload_to_json(payload: &[u8]) -> Value {
        serde_json::from_slice(payload)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(payload).into_owned()))
    }

    fn topic_directory(&self, topic: &str) -> PathBuf {
        let topic = Self::sanitize_component(topic);
        self.output_dir.join(topic)
    }

    async fn write_topic_batch(
        &self,
        source_file_path: &str,
        byte_offset: u64,
        topic: String,
        records: Vec<DataRecord>,
    ) -> Result<()> {
        let directory = self.topic_directory(&topic);
        fs::create_dir_all(&directory)
            .await
            .with_context(|| format!("failed to create directory {}", directory.display()))?;

        let record_count = records.len();
        let first_timestamp = records
            .first()
            .map(|record| record.timestamp)
            .unwrap_or_default();
        let filename = format!(
            "{}_{}_{}_{}.ndjson",
            first_timestamp,
            byte_offset,
            record_count,
            Uuid::new_v4()
        );
        let final_path = directory.join(&filename);
        let temp_path = directory.join(format!("{}.tmp", Uuid::new_v4()));

        let mut buffer = Vec::new();
        for record in records {
            trace!(topic = %record.topic, "encoding record for batch");
            let persisted = PersistedRecord {
                file_path: source_file_path.to_string(),
                byte_offset,
                block_height: record.block_height,
                tx_hash: record.tx_hash.clone(),
                timestamp: record.timestamp,
                topic: record.topic.clone(),
                payload: Self::payload_to_json(&record.payload),
            };

            let mut serialized = serde_json::to_vec(&persisted)
                .context("failed to encode record for file output")?;
            serialized.push(b'\n');
            buffer.extend_from_slice(&serialized);
        }

        fs::write(&temp_path, &buffer)
            .await
            .with_context(|| format!("failed to write {}", temp_path.display()))?;

        fs::rename(&temp_path, &final_path).await.with_context(|| {
            format!(
                "failed to rename {} to {}",
                temp_path.display(),
                final_path.display()
            )
        })?;

        trace!(
            topic = %topic,
            record_count,
            output = %final_path.display(),
            "persisted topic batch to local sink"
        );

        Ok(())
    }
}

#[async_trait]
impl RecordSink for FileWriter {
    async fn send_batch(
        &self,
        file_path: String,
        offset: u64,
        records: Vec<DataRecord>,
    ) -> Result<()> {
        if records.is_empty() {
            return Err(anyhow!("cannot write empty batch"));
        }

        let record_count = records.len();
        let mut grouped_records: HashMap<String, Vec<DataRecord>> = HashMap::new();
        for record in records {
            grouped_records
                .entry(record.topic.clone())
                .or_default()
                .push(record);
        }
        let topic_count = grouped_records.len();

        let concurrency = MAX_CONCURRENT_FILE_WRITES.min(topic_count.max(1));
        debug!(
            file_path = %file_path,
            record_count,
            topic_count,
            concurrency,
            "writing batch to file sink"
        );

        stream::iter(grouped_records.into_iter())
            .map(|(topic, topic_records)| {
                let file_path = file_path.clone();
                async move {
                    self.write_topic_batch(&file_path, offset, topic, topic_records)
                        .await
                }
            })
            .buffer_unordered(concurrency)
            .try_collect::<Vec<_>>()
            .await?;

        Ok(())
    }
}

#[derive(Serialize)]
struct PersistedRecord {
    file_path: String,
    byte_offset: u64,
    block_height: Option<u64>,
    tx_hash: Option<String>,
    timestamp: u64,
    topic: String,
    payload: Value,
}
