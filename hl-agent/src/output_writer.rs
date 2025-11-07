use crate::sorter_client::proto::DataRecord;
use crate::sorter_client::BatchSender;
use anyhow::{anyhow, Context, Result};
use serde::Serialize;
use serde_json::{self, Value};
use std::path::PathBuf;
use tokio::fs;
use tonic::async_trait;
use tracing::debug;
use uuid::Uuid;

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
        BatchSender::send_batch(self, file_path, offset, records)
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

    fn block_height_segment(block_height: Option<u64>) -> String {
        block_height
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string())
    }

    fn payload_to_json(payload: &[u8]) -> Value {
        serde_json::from_slice(payload)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(payload).into_owned()))
    }

    fn directory_path(&self, topic: &str, partition_key: &str) -> PathBuf {
        let topic = Self::sanitize_component(topic);
        let partition_key = Self::sanitize_component(partition_key);
        self.output_dir.join(topic).join(partition_key)
    }

    async fn write_record(
        &self,
        source_file_path: &str,
        byte_offset: u64,
        record: DataRecord,
    ) -> Result<()> {
        let directory = self.directory_path(&record.topic, &record.partition_key);
        fs::create_dir_all(&directory)
            .await
            .with_context(|| format!("failed to create directory {}", directory.display()))?;

        let filename = format!(
            "{}_{}.json",
            record.timestamp,
            Self::block_height_segment(record.block_height)
        );
        let final_path = directory.join(&filename);
        let temp_path = directory.join(format!("{}.{}.tmp", filename, Uuid::new_v4()));

        let persisted = PersistedRecord {
            file_path: source_file_path.to_string(),
            byte_offset,
            block_height: record.block_height,
            tx_hash: record.tx_hash.clone(),
            timestamp: record.timestamp,
            topic: record.topic.clone(),
            partition_key: record.partition_key.clone(),
            payload: Self::payload_to_json(&record.payload),
        };

        let serialized =
            serde_json::to_vec(&persisted).context("failed to encode record for file output")?;

        fs::write(&temp_path, &serialized)
            .await
            .with_context(|| format!("failed to write {}", temp_path.display()))?;

        fs::rename(&temp_path, &final_path).await.with_context(|| {
            format!(
                "failed to rename {} to {}",
                temp_path.display(),
                final_path.display()
            )
        })?;

        debug!(
            topic = %record.topic,
            partition_key = %record.partition_key,
            output = %final_path.display(),
            "persisted record to local sink"
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

        for record in records {
            self.write_record(&file_path, offset, record).await?;
        }

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
    partition_key: String,
    payload: Value,
}
