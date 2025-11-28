use crate::parsers::current_timestamp;
use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::task;

#[derive(Debug, Clone)]
pub struct CheckpointDB {
    path: Arc<PathBuf>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct CheckpointRecord {
    pub file_path: PathBuf,
    pub byte_offset: u64,
    pub file_size: u64,
    pub last_modified_ts: i64,
    pub updated_at: i64,
}

impl CheckpointDB {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create checkpoint directory {}", parent.display())
            })?;
        }

        let conn = open_connection(&path)?;
        initialize_schema(&conn)?;

        Ok(Self {
            path: Arc::new(path),
        })
    }

    pub async fn get(&self, file_path: &Path) -> Result<Option<CheckpointRecord>> {
        let db_path = self.path.clone();
        let path = normalize_path(file_path);
        task::spawn_blocking(move || {
            let conn = open_connection(&db_path)?;
            let mut stmt = conn
                .prepare(
                    "
                    SELECT file_path, byte_offset, file_size, last_modified_ts, updated_at
                    FROM checkpoints
                    WHERE file_path = ?1
                    ",
                )
                .context("failed to prepare checkpoint select statement")?;

            let record = stmt
                .query_row(params![path], |row| {
                    let file_path: String = row.get(0)?;
                    Ok(CheckpointRecord {
                        file_path: PathBuf::from(file_path),
                        byte_offset: row.get::<_, i64>(1)? as u64,
                        file_size: row.get::<_, i64>(2)? as u64,
                        last_modified_ts: row.get::<_, i64>(3)?,
                        updated_at: row.get::<_, i64>(4)?,
                    })
                })
                .optional()
                .context("failed to query checkpoint table")?;

            Ok::<_, anyhow::Error>(record)
        })
        .await
        .context("checkpoint get join error")?
    }

    pub async fn get_offset(&self, file_path: &Path) -> Result<u64> {
        Ok(self
            .get(file_path)
            .await?
            .map(|rec| rec.byte_offset)
            .unwrap_or(0))
    }

    pub async fn set_offset(
        &self,
        file_path: &Path,
        offset: u64,
        file_size: u64,
        last_modified_ts: i64,
    ) -> Result<()> {
        let db_path = self.path.clone();
        let path = normalize_path(file_path);
        let timestamp = current_timestamp();
        task::spawn_blocking(move || {
            let conn = open_connection(&db_path)?;
            conn.execute(
                "
                INSERT INTO checkpoints (
                    file_path,
                    byte_offset,
                    file_size,
                    last_modified_ts,
                    updated_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(file_path) DO UPDATE SET
                    byte_offset = excluded.byte_offset,
                    file_size = excluded.file_size,
                    last_modified_ts = excluded.last_modified_ts,
                    updated_at = excluded.updated_at
                ",
                params![
                    path,
                    offset as i64,
                    file_size as i64,
                    last_modified_ts,
                    timestamp
                ],
            )
            .context("failed to upsert checkpoint record")?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("checkpoint set join error")??;

        Ok(())
    }
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn open_connection(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("failed to open checkpoint db {}", path.display()))?;

    conn.pragma_update(None, "journal_mode", "WAL")
        .context("failed to enable WAL mode for checkpoint db")?;
    conn.pragma_update(None, "synchronous", "NORMAL")
        .context("failed to set checkpoint db synchronous mode")?;

    Ok(conn)
}

fn initialize_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS checkpoints (
            file_path TEXT PRIMARY KEY,
            byte_offset INTEGER NOT NULL,
            file_size INTEGER NOT NULL,
            last_modified_ts INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        ",
    )
    .context("failed to create checkpoints table")?;

    Ok(())
}
