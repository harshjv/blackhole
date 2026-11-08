use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS principal (
  id INTEGER PRIMARY KEY,
  kind TEXT NOT NULL,
  name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS access_key (
  id TEXT PRIMARY KEY,
  secret TEXT NOT NULL UNIQUE,
  principal_id INTEGER NOT NULL REFERENCES principal(id),
  drive_id INTEGER NOT NULL REFERENCES drive(id),
  prefix_node INTEGER,
  read_only INTEGER NOT NULL DEFAULT 0,
  admin INTEGER NOT NULL DEFAULT 0,
  expires_at INTEGER,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS drive (
  id INTEGER PRIMARY KEY,
  alias TEXT NOT NULL UNIQUE,
  visibility TEXT NOT NULL DEFAULT 'private',
  hold INTEGER NOT NULL DEFAULT 0,
  parent_id INTEGER REFERENCES drive(id),
  fork_seq INTEGER,
  chain_depth INTEGER NOT NULL DEFAULT 0,
  head_seq INTEGER NOT NULL DEFAULT 0,
  next_node INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS "commit" (
  drive_id INTEGER NOT NULL REFERENCES drive(id),
  seq INTEGER NOT NULL,
  committed_at INTEGER NOT NULL,
  actor_id INTEGER NOT NULL REFERENCES principal(id),
  intent TEXT NOT NULL DEFAULT '',
  row_digest BLOB NOT NULL,
  prev_hash BLOB NOT NULL,
  hash BLOB NOT NULL,
  PRIMARY KEY (drive_id, seq)
);
CREATE INDEX IF NOT EXISTS commit_actor ON "commit"(drive_id, actor_id, seq);
CREATE TABLE IF NOT EXISTS dentry (
  id INTEGER PRIMARY KEY,
  drive_id INTEGER NOT NULL,
  parent_node INTEGER NOT NULL,
  name TEXT NOT NULL,
  node INTEGER,
  from_seq INTEGER NOT NULL,
  to_seq INTEGER
);
CREATE INDEX IF NOT EXISTS dentry_lookup ON dentry(drive_id, parent_node, name, from_seq);
CREATE INDEX IF NOT EXISTS dentry_node ON dentry(drive_id, node, from_seq);
CREATE INDEX IF NOT EXISTS dentry_from ON dentry(drive_id, from_seq);
CREATE INDEX IF NOT EXISTS dentry_to ON dentry(drive_id, to_seq);
CREATE TABLE IF NOT EXISTS node_version (
  id INTEGER PRIMARY KEY,
  drive_id INTEGER NOT NULL,
  node INTEGER NOT NULL,
  kind TEXT NOT NULL,
  size INTEGER NOT NULL DEFAULT 0,
  mtime INTEGER NOT NULL DEFAULT 0,
  manifest_hash BLOB,
  purged_seq INTEGER,
  from_seq INTEGER NOT NULL,
  to_seq INTEGER
);
CREATE INDEX IF NOT EXISTS node_version_lookup ON node_version(drive_id, node, from_seq);
CREATE INDEX IF NOT EXISTS node_version_from ON node_version(drive_id, from_seq);
CREATE INDEX IF NOT EXISTS node_version_to ON node_version(drive_id, to_seq);
CREATE TABLE IF NOT EXISTS manifest (
  hash BLOB PRIMARY KEY,
  body BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS pin (
  drive_id INTEGER NOT NULL,
  node INTEGER NOT NULL,
  PRIMARY KEY (drive_id, node)
);
CREATE TABLE IF NOT EXISTS pending_upload (
  hash BLOB PRIMARY KEY,
  since INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS shard (
  hash BLOB PRIMARY KEY,
  size INTEGER NOT NULL
);
"#;

pub async fn open(path: &Path) -> Result<SqlitePool> {
    let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))
        .context("sqlite url")?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(30))
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new().max_connections(8).connect_with(opts).await.context("opening authority db")?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await.context("applying schema")?;
    Ok(pool)
}
