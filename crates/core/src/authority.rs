use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;

use anyhow::{Context, Result, bail};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqliteConnection, SqlitePool};

use crate::format::{Hash, Manifest};
use crate::store::ShardStore;

pub const ROOT_NODE: i64 = 0;
pub const MAX_CHAIN_DEPTH: i64 = 8;

#[derive(Debug, thiserror::Error)]
pub enum BhError {
    #[error("access denied: {0}")]
    Denied(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
}

#[derive(Clone, Debug)]
pub struct Key {
    pub id: String,
    pub secret: String,
    pub principal_id: i64,
    pub drive_id: i64,
    pub prefix_node: Option<i64>,
    pub read_only: bool,
    pub admin: bool,
    pub expires_at: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct Ctx {
    pub principal_id: i64,
    pub principal_name: String,
    pub key: Key,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Read,
    Write,
    Admin,
}

#[derive(Clone, Debug)]
pub struct Drive {
    pub id: i64,
    pub alias: String,
    pub visibility: String,
    pub hold: bool,
    pub parent_id: Option<i64>,
    pub fork_seq: Option<i64>,
    pub chain_depth: i64,
    pub head_seq: i64,
    pub next_node: i64,
    pub created_at: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    File,
    Dir,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::File => "file",
            Kind::Dir => "dir",
        }
    }

    fn parse(s: &str) -> Kind {
        if s == "dir" { Kind::Dir } else { Kind::File }
    }
}

#[derive(Clone, Debug)]
pub struct Version {
    pub drive_id: i64,
    pub node: i64,
    pub kind: Kind,
    pub size: u64,
    pub mtime: i64,
    pub manifest_hash: Option<Hash>,
    pub purged_seq: Option<i64>,
    pub from_seq: i64,
    pub to_seq: Option<i64>,
}

#[derive(Clone, Debug)]
pub enum Op {
    Put { path: String, manifest_hash: Hash, size: u64, mtime: i64 },
    PutIf { path: String, manifest_hash: Hash, size: u64, mtime: i64, expect_seq: i64 },
    Mkdir { path: String },
    Delete { path: String },
    Rename { from: String, to: String },
}

impl Op {
    fn path(&self) -> &str {
        match self {
            Op::Put { path, .. } | Op::PutIf { path, .. } | Op::Mkdir { path } | Op::Delete { path } => path,
            Op::Rename { from, .. } => from,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CommitInfo {
    pub drive_id: i64,
    pub seq: i64,
    pub committed_at: i64,
    pub actor: String,
    pub intent: String,
    pub changes: i64,
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub name: String,
    pub node: i64,
    pub version: Version,
}

#[derive(Clone, Debug)]
pub struct Event {
    pub drive_alias: String,
    pub seq: i64,
    pub at: i64,
    pub actor: String,
    pub intent: String,
    pub what: String,
}

#[derive(Clone, Debug)]
pub struct UndoReport {
    pub commit: Option<CommitInfo>,
    pub restored: Vec<String>,
    pub conflicts: Vec<String>,
    pub since_seq: Option<i64>,
}

#[derive(Clone)]
pub struct Authority {
    pub pool: SqlitePool,
    pub store: ShardStore,
    manifests: std::sync::Arc<quick_cache::sync::Cache<Hash, std::sync::Arc<Manifest>, ManifestWeight>>,
    used_memo: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<Hash, (u64, i64)>>>,
}

#[derive(Clone)]
pub struct ManifestWeight;

impl quick_cache::Weighter<Hash, std::sync::Arc<Manifest>> for ManifestWeight {
    fn weight(&self, _key: &Hash, val: &std::sync::Arc<Manifest>) -> u64 {
        val.shards.len() as u64 + 1
    }
}

type Chain = Vec<(Drive, i64)>;

pub fn now_millis() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn components(path: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for c in path.split('/') {
        match c {
            "" | "." => {}
            ".." => bail!("path may not contain '..': {path}"),
            c => out.push(c.to_string()),
        }
    }
    Ok(out)
}

pub fn normalize(path: &str) -> Result<String> {
    Ok(format!("/{}", components(path)?.join("/")))
}

fn split_parent(path: &str) -> Result<(String, String)> {
    let mut c = components(path)?;
    let name = c.pop().ok_or_else(|| anyhow::anyhow!("root has no parent"))?;
    Ok((format!("/{}", c.join("/")), name))
}

fn random_id() -> Result<String> {
    let mut buf = [0u8; 16];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut buf)?;
    Ok(hex::encode(buf))
}

fn drive_from_row(r: &SqliteRow) -> Drive {
    Drive {
        id: r.get("id"),
        alias: r.get("alias"),
        visibility: r.get("visibility"),
        hold: r.get("hold"),
        parent_id: r.get("parent_id"),
        fork_seq: r.get("fork_seq"),
        chain_depth: r.get("chain_depth"),
        head_seq: r.get("head_seq"),
        next_node: r.get("next_node"),
        created_at: r.get("created_at"),
    }
}

fn version_from_row(r: &SqliteRow) -> Version {
    let mh: Option<Vec<u8>> = r.get("manifest_hash");
    Version {
        drive_id: r.get("drive_id"),
        node: r.get("node"),
        kind: Kind::parse(r.get("kind")),
        size: r.get::<i64, _>("size") as u64,
        mtime: r.get("mtime"),
        manifest_hash: mh.map(|v| Hash(v.try_into().expect("32-byte hash"))),
        purged_seq: r.get("purged_seq"),
        from_seq: r.get("from_seq"),
        to_seq: r.get("to_seq"),
    }
}

fn root_version(drive_id: i64) -> Version {
    Version { drive_id, node: ROOT_NODE, kind: Kind::Dir, size: 0, mtime: 0, manifest_hash: None, purged_seq: None, from_seq: 0, to_seq: None }
}

impl Authority {
    pub async fn open(root: &std::path::Path) -> Result<Authority> {
        std::fs::create_dir_all(root)?;
        let pool = crate::db::open(&root.join("authority.sqlite")).await?;
        let (store, _) = ShardStore::from_env(root)?;
        store.uploads().attach(pool.clone());
        let manifests = std::sync::Arc::new(quick_cache::sync::Cache::with_weighter(4096, 16_000_000, ManifestWeight));
        let auth = Authority { pool, store, manifests, used_memo: Default::default() };
        auth.reload_pins().await?;
        Ok(auth)
    }

    pub async fn principal(&self, name: &str) -> Result<i64> {
        let mut conn = self.pool.acquire().await?;
        ensure_principal(&mut conn, name, "user").await
    }

    pub async fn create_drive(&self, alias: &str, owner: &str) -> Result<(Drive, Key)> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let owner_id = ensure_principal(&mut tx, owner, "user").await?;
        let now = now_millis();
        sqlx::query("INSERT INTO drive(alias, created_at) VALUES (?, ?)").bind(alias).bind(now).execute(&mut *tx).await.context("creating drive")?;
        let drive = drive_by_alias(&mut tx, alias).await?;
        let key = insert_key(&mut tx, owner_id, drive.id, None, false, true, None).await?;
        tx.commit().await?;
        Ok((drive, key))
    }

    pub async fn drives(&self) -> Result<Vec<Drive>> {
        let rows = sqlx::query("SELECT * FROM drive ORDER BY id").fetch_all(&self.pool).await?;
        Ok(rows.iter().map(drive_from_row).collect())
    }

    pub async fn drive(&self, alias: &str) -> Result<Drive> {
        let mut conn = self.pool.acquire().await?;
        drive_by_alias(&mut conn, alias).await
    }

    pub async fn ctx(&self, secret: &str) -> Result<Ctx> {
        let row = sqlx::query(
            "SELECT k.*, p.name AS principal_name FROM access_key k JOIN principal p ON p.id = k.principal_id WHERE k.secret = ?",
        )
        .bind(secret)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| BhError::Denied("unknown key".into()))?;
        let key = key_from_row(&row);
        Ok(Ctx { principal_id: key.principal_id, principal_name: row.get("principal_name"), key })
    }

    pub async fn ctx_by_id(&self, key_id: &str) -> Result<Ctx> {
        let row = sqlx::query(
            "SELECT k.*, p.name AS principal_name FROM access_key k JOIN principal p ON p.id = k.principal_id WHERE k.id = ?",
        )
        .bind(key_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| BhError::Denied("unknown key id".into()))?;
        let key = key_from_row(&row);
        Ok(Ctx { principal_id: key.principal_id, principal_name: row.get("principal_name"), key })
    }

    pub async fn create_key(
        &self,
        ctx: &Ctx,
        alias: &str,
        principal: &str,
        prefix: Option<&str>,
        read_only: bool,
        admin: bool,
        ttl_millis: Option<i64>,
    ) -> Result<Key> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let drive = drive_by_alias(&mut tx, alias).await?;
        let chain = chain_at(&mut tx, &drive, drive.head_seq).await?;
        authorize(&mut tx, ctx, &drive, &chain, Some(ROOT_NODE), Action::Admin).await?;
        let prefix_node = match prefix {
            Some(p) => Some(resolve_path(&mut tx, &chain, p).await?.ok_or_else(|| BhError::NotFound(p.to_string()))?.1.node),
            None => None,
        };
        let pid = ensure_principal(&mut tx, principal, "key").await?;
        let key = insert_key(&mut tx, pid, drive.id, prefix_node, read_only, admin, ttl_millis.map(|t| now_millis() + t)).await?;
        tx.commit().await?;
        Ok(key)
    }

    pub async fn fork(&self, ctx: &Ctx, alias: &str, new_alias: &str) -> Result<(Drive, Key)> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let parent = drive_by_alias(&mut tx, alias).await?;
        let chain = chain_at(&mut tx, &parent, parent.head_seq).await?;
        authorize(&mut tx, ctx, &parent, &chain, Some(ROOT_NODE), Action::Admin).await?;
        if parent.chain_depth + 1 > MAX_CHAIN_DEPTH {
            bail!("fork chain depth {} exceeds {MAX_CHAIN_DEPTH}; flattening is not implemented", parent.chain_depth + 1);
        }
        sqlx::query("INSERT INTO drive(alias, parent_id, fork_seq, chain_depth, next_node, created_at) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(new_alias)
            .bind(parent.id)
            .bind(parent.head_seq)
            .bind(parent.chain_depth + 1)
            .bind(parent.next_node)
            .bind(now_millis())
            .execute(&mut *tx)
            .await
            .context("creating fork")?;
        let drive = drive_by_alias(&mut tx, new_alias).await?;
        let key = insert_key(&mut tx, ctx.principal_id, drive.id, None, false, true, None).await?;
        tx.commit().await?;
        Ok((drive, key))
    }

    pub async fn store_manifests(&self, manifests: &[Manifest]) -> Result<Vec<Hash>> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let mut hashes = Vec::with_capacity(manifests.len());
        for manifest in manifests {
            let body = manifest.encode();
            let hash = Hash::of(&body);
            sqlx::query("INSERT OR IGNORE INTO manifest(hash, body) VALUES (?, ?)").bind(hash.0.to_vec()).bind(body).execute(&mut *tx).await?;
            for s in &manifest.shards {
                sqlx::query("INSERT OR IGNORE INTO shard(hash, size) VALUES (?, ?)").bind(s.hash.0.to_vec()).bind(s.len as i64).execute(&mut *tx).await?;
            }
            hashes.push(hash);
        }
        tx.commit().await?;
        Ok(hashes)
    }

    pub async fn store_manifest(&self, manifest: &Manifest) -> Result<Hash> {
        Ok(self.store_manifests(std::slice::from_ref(manifest)).await?[0])
    }

    pub async fn manifest(&self, hash: Hash) -> Result<std::sync::Arc<Manifest>> {
        if let Some(m) = self.manifests.get(&hash) {
            return Ok(m);
        }
        let row = sqlx::query("SELECT body FROM manifest WHERE hash = ?")
            .bind(hash.0.to_vec())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| BhError::NotFound(format!("manifest {hash}")))?;
        let body: Vec<u8> = row.get("body");
        let m = std::sync::Arc::new(tokio::task::spawn_blocking(move || Manifest::decode(&body)).await??);
        self.manifests.insert(hash, m.clone());
        Ok(m)
    }

    pub async fn commit(&self, ctx: &Ctx, alias: &str, intent: &str, ops: Vec<Op>) -> Result<CommitInfo> {
        self.commit_at(ctx, alias, intent, ops, None).await
    }

    pub async fn commit_at(&self, ctx: &Ctx, alias: &str, intent: &str, ops: Vec<Op>, expect_head: Option<i64>) -> Result<CommitInfo> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let mut drive = drive_by_alias(&mut tx, alias).await?;
        if let Some(h) = expect_head
            && drive.head_seq != h
        {
            return Err(BhError::Conflict(format!("{alias} moved from #{h} to #{} while planning; retry", drive.head_seq)).into());
        }
        let seq = drive.head_seq + 1;
        let chain = chain_at(&mut tx, &drive, seq).await?;
        for op in &ops {
            let node = deepest_existing(&mut tx, &chain, op.path()).await?;
            authorize(&mut tx, ctx, &drive, &chain, Some(node), Action::Write).await?;
            if let Op::Rename { to, .. } = op {
                let node = deepest_existing(&mut tx, &chain, to).await?;
                authorize(&mut tx, ctx, &drive, &chain, Some(node), Action::Write).await?;
            }
        }
        let mut changes = 0i64;
        for op in ops {
            changes += apply_op(&mut tx, &mut drive, &chain, seq, op).await?;
        }
        let now = seal_commit(&mut tx, &drive, seq, ctx.principal_id, intent).await?;
        sqlx::query("UPDATE drive SET head_seq = ?, next_node = ? WHERE id = ?").bind(seq).bind(drive.next_node).bind(drive.id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(CommitInfo { drive_id: drive.id, seq, committed_at: now, actor: ctx.principal_name.clone(), intent: intent.to_string(), changes })
    }

    pub async fn verify(&self, ctx: &Ctx, alias: &str) -> Result<Vec<VerifyLine>> {
        let mut conn = self.pool.acquire().await?;
        let drive = drive_by_alias(&mut conn, alias).await?;
        let head = chain_at(&mut conn, &drive, drive.head_seq).await?;
        authorize(&mut conn, ctx, &drive, &head, None, Action::Read).await?;
        let rows = sqlx::query("SELECT seq, committed_at, actor_id, intent, row_digest, prev_hash, hash FROM \"commit\" WHERE drive_id = ? ORDER BY seq").bind(drive.id).fetch_all(&mut *conn).await?;
        let mut out = Vec::with_capacity(rows.len());
        let mut expected_prev = genesis_prev(&mut conn, &drive).await?;
        for r in rows {
            let seq: i64 = r.get("seq");
            let stored_prev: Vec<u8> = r.get("prev_hash");
            let stored_digest: Vec<u8> = r.get("row_digest");
            let stored_hash: Vec<u8> = r.get("hash");
            let header = commit_hash(drive.id, seq, r.get("committed_at"), r.get("actor_id"), r.get("intent"), &stored_prev, &stored_digest);
            let purged: i64 = sqlx::query("SELECT COUNT(*) AS n FROM node_version WHERE drive_id = ? AND from_seq = ? AND purged_seq IS NOT NULL").bind(drive.id).bind(seq).fetch_one(&mut *conn).await?.get("n");
            let status = if stored_prev != expected_prev {
                "bad chain: prev_hash does not match the previous commit".to_string()
            } else if header.0.as_slice() != stored_hash.as_slice() {
                "bad header: commit fields were altered".to_string()
            } else if purged > 0 {
                let at: Option<i64> = sqlx::query("SELECT MIN(purged_seq) AS p FROM node_version WHERE drive_id = ? AND from_seq = ?").bind(drive.id).bind(seq).fetch_one(&mut *conn).await?.get("p");
                format!("ok, content purged at #{}", at.unwrap_or(0))
            } else if row_digest(&mut conn, drive.id, seq).await?.0.as_slice() != stored_digest.as_slice() {
                "bad rows: file or name rows of this commit were altered".to_string()
            } else {
                "ok".to_string()
            };
            out.push(VerifyLine { seq, hash: hex::encode(&stored_hash), status });
            expected_prev = stored_hash;
        }
        Ok(out)
    }

    pub async fn attest(&self, ctx: &Ctx, alias: &str) -> Result<Attestation> {
        let mut conn = self.pool.acquire().await?;
        let drive = drive_by_alias(&mut conn, alias).await?;
        let head = chain_at(&mut conn, &drive, drive.head_seq).await?;
        authorize(&mut conn, ctx, &drive, &head, None, Action::Read).await?;
        let hash: Option<Vec<u8>> = sqlx::query("SELECT hash FROM \"commit\" WHERE drive_id = ? AND seq = ?").bind(drive.id).bind(drive.head_seq).fetch_optional(&mut *conn).await?.map(|r| r.get("hash"));
        Ok(Attestation { drive: drive.alias, head_seq: drive.head_seq, head_hash: hash.map(hex::encode).unwrap_or_else(|| "0".repeat(64)), at: now_millis(), hold: drive.hold })
    }

    pub async fn set_hold(&self, ctx: &Ctx, alias: &str, on: bool) -> Result<()> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let drive = drive_by_alias(&mut tx, alias).await?;
        let head = chain_at(&mut tx, &drive, drive.head_seq).await?;
        authorize(&mut tx, ctx, &drive, &head, Some(ROOT_NODE), Action::Admin).await?;
        sqlx::query("UPDATE drive SET hold = ? WHERE id = ?").bind(on).bind(drive.id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn purge(&self, ctx: &Ctx, alias: &str, path: &str, reason: &str) -> Result<PurgeReport> {
        let path = normalize(path)?;
        let mut removed_manifests: Vec<Hash> = Vec::new();
        let (seq, versions) = {
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let mut drive = drive_by_alias(&mut tx, alias).await?;
            if drive.hold {
                return Err(BhError::Conflict(format!("{alias} is under legal hold; purge refused")).into());
            }
            let seq = drive.head_seq + 1;
            let chain = chain_at(&mut tx, &drive, seq).await?;
            authorize(&mut tx, ctx, &drive, &chain, Some(ROOT_NODE), Action::Admin).await?;
            let node = match resolve_path(&mut tx, &chain, &path).await? {
                Some((n, _)) => n,
                None => find_node_ever(&mut tx, &chain, &path).await?.ok_or_else(|| BhError::NotFound(path.clone()))?,
            };
            if resolve_path(&mut tx, &chain, &path).await?.is_some() {
                apply_op(&mut tx, &mut drive, &chain, seq, Op::Delete { path: path.clone() }).await?;
            }
            let rows = sqlx::query("SELECT id, manifest_hash FROM node_version WHERE drive_id = ? AND node = ? AND manifest_hash IS NOT NULL").bind(drive.id).bind(node).fetch_all(&mut *tx).await?;
            let mut hashes: Vec<Hash> = Vec::new();
            for r in &rows {
                let id: i64 = r.get("id");
                let h: Vec<u8> = r.get("manifest_hash");
                hashes.push(Hash(h.try_into().expect("32-byte hash")));
                sqlx::query("UPDATE node_version SET manifest_hash = NULL, purged_seq = ? WHERE id = ?").bind(seq).bind(id).execute(&mut *tx).await?;
            }
            hashes.sort();
            hashes.dedup();
            for h in hashes {
                let still: i64 = sqlx::query("SELECT COUNT(*) AS n FROM node_version WHERE manifest_hash = ?").bind(h.0.to_vec()).fetch_one(&mut *tx).await?.get("n");
                if still == 0 {
                    sqlx::query("DELETE FROM manifest WHERE hash = ?").bind(h.0.to_vec()).execute(&mut *tx).await?;
                    removed_manifests.push(h);
                }
            }
            seal_commit(&mut tx, &drive, seq, ctx.principal_id, &format!("purge {path}: {reason}")).await?;
            sqlx::query("UPDATE drive SET head_seq = ?, next_node = ? WHERE id = ?").bind(seq).bind(drive.next_node).bind(drive.id).execute(&mut *tx).await?;
            tx.commit().await?;
            (seq, rows.len() as i64)
        };
        let shards_removed = self.collect_shards().await?;
        Ok(PurgeReport { seq, versions_purged: versions, manifests_removed: removed_manifests.len() as i64, shards_removed })
    }

    pub async fn gc(&self, ctx: &Ctx, alias: &str, keep_versions: i64, keep_millis: Option<i64>) -> Result<GcReport> {
        let mut removed_manifests = 0i64;
        let (seq, expired) = {
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let drive = drive_by_alias(&mut tx, alias).await?;
            if drive.hold {
                return Err(BhError::Conflict(format!("{alias} is under legal hold; gc refused")).into());
            }
            let seq = drive.head_seq + 1;
            let chain = chain_at(&mut tx, &drive, seq).await?;
            authorize(&mut tx, ctx, &drive, &chain, Some(ROOT_NODE), Action::Admin).await?;
            let fork_points: Vec<i64> = sqlx::query("SELECT fork_seq FROM drive WHERE parent_id = ?").bind(drive.id).fetch_all(&mut *tx).await?.iter().map(|r| r.get::<i64, _>("fork_seq")).collect();
            let cutoff = keep_millis.map(|k| now_millis() - k);
            let rows = sqlx::query(
                "SELECT v.id, v.node, v.manifest_hash, v.from_seq, v.to_seq, c.committed_at AS closed_at FROM node_version v \
                 JOIN \"commit\" c ON c.drive_id = v.drive_id AND c.seq = v.to_seq \
                 WHERE v.drive_id = ? AND v.kind = 'file' AND v.to_seq IS NOT NULL AND v.manifest_hash IS NOT NULL ORDER BY v.node, v.to_seq DESC",
            )
            .bind(drive.id)
            .fetch_all(&mut *tx)
            .await?;
            let mut per_node: BTreeMap<i64, i64> = BTreeMap::new();
            let mut expired: Vec<(i64, Hash)> = Vec::new();
            for r in rows {
                let node: i64 = r.get("node");
                let rank = per_node.entry(node).or_insert(0);
                *rank += 1;
                let from: i64 = r.get("from_seq");
                let to: i64 = r.get("to_seq");
                let pinned_by_fork = fork_points.iter().any(|f| from <= *f && *f < to);
                let young = cutoff.map(|c| r.get::<i64, _>("closed_at") > c).unwrap_or(false);
                if *rank <= keep_versions || pinned_by_fork || young {
                    continue;
                }
                let h: Vec<u8> = r.get("manifest_hash");
                expired.push((r.get("id"), Hash(h.try_into().expect("32-byte hash"))));
            }
            for (id, _) in &expired {
                sqlx::query("UPDATE node_version SET manifest_hash = NULL, purged_seq = ? WHERE id = ?").bind(seq).bind(id).execute(&mut *tx).await?;
            }
            let mut hashes: Vec<Hash> = expired.iter().map(|(_, h)| *h).collect();
            hashes.sort();
            hashes.dedup();
            for h in hashes {
                let still: i64 = sqlx::query("SELECT COUNT(*) AS n FROM node_version WHERE manifest_hash = ?").bind(h.0.to_vec()).fetch_one(&mut *tx).await?.get("n");
                if still == 0 {
                    sqlx::query("DELETE FROM manifest WHERE hash = ?").bind(h.0.to_vec()).execute(&mut *tx).await?;
                    removed_manifests += 1;
                }
            }
            if expired.is_empty() {
                return Ok(GcReport { seq: None, versions_expired: 0, manifests_removed: 0, shards_removed: 0 });
            }
            let intent = match keep_millis {
                Some(k) => format!("gc --keep-versions {keep_versions} --keep {} s", k / 1000),
                None => format!("gc --keep-versions {keep_versions}"),
            };
            seal_commit(&mut tx, &drive, seq, ctx.principal_id, &intent).await?;
            sqlx::query("UPDATE drive SET head_seq = ? WHERE id = ?").bind(seq).bind(drive.id).execute(&mut *tx).await?;
            tx.commit().await?;
            (seq, expired.len() as i64)
        };
        let shards_removed = self.collect_shards().await?;
        Ok(GcReport { seq: Some(seq), versions_expired: expired, manifests_removed: removed_manifests, shards_removed })
    }

    pub async fn collect_shards(&self) -> Result<i64> {
        let bodies = sqlx::query("SELECT body FROM manifest").fetch_all(&self.pool).await?;
        let mut live: BTreeSet<Hash> = BTreeSet::new();
        for b in bodies {
            let m = Manifest::decode(b.get::<Vec<u8>, _>("body").as_slice())?;
            live.extend(m.shards.iter().map(|s| s.hash));
        }
        let rows = sqlx::query("SELECT hash FROM shard").fetch_all(&self.pool).await?;
        let mut removed = 0;
        for r in rows {
            let h = Hash(r.get::<Vec<u8>, _>("hash").try_into().expect("32-byte hash"));
            if !live.contains(&h) {
                self.store.delete(h).await?;
                sqlx::query("DELETE FROM shard WHERE hash = ?").bind(h.0.to_vec()).execute(&self.pool).await?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    pub async fn seq_at(&self, alias: &str, at: At) -> Result<(Drive, i64)> {
        let mut conn = self.pool.acquire().await?;
        let drive = drive_by_alias(&mut conn, alias).await?;
        let (d, seq) = resolve_at(&mut conn, &drive, at).await?;
        Ok((d, seq))
    }

    pub async fn stat(&self, ctx: &Ctx, alias: &str, path: &str, at: At) -> Result<Option<(i64, Version)>> {
        let mut conn = self.pool.acquire().await?;
        let drive = drive_by_alias(&mut conn, alias).await?;
        let head_chain = chain_at(&mut conn, &drive, drive.head_seq).await?;
        authorize(&mut conn, ctx, &drive, &head_chain, None, Action::Read).await?;
        let (read_drive, seq) = resolve_at(&mut conn, &drive, at).await?;
        let chain = chain_at(&mut conn, &read_drive, seq).await?;
        let found = resolve_path(&mut conn, &chain, path).await?;
        if let Some((node, _)) = &found {
            authorize(&mut conn, ctx, &drive, &head_chain, Some(*node), Action::Read).await?;
        }
        Ok(found)
    }

    pub async fn list(&self, ctx: &Ctx, alias: &str, path: &str, at: At) -> Result<Vec<Entry>> {
        let mut conn = self.pool.acquire().await?;
        let drive = drive_by_alias(&mut conn, alias).await?;
        let (read_drive, seq) = resolve_at(&mut conn, &drive, at).await?;
        let chain = chain_at(&mut conn, &read_drive, seq).await?;
        let (node, version) = resolve_path(&mut conn, &chain, path).await?.ok_or_else(|| BhError::NotFound(path.to_string()))?;
        let head_chain = chain_at(&mut conn, &drive, drive.head_seq).await?;
        authorize(&mut conn, ctx, &drive, &head_chain, Some(node), Action::Read).await?;
        if version.kind != Kind::Dir {
            return Ok(vec![Entry { name: split_parent(path)?.1, node, version }]);
        }
        let names = readdir(&mut conn, &chain, node).await?;
        let mut out = Vec::with_capacity(names.len());
        for (name, child) in names {
            let v = version_at(&mut conn, &chain, child).await?.ok_or_else(|| anyhow::anyhow!("dangling node {child}"))?;
            out.push(Entry { name, node: child, version: v });
        }
        Ok(out)
    }

    pub async fn open_file(&self, ctx: &Ctx, alias: &str, path: &str, at: At) -> Result<(Version, std::sync::Arc<Manifest>)> {
        let (_, version) = self.stat(ctx, alias, path, at).await?.ok_or_else(|| BhError::NotFound(path.to_string()))?;
        if version.kind != Kind::File {
            bail!("{path} is a directory");
        }
        let manifest = match (version.manifest_hash, version.purged_seq) {
            (Some(h), _) => self.manifest(h).await?,
            (None, Some(p)) => return Err(BhError::NotFound(format!("{path} was purged at #{p}")).into()),
            (None, None) => std::sync::Arc::new(Manifest::default()),
        };
        Ok((version, manifest))
    }

    pub async fn read_range(&self, manifest: &Manifest, offset: u64, len: u64) -> Result<(Vec<u8>, u64)> {
        let total = manifest.size();
        if offset >= total || len == 0 {
            return Ok((Vec::new(), 0));
        }
        let end = (offset + len).min(total);
        let mut out = Vec::with_capacity((end - offset) as usize);
        let mut fetched = 0u64;
        let (mut i, mut inner) = manifest.locate(offset).unwrap();
        let mut pos = offset;
        while pos < end {
            let s = manifest.shards[i];
            let take = ((s.len as u64) - inner).min(end - pos);
            let local = self.store.cache().map(|c| c.cached_bytes(std::iter::once(s.hash)) > 0).unwrap_or(true);
            if !local && self.store.uploads().is_pending(s.hash).await {
                return Err(BhError::Conflict(format!("shard {} of this version is still uploading from the writer's machine; try again shortly", i + 1)).into());
            }
            if inner == 0 && take == s.len as u64 {
                let bytes = self.store.get(s.hash).await?;
                out.extend_from_slice(&bytes);
            } else {
                let bytes = self.store.get_range(s.hash, inner..inner + take).await?;
                out.extend_from_slice(&bytes);
            }
            fetched += 1;
            pos += take;
            inner = 0;
            i += 1;
        }
        Ok((out, fetched))
    }

    pub async fn history(&self, ctx: &Ctx, alias: &str, path: &str) -> Result<Vec<Event>> {
        let mut conn = self.pool.acquire().await?;
        let drive = drive_by_alias(&mut conn, alias).await?;
        let chain = chain_at(&mut conn, &drive, drive.head_seq).await?;
        let node = match resolve_path(&mut conn, &chain, path).await? {
            Some((n, _)) => n,
            None => find_node_ever(&mut conn, &chain, path).await?.ok_or_else(|| BhError::NotFound(path.to_string()))?,
        };
        authorize(&mut conn, ctx, &drive, &chain, Some(node), Action::Read).await?;
        let mut events = Vec::new();
        for (d, seq) in chain.iter().rev() {
            let rows = sqlx::query(
                "SELECT v.from_seq AS seq, v.size, v.kind, v.purged_seq, c.committed_at, c.intent, p.name AS actor FROM node_version v \
                 JOIN \"commit\" c ON c.drive_id = v.drive_id AND c.seq = v.from_seq JOIN principal p ON p.id = c.actor_id \
                 WHERE v.drive_id = ? AND v.node = ? AND v.from_seq <= ? ORDER BY v.from_seq",
            )
            .bind(d.id)
            .bind(node)
            .bind(seq)
            .fetch_all(&mut *conn)
            .await?;
            for r in rows {
                let kind: String = r.get("kind");
                let purged: Option<i64> = r.get("purged_seq");
                let what = if kind == "dir" {
                    "mkdir".to_string()
                } else {
                    match purged {
                        Some(p) => format!("write {} bytes (content purged at #{p})", r.get::<i64, _>("size")),
                        None => format!("write {} bytes", r.get::<i64, _>("size")),
                    }
                };
                events.push(Event { drive_alias: d.alias.clone(), seq: r.get("seq"), at: r.get("committed_at"), actor: r.get("actor"), intent: r.get("intent"), what });
            }
            let rows = sqlx::query(
                "SELECT e.name, e.parent_node, e.from_seq, e.to_seq, c1.committed_at AS at1, c1.intent AS i1, p1.name AS a1, \
                 c2.committed_at AS at2, c2.intent AS i2, p2.name AS a2 FROM dentry e \
                 JOIN \"commit\" c1 ON c1.drive_id = e.drive_id AND c1.seq = e.from_seq JOIN principal p1 ON p1.id = c1.actor_id \
                 LEFT JOIN \"commit\" c2 ON c2.drive_id = e.drive_id AND c2.seq = e.to_seq LEFT JOIN principal p2 ON p2.id = c2.actor_id \
                 WHERE e.drive_id = ? AND e.node = ? AND e.from_seq <= ? ORDER BY e.from_seq",
            )
            .bind(d.id)
            .bind(node)
            .bind(seq)
            .fetch_all(&mut *conn)
            .await?;
            for r in rows {
                let name: String = r.get("name");
                events.push(Event { drive_alias: d.alias.clone(), seq: r.get("from_seq"), at: r.get("at1"), actor: r.get("a1"), intent: r.get("i1"), what: format!("link {name}") });
                let to: Option<i64> = r.get("to_seq");
                if let Some(to) = to.filter(|t| *t <= *seq) {
                    events.push(Event { drive_alias: d.alias.clone(), seq: to, at: r.get("at2"), actor: r.get("a2"), intent: r.get("i2"), what: format!("unlink {name}") });
                }
            }
        }
        events.sort_by_key(|e| (e.at, e.seq));
        Ok(events)
    }

    pub async fn log(&self, ctx: &Ctx, alias: &str, since: Option<i64>, by: Option<&str>) -> Result<Vec<CommitInfo>> {
        let mut conn = self.pool.acquire().await?;
        let drive = drive_by_alias(&mut conn, alias).await?;
        let chain = chain_at(&mut conn, &drive, drive.head_seq).await?;
        authorize(&mut conn, ctx, &drive, &chain, Some(ROOT_NODE), Action::Read).await?;
        let rows = sqlx::query(
            "SELECT c.seq, c.committed_at, c.intent, p.name AS actor, \
             (SELECT COUNT(*) FROM node_version v WHERE v.drive_id = c.drive_id AND v.from_seq = c.seq) + \
             (SELECT COUNT(*) FROM dentry e WHERE e.drive_id = c.drive_id AND (e.from_seq = c.seq OR e.to_seq = c.seq)) AS changes \
             FROM \"commit\" c JOIN principal p ON p.id = c.actor_id WHERE c.drive_id = ? AND c.committed_at >= ? AND (? IS NULL OR p.name = ?) ORDER BY c.seq",
        )
        .bind(drive.id)
        .bind(since.unwrap_or(0))
        .bind(by)
        .bind(by)
        .fetch_all(&mut *conn)
        .await?;
        Ok(rows
            .iter()
            .map(|r| CommitInfo { drive_id: drive.id, seq: r.get("seq"), committed_at: r.get("committed_at"), actor: r.get("actor"), intent: r.get("intent"), changes: r.get("changes") })
            .collect())
    }

    pub async fn restore(&self, ctx: &Ctx, alias: &str, path: &str, at: At) -> Result<Option<CommitInfo>> {
        let (ops, planned_head) = {
            let mut conn = self.pool.acquire().await?;
            let drive = drive_by_alias(&mut conn, alias).await?;
            let head = chain_at(&mut conn, &drive, drive.head_seq).await?;
            authorize(&mut conn, ctx, &drive, &head, None, Action::Write).await?;
            let (target_drive, target_seq) = resolve_at(&mut conn, &drive, at).await?;
            let target = chain_at(&mut conn, &target_drive, target_seq).await?;
            let mut ops = BTreeMap::new();
            diff_into(&mut conn, &target, &head, &normalize(path)?, &BTreeSet::new(), &mut ops).await?;
            (ops, drive.head_seq)
        };
        if ops.is_empty() {
            return Ok(None);
        }
        let intent = format!("restore {path} --at {at}");
        Ok(Some(self.commit_at(ctx, alias, &intent, order_ops(ops), Some(planned_head)).await?))
    }

    pub async fn undo(&self, ctx: &Ctx, alias: &str, by: &str, since_millis: i64) -> Result<UndoReport> {
        let mut conn = self.pool.acquire().await?;
        let drive = drive_by_alias(&mut conn, alias).await?;
        let actor_id: Option<i64> = sqlx::query("SELECT id FROM principal WHERE name = ?").bind(by).fetch_optional(&mut *conn).await?.map(|r| r.get("id"));
        let Some(actor_id) = actor_id else {
            return Ok(UndoReport { commit: None, restored: vec![], conflicts: vec![], since_seq: None });
        };
        let s0: Option<i64> = sqlx::query("SELECT MIN(seq) AS s FROM \"commit\" WHERE drive_id = ? AND actor_id = ? AND committed_at >= ?")
            .bind(drive.id)
            .bind(actor_id)
            .bind(since_millis)
            .fetch_one(&mut *conn)
            .await?
            .get("s");
        let Some(s0) = s0 else {
            return Ok(UndoReport { commit: None, restored: vec![], conflicts: vec![], since_seq: None });
        };
        let head = chain_at(&mut conn, &drive, drive.head_seq).await?;
        authorize(&mut conn, ctx, &drive, &head, None, Action::Write).await?;
        let planned_head = drive.head_seq;
        let before = chain_at(&mut conn, &drive, s0 - 1).await?;
        let mine = touched(&mut conn, &drive, s0, actor_id, true).await?;
        let theirs = touched(&mut conn, &drive, s0, actor_id, false).await?;
        let conflict_nodes: BTreeSet<i64> = mine.nodes.intersection(&theirs.nodes).copied().collect();
        let conflict_entries: BTreeSet<(i64, String)> = mine.entries.intersection(&theirs.entries).cloned().collect();
        let mut conflicts = BTreeSet::new();
        let mut skip = BTreeSet::new();
        for n in &conflict_nodes {
            for chain in [&before, &head] {
                if let Some(p) = path_of(&mut conn, chain, *n).await? {
                    conflicts.insert(p.clone());
                    skip.insert(p);
                }
            }
        }
        for (parent, name) in &conflict_entries {
            for chain in [&before, &head] {
                if let Some(p) = path_of(&mut conn, chain, *parent).await? {
                    let p = join(&p, name);
                    conflicts.insert(p.clone());
                    skip.insert(p);
                }
            }
        }
        let mut ops = BTreeMap::new();
        for (parent, name) in mine.entries.difference(&conflict_entries) {
            let want = lookup(&mut conn, &before, *parent, name).await?;
            let have = lookup(&mut conn, &head, *parent, name).await?;
            if want == have {
                continue;
            }
            let Some(parent_path) = path_of(&mut conn, &head, *parent).await?.or(path_of(&mut conn, &before, *parent).await?) else { continue };
            let path = join(&parent_path, name);
            if skip.contains(&path) {
                continue;
            }
            if let Some(h) = have {
                let hv = version_at(&mut conn, &head, h).await?.ok_or_else(|| anyhow::anyhow!("dangling node {h}"))?;
                if hv.kind == Kind::Dir {
                    let clean = plan_undo_delete(&mut conn, &head, &path, h, &mine, &theirs, &mut ops, &mut conflicts).await?;
                    if !clean {
                        continue;
                    }
                } else {
                    ops.insert(path.clone(), Op::Delete { path: path.clone() });
                }
            }
            if let Some(n) = want {
                let v = version_at(&mut conn, &before, n).await?.ok_or_else(|| anyhow::anyhow!("dangling node {n}"))?;
                put_tree(&mut conn, &before, &path, v, &skip, &mut ops).await?;
            }
        }
        for n in mine.nodes.difference(&conflict_nodes) {
            let Some(path) = path_of(&mut conn, &head, *n).await? else { continue };
            if skip.contains(&path) || ops.contains_key(&path) {
                continue;
            }
            let want = version_at(&mut conn, &before, *n).await?;
            let have = version_at(&mut conn, &head, *n).await?;
            match (want, have) {
                (Some(w), Some(h)) if w.kind == Kind::File && h.kind == Kind::File && w.manifest_hash != h.manifest_hash => {
                    ops.insert(path.clone(), Op::Put { path, manifest_hash: w.manifest_hash.unwrap(), size: w.size, mtime: w.mtime });
                }
                (None, Some(h)) if h.kind == Kind::Dir => {
                    plan_undo_delete(&mut conn, &head, &path, *n, &mine, &theirs, &mut ops, &mut conflicts).await?;
                }
                (None, Some(_)) => {
                    ops.insert(path.clone(), Op::Delete { path });
                }
                _ => {}
            }
        }
        drop(conn);
        let restored: Vec<String> = ops.keys().cloned().collect();
        let commit = if ops.is_empty() {
            None
        } else {
            let intent = format!("undo --by {by} --since-seq {s0}");
            Some(self.commit_at(ctx, alias, &intent, order_ops(ops), Some(planned_head)).await?)
        };
        Ok(UndoReport { commit, restored, conflicts: conflicts.into_iter().collect(), since_seq: Some(s0) })
    }

    pub async fn put_bytes(&self, data: Vec<u8>) -> Result<(Hash, u64)> {
        let (manifest, _) = self.put_reader(std::io::Cursor::new(data)).await?;
        let size = manifest.size();
        let hash = self.store_manifest(&manifest).await?;
        Ok((hash, size))
    }

    pub async fn put_stream<S, E>(&self, mut stream: S) -> Result<(Hash, u64)>
    where
        S: futures::Stream<Item = std::result::Result<bytes::Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        use futures::StreamExt;
        let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(8);
        let reader = ChannelReader { rx, cur: None, pos: 0 };
        let put = self.put_reader(reader);
        let feed = async {
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| anyhow::anyhow!("reading upload body: {e}"))?;
                if tx.send(chunk).await.is_err() {
                    break;
                }
            }
            drop(tx);
            Ok::<(), anyhow::Error>(())
        };
        let (put, fed) = tokio::join!(put, feed);
        fed?;
        let (manifest, _) = put?;
        let size = manifest.size();
        let hash = self.store_manifest(&manifest).await?;
        Ok((hash, size))
    }

    pub async fn put_reader<R: std::io::Read + Send + 'static>(&self, reader: R) -> Result<(Manifest, crate::store::PutStats)> {
        let parallel: usize = std::env::var("BLACKHOLE_SHARD_PARALLEL").ok().and_then(|v| v.parse().ok()).unwrap_or(8);
        let (tx, rx) = tokio::sync::mpsc::channel::<crate::format::Shard>(parallel * 2);
        let chunker = tokio::task::spawn_blocking(move || -> Result<Manifest> {
            crate::format::chunk(reader, |shard| tx.blocking_send(shard).map_err(|_| anyhow::anyhow!("shard consumer stopped")))
        });
        let store = self.store.clone();
        let pool = self.pool.clone();
        let stats = std::sync::Arc::new(std::sync::Mutex::new(crate::store::PutStats::default()));
        let uploads = {
            let stats = stats.clone();
            tokio_stream_for_each(rx, parallel, move |shard| {
                let store = store.clone();
                let pool = pool.clone();
                let stats = stats.clone();
                async move {
                    let mut st = crate::store::PutStats::default();
                    let known = sqlx::query("SELECT 1 FROM shard WHERE hash = ?").bind(shard.hash.0.to_vec()).fetch_optional(&pool).await?.is_some();
                    if known {
                        st.deduplicated += 1;
                        st.deduplicated_bytes += shard.data.len() as u64;
                    } else {
                        store.put(shard.hash, shard.data, &mut st).await?;
                    }
                    let mut g = stats.lock().unwrap();
                    g.uploaded += st.uploaded;
                    g.uploaded_bytes += st.uploaded_bytes;
                    g.deduplicated += st.deduplicated;
                    g.deduplicated_bytes += st.deduplicated_bytes;
                    Ok::<(), anyhow::Error>(())
                }
            })
        };
        let (manifest, uploaded) = tokio::join!(chunker, uploads);
        uploaded?;
        let manifest = manifest??;
        let st = *stats.lock().unwrap();
        Ok((manifest, st))
    }

    pub async fn diff(&self, ctx: &Ctx, alias: &str, at: At, base_alias: &str, base_at: At) -> Result<Vec<Change>> {
        let mut conn = self.pool.acquire().await?;
        let drive = drive_by_alias(&mut conn, alias).await?;
        let (d, seq) = resolve_at(&mut conn, &drive, at).await?;
        let target = chain_at(&mut conn, &d, seq).await?;
        let base_drive = drive_by_alias(&mut conn, base_alias).await?;
        let (bd, bseq) = resolve_at(&mut conn, &base_drive, base_at).await?;
        let base = chain_at(&mut conn, &bd, bseq).await?;
        authorize(&mut conn, ctx, &drive, &target, Some(ROOT_NODE), Action::Read).await?;
        authorize(&mut conn, ctx, &base_drive, &base, Some(ROOT_NODE), Action::Read).await?;
        let mut ops = BTreeMap::new();
        diff_into(&mut conn, &target, &base, "/", &BTreeSet::new(), &mut ops).await?;
        let mut out = Vec::with_capacity(ops.len());
        for (path, op) in ops {
            let existed = resolve_path(&mut conn, &base, &path).await?.is_some();
            let (kind, size) = match op {
                Op::Put { size, .. } | Op::PutIf { size, .. } => (if existed { "modified" } else { "added" }, size),
                Op::Mkdir { .. } => ("added", 0),
                Op::Delete { .. } => ("deleted", 0),
                Op::Rename { .. } => ("renamed", 0),
            };
            out.push(Change { path, kind: kind.to_string(), size });
        }
        Ok(out)
    }

    pub async fn fork_changes(&self, ctx: &Ctx, fork_alias: &str) -> Result<(Drive, Drive, Vec<Change>)> {
        let fork = self.drive(fork_alias).await?;
        let (parent_id, fork_seq) = match (fork.parent_id, fork.fork_seq) {
            (Some(p), Some(s)) => (p, s),
            _ => bail!("{fork_alias} is not a fork"),
        };
        let parent = {
            let mut conn = self.pool.acquire().await?;
            drive_by_id(&mut conn, parent_id).await?
        };
        let changes = self.diff(ctx, fork_alias, At::Head, &parent.alias, At::Seq(fork_seq)).await?;
        Ok((fork, parent, changes))
    }

    pub async fn merge(&self, ctx: &Ctx, fork_alias: &str) -> Result<MergeReport> {
        let (fork, parent, changes) = self.fork_changes(ctx, fork_alias).await?;
        let planned_head = parent.head_seq;
        let fork_seq = fork.fork_seq.unwrap_or(0);
        let parent_changes = self.diff(ctx, &parent.alias, At::Head, &parent.alias, At::Seq(fork_seq)).await?;
        let theirs: BTreeSet<String> = parent_changes.into_iter().map(|c| c.path).collect();
        let mut conflicts = Vec::new();
        let mut ops = BTreeMap::new();
        {
            let mut conn = self.pool.acquire().await?;
            let target = chain_at(&mut conn, &fork, fork.head_seq).await?;
            let base = chain_at(&mut conn, &parent, fork_seq).await?;
            let mut all = BTreeMap::new();
            diff_into(&mut conn, &target, &base, "/", &BTreeSet::new(), &mut all).await?;
            for (path, op) in all {
                let clash = theirs.iter().any(|t| t == &path || t.starts_with(&format!("{path}/")) || path.starts_with(&format!("{t}/")));
                if clash { conflicts.push(path) } else { ops.insert(path, op); }
            }
        }
        let applied: Vec<Change> = changes.into_iter().filter(|c| ops.contains_key(&c.path)).collect();
        let commit = if ops.is_empty() {
            None
        } else {
            let intent = format!("merge {fork_alias} (forked at #{fork_seq}, head #{})", fork.head_seq);
            Some(self.commit_at(ctx, &parent.alias, &intent, order_ops(ops), Some(planned_head)).await?)
        };
        Ok(MergeReport { commit, applied, conflicts })
    }

    pub async fn changes_since(&self, ctx: &Ctx, alias: &str, after_seq: i64, limit: i64) -> Result<Vec<ChangeSet>> {
        let mut conn = self.pool.acquire().await?;
        let drive = drive_by_alias(&mut conn, alias).await?;
        let head = chain_at(&mut conn, &drive, drive.head_seq).await?;
        authorize(&mut conn, ctx, &drive, &head, Some(ROOT_NODE), Action::Read).await?;
        let rows = sqlx::query("SELECT c.seq, c.committed_at, c.intent, p.name AS actor FROM \"commit\" c JOIN principal p ON p.id = c.actor_id WHERE c.drive_id = ? AND c.seq > ? ORDER BY c.seq LIMIT ?")
            .bind(drive.id)
            .bind(after_seq)
            .bind(limit)
            .fetch_all(&mut *conn)
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let seq: i64 = r.get("seq");
            let at = chain_at(&mut conn, &drive, seq).await?;
            let before = chain_at(&mut conn, &drive, seq - 1).await?;
            let mut paths: BTreeMap<String, &str> = BTreeMap::new();
            let opened = sqlx::query("SELECT node FROM node_version WHERE drive_id = ? AND from_seq = ?").bind(drive.id).bind(seq).fetch_all(&mut *conn).await?;
            for o in opened {
                if let Some(p) = path_of(&mut conn, &at, o.get::<i64, _>("node")).await? {
                    paths.insert(p, "write");
                }
            }
            let linked = sqlx::query("SELECT parent_node, name, node FROM dentry WHERE drive_id = ? AND from_seq = ?").bind(drive.id).bind(seq).fetch_all(&mut *conn).await?;
            for l in linked {
                let node: Option<i64> = l.get("node");
                if let Some(pp) = path_of(&mut conn, &at, l.get::<i64, _>("parent_node")).await? {
                    let p = join(&pp, &l.get::<String, _>("name"));
                    if node.is_none() {
                        paths.insert(p, "delete");
                    } else {
                        paths.entry(p).or_insert("link");
                    }
                }
            }
            let closed = sqlx::query("SELECT parent_node, name FROM dentry WHERE drive_id = ? AND to_seq = ?").bind(drive.id).bind(seq).fetch_all(&mut *conn).await?;
            for c in closed {
                if let Some(pp) = path_of(&mut conn, &before, c.get::<i64, _>("parent_node")).await? {
                    let p = join(&pp, &c.get::<String, _>("name"));
                    paths.entry(p).or_insert("unlink");
                }
            }
            out.push(ChangeSet {
                commit: CommitInfo { drive_id: drive.id, seq, committed_at: r.get("committed_at"), actor: r.get("actor"), intent: r.get("intent"), changes: paths.len() as i64 },
                paths: paths.into_iter().map(|(p, k)| (p, k.to_string())).collect(),
            });
        }
        Ok(out)
    }

    pub async fn head_seq(&self, alias: &str) -> Result<i64> {
        Ok(self.drive(alias).await?.head_seq)
    }

    pub async fn reload_pins(&self) -> Result<()> {
        if self.store.cache().is_none() {
            return Ok(());
        }
        let mut conn = self.pool.acquire().await?;
        let rows = sqlx::query("SELECT p.drive_id, p.node, v.manifest_hash FROM pin p JOIN node_version v ON v.drive_id = p.drive_id AND v.node = p.node AND v.to_seq IS NULL AND v.manifest_hash IS NOT NULL").fetch_all(&mut *conn).await?;
        let mut set = std::collections::HashSet::new();
        for r in rows {
            let h: Vec<u8> = r.get("manifest_hash");
            let m = self.manifest(Hash(h.try_into().expect("32-byte hash"))).await?;
            set.extend(m.shards.iter().map(|s| s.hash));
        }
        self.store.set_pinned(set);
        Ok(())
    }

    pub async fn pin(&self, ctx: &Ctx, alias: &str, path: &str, on: bool) -> Result<(i64, i64)> {
        let mut files: Vec<(String, Version)> = Vec::new();
        let drive = self.drive(alias).await?;
        collect_files(self, ctx, alias, &normalize(path)?, &mut files).await?;
        let mut shards = 0i64;
        for (_, v) in &files {
            if on {
                sqlx::query("INSERT OR IGNORE INTO pin(drive_id, node) VALUES (?, ?)").bind(drive.id).bind(v.node).execute(&self.pool).await?;
                if let Some(h) = v.manifest_hash {
                    let m = self.manifest(h).await?;
                    for sref in m.shards.iter() {
                        self.store.prefetch(sref.hash).await?;
                        shards += 1;
                    }
                }
            } else {
                sqlx::query("DELETE FROM pin WHERE drive_id = ? AND node = ?").bind(drive.id).bind(v.node).execute(&self.pool).await?;
            }
        }
        self.reload_pins().await?;
        Ok((files.len() as i64, shards))
    }

    pub async fn evict(&self, ctx: &Ctx, alias: &str, path: &str) -> Result<(i64, u64)> {
        let Some(c) = self.store.cache() else { bail!("no local cache: shards are on local disk, nothing to evict") };
        let mut files: Vec<(String, Version)> = Vec::new();
        collect_files(self, ctx, alias, &normalize(path)?, &mut files).await?;
        let mut freed = 0;
        for (_, v) in &files {
            if let Some(h) = v.manifest_hash {
                let m = self.manifest(h).await?;
                freed += c.evict(m.shards.iter().map(|s| s.hash));
            }
        }
        Ok((files.len() as i64, freed))
    }

    pub async fn dir_changed_at(&self, ctx: &Ctx, alias: &str, dir: i64, at: At) -> Result<Option<i64>> {
        let mut conn = self.pool.acquire().await?;
        let drive = drive_by_alias(&mut conn, alias).await?;
        let (d, seq) = resolve_at(&mut conn, &drive, at).await?;
        let chain = chain_at(&mut conn, &d, seq).await?;
        authorize(&mut conn, ctx, &drive, &chain, Some(dir), Action::Read).await?;
        let mut latest: Option<i64> = None;
        for (cd, cseq) in &chain {
            let row = sqlx::query(
                "SELECT MAX(c.committed_at) AS t FROM dentry e JOIN \"commit\" c ON c.drive_id = e.drive_id AND c.seq = CASE WHEN e.to_seq IS NOT NULL AND e.to_seq <= ? THEN e.to_seq ELSE e.from_seq END \
                 WHERE e.drive_id = ? AND e.parent_node = ? AND e.from_seq <= ?",
            )
            .bind(cseq)
            .bind(cd.id)
            .bind(dir)
            .bind(cseq)
            .fetch_one(&mut *conn)
            .await?;
            let t: Option<i64> = row.get("t");
            if let Some(t) = t {
                latest = Some(latest.map_or(t, |l| l.max(t)));
            }
        }
        Ok(latest)
    }

    pub async fn local_bytes(&self, v: &Version) -> u64 {
        let Some(c) = self.store.cache() else { return v.size };
        let Some(h) = v.manifest_hash else { return 0 };
        let now = now_millis();
        if let Some((used, at)) = self.used_memo.lock().unwrap().get(&h).copied()
            && now - at < 10_000
        {
            return used;
        }
        let used = match self.manifest(h).await {
            Ok(m) => c.cached_bytes(m.shards.iter().map(|s| s.hash)),
            Err(_) => 0,
        };
        self.used_memo.lock().unwrap().insert(h, (used, now));
        used
    }

    pub async fn node_path(&self, ctx: &Ctx, alias: &str, node: i64, at: At) -> Result<Option<String>> {
        let mut conn = self.pool.acquire().await?;
        let drive = drive_by_alias(&mut conn, alias).await?;
        let (d, seq) = resolve_at(&mut conn, &drive, at).await?;
        let chain = chain_at(&mut conn, &d, seq).await?;
        authorize(&mut conn, ctx, &drive, &chain, None, Action::Read).await?;
        path_of(&mut conn, &chain, node).await
    }

    pub async fn version_of(&self, ctx: &Ctx, alias: &str, node: i64, at: At) -> Result<Option<Version>> {
        let mut conn = self.pool.acquire().await?;
        let drive = drive_by_alias(&mut conn, alias).await?;
        let (d, seq) = resolve_at(&mut conn, &drive, at).await?;
        let chain = chain_at(&mut conn, &d, seq).await?;
        authorize(&mut conn, ctx, &drive, &chain, Some(node), Action::Read).await?;
        version_at(&mut conn, &chain, node).await
    }

    pub async fn lookup_child(&self, ctx: &Ctx, alias: &str, parent: i64, name: &str, at: At) -> Result<Option<(i64, Version)>> {
        let mut conn = self.pool.acquire().await?;
        let drive = drive_by_alias(&mut conn, alias).await?;
        let (d, seq) = resolve_at(&mut conn, &drive, at).await?;
        let chain = chain_at(&mut conn, &d, seq).await?;
        authorize(&mut conn, ctx, &drive, &chain, Some(parent), Action::Read).await?;
        let Some(node) = lookup(&mut conn, &chain, parent, name).await? else { return Ok(None) };
        Ok(version_at(&mut conn, &chain, node).await?.map(|v| (node, v)))
    }

    pub async fn readdir_node(&self, ctx: &Ctx, alias: &str, dir: i64, at: At) -> Result<Vec<Entry>> {
        let mut conn = self.pool.acquire().await?;
        let drive = drive_by_alias(&mut conn, alias).await?;
        let (d, seq) = resolve_at(&mut conn, &drive, at).await?;
        let chain = chain_at(&mut conn, &d, seq).await?;
        authorize(&mut conn, ctx, &drive, &chain, Some(dir), Action::Read).await?;
        let mut out = Vec::new();
        for (name, node) in readdir(&mut conn, &chain, dir).await? {
            if let Some(v) = version_at(&mut conn, &chain, node).await? {
                out.push(Entry { name, node, version: v });
            }
        }
        Ok(out)
    }

    pub async fn stats(&self) -> Result<Stats> {
        let r = sqlx::query(
            "SELECT (SELECT COUNT(*) FROM shard) AS shards, (SELECT COALESCE(SUM(size),0) FROM shard) AS shard_bytes, \
             (SELECT COUNT(*) FROM manifest) AS manifests, (SELECT COUNT(*) FROM node_version) AS versions, \
             (SELECT COUNT(*) FROM dentry) AS dentries, (SELECT COUNT(*) FROM \"commit\") AS commits, \
             (SELECT COALESCE(SUM(size),0) FROM node_version WHERE kind = 'file') AS logical_bytes",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(Stats {
            shards: r.get("shards"),
            shard_bytes: r.get("shard_bytes"),
            manifests: r.get("manifests"),
            versions: r.get("versions"),
            dentries: r.get("dentries"),
            commits: r.get("commits"),
            logical_bytes: r.get("logical_bytes"),
        })
    }
}

#[derive(Clone, Debug)]
pub struct Change {
    pub path: String,
    pub kind: String,
    pub size: u64,
}

#[derive(Clone, Debug)]
pub struct VerifyLine {
    pub seq: i64,
    pub hash: String,
    pub status: String,
}

#[derive(Clone, Debug)]
pub struct Attestation {
    pub drive: String,
    pub head_seq: i64,
    pub head_hash: String,
    pub at: i64,
    pub hold: bool,
}

#[derive(Clone, Debug)]
pub struct PurgeReport {
    pub seq: i64,
    pub versions_purged: i64,
    pub manifests_removed: i64,
    pub shards_removed: i64,
}

#[derive(Clone, Debug)]
pub struct GcReport {
    pub seq: Option<i64>,
    pub versions_expired: i64,
    pub manifests_removed: i64,
    pub shards_removed: i64,
}

#[derive(Clone, Debug)]
pub struct ChangeSet {
    pub commit: CommitInfo,
    pub paths: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
pub struct MergeReport {
    pub commit: Option<CommitInfo>,
    pub applied: Vec<Change>,
    pub conflicts: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct Stats {
    pub shards: i64,
    pub shard_bytes: i64,
    pub manifests: i64,
    pub versions: i64,
    pub dentries: i64,
    pub commits: i64,
    pub logical_bytes: i64,
}

#[derive(Clone, Copy, Debug)]
pub enum At {
    Head,
    Seq(i64),
    Time(i64),
}

impl std::fmt::Display for At {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            At::Head => write!(f, "head"),
            At::Seq(s) => write!(f, "#{s}"),
            At::Time(t) => write!(f, "{t}"),
        }
    }
}

async fn resolve_at(conn: &mut SqliteConnection, drive: &Drive, at: At) -> Result<(Drive, i64)> {
    match at {
        At::Head => Ok((drive.clone(), drive.head_seq)),
        At::Seq(s) => Ok((drive.clone(), s.clamp(0, drive.head_seq))),
        At::Time(t) => {
            if t < drive.created_at {
                if let Some(pid) = drive.parent_id {
                    let parent = drive_by_id(conn, pid).await?;
                    return Box::pin(resolve_at(conn, &parent, At::Time(t))).await;
                }
            }
            let seq: Option<i64> = sqlx::query("SELECT MAX(seq) AS s FROM \"commit\" WHERE drive_id = ? AND committed_at <= ?")
                .bind(drive.id)
                .bind(t)
                .fetch_one(&mut *conn)
                .await?
                .get("s");
            Ok((drive.clone(), seq.unwrap_or(0)))
        }
    }
}

fn commit_hash(drive_id: i64, seq: i64, committed_at: i64, actor_id: i64, intent: &str, prev: &[u8], digest: &[u8]) -> Hash {
    let mut buf = Vec::with_capacity(96 + intent.len());
    buf.extend_from_slice(b"BHC1");
    buf.extend_from_slice(&drive_id.to_le_bytes());
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(&committed_at.to_le_bytes());
    buf.extend_from_slice(&actor_id.to_le_bytes());
    buf.extend_from_slice(&(intent.len() as u64).to_le_bytes());
    buf.extend_from_slice(intent.as_bytes());
    buf.extend_from_slice(prev);
    buf.extend_from_slice(digest);
    Hash::of(&buf)
}

async fn row_digest(conn: &mut SqliteConnection, drive_id: i64, seq: i64) -> Result<Hash> {
    let mut buf = String::new();
    for r in sqlx::query("SELECT id, parent_node, name, node FROM dentry WHERE drive_id = ? AND from_seq = ? ORDER BY id").bind(drive_id).bind(seq).fetch_all(&mut *conn).await? {
        let node: Option<i64> = r.get("node");
        buf.push_str(&format!("d|{}|{}|{}|{}\n", r.get::<i64, _>("id"), r.get::<i64, _>("parent_node"), r.get::<String, _>("name"), node.map(|n| n.to_string()).unwrap_or_else(|| "-".into())));
    }
    for r in sqlx::query("SELECT id FROM dentry WHERE drive_id = ? AND to_seq = ? ORDER BY id").bind(drive_id).bind(seq).fetch_all(&mut *conn).await? {
        buf.push_str(&format!("dc|{}\n", r.get::<i64, _>("id")));
    }
    for r in sqlx::query("SELECT id, node, kind, size, mtime, manifest_hash FROM node_version WHERE drive_id = ? AND from_seq = ? ORDER BY id").bind(drive_id).bind(seq).fetch_all(&mut *conn).await? {
        let mh: Option<Vec<u8>> = r.get("manifest_hash");
        buf.push_str(&format!("v|{}|{}|{}|{}|{}|{}\n", r.get::<i64, _>("id"), r.get::<i64, _>("node"), r.get::<String, _>("kind"), r.get::<i64, _>("size"), r.get::<i64, _>("mtime"), mh.map(hex::encode).unwrap_or_else(|| "-".into())));
    }
    for r in sqlx::query("SELECT id FROM node_version WHERE drive_id = ? AND to_seq = ? ORDER BY id").bind(drive_id).bind(seq).fetch_all(&mut *conn).await? {
        buf.push_str(&format!("vc|{}\n", r.get::<i64, _>("id")));
    }
    Ok(Hash::of(buf.as_bytes()))
}

async fn genesis_prev(conn: &mut SqliteConnection, drive: &Drive) -> Result<Vec<u8>> {
    if let (Some(pid), Some(fseq)) = (drive.parent_id, drive.fork_seq)
        && fseq > 0
    {
        let row = sqlx::query("SELECT hash FROM \"commit\" WHERE drive_id = ? AND seq = ?").bind(pid).bind(fseq).fetch_optional(&mut *conn).await?;
        if let Some(r) = row {
            return Ok(r.get("hash"));
        }
    }
    Ok(vec![0u8; 32])
}

async fn seal_commit(conn: &mut SqliteConnection, drive: &Drive, seq: i64, actor_id: i64, intent: &str) -> Result<i64> {
    let prev: Vec<u8> = if seq > 1 {
        sqlx::query("SELECT hash FROM \"commit\" WHERE drive_id = ? AND seq = ?").bind(drive.id).bind(seq - 1).fetch_one(&mut *conn).await?.get("hash")
    } else {
        genesis_prev(conn, drive).await?
    };
    let digest = row_digest(conn, drive.id, seq).await?;
    let now = now_millis();
    let hash = commit_hash(drive.id, seq, now, actor_id, intent, &prev, &digest.0);
    sqlx::query("INSERT INTO \"commit\"(drive_id, seq, committed_at, actor_id, intent, row_digest, prev_hash, hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(drive.id)
        .bind(seq)
        .bind(now)
        .bind(actor_id)
        .bind(intent)
        .bind(digest.0.to_vec())
        .bind(prev)
        .bind(hash.0.to_vec())
        .execute(&mut *conn)
        .await?;
    Ok(now)
}

struct ChannelReader {
    rx: tokio::sync::mpsc::Receiver<bytes::Bytes>,
    cur: Option<bytes::Bytes>,
    pos: usize,
}

impl std::io::Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if let Some(cur) = &self.cur {
                if self.pos < cur.len() {
                    let n = (cur.len() - self.pos).min(buf.len());
                    buf[..n].copy_from_slice(&cur[self.pos..self.pos + n]);
                    self.pos += n;
                    return Ok(n);
                }
                self.cur = None;
            }
            match self.rx.blocking_recv() {
                Some(b) => {
                    self.cur = Some(b);
                    self.pos = 0;
                }
                None => return Ok(0),
            }
        }
    }
}

async fn tokio_stream_for_each<T, F, Fut>(mut rx: tokio::sync::mpsc::Receiver<T>, parallel: usize, f: F) -> Result<()>
where
    T: Send + 'static,
    F: Fn(T) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    use futures::stream::{FuturesUnordered, StreamExt};
    let mut inflight = FuturesUnordered::new();
    loop {
        tokio::select! {
            item = rx.recv(), if inflight.len() < parallel => match item {
                Some(shard) => inflight.push(f(shard)),
                None => break,
            },
            done = inflight.next(), if !inflight.is_empty() => {
                if let Some(r) = done { r?; }
            }
        }
    }
    while let Some(r) = inflight.next().await {
        r?;
    }
    Ok(())
}

async fn collect_files(auth: &Authority, ctx: &Ctx, alias: &str, path: &str, out: &mut Vec<(String, Version)>) -> Result<()> {
    let Some((_, v)) = auth.stat(ctx, alias, path, At::Head).await? else {
        return Err(BhError::NotFound(path.to_string()).into());
    };
    if v.kind == Kind::File {
        out.push((path.to_string(), v));
        return Ok(());
    }
    for e in auth.list(ctx, alias, path, At::Head).await? {
        let child = join(path, &e.name);
        Box::pin(collect_files(auth, ctx, alias, &child, out)).await?;
    }
    Ok(())
}

async fn ensure_principal(conn: &mut SqliteConnection, name: &str, kind: &str) -> Result<i64> {
    sqlx::query("INSERT OR IGNORE INTO principal(kind, name) VALUES (?, ?)").bind(kind).bind(name).execute(&mut *conn).await?;
    Ok(sqlx::query("SELECT id FROM principal WHERE name = ?").bind(name).fetch_one(&mut *conn).await?.get("id"))
}

async fn insert_key(conn: &mut SqliteConnection, principal_id: i64, drive_id: i64, prefix_node: Option<i64>, read_only: bool, admin: bool, expires_at: Option<i64>) -> Result<Key> {
    let id = format!("BHK{}", &random_id()?[..16].to_uppercase());
    let secret = format!("{}{}", random_id()?, random_id()?[..8].to_string());
    sqlx::query("INSERT INTO access_key(id, secret, principal_id, drive_id, prefix_node, read_only, admin, expires_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(&id)
        .bind(&secret)
        .bind(principal_id)
        .bind(drive_id)
        .bind(prefix_node)
        .bind(read_only)
        .bind(admin)
        .bind(expires_at)
        .bind(now_millis())
        .execute(&mut *conn)
        .await?;
    Ok(Key { id, secret, principal_id, drive_id, prefix_node, read_only, admin, expires_at })
}

fn key_from_row(r: &SqliteRow) -> Key {
    Key {
        id: r.get("id"),
        secret: r.get("secret"),
        principal_id: r.get("principal_id"),
        drive_id: r.get("drive_id"),
        prefix_node: r.get("prefix_node"),
        read_only: r.get("read_only"),
        admin: r.get("admin"),
        expires_at: r.get("expires_at"),
    }
}

async fn drive_by_alias(conn: &mut SqliteConnection, alias: &str) -> Result<Drive> {
    let row = sqlx::query("SELECT * FROM drive WHERE alias = ?").bind(alias).fetch_optional(&mut *conn).await?.ok_or_else(|| BhError::NotFound(format!("drive {alias}")))?;
    Ok(drive_from_row(&row))
}

async fn drive_by_id(conn: &mut SqliteConnection, id: i64) -> Result<Drive> {
    let row = sqlx::query("SELECT * FROM drive WHERE id = ?").bind(id).fetch_optional(&mut *conn).await?.ok_or_else(|| BhError::NotFound(format!("drive id {id}")))?;
    Ok(drive_from_row(&row))
}

async fn chain_at(conn: &mut SqliteConnection, drive: &Drive, seq: i64) -> Result<Chain> {
    let mut chain = vec![(drive.clone(), seq)];
    let mut cur = drive.clone();
    while let (Some(pid), Some(fseq)) = (cur.parent_id, cur.fork_seq) {
        let parent = drive_by_id(conn, pid).await?;
        chain.push((parent.clone(), fseq));
        cur = parent;
        if chain.len() as i64 > MAX_CHAIN_DEPTH + 1 {
            bail!("fork chain too deep");
        }
    }
    Ok(chain)
}

async fn authorize(conn: &mut SqliteConnection, ctx: &Ctx, drive: &Drive, chain: &Chain, node: Option<i64>, action: Action) -> Result<()> {
    let k = &ctx.key;
    if k.drive_id != drive.id {
        let owns_ancestor = k.admin && chain.iter().any(|(d, _)| d.id == k.drive_id);
        if !owns_ancestor {
            return Err(BhError::Denied(format!("key is not for drive {}", drive.alias)).into());
        }
    }
    if let Some(exp) = k.expires_at
        && now_millis() > exp
    {
        return Err(BhError::Denied("key expired".into()).into());
    }
    if k.read_only && action != Action::Read {
        return Err(BhError::Denied("key is read-only".into()).into());
    }
    if action == Action::Admin && !k.admin {
        return Err(BhError::Denied("key is not admin".into()).into());
    }
    if let (Some(prefix), Some(node)) = (k.prefix_node, node)
        && !is_under(conn, chain, node, prefix).await?
    {
        return Err(BhError::Denied("outside the key's prefix".into()).into());
    }
    Ok(())
}

async fn is_under(conn: &mut SqliteConnection, chain: &Chain, mut node: i64, prefix: i64) -> Result<bool> {
    for _ in 0..4096 {
        if node == prefix {
            return Ok(true);
        }
        if node == ROOT_NODE {
            return Ok(false);
        }
        match parent_of(conn, chain, node).await? {
            Some((p, _)) => node = p,
            None => return Ok(false),
        }
    }
    bail!("directory cycle")
}

async fn parent_of(conn: &mut SqliteConnection, chain: &Chain, node: i64) -> Result<Option<(i64, String)>> {
    for (d, seq) in chain {
        let rows = sqlx::query("SELECT parent_node, name FROM dentry WHERE drive_id = ? AND node = ? AND from_seq <= ? AND (to_seq IS NULL OR to_seq > ?)")
            .bind(d.id)
            .bind(node)
            .bind(seq)
            .bind(seq)
            .fetch_all(&mut *conn)
            .await?;
        for r in rows {
            let parent: i64 = r.get("parent_node");
            let name: String = r.get("name");
            if lookup(conn, chain, parent, &name).await? == Some(node) {
                return Ok(Some((parent, name)));
            }
        }
    }
    Ok(None)
}

async fn path_of(conn: &mut SqliteConnection, chain: &Chain, mut node: i64) -> Result<Option<String>> {
    let mut parts = Vec::new();
    while node != ROOT_NODE {
        match parent_of(conn, chain, node).await? {
            Some((p, name)) => {
                parts.push(name);
                node = p;
            }
            None => return Ok(None),
        }
    }
    parts.reverse();
    Ok(Some(format!("/{}", parts.join("/"))))
}

async fn lookup_in(conn: &mut SqliteConnection, drive_id: i64, seq: i64, parent: i64, name: &str) -> Result<Option<Option<i64>>> {
    let row = sqlx::query("SELECT node FROM dentry WHERE drive_id = ? AND parent_node = ? AND name = ? AND from_seq <= ? AND (to_seq IS NULL OR to_seq > ?) ORDER BY from_seq DESC LIMIT 1")
        .bind(drive_id)
        .bind(parent)
        .bind(name)
        .bind(seq)
        .bind(seq)
        .fetch_optional(&mut *conn)
        .await?;
    Ok(row.map(|r| r.get::<Option<i64>, _>("node")))
}

async fn lookup(conn: &mut SqliteConnection, chain: &Chain, parent: i64, name: &str) -> Result<Option<i64>> {
    for (d, seq) in chain {
        if let Some(hit) = lookup_in(conn, d.id, *seq, parent, name).await? {
            return Ok(hit);
        }
    }
    Ok(None)
}

async fn version_in(conn: &mut SqliteConnection, drive_id: i64, seq: i64, node: i64) -> Result<Option<Version>> {
    let row = sqlx::query("SELECT * FROM node_version WHERE drive_id = ? AND node = ? AND from_seq <= ? AND (to_seq IS NULL OR to_seq > ?) ORDER BY from_seq DESC LIMIT 1")
        .bind(drive_id)
        .bind(node)
        .bind(seq)
        .bind(seq)
        .fetch_optional(&mut *conn)
        .await?;
    Ok(row.as_ref().map(version_from_row))
}

async fn version_at(conn: &mut SqliteConnection, chain: &Chain, node: i64) -> Result<Option<Version>> {
    if node == ROOT_NODE {
        return Ok(Some(root_version(chain[0].0.id)));
    }
    for (d, seq) in chain {
        if let Some(v) = version_in(conn, d.id, *seq, node).await? {
            return Ok(Some(v));
        }
    }
    Ok(None)
}

async fn resolve_path(conn: &mut SqliteConnection, chain: &Chain, path: &str) -> Result<Option<(i64, Version)>> {
    let mut node = ROOT_NODE;
    for c in components(path)? {
        match lookup(conn, chain, node, &c).await? {
            Some(n) => node = n,
            None => return Ok(None),
        }
    }
    Ok(version_at(conn, chain, node).await?.map(|v| (node, v)))
}

async fn deepest_existing(conn: &mut SqliteConnection, chain: &Chain, path: &str) -> Result<i64> {
    let mut node = ROOT_NODE;
    for c in components(path)? {
        match lookup(conn, chain, node, &c).await? {
            Some(n) => node = n,
            None => break,
        }
    }
    Ok(node)
}

async fn find_node_ever(conn: &mut SqliteConnection, chain: &Chain, path: &str) -> Result<Option<i64>> {
    let (parent_path, name) = split_parent(path)?;
    let Some((parent, _)) = resolve_path(conn, chain, &parent_path).await? else { return Ok(None) };
    prior_node(conn, chain, parent, &name).await
}

async fn readdir(conn: &mut SqliteConnection, chain: &Chain, dir: i64) -> Result<Vec<(String, i64)>> {
    let mut merged: BTreeMap<String, Option<i64>> = BTreeMap::new();
    for (d, seq) in chain.iter().rev() {
        let rows = sqlx::query("SELECT name, node FROM dentry WHERE drive_id = ? AND parent_node = ? AND from_seq <= ? AND (to_seq IS NULL OR to_seq > ?)")
            .bind(d.id)
            .bind(dir)
            .bind(seq)
            .bind(seq)
            .fetch_all(&mut *conn)
            .await?;
        for r in rows {
            merged.insert(r.get("name"), r.get("node"));
        }
    }
    Ok(merged.into_iter().filter_map(|(n, node)| node.map(|x| (n, x))).collect())
}

async fn close_dentry(conn: &mut SqliteConnection, drive_id: i64, seq: i64, parent: i64, name: &str) -> Result<bool> {
    let res = sqlx::query("UPDATE dentry SET to_seq = ? WHERE drive_id = ? AND parent_node = ? AND name = ? AND to_seq IS NULL AND from_seq < ?")
        .bind(seq)
        .bind(drive_id)
        .bind(parent)
        .bind(name)
        .bind(seq)
        .execute(&mut *conn)
        .await?;
    if res.rows_affected() == 0 {
        let res = sqlx::query("DELETE FROM dentry WHERE drive_id = ? AND parent_node = ? AND name = ? AND to_seq IS NULL AND from_seq = ?")
            .bind(drive_id)
            .bind(parent)
            .bind(name)
            .bind(seq)
            .execute(&mut *conn)
            .await?;
        return Ok(res.rows_affected() > 0);
    }
    Ok(true)
}

async fn open_dentry(conn: &mut SqliteConnection, drive_id: i64, seq: i64, parent: i64, name: &str, node: Option<i64>) -> Result<()> {
    sqlx::query("INSERT INTO dentry(drive_id, parent_node, name, node, from_seq) VALUES (?, ?, ?, ?, ?)")
        .bind(drive_id)
        .bind(parent)
        .bind(name)
        .bind(node)
        .bind(seq)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

async fn close_version(conn: &mut SqliteConnection, drive_id: i64, seq: i64, node: i64) -> Result<()> {
    sqlx::query("UPDATE node_version SET to_seq = ? WHERE drive_id = ? AND node = ? AND to_seq IS NULL AND from_seq < ?")
        .bind(seq)
        .bind(drive_id)
        .bind(node)
        .bind(seq)
        .execute(&mut *conn)
        .await?;
    sqlx::query("DELETE FROM node_version WHERE drive_id = ? AND node = ? AND to_seq IS NULL AND from_seq = ?")
        .bind(drive_id)
        .bind(node)
        .bind(seq)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

async fn open_version(conn: &mut SqliteConnection, drive_id: i64, seq: i64, node: i64, kind: Kind, size: u64, mtime: i64, manifest_hash: Option<Hash>) -> Result<()> {
    sqlx::query("INSERT INTO node_version(drive_id, node, kind, size, mtime, manifest_hash, from_seq) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .bind(drive_id)
        .bind(node)
        .bind(kind.as_str())
        .bind(size as i64)
        .bind(mtime)
        .bind(manifest_hash.map(|h| h.0.to_vec()))
        .bind(seq)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

async fn visible_in_ancestors(conn: &mut SqliteConnection, chain: &Chain, parent: i64, name: &str) -> Result<bool> {
    if chain.len() < 2 {
        return Ok(false);
    }
    Ok(lookup(conn, &chain[1..].to_vec(), parent, name).await?.is_some())
}

async fn unlink(conn: &mut SqliteConnection, drive: &Drive, chain: &Chain, seq: i64, parent: i64, name: &str) -> Result<()> {
    close_dentry(conn, drive.id, seq, parent, name).await?;
    if visible_in_ancestors(conn, chain, parent, name).await? {
        open_dentry(conn, drive.id, seq, parent, name, None).await?;
    }
    Ok(())
}

async fn link(conn: &mut SqliteConnection, drive: &Drive, seq: i64, parent: i64, name: &str, node: i64) -> Result<()> {
    close_dentry(conn, drive.id, seq, parent, name).await?;
    open_dentry(conn, drive.id, seq, parent, name, Some(node)).await
}

async fn ensure_dirs(conn: &mut SqliteConnection, drive: &mut Drive, chain: &Chain, seq: i64, parts: &[String]) -> Result<i64> {
    let mut node = ROOT_NODE;
    for c in parts {
        match lookup(conn, chain, node, c).await? {
            Some(n) => {
                let v = version_at(conn, chain, n).await?.ok_or_else(|| anyhow::anyhow!("dangling node {n}"))?;
                if v.kind != Kind::Dir {
                    return Err(BhError::Conflict(format!("{c} is a file, not a directory")).into());
                }
                node = n;
            }
            None => {
                let n = match prior_node(conn, chain, node, c).await? {
                    Some(old) if version_ever(conn, chain, old).await?.map(|v| v.kind) == Some(Kind::Dir) && parent_of(conn, chain, old).await?.is_none() => old,
                    _ => {
                        let n = drive.next_node;
                        drive.next_node += 1;
                        n
                    }
                };
                link(conn, drive, seq, node, c, n).await?;
                close_version(conn, drive.id, seq, n).await?;
                open_version(conn, drive.id, seq, n, Kind::Dir, 0, now_millis(), None).await?;
                node = n;
            }
        }
    }
    Ok(node)
}

async fn prior_node(conn: &mut SqliteConnection, chain: &Chain, parent: i64, name: &str) -> Result<Option<i64>> {
    for (d, seq) in chain {
        let row = sqlx::query("SELECT node FROM dentry WHERE drive_id = ? AND parent_node = ? AND name = ? AND node IS NOT NULL AND from_seq <= ? ORDER BY from_seq DESC LIMIT 1")
            .bind(d.id)
            .bind(parent)
            .bind(name)
            .bind(seq)
            .fetch_optional(&mut *conn)
            .await?;
        if let Some(r) = row {
            return Ok(Some(r.get("node")));
        }
    }
    Ok(None)
}

async fn version_ever(conn: &mut SqliteConnection, chain: &Chain, node: i64) -> Result<Option<Version>> {
    for (d, seq) in chain {
        let row = sqlx::query("SELECT * FROM node_version WHERE drive_id = ? AND node = ? AND from_seq <= ? ORDER BY from_seq DESC LIMIT 1")
            .bind(d.id)
            .bind(node)
            .bind(seq)
            .fetch_optional(&mut *conn)
            .await?;
        if let Some(r) = row {
            return Ok(Some(version_from_row(&r)));
        }
    }
    Ok(None)
}

async fn delete_tree(conn: &mut SqliteConnection, drive: &Drive, chain: &Chain, seq: i64, parent: i64, name: &str, node: i64) -> Result<i64> {
    let v = version_at(conn, chain, node).await?;
    let mut n = 1;
    if let Some(v) = &v
        && v.kind == Kind::Dir
    {
        for (child_name, child) in readdir(conn, chain, node).await? {
            n += Box::pin(delete_tree(conn, drive, chain, seq, node, &child_name, child)).await?;
        }
    }
    unlink(conn, drive, chain, seq, parent, name).await?;
    close_version(conn, drive.id, seq, node).await?;
    Ok(n)
}

async fn apply_op(conn: &mut SqliteConnection, drive: &mut Drive, chain: &Chain, seq: i64, op: Op) -> Result<i64> {
    match op {
        Op::Mkdir { path } => {
            let parts = components(&path)?;
            ensure_dirs(conn, drive, chain, seq, &parts).await?;
            Ok(1)
        }
        Op::PutIf { path, manifest_hash, size, mtime, expect_seq } => {
            let current = resolve_path(conn, chain, &path).await?.map(|(_, v)| v.from_seq).unwrap_or(0);
            if current != expect_seq {
                return Err(BhError::Conflict(format!("{path} is at version #{current}, expected #{expect_seq}")).into());
            }
            Box::pin(apply_op(conn, drive, chain, seq, Op::Put { path, manifest_hash, size, mtime })).await
        }
        Op::Put { path, manifest_hash, size, mtime } => {
            let (parent_path, name) = split_parent(&path)?;
            let parent = ensure_dirs(conn, drive, chain, seq, &components(&parent_path)?).await?;
            match lookup(conn, chain, parent, &name).await? {
                Some(existing) => {
                    let v = version_at(conn, chain, existing).await?.ok_or_else(|| anyhow::anyhow!("dangling node {existing}"))?;
                    if v.kind == Kind::Dir {
                        return Err(BhError::Conflict(format!("{path} is a directory")).into());
                    }
                    if lookup_in(conn, drive.id, seq, parent, &name).await?.is_none() {
                        open_dentry(conn, drive.id, seq, parent, &name, Some(existing)).await?;
                    }
                    close_version(conn, drive.id, seq, existing).await?;
                    open_version(conn, drive.id, seq, existing, Kind::File, size, mtime, Some(manifest_hash)).await?;
                }
                None => {
                    let node = match find_node_ever(conn, chain, &path).await? {
                        Some(n) if parent_of(conn, chain, n).await?.is_none() => n,
                        _ => {
                            let n = drive.next_node;
                            drive.next_node += 1;
                            n
                        }
                    };
                    link(conn, drive, seq, parent, &name, node).await?;
                    close_version(conn, drive.id, seq, node).await?;
                    open_version(conn, drive.id, seq, node, Kind::File, size, mtime, Some(manifest_hash)).await?;
                }
            }
            Ok(1)
        }
        Op::Delete { path } => {
            let (parent_path, name) = split_parent(&path)?;
            let Some((parent, _)) = resolve_path(conn, chain, &parent_path).await? else {
                return Err(BhError::NotFound(path).into());
            };
            let Some(node) = lookup(conn, chain, parent, &name).await? else {
                return Err(BhError::NotFound(path).into());
            };
            delete_tree(conn, drive, chain, seq, parent, &name, node).await
        }
        Op::Rename { from, to } => {
            let (from_parent_path, from_name) = split_parent(&from)?;
            let Some((from_parent, _)) = resolve_path(conn, chain, &from_parent_path).await? else {
                return Err(BhError::NotFound(from).into());
            };
            let Some(node) = lookup(conn, chain, from_parent, &from_name).await? else {
                return Err(BhError::NotFound(from).into());
            };
            let (to_parent_path, to_name) = split_parent(&to)?;
            let to_parent = ensure_dirs(conn, drive, chain, seq, &components(&to_parent_path)?).await?;
            if is_under(conn, chain, to_parent, node).await? {
                return Err(BhError::Conflict(format!("cannot move {from} into itself")).into());
            }
            if let Some(target) = lookup(conn, chain, to_parent, &to_name).await? {
                if target == node {
                    return Ok(0);
                }
                let tv = version_at(conn, chain, target).await?.ok_or_else(|| anyhow::anyhow!("dangling node {target}"))?;
                let sv = version_at(conn, chain, node).await?.ok_or_else(|| anyhow::anyhow!("dangling node {node}"))?;
                if tv.kind != Kind::File || sv.kind != Kind::File {
                    return Err(BhError::Conflict(format!("{to} exists")).into());
                }
                if lookup_in(conn, drive.id, seq, to_parent, &to_name).await?.is_none() {
                    open_dentry(conn, drive.id, seq, to_parent, &to_name, Some(target)).await?;
                }
                close_version(conn, drive.id, seq, target).await?;
                open_version(conn, drive.id, seq, target, Kind::File, sv.size, sv.mtime, sv.manifest_hash).await?;
                unlink(conn, drive, chain, seq, from_parent, &from_name).await?;
                close_version(conn, drive.id, seq, node).await?;
                return Ok(1);
            }
            unlink(conn, drive, chain, seq, from_parent, &from_name).await?;
            link(conn, drive, seq, to_parent, &to_name, node).await?;
            Ok(1)
        }
    }
}

async fn diff_into(conn: &mut SqliteConnection, target: &Chain, head: &Chain, path: &str, skip: &BTreeSet<String>, ops: &mut BTreeMap<String, Op>) -> Result<()> {
    if skip.contains(path) {
        return Ok(());
    }
    let want = resolve_path(conn, target, path).await?;
    let have = resolve_path(conn, head, path).await?;
    match (want, have) {
        (None, None) => {}
        (None, Some(_)) => {
            ops.insert(path.to_string(), Op::Delete { path: path.to_string() });
        }
        (Some((_, w)), None) => put_tree(conn, target, path, w, skip, ops).await?,
        (Some((wn, w)), Some((hn, h))) => match (w.kind, h.kind) {
            (Kind::File, Kind::File) => {
                if w.manifest_hash != h.manifest_hash {
                    ops.insert(path.to_string(), Op::Put { path: path.to_string(), manifest_hash: w.manifest_hash.unwrap(), size: w.size, mtime: w.mtime });
                }
            }
            (Kind::Dir, Kind::Dir) => {
                let mut names: BTreeSet<String> = readdir(conn, target, wn).await?.into_iter().map(|(n, _)| n).collect();
                names.extend(readdir(conn, head, hn).await?.into_iter().map(|(n, _)| n));
                for n in names {
                    let child = if path == "/" { format!("/{n}") } else { format!("{path}/{n}") };
                    Box::pin(diff_into(conn, target, head, &child, skip, ops)).await?;
                }
            }
            _ => {
                ops.insert(path.to_string(), Op::Delete { path: path.to_string() });
                put_tree(conn, target, path, w, skip, ops).await?;
            }
        },
    }
    Ok(())
}

async fn put_tree(conn: &mut SqliteConnection, target: &Chain, path: &str, v: Version, skip: &BTreeSet<String>, ops: &mut BTreeMap<String, Op>) -> Result<()> {
    if skip.contains(path) {
        return Ok(());
    }
    match v.kind {
        Kind::File => {
            ops.insert(path.to_string(), Op::Put { path: path.to_string(), manifest_hash: v.manifest_hash.unwrap(), size: v.size, mtime: v.mtime });
        }
        Kind::Dir => {
            ops.insert(path.to_string(), Op::Mkdir { path: path.to_string() });
            for (n, child) in readdir(conn, target, v.node).await? {
                let cv = version_at(conn, target, child).await?.ok_or_else(|| anyhow::anyhow!("dangling node {child}"))?;
                let child_path = if path == "/" { format!("/{n}") } else { format!("{path}/{n}") };
                Box::pin(put_tree(conn, target, &child_path, cv, skip, ops)).await?;
            }
        }
    }
    Ok(())
}

fn order_ops(ops: BTreeMap<String, Op>) -> Vec<Op> {
    let mut deletes: Vec<Op> = Vec::new();
    let mut rest: Vec<Op> = Vec::new();
    for (_, op) in ops {
        if matches!(op, Op::Delete { .. }) { deletes.push(op) } else { rest.push(op) }
    }
    deletes.sort_by_key(|o| std::cmp::Reverse(o.path().len()));
    let delete_paths: BTreeSet<String> = deletes.iter().map(|o| o.path().to_string()).collect();
    deletes.retain(|o| {
        let p = o.path();
        !delete_paths.iter().any(|d| d != p && p.starts_with(&format!("{d}/")))
    });
    deletes.extend(rest);
    deletes
}

async fn plan_undo_delete(
    conn: &mut SqliteConnection,
    head: &Chain,
    path: &str,
    node: i64,
    mine: &Touched,
    theirs: &Touched,
    ops: &mut BTreeMap<String, Op>,
    conflicts: &mut BTreeSet<String>,
) -> Result<bool> {
    let mut clean = true;
    for (name, child) in readdir(conn, head, node).await? {
        let child_path = join(path, &name);
        let owned = mine.nodes.contains(&child) && !theirs.nodes.contains(&child);
        if !owned {
            conflicts.insert(child_path);
            clean = false;
            continue;
        }
        let v = version_at(conn, head, child).await?.ok_or_else(|| anyhow::anyhow!("dangling node {child}"))?;
        if v.kind == Kind::Dir {
            if !Box::pin(plan_undo_delete(conn, head, &child_path, child, mine, theirs, ops, conflicts)).await? {
                clean = false;
            }
        } else {
            ops.insert(child_path.clone(), Op::Delete { path: child_path });
        }
    }
    if clean {
        ops.insert(path.to_string(), Op::Delete { path: path.to_string() });
    }
    Ok(clean)
}

struct Touched {
    nodes: BTreeSet<i64>,
    entries: BTreeSet<(i64, String)>,
}

fn join(parent: &str, name: &str) -> String {
    if parent == "/" { format!("/{name}") } else { format!("{parent}/{name}") }
}

async fn touched(conn: &mut SqliteConnection, drive: &Drive, s0: i64, actor_id: i64, by_actor: bool) -> Result<Touched> {
    let sql_dentry = if by_actor {
        "SELECT DISTINCT e.parent_node, e.name FROM dentry e JOIN \"commit\" c ON c.drive_id = e.drive_id AND (c.seq = e.from_seq OR c.seq = e.to_seq) \
         WHERE e.drive_id = ? AND c.seq >= ? AND c.actor_id = ?"
    } else {
        "SELECT DISTINCT e.parent_node, e.name FROM dentry e JOIN \"commit\" c ON c.drive_id = e.drive_id AND (c.seq = e.from_seq OR c.seq = e.to_seq) \
         WHERE e.drive_id = ? AND c.seq >= ? AND c.actor_id <> ?"
    };
    let sql_version = if by_actor {
        "SELECT DISTINCT v.node FROM node_version v JOIN \"commit\" c ON c.drive_id = v.drive_id AND (c.seq = v.from_seq OR c.seq = v.to_seq) \
         WHERE v.drive_id = ? AND c.seq >= ? AND c.actor_id = ?"
    } else {
        "SELECT DISTINCT v.node FROM node_version v JOIN \"commit\" c ON c.drive_id = v.drive_id AND (c.seq = v.from_seq OR c.seq = v.to_seq) \
         WHERE v.drive_id = ? AND c.seq >= ? AND c.actor_id <> ?"
    };
    let entries = sqlx::query(sql_dentry)
        .bind(drive.id)
        .bind(s0)
        .bind(actor_id)
        .fetch_all(&mut *conn)
        .await?
        .iter()
        .map(|r| (r.get::<i64, _>("parent_node"), r.get::<String, _>("name")))
        .collect();
    let nodes = sqlx::query(sql_version).bind(drive.id).bind(s0).bind(actor_id).fetch_all(&mut *conn).await?.iter().map(|r| r.get::<i64, _>("node")).collect();
    Ok(Touched { nodes, entries })
}
