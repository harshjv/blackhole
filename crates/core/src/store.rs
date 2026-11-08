use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::{ClientOptions, RetryConfig};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::format::Hash;

#[derive(Clone)]
pub struct ShardStore {
    inner: Arc<dyn ObjectStore>,
    cache: Option<Arc<Cache>>,
    fetches: Arc<std::sync::atomic::AtomicU64>,
    inflight: Arc<std::sync::Mutex<std::collections::HashMap<Hash, Arc<tokio::sync::Mutex<()>>>>>,
    uploads: Arc<Uploads>,
}

pub struct Uploads {
    pool: std::sync::OnceLock<sqlx::SqlitePool>,
    pending: std::sync::Mutex<std::collections::HashSet<Hash>>,
    failed: std::sync::Mutex<std::collections::HashSet<Hash>>,
    sem: Arc<tokio::sync::Semaphore>,
    done: tokio::sync::Notify,
}

impl Uploads {
    fn new() -> Uploads {
        Uploads { pool: std::sync::OnceLock::new(), pending: Default::default(), failed: Default::default(), sem: Arc::new(tokio::sync::Semaphore::new(8)), done: tokio::sync::Notify::new() }
    }

    pub fn attach(&self, pool: sqlx::SqlitePool) {
        let _ = self.pool.set(pool);
    }

    async fn record(&self, hash: Hash) {
        if let Some(p) = self.pool.get() {
            let _ = sqlx::query("INSERT OR IGNORE INTO pending_upload(hash, since) VALUES (?, ?)").bind(hash.0.to_vec()).bind(crate::authority::now_millis()).execute(p).await;
        }
    }

    async fn settle(&self, hash: Hash) {
        if let Some(p) = self.pool.get() {
            let _ = sqlx::query("DELETE FROM pending_upload WHERE hash = ?").bind(hash.0.to_vec()).execute(p).await;
        }
    }

    pub async fn is_pending(&self, hash: Hash) -> bool {
        if self.pending.lock().unwrap().contains(&hash) {
            return true;
        }
        match self.pool.get() {
            Some(p) => sqlx::query("SELECT 1 FROM pending_upload WHERE hash = ?").bind(hash.0.to_vec()).fetch_optional(p).await.ok().flatten().is_some(),
            None => false,
        }
    }

    pub fn pending(&self) -> usize {
        self.pending.lock().unwrap().len()
    }
}

pub struct Cache {
    dir: std::path::PathBuf,
    cap: u64,
    state: std::sync::Mutex<CacheState>,
    pinned: std::sync::RwLock<std::collections::HashSet<Hash>>,
}

#[derive(Default)]
struct CacheState {
    bytes: u64,
    entries: std::collections::HashMap<Hash, (u64, u64)>,
}

impl Cache {
    fn open(dir: &std::path::Path, cap: u64) -> Result<Cache> {
        std::fs::create_dir_all(dir)?;
        let mut state = CacheState::default();
        for prefix in std::fs::read_dir(dir)? {
            let prefix = prefix?;
            if !prefix.file_type()?.is_dir() {
                continue;
            }
            for f in std::fs::read_dir(prefix.path())? {
                let f = f?;
                let meta = f.metadata()?;
                if let Ok(h) = Hash::from_hex(&f.file_name().to_string_lossy()) {
                    let touched = meta.modified().ok().and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_millis() as u64).unwrap_or(0);
                    state.bytes += meta.len();
                    state.entries.insert(h, (meta.len(), touched));
                }
            }
        }
        Ok(Cache { dir: dir.to_path_buf(), cap, state: std::sync::Mutex::new(state), pinned: std::sync::RwLock::new(Default::default()) })
    }

    fn file(&self, hash: Hash) -> std::path::PathBuf {
        let hex = hash.to_hex();
        self.dir.join(&hex[..2]).join(hex)
    }

    fn read(&self, hash: Hash) -> Option<Vec<u8>> {
        let path = self.file(hash);
        let data = std::fs::read(&path).ok()?;
        let now = crate::authority::now_millis() as u64;
        if let Some(e) = self.state.lock().unwrap().entries.get_mut(&hash) {
            e.1 = now;
        }
        let _ = filetime_touch(&path);
        Some(data)
    }

    fn write(&self, hash: Hash, data: &[u8]) -> Result<()> {
        let path = self.file(hash);
        std::fs::create_dir_all(path.parent().unwrap())?;
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, data)?;
        std::fs::rename(&tmp, &path)?;
        let mut st = self.state.lock().unwrap();
        if st.entries.insert(hash, (data.len() as u64, crate::authority::now_millis() as u64)).is_none() {
            st.bytes += data.len() as u64;
        }
        let pinned = self.pinned.read().unwrap();
        let pending_dir = self.dir.join("pending");
        while st.bytes > self.cap {
            let victim = st.entries.iter().filter(|(h, _)| !pinned.contains(h) && !pending_dir.join(h.to_hex()).exists()).min_by_key(|(_, (_, t))| *t).map(|(h, (n, _))| (*h, *n));
            let Some((h, n)) = victim else { break };
            let _ = std::fs::remove_file(self.file(h));
            st.entries.remove(&h);
            st.bytes -= n;
        }
        Ok(())
    }

    pub fn stats(&self) -> (u64, usize, u64) {
        let st = self.state.lock().unwrap();
        (st.bytes, st.entries.len(), self.cap)
    }

    pub fn evict(&self, hashes: impl Iterator<Item = Hash>) -> u64 {
        let pinned = self.pinned.read().unwrap();
        let pending_dir = self.dir.join("pending");
        let mut st = self.state.lock().unwrap();
        let mut freed = 0;
        for h in hashes {
            if pinned.contains(&h) || pending_dir.join(h.to_hex()).exists() {
                continue;
            }
            if let Some((n, _)) = st.entries.remove(&h) {
                let _ = std::fs::remove_file(self.file(h));
                st.bytes -= n;
                freed += n;
            }
        }
        freed
    }

    pub fn cached_bytes(&self, hashes: impl Iterator<Item = Hash>) -> u64 {
        let st = self.state.lock().unwrap();
        hashes.filter_map(|h| st.entries.get(&h).map(|(n, _)| *n)).sum()
    }

    pub fn pending_on_disk(&self) -> usize {
        std::fs::read_dir(self.dir.join("pending")).map(|d| d.count()).unwrap_or(0)
    }
}

fn filetime_touch(path: &std::path::Path) -> std::io::Result<()> {
    let f = std::fs::OpenOptions::new().write(true).open(path)?;
    f.set_modified(std::time::SystemTime::now())
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PutStats {
    pub uploaded: u64,
    pub uploaded_bytes: u64,
    pub deduplicated: u64,
    pub deduplicated_bytes: u64,
}

impl ShardStore {
    pub fn local(dir: &std::path::Path) -> Result<ShardStore> {
        std::fs::create_dir_all(dir)?;
        let fs = LocalFileSystem::new_with_prefix(dir).context("opening shard dir")?;
        Ok(ShardStore::new(Arc::new(fs)))
    }

    pub fn new(inner: Arc<dyn ObjectStore>) -> ShardStore {
        ShardStore { inner, cache: None, fetches: Arc::new(Default::default()), inflight: Arc::new(Default::default()), uploads: Arc::new(Uploads::new()) }
    }

    pub fn with_cache(mut self, dir: &std::path::Path, cap_bytes: u64) -> Result<ShardStore> {
        self.cache = Some(Arc::new(Cache::open(dir, cap_bytes)?));
        Ok(self)
    }

    pub fn cache(&self) -> Option<&Cache> {
        self.cache.as_deref()
    }

    pub fn uploads(&self) -> &Uploads {
        &self.uploads
    }

    pub fn resume_uploads(&self) -> Result<usize> {
        let Some(c) = &self.cache else { return Ok(0) };
        let dir = c.dir.join("pending");
        std::fs::create_dir_all(&dir)?;
        let mut n = 0;
        for f in std::fs::read_dir(&dir)? {
            let f = f?;
            if let Ok(h) = Hash::from_hex(&f.file_name().to_string_lossy()) {
                if c.file(h).exists() {
                    self.enqueue(h);
                    n += 1;
                } else {
                    let _ = std::fs::remove_file(f.path());
                }
            }
        }
        Ok(n)
    }

    fn enqueue(&self, hash: Hash) {
        let Some(c) = &self.cache else { return };
        {
            let mut p = self.uploads.pending.lock().unwrap();
            if !p.insert(hash) {
                return;
            }
        }
        let _ = std::fs::create_dir_all(c.dir.join("pending"));
        let _ = std::fs::write(c.dir.join("pending").join(hash.to_hex()), b"");
        let inner = self.inner.clone();
        let cache = c.clone();
        let uploads = self.uploads.clone();
        tokio::spawn(async move {
            uploads.record(hash).await;
            let _permit = uploads.sem.clone().acquire_owned().await.ok();
            let data = match std::fs::read(cache.file(hash)) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("pending shard {hash} missing from cache: {e}");
                    uploads.pending.lock().unwrap().remove(&hash);
                    uploads.failed.lock().unwrap().insert(hash);
                    uploads.done.notify_waiters();
                    return;
                }
            };
            let payload = PutPayload::from(data);
            let mut attempt = 0u32;
            loop {
                match inner.put(&Self::path(hash), payload.clone()).await {
                    Ok(_) => {
                        let _ = std::fs::remove_file(cache.dir.join("pending").join(hash.to_hex()));
                        uploads.settle(hash).await;
                        uploads.pending.lock().unwrap().remove(&hash);
                        break;
                    }
                    Err(e) if attempt < 20 => {
                        attempt += 1;
                        tokio::time::sleep(std::time::Duration::from_millis((250 * attempt as u64).min(5000))).await;
                        if attempt % 5 == 0 {
                            eprintln!("still retrying upload of shard {hash} (attempt {attempt}): {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!("upload of shard {hash} gave up for now; it stays pending for the next start: {e}");
                        uploads.pending.lock().unwrap().remove(&hash);
                        uploads.failed.lock().unwrap().insert(hash);
                        break;
                    }
                }
            }
            uploads.done.notify_waiters();
        });
    }

    pub async fn drain(&self) -> Result<()> {
        loop {
            let notified = self.uploads.done.notified();
            if self.uploads.pending() == 0 {
                break;
            }
            notified.await;
        }
        let failed = self.uploads.failed.lock().unwrap().len();
        if failed > 0 {
            anyhow::bail!("{failed} shard(s) could not be uploaded; they stay in the local cache and retry at the next start");
        }
        Ok(())
    }

    pub fn fetches(&self) -> u64 {
        self.fetches.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_pinned(&self, set: std::collections::HashSet<Hash>) {
        if let Some(c) = &self.cache {
            *c.pinned.write().unwrap() = set;
        }
    }

    pub async fn prefetch(&self, hash: Hash) -> Result<()> {
        if let Some(c) = &self.cache
            && !c.file(hash).exists()
        {
            self.get(hash).await?;
        }
        Ok(())
    }

    pub fn from_env(root: &std::path::Path) -> Result<(ShardStore, String)> {
        let Ok(target) = std::env::var("BLACKHOLE_SHARDS") else {
            return Ok((ShardStore::local(&root.join("shards"))?, format!("local {}", root.join("shards").display())));
        };
        if let Some(dir) = target.strip_prefix("file://") {
            std::fs::create_dir_all(dir)?;
            let store = ShardStore::new(Arc::new(LocalFileSystem::new_with_prefix(dir).context("opening file:// shard store")?));
            let cap_gb: f64 = std::env::var("BLACKHOLE_CACHE_GB").ok().and_then(|v| v.parse().ok()).unwrap_or(10.0);
            let store = store.with_cache(&root.join("cache"), (cap_gb * 1_073_741_824.0) as u64)?;
            return Ok((store, format!("{target}, cache {} ({cap_gb} GB)", root.join("cache").display())));
        }
        let bucket = target.strip_prefix("s3://").or_else(|| target.strip_prefix("r2://")).context("BLACKHOLE_SHARDS must look like s3://bucket or file:///dir")?;
        let mut b = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(std::env::var("BLACKHOLE_S3_REGION").unwrap_or_else(|_| "auto".into()))
            .with_access_key_id(std::env::var("BLACKHOLE_S3_KEY").context("BLACKHOLE_S3_KEY")?)
            .with_secret_access_key(std::env::var("BLACKHOLE_S3_SECRET").context("BLACKHOLE_S3_SECRET")?)
            .with_virtual_hosted_style_request(false)
            .with_client_options(ClientOptions::new().with_http1_only().with_timeout(std::time::Duration::from_secs(120)))
            .with_retry(RetryConfig { max_retries: 6, retry_timeout: std::time::Duration::from_secs(180), ..Default::default() });
        let mut label = format!("{target}");
        if let Ok(ep) = std::env::var("BLACKHOLE_S3_ENDPOINT") {
            b = b.with_endpoint(&ep).with_allow_http(ep.starts_with("http://"));
            label = format!("{target} at {ep}");
        }
        let store = ShardStore::new(Arc::new(b.build().context("building S3 shard store")?));
        let cap_gb: f64 = std::env::var("BLACKHOLE_CACHE_GB").ok().and_then(|v| v.parse().ok()).unwrap_or(10.0);
        let store = store.with_cache(&root.join("cache"), (cap_gb * 1_073_741_824.0) as u64)?;
        Ok((store, format!("{label}, cache {} ({cap_gb} GB)", root.join("cache").display())))
    }

    fn path(hash: Hash) -> ObjPath {
        let hex = hash.to_hex();
        ObjPath::from(format!("shards/{}/{}", &hex[..2], hex))
    }

    pub async fn contains(&self, hash: Hash) -> Result<bool> {
        match self.inner.head(&Self::path(hash)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn put(&self, hash: Hash, data: Vec<u8>, stats: &mut PutStats) -> Result<()> {
        let len = data.len() as u64;
        if let Some(c) = &self.cache {
            if c.file(hash).exists() {
                stats.deduplicated += 1;
                stats.deduplicated_bytes += len;
                return Ok(());
            }
            if self.contains(hash).await? {
                stats.deduplicated += 1;
                stats.deduplicated_bytes += len;
                return Ok(());
            }
            c.write(hash, &data)?;
            self.enqueue(hash);
            stats.uploaded += 1;
            stats.uploaded_bytes += len;
            return Ok(());
        }
        if self.contains(hash).await? {
            stats.deduplicated += 1;
            stats.deduplicated_bytes += len;
            return Ok(());
        }
        let payload = PutPayload::from(data);
        let mut attempt = 0;
        loop {
            match self.inner.put(&Self::path(hash), payload.clone()).await {
                Ok(_) => break,
                Err(e) if attempt < 4 => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(250 * attempt)).await;
                    eprintln!("retrying shard {hash} after: {e}");
                }
                Err(e) => return Err(anyhow::Error::from(e).context(format!("storing shard {hash}"))),
            }
        }
        stats.uploaded += 1;
        stats.uploaded_bytes += len;
        Ok(())
    }

    pub async fn get(&self, hash: Hash) -> Result<Bytes> {
        let Some(c) = &self.cache else {
            self.fetches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let r = self.inner.get(&Self::path(hash)).await.with_context(|| format!("fetching shard {hash}"))?;
            return Ok(r.bytes().await?);
        };
        if let Some(data) = c.read(hash) {
            return Ok(Bytes::from(data));
        }
        let gate = self.inflight.lock().unwrap().entry(hash).or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))).clone();
        let _held = gate.lock().await;
        if let Some(data) = c.read(hash) {
            return Ok(Bytes::from(data));
        }
        self.fetches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let r = self.inner.get(&Self::path(hash)).await.with_context(|| format!("fetching shard {hash}"))?;
        let bytes = r.bytes().await?;
        c.write(hash, &bytes)?;
        self.inflight.lock().unwrap().remove(&hash);
        Ok(bytes)
    }

    pub async fn delete(&self, hash: Hash) -> Result<()> {
        if let Some(c) = &self.cache {
            let _ = std::fs::remove_file(c.file(hash));
            let mut st = c.state.lock().unwrap();
            if let Some((n, _)) = st.entries.remove(&hash) {
                st.bytes -= n;
            }
        }
        match self.inner.delete(&Self::path(hash)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_range(&self, hash: Hash, range: Range<u64>) -> Result<Bytes> {
        if self.cache.is_some() {
            let whole = self.get(hash).await?;
            let end = (range.end as usize).min(whole.len());
            return Ok(whole.slice((range.start as usize).min(end)..end));
        }
        self.fetches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(self.inner.get_range(&Self::path(hash), range).await.with_context(|| format!("fetching shard range {hash}"))?)
    }
}
