use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use blackhole_core::{At, Authority, BhError, Ctx, Kind, Op, Version, now_millis};
use clap::Parser;
use nfsserve::nfs::{fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, set_size3, specdata3};
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use tokio::sync::Mutex;

#[derive(Parser)]
#[command(name = "blackhole-mount", version, about = "Mount a Blackhole drive as a folder (loopback NFSv3)")]
struct Cli {
    /// Drive to mount
    drive: String,
    /// Directory to mount it at (created if missing)
    mountpoint: PathBuf,
    /// Root directory of the authority (or set BLACKHOLE_ROOT)
    #[arg(long, env = "BLACKHOLE_ROOT", default_value = ".blackhole")]
    root: PathBuf,
    /// Key secret; every save through the mount is a commit under it (or set BLACKHOLE_KEY)
    #[arg(long, env = "BLACKHOLE_KEY")]
    key: String,
    /// Mount the drive read-only as it was: "#12", "2h ago", or an RFC3339 time
    #[arg(long)]
    at: Option<String>,
    /// NFS port to serve on (0 picks a free one)
    #[arg(long, default_value_t = 0)]
    port: u16,
    /// Serve NFS without running the mount command (mount it yourself, e.g. on Linux)
    #[arg(long)]
    serve_only: bool,
    /// Commit a written file this long after its last write
    #[arg(long, default_value_t = 800)]
    flush_ms: u64,
    /// Kernel readahead in MiB passed to the mount
    #[arg(long, default_value_t = 4)]
    readahead: u32,
}

const ROOT_ID: fileid3 = 1;
const FSID: u64 = 0xb1ac;

fn fid(node: i64) -> fileid3 {
    (node + 1) as fileid3
}

fn node_of(id: fileid3) -> i64 {
    id as i64 - 1
}

fn time3(ms: i64) -> nfstime3 {
    nfstime3 { seconds: (ms / 1000).max(0) as u32, nseconds: ((ms % 1000).max(0) * 1_000_000) as u32 }
}

fn err(e: anyhow::Error) -> nfsstat3 {
    match e.downcast_ref::<BhError>() {
        Some(BhError::NotFound(_)) => nfsstat3::NFS3ERR_NOENT,
        Some(BhError::Denied(_)) => nfsstat3::NFS3ERR_ACCES,
        Some(BhError::Conflict(m)) if m.contains("exists") => nfsstat3::NFS3ERR_EXIST,
        Some(BhError::Conflict(_)) => nfsstat3::NFS3ERR_IO,
        None => {
            eprintln!("mount error: {e:#}");
            nfsstat3::NFS3ERR_IO
        }
    }
}

fn name_str(n: &filename3) -> Result<String, nfsstat3> {
    std::str::from_utf8(n.as_ref()).map(|s| s.to_string()).map_err(|_| nfsstat3::NFS3ERR_INVAL)
}

fn parse_duration_millis(s: &str) -> Result<i64> {
    let (num, unit) = s.split_at(s.trim_end_matches(|c: char| c.is_ascii_alphabetic()).len());
    let n: f64 = num.parse().with_context(|| format!("bad duration {s}"))?;
    let mult = match unit {
        "ms" => 1.0,
        "s" | "" => 1000.0,
        "m" => 60_000.0,
        "h" => 3_600_000.0,
        "d" => 86_400_000.0,
        _ => bail!("bad duration unit in {s}"),
    };
    Ok((n * mult) as i64)
}

fn parse_at(s: Option<&str>) -> Result<At> {
    let Some(s) = s else { return Ok(At::Head) };
    if let Some(seq) = s.strip_prefix('#') {
        return Ok(At::Seq(seq.parse()?));
    }
    if let Some(ago) = s.strip_suffix(" ago").or_else(|| s.strip_prefix('-')) {
        return Ok(At::Time(now_millis() - parse_duration_millis(ago)?));
    }
    if let Ok(d) = parse_duration_millis(s) {
        return Ok(At::Time(now_millis() - d));
    }
    let t = time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).with_context(|| format!("expected #seq, a duration, or RFC3339, got {s}"))?;
    Ok(At::Time((t.unix_timestamp_nanos() / 1_000_000) as i64))
}

struct Dirty {
    node: i64,
    file: std::fs::File,
    path: PathBuf,
    len: u64,
    generation: u64,
    mtime: i64,
}

impl Dirty {
    fn read_at(&self, offset: u64, count: usize) -> std::io::Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let start = offset.min(self.len);
        let end = (start + count as u64).min(self.len);
        let mut buf = vec![0u8; (end - start) as usize];
        self.file.read_exact_at(&mut buf, start)?;
        Ok(buf)
    }

    fn write_at(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.file.write_all_at(data, offset)?;
        self.len = self.len.max(offset + data.len() as u64);
        Ok(())
    }

    fn truncate(&mut self, len: u64) -> std::io::Result<()> {
        self.file.set_len(len)?;
        self.len = len;
        Ok(())
    }
}

struct BhFs {
    auth: Authority,
    ctx: Ctx,
    drive: String,
    at: At,
    readonly: bool,
    uid: u32,
    gid: u32,
    flush_ms: u64,
    journal: PathBuf,
    dirty: Mutex<HashMap<fileid3, Dirty>>,
    trace: bool,
    reads: std::sync::atomic::AtomicU64,
    read_bytes: std::sync::atomic::AtomicU64,
    volatile: Mutex<Volatile>,
}

const VOLATILE_BASE: fileid3 = 1 << 62;

#[derive(Default)]
struct Volatile {
    next: fileid3,
    by_name: HashMap<(fileid3, String), fileid3>,
    files: HashMap<fileid3, (fileid3, String, Vec<u8>, i64)>,
}

fn is_volatile(id: fileid3) -> bool {
    id >= VOLATILE_BASE
}

impl BhFs {
    fn attr_used(&self, id: fileid3, v: &Version, size_override: Option<(u64, i64)>, used: Option<u64>) -> fattr3 {
        let dir = v.kind == Kind::Dir;
        let (size, mtime) = size_override.unwrap_or((v.size, v.mtime));
        let used = used.unwrap_or(size);
        fattr3 {
            ftype: if dir { ftype3::NF3DIR } else { ftype3::NF3REG },
            mode: if dir { 0o755 } else if self.readonly { 0o444 } else { 0o644 },
            nlink: if dir { 2 } else { 1 },
            uid: self.uid,
            gid: self.gid,
            size,
            used,
            rdev: specdata3::default(),
            fsid: FSID,
            fileid: id,
            atime: time3(mtime),
            mtime: time3(mtime),
            ctime: time3(mtime),
        }
    }

    fn volatile_attr(&self, id: fileid3, size: u64, mtime: i64) -> fattr3 {
        fattr3 {
            ftype: ftype3::NF3REG,
            mode: 0o644,
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            size,
            used: size,
            rdev: specdata3::default(),
            fsid: FSID,
            fileid: id,
            atime: time3(mtime),
            mtime: time3(mtime),
            ctime: time3(mtime),
        }
    }

    async fn version(&self, id: fileid3) -> Result<Version, nfsstat3> {
        self.auth.version_of(&self.ctx, &self.drive, node_of(id), self.at).await.map_err(err)?.ok_or(nfsstat3::NFS3ERR_NOENT)
    }

    async fn path(&self, id: fileid3) -> Result<String, nfsstat3> {
        self.auth.node_path(&self.ctx, &self.drive, node_of(id), self.at).await.map_err(err)?.ok_or(nfsstat3::NFS3ERR_NOENT)
    }

    async fn child_path(&self, dirid: fileid3, name: &filename3) -> Result<String, nfsstat3> {
        let parent = self.path(dirid).await?;
        let n = name_str(name)?;
        Ok(if parent == "/" { format!("/{n}") } else { format!("{parent}/{n}") })
    }

    async fn content(&self, id: fileid3, v: &Version) -> Result<Vec<u8>, nfsstat3> {
        let Some(h) = v.manifest_hash else {
            return if v.purged_seq.is_some() { Err(nfsstat3::NFS3ERR_NOENT) } else { Ok(Vec::new()) };
        };
        let manifest = self.auth.manifest(h).await.map_err(err)?;
        let (bytes, _) = self.auth.read_range(&manifest, 0, v.size).await.map_err(err)?;
        let _ = id;
        Ok(bytes)
    }

    async fn load_dirty<'a>(&self, map: &'a mut HashMap<fileid3, Dirty>, id: fileid3) -> Result<&'a mut Dirty, nfsstat3> {
        if !map.contains_key(&id) {
            let v = self.version(id).await?;
            if v.kind == Kind::Dir {
                return Err(nfsstat3::NFS3ERR_ISDIR);
            }
            let data = self.content(id, &v).await?;
            std::fs::create_dir_all(&self.journal).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            let path = self.journal.join(format!("{}-{}", node_of(id), now_millis()));
            let file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            let mut d = Dirty { node: node_of(id), file, path, len: 0, generation: 0, mtime: v.mtime };
            d.write_at(0, &data).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            map.insert(id, d);
        }
        Ok(map.get_mut(&id).unwrap())
    }

    async fn flush(&self, id: fileid3, only_generation: Option<u64>) -> Result<(), nfsstat3> {
        let (node, file, generation, mtime) = {
            let map = self.dirty.lock().await;
            let Some(d) = map.get(&id) else { return Ok(()) };
            if let Some(g) = only_generation
                && d.generation != g
            {
                return Ok(());
            }
            let mut f = d.file.try_clone().map_err(|_| nfsstat3::NFS3ERR_IO)?;
            use std::io::Seek;
            f.seek(std::io::SeekFrom::Start(0)).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            (d.node, f, d.generation, d.mtime)
        };
        let path = self.auth.node_path(&self.ctx, &self.drive, node, At::Head).await.map_err(err)?.ok_or(nfsstat3::NFS3ERR_NOENT)?;
        let (manifest, _) = self.auth.put_reader(std::io::BufReader::with_capacity(4 << 20, file)).await.map_err(err)?;
        let size = manifest.size();
        let hash = self.auth.store_manifest(&manifest).await.map_err(err)?;
        self.auth.commit(&self.ctx, &self.drive, "mount write", vec![Op::Put { path, manifest_hash: hash, size, mtime }]).await.map_err(err)?;
        let mut map = self.dirty.lock().await;
        if map.get(&id).map(|d| d.generation) == Some(generation)
            && let Some(d) = map.remove(&id)
        {
            let _ = std::fs::remove_file(&d.path);
        }
        Ok(())
    }

    async fn flush_all(&self) {
        let ids: Vec<fileid3> = self.dirty.lock().await.keys().copied().collect();
        for id in ids {
            if let Err(e) = self.flush(id, None).await {
                eprintln!("flush of file {id} failed: {e:?}");
            }
        }
    }
}

struct Fs(Arc<BhFs>);

impl std::ops::Deref for Fs {
    type Target = BhFs;
    fn deref(&self) -> &BhFs {
        &self.0
    }
}

impl Fs {
    fn schedule_flush(&self, id: fileid3, generation: u64) {
        let fs = Arc::clone(&self.0);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(fs.flush_ms)).await;
            if let Err(e) = fs.flush(id, Some(generation)).await {
                eprintln!("flush of file {id} failed: {e:?}");
            }
        });
    }
}

#[async_trait]
impl NFSFileSystem for Fs {
    fn capabilities(&self) -> VFSCapabilities {
        if self.readonly { VFSCapabilities::ReadOnly } else { VFSCapabilities::ReadWrite }
    }

    fn root_dir(&self) -> fileid3 {
        ROOT_ID
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let n = name_str(filename)?;
        if is_junk(&n) {
            return self.volatile.lock().await.by_name.get(&(dirid, n)).copied().ok_or(nfsstat3::NFS3ERR_NOENT);
        }
        if n == "." {
            return Ok(dirid);
        }
        if n == ".." {
            let p = self.path(dirid).await?;
            if p == "/" {
                return Ok(ROOT_ID);
            }
            let parent_path = p.rsplit_once('/').map(|(a, _)| if a.is_empty() { "/" } else { a }).unwrap_or("/");
            let (node, _) = self.auth.stat(&self.ctx, &self.drive, parent_path, self.at).await.map_err(err)?.ok_or(nfsstat3::NFS3ERR_NOENT)?;
            return Ok(fid(node));
        }
        match self.auth.lookup_child(&self.ctx, &self.drive, node_of(dirid), &n, self.at).await.map_err(err)? {
            Some((node, _)) => Ok(fid(node)),
            None => Err(nfsstat3::NFS3ERR_NOENT),
        }
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        if is_volatile(id) {
            let vol = self.volatile.lock().await;
            let (_, _, data, mtime) = vol.files.get(&id).ok_or(nfsstat3::NFS3ERR_NOENT)?;
            return Ok(self.volatile_attr(id, data.len() as u64, *mtime));
        }
        let v = self.version(id).await?;
        let mut over = self.dirty.lock().await.get(&id).map(|d| (d.len, d.mtime));
        if v.kind == Kind::Dir
            && let Ok(Some(t)) = self.auth.dir_changed_at(&self.ctx, &self.drive, node_of(id), self.at).await
        {
            over = Some((0, t.max(v.mtime)));
        }
        let used = if over.is_some() || v.kind == Kind::Dir { None } else { Some(self.auth.local_bytes(&v).await) };
        Ok(self.attr_used(id, &v, over, used))
    }

    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        if self.readonly {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        if is_volatile(id) {
            if let set_size3::size(n) = setattr.size {
                let mut vol = self.volatile.lock().await;
                if let Some(f) = vol.files.get_mut(&id) {
                    f.2.resize(n as usize, 0);
                    f.3 = now_millis();
                }
            }
            return self.getattr(id).await;
        }
        if let set_size3::size(n) = setattr.size {
            let generation = {
                let mut map = self.dirty.lock().await;
                let d = self.load_dirty(&mut map, id).await?;
                d.truncate(n).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                d.generation += 1;
                d.mtime = now_millis();
                d.generation
            };
            self.schedule_flush(id, generation);
        }
        self.getattr(id).await
    }

    async fn read(&self, id: fileid3, offset: u64, count: u32) -> Result<(Vec<u8>, bool), nfsstat3> {
        if self.trace {
            let n = self.reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            let total = self.read_bytes.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed) + count as u64;
            eprintln!("read #{n} file {id} offset {offset} len {count}  (cumulative {} MiB)", total / 1048576);
        }
        if is_volatile(id) {
            let vol = self.volatile.lock().await;
            let (_, _, data, _) = vol.files.get(&id).ok_or(nfsstat3::NFS3ERR_NOENT)?;
            let start = (offset as usize).min(data.len());
            let end = (start + count as usize).min(data.len());
            return Ok((data[start..end].to_vec(), end >= data.len()));
        }
        if let Some(d) = self.dirty.lock().await.get(&id) {
            let bytes = d.read_at(offset, count as usize).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            let eof = offset + bytes.len() as u64 >= d.len;
            return Ok((bytes, eof));
        }
        let v = self.version(id).await?;
        if v.kind == Kind::Dir {
            return Err(nfsstat3::NFS3ERR_ISDIR);
        }
        let Some(h) = v.manifest_hash else {
            return if v.purged_seq.is_some() { Err(nfsstat3::NFS3ERR_NOENT) } else { Ok((Vec::new(), true)) };
        };
        let manifest = self.auth.manifest(h).await.map_err(err)?;
        let (bytes, _) = self.auth.read_range(&manifest, offset, count as u64).await.map_err(err)?;
        let eof = offset + bytes.len() as u64 >= v.size;
        if self.auth.store.cache().is_some()
            && let Some((i, _)) = manifest.locate(offset + count as u64)
            && let Some(next) = manifest.shards.get(i + 1)
        {
            let store = self.auth.store.clone();
            let h = next.hash;
            tokio::spawn(async move {
                let _ = store.prefetch(h).await;
            });
        }
        if self.trace {
            eprintln!("  fetches from store so far: {}, pending uploads {}", self.auth.store.fetches(), self.auth.store.uploads().pending());
        }
        Ok((bytes, eof))
    }

    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        if self.readonly {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        if is_volatile(id) {
            {
                let mut vol = self.volatile.lock().await;
                let f = vol.files.get_mut(&id).ok_or(nfsstat3::NFS3ERR_NOENT)?;
                let end = offset as usize + data.len();
                if f.2.len() < end {
                    f.2.resize(end, 0);
                }
                f.2[offset as usize..end].copy_from_slice(data);
                f.3 = now_millis();
            }
            return self.getattr(id).await;
        }
        let generation = {
            let mut map = self.dirty.lock().await;
            let d = self.load_dirty(&mut map, id).await?;
            d.write_at(offset, data).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            d.generation += 1;
            d.mtime = now_millis();
            d.generation
        };
        self.schedule_flush(id, generation);
        self.getattr(id).await
    }

    async fn create(&self, dirid: fileid3, filename: &filename3, _attr: sattr3) -> Result<(fileid3, fattr3), nfsstat3> {
        if self.readonly {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        let n = name_str(filename)?;
        if is_junk(&n) {
            let mut vol = self.volatile.lock().await;
            let id = match vol.by_name.get(&(dirid, n.clone())) {
                Some(id) => *id,
                None => {
                    vol.next += 1;
                    let id = VOLATILE_BASE + vol.next;
                    vol.by_name.insert((dirid, n.clone()), id);
                    vol.files.insert(id, (dirid, n, Vec::new(), now_millis()));
                    id
                }
            };
            let (_, _, data, mtime) = vol.files.get(&id).unwrap();
            return Ok((id, self.volatile_attr(id, data.len() as u64, *mtime)));
        }
        let path = self.child_path(dirid, filename).await?;
        if let Some((node, _)) = self.auth.lookup_child(&self.ctx, &self.drive, node_of(dirid), &name_str(filename)?, At::Head).await.map_err(err)? {
            let id = fid(node);
            return Ok((id, self.getattr(id).await?));
        }
        let (hash, size) = self.auth.put_bytes(Vec::new()).await.map_err(err)?;
        self.auth.commit(&self.ctx, &self.drive, "mount create", vec![Op::Put { path, manifest_hash: hash, size, mtime: now_millis() }]).await.map_err(err)?;
        let id = self.lookup(dirid, filename).await?;
        Ok((id, self.getattr(id).await?))
    }

    async fn create_exclusive(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        if self.readonly {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        if self.lookup(dirid, filename).await.is_ok() {
            return Err(nfsstat3::NFS3ERR_EXIST);
        }
        Ok(self.create(dirid, filename, sattr3::default()).await?.0)
    }

    async fn mkdir(&self, dirid: fileid3, dirname: &filename3) -> Result<(fileid3, fattr3), nfsstat3> {
        if self.readonly {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        let path = self.child_path(dirid, dirname).await?;
        self.auth.commit(&self.ctx, &self.drive, "mount mkdir", vec![Op::Mkdir { path }]).await.map_err(err)?;
        let id = self.lookup(dirid, dirname).await?;
        Ok((id, self.getattr(id).await?))
    }

    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        if self.readonly {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        let n = name_str(filename)?;
        if is_junk(&n) {
            let mut vol = self.volatile.lock().await;
            if let Some(id) = vol.by_name.remove(&(dirid, n)) {
                vol.files.remove(&id);
            }
            return Ok(());
        }
        if let Ok(id) = self.lookup(dirid, filename).await
            && let Some(d) = self.dirty.lock().await.remove(&id)
        {
            let _ = std::fs::remove_file(&d.path);
        }
        let path = self.child_path(dirid, filename).await?;
        self.auth.commit(&self.ctx, &self.drive, "mount remove", vec![Op::Delete { path }]).await.map_err(err)?;
        Ok(())
    }

    async fn rename(&self, from_dirid: fileid3, from_filename: &filename3, to_dirid: fileid3, to_filename: &filename3) -> Result<(), nfsstat3> {
        if self.readonly {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        let from_n = name_str(from_filename)?;
        let to_n = name_str(to_filename)?;
        if is_junk(&from_n) || is_junk(&to_n) {
            let mut vol = self.volatile.lock().await;
            if let Some(id) = vol.by_name.remove(&(from_dirid, from_n)) {
                if let Some(old) = vol.by_name.remove(&(to_dirid, to_n.clone())) {
                    vol.files.remove(&old);
                }
                vol.by_name.insert((to_dirid, to_n.clone()), id);
                if let Some(f) = vol.files.get_mut(&id) {
                    f.0 = to_dirid;
                    f.1 = to_n;
                }
            }
            return Ok(());
        }
        if let Ok(id) = self.lookup(from_dirid, from_filename).await {
            self.flush(id, None).await?;
        }
        let from = self.child_path(from_dirid, from_filename).await?;
        let to = self.child_path(to_dirid, to_filename).await?;
        if let Ok(id) = self.lookup(to_dirid, to_filename).await
            && let Some(d) = self.dirty.lock().await.remove(&id)
        {
            let _ = std::fs::remove_file(&d.path);
        }
        self.auth.commit(&self.ctx, &self.drive, "mount rename", vec![Op::Rename { from, to }]).await.map_err(err)?;
        Ok(())
    }

    async fn readdir(&self, dirid: fileid3, start_after: fileid3, max_entries: usize) -> Result<ReadDirResult, nfsstat3> {
        let mut entries = self.auth.readdir_node(&self.ctx, &self.drive, node_of(dirid), self.at).await.map_err(err)?;
        entries.sort_by(|a, b| a.node.cmp(&b.node));
        let mut out = Vec::new();
        let mut started = start_after == 0;
        let mut end = true;
        for e in entries {
            let id = fid(e.node);
            if !started {
                if id == start_after {
                    started = true;
                }
                continue;
            }
            if out.len() >= max_entries {
                end = false;
                break;
            }
            let used = if e.version.kind == Kind::Dir { None } else { Some(self.auth.local_bytes(&e.version).await) };
            let mut over = None;
            if e.version.kind == Kind::Dir
                && let Ok(Some(t)) = self.auth.dir_changed_at(&self.ctx, &self.drive, e.node, self.at).await
            {
                over = Some((0, t.max(e.version.mtime)));
            }
            out.push(DirEntry { fileid: id, name: e.name.as_bytes().into(), attr: self.attr_used(id, &e.version, over, used) });
        }
        Ok(ReadDirResult { entries: out, end })
    }

    async fn symlink(&self, _dirid: fileid3, _linkname: &filename3, _symlink: &nfspath3, _attr: &sattr3) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn readlink(&self, _id: fileid3) -> Result<nfspath3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
}

fn is_junk(name: &str) -> bool {
    name.starts_with("._") || name == ".DS_Store" || name == ".Spotlight-V100" || name == ".Trashes" || name == ".fseventsd"
}

fn id_of(flag: &str) -> u32 {
    Command::new("id").arg(flag).output().ok().and_then(|o| String::from_utf8(o.stdout).ok()).and_then(|s| s.trim().parse().ok()).unwrap_or(501)
}

fn mount_cmd(port: u16, mountpoint: &std::path::Path, readonly: bool, readahead: u32) -> Command {
    let ro = if readonly { ",ro" } else { "" };
    if cfg!(target_os = "macos") {
        let mut c = Command::new("mount_nfs");
        c.arg("-o").arg(format!("vers=3,tcp,locallocks,rsize=1048576,wsize=1048576,readahead={readahead},actimeo=10,port={port},mountport={port}{ro}")).arg("127.0.0.1:/").arg(mountpoint);
        c
    } else {
        let mut c = Command::new("mount");
        c.arg("-t").arg("nfs").arg("-o").arg(format!("user,noacl,nolock,vers=3,tcp,rsize=1048576,wsize=1048576,actimeo=10,port={port},mountport={port}{ro}")).arg("127.0.0.1:/").arg(mountpoint);
        c
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let auth = Authority::open(&cli.root).await?;
    let ctx = auth.ctx(&cli.key).await?;
    auth.drive(&cli.drive).await?;
    if matches!(parse_at(cli.at.as_deref())?, At::Head) {
        let resumed = auth.store.resume_uploads()?;
        if resumed > 0 {
            eprintln!("resuming {resumed} pending shard upload(s) from the last run");
        }
    }
    let at = parse_at(cli.at.as_deref())?;
    let readonly = !matches!(at, At::Head);
    let fs = Arc::new(BhFs { auth, ctx, drive: cli.drive.clone(), at, readonly, uid: id_of("-u"), gid: id_of("-g"), flush_ms: cli.flush_ms, journal: cli.root.join("journal"), dirty: Mutex::new(HashMap::new()), trace: std::env::var("BLACKHOLE_MOUNT_TRACE").is_ok(), reads: Default::default(), read_bytes: Default::default(), volatile: Mutex::new(Volatile::default()) });
    let listener = NFSTcpListener::bind(&format!("127.0.0.1:{}", cli.port), Fs(Arc::clone(&fs))).await.context("binding NFS listener")?;
    let port = listener.get_listen_port();
    let server = tokio::spawn(async move { listener.handle_forever().await });
    if cli.serve_only {
        eprintln!("serving {} on 127.0.0.1:{port} (NFSv3); mount it yourself", cli.drive);
        server.await??;
        return Ok(());
    }
    std::fs::create_dir_all(&cli.mountpoint)?;
    let status = mount_cmd(port, &cli.mountpoint, readonly, cli.readahead).status().context("running the mount command")?;
    if !status.success() {
        bail!("mount command failed with {status}");
    }
    eprintln!("{} mounted at {} ({}; nfs port {port}). Ctrl-C to unmount.", cli.drive, cli.mountpoint.display(), if readonly { format!("read only, as of {}", cli.at.clone().unwrap_or_default()) } else { "read write".into() });
    tokio::signal::ctrl_c().await?;
    eprintln!("flushing and unmounting");
    fs.flush_all().await;
    let pending = fs.auth.store.uploads().pending();
    if pending > 0 {
        eprintln!("waiting for {pending} shard upload(s); Ctrl-C again leaves them pending for the next start");
        tokio::select! {
            r = fs.auth.store.drain() => { if let Err(e) = r { eprintln!("{e}"); } }
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    let _ = Command::new("umount").arg(&cli.mountpoint).status();
    server.abort();
    Ok(())
}
