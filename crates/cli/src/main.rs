use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use blackhole_core::{At, Authority, Ctx, Kind, Op, PutStats, format, normalize, now_millis};
use clap::{Args, Parser, Subcommand};
use sqlx::Row;
use futures::StreamExt;

#[derive(Parser)]
#[command(name = "blackhole", version, about = "A versioned drive for fleets of agents: every write names the key that made it, every save is a version, one actor can be undone", long_about = None)]
struct Cli {
    /// Root directory holding the authority database, shards or cache, journal (or set BLACKHOLE_ROOT)
    #[arg(long, env = "BLACKHOLE_ROOT", default_value = ".blackhole", global = true)]
    root: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Clone)]
struct Auth {
    /// Key secret to act as (or set BLACKHOLE_KEY)
    #[arg(long, env = "BLACKHOLE_KEY")]
    key: String,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create a root here (authority database and shard directory)
    Init,
    /// Create, list or hold drives
    Drive {
        #[command(subcommand)]
        cmd: DriveCmd,
    },
    /// Mint keys: one per agent, scoped to a drive and optionally a prefix, read-only or expiring
    Key {
        #[command(subcommand)]
        cmd: KeyCmd,
    },
    /// Upload a file or a whole directory as one commit (DRIVE:/path)
    Put {
        /// Local file or directory
        local: PathBuf,
        /// DRIVE:/path inside the drive
        target: String,
        /// One line saying why; recorded in the commit
        #[arg(long, default_value = "")]
        intent: String,
        #[command(flatten)]
        auth: Auth,
    },
    /// Download a file or directory from a drive, at head or as of a commit or time
    Get {
        /// DRIVE:/path to read
        source: String,
        /// Local file or directory
        local: PathBuf,
        #[arg(long)]
        at: Option<String>,
        #[command(flatten)]
        auth: Auth,
    },
    /// Print a file, optionally a byte range, at head or as of a commit or time
    Cat {
        /// DRIVE:/path to read
        source: String,
        /// Byte range start-end, e.g. 1000-2000
        #[arg(long)]
        range: Option<String>,
        #[arg(long)]
        at: Option<String>,
        #[command(flatten)]
        auth: Auth,
    },
    /// List a directory: kind, size, last commit, name
    Ls {
        /// DRIVE:/path inside the drive
        target: String,
        #[arg(long)]
        at: Option<String>,
        #[command(flatten)]
        auth: Auth,
    },
    /// Delete a file or directory (recursively) as one commit
    Rm {
        /// DRIVE:/path inside the drive
        target: String,
        /// One line saying why; recorded in the commit
        #[arg(long, default_value = "")]
        intent: String,
        #[command(flatten)]
        auth: Auth,
    },
    /// Rename or move within a drive; renaming over a file makes a new version of it
    Mv {
        /// DRIVE:/path to move
        from: String,
        /// DRIVE:/new/path
        to: String,
        /// One line saying why; recorded in the commit
        #[arg(long, default_value = "")]
        intent: String,
        #[command(flatten)]
        auth: Auth,
    },
    /// Every version of a file with who wrote it, when and why; works for deleted files
    History {
        /// DRIVE:/path inside the drive
        target: String,
        #[command(flatten)]
        auth: Auth,
    },
    /// Commits on a drive, filterable by actor and time
    Log {
        /// Drive alias
        drive: String,
        /// Only commits after this: a duration like 2h or an RFC3339 time
        #[arg(long)]
        since: Option<String>,
        /// Only commits by this principal
        #[arg(long)]
        by: Option<String>,
        #[command(flatten)]
        auth: Auth,
    },
    /// Put a file or directory back to how it was at a commit or time, as a new commit
    Restore {
        /// DRIVE:/path inside the drive
        target: String,
        #[arg(long)]
        at: String,
        #[command(flatten)]
        auth: Auth,
    },
    /// Copy-on-write fork of a drive: one row, zero bytes; prints an admin key for the fork
    Fork {
        /// Drive alias
        drive: String,
        /// Alias for the new fork
        new_alias: String,
        #[command(flatten)]
        auth: Auth,
    },
    /// Revert everything one actor wrote since a time, keeping other actors' later edits
    Undo {
        /// Drive alias
        drive: String,
        /// Principal whose writes to revert
        #[arg(long)]
        by: String,
        /// How far back: a duration like 20m or an RFC3339 time
        #[arg(long)]
        since: String,
        #[command(flatten)]
        auth: Auth,
    },
    /// Recompute the hash chain over every commit; exits 2 and names any altered commit
    Verify {
        /// Drive alias
        drive: String,
        #[command(flatten)]
        auth: Auth,
    },
    /// Print the head sequence and head hash as JSON, for signing or publishing
    Attest {
        /// Drive alias
        drive: String,
        #[command(flatten)]
        auth: Auth,
    },
    /// Keep a path's shards in the local cache (bucket-backed roots only); --off releases them
    Pin {
        /// DRIVE:/path inside the drive
        target: String,
        /// Release the pin instead
        #[arg(long)]
        off: bool,
        #[command(flatten)]
        auth: Auth,
    },
    /// Drop a path's local copies from the cache; the files stay in the bucket
    Evict {
        /// DRIVE:/path inside the drive
        target: String,
        #[command(flatten)]
        auth: Auth,
    },
    /// Expire old closed versions by count and age; never under hold, never a version a fork sees
    Gc {
        /// Drive alias
        drive: String,
        /// Newest closed versions to keep per file
        #[arg(long, default_value_t = 10)]
        keep_versions: i64,
        /// Also keep versions closed more recently than this, e.g. 30d
        #[arg(long)]
        keep: Option<String>,
        #[command(flatten)]
        auth: Auth,
    },
    /// Erase every version of a path, leaving a tombstone commit the chain still verifies
    Purge {
        /// DRIVE:/path inside the drive
        target: String,
        /// Why, recorded in the tombstone commit
        #[arg(long)]
        reason: String,
        #[command(flatten)]
        auth: Auth,
    },
    /// Stream commits as they land, with actor, intent and changed paths
    Watch {
        /// Drive alias
        drive: String,
        /// Start after this commit sequence number
        #[arg(long, default_value_t = 0)]
        after: i64,
        /// Poll interval
        #[arg(long, default_value_t = 250)]
        interval_ms: u64,
        /// Print what is there and exit
        #[arg(long)]
        once: bool,
        #[command(flatten)]
        auth: Auth,
    },
    /// Storage, cache and pending-upload figures for this root
    Stats,
}

#[derive(Subcommand)]
enum DriveCmd {
    /// Create a drive and print its owner's admin key
    Create {
        /// Drive alias
        alias: String,
        /// Principal name for the owner's admin key
        #[arg(long, default_value = "owner")]
        owner: String,
    },
    /// List drives with head commit and fork parent
    List,
    /// Legal hold: --on refuses purge and gc until --off
    Hold {
        /// Drive alias
        alias: String,
        /// Put the drive under hold
        #[arg(long, conflicts_with = "off")]
        on: bool,
        /// Release the hold
        #[arg(long)]
        off: bool,
        #[command(flatten)]
        auth: Auth,
    },
}

#[derive(Subcommand)]
enum KeyCmd {
    /// Mint a key for a principal on a drive; prints the secret, the id goes to stderr (--json prints both)
    Create {
        /// Drive alias
        drive: String,
        /// Who this key acts as (an agent, a run, a person)
        #[arg(long)]
        principal: String,
        /// Limit the key to this directory and below
        #[arg(long)]
        prefix: Option<String>,
        /// Reads only
        #[arg(long)]
        read_only: bool,
        /// Allow minting keys, forking, hold, purge and gc
        #[arg(long)]
        admin: bool,
        /// Expire after this long, e.g. 8h
        #[arg(long)]
        ttl: Option<String>,
        /// Print {"id","secret"} as JSON
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        auth: Auth,
    },
}

fn split_target(t: &str) -> Result<(String, String)> {
    let (drive, path) = t.split_once(':').ok_or_else(|| anyhow::anyhow!("expected <drive>:<path>, got {t}"))?;
    Ok((drive.to_string(), normalize(path)?))
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
    let t = time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).with_context(|| format!("expected #seq, <duration> ago, or RFC3339 time, got {s}"))?;
    Ok(At::Time((t.unix_timestamp_nanos() / 1_000_000) as i64))
}

fn parse_since(s: &str) -> Result<i64> {
    if let Ok(d) = parse_duration_millis(s) {
        return Ok(now_millis() - d);
    }
    match parse_at(Some(s))? {
        At::Time(t) => Ok(t),
        _ => bail!("--since takes a duration like 20m or an RFC3339 time"),
    }
}

fn fmt_time(ms: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000)
        .ok()
        .and_then(|t| t.format(&time::format_description::well_known::Rfc3339).ok())
        .unwrap_or_else(|| ms.to_string())
}

struct Chunked {
    manifest: format::Manifest,
    mtime: i64,
}

async fn chunk_file(auth: &Authority, local: &Path, stats: &mut PutStats) -> Result<Chunked> {
    let file = std::fs::File::open(local).with_context(|| format!("opening {}", local.display()))?;
    let mtime = file.metadata()?.modified().ok().and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_millis() as i64).unwrap_or(0);
    let (manifest, st) = auth.put_reader(std::io::BufReader::with_capacity(4 << 20, file)).await?;
    stats.uploaded += st.uploaded;
    stats.uploaded_bytes += st.uploaded_bytes;
    stats.deduplicated += st.deduplicated;
    stats.deduplicated_bytes += st.deduplicated_bytes;
    Ok(Chunked { manifest, mtime })
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for e in std::fs::read_dir(dir)? {
        let p = e?.path();
        if p.is_dir() {
            walk(&p, out)?;
        } else if p.is_file() {
            out.push(p);
        }
    }
    Ok(())
}

async fn cmd_put(auth: &Authority, ctx: &Ctx, local: &Path, target: &str, intent: &str) -> Result<()> {
    let (drive, path) = split_target(target)?;
    let started = Instant::now();
    let mut stats = PutStats::default();
    let mut files = Vec::new();
    if local.is_dir() {
        let mut found = Vec::new();
        walk(local, &mut found)?;
        found.sort();
        for f in found {
            let rel = f.strip_prefix(local)?.to_string_lossy().replace('\\', "/");
            files.push((f, normalize(&format!("{path}/{rel}"))?));
        }
    } else {
        files.push((local.to_path_buf(), path.clone()));
    }
    let parallel: usize = std::env::var("BLACKHOLE_PARALLEL").ok().and_then(|v| v.parse().ok()).unwrap_or(32);
    let results: Vec<Result<(usize, Chunked, PutStats)>> = futures::stream::iter(files.iter().enumerate().map(|(i, (f, _))| {
        let auth = auth.clone();
        let f = f.clone();
        async move {
            let mut st = PutStats::default();
            let c = chunk_file(&auth, &f, &mut st).await?;
            Ok((i, c, st))
        }
    }))
    .buffer_unordered(parallel)
    .collect()
    .await;
    let mut slots: Vec<Option<Chunked>> = (0..files.len()).map(|_| None).collect();
    for r in results {
        let (i, c, st) = r?;
        stats.uploaded += st.uploaded;
        stats.uploaded_bytes += st.uploaded_bytes;
        stats.deduplicated += st.deduplicated;
        stats.deduplicated_bytes += st.deduplicated_bytes;
        slots[i] = Some(c);
    }
    let chunked: Vec<Chunked> = slots.into_iter().map(|c| c.unwrap()).collect();
    let chunk_ms = started.elapsed().as_millis();
    let manifests: Vec<format::Manifest> = chunked.iter().map(|c| c.manifest.clone()).collect();
    let hashes = auth.store_manifests(&manifests).await?;
    let manifest_ms = started.elapsed().as_millis() - chunk_ms;
    let mut ops: Vec<Op> = files
        .into_iter()
        .zip(chunked.iter().zip(hashes))
        .map(|((_, p), (c, hash))| Op::Put { path: p, manifest_hash: hash, size: c.manifest.size(), mtime: c.mtime })
        .collect();
    if ops.is_empty() {
        ops.push(Op::Mkdir { path: path.clone() });
    }
    let n = ops.len();
    let c = auth.commit(ctx, &drive, intent, ops).await?;
    let pending = auth.store.uploads().pending();
    if pending > 0 {
        eprintln!("committed {drive}#{}; uploading {pending} shard(s) in the background, waiting before exit (Ctrl-C leaves them pending for the next start)", c.seq);
        auth.store.drain().await?;
    }
    println!(
        "{drive}#{} {} file(s) by {} in {} ms (chunk+store {} ms, manifests {} ms): {} shard(s) stored ({} bytes), {} deduplicated ({} bytes)",
        c.seq,
        n,
        c.actor,
        started.elapsed().as_millis(),
        chunk_ms,
        manifest_ms,
        stats.uploaded,
        stats.uploaded_bytes,
        stats.deduplicated,
        stats.deduplicated_bytes
    );
    Ok(())
}

async fn cmd_get(auth: &Authority, ctx: &Ctx, source: &str, local: &Path, at: At) -> Result<()> {
    let (drive, path) = split_target(source)?;
    let entries = auth.list(ctx, &drive, &path, at).await?;
    let (_, v) = auth.stat(ctx, &drive, &path, at).await?.ok_or_else(|| anyhow::anyhow!("not found: {path}"))?;
    if v.kind == Kind::Dir {
        std::fs::create_dir_all(local)?;
        for e in entries {
            let child = if path == "/" { format!("/{}", e.name) } else { format!("{path}/{}", e.name) };
            Box::pin(cmd_get(auth, ctx, &format!("{drive}:{child}"), &local.join(&e.name), at)).await?;
        }
        return Ok(());
    }
    let (v, manifest) = auth.open_file(ctx, &drive, &path, at).await?;
    if let Some(parent) = local.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut out = std::fs::File::create(local)?;
    for s in &manifest.shards {
        let bytes = auth.store.get(s.hash).await?;
        std::io::Write::write_all(&mut out, &bytes)?;
    }
    eprintln!("{} bytes, {} shard(s) -> {}", v.size, manifest.shards.len(), local.display());
    Ok(())
}

async fn cmd_cat(auth: &Authority, ctx: &Ctx, source: &str, range: Option<&str>, at: At) -> Result<()> {
    let (drive, path) = split_target(source)?;
    let (v, manifest) = auth.open_file(ctx, &drive, &path, at).await?;
    let (offset, len) = match range {
        Some(r) => {
            let (a, b) = r.split_once('-').ok_or_else(|| anyhow::anyhow!("range is start-end"))?;
            let a: u64 = a.parse()?;
            let b: u64 = if b.is_empty() { v.size } else { b.parse()? };
            (a, b.saturating_sub(a))
        }
        None => (0, v.size),
    };
    let started = Instant::now();
    let (bytes, fetched) = auth.read_range(&manifest, offset, len).await?;
    std::io::Write::write_all(&mut std::io::stdout().lock(), &bytes)?;
    eprintln!("{} bytes read, {} of {} shard(s) fetched, {} ms", bytes.len(), fetched, manifest.shards.len(), started.elapsed().as_millis());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Cmd::Init = cli.cmd {
        let auth = Authority::open(&cli.root).await?;
        auth.principal("owner").await?;
        println!("initialized {}", cli.root.display());
        return Ok(());
    }
    if !cli.root.join("authority.sqlite").exists() {
        bail!("no blackhole root at {} (run `blackhole init`)", cli.root.display());
    }
    let auth = Authority::open(&cli.root).await?;
    match cli.cmd {
        Cmd::Init => unreachable!(),
        Cmd::Watch { drive, after, interval_ms, once, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            let mut cursor = after;
            loop {
                for cs in auth.changes_since(&ctx, &drive, cursor, 1000).await? {
                    let seen_at = now_millis();
                    println!("#{}\t{}\t{}\t+{} ms\t{}", cs.commit.seq, fmt_time(cs.commit.committed_at), cs.commit.actor, seen_at - cs.commit.committed_at, cs.commit.intent);
                    for (p, k) in &cs.paths {
                        println!("\t{k}\t{p}");
                    }
                    cursor = cs.commit.seq;
                }
                if once {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
            }
        }
        Cmd::Stats => {
            let (_, label) = blackhole_core::ShardStore::from_env(&cli.root)?;
            println!("shard store: {label}");
            if let Some(c) = auth.store.cache() {
                let (bytes, n, cap) = c.stats();
                let recorded: i64 = sqlx::query("SELECT COUNT(*) AS n FROM pending_upload").fetch_one(&auth.pool).await?.get("n");
                println!("cache: {n} shard(s), {} MiB of {} MiB; {} shard(s) pending upload ({} recorded in the authority)", bytes / 1048576, cap / 1048576, c.pending_on_disk(), recorded);
            }
            let s = auth.stats().await?;
            println!("commits {}  versions {}  dentries {}  manifests {}", s.commits, s.versions, s.dentries, s.manifests);
            println!("shards {}  stored bytes {}  logical bytes {}  dedup ratio {:.2}", s.shards, s.shard_bytes, s.logical_bytes, if s.shard_bytes > 0 { s.logical_bytes as f64 / s.shard_bytes as f64 } else { 0.0 });
        }
        Cmd::Drive { cmd: DriveCmd::Create { alias, owner } } => {
            let (d, k) = auth.create_drive(&alias, &owner).await?;
            println!("drive {} created; admin key for {owner}: {}", d.alias, k.secret);
            eprintln!("  key id {} (the S3 access key id; the secret above is the S3 secret, the CLI --key and the MCP bearer)", k.id);
        }
        Cmd::Drive { cmd: DriveCmd::Hold { alias, on, off, auth: a } } => {
            let ctx = auth.ctx(&a.key).await?;
            if on == off {
                bail!("pass --on or --off");
            }
            auth.set_hold(&ctx, &alias, on).await?;
            println!("{alias}: legal hold {}", if on { "on: purge refused until released" } else { "off" });
        }
        Cmd::Verify { drive, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            let lines = auth.verify(&ctx, &drive).await?;
            let mut bad = 0;
            for l in &lines {
                if !l.status.starts_with("ok") {
                    bad += 1;
                }
                println!("#{}\t{}\t{}", l.seq, &l.hash[..16], l.status);
            }
            println!("{}: {} commit(s), {} bad", drive, lines.len(), bad);
            if bad > 0 {
                std::process::exit(2);
            }
        }
        Cmd::Attest { drive, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            let t = auth.attest(&ctx, &drive).await?;
            println!("{{\"drive\":\"{}\",\"head_seq\":{},\"head_hash\":\"{}\",\"at\":\"{}\",\"hold\":{}}}", t.drive, t.head_seq, t.head_hash, fmt_time(t.at), t.hold);
        }
        Cmd::Pin { target, off, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            let (drive, path) = split_target(&target)?;
            if auth.store.cache().is_none() {
                bail!("pinning needs a remote shard store with a local cache (set BLACKHOLE_SHARDS)");
            }
            let (files, shards) = auth.pin(&ctx, &drive, &path, !off).await?;
            if off { println!("unpinned {files} file(s) under {path}") } else { println!("pinned {files} file(s), {shards} shard(s) in the local cache") }
        }
        Cmd::Evict { target, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            let (drive, path) = split_target(&target)?;
            let (files, freed) = auth.evict(&ctx, &drive, &path).await?;
            println!("evicted the local copies of {files} file(s) under {path}: {} MiB freed; pinned and pending shards kept", freed / 1048576);
        }
        Cmd::Gc { drive, keep_versions, keep, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            let keep_ms = keep.as_deref().map(parse_duration_millis).transpose()?;
            let r = auth.gc(&ctx, &drive, keep_versions, keep_ms).await?;
            match r.seq {
                Some(seq) => println!("{drive}#{seq} gc: {} version(s) expired, {} manifest(s) removed, {} shard(s) deleted from storage", r.versions_expired, r.manifests_removed, r.shards_removed),
                None => println!("{drive}: nothing to expire"),
            }
        }
        Cmd::Purge { target, reason, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            let (drive, path) = split_target(&target)?;
            let r = auth.purge(&ctx, &drive, &path, &reason).await?;
            println!("{drive}#{} purged {path}: {} version(s) emptied, {} manifest(s) removed, {} shard(s) deleted from storage", r.seq, r.versions_purged, r.manifests_removed, r.shards_removed);
        }
        Cmd::Drive { cmd: DriveCmd::List } => {
            for d in auth.drives().await? {
                let parent = d.parent_id.map(|p| format!(" fork of #{p}@{}", d.fork_seq.unwrap_or(0))).unwrap_or_default();
                println!("{}\t{}\thead #{}{parent}", d.id, d.alias, d.head_seq);
            }
        }
        Cmd::Key { cmd: KeyCmd::Create { drive, principal, prefix, read_only, admin, ttl, json, auth: a } } => {
            let ctx = auth.ctx(&a.key).await?;
            let ttl = ttl.as_deref().map(parse_duration_millis).transpose()?;
            let k = auth.create_key(&ctx, &drive, &principal, prefix.as_deref(), read_only, admin, ttl).await?;
            if json {
                println!("{{\"id\":\"{}\",\"secret\":\"{}\",\"principal\":\"{principal}\",\"drive\":\"{drive}\"}}", k.id, k.secret);
            } else {
                println!("{}", k.secret);
                eprintln!("  key id {}", k.id);
            }
        }
        Cmd::Put { local, target, intent, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            cmd_put(&auth, &ctx, &local, &target, &intent).await?;
        }
        Cmd::Get { source, local, at, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            cmd_get(&auth, &ctx, &source, &local, parse_at(at.as_deref())?).await?;
        }
        Cmd::Cat { source, range, at, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            cmd_cat(&auth, &ctx, &source, range.as_deref(), parse_at(at.as_deref())?).await?;
        }
        Cmd::Ls { target, at, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            let (drive, path) = split_target(&target)?;
            for e in auth.list(&ctx, &drive, &path, parse_at(at.as_deref())?).await? {
                let kind = if e.version.kind == Kind::Dir { "d" } else { "f" };
                println!("{kind}\t{}\t#{}\t{}", e.version.size, e.version.from_seq, e.name);
            }
        }
        Cmd::Rm { target, intent, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            let (drive, path) = split_target(&target)?;
            let c = auth.commit(&ctx, &drive, &intent, vec![Op::Delete { path }]).await?;
            println!("{drive}#{} removed {} entr(ies)", c.seq, c.changes);
        }
        Cmd::Mv { from, to, intent, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            let (drive, from_path) = split_target(&from)?;
            let (drive2, to_path) = split_target(&to)?;
            if drive != drive2 {
                bail!("mv works within one drive");
            }
            let c = auth.commit(&ctx, &drive, &intent, vec![Op::Rename { from: from_path, to: to_path }]).await?;
            println!("{drive}#{}", c.seq);
        }
        Cmd::History { target, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            let (drive, path) = split_target(&target)?;
            for e in auth.history(&ctx, &drive, &path).await? {
                println!("{}#{}\t{}\t{}\t{}\t{}", e.drive_alias, e.seq, fmt_time(e.at), e.actor, e.what, e.intent);
            }
        }
        Cmd::Log { drive, since, by, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            let since = since.as_deref().map(parse_since).transpose()?;
            for c in auth.log(&ctx, &drive, since, by.as_deref()).await? {
                println!("#{}\t{}\t{}\t{} change(s)\t{}", c.seq, fmt_time(c.committed_at), c.actor, c.changes, c.intent);
            }
        }
        Cmd::Restore { target, at, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            let (drive, path) = split_target(&target)?;
            match auth.restore(&ctx, &drive, &path, parse_at(Some(&at))?).await? {
                Some(c) => println!("{drive}#{} restored {path}: {} change(s)", c.seq, c.changes),
                None => println!("{path} already matches {at}"),
            }
        }
        Cmd::Fork { drive, new_alias, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            let started = Instant::now();
            let (d, k) = auth.fork(&ctx, &drive, &new_alias).await?;
            println!("forked {drive} -> {} at #{} in {} ms; admin key: {}", d.alias, d.fork_seq.unwrap_or(0), started.elapsed().as_millis(), k.secret);
            eprintln!("  key id {}", k.id);
        }
        Cmd::Undo { drive, by, since, auth: a } => {
            let ctx = auth.ctx(&a.key).await?;
            let r = auth.undo(&ctx, &drive, &by, parse_since(&since)?).await?;
            match (r.since_seq, &r.commit) {
                (None, _) => println!("no commits by {by} since {since}"),
                (Some(s0), None) => println!("nothing to undo for {by} since #{s0}; conflicts: {}", r.conflicts.len()),
                (Some(s0), Some(c)) => println!("{drive}#{} undid {by} since #{s0}: {} path(s) restored, {} change(s)", c.seq, r.restored.len(), c.changes),
            }
            for p in &r.restored {
                println!("  restored {p}");
            }
            for p in &r.conflicts {
                println!("  conflict {p} (also changed by someone else; left as is)");
            }
        }
    }
    Ok(())
}
