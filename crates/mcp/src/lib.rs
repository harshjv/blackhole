
use anyhow::Result;
use blackhole_core::{At, Authority, Ctx, Kind, Op, normalize, now_millis};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerConfig};
use rmcp::RoleServer;
use rmcp::service::RequestContext;
use rmcp::{ErrorData as McpError, ServerHandler, tool, tool_handler, tool_router};
use std::future::Future;
use schemars::JsonSchema;
use serde::Deserialize;

const INSTRUCTIONS: &str = "Blackhole is a versioned drive shared by people and agents. Every write you make is recorded under your key's principal with an intent string, so write a short intent on every write, delete, rename or restore. Paths are absolute inside a drive and look like /docs/a.txt. Use list and read before writing. history shows who changed a file and when; log shows commits; undo reverts one actor's writes since a time without touching others' work; fork makes an instant copy-on-write drive for risky work, and diff/merge bring it back.";

const READ_CAP: u64 = 512 * 1024;

#[derive(Clone)]
pub struct Blackhole {
    auth: Authority,
    default_key: Option<String>,
}

impl Blackhole {
    pub fn new(auth: Authority, default_key: Option<String>) -> Blackhole {
        Blackhole { auth, default_key }
    }
}

fn bearer(context: &RequestContext<RoleServer>) -> Option<String> {
    let parts = context.extensions.get::<http::request::Parts>()?;
    let v = parts.headers.get(http::header::AUTHORIZATION)?.to_str().ok()?;
    v.strip_prefix("Bearer ").map(|k| k.trim().to_string())
}

async fn run<F: Future<Output = Result<String>>>(f: F) -> Result<CallToolResult, McpError> {
    match f.await {
        Ok(s) => Ok(CallToolResult::success(vec![ContentBlock::text(s)])),
        Err(e) => Ok(CallToolResult::error(vec![ContentBlock::text(format!("{e:#}"))])),
    }
}

fn parse_duration_millis(s: &str) -> Result<i64> {
    let (num, unit) = s.split_at(s.trim_end_matches(|c: char| c.is_ascii_alphabetic()).len());
    let n: f64 = num.parse().map_err(|_| anyhow::anyhow!("bad duration {s}"))?;
    let mult = match unit {
        "ms" => 1.0,
        "s" | "" => 1000.0,
        "m" => 60_000.0,
        "h" => 3_600_000.0,
        "d" => 86_400_000.0,
        _ => anyhow::bail!("bad duration unit in {s}"),
    };
    Ok((n * mult) as i64)
}

fn parse_at(s: Option<&str>) -> Result<At> {
    let Some(s) = s.map(str::trim).filter(|s| !s.is_empty()) else { return Ok(At::Head) };
    if let Some(seq) = s.strip_prefix('#') {
        return Ok(At::Seq(seq.parse()?));
    }
    if let Some(ago) = s.strip_suffix(" ago").or_else(|| s.strip_prefix('-')) {
        return Ok(At::Time(now_millis() - parse_duration_millis(ago)?));
    }
    if let Ok(d) = parse_duration_millis(s) {
        return Ok(At::Time(now_millis() - d));
    }
    let t = time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).map_err(|_| anyhow::anyhow!("expected #seq, a duration like 20m, or an RFC3339 time, got {s}"))?;
    Ok(At::Time((t.unix_timestamp_nanos() / 1_000_000) as i64))
}

fn parse_since(s: &str) -> Result<i64> {
    match parse_at(Some(s))? {
        At::Time(t) => Ok(t),
        At::Seq(_) => anyhow::bail!("since takes a duration like 20m or an RFC3339 time"),
        At::Head => Ok(0),
    }
}

fn fmt_time(ms: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000)
        .ok()
        .and_then(|t| t.format(&time::format_description::well_known::Rfc3339).ok())
        .unwrap_or_else(|| ms.to_string())
}

#[derive(Deserialize, JsonSchema)]
struct ListArgs {
    /// Drive alias
    drive: String,
    /// Directory path inside the drive, default /
    path: Option<String>,
    /// Read the drive as it was: "#12" (commit), "20m ago", or an RFC3339 time
    at: Option<String>,
    /// Act with another key instead of the server's default key
    key: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct ReadArgs {
    drive: String,
    /// File path inside the drive
    path: String,
    /// Byte offset to start from, default 0
    offset: Option<u64>,
    /// Max bytes to return, default and cap 524288
    length: Option<u64>,
    /// Read the file as it was: "#12", "20m ago", or an RFC3339 time
    at: Option<String>,
    key: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct WriteArgs {
    drive: String,
    /// File path inside the drive; parent directories are created
    path: String,
    /// Full new content of the file (UTF-8 text)
    content: String,
    /// One line saying why you are writing this
    intent: Option<String>,
    /// Optimistic lock: the version (#seq from list/read/history) you expect the file to be at; 0 means the file must not exist yet. The write fails if someone changed it.
    if_version: Option<i64>,
    key: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct DeleteArgs {
    drive: String,
    /// File or directory path; directories are deleted recursively
    path: String,
    intent: Option<String>,
    key: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct RenameArgs {
    drive: String,
    from: String,
    to: String,
    intent: Option<String>,
    key: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct HistoryArgs {
    drive: String,
    path: String,
    key: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct LogArgs {
    drive: String,
    /// Only commits after this: a duration like "1h" or an RFC3339 time
    since: Option<String>,
    /// Only commits by this principal
    by: Option<String>,
    key: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct RestoreArgs {
    drive: String,
    /// File or directory path to restore
    path: String,
    /// Target state: "#12", "20m ago", or an RFC3339 time
    at: String,
    key: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct UndoArgs {
    drive: String,
    /// Principal whose writes to revert (as shown in log and history)
    by: String,
    /// How far back: a duration like "20m" or an RFC3339 time
    since: String,
    key: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct ForkArgs {
    /// Drive to fork
    drive: String,
    /// Alias for the new fork
    new_alias: String,
    key: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct ForkRefArgs {
    /// Alias of a fork
    fork: String,
    key: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct DrivesArgs {}

#[derive(Deserialize, JsonSchema)]
struct ChangesArgs {
    drive: String,
    /// Return commits after this sequence number; pass the last seq you saw to poll
    after_seq: i64,
    /// Max commits to return, default 100
    limit: Option<i64>,
    key: Option<String>,
}

#[tool_router]
impl Blackhole {
    async fn ctx(&self, key: &Option<String>, context: &RequestContext<RoleServer>) -> Result<Ctx> {
        let key = key.clone().or_else(|| bearer(context)).or_else(|| self.default_key.clone()).ok_or_else(|| anyhow::anyhow!("no key: pass key, send Authorization: Bearer <key>, or start the server with one"))?;
        self.auth.ctx(&key).await
    }

    #[tool(description = "List the drives in this workspace with their head commit and fork parent")]
    async fn drives(&self, Parameters(_a): Parameters<DrivesArgs>, _context: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        run(async {
            let drives = self.auth.drives().await?;
            let mut out = String::new();
            for d in drives {
                let parent = d.parent_id.map(|p| format!("  fork of drive {p} at #{}", d.fork_seq.unwrap_or(0))).unwrap_or_default();
                out.push_str(&format!("{}  head #{}{parent}\n", d.alias, d.head_seq));
            }
            Ok(if out.is_empty() { "no drives".to_string() } else { out })
        })
        .await
    }

    #[tool(description = "List a directory in a drive: kind, size, last commit, name. Optionally as of an earlier commit or time.")]
    async fn list(&self, Parameters(a): Parameters<ListArgs>, context: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        run(async {
            let ctx = self.ctx(&a.key, &context).await?;
            let path = normalize(a.path.as_deref().unwrap_or("/"))?;
            let at = parse_at(a.at.as_deref())?;
            let entries = self.auth.list(&ctx, &a.drive, &path, at).await?;
            let mut out = String::new();
            for e in entries {
                let kind = if e.version.kind == Kind::Dir { "dir " } else { "file" };
                out.push_str(&format!("{kind}  {:>10}  #{:<6}  {}\n", e.version.size, e.version.from_seq, e.name));
            }
            Ok(if out.is_empty() { format!("{path} is empty") } else { out })
        })
        .await
    }

    #[tool(description = "Read a file from a drive as UTF-8 text. Large files are returned in slices of up to 512 KiB; use offset and length to page.")]
    async fn read(&self, Parameters(a): Parameters<ReadArgs>, context: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        run(async {
            let ctx = self.ctx(&a.key, &context).await?;
            let path = normalize(&a.path)?;
            let at = parse_at(a.at.as_deref())?;
            let (v, manifest) = self.auth.open_file(&ctx, &a.drive, &path, at).await?;
            let offset = a.offset.unwrap_or(0);
            let length = match a.length {
                Some(l) if l > 0 => l.min(READ_CAP),
                _ => READ_CAP,
            };
            let (mut bytes, _) = self.auth.read_range(&manifest, offset, length).await?;
            let mut end = offset + bytes.len() as u64;
            if end < v.size
                && let Err(e) = std::str::from_utf8(&bytes)
                && e.error_len().is_none()
                && e.valid_up_to() > 0
            {
                bytes.truncate(e.valid_up_to());
                end = offset + bytes.len() as u64;
            }
            let body = String::from_utf8_lossy(&bytes);
            let more = if end < v.size { format!("; call again with offset={end} for more") } else { String::new() };
            Ok(format!("[{path} version #{}; bytes {offset}-{end} of {}{more}]\n{body}", v.from_seq, v.size))
        })
        .await
    }

    #[tool(description = "Write the full content of a file (creates or replaces it) as one commit under your principal. Always pass an intent.")]
    async fn write(&self, Parameters(a): Parameters<WriteArgs>, context: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        run(async {
            let ctx = self.ctx(&a.key, &context).await?;
            let path = normalize(&a.path)?;
            let (hash, size) = self.auth.put_bytes(a.content.into_bytes()).await?;
            let op = match a.if_version {
                Some(expect_seq) => Op::PutIf { path: path.clone(), manifest_hash: hash, size, mtime: now_millis(), expect_seq },
                None => Op::Put { path: path.clone(), manifest_hash: hash, size, mtime: now_millis() },
            };
            let c = self.auth.commit(&ctx, &a.drive, a.intent.as_deref().unwrap_or(""), vec![op]).await?;
            Ok(format!("wrote {path} ({size} bytes) as {} in {}#{}; version #{}", c.actor, a.drive, c.seq, c.seq))
        })
        .await
    }

    #[tool(description = "Delete a file or directory (recursively) as one commit under your principal.")]
    async fn delete(&self, Parameters(a): Parameters<DeleteArgs>, context: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        run(async {
            let ctx = self.ctx(&a.key, &context).await?;
            let path = normalize(&a.path)?;
            let c = self.auth.commit(&ctx, &a.drive, a.intent.as_deref().unwrap_or(""), vec![Op::Delete { path: path.clone() }]).await?;
            Ok(format!("deleted {path} ({} entries) in {}#{}", c.changes, a.drive, c.seq))
        })
        .await
    }

    #[tool(description = "Rename or move a file or directory within a drive. History follows the file.")]
    async fn rename(&self, Parameters(a): Parameters<RenameArgs>, context: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        run(async {
            let ctx = self.ctx(&a.key, &context).await?;
            let from = normalize(&a.from)?;
            let to = normalize(&a.to)?;
            let c = self.auth.commit(&ctx, &a.drive, a.intent.as_deref().unwrap_or(""), vec![Op::Rename { from: from.clone(), to: to.clone() }]).await?;
            Ok(format!("renamed {from} -> {to} in {}#{}", a.drive, c.seq))
        })
        .await
    }

    #[tool(description = "Show every version of a file: commit, time, who wrote it, what changed, and their intent. Works for deleted files too.")]
    async fn history(&self, Parameters(a): Parameters<HistoryArgs>, context: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        run(async {
            let ctx = self.ctx(&a.key, &context).await?;
            let path = normalize(&a.path)?;
            let events = self.auth.history(&ctx, &a.drive, &path).await?;
            let mut out = String::new();
            for e in events {
                out.push_str(&format!("{}#{}  {}  {}  {}  {}\n", e.drive_alias, e.seq, fmt_time(e.at), e.actor, e.what, e.intent));
            }
            Ok(if out.is_empty() { format!("no history for {path}") } else { out })
        })
        .await
    }

    #[tool(description = "List commits on a drive: seq, time, actor, number of changes, intent. Filter by actor and by time.")]
    async fn log(&self, Parameters(a): Parameters<LogArgs>, context: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        run(async {
            let ctx = self.ctx(&a.key, &context).await?;
            let since = a.since.as_deref().map(parse_since).transpose()?;
            let commits = self.auth.log(&ctx, &a.drive, since, a.by.as_deref()).await?;
            let mut out = String::new();
            for c in commits {
                out.push_str(&format!("#{}  {}  {}  {} change(s)  {}\n", c.seq, fmt_time(c.committed_at), c.actor, c.changes, c.intent));
            }
            Ok(if out.is_empty() { "no commits".to_string() } else { out })
        })
        .await
    }

    #[tool(description = "Restore a file or directory to how it was at a commit or time, as a new commit. Nothing in history is lost.")]
    async fn restore(&self, Parameters(a): Parameters<RestoreArgs>, context: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        run(async {
            let ctx = self.ctx(&a.key, &context).await?;
            let path = normalize(&a.path)?;
            let at = parse_at(Some(&a.at))?;
            match self.auth.restore(&ctx, &a.drive, &path, at).await? {
                Some(c) => Ok(format!("restored {path} to {} in {}#{} ({} changes)", a.at, a.drive, c.seq, c.changes)),
                None => Ok(format!("{path} already matches {}", a.at)),
            }
        })
        .await
    }

    #[tool(description = "Revert everything one principal wrote since a time, keeping other principals' later edits. Files both touched are reported as conflicts and left alone. Creates one new commit.")]
    async fn undo(&self, Parameters(a): Parameters<UndoArgs>, context: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        run(async {
            let ctx = self.ctx(&a.key, &context).await?;
            let since = parse_since(&a.since)?;
            let r = self.auth.undo(&ctx, &a.drive, &a.by, since).await?;
            let mut out = match (r.since_seq, &r.commit) {
                (None, _) => format!("no commits by {} since {}\n", a.by, a.since),
                (Some(s0), None) => format!("nothing to undo for {} since #{s0}\n", a.by),
                (Some(s0), Some(c)) => format!("{}#{}: undid {} since #{s0}, {} path(s) restored\n", a.drive, c.seq, a.by, r.restored.len()),
            };
            for p in &r.restored {
                out.push_str(&format!("restored {p}\n"));
            }
            for p in &r.conflicts {
                out.push_str(&format!("conflict {p} (someone else also changed it; left as is)\n"));
            }
            Ok(out)
        })
        .await
    }

    #[tool(description = "Commits on a drive after a sequence number, with who made them and which paths changed. Poll with the last seq you saw to follow other agents' work.")]
    async fn changes(&self, Parameters(a): Parameters<ChangesArgs>, context: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        run(async {
            let ctx = self.ctx(&a.key, &context).await?;
            let sets = self.auth.changes_since(&ctx, &a.drive, a.after_seq, a.limit.unwrap_or(100)).await?;
            let head = self.auth.head_seq(&a.drive).await?;
            let mut out = format!("head #{head}\n");
            for cs in sets {
                out.push_str(&format!("#{}  {}  {}  {}\n", cs.commit.seq, fmt_time(cs.commit.committed_at), cs.commit.actor, cs.commit.intent));
                for (p, k) in &cs.paths {
                    out.push_str(&format!("    {k:<7} {p}\n"));
                }
            }
            Ok(out)
        })
        .await
    }

    #[tool(description = "Fork a drive instantly (copy-on-write, zero bytes copied). Returns the fork alias and a key for it. Work in the fork, then diff and merge.")]
    async fn fork(&self, Parameters(a): Parameters<ForkArgs>, context: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        run(async {
            let ctx = self.ctx(&a.key, &context).await?;
            let (d, k) = self.auth.fork(&ctx, &a.drive, &a.new_alias).await?;
            Ok(format!("forked {} -> {} at #{}; key for the fork: {} (id {})", a.drive, d.alias, d.fork_seq.unwrap_or(0), k.secret, k.id))
        })
        .await
    }

    #[tool(description = "Show what a fork changed relative to the point it was forked from: added, modified, deleted paths.")]
    async fn diff(&self, Parameters(a): Parameters<ForkRefArgs>, context: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        run(async {
            let ctx = self.ctx(&a.key, &context).await?;
            let (fork, parent, changes) = self.auth.fork_changes(&ctx, &a.fork).await?;
            let mut out = format!("{} vs {} at #{}: {} change(s)\n", fork.alias, parent.alias, fork.fork_seq.unwrap_or(0), changes.len());
            for c in changes {
                out.push_str(&format!("{:<8} {:>10}  {}\n", c.kind, c.size, c.path));
            }
            Ok(out)
        })
        .await
    }

    #[tool(description = "Apply a fork's changes onto its parent drive as one commit. Paths the parent changed since the fork are reported as conflicts and skipped.")]
    async fn merge(&self, Parameters(a): Parameters<ForkRefArgs>, context: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        run(async {
            let ctx = self.ctx(&a.key, &context).await?;
            let r = self.auth.merge(&ctx, &a.fork).await?;
            let mut out = match &r.commit {
                Some(c) => format!("merged {} into drive {} as #{}: {} change(s) applied\n", a.fork, c.drive_id, c.seq, r.applied.len()),
                None => format!("nothing to merge from {}\n", a.fork),
            };
            for c in &r.applied {
                out.push_str(&format!("{:<8} {}\n", c.kind, c.path));
            }
            for p in &r.conflicts {
                out.push_str(&format!("conflict {p} (parent changed it since the fork; skipped)\n"));
            }
            Ok(out)
        })
        .await
    }
}

#[tool_handler]
impl ServerHandler for Blackhole {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("blackhole", env!("CARGO_PKG_VERSION")))
            .with_instructions(INSTRUCTIONS)
    }
}

