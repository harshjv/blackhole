use std::path::PathBuf;

use anyhow::Result;
use blackhole_core::Authority;
use blackhole_mcp::Blackhole;
use clap::Parser;
use rmcp::ServiceExt;

#[derive(Parser)]
#[command(name = "blackhole-mcp", version, about = "MCP server for a Blackhole drive (stdio)")]
struct Cli {
    /// Root directory of the authority (or set BLACKHOLE_ROOT)
    #[arg(long, env = "BLACKHOLE_ROOT", default_value = ".blackhole")]
    root: PathBuf,
    /// Key secret used as the default actor; tools may pass their own key per call (or set BLACKHOLE_KEY)
    #[arg(long, env = "BLACKHOLE_KEY")]
    key: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let auth = Authority::open(&cli.root).await?;
    auth.ctx(&cli.key).await?;
    let server = Blackhole::new(auth, Some(cli.key));
    let running = server.serve(rmcp::transport::stdio()).await?;
    running.waiting().await?;
    Ok(())
}
