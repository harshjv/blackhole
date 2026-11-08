use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use blackhole_core::Authority;
use clap::Parser;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;

#[derive(Parser)]
#[command(name = "blackhole-s3", version, about = "S3-compatible endpoint for Blackhole drives")]
struct Cli {
    /// Root directory of the authority (or set BLACKHOLE_ROOT)
    #[arg(long, env = "BLACKHOLE_ROOT", default_value = ".blackhole")]
    root: PathBuf,
    /// Address to listen on; buckets are drives, the access key id is a key id and the secret its secret
    #[arg(long, env = "BLACKHOLE_S3_ADDR", default_value = "127.0.0.1:9900")]
    addr: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let auth = Authority::open(&cli.root).await?;
    let service = blackhole_s3::service(auth);
    let listener = tokio::net::TcpListener::bind(cli.addr).await?;
    eprintln!("blackhole-s3 listening on http://{} (path-style; access key = secret key = a Blackhole key; bucket = drive alias)", cli.addr);
    let http = Builder::new(TokioExecutor::new());
    loop {
        let (stream, _) = tokio::select! {
            r = listener.accept() => r?,
            _ = tokio::signal::ctrl_c() => break,
        };
        let service = service.clone();
        let http = http.clone();
        tokio::spawn(async move {
            if let Err(e) = http.serve_connection(TokioIo::new(stream), service).await {
                eprintln!("connection error: {e}");
            }
        });
    }
    Ok(())
}
