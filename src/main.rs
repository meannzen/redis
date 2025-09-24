use anyhow::{Ok, Result};
use clap::Parser;
use redis_starter_rust::{server_cli::Cli, server};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let port = cli.port();
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    server::run(listener, cli, tokio::signal::ctrl_c()).await;
    Ok(())
}
