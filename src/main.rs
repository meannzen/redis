use anyhow::{Ok, Result};
use redis_starter_rust::{config::Config, server};
use tokio::net::TcpListener;
#[tokio::main]
async fn main() -> Result<()> {
    let config: Config = std::env::args().into();
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    server::run(listener, config, tokio::signal::ctrl_c()).await;
    Ok(())
}
