use anyhow::{Ok, Result};
use redis_starter_rust::server;
use tokio::net::TcpListener;
#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    server::run(listener, tokio::signal::ctrl_c()).await;
    Ok(())
}
