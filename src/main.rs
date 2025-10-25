use clap::Parser;
use redis_starter_rust::{
    clients::Client,
    server,
    server_cli::{Cli, ReplicaOf},
    Result,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let server_cli = cli.clone();
    let server_handle = tokio::spawn(async move {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", server_cli.port()))
            .await
            .expect("Failed to bind listener");
        server::run(listener, server_cli, tokio::signal::ctrl_c()).await
    });

    if let Some(ReplicaOf {
        host,
        port: master_port,
    }) = cli.replicaof
    {
        let mut client = Client::connect(format!("{}:{}", host, master_port)).await?;
        Client::replica(&mut client).await?;
    }

    server_handle.await?;
    Ok(())
}
