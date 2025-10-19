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
        let value = client.ping(None).await.unwrap();
        if let Ok(string) = std::str::from_utf8(&value) {
            println!("\"{}\"", string);
        } else {
            println!("{:?}", value);
        }
        drop(client);
    }

    server_handle.await?;
    Ok(())
}
