use clap::Parser;
use redis_starter_rust::{
    server,
    server_cli::{Cli, ReplicaOf},
    Result,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

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
        match TcpStream::connect(format!("{}:{}", host, master_port)).await {
            Ok(mut stream) => {
                let ping_resp = b"*1\r\n$4\r\nPING\r\n";
                if let Err(e) = stream.write_all(ping_resp).await {
                    eprintln!("Failed to send PING: {}", e);
                } else {
                    println!("PING sent to master!");
                }
                let mut buffer = [0; 1024];
                if let Ok(n) = stream.read(&mut buffer).await {
                    if n > 0 {
                        let response = String::from_utf8_lossy(&buffer[..n]);
                        println!("Response from master: {}", response.trim_end());
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "Failed to connect to master {}:{}: {}",
                    host, master_port, e
                );
            }
        }
    }

    server_handle.await?;
    Ok(())
}
