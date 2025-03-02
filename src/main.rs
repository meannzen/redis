use anyhow::Result;
use redis_starter_rust::Connection;
use tokio::net::{TcpListener, TcpStream};
#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            process_socket(stream).await;
        });
    }
}

async fn process_socket(stream: TcpStream) {
    let mut connection = Connection::new(stream);
    connection.read_frame().await;
}
