use std::io;

use anyhow::Result;
use bytes::Bytes;
use redis_starter_rust::{Connection, Frame};
use tokio::net::{TcpListener, TcpStream};
const PING: &[u8] = b"ping";
const ECHO: &[u8] = b"echo";
#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let _ = process_socket(stream).await;
        });
    }
}

async fn process_socket(stream: TcpStream) -> io::Result<()> {
    let mut connection = Connection::new(stream);
    loop {
        let response = connection.read_frame().await;
        if let Ok(frame_option) = response {
            match frame_option {
                Some(frame) => match frame {
                    Frame::Array(val) => {
                        let len = val.len();
                        if len == 0 {
                            break;
                        }
                        let command = &val[0];
                        if let Frame::Bulk(cmd) = command {
                            match cmd.to_ascii_lowercase().as_ref() {
                                PING => {
                                    connection
                                        .write_frame(&Frame::Simple("PONG".to_string()))
                                        .await?;
                                }
                                ECHO => {
                                    if val.len() == 2 {
                                        let msg = &val[1];
                                        connection.write_frame(msg).await?;
                                    }
                                }
                                _ => {
                                    let error = format!(
                                        "ERR unknown command '{}'",
                                        String::from_utf8_lossy(cmd)
                                    );
                                    connection
                                        .write_frame(&Frame::Bulk(Bytes::from(error)))
                                        .await?;
                                }
                            }
                        }
                    }
                    _ => {
                        break;
                    }
                },
                None => {
                    break;
                }
            }
        } else {
            break;
        }
    }
    Ok(())
}
