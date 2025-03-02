use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }
    pub async fn read_frame(&mut self) {
        loop {
            let n = self.stream.read_buf(&mut self.buffer).await.unwrap();
            if n == 0 {
                break;
            }
            self.stream.write_all(b"+PONG\r\n").await.unwrap();
            self.stream.flush().await.unwrap();
        }
    }
}
