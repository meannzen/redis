use std::io::{Error, ErrorKind};

use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::{command::ping::Ping, Connection, Frame};

pub struct Client {
    connection: Connection,
}

impl Client {
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client> {
        let socket = TcpStream::connect(addr).await?;

        let connection = Connection::new(socket);

        Ok(Client { connection })
    }

    pub async fn ping(&mut self, msg: Option<Bytes>) -> crate::Result<Bytes> {
        let frame = Ping::new(msg).into_frame();
        self.connection.write_frame(&frame).await?;
        match self.read_response().await? {
            Frame::Simple(value) => Ok(value.into()),
            Frame::Bulk(value) => Ok(value.into()),
            frame => Err(frame.to_error()),
        }
    }

    async fn read_response(&mut self) -> crate::Result<Frame> {
        let response = self.connection.read_frame().await?;
        match response {
            Some(Frame::Error(msg)) => Err(msg.into()),
            Some(frame) => Ok(frame),
            None => {
                let err = Error::new(ErrorKind::ConnectionReset, "Connection reset server");
                Err(err.into())
            }
        }
    }
}
