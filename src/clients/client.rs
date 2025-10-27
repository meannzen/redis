use bytes::Bytes;
use std::io::{Error, ErrorKind};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::command::ping::Ping;
use crate::{
    command::{PSync, ReplConf},
    Connection, Frame, Result,
};

/// A client for interacting with a Redis-like server over TCP.
///
/// The `Client` struct manages a TCP connection to the server, allowing commands
/// such as `PING`, `REPLCONF`, and `PSYNC` to be sent and responses to be received.
pub struct Client {
    connection: Connection,
}

impl Client {
    /// Establishes a new connection to a server at the specified address.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address of the server to connect to (e.g., "127.0.0.1:6379").
    ///
    /// # Returns
    ///
    /// A `Result` containing the `Client` on success or an error if the connection fails.
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Self> {
        let socket = TcpStream::connect(addr)
            .await
            .map_err(|e| crate::Error::from(e))?;
        let connection = Connection::new(socket);
        Ok(Client { connection })
    }

    /// Configures the client as a replica of a master server.
    ///
    /// This method performs the necessary handshake for replication, including sending
    /// `PING`, `REPLCONF`, and `PSYNC` commands, and then enters a loop to process
    /// incoming frames from the server.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or an error if the handshake or frame processing fails.
    pub async fn replica(&mut self) -> Result<()> {
        self.ping(None).await?;
        self.replconf("listening-port".into(), "6380".into())
            .await?;
        self.replconf("capa".into(), "psync2".into()).await?;

        self.p_sync("?".into(), "-1".into()).await?;
        self.connection.read_file().await?;
        while let Some(x) = self.connection.read_frame().await? {
            println!("{x:?}");
        }
        Ok(())
    }

    /// Sends a `PING` command to the server and returns the response.
    ///
    /// # Arguments
    ///
    /// * `msg` - An optional message to include with the `PING` command.
    ///
    /// # Returns
    ///
    /// A `Result` containing the server's response as `Bytes` or an error.
    pub async fn ping(&mut self, msg: Option<Bytes>) -> Result<Bytes> {
        let frame = Ping::new(msg).into_frame();
        self.connection.write_frame(&frame).await?;
        self.process_response().await
    }

    /// Sends a `REPLCONF` command to the server with the specified key-value pair.
    ///
    /// # Arguments
    ///
    /// * `key` - The configuration key (e.g., "listening-port").
    /// * `value` - The configuration value (e.g., "6380").
    ///
    /// # Returns
    ///
    /// A `Result` containing the server's response as `Bytes` or an error.
    pub async fn replconf(&mut self, key: Bytes, value: Bytes) -> Result<Bytes> {
        let frame = ReplConf::new(key, value).into_frame();
        self.connection.write_frame(&frame).await?;
        self.process_response().await
    }

    /// Sends a `PSYNC` command to the server for replication synchronization.
    ///
    /// # Arguments
    ///
    /// * `replication_id` - The replication ID (e.g., "?").
    /// * `offset` - The replication offset (e.g., "-1").
    ///
    /// # Returns
    ///
    /// A `Result` containing the server's response as `Bytes` or an error.
    pub async fn p_sync(&mut self, replication_id: Bytes, offset: Bytes) -> Result<Bytes> {
        let frame = PSync::new([replication_id, offset]).into_frame();
        self.connection.write_frame(&frame).await?;
        self.process_response().await
    }

    /// Reads and processes a response frame from the server.
    ///
    /// # Returns
    ///
    /// A `Result` containing the response as `Bytes` if valid, or an error if the
    /// response is invalid or the connection is reset.
    async fn process_response(&mut self) -> Result<Bytes> {
        match self.connection.read_frame().await? {
            Some(Frame::Simple(value)) => Ok(value.into()),
            Some(Frame::Bulk(value)) => Ok(value),
            Some(Frame::Error(msg)) => Err(crate::Error::from(msg)),
            Some(frame) => Err(frame.to_error()),
            None => {
                Err(Error::new(ErrorKind::ConnectionReset, "Connection reset by server").into())
            }
        }
    }
}
