use std::pin::Pin;
use tokio::select;

use bytes::Bytes;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamMap};

use crate::{
    command::Unknown, parse::Parse, server::Shutdown, store::Db, Command, Connection, Frame,
};

type Message = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

impl Subscribe {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Subscribe> {
        use crate::parse::ParseError::EndOfStream;

        let mut channels = vec![parse.next_string()?];
        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Subscribe { channels })
    }

    pub async fn apply(
        mut self,
        db: &Db,
        conn: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        let mut subscriptions = StreamMap::new();
        loop {
            for channel in self.channels.drain(..) {
                subscribe_to_channal(channel, &mut subscriptions, db, conn).await?;
            }

            select! {
                res = conn.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        None => return Ok(())
                    };

                    handle_command(
                        frame,
                        &mut self.channels,
                        conn,
                    ).await?;
                }
                _ = shutdown.recv() => {
                    return Ok(());
                }
            }
        }
    }
}

async fn subscribe_to_channal(
    channal: String,
    subscriptions: &mut StreamMap<String, Message>,
    db: &Db,
    conn: &mut Connection,
) -> crate::Result<()> {
    let mut rx = db.subscribe(channal.clone());

    let tx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => yield msg,
                Err(broadcast::error::RecvError::Lagged(_))=> {},
                Err(_) => break
            }
        }
    });

    subscriptions.insert(channal.clone(), tx);

    let mut frame = Frame::array();
    frame.push_bulk(Bytes::from_static(b"subscribe"));
    frame.push_bulk(Bytes::from(channal));
    frame.push_int(subscriptions.len() as u64);

    conn.write_frame(&frame).await?;
    Ok(())
}

async fn handle_command(
    frame: Frame,
    subscribe_to: &mut Vec<String>,
    conn: &mut Connection,
) -> crate::Result<()> {
    match Command::from_frame(frame)? {
        Command::Subscribe(cmd) => {
            subscribe_to.extend(cmd.channels.into_iter());
        }

        Command::Ping(_) => {
            let mut frame = Frame::array();
            frame.push_bulk(Bytes::from_static(b"pong"));
            frame.push_bulk(Bytes::from_static(b""));
            conn.write_frame(&frame).await?;
        }
        command => {
            let frame = Frame::Error(format!("ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", command.get_name()));
            conn.write_frame(&frame).await?;
        }
    }

    Ok(())
}
