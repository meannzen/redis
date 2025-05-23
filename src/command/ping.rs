use bytes::Bytes;

use crate::{
    parse::{Parse, ParseError},
    Connection, Frame,
};
#[derive(Debug, Default)]
pub struct Ping {
    msg: Option<Bytes>,
}

impl Ping {
    pub fn new(msg: Option<Bytes>) -> Self {
        Ping { msg }
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Ping> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Ping { msg: Some(msg) }),
            Err(ParseError::EndOfStream) => Ok(Ping::default()),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.msg {
            None => Frame::Simple("PONG".to_string()),
            Some(msg) => Frame::Bulk(msg),
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}
