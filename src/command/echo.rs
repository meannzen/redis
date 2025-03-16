use bytes::Bytes;

use crate::{
    parse::{Parse, ParseError},
    Connection, Frame,
};

#[derive(Debug, Default)]
pub struct Echo {
    msg: Option<Bytes>,
}

impl Echo {
    pub fn new(msg: Option<Bytes>) -> Self {
        Echo { msg }
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Echo> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Echo::new(Some(msg))),
            Err(ParseError::EndOfStream) => Ok(Echo::default()),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.msg {
            None => Frame::Simple("".to_string()),
            Some(msg) => Frame::Bulk(msg),
        };
        dst.write_frame(&response).await?;

        Ok(())
    }
}
