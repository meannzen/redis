use bytes::Bytes;

use crate::{parse::Parse, Connection, Frame};

#[derive(Debug)]
pub struct ReplConf {
    args: Bytes,
    option: Bytes,
}

impl ReplConf {
    pub fn new(args: Bytes, option: Bytes) -> Self {
        Self { args, option }
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<ReplConf> {
        let args = parse.next_bytes()?;
        let option = parse.next_bytes()?;
        Ok(ReplConf { args, option })
    }

    pub fn args_option(&self) -> (&Bytes, &Bytes) {
        (&self.args, &self.option)
    }

    pub async fn apply(self, conn: &mut Connection) -> crate::Result<()> {
        let frame = Frame::Simple("OK".to_string());
        conn.write_frame(&frame).await?;
        Ok(())
    }

    pub fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("replconf".as_bytes()));
        frame.push_bulk(self.args);
        frame.push_bulk(self.option);
        frame
    }
}
