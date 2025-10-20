use bytes::Bytes;

use crate::{parse::Parse, Connection, Frame};

#[derive(Debug, Clone)]
pub struct PSync {
    args: [Bytes; 2],
}

impl PSync {
    pub fn new(value: [Bytes; 2]) -> Self {
        PSync { args: value }
    }

    pub fn value(&self) -> &[Bytes; 2] {
        &self.args
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<PSync> {
        let v1 = parse.next_bytes()?;
        let v2 = parse.next_bytes()?;
        Ok(PSync { args: [v1, v2] })
    }

    pub async fn apply(self, conn: &mut Connection) -> crate::Result<()> {
        let frame = Frame::Simple("OK".to_string());
        conn.write_frame(&frame).await?;
        Ok(())
    }

    pub fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("psync".as_bytes()));
        let [x, y] = self.args;
        frame.push_bulk(x);
        frame.push_bulk(y);
        frame
    }
}
