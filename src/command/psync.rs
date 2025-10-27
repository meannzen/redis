use bytes::Bytes;

use crate::{parse::Parse, server::ReplicaConnection, Connection, Frame, MASTER_ID};
use hex;
const DB: &'static str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

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

    pub async fn apply(
        self,
        conn: &mut Connection,
        replica_connection: &ReplicaConnection,
    ) -> crate::Result<()> {
        let frame = Frame::Simple(format!("FULLRESYNC {} 0", MASTER_ID));
        conn.write_frame(&frame).await?;
        replica_connection.lock().unwrap().push(conn.try_clone()?);
        let file = hex::decode(DB).map_err(|e| crate::Error::from(e))?;
        conn.write_content_file(file).await?;
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
