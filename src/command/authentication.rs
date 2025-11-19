use bytes::Bytes;

use crate::{parse::Parse, Connection, Frame};
#[allow(dead_code)]
#[derive(Debug)]
pub struct ACL {
    command: String,
    user: Option<String>,
}

impl ACL {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<ACL> {
        let command = parse.next_string()?;
        let user = parse.next_string().ok();

        Ok(ACL { command, user })
    }

    pub async fn apply(self, conn: &mut Connection) -> crate::Result<()> {
        let command_str = self.command.to_lowercase();

        let frame = if command_str == "whoami" {
            Frame::Bulk(Bytes::from_static(b"default"))
        } else if command_str == "getuser" {
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"flags")),
                Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"nopass"))]),
                Frame::Bulk(Bytes::from_static(b"passwords")),
                Frame::Array(vec![]),
            ])
        } else {
            Frame::Error(format!("Unknown command {}", command_str))
        };

        conn.write_frame(&frame).await?;
        Ok(())
    }
}
