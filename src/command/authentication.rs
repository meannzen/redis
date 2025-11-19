use bytes::Bytes;

use crate::{parse::Parse, Connection, Frame};

#[derive(Debug)]
pub struct ACL {
    command: String,
}

impl ACL {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<ACL> {
        let command = parse.next_string()?;

        Ok(ACL { command })
    }

    pub async fn apply(self, conn: &mut Connection) -> crate::Result<()> {
        let command_str = self.command.to_lowercase();
        let frame = if command_str == "whoami" {
            Frame::Bulk(Bytes::from_static(b"default"))
        } else {
            Frame::Error(format!("Unknown command {}", command_str))
        };

        conn.write_frame(&frame).await?;
        Ok(())
    }
}
