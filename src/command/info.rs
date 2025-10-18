use crate::{parse::Parse, server_cli::Cli, Connection, Frame};
#[derive(Debug)]
pub struct Info {
    argument: String,
}

impl Info {
    pub fn new(cmd: impl ToString) -> Self {
        Info {
            argument: cmd.to_string(),
        }
    }
    pub fn parse_frame(phrse: &mut Parse) -> crate::Result<Info> {
        let argument = phrse.next_string()?;
        Ok(Info { argument })
    }

    pub async fn apply(self, config: &Cli, con: &mut Connection) -> crate::Result<()> {
        let mut frame = Frame::Null;
        if &self.argument.to_lowercase() == "replication" {
            if config.replicaof.is_some() {
                frame = Frame::Simple("role:slave".to_string());
            } else {
                frame = Frame::Simple("role:master".to_string());
            }
        }
        con.write_frame(&frame).await?;
        Ok(())
    }
}
