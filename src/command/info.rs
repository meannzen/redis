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
                frame = Frame::Simple(
                    "role:master master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb master_repl_offset:0"
                        .to_string(),
                )
            }
        }
        con.write_frame(&frame).await?;
        Ok(())
    }
}
