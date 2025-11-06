use crate::{parse::Parse, server_cli::Cli, Connection, Frame, MASTER_ID};
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
        let mut frame = Frame::array();
        if &self.argument.to_lowercase() == "replication" {
            if config.replicaof.is_some() {
                frame = Frame::Bulk("role:slave".into());
            } else {
                let master = format!(
                    "role:master master_replid:{} master_repl_offset:0",
                    MASTER_ID
                );

                frame = Frame::Bulk(master.into());
            }
        }
        con.write_frame(&frame).await?;
        Ok(())
    }
}
