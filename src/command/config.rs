use bytes::Bytes;

use crate::{parse::Parse, Connection, Frame};

#[derive(Debug)]
pub struct Config {
    cmd: String,
}

impl Config {
    pub fn new(cmd: impl ToString) -> Self {
        Config {
            cmd: cmd.to_string(),
        }
    }

    pub fn cmd(&self) -> &str {
        &self.cmd
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Config> {
        let cmd = parse.next_string()?;
        Ok(Config { cmd })
    }

    pub async fn apply(
        self,
        config: &crate::config::Cli,
        dst: &mut Connection,
    ) -> crate::Result<()> {
        let mut frame: Frame = Frame::array();
        if self.cmd == "dir" {
            if let Some(dir) = config.dir.clone() {
                frame.push_bulk(Bytes::from("dir"));
                frame.push_bulk(Bytes::from(dir));
            }
        }
        if self.cmd == "dbfilename" {
            if let Some(dir) = config.dbfilename.clone() {
                frame.push_bulk(Bytes::from("dbfilename"));
                frame.push_bulk(Bytes::from(dir));
            }
        }
        dst.write_frame(&frame).await?;
        Ok(())
    }
}
