use bytes::Bytes;

use crate::{parse::Parse, store::Db, Connection, Frame};

#[derive(Debug)]
pub struct Set {
    key: String,
    value: Bytes
}

impl Set {
    pub fn new(key: impl ToString, value: Bytes)->Self {
        Set {
            key: key.to_string(),
            value,
        }
    }

    pub fn key(&self)->&str {
        &self.key
    }

    pub fn parse_frame(parse: &mut Parse)->crate::Result<Set> {
        let key = parse.next_string()?;
        let value = parse.next_bytes()?;
        Ok(
            Set {
                key,
                value
            }
        )
    }

    pub async fn apply(self, db: &Db,dst: &mut Connection)->crate::Result<()> {
        db.set(self.key, self.value);
        let response = Frame::Simple("OK".to_string());
        dst.write_frame(&response).await?;
        Ok(())
    }
}
