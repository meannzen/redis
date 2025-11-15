use bytes::Bytes;

use crate::{parse::Parse, store::Db, Connection, Frame};

#[derive(Debug)]
pub struct RPush {
    key: String,
    values: Vec<Bytes>,
}

impl RPush {
    pub fn new(key: impl ToString) -> RPush {
        RPush {
            key: key.to_string(),
            values: Vec::new(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<RPush> {
        let key = parse.next_string()?;
        let mut values = Vec::new();
        while let Ok(byte) = parse.next_bytes() {
            values.push(byte);
        }
        Ok(RPush { key, values })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let key_count = db.rpush(self.key, self.values);
        conn.write_frame(&Frame::Integer(key_count)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct LPush {
    key: String,
    values: Vec<Bytes>,
}

impl LPush {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<LPush> {
        let key = parse.next_string()?;
        let mut values = Vec::new();
        while let Ok(byte) = parse.next_bytes() {
            values.push(byte);
        }

        Ok(LPush { key, values })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let mut values = self.values.clone();
        values.reverse();
        let size = db.lpush(self.key, values);
        conn.write_frame(&Frame::Integer(size)).await?;
        Ok(())
    }
}
