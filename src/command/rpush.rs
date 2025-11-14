use crate::{parse::Parse, Connection, Frame};

#[derive(Debug)]
pub struct RPush {
    key: String,
    value: String,
}

impl RPush {
    pub fn new(key: impl ToString, value: impl ToString) -> RPush {
        RPush {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<RPush> {
        let key = parse.next_string()?;
        let value = parse.next_string()?;
        Ok(RPush { key, value })
    }

    pub async fn apply(self, conn: &mut Connection) -> crate::Result<()> {
        conn.write_frame(&Frame::Integer(1)).await?;
        Ok(())
    }
}
