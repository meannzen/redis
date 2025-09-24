use crate::{parse::Parse, store::Db, Connection, Frame};

#[derive(Debug, Default)]
pub struct Keys {
    key: String,
}

impl Keys {
    pub fn new(key: impl ToString) -> Self {
        Keys {
            key: key.to_string(),
        }
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Keys> {
        let key = parse.next_string()?;
        Ok(Keys { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let keys = db.get_keys(&self.key);
        let mut array_frame = vec![];
        for bytes in keys {
            let bulk = Frame::Bulk(bytes);
            array_frame.push(bulk);
        }
        dst.write_frame(&Frame::Array(array_frame)).await?;
        Ok(())
    }
}
