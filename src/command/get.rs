use crate::{parse::Parse, store::Db, Connection, Frame};

#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: impl ToString) -> Self {
        Get {
            key: key.to_string(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Get> {
        let key = parse.next_string()?;
        Ok(Get { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = if let Some(value) = db.get(&self.key) {
            Frame::Bulk(value)
        } else {
            Frame::Null            
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}
