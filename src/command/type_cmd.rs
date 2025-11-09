use crate::{parse::Parse, store::Db, Connection, Frame};

#[derive(Debug)]
pub struct Type {
    key: String,
}

impl Type {
    pub fn new(key: impl ToString) -> Self {
        Type {
            key: key.to_string(),
        }
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Type> {
        let key = parse.next_string()?;

        Ok(Type { key })
    }
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = if db.get(&self.key).is_some() {
            Frame::Simple("string".to_string())
        } else {
            Frame::Simple("none".to_string())
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}
