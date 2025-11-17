use crate::{parse::Parse, store::Db, Connection};

#[derive(Debug)]
pub struct ZAdd {
    key: String,
    score: f64,
    member: String,
}

impl ZAdd {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<ZAdd> {
        let key = parse.next_string()?;
        let score: f64 = parse.next_string()?.parse()?;

        let member = parse.next_string()?;

        Ok(ZAdd { key, score, member })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let value = db.zadd(self.key, self.member, self.score);
        conn.write_frame(&crate::Frame::Integer(value as u64))
            .await?;
        Ok(())
    }
}
