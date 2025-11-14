use crate::{parse::Parse, store::Db, Connection, Frame};

#[derive(Debug)]
pub struct LRange {
    key: String,
    start: i64,
    end: i64,
}

impl LRange {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<LRange> {
        let key = parse.next_string()?;
        let start: i64 = parse.next_string()?.parse()?;
        let end: i64 = parse.next_string()?.parse()?;
        Ok(LRange { key, start, end })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let mut frame = Frame::array();
        let list = db.lrange(self.key, self.start, self.end);
        if let Frame::Array(ref mut v) = frame {
            for byte in list {
                v.push(Frame::Bulk(byte));
            }
        }

        conn.write_frame(&frame).await?;
        Ok(())
    }
}
