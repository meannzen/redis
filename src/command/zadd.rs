use bytes::Bytes;

use crate::{parse::Parse, store::Db, Connection, Frame};

#[derive(Debug)]
pub struct ZAdd {
    key: String,
    score: f64,
    member: Bytes,
}

#[derive(Debug)]
pub struct ZRank {
    key: String,
    member: Bytes,
}

#[derive(Debug)]
pub struct ZRange {
    key: String,
    start: i64,
    end: i64,
}

impl ZAdd {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<ZAdd> {
        let key = parse.next_string()?;
        let score: f64 = parse.next_string()?.parse()?;

        let member = parse.next_bytes()?;

        Ok(ZAdd { key, score, member })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let value = db.zadd(self.key, self.member, self.score);
        conn.write_frame(&crate::Frame::Integer(value as u64))
            .await?;
        Ok(())
    }
}

impl ZRank {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<ZRank> {
        Ok(ZRank {
            key: parse.next_string()?,
            member: parse.next_bytes()?,
        })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let frame = match db.zrank(self.key, self.member) {
            Some(rank) => Frame::Integer(rank as u64),
            None => Frame::Null,
        };

        conn.write_frame(&frame).await?;
        Ok(())
    }
}

impl ZRange {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<ZRange> {
        let key = parse.next_string()?;
        let start: i64 = parse.next_string()?.parse()?;
        let end: i64 = parse.next_string()?.parse()?;
        Ok(ZRange { key, start, end })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let values = db.zrange(self.key, self.start, self.end);

        let mut frame = Frame::array();
        for bytes in values {
            frame.push_bulk(bytes);
        }

        conn.write_frame(&frame).await?;

        Ok(())
    }
}
