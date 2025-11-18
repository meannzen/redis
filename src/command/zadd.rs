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

#[derive(Debug)]
pub struct ZCard {
    key: String,
}

#[derive(Debug)]
pub struct ZScore {
    key: String,
    member: Bytes,
}

#[derive(Debug)]
pub struct ZRem {
    key: String,
    member: Bytes,
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

impl ZCard {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<ZCard> {
        Ok(ZCard {
            key: parse.next_string()?,
        })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let len = db.zcard(self.key);
        let frame = Frame::Integer(len as u64);

        conn.write_frame(&frame).await?;
        Ok(())
    }
}

impl ZScore {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<ZScore> {
        let key = parse.next_string()?;
        let member = parse.next_bytes()?;

        Ok(ZScore { key, member })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let frame = if let Some(v) = db.zscore(self.key, self.member) {
            Frame::Bulk(Bytes::from(v.to_string()))
        } else {
            Frame::Null
        };

        conn.write_frame(&frame).await?;
        Ok(())
    }
}

impl ZRem {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<ZRem> {
        Ok(ZRem {
            key: parse.next_string()?,
            member: parse.next_bytes()?,
        })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let value = db.zrem(self.key, self.member);
        let frame = Frame::Integer(value);

        conn.write_frame(&frame).await?;
        Ok(())
    }
}
