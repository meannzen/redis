use std::time::{Duration, Instant};

use tokio::time::sleep;

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

#[derive(Debug)]
pub struct LLen {
    key: String,
}

impl LLen {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<LLen> {
        Ok(LLen {
            key: parse.next_string()?,
        })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let len = db.llen(self.key) as u64;
        conn.write_frame(&Frame::Integer(len)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct LPop {
    key: String,
    range: Option<(i64, i64)>,
}

impl LPop {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<LPop> {
        let key = parse.next_string()?;
        let mut range = None;
        let mut start: i64 = 0;
        let mut end: i64 = 0;
        let mut has_start = false;
        if let Ok(start_str) = parse.next_string() {
            has_start = true;
            start = start_str.parse()?;
        }

        let mut has_end = false;
        if let Ok(end_str) = parse.next_string() {
            has_end = true;
            start = end_str.parse()?;
        }

        if !has_end {
            end = start - 1;
            start = 0;
        }

        if start != end {
            range = Some((start, end));
        }

        if !has_end && !has_start {
            range = None;
        }

        Ok(LPop { key, range })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let mut frame = Frame::Null;
        if let Some(range) = self.range {
            let v = db.lpop_rang(self.key.clone(), range.0, range.1);
            if !v.is_empty() {
                frame = Frame::array();
                if let Frame::Array(ref mut list) = frame {
                    for byte in v {
                        list.push(Frame::Bulk(byte));
                    }
                }
            }
        } else if let Some(byte) = db.lpop(self.key) {
            frame = Frame::Bulk(byte);
        }

        conn.write_frame(&frame).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct BLPop {
    key: String,
    timeout: Duration,
}

impl BLPop {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<BLPop> {
        let key = parse.next_string()?;
        let value: f64 = parse.next_string()?.parse()?;
        let timeout = Duration::from_secs_f64(value);
        Ok(BLPop { key, timeout })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let timeout = if self.timeout.as_secs_f64() < 0.0 {
            Duration::ZERO
        } else {
            self.timeout
        };

        dbg!(&self);
        let try_pop = || db.bl_pop(self.key.clone());
        if timeout == Duration::ZERO {
            loop {
                let vec_byte = try_pop();
                if !vec_byte.is_empty() {
                    let mut frame = Frame::array();
                    if let Frame::Array(ref mut v) = frame {
                        for byte in vec_byte {
                            v.push(Frame::Bulk(byte));
                        }
                    }
                    conn.write_frame(&frame).await?;
                    return Ok(());
                }

                sleep(Duration::from_millis(10)).await;
            }
        }
        let deadline = Instant::now() + timeout;

        loop {
            let vec_byte = try_pop();
            if !vec_byte.is_empty() {
                let mut frame = Frame::array();
                if let Frame::Array(ref mut v) = frame {
                    for byte in vec_byte {
                        v.push(Frame::Bulk(byte));
                    }
                }
                conn.write_frame(&frame).await?;
                return Ok(());
            }

            let now = Instant::now();
            if now >= deadline {
                conn.write_null_array().await?;
                return Ok(());
            }

            // Sleep until deadline or a small tick
            let remaining = deadline.saturating_duration_since(now);
            let sleep_dur = remaining.min(Duration::from_millis(10));
            sleep(sleep_dur).await;
        }
    }
}
