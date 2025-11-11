use bytes::Bytes;
use std::str::FromStr;

use crate::{parse::Parse, store::Db, stream::StreamId, Connection, Frame};

#[derive(Debug)]
pub struct XRange {
    key: String,
    start: StreamId,
    end: StreamId,
}

impl XRange {
    pub fn new(start: StreamId, end: StreamId, key: String) -> XRange {
        XRange { start, end, key }
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<XRange> {
        let key = parse.next_string()?;
        let start_str = parse.next_string()?;
        let end_str = parse.next_string()?;
        let start = if start_str == "-" {
            StreamId { ms: 0, seq: 0 }
        } else {
            StreamId::from_str(&start_str)?
        };
        let end = if end_str == "+" {
            StreamId {
                ms: u64::MAX,
                seq: u64::MAX,
            }
        } else {
            StreamId::from_str(&end_str)?
        };

        Ok(XRange { start, end, key })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let mut out = Frame::array();

        if let Some(entries) = db.xrange(self.key, self.start, self.end) {
            for (id, fields) in entries.into_iter() {
                let mut entry = Frame::array();

                if let Frame::Array(ref mut entry_vec) = entry {
                    entry_vec.push(Frame::Bulk(Bytes::from(id.to_string())));

                    let mut fields_arr = Frame::array();
                    if let Frame::Array(ref mut fields_vec) = fields_arr {
                        for (name, value) in fields.into_iter() {
                            fields_vec.push(Frame::Bulk(Bytes::from(name)));
                            fields_vec.push(Frame::Bulk(value));
                        }
                    }

                    entry_vec.push(fields_arr);
                }

                if let Frame::Array(ref mut out_vec) = out {
                    out_vec.push(entry);
                }
            }
        }

        conn.write_frame(&out).await?;
        Ok(())
    }
}
