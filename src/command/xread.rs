use std::str::FromStr;

use bytes::Bytes;

use crate::{parse::Parse, store::Db, stream::StreamId, Connection, Frame};

#[derive(Debug)]
pub struct XRead {
    key: String,
    id: StreamId,
}

impl XRead {
    pub fn new(key: String, id: StreamId) -> XRead {
        XRead { key, id }
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<XRead> {
        let key = parse.next_string()?;
        let id = StreamId::from_str(&parse.next_string()?)?;
        Ok(XRead { key, id })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let mut key_wrapper = Frame::array();

        if let Frame::Array(ref mut wrapper_vec) = key_wrapper {
            wrapper_vec.push(Frame::Bulk(Bytes::from(self.key.clone())));
        }

        let mut entries_array = Frame::array();

        if let Some(entries) = db.xread(self.key, self.id) {
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

                if let Frame::Array(ref mut entries_vec) = entries_array {
                    entries_vec.push(entry);
                }
            }
        }

        if let Frame::Array(ref mut wrapper_vec) = key_wrapper {
            wrapper_vec.push(entries_array);
        }

        let mut final_out = Frame::array();
        if let Frame::Array(ref mut final_vec) = final_out {
            final_vec.push(key_wrapper);
        }

        conn.write_frame(&final_out).await?;
        Ok(())
    }
}
