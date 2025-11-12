use std::str::FromStr;

use bytes::Bytes;

use crate::{parse::Parse, store::Db, stream::StreamId, Connection, Frame};

#[derive(Debug)]
pub struct XRead {
    keys: Vec<String>,
    ids: Vec<StreamId>,
}

impl XRead {
    pub fn new(keys: Vec<String>, ids: Vec<StreamId>) -> XRead {
        XRead { keys, ids }
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<XRead> {
        let mut keys = vec![];
        let mut ids = vec![];
        while let Ok(s) = parse.next_string() {
            if let Ok(id) = StreamId::from_str(&s) {
                ids.push(id);
            } else {
                keys.push(s);
            }
        }

        Ok(XRead { keys, ids })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let mut final_out = Frame::array();

        for (key, id) in self.keys.into_iter().zip(self.ids.into_iter()) {
            let mut key_wrapper = Frame::array();

            if let Frame::Array(ref mut wrapper_vec) = key_wrapper {
                wrapper_vec.push(Frame::Bulk(Bytes::from(key.clone())));
            }

            let mut entries_array = Frame::array();

            if let Some(entries) = db.xread(key, id) {
                for (entry_id, fields) in entries.into_iter() {
                    let mut entry = Frame::array();

                    if let Frame::Array(ref mut entry_vec) = entry {
                        entry_vec.push(Frame::Bulk(Bytes::from(entry_id.to_string())));

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

            if let Frame::Array(ref mut final_vec) = final_out {
                final_vec.push(key_wrapper);
            }
        }

        conn.write_frame(&final_out).await?;
        Ok(())
    }
}
