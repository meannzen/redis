use std::str::FromStr;

use bytes::Bytes;

use crate::stream::{Fields, StreamId};
use crate::{parse::Parse, store::Db, Connection, Frame};

#[derive(Debug)]
pub struct XAdd {
    key: String,
    id: String,
    fields: Fields,
}

impl XAdd {
    pub fn new(key: impl ToString, id: impl ToString, fields: Fields) -> Self {
        XAdd {
            key: key.to_string(),
            id: id.to_string(),
            fields,
        }
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<XAdd> {
        let key = parse.next_string()?;
        let id = parse.next_string()?;

        let mut fields: Fields = Vec::new();

        loop {
            match parse.next_string() {
                Ok(field_name) => {
                    let value = parse.next_bytes()?;
                    fields.push((field_name, value));
                }
                Err(crate::parse::ParseError::EndOfStream) => break,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(XAdd { key, id, fields })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let sid = StreamId::from_str(&self.id).map_err(|e| e.to_string())?;
        let stored = db.xadd(self.key, sid, self.fields);
        dst.write_frame(&Frame::Bulk(Bytes::from(stored))).await?;
        Ok(())
    }
}
