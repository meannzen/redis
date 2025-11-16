use bytes::Bytes;

use crate::{parse::Parse, Connection, Frame};

#[derive(Debug)]
pub struct Subscribe {
    channel: String,
}

impl Subscribe {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Subscribe> {
        Ok(Subscribe {
            channel: parse.next_string()?,
        })
    }

    pub async fn apply(self, conn: &mut Connection) -> crate::Result<()> {
        let mut frame = Frame::array();
        if let Frame::Array(ref mut arr) = frame {
            arr.push(Frame::Bulk(Bytes::from("subscribe")));
            arr.push(Frame::Bulk(Bytes::from(self.channel)));
            arr.push(Frame::Integer(1));
        }

        conn.write_frame(&frame).await?;
        Ok(())
    }
}
