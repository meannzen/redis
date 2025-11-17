use crate::{parse::Parse, Connection};

#[derive(Debug)]
pub struct ZAdd {
    key: String,
    value: f64,
    member: String,
}

impl ZAdd {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<ZAdd> {
        let key = parse.next_string()?;
        let value: f64 = parse.next_string()?.parse()?;

        let member = parse.next_string()?;

        Ok(ZAdd { key, value, member })
    }

    pub async fn apply(self, conn: &mut Connection) -> crate::Result<()> {
        conn.write_frame(&crate::Frame::Integer(1)).await?;
        Ok(())
    }
}
