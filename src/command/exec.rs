use crate::{parse::Parse, Connection};

#[derive(Debug)]
pub struct Exec;

impl Exec {
    // temporary not sure next is parse or not
    pub fn parse_frame(_parse: &mut Parse) -> crate::Result<Exec> {
        Ok(Exec)
    }

    pub async fn apply(self, conn: &mut Connection) -> crate::Result<()> {
        conn.write_frame(&crate::Frame::Error("ERR EXEC without MULTI".to_string()))
            .await?;
        Ok(())
    }
}
