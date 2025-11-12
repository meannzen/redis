use crate::{parse::Parse, Connection};

#[derive(Debug)]
pub struct Multi;

impl Multi {
    pub fn parse_frame(_parse: &mut Parse) -> crate::Result<Multi> {
        Ok(Multi)
    }

    pub async fn apply(self, conn: &mut Connection) -> crate::Result<()> {
        conn.write_frame(&crate::Frame::Simple("OK".to_string()))
            .await?;
        Ok(())
    }
}
