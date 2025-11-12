use crate::{parse::Parse, server::TransactionState, Connection};

#[derive(Debug)]
pub struct Multi;

impl Multi {
    pub fn parse_frame(_parse: &mut Parse) -> crate::Result<Multi> {
        Ok(Multi)
    }

    pub async fn apply(self, trans: &TransactionState, conn: &mut Connection) -> crate::Result<()> {
        conn.write_frame(&crate::Frame::Simple("OK".to_string()))
            .await?;
        let mut multi = trans.multi.lock().unwrap();
        *multi = true;
        Ok(())
    }
}
