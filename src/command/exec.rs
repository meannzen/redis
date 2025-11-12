use crate::{parse::Parse, server::TransactionState, Connection, Frame};

#[derive(Debug)]
pub struct Exec;

impl Exec {
    // temporary not sure next is parse or not
    pub fn parse_frame(_parse: &mut Parse) -> crate::Result<Exec> {
        Ok(Exec)
    }

    pub async fn apply(self, trans: &TransactionState, conn: &mut Connection) -> crate::Result<()> {
        let frame;
        {
            let mut multi = trans.multi.lock().unwrap();
            if *multi {
                frame = Frame::array();
                *multi = false;
            } else {
                frame = Frame::Error("ERR EXEC without MULTI".to_string());
            }
        }

        conn.write_frame(&frame).await?;
        Ok(())
    }
}
