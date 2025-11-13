use bytes::Bytes;

use crate::{
    parse::Parse,
    server::{QueueCommand, TransactionState},
    store::Db,
    Connection, Frame,
};

#[derive(Debug, Clone)]
pub struct Incr {
    key: String,
}

impl Incr {
    pub fn new(key: impl ToString) -> Incr {
        Incr {
            key: key.to_string(),
        }
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Incr> {
        let key = parse.next_string()?;
        Ok(Incr { key })
    }

    pub async fn apply(
        self,
        db: &Db,
        conn: &mut Connection,
        trans: &TransactionState,
    ) -> crate::Result<()> {
        use atoi::atoi;
        let mut frame = Frame::Simple("QUEUED".to_string());
        let mut is_queue = false;
        {
            let multi = trans.multi.lock().unwrap();
            if *multi {
                is_queue = true;
                let mut queue_command = trans.queue_command.lock().unwrap();
                queue_command.push_back(QueueCommand::INCR(self.clone()));
            }
        }

        if !is_queue {
            if let Some(value) = db.get(&self.key) {
                if let Some(mut value) = atoi::<u64>(&value) {
                    value += 1;
                    db.set(self.key, Bytes::from(value.to_string()), None);
                    frame = Frame::Integer(value);
                } else {
                    frame = Frame::Error("ERR value is not an integer or out of range".to_string());
                }
            } else {
                db.set(self.key, Bytes::from("1"), None);

                frame = Frame::Integer(1);
            }
        }

        conn.write_frame(&frame).await?;
        Ok(())
    }
}
