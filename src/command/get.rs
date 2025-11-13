use crate::{
    parse::Parse,
    server::{QueueCommand, TransactionState},
    store::Db,
    Connection, Frame,
};

#[derive(Debug, Clone)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: impl ToString) -> Self {
        Get {
            key: key.to_string(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Get> {
        let key = parse.next_string()?;
        Ok(Get { key })
    }

    pub async fn apply(
        self,
        db: &Db,
        conn: &mut Connection,
        trans: &TransactionState,
    ) -> crate::Result<()> {
        let mut is_queue = false;
        {
            let multi = trans.multi.lock().unwrap();
            if *multi {
                is_queue = true;
                let mut queue_command = trans.queue_command.lock().unwrap();
                queue_command.push_back(QueueCommand::GET(self.clone()));
            }
        }

        let response = if is_queue {
            Frame::Simple("QUEUED".to_string())
        } else if let Some(value) = db.get(&self.key) {
            Frame::Bulk(value)
        } else {
            Frame::Null
        };

        conn.write_frame(&response).await?;
        Ok(())
    }
}
