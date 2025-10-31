use std::time::Duration;

use crate::{parse::Parse, server::ReplicaConnection, Connection, Frame};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Wait {
    numreplicas: u64,
    timeout: Duration,
}

impl Wait {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Wait> {
        let numreplicas: u64 = parse.next_string()?.parse()?;
        let timeout = Duration::from_millis(parse.next_string()?.parse::<u64>()?);
        Ok(Self {
            numreplicas,
            timeout,
        })
    }

    pub async fn apply(
        self,
        conn: &mut Connection,
        replica_connection: &ReplicaConnection,
    ) -> crate::Result<()> {
        let len = replica_connection.lock().unwrap().len() as u64;
        let frame = Frame::Integer(len);
        conn.write_frame(&frame).await?;
        Ok(())
    }
}
