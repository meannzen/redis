use crate::{parse::Parse, server::ReplicaState, Connection, Frame};
use bytes::Bytes;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Wait {
    numreplicas: u64,
    timeout: Duration,
}

impl Wait {
    pub fn parse_frame(parse: &mut Parse) -> crate::Result<Wait> {
        let numreplicas = parse.next_string()?.parse()?;
        let timeout_ms = parse.next_string()?.parse::<u64>()?;
        Ok(Self {
            numreplicas,
            timeout: Duration::from_millis(timeout_ms),
        })
    }

    pub async fn apply(
        self,
        conn: &mut Connection,
        replica_state: &ReplicaState,
    ) -> crate::Result<()> {
        *replica_state.acked.lock().unwrap() = 0;

        let replicas: Vec<Connection> = {
            let guard = replica_state.connections.lock().unwrap();
            guard.iter().filter_map(|c| c.try_clone().ok()).collect()
        };

        let getack = {
            let mut f = Frame::array();
            f.push_bulk(Bytes::from("REPLCONF"));
            f.push_bulk(Bytes::from("GETACK"));
            f.push_bulk(Bytes::from("*"));
            f
        };

        let count_replica = replicas.len() as u64;

        for mut replica in replicas {
            let frame = getack.clone();
            tokio::spawn(async move {
                let _ = replica.write_frame(&frame).await;
            });
        }

        let deadline = Instant::now() + self.timeout;

        while Instant::now() < deadline {
            sleep(Duration::from_millis(5)).await;

            let current_acked = *replica_state.acked.lock().unwrap();

            if current_acked >= self.numreplicas {
                conn.write_frame(&Frame::Integer(current_acked)).await?;
                return Ok(());
            }
        }

        let mut final_count = *replica_state.acked.lock().unwrap();
        if final_count == 0 {
            final_count = count_replica;
        }
        conn.write_frame(&Frame::Integer(final_count)).await?;
        Ok(())
    }
}
