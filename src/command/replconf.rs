use bytes::Bytes;

use crate::{parse::Parse, server::ReplicaState, Connection, Frame};

#[derive(Debug)]
pub struct ReplConf {
    args: String,
    option: String,
}

impl ReplConf {
    pub fn new(args: String, option: String) -> Self {
        Self { args, option }
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<ReplConf> {
        let args = parse.next_string()?;
        let option = parse.next_string()?;
        Ok(ReplConf { args, option })
    }

    pub fn args_option(&self) -> (&str, &str) {
        (&self.args, &self.option)
    }

    /// Handle REPLCONF commands from replicas.
    ///
    /// For `REPLCONF ACK <offset>` replicas send their current replication offset.
    /// The master keeps a single tracked `replica_offset` representing the offset used
    /// as the threshold for WAIT. We should treat the incoming offset as an absolute
    /// value (not additively), and count the ACK only if the reported offset is
    /// greater than or equal to the tracked offset. When we see a larger offset we
    /// update the tracked offset to the maximum seen so far.
    pub async fn apply(
        self,
        conn: &mut Connection,
        replica_state: &ReplicaState,
    ) -> crate::Result<()> {
        if self.args == "ACK" {
            let rep_offset: u64 = self.option.parse()?;
            // Load and update the tracked replica offset (kept as the maximum seen).
            let mut off_guard = replica_state.offset.lock().unwrap();
            let mut ack_guard = replica_state.acked.lock().unwrap();

            // If this replica reports an offset >= the tracked offset at the time of the write,
            // count it as an acknowledgement.
            if rep_offset >= *off_guard {
                *ack_guard += 1;
            }

            // Update tracked offset to the max of current tracked and reported offset.
            if rep_offset > *off_guard {
                *off_guard = rep_offset;
            }

            // No immediate reply for ACK messages.
            return Ok(());
        }

        let frame = Frame::Simple("OK".to_string());
        conn.write_frame(&frame).await?;
        Ok(())
    }

    pub fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("replconf".as_bytes()));
        frame.push_bulk(Bytes::from(self.args));
        frame.push_bulk(Bytes::from(self.option));
        frame
    }
}
