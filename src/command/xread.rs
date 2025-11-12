use std::{str::FromStr, time::Duration};

use bytes::Bytes;
use tokio::time::sleep;

use crate::{parse::Parse, store::Db, stream::StreamId, Connection, Frame};

#[derive(Debug)]
pub struct XRead {
    keys: Vec<String>,
    ids: Vec<StreamId>,
    timeout: Option<Duration>,
}

impl XRead {
    pub fn new(keys: Vec<String>, ids: Vec<StreamId>) -> XRead {
        XRead {
            keys,
            ids,
            timeout: None,
        }
    }

    pub fn parse_frame(parse: &mut Parse) -> crate::Result<XRead> {
        let mut keys = vec![];
        let mut ids = vec![];
        let mut timeout = None;
        let mut cmd_str = parse.next_string()?.to_lowercase();
        if cmd_str == "block" {
            timeout = Some(Duration::from_millis(parse.next_string()?.parse::<u64>()?));
            cmd_str = parse.next_string()?.to_lowercase();
        }

        if cmd_str != "streams" {
            return Err(format!("Unknow command: {cmd_str}").into());
        }

        while let Ok(s) = parse.next_string() {
            if s == "$" {
                ids.push(StreamId {
                    ms: u64::MAX,
                    seq: u64::MAX,
                });
            } else if let Ok(id) = StreamId::from_str(&s) {
                ids.push(id);
            } else {
                dbg!(&s);
                keys.push(s);
            }
        }

        Ok(XRead { keys, ids, timeout })
    }

    pub async fn apply(self, db: &Db, conn: &mut Connection) -> crate::Result<()> {
        let mut self_mut = self;

        let mut actual_ids: Vec<StreamId> = Vec::with_capacity(self_mut.ids.len());

        for (key, id) in self_mut.keys.iter().zip(self_mut.ids.iter()) {
            if id.ms == u64::MAX && id.seq == u64::MAX {
                let last_id = db.get_last_stream_id(key);
                actual_ids.push(last_id.unwrap_or(StreamId { ms: 0, seq: 0 }));
            } else {
                actual_ids.push(id.clone());
            }
        }
        self_mut.ids = actual_ids;

        let deadline = self_mut
            .timeout
            .filter(|&d| d > Duration::from_millis(0))
            .map(|d| tokio::time::Instant::now() + d);

        loop {
            let mut final_out = Frame::array();
            let mut has_entries = false;

            for (key, id) in self_mut.keys.iter().zip(self_mut.ids.iter()) {
                let mut key_wrapper = Frame::array();

                if let Frame::Array(ref mut wrapper_vec) = key_wrapper {
                    wrapper_vec.push(Frame::Bulk(Bytes::from(key.clone())));
                }

                let mut entries_array = Frame::array();

                if let Some(entries) = db.xread(key.clone(), id.clone()) {
                    let filtered_entries: Vec<_> = entries
                        .into_iter()
                        .filter(|(entry_id, _)| entry_id > id)
                        .collect();

                    if !filtered_entries.is_empty() {
                        has_entries = true;
                    }

                    for (entry_id, fields) in filtered_entries.into_iter() {
                        let mut entry = Frame::array();

                        if let Frame::Array(ref mut entry_vec) = entry {
                            entry_vec.push(Frame::Bulk(Bytes::from(entry_id.to_string())));

                            let mut fields_arr = Frame::array();
                            if let Frame::Array(ref mut fields_vec) = fields_arr {
                                for (name, value) in fields.into_iter() {
                                    fields_vec.push(Frame::Bulk(Bytes::from(name)));
                                    fields_vec.push(Frame::Bulk(value));
                                }
                            }
                            entry_vec.push(fields_arr);
                        }

                        if let Frame::Array(ref mut entries_vec) = entries_array {
                            entries_vec.push(entry);
                        }
                    }
                }

                if let Frame::Array(ref mut wrapper_vec) = key_wrapper {
                    wrapper_vec.push(entries_array);
                }

                if let Frame::Array(ref mut final_vec) = final_out {
                    final_vec.push(key_wrapper);
                }
            }

            if has_entries {
                conn.write_frame(&final_out).await?;
                return Ok(());
            }

            if self_mut.timeout.is_none()
                || (self_mut
                    .timeout
                    .as_ref()
                    .is_some_and(|d| d > &Duration::from_millis(0) && deadline.is_none()))
            {
                conn.write_null_array().await?;
                return Ok(());
            }

            if self_mut.timeout == Some(Duration::from_millis(0)) {
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            let time_remaining =
                deadline.map(|d| d.saturating_duration_since(tokio::time::Instant::now()));

            match time_remaining {
                Some(remaining) if remaining > Duration::from_millis(0) => {
                    let wait_duration = remaining.min(Duration::from_millis(100));
                    sleep(wait_duration).await;
                }
                _ => {
                    conn.write_null_array().await?;
                    return Ok(());
                }
            }
        }
    }
}
