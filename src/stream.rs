use std::{collections::BTreeMap, num::ParseIntError, str::FromStr};

use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub fn is_invalid(&self) -> bool {
        self.ms == 0 && self.seq == 0
    }
}

pub type Fields = Vec<(String, Bytes)>;
#[derive(Debug, Default)]
pub struct Stream {
    entries: BTreeMap<StreamId, Fields>,
}

impl Stream {
    pub fn xadd(&mut self, id: StreamId, fields: Fields) -> StreamId {
        self.entries.insert(id.clone(), fields);
        id
    }

    pub fn xrange(&self, start: StreamId, end: StreamId) -> BTreeMap<StreamId, Fields> {
        self.entries
            .range(start..=end)
            .map(|(id, fields)| (id.clone(), fields.clone()))
            .collect()
    }
    pub fn xread(&self, id: StreamId) -> BTreeMap<StreamId, Fields> {
        self.entries
            .range(id..)
            .map(|(id, fields)| (id.clone(), fields.clone()))
            .collect()
    }

    pub fn last_id(&self) -> Option<StreamId> {
        self.entries.keys().last().cloned()
    }

    pub fn generate_id(&self, ms: u64) -> StreamId {
        if ms == 0 {
            let last_seq = self
                .entries
                .keys()
                .filter(|k| k.ms == ms)
                .map(|k| k.seq)
                .max()
                .unwrap_or(0);
            return StreamId {
                ms,
                seq: last_seq + 1,
            };
        }

        let last_seq_for_ms = self
            .entries
            .keys()
            .filter(|k| k.ms == ms)
            .map(|k| k.seq)
            .max();

        match last_seq_for_ms {
            Some(seq) => StreamId { ms, seq: seq + 1 },
            None => StreamId { ms, seq: 0 },
        }
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

#[derive(Debug)]
pub enum ParseStreamIdError {
    MissingPart,
    EmptyPart,
    InvalidNumber(ParseIntError),
}

impl std::fmt::Display for ParseStreamIdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingPart => write!(f, "invalid stream id: missing '-' separator or part"),
            Self::EmptyPart => write!(f, "invalid stream id: empty millisecorn or sequence"),
            Self::InvalidNumber(e) => write!(f, "invalid number value in steam id: {}", e),
        }
    }
}

impl std::error::Error for ParseStreamIdError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidNumber(e) => Some(e),
            _ => None,
        }
    }
}

impl From<ParseIntError> for ParseStreamIdError {
    fn from(value: ParseIntError) -> Self {
        ParseStreamIdError::InvalidNumber(value)
    }
}

impl FromStr for StreamId {
    type Err = ParseStreamIdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.splitn(2, "-");
        let ms_str = parts.next().ok_or(ParseStreamIdError::MissingPart)?.trim();
        let seq_str = parts.next().ok_or(ParseStreamIdError::MissingPart)?.trim();

        if ms_str.is_empty() || seq_str.is_empty() {
            return Err(ParseStreamIdError::EmptyPart);
        }

        let ms: u64 = ms_str.parse()?;
        let seq: u64 = seq_str.parse()?;

        Ok(StreamId { ms, seq })
    }
}
