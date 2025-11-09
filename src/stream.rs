use std::{collections::BTreeMap, num::ParseIntError, str::FromStr};

use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct StreamId {
    ms: u64,
    seq: u64,
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
    InvalideNumber(ParseIntError),
}

impl std::fmt::Display for ParseStreamIdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingPart => write!(f, "invalid stream id: missing '-' seperator or part"),
            Self::EmptyPart => write!(f, "invalid stream id: empty millisecorn or sequence"),
            Self::InvalideNumber(e) => write!(f, "invalid number value in steam id: {}", e),
        }
    }
}

impl std::error::Error for ParseStreamIdError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalideNumber(e) => Some(e),
            _ => None,
        }
    }
}

impl From<ParseIntError> for ParseStreamIdError {
    fn from(value: ParseIntError) -> Self {
        ParseStreamIdError::InvalideNumber(value)
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
