use std::{str, vec};

use bytes::Bytes;
use thiserror::Error;

use crate::Frame;

#[derive(Debug)]
pub struct Parse {
    parth: vec::IntoIter<Frame>,
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("protocol error; unexpected end of stream")]
    EndOfStream,
    #[error("{0}")]
    Other(#[from] crate::Error),
}

impl Parse {
    pub fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(arr) => arr,
            frame => return Err(format!("protocol error; expected arry got: {:?}", frame).into()),
        };

        Ok(Parse {
            parth: array.into_iter(),
        })
    }

    fn next(&mut self) -> Result<Frame, ParseError> {
        self.parth.next().ok_or(ParseError::EndOfStream)
    }

    pub fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(b) => str::from_utf8(&b[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error; invalid string".into()),
            frame => Err(format!(
                    "protocol error ; expected simple string or bulk but got: {:?}",
                    frame
                )
                .into())
            
        }
    }

    pub fn next_int(&mut self) -> Result<u64, ParseError> {
        use atoi::atoi;
        const MESSAGE: &str = "protocol error; invalid integer";
        match self.next()? {
            Frame::Integer(v) => Ok(v),
            Frame::Simple(s) => atoi::<u64>(s.as_bytes()).ok_or_else(|| MESSAGE.into()),
            Frame::Bulk(b) => atoi::<u64>(&b).ok_or_else(|| MESSAGE.into()),
            frame => Err(format!("protocol error ; expected int but got: {:?}", frame).into())
            
        }
    }

    pub fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            frame => Err(format!(
                "protocol error; expected simple fram or bulk frame, got :{:?}",
                frame
            )
            .into()),
        }
    }

    pub fn finish(&mut self) -> Result<(), ParseError> {
        if self.parth.next().is_none() {
            Ok(())
        } else {
            Err("protocol error; expected end of frame, but there was more".into())
        }
    }
}

impl From<String> for ParseError {
    fn from(value: String) -> Self {
        ParseError::Other(value.into())
    }
}

impl From<&str> for ParseError {
    fn from(value: &str) -> Self {
        ParseError::Other(value.to_string().into())
    }
}
