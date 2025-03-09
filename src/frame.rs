use std::{io::Cursor, num::TryFromIntError, string::FromUtf8Error};

use bytes::{Buf, Bytes};
#[derive(Clone, Debug)]
pub enum Frame {
    Simple(String),
    Integer(u64),
    Bulk(Bytes),
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    Incomplete,
    Other(crate::Error),
}

impl Frame {
    pub(crate) fn array() -> Self {
        Frame::Array(vec![])
    }

    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("not a array frame"),
        }
    }

    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'*' => {
                let len = get_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }
            b':' => {
                let len = get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            b'$' => {
                let len = get_decimal(src)?.try_into()?;
                let n = len + 2;

                if src.remaining() < n {
                    return Err(Error::Incomplete);
                }

                let data = Bytes::copy_from_slice(&src.chunk()[..len]);

                skip(src, n)?;

                Ok(Frame::Bulk(data))
            }
            _ => unimplemented!(),
        }
    }

    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            }

            b'$' => {
                let len: usize = get_decimal(src)?.try_into()?;
                skip(src, len + 2)
            }
            b'*' => {
                let len = get_decimal(src)?;
                for _ in 0..len {
                    Frame::check(src)?;
                }

                Ok(())
            }

            value => Err(format!("protocol error; invalid frame type byte `{}`", value).into()),
        }
    }
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(src.get_u8())
}

fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;

    let line = get_line(src)?;
    atoi::<u64>(line).ok_or_else(|| "protocol error: invalid frame format".into())
}

fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    let end = src.get_ref().len() - 1;
    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            return Ok(&src.get_ref()[start..i]);
        }
    }
    Err(Error::Incomplete)
}

impl std::fmt::Display for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::str;
        match self {
            Frame::Simple(response) => response.fmt(f),
            Frame::Integer(v) => v.fmt(f),
            Frame::Bulk(s) => match str::from_utf8(s) {
                Ok(string) => string.fmt(f),
                Err(_) => write!(f, "{:?}", s),
            },

            Frame::Array(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    part.fmt(f)?;
                }
                Ok(())
            }
        }
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::Other(value.into())
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_s: FromUtf8Error) -> Self {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_s: TryFromIntError) -> Self {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Incomplete => "steam ended early".fmt(f),
            Error::Other(err) => err.fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::io::Cursor;

    #[test]
    fn test_parse_integer() {
        let mut input = Cursor::new(&b":123\r\n"[..]);
        let result = Frame::parse(&mut input);
        assert!(result.is_ok());
        if let Ok(Frame::Integer(num)) = result {
            assert_eq!(num, 123);
        } else {
            panic!("Expected Integer frame");
        }
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut input = Cursor::new(&b"$5\r\nhello\r\n"[..]);
        let result = Frame::parse(&mut input);
        assert!(result.is_ok());
        if let Ok(Frame::Bulk(bytes)) = result {
            assert_eq!(bytes, Bytes::from("hello"));
        } else {
            panic!("Expected Bulk frame");
        }
    }

    #[test]
    fn test_parse_array() {
        let mut input = Cursor::new(&b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"[..]);
        let result = Frame::parse(&mut input);
        assert!(result.is_ok());
        if let Ok(Frame::Array(vec)) = result {
            assert_eq!(vec.len(), 2);
            if let Frame::Bulk(bytes) = &vec[0] {
                assert_eq!(bytes, &Bytes::from("hello"));
            }
            if let Frame::Bulk(bytes) = &vec[1] {
                assert_eq!(bytes, &Bytes::from("world"));
            }
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_incomplete_input() {
        let mut input = Cursor::new(&b"$5\r\nhel"[..]);
        let result = Frame::parse(&mut input);
        assert!(matches!(result, Err(Error::Incomplete)));
    }

    #[test]
    fn test_push_bulk() {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("test"));
        if let Frame::Array(vec) = frame {
            assert_eq!(vec.len(), 1);
            if let Frame::Bulk(bytes) = &vec[0] {
                assert_eq!(bytes, &Bytes::from("test"));
            }
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_push_int() {
        let mut frame = Frame::array();
        frame.push_int(42);
        if let Frame::Array(vec) = frame {
            assert_eq!(vec.len(), 1);
            if let Frame::Integer(num) = vec[0] {
                assert_eq!(num, 42);
            }
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_display_bulk() {
        let frame = Frame::Bulk(Bytes::from("hello"));
        assert_eq!(format!("{}", frame), "hello");
    }

    #[test]
    fn test_display_array() {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("hello"));
        frame.push_int(42);
        assert_eq!(format!("{}", frame), "hello 42");
    }

    #[test]
    fn test_empty_array() {
        let mut input = Cursor::new(&b"*0\r\n"[..]);
        let result = Frame::parse(&mut input);
        assert!(result.is_ok());
        if let Ok(Frame::Array(vec)) = result {
            assert_eq!(vec.len(), 0);
        } else {
            panic!("Expected empty Array frame");
        }
    }

    #[test]
    fn test_invalid_length() {
        let mut input = Cursor::new(&b"$a\r\nhello\r\n"[..]);
        let result = Frame::parse(&mut input);
        assert!(matches!(result, Err(Error::Other(_))));
    }
}
