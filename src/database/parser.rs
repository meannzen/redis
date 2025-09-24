use bytes::Bytes;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::time::{Duration, UNIX_EPOCH};
use tokio::time::Duration as TokioDuration;

const MAGIC_LEN: usize = 9;
const RDB_MAGIC: &[u8] = b"REDIS";

pub struct RdbParse;

#[derive(Debug, Default)]
pub struct Database {
    pub entries: HashMap<String, Entry>,
}

#[derive(Debug)]
pub struct Entry {
    pub data: Bytes,
    pub expire: Option<TokioDuration>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            entries: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: Bytes, expire: Option<TokioDuration>) {
        self.entries.insert(
            key,
            Entry {
                data: value,
                expire,
            },
        );
    }
}

impl RdbParse {
    pub fn parse(path: &str) -> Result<Database, std::io::Error> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Self::parse_bytes(&buffer)
    }

    pub fn parse_bytes(buffer: &[u8]) -> Result<Database, std::io::Error> {
        let mut database = Database::new();
        let mut position = 0;
        let mut current_expiry: Option<TokioDuration> = None;

        if buffer.len() < MAGIC_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "File too short for magic string",
            ));
        }

        if &buffer[position..position + 5] != RDB_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid RDB magic string",
            ));
        }
        position += MAGIC_LEN;

        while position < buffer.len() {
            let opcode = buffer[position];

            match opcode {
                0xFA => {
                    position += 1;
                    if let Ok((_, _, consumed)) = Self::decode_string(&buffer, position) {
                        position += consumed;
                    } else {
                        break;
                    }
                    if let Ok((_, _, consumed)) = Self::decode_string(&buffer, position) {
                        position += consumed;
                    } else {
                        break;
                    }
                }
                0xFE => {
                    position += 1;
                    if let Ok((_, consumed)) = Self::get_length(&buffer, position) {
                        position += consumed;
                    } else {
                        break;
                    }
                }
                0xFB => {
                    position += 1;
                    if let Ok((_, consumed)) = Self::get_length(&buffer, position) {
                        position += consumed;
                    } else {
                        break;
                    }
                    if let Ok((_, consumed)) = Self::get_length(&buffer, position) {
                        position += consumed;
                    } else {
                        break;
                    }
                }
                0xFC => {
                    position += 1;
                    if buffer.len() >= position + 8 {
                        let expiry_ms =
                            u64::from_le_bytes(buffer[position..position + 8].try_into().unwrap());
                        current_expiry = Self::calculate_expiry(expiry_ms);
                        position += 8;
                    } else {
                        break;
                    }
                }
                0xFD => {
                    position += 1;
                    if buffer.len() >= position + 4 {
                        let expiry_sec =
                            u32::from_le_bytes(buffer[position..position + 4].try_into().unwrap())
                                as u64;
                        current_expiry = Self::calculate_expiry(expiry_sec * 1000);
                        position += 4;
                    } else {
                        break;
                    }
                }
                0x00 => {
                    position += 1;
                    match Self::decode_string(&buffer, position) {
                        Ok((key, _, consumed)) => {
                            position += consumed;
                            match Self::decode_string(&buffer, position) {
                                Ok((_, value_bytes, consumed)) => {
                                    position += consumed;
                                    database.set(key, value_bytes, current_expiry.take());
                                }
                                Err(_) => {
                                    current_expiry.take();
                                    break;
                                }
                            }
                        }
                        Err(_) => {
                            current_expiry.take();
                            break;
                        }
                    }
                }
                0xFF => {
                    break;
                }
                _ => {
                    position += 1;
                    while position < buffer.len() {
                        if [0xFA, 0xFE, 0xFB, 0xFC, 0xFD, 0x00, 0xFF].contains(&buffer[position]) {
                            break;
                        }
                        position += 1;
                    }
                }
            }
        }

        Ok(database)
    }

    fn calculate_expiry(expiry_ms: u64) -> Option<TokioDuration> {
        let expiry_instant = UNIX_EPOCH + Duration::from_millis(expiry_ms);
        let now = std::time::SystemTime::now();

        if now >= expiry_instant {
            Some(TokioDuration::from_secs(0))
        } else {
            expiry_instant
                .duration_since(now)
                .ok()
                .map(|d| TokioDuration::from_secs(d.as_secs()))
        }
    }

    fn decode_string(bytes: &[u8], position: usize) -> std::io::Result<(String, Bytes, usize)> {
        if position >= bytes.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Position out of bounds in decode_string",
            ));
        }

        let byte = bytes[position];

        if byte >> 6 == 0b11 {
            match byte {
                0xC0 | 0xC1 | 0xC2 => {
                    let (len, bytes_read) = match byte {
                        0xC0 => (1, 1),
                        0xC1 => (2, 2),
                        0xC2 => (4, 4),
                        _ => unreachable!(),
                    };

                    if position + bytes_read > bytes.len() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Not enough bytes for integer string",
                        ));
                    }

                    let value = match len {
                        1 => (bytes[position + 1] as i8).to_string(),
                        2 => i16::from_le_bytes(
                            bytes[position + 1..position + 3].try_into().unwrap(),
                        )
                        .to_string(),
                        4 => i32::from_le_bytes(
                            bytes[position + 1..position + 5].try_into().unwrap(),
                        )
                        .to_string(),
                        _ => unreachable!(),
                    };
                    let data = &bytes[position..position + bytes_read];
                    Ok((value, Bytes::from(data.to_vec()), bytes_read))
                }
                0xC3 => Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "LZF compressed string not supported",
                )),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    format!("Special string encoding 0x{:02X} not supported", byte),
                )),
            }
        } else {
            match byte {
                0x00..=0x3F => {
                    let len = (byte & 0x3F) as usize;
                    if position + 1 + len > bytes.len() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Not enough bytes for raw string",
                        ));
                    }
                    let data = &bytes[position + 1..position + 1 + len];
                    let string = String::from_utf8_lossy(data).into_owned();
                    Ok((string, Bytes::from(data.to_vec()), 1 + len))
                }
                _ => {
                    let (length, consumed) = Self::get_length(bytes, position)?;
                    if position + consumed + length > bytes.len() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Not enough bytes for encoded string data",
                        ));
                    }
                    let data = &bytes[position + consumed..position + consumed + length];
                    let string = String::from_utf8_lossy(data).into_owned();
                    Ok((string, Bytes::from(data.to_vec()), consumed + length))
                }
            }
        }
    }

    fn get_length(bytes: &[u8], position: usize) -> std::io::Result<(usize, usize)> {
        if position >= bytes.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Position out of bounds in get_length",
            ));
        }

        let byte = bytes[position];
        let prefix = byte >> 6;

        match prefix {
            0b00 => Ok(((byte & 0x3F) as usize, 1)),
            0b01 => {
                if position + 2 > bytes.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Not enough bytes for 14-bit length",
                    ));
                }
                let value =
                    u16::from_be_bytes([byte & 0x3F, bytes[position + 1]].try_into().unwrap())
                        as usize;
                Ok((value, 2))
            }
            0b10 => {
                if position + 5 > bytes.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Not enough bytes for 32-bit length",
                    ));
                }
                let value =
                    u32::from_be_bytes(bytes[position + 1..position + 5].try_into().unwrap())
                        as usize;
                Ok((value, 5))
            }
            0b11 => match byte {
                0xC0 | 0xC1 | 0xC2 => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Integer encoding should be handled in decode_string",
                )),
                0xC3 => Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "LZF compression not supported",
                )),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    format!("Special encoding 0x{:02X} not supported", byte),
                )),
            },
            _ => unreachable!(),
        }
    }
}
