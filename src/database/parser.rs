use bytes::Bytes;
use std::{collections::HashMap, fs::File, io::Read};
use tokio::time::Duration;

const MAGIC_LEN: usize = 5;
const VERSION_LEN: usize = 4;
pub struct RdbParse;

#[derive(Debug)]
pub struct Database {
    pub entries: HashMap<String, Entry>,
}

#[derive(Debug)]
pub struct Entry {
    pub data: Bytes,
    pub expire: Option<Duration>,
}

impl Database {
    fn new() -> Self {
        Database {
            entries: HashMap::new(),
        }
    }

    fn set(&mut self, key: String, value: Bytes, expire: Option<Duration>) {
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
        let mut database = Database::new();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut position = 0;

        if buffer.len() < MAGIC_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "File too short for magic string",
            ));
        }
        let magic_string = String::from_utf8_lossy(&buffer[position..position + MAGIC_LEN]);
        dbg!("Magic String: {}", magic_string);
        position += MAGIC_LEN;

        if buffer.len() < position + VERSION_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "File too short for version",
            ));
        }
        let version_string = String::from_utf8_lossy(&buffer[position..position + VERSION_LEN]);
        dbg!("Version: {}", version_string);
        position += VERSION_LEN;

        while position < buffer.len() {
            match buffer[position] {
                0xFA => {
                    dbg!("Metadata subsection start");
                    position += 1;
                    let (name, consumed) = decode_string(&buffer, position)?;
                    position += consumed;
                    let (value, consumed) = decode_string(&buffer, position)?;
                    position += consumed;
                    println!("Metadata: {} = {}", name, value);
                }
                0xFE => {
                    dbg!("Database subsection start");
                    position += 1;
                    let (db_index, consumed) = get_length(&buffer, position)?;
                    println!("Database index: {}, Consumed: {}", db_index, consumed);
                    position += consumed;
                }
                0xFB => {
                    println!("Hash table size information");
                    position += 1;
                    let (total_size, consumed) = get_length(&buffer, position)?;
                    position += consumed;
                    let (expire_size, consumed) = get_length(&buffer, position)?;
                    position += consumed;
                    println!("Total keys: {}, Expiring keys: {}", total_size, expire_size);
                }
                0xFC => {
                    println!("Millisecond expiry");
                    position += 1;
                    if buffer.len() < position + 8 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Not enough bytes for millisecond expiry",
                        ));
                    }
                    let expiry =
                        u64::from_le_bytes(buffer[position..position + 8].try_into().unwrap());
                    println!("Expiry (ms): {}", expiry);
                    position += 8;
                }
                0xFD => {
                    println!("Second expiry");
                    position += 1;
                    if buffer.len() < position + 4 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Not enough bytes for second expiry",
                        ));
                    }
                    let expiry =
                        u32::from_le_bytes(buffer[position..position + 4].try_into().unwrap());
                    println!("Expiry (s): {}", expiry);
                    position += 4;
                }
                0x00 => {
                    println!("String value type");
                    position += 1;
                    let (key, consumed) = decode_string(&buffer, position)?;
                    position += consumed;
                    let (value, consumed) = decode_string(&buffer, position)?;
                    position += consumed;
                    database.set(key, Bytes::from(value), None);
                }
                0xFF => {
                    position += 1;
                    if buffer.len() >= position + 8 {
                        let checksum =
                            u64::from_le_bytes(buffer[position..position + 8].try_into().unwrap());
                        println!("Checksum: {}", checksum);
                        // position += 8;
                    }
                    break;
                }
                _ => {
                    dbg!("Unknown byte: 0x{:02X}, skipping", buffer[position]);
                    position += 1;
                }
            }
        }

        Ok(database)
    }
}

fn decode_string(bytes: &[u8], position: usize) -> std::io::Result<(String, usize)> {
    if position >= bytes.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Position out of bounds in decode_string",
        ));
    }

    let byte = bytes[position];
    match byte {
        0xC0 => {
            if position + 1 >= bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Not enough bytes for 8-bit integer string",
                ));
            }
            let value = bytes[position + 1];
            Ok((value.to_string(), 2))
        }
        0xC1 => {
            if position + 2 >= bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Not enough bytes for 16-bit integer string",
                ));
            }
            let value = u16::from_le_bytes(bytes[position + 1..position + 3].try_into().unwrap());
            Ok((value.to_string(), 3))
        }
        0xC2 => {
            if position + 4 >= bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Not enough bytes for 32-bit integer string",
                ));
            }
            let value = u32::from_le_bytes(bytes[position + 1..position + 5].try_into().unwrap());
            Ok((value.to_string(), 5))
        }
        _ => {
            let (length, consumed) = get_length(bytes, position)?;
            if position + consumed + length > bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Not enough bytes for normal string data",
                ));
            }
            let string_data = &bytes[position + consumed..position + consumed + length];
            let string = String::from_utf8_lossy(string_data).to_string(); // Use lossy for non-UTF-8 tolerance
            Ok((string, consumed + length))
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
    match byte >> 6 {
        0b00 => Ok(((byte & 0x3F) as usize, 1)),
        0b01 => {
            if position + 1 >= bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Not enough bytes for 14-bit length",
                ));
            }
            let value = u16::from_be_bytes(bytes[position..position + 2].try_into().unwrap());
            Ok(((value & 0x3FFF) as usize, 2))
        }
        0b10 => {
            if position + 4 >= bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Not enough bytes for 32-bit length",
                ));
            }
            let value = u32::from_be_bytes(bytes[position + 1..position + 5].try_into().unwrap());
            Ok((value as usize, 5))
        }
        0b11 => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Special string encoding (0b11) not allowed in numeric length",
        )),
        _ => unreachable!(),
    }
}
