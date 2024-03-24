use std::io::Cursor;
use std::io::Read;

use bytes::Buf;

use crate::command::Command;
use crate::command::Response;

/// Going with something like the Redis protocol here (although not exact) - https://redis.io/docs/reference/protocol-spec/
pub enum Frame {
    Simple(String),
    // TODO: Is key value a bit higher level for frames? Feels like something more application
    // level.
    KeyValue(Vec<u8>, Vec<u8>),
}

impl Frame {
    const DELIMITER: &'static [u8; 2] = b"\r\n";
    pub fn try_parse(mut bytes: &mut Cursor<&[u8]>) -> Option<Frame> {
        let first_byte = Self::get_u8(&mut bytes)?;
        match first_byte {
            b'+' => {
                // Read until \r\n

                let start = bytes.position() as usize;
                let end = bytes.get_ref().len() - 1;
                for i in start..end {
                    if bytes.get_ref()[i] == b'\r' && bytes.get_ref()[i + 1] == b'\n' {
                        bytes.set_position(i as u64 + 2);
                        let line = &bytes.get_ref()[start..i];
                        return Some(Frame::Simple(String::from_utf8_lossy(line).into_owned()));
                    }
                }
                return None;
            }
            b'$' => {
                // $<key-length>\r\n<key>\r\n<value-length>\r\n<value>\r\n
                let key_length = Self::get_u32(bytes)? as usize; // TODO: FIXME: Byte endianness!
                Self::advance_past_delimiter(bytes)?;

                let key = Self::get_bytes(bytes, key_length)?;
                Self::advance_past_delimiter(bytes)?;
                let value_length = Self::get_u32(bytes)? as usize; // TODO: FIXME: Byte endianness!
                Self::advance_past_delimiter(bytes)?;
                let value = Self::get_bytes(bytes, value_length)?;
                Self::advance_past_delimiter(bytes)?;

                return Some(Frame::KeyValue(key, value));
            }
            0 => return None,
            byte => todo!("Handle {}", byte),
        }
    }

    fn get_u8(cursor: &mut Cursor<&[u8]>) -> Option<u8> {
        if !cursor.has_remaining() {
            return None;
        }

        return Some(cursor.get_u8());
    }

    fn get_u32(cursor: &mut Cursor<&[u8]>) -> Option<u32> {
        if cursor.remaining() < 4 {
            return None;
        }

        return Some(cursor.get_u32());
    }

    fn get_bytes(cursor: &mut Cursor<&[u8]>, count: usize) -> Option<Vec<u8>> {
        if cursor.remaining() < count {
            return None;
        }

        let mut buffer = vec![0; count];
        cursor.read_exact(&mut buffer).unwrap();

        Some(buffer)
    }

    fn advance_past_delimiter(cursor: &mut Cursor<&[u8]>) -> Option<()> {
        if cursor.remaining() < 2 {
            return None;
        }

        let pos = cursor.position() as usize;
        if (cursor.get_ref()[pos..=(pos + 1)]) != *Self::DELIMITER {
            return None;
        }
        cursor.advance(2);
        Some(())
    }

    pub fn from_cmd(cmd: &Command) -> Frame {
        match cmd {
            Command::Ping => Frame::Simple("PING".to_string()),
            Command::Put((key, value)) => {
                // SPEEDUP: Don't clone these
                let key = key.to_be_bytes().to_vec();
                let value: Vec<u8> = value.to_vec();
                Frame::KeyValue(key, value)
            }
        }
    }

    pub fn from_response(resp: &Response) -> Frame {
        match resp {
            Response::Pong => Frame::Simple("PONG".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};

    use super::Frame;

    #[test]
    fn parse_key_value_frame() {
        let mut bytes = vec![];
        let key = 5_u32;
        let value = 99_u32;
        let value = value.to_be_bytes();
        let _ = bytes.write_all(b"$");
        let _ = bytes.write_all(&4_u32.to_be_bytes());
        let _ = bytes.write_all(b"\r\n");
        let _ = bytes.write_all(&key.to_be_bytes());
        let _ = bytes.write_all(b"\r\n");
        let value_len = value.len() as u32;
        let _ = bytes.write_all(&value_len.to_be_bytes());
        let _ = bytes.write_all(b"\r\n");
        let _ = bytes.write_all(&value);
        let _ = bytes.write_all(b"\r\n");

        let mut cursor = io::Cursor::new(bytes.as_slice());

        let frame = Frame::try_parse(&mut cursor).unwrap();

        match frame {
            Frame::KeyValue(k, v) => {
                assert_eq!(k, 5_u32.to_be_bytes());
                assert_eq!(v, 99_u32.to_be_bytes());
            }
            _ => assert!(false, "Frame was not parsed as key_value"),
        }
    }
}
