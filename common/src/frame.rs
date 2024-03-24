use std::io::Cursor;
use std::io::Read;

use bytes::Buf;

use crate::command::Command;
use crate::command::Response;

/// Going with something like the Redis protocol here (although not exact) - https://redis.io/docs/reference/protocol-spec/
pub enum Frame {
    Simple(String),
    Biggie(Vec<u8>, Vec<u8>),
}

impl Frame {
    const DELIMITER: &'static [u8; 2] = b"\r\n";
    pub fn try_parse(mut bytes: &mut Cursor<&[u8]>) -> Option<Frame> {
        match Self::get_u8(&mut bytes)? {
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
                // Ready many \r\n until the last one

                // $<key-length>\r\n<key>\r\n<value-length>\r\n<value>\r\n
                let start = bytes.position() as usize;
                let end = bytes.get_ref().len() - 1;
                // read 4(n) bytes, read \r\n, read n bytes, read \r\n, read 4(x) bytes, read \r\n,
                // read x bytes, read \r\n

                let key_length = Self::get_u32(bytes)? as usize; // TODO: FIXME: Byte endianness!
                Self::advance_past_delimiter(bytes)?;

                let key = Self::get_bytes(bytes, key_length)?;
                Self::advance_past_delimiter(bytes)?;
                let value_length = Self::get_u32(bytes)? as usize; // TODO: FIXME: Byte endianness!
                let value = Self::get_bytes(bytes, value_length)?;
                Self::advance_past_delimiter(bytes)?;

                return Some(Frame::Biggie(key, value));
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
        if cursor.get_ref()[pos] != b'\r' && cursor.get_ref()[pos + 1] != b'\n' {
            return None;
        }
        cursor.advance(2);
        Some(())
    }

    pub fn from_cmd(cmd: &Command) -> Frame {
        match cmd {
            Command::Ping => Frame::Simple("PING".to_string()),
            Command::Put((key, value)) => {
                // TODO: FIXME: Need to think about byte endianness
                // SPEEDUP: Don't clone these
                let key = key.to_string().as_bytes().to_vec();
                let value: Vec<u8> = value.to_vec();
                Frame::Biggie(key, value)
            }
        }
    }

    pub fn from_response(resp: &Response) -> Frame {
        match resp {
            Response::Pong => Frame::Simple("PONG".to_string()),
        }
    }
}
