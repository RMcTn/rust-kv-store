use std::io::{self, Cursor};

use bytes::Buf;

use crate::command::Command;
use crate::command::Response;

/// Going with something like the Redis protocol here (although not exact) - https://redis.io/docs/reference/protocol-spec/
pub enum Frame {
    Simple(String),
    Biggie(Vec<u8>),
}

impl Frame {
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

    pub fn from_cmd(cmd: &Command) -> Frame {
        match cmd {
            Command::Ping => Frame::Simple("PING".to_string()),
            Command::Put((key, value)) => {
                // Encode our key and value in the below format.
                // Just have a custom format?
                // $<key-length>\r\n<key>\r\n<value-length>\r\n<value>\r\n
                let mut buffer = vec![];

                buffer.push(b'$');
                buffer.push(b'4'); // 4 bytes since the key is just u32
                buffer.extend_from_slice(b"\r\n");
                // TODO: Please just make the keys bytes at this point
                buffer.extend_from_slice(key.to_string().as_bytes());
                buffer.extend_from_slice(b"\r\n");
                buffer.extend_from_slice(value.len().to_string().as_bytes());
                buffer.extend_from_slice(b"\r\n");
                buffer.extend_from_slice(value);

                Frame::Biggie(buffer)
            }
        }
    }

    pub fn from_response(resp: &Response) -> Frame {
        match resp {
            Response::Pong => Frame::Simple("PONG".to_string()),
        }
    }
}
