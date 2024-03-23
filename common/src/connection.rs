use std::{
    io::{self, BufReader, BufWriter, Read, Write},
    net::{SocketAddr, TcpStream},
};

use bytes::{Buf, BytesMut};

use crate::{
    command::{Command, Response},
    frame::Frame,
};

pub struct Connection {
    writer: BufWriter<TcpStream>,
    reader: BufReader<TcpStream>,
    buffer: BytesMut,
    pub addr: SocketAddr,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        let addr = stream.peer_addr().unwrap();
        Self {
            writer: BufWriter::new(stream.try_clone().unwrap()),
            reader: BufReader::new(stream.try_clone().unwrap()),
            addr,
            buffer: BytesMut::zeroed(1024),
        }
    }

    pub fn read_command(&mut self) -> Option<Command> {
        if let Some(frame) = self.read_frame() {
            match frame {
                Frame::Simple(s) => {
                    if s == "PING" {
                        return Some(Command::Ping);
                    }
                }
                Frame::Biggie(_) => todo!(),
            }
        }
        return None;
    }

    pub fn send_command(&mut self, cmd: Command) -> io::Result<()> {
        match cmd {
            Command::Ping => {
                let frame = Frame::from_cmd(&cmd);
                self.send_frame(&frame)?;
            }
            Command::Put(_) => {
                let frame = Frame::from_cmd(&cmd);
                self.send_frame(&frame)?;
            }
        }
        Ok(())
    }

    pub fn send_response(&mut self, resp: Response) -> io::Result<()> {
        match resp {
            Response::Pong => {
                let frame = Frame::from_response(&resp);
                self.send_frame(&frame)?;
            }
        }
        Ok(())
    }

    pub fn read_response(&mut self) -> Option<Response> {
        if let Some(frame) = self.read_frame() {
            self.buffer.clear();
            match frame {
                Frame::Simple(s) => {
                    dbg!(&s);
                    if s == "PONG" {
                        return Some(Response::Pong);
                    }
                }
                Frame::Biggie(_) => todo!(),
            }
        }
        return None;
    }

    fn read_frame(&mut self) -> Option<Frame> {
        loop {
            if let Some(frame) = self.parse_frame() {
                return Some(frame);
            }

            if let Ok(bytes_read) = self.reader.read(&mut self.buffer) {
                // TODO: Handle disconnects
            }
        }
    }

    fn send_frame(&mut self, frame: &Frame) -> io::Result<()> {
        self.write_frame(frame)
    }

    fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(s) => {
                self.writer.write(b"+")?;
                self.writer.write_all(s.as_bytes())?;
                self.writer.write_all(b"\r\n")?;
                self.writer.flush()?;
            }
            Frame::Biggie(_) => todo!(),
        }
        Ok(())
    }

    fn parse_frame(&mut self) -> Option<Frame> {
        let mut cursor = io::Cursor::new(self.buffer.as_ref());
        if let Some(frame) = Frame::try_parse(&mut cursor) {
            let frame_length = cursor.position();

            cursor.set_position(0);
            self.buffer.advance(frame_length as usize);
            return Some(frame);
        }
        None
    }
}
