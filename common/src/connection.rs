use std::{
    error::Error,
    io::{self, BufReader, BufWriter, Write},
    net::{SocketAddr, TcpStream},
};

use serde::Deserialize;

use crate::command::{Command, Response};

pub struct Connection {
    writer: BufWriter<TcpStream>,
    reader: BufReader<TcpStream>,
    pub addr: SocketAddr,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        let addr = stream.peer_addr().unwrap();
        Self {
            writer: BufWriter::new(stream.try_clone().unwrap()),
            reader: BufReader::new(stream.try_clone().unwrap()),
            addr,
        }
    }

    pub fn read_command(&mut self) -> Result<Command, Box<dyn Error>> {
        let mut de = serde_json::Deserializer::from_reader(&mut self.reader);
        let command = Command::deserialize(&mut de).unwrap();
        Ok(command)
    }

    pub fn send_command(&mut self, cmd: Command) -> io::Result<()> {
        let json = serde_json::to_string(&cmd)?;
        self.writer.write_all(&json.as_bytes())?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn send_response(&mut self, resp: Response) -> io::Result<()> {
        let json = serde_json::to_string(&resp)?;
        self.writer.write_all(&json.as_bytes())?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn read_response(&mut self) -> Result<Response, Box<dyn Error>> {
        let mut de = serde_json::Deserializer::from_reader(&mut self.reader);
        let response = Response::deserialize(&mut de).unwrap();
        Ok(response)
    }
}
