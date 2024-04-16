use std::io;

use common::{
    command::{Command, Response},
    connection::Connection,
};

// TODO: Specify timeout
pub struct Client {
    connection: Connection,
}

impl Client {
    pub fn new(connection: Connection) -> Self {
        // TODO: Feels a bit weird that we need to create the connection for the client, then create
        //  a client. Why aren't we doing this as one?
        Self { connection }
    }
    pub fn ping(&mut self) -> io::Result<()> {
        self.connection.send_command(Command::Ping)?;
        loop {
            if let Ok(Response::Pong) = self.connection.read_response() {
                println!("Got PONG from server");
                return Ok(());
            }
        }
    }

    pub fn put(&mut self, key: &[u8], value: Vec<u8>) -> io::Result<()> {
        self.connection
            .send_command(Command::Put((key.to_vec(), value)))
    }

    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.connection.send_command(Command::Get(key.to_vec()))?;
        loop {
            if let Ok(Response::Value(value)) = self.connection.read_response() {
                println!("Got value from server for key {:?}", key);
                return Ok(value);
            }
        }
    }
}
