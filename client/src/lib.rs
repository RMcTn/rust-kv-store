use std::io;

use common::{
    command::{Command, Response},
    connection::Connection,
};

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
            if let Ok(resp) = self.connection.read_response() {
                match resp {
                    Response::Pong => {
                        println!("Got PONG from server");
                        return Ok(());
                    }
                }
            }
        }
    }

    pub fn put(&mut self, key: u32, value: Vec<u8>) -> io::Result<()> {
        self.connection.send_command(Command::Put((key, value)))
    }
}
