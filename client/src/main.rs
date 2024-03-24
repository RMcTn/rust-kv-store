use std::{error::Error, net::TcpStream};

use common::{
    command::{Command, Response},
    connection::Connection,
};

fn main() -> Result<(), Box<dyn Error>> {
    let remote_addr = "127.0.0.1:7777";
    println!("Connecting to {}", remote_addr);

    let stream = TcpStream::connect(remote_addr)?;
    println!("Connected to {}", remote_addr);

    let mut connection = Connection::new(stream);

    let key = 50;
    let value = "Will this send?";

    println!("Sending PING command");
    connection.send_command(Command::Ping)?;
    println!("PING command sent");

    loop {
        if let Some(resp) = connection.read_response() {
            match resp {
                Response::Pong => {
                    println!("Got PONG from server");
                    break;
                }
            }
        }
    }

    println!("Sending PUT command with key: {}, value: {}", key, value);
    connection.send_command(Command::Put((key, value.as_bytes().to_vec())))?;
    println!("PUT command sent");

    Ok(())
}
