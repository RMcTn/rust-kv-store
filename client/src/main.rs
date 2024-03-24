use std::{error::Error, net::TcpStream};

use client::Client;
use common::connection::Connection;

fn main() -> Result<(), Box<dyn Error>> {
    let remote_addr = "127.0.0.1:7777";
    println!("Connecting to {}", remote_addr);

    let stream = TcpStream::connect(remote_addr)?;
    println!("Connected to {}", remote_addr);

    let connection = Connection::new(stream);

    let mut client = Client::new(connection);

    let key = 50;
    let value = "Will this send?";

    println!("Sending PING command");
    client.ping()?;
    println!("PING command sent");

    println!("Sending PUT command with key: {}, value: {}", key, value);
    client.put(key, value.as_bytes().to_vec())?;
    println!("PUT command sent");

    Ok(())
}
