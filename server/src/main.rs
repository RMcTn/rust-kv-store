use std::{error::Error, net::TcpListener, path::PathBuf, str::FromStr};

use common::{
    command::{Command, Response},
    connection::Connection,
};
use store::Store;

fn main() -> Result<(), Box<dyn Error>> {
    let addr = "127.0.0.1:7777";
    let listener = TcpListener::bind(&addr)?;

    println!("Listening for TCP connections on {}", addr);

    let store_dir = PathBuf::from_str("store_stuff/")?;

    let store = Store::new(&store_dir, true);

    for stream in listener.incoming() {
        let stream = stream?;
        let connection = Connection::new(stream);
        handle_client(connection);
    }

    Ok(())
}

fn handle_client(mut connection: Connection) {
    println!("Client connected from {}", connection.addr);

    loop {
        if let Some(cmd) = connection.read_command() {
            dbg!("Got command {:?}", cmd);
            match cmd {
                Command::Ping => connection.send_response(Response::Pong).unwrap(),
            }
        }
    }
}
