use std::{
    error::Error,
    net::TcpListener,
    path::PathBuf,
    str::FromStr,
    sync::mpsc::{self, Sender},
    thread,
};

use common::{
    command::{Command, Response},
    connection::Connection,
};
use store::Store;

fn main() -> Result<(), Box<dyn Error>> {
    let addr = "127.0.0.1:7777";
    let listener = TcpListener::bind(&addr)?;

    println!("Listening for TCP connections on {}", addr);

    // TODO: Configurable store dir
    let store_dir = PathBuf::from_str("server_store_stuff/")?;

    let mut store = Store::new(&store_dir, true);
    let (sender, receiver) = mpsc::channel::<(u32, Vec<u8>)>();

    thread::spawn(move || loop {
        match receiver.recv() {
            Ok((key, value)) => {
                store.put(key, &value);
                dbg!(store.get(&key));
            }
            Err(_) => todo!(),
        }
    });

    for stream in listener.incoming() {
        let stream = stream?;
        let connection = Connection::new(stream);
        handle_client(connection, sender.clone());
    }

    Ok(())
}

fn handle_client(mut connection: Connection, channel: Sender<(u32, Vec<u8>)>) {
    println!("Client connected from {}", connection.addr);

    loop {
        if let Some(cmd) = connection.read_command() {
            dbg!("Got command {:?}", &cmd);
            match cmd {
                Command::Ping => connection.send_response(Response::Pong).unwrap(),
                Command::Put((key, value)) => channel.send((key, value)).unwrap(),
            }
        }
    }
}
