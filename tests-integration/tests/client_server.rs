use std::{net::TcpStream, path::PathBuf, thread};

use client::Client;
use common::connection::Connection;
use server::Server;

#[test]
fn ping() {
    let server_addr = "127.0.0.1:3333";
    let store_dir = PathBuf::from("tmp/ping");
    let mut server = Server::new(server_addr.parse().unwrap(), &store_dir, false);

    thread::spawn(move || {
        server.run();
    });

    let server_connection = TcpStream::connect(server_addr).unwrap();
    let connection = Connection::new(server_connection);
    let mut client = Client::new(connection);
    assert!(client.ping().is_ok());
}

#[test]
fn put_and_get() {
    // TODO: Need to do something about these server addresses
    let server_addr = "127.0.0.1:3334";
    let store_dir = PathBuf::from("tmp/put");
    let mut server = Server::new(server_addr.parse().unwrap(), &store_dir, false);

    thread::spawn(move || {
        server.run();
    });

    let server_connection = TcpStream::connect(server_addr).unwrap();
    let connection = Connection::new(server_connection);
    let mut client = Client::new(connection);
    let key = 50;
    let value = "Woowee for tests".as_bytes().to_vec();
    assert!(client.put(key, value.clone()).is_ok());
    assert_eq!(client.get(key).unwrap(), Some(value));
}
