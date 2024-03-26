use std::{
    net::{SocketAddr, TcpListener},
    path::Path,
    sync::{Arc, RwLock},
};

use common::{
    command::{Command, Response},
    connection::Connection,
};
use store::Store;

pub struct Server {
    pub listener: TcpListener,
    pub store: Arc<RwLock<Store>>,
}

impl Server {
    pub fn new(addr: SocketAddr, store_dir: &Path, keep_existing_dir: bool) -> Self {
        let listener = TcpListener::bind(&addr).unwrap();
        let store = Arc::new(RwLock::new(Store::new(&store_dir, keep_existing_dir)));
        Self { store, listener }
    }

    pub fn run(&mut self) {
        // TODO: Don't block for each request
        for stream in self.listener.incoming() {
            let stream = stream.unwrap();
            let connection = Connection::new(stream);
            Self::handle_client(connection, self.store.clone());
        }
    }

    fn handle_client(mut connection: Connection, store: Arc<RwLock<Store>>) {
        println!("Client connected from {}", connection.addr);

        loop {
            if let Ok(cmd) = connection.read_command() {
                dbg!("Got command {:?}", &cmd);
                match cmd {
                    Command::Ping => connection.send_response(Response::Pong).unwrap(),
                    Command::Put((key, value)) => store.write().unwrap().put(key, &value),
                    Command::Get(key) => {
                        let value = store.read().unwrap().get(&key);
                        connection.send_response(Response::Value(value)).unwrap()
                    }
                }
            }
        }
    }
}
