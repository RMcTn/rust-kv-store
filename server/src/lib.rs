use std::{
    net::{SocketAddr, TcpListener},
    path::Path,
    sync::{
        mpsc::{self, Sender},
        Arc, Mutex,
    },
    thread,
};

use common::{
    command::{Command, Response},
    connection::Connection,
};
use store::Store;

pub struct Server {
    pub listener: TcpListener,
    pub store: Arc<Mutex<Store>>,
}

impl Server {
    pub fn new(addr: SocketAddr, store_dir: &Path, keep_existing_dir: bool) -> Self {
        let listener = TcpListener::bind(&addr).unwrap();
        let store = Arc::new(Mutex::new(Store::new(&store_dir, keep_existing_dir)));
        Self { store, listener }
    }

    pub fn run(&mut self) {
        let (sender, receiver) = mpsc::channel::<(u32, Vec<u8>)>();

        let store = self.store.clone();
        thread::spawn(move || loop {
            match receiver.recv() {
                Ok((key, value)) => {
                    store.lock().unwrap().put(key, &value);
                    dbg!(store.lock().unwrap().get(&key));
                }
                Err(_) => todo!(),
            }
        });

        for stream in self.listener.incoming() {
            let stream = stream.unwrap();
            let connection = Connection::new(stream);
            Self::handle_client(connection, sender.clone());
        }
    }

    fn handle_client(mut connection: Connection, channel: Sender<(u32, Vec<u8>)>) {
        println!("Client connected from {}", connection.addr);

        loop {
            if let Ok(cmd) = connection.read_command() {
                dbg!("Got command {:?}", &cmd);
                match cmd {
                    Command::Ping => connection.send_response(Response::Pong).unwrap(),
                    Command::Put((key, value)) => channel.send((key, value)).unwrap(),
                }
            }
        }
    }
}
