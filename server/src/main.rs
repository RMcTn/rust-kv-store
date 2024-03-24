use std::{error::Error, path::PathBuf, str::FromStr};

use server::Server;

fn main() -> Result<(), Box<dyn Error>> {
    let addr = "127.0.0.1:7777";

    // TODO: Configurable store dir
    let store_dir = PathBuf::from_str("server_store_stuff/")?;

    let mut server = Server::new(addr.parse().unwrap(), &store_dir, false);
    println!("Listening for TCP connections on {}", addr);
    server.run();

    Ok(())
}
