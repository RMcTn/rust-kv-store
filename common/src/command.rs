use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Ping,
    Put((u32, Vec<u8>)),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Pong,
}
