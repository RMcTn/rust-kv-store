use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Ping,
    Put((u32, Vec<u8>)),
    Get(u32),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Pong,
    Value(Option<Vec<u8>>),
}
