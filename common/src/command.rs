use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Ping,
    Put((Vec<u8>, Vec<u8>)),
    Get(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Pong,
    Value(Option<Vec<u8>>),
}
