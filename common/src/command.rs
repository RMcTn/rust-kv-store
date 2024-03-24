#[derive(Debug)]
pub enum Command {
    Ping,
    Put((u32, Vec<u8>)),
}

#[derive(Debug)]
pub enum Response {
    Pong,
}
