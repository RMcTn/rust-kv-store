#[derive(Debug)]
pub enum Command<'a> {
    Ping,
    Put((u32, &'a [u8])),
}

#[derive(Debug)]
pub enum Response {
    Pong,
}
