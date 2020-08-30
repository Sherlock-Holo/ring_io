use std::io;

#[derive(Debug)]
pub enum State {
    Submitted,
    Completed(Option<io::Result<usize>>),
    Canceled,
}