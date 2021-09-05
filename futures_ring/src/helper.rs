use std::convert;

// when https://github.com/rust-lang/rust/issues/70142 is stable, remove it and use the native
// method
pub trait ResultExt<T, E> {
    fn flatten_result(self) -> Result<T, E>;
}

impl<T, E> ResultExt<T, E> for Result<Result<T, E>, E> {
    fn flatten_result(self) -> Result<T, E> {
        self.and_then(convert::identity)
    }
}
