///
pub trait SendRecv {
    ///
    fn get_invoke_request(&self) -> Option<Vec<u8>>;

    ///
    fn get_get_request(&self) -> (u32, Vec<u8>);
}
