extern crate splinter;

use self::splinter::sendrecv::SendRecv;

pub struct YCSBT {}

impl YCSBT {
    pub fn new() -> YCSBT {
        YCSBT {}
    }
}

impl SendRecv for YCSBT {
    fn get_invoke_request(&self) -> Option<Vec<u8>> {
        let mut req = Vec::with_capacity(8);
        req.extend_from_slice("pushback".as_bytes());
        Some(req)
    }

    fn get_get_request(&self) -> (u32, Vec<u8>) {
        (0, Vec::new())
    }
}
